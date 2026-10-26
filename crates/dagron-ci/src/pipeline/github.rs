//! The GitHub Actions front-end, level 1: a `.github/workflows` file read
//! into the same IR a native pipeline is, for the event at hand.
//!
//! Level 1 is the workflow file itself: its triggers, its jobs, their
//! matrices, containers and services, and their `run:` steps. An action
//! (`uses:`) is level 2, and what an action leans on (the cache, artifacts,
//! a token) is level 3. Of level 2 a local action is here
//! ([`local_action`]): one in the repository, whose steps the step runner
//! runs. Nothing else of either is. A workflow that needs it is refused,
//! with what it needs: nothing in a file is passed over without a word.
//!
//! **Which files.** None, until the repository says so. `.dagron/ci.yaml`
//! lists the workflow files that run here ([`Settings`]), since a
//! repository that has workflows has GitHub running them already.
//!
//! **Two readings.** [`parse`] reads a file for what does not depend on an
//! event: its shape, what it uses that is not taken, and whether each
//! expression is one. [`Workflow::lower`] then reads it for an event: it
//! evaluates what is known when a run is made (the event, a dispatch's
//! inputs, a matrix), expands each matrix into jobs, and leaves out a job
//! whose `if:` is false, with the jobs that needed it.
//!
//! **What is evaluated where.**
//! - *When the run is made:* a job's `if:`, `runs-on`, `container`,
//!   `services`, `strategy`, `env`, `timeout-minutes` and
//!   `continue-on-error`, the workflow's `env` and `concurrency`.
//! - *While the job runs,* by the step runner, which has what only it can
//!   know (how the steps before went, what they wrote to `GITHUB_ENV` and
//!   `GITHUB_OUTPUT`): everything of a step.
//!
//! **A job's `if:` and the status functions.** With none, a job runs where
//! every job it needs succeeded. `always()`, `failure()` and their like say
//! otherwise, and are answered here for both cases, every needed job having
//! succeeded and one having failed: the job gets the trigger rule that says
//! the same (`all_done`, `one_failed`). An `if:` that reads `needs` is not
//! taken yet.
//!
//! **An expression in a script.** `${{ … }}` in a `run:` is pasted into the
//! script by GitHub before any shell reads it, which is how a pull
//! request's title becomes a command. Here an expression that reads only
//! what no outsider chooses (a matrix value, the commit's id, the event's
//! name) is pasted as GitHub pastes it, and any other is replaced by a
//! shell variable that holds its value. So `echo "${{ github.ref_name }}"`
//! prints the branch's name and runs nothing of it. Inside single quotes a
//! variable is not expanded: such a value is passed through `env:`.
//!
//! **A `{{` in a workflow file is text.** dagron fills each `{{ name }}` of
//! a task's text with the run's parameter of that name, before anything
//! runs: how a native pipeline names what it may. A workflow file is not
//! written for that, and its text is not the only text here that is not:
//! an input's value and a branch's name are in a job's contexts. Left as it
//! is, a script's `{{ version }}` would be filled with an input called
//! `version`, and a dispatcher's `{{ ci_ref }}` with a branch's name inside
//! the JSON the step runner reads its contexts from. So every `{{` a
//! workflow's jobs carry is written as the parameter [`BRACES`], whose value
//! is the two braces ([`inert`]): dagron fills it in, once, and what was
//! written arrives as it was written.

use std::collections::{BTreeMap, BTreeSet};

use dagron_ci_expr::{self as expr, Host, Template, Value as Ev};
use serde_json::{json, Map, Value};

use super::compile::{Event, ACTIONS_CACHE_RESTORE_FROM, SCHEDULE_INPUT};
use super::glob::Patterns;
use super::{
    Concurrency, Dispatch, Input, MergeGroupFilter, Pipeline, Problem, PullRequestFilter,
    PushFilter, Step, Task, Triggers,
};

/// Where a repository's workflow files are.
pub const WORKFLOW_DIR: &str = ".github/workflows";

/// The repository's settings file, beside its pipelines.
pub const SETTINGS_FILE: &str = ".dagron/ci.yaml";

/// The most jobs one workflow may become, matrices expanded: GitHub's own
/// ceiling for a matrix.
pub const MAX_JOBS: usize = 256;

/// The appendix of the plan that says what of GitHub Actions is taken, and
/// when: named in every refusal, so that its reader knows where to look.
const MATRIX: &str = "docs/CI_GITHUB.md";

/// The shell a `run:` step has when nothing names one: `bash` where the
/// job's image has one and `sh` where it does not, as GitHub's runner has
/// it on Linux. The step runner knows it by this name.
pub const DEFAULT_SHELL: &str = "auto";

/// The triggers an event is made for here, by GitHub's names.
const HEARD: &[&str] = &[
    "merge_group",
    "pull_request",
    "push",
    "schedule",
    "workflow_dispatch",
];

/// A repository's settings for this front-end: `.dagron/ci.yaml`, under
/// `github:`.
///
/// ```yaml
/// github:
///   workflows: [ci.yml, lint.yml]
///   runs_on:
///     ubuntu-latest: docker.io/library/ubuntu:24.04
///   provided: [dtolnay/rust-toolchain]
///   docker: true
/// ```
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Settings {
    /// The files of `.github/workflows` that run here, by name. None unless
    /// listed.
    pub workflows: Vec<String>,
    /// The image a `runs-on` label means. A job whose label has none, and
    /// that names no `container`, is refused.
    pub runs_on: BTreeMap<String, String>,
    /// Actions whose work the images above already hold (a toolchain's
    /// installer): a step that uses one is left out, and said to be.
    pub provided: BTreeSet<String>,
    /// Where the actions a step uses are fetched from, and which may be.
    /// The operator's, not the repository's: no settings file sets it.
    pub actions: ActionSource,
    /// Whether a job that may ask for an OIDC token (`id-token: write`) is
    /// given dagron's (`CI_GITHUB_OIDC=on`). The operator's, as `actions` is.
    pub oidc: bool,
    /// The audiences, beyond the forge's own, a job's OIDC token may be
    /// asked for: `github.oidc_audiences`.
    pub oidc_audiences: Vec<String>,
    /// Whether a job is given a `GITHUB_TOKEN` (`CI_GITHUB_TOKEN=on`). The
    /// operator's, as `oidc` is.
    pub github_token: bool,
    /// Whether each job of a trusted run has a Docker daemon beside it, as
    /// a hosted runner has one: `github.docker`, where the operator lists
    /// the repository ([`DockerRepos`]) and the engine offers one
    /// (`dagron_core::docker_daemon`). The repository's word alone does
    /// nothing: dispatch clears it for a repository the operator does not
    /// list.
    pub docker: bool,
    /// Where a job reaches GitHub's REST API, in place of GitHub's own
    /// address: dagron-ci's door (`CI_GITHUB_API_FOR_JOBS`), for a
    /// repository on github.com. `github.api_url`, and so `GITHUB_API_URL`.
    pub api_url: Option<String>,
    /// The GitLab CI front-end's: `gitlab:`.
    pub gitlab: super::gitlab::Settings,
}


/// The repositories whose jobs may have a Docker daemon of their own, which
/// is privileged on the engine's host unless the engine runs it on Sysbox
/// (`CI_GITHUB_DOCKER`): `owner/*` and
/// `owner/repo`, comma-separated, lowercase. Unset: none.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DockerRepos(Vec<String>);

impl DockerRepos {
    /// `CI_GITHUB_DOCKER`, empty where unset.
    pub fn parse(list: &str) -> Result<DockerRepos, String> {
        let named = |part: &str| {
            !part.is_empty()
                && part
                    .chars()
                    .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
        };
        let mut out = Vec::new();
        for entry in list.split(',').map(str::trim).filter(|e| !e.is_empty()) {
            let ok = entry
                .split_once('/')
                .is_some_and(|(owner, repo)| named(owner) && (repo == "*" || named(repo)));
            if !ok {
                return Err(format!(
                    "CI_GITHUB_DOCKER: {entry:?} is not owner/* or owner/repo"
                ));
            }
            out.push(entry.to_ascii_lowercase());
        }
        Ok(DockerRepos(out))
    }

    /// Whether a job of the repository `full_name` (`owner/repo`) may have
    /// one.
    pub fn allows(&self, full_name: &str) -> bool {
        let name = full_name.to_ascii_lowercase();
        let owner = name.split('/').next().unwrap_or_default();
        self.0
            .iter()
            .any(|entry| *entry == name || entry.strip_suffix("/*") == Some(owner))
    }

    /// What it lists, for the log.
    pub fn listed(&self) -> &[String] {
        &self.0
    }
}

/// What the variable a job is given a secret of its repository's in is
/// named: `CI_SECRET_<NAME>`, which the step runner reads `secrets.NAME`
/// from.
pub const SECRET_VARIABLE: &str = "CI_SECRET_";

/// What the variable a job is given each of its own and its workflow's
/// `env:` that reads a secret in is named: `CI_JOB_ENVX_<NAME>`, its text as
/// written, which the step runner reads as the job starts.
pub const LATE_JOB_VARIABLE: &str = "CI_JOB_ENVX_";

/// The repository's secret that turns on a job's `::debug::` lines, as
/// GitHub's does: given every job that is given any secret.
const STEP_DEBUG: &str = "ACTIONS_STEP_DEBUG";

/// The environment a repository's secrets are in, whose workflow files
/// read them as `secrets.NAME`: its address, `<host>/<owner>/<repo>` in
/// lower case, as dagron-core's `envpolicy::repository_environment` names
/// it (dagron-ci reads no crate with a database of its own; a test holds
/// the two to one name). The engine gives it to that repository's runs
/// alone.
pub fn repository_environment(server_url: &str, full_name: &str) -> String {
    let host = server_url
        .strip_prefix("https://")
        .or_else(|| server_url.strip_prefix("http://"))
        .unwrap_or(server_url)
        .trim_end_matches('/');
    format!("{host}/{full_name}").to_ascii_lowercase()
}

/// Whether `name` is a secret's, as GitHub has them: letters, digits and
/// `_`, not beginning with a digit or `GITHUB_`.
fn is_secret_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
        && !name.to_ascii_uppercase().starts_with("GITHUB_")
        && name.len() <= 128
}

/// The most audiences a job's OIDC token may be asked for, the forge's own
/// among them: as many as a task's identity names.
const MAX_AUDIENCES: usize = 8;

/// Where the actions of other repositories a workflow's steps use are
/// fetched from, and which may be: `CI_ACTIONS_MIRROR` and
/// `CI_ACTIONS_ALLOW`.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ActionSource {
    /// What an action's repository is under: `<mirror>/<owner>/<repo>.git`.
    /// Unset: github.com.
    pub mirror: Option<String>,
    /// `owner/*` and `owner/repo`, lowercase. Unset: any action, at any
    /// ref. Set: these alone, each pinned to a commit's full id.
    pub allow: Option<Vec<String>>,
    /// Where one file of an action's repository is read, at a ref, with
    /// `{owner}`, `{repo}`, `{ref}` and `{path}` in it (`CI_ACTIONS_MIRROR_RAW`):
    /// how dagron-ci reads an action's `action.yml` to learn the actions it
    /// uses in turn. Unset with no mirror: github.com's raw files.
    pub raw: Option<String>,
}

/// Where an action is fetched from where the operator names no mirror.
pub const DEFAULT_ACTIONS_MIRROR: &str = "https://github.com";

/// The most actions of other repositories one job may use: as many as a
/// task's workspace fetches.
pub(crate) const MAX_ACTIONS: usize = 32;

impl ActionSource {
    /// `CI_ACTIONS_MIRROR` and `CI_ACTIONS_ALLOW`, each empty where unset.
    pub fn parse(mirror: &str, allow: &str) -> Result<ActionSource, String> {
        let mirror = match mirror.trim().trim_end_matches('/') {
            "" => None,
            url => {
                let rest = url
                    .strip_prefix("https://")
                    .or_else(|| url.strip_prefix("http://"));
                let host = rest.map(|rest| rest.split('/').next().unwrap_or_default());
                let plain = rest.is_some_and(|rest| {
                    !rest.contains(['?', '#', '\\', '@'])
                        && !rest.contains("{{")
                        && !rest.split('/').any(|part| part == "..")
                });
                let host_ok = host.is_some_and(|host| {
                    !host.is_empty()
                        && host.chars().all(|c| {
                            c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | ':' | '[' | ']')
                        })
                });
                if !plain || !host_ok || url.chars().any(|c| c.is_whitespace() || c.is_control()) {
                    return Err(format!(
                        "CI_ACTIONS_MIRROR={url:?} is not where actions are fetched from: an \
                         https:// URL (http:// where nothing on the way needs hiding), with no \
                         user, query or fragment, that <owner>/<repo>.git follows"
                    ));
                }
                Some(url.to_string())
            }
        };
        let allow = match allow.trim() {
            "" => None,
            list => {
                let named = |part: &str| {
                    !part.is_empty()
                        && part
                            .chars()
                            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.'))
                };
                let mut out = Vec::new();
                for entry in list.split(',').map(str::trim).filter(|e| !e.is_empty()) {
                    let ok = entry
                        .split_once('/')
                        .is_some_and(|(owner, repo)| named(owner) && (repo == "*" || named(repo)));
                    if !ok {
                        return Err(format!(
                            "CI_ACTIONS_ALLOW: {entry:?} is not owner/* or owner/repo"
                        ));
                    }
                    out.push(entry.to_ascii_lowercase());
                }
                if out.is_empty() {
                    return Err(format!(
                        "CI_ACTIONS_ALLOW={list:?} names no action: owner/* or owner/repo, \
                         comma-separated"
                    ));
                }
                Some(out)
            }
        };
        Ok(ActionSource {
            mirror,
            allow,
            raw: None,
        })
    }

    /// These, with one file of an action's repository read at `template`
    /// (`CI_ACTIONS_MIRROR_RAW`), empty where unset: an https:// URL with
    /// `{owner}`, `{repo}`, `{ref}` and `{path}` in it.
    pub fn with_raw(mut self, template: &str) -> Result<ActionSource, String> {
        let template = template.trim();
        if template.is_empty() {
            return Ok(self);
        }
        let named = ["{owner}", "{repo}", "{ref}", "{path}"]
            .iter()
            .all(|part| template.contains(part));
        let rest = template
            .strip_prefix("https://")
            .or_else(|| template.strip_prefix("http://"));
        // The host is the operator's: no action's words choose it.
        let host_ok = rest
            .map(|rest| rest.split('/').next().unwrap_or_default())
            .is_some_and(|host| !host.is_empty() && !host.contains(['{', '}']));
        let plain = !template
            .chars()
            .any(|c| c.is_whitespace() || c.is_control())
            && !template.contains(['?', '#', '@', '\\']);
        if !named || !plain || !host_ok {
            return Err(format!(
                "CI_ACTIONS_MIRROR_RAW={template:?} is not where a file of an action's \
                 repository is read: an https:// URL (http:// where nothing on the way needs \
                 hiding), with no user, query or fragment, with {{owner}}, {{repo}}, {{ref}} \
                 and {{path}} in it after its host"
            ));
        }
        self.raw = Some(template.to_string());
        Ok(self)
    }

    /// Where the file `file` (`action.yml`, under the action's directory)
    /// of `action`'s repository is read at its ref: github.com's raw files
    /// where no mirror is named, the mirror's raw files where it says
    /// where they are, and nowhere else.
    pub fn file_url(&self, action: &expr::uses::Remote<'_>, file: &str) -> Option<String> {
        let path = match action.path {
            "" => file.to_string(),
            dir => format!("{dir}/{file}"),
        };
        match (&self.mirror, &self.raw) {
            (_, Some(template)) => Some(
                template
                    .replace("{owner}", action.owner)
                    .replace("{repo}", action.repo)
                    .replace("{ref}", action.git_ref)
                    .replace("{path}", &path),
            ),
            (None, None) => Some(format!(
                "https://raw.githubusercontent.com/{}/{}/{}/{path}",
                action.owner, action.repo, action.git_ref
            )),
            (Some(_), None) => None,
        }
    }

    /// The base an action's repository is under.
    pub fn mirror(&self) -> &str {
        self.mirror.as_deref().unwrap_or(DEFAULT_ACTIONS_MIRROR)
    }

    /// Why a step may not use `action` here, where it may not.
    pub fn refuses(&self, action: &expr::uses::Remote<'_>) -> Option<String> {
        let allow = self.allow.as_ref()?;
        let (owner, repo) = (
            action.owner.to_ascii_lowercase(),
            action.repo.to_ascii_lowercase(),
        );
        if !allow
            .iter()
            .any(|a| *a == format!("{owner}/*") || *a == format!("{owner}/{repo}"))
        {
            return Some(format!(
                "uses {}, which is not among the actions this dagron-ci allows \
                 (CI_ACTIONS_ALLOW)",
                action.repository()
            ));
        }
        if !action.is_pinned() {
            return Some(format!(
                "uses {}@{}: where the actions that may be used are listed \
                 (CI_ACTIONS_ALLOW), each is pinned to a commit's full id, which no one can move",
                action.repository(),
                action.git_ref
            ));
        }
        None
    }

    /// What a job's workspace fetches for `action`: its repository, the ref,
    /// and the directory, under the actions', the step runner reads it from.
    /// One of `owner`'s, from github.com and not a mirror, is fetched with a
    /// credential, where the token service gives one: `owner` is the
    /// repository's, for a trusted run ([`ActionSource::credentialed`]).
    pub(crate) fn fetch(&self, action: &expr::uses::Remote<'_>, owner: Option<&str>) -> Value {
        let mut fetch = json!({
            "repository": format!("{}/{}/{}.git", self.mirror(), action.owner, action.repo),
            "ref": action.git_ref,
            "path": action.fetched_dir(),
        });
        if self.credentialed(action, owner) {
            fetch["credential"] = json!(true);
        }
        fetch
    }

    /// Whether `action` is fetched, and read, with a credential: one of
    /// `owner`'s, where actions come from github.com itself. A mirror is
    /// sent none.
    pub(crate) fn credentialed(
        &self,
        action: &expr::uses::Remote<'_>,
        owner: Option<&str>,
    ) -> bool {
        self.mirror.is_none() && owner.is_some_and(|owner| owner.eq_ignore_ascii_case(action.owner))
    }
}

impl Settings {
    /// Whether the workflow file `name` (`ci.yml`) runs here.
    pub fn runs(&self, name: &str) -> bool {
        self.workflows.iter().any(|listed| listed == name)
    }
}

/// Read `.dagron/ci.yaml`. A file with no `github:` key is no settings.
pub fn settings(text: &str) -> Result<Settings, Vec<Problem>> {
    let value: Value = serde_yaml::from_str(text).map_err(|e| {
        vec![Problem {
            path: String::new(),
            message: format!("not YAML settings can be read from: {e}"),
        }]
    })?;
    let mut check = Check::default();
    let mut out = Settings::default();
    let Some(top) = value.as_object() else {
        if !value.is_null() {
            check.problem("", "is a mapping: `github:` and what is under it");
        }
        return check.done(out);
    };
    check.only("", top, &["github", "gitlab"]);
    if let Some(gitlab) = top.get("gitlab") {
        match super::gitlab::settings(gitlab) {
            Ok(gitlab) => out.gitlab = gitlab,
            Err(problems) => {
                for p in problems {
                    check.problem(&p.path, p.message);
                }
            }
        }
    }
    let github = match top.get("github") {
        None | Some(Value::Null) => return check.done(out),
        Some(Value::Object(github)) => github,
        Some(_) => {
            check.problem(
                "github",
                "is a mapping: workflows, runs_on, provided, oidc_audiences, docker",
            );
            return check.done(out);
        }
    };
    check.only(
        "github",
        github,
        &[
            "docker",
            "oidc_audiences",
            "provided",
            "runs_on",
            "workflows",
        ],
    );
    match github.get("docker") {
        None | Some(Value::Null) => {}
        Some(Value::Bool(docker)) => out.docker = *docker,
        Some(_) => check.problem(
            "github.docker",
            "is true, for a Docker daemon beside each job, or false",
        ),
    }
    for audience in check.strings("github.oidc_audiences", github.get("oidc_audiences")) {
        let plain = !audience.is_empty()
            && audience.len() <= 256
            && !audience.contains("{{")
            && !audience
                .chars()
                .any(|c| c.is_whitespace() || c.is_control());
        if !plain {
            check.problem(
                "github.oidc_audiences",
                format!(
                    "{audience:?} is not an audience: up to 256 characters, with no space in it"
                ),
            );
        } else if !out.oidc_audiences.contains(&audience) {
            out.oidc_audiences.push(audience);
        }
    }
    if out.oidc_audiences.len() >= MAX_AUDIENCES {
        check.problem(
            "github.oidc_audiences",
            format!(
                "names {} audiences; at most {}, the forge's own being one more",
                out.oidc_audiences.len(),
                MAX_AUDIENCES - 1
            ),
        );
    }
    for name in check.strings("github.workflows", github.get("workflows")) {
        let file = name.ends_with(".yml") || name.ends_with(".yaml");
        if !file || name.contains(['/', '\\']) || name.starts_with('.') {
            check.problem(
                "github.workflows",
                format!("{name:?} is not a file of {WORKFLOW_DIR}: name it as it is there, ci.yml"),
            );
        } else {
            out.workflows.push(name);
        }
    }
    match github.get("runs_on") {
        None | Some(Value::Null) => {}
        Some(Value::Object(map)) => {
            for (label, image) in map {
                match image
                    .as_str()
                    .filter(|image| super::is_image(image) && !image.contains("{{"))
                {
                    Some(image) => {
                        out.runs_on.insert(label.clone(), image.to_string());
                    }
                    None => check.problem(
                        &format!("github.runs_on.{label}"),
                        "is the image a job with that label runs in",
                    ),
                }
            }
        }
        Some(_) => check.problem("github.runs_on", "maps a runs-on label to an image"),
    }
    out.provided = check
        .strings("github.provided", github.get("provided"))
        .into_iter()
        .collect();
    check.done(out)
}

/// A workflow file, read for what does not depend on an event.
#[derive(Clone, Debug)]
pub struct Workflow {
    /// Its `name:`, as written. What it is called on a commit is its file's
    /// name, which is one word.
    pub name: Option<String>,
    /// The events that run it, of those an event is made for here.
    pub on: Triggers,
    /// Triggers it names that nothing here makes an event for (`schedule`,
    /// `release`, …): it never runs for them.
    pub unheard: Vec<String>,
    env: Vec<(String, Template)>,
    /// Its `env:` as written, for a variable read as the job runs
    /// ([`late_entries`]).
    env_text: BTreeMap<String, String>,
    defaults: Defaults,
    concurrency: Option<Template>,
    /// `concurrency.cancel-in-progress`: a newer run of the group takes the
    /// place of the one before it.
    cancel_in_progress: bool,
    /// Its `permissions:`, where it says ([`permissions`]).
    permissions: Option<Permissions>,
    jobs: Vec<Job>,
}

/// What a `permissions:` grants: whether a job may ask for an OIDC token,
/// and what its `GITHUB_TOKEN` may do, by a GitHub App's names.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct Permissions {
    id_token: bool,
    /// Each permission at `read` or `write`; one written `none`, or not
    /// written, is not there.
    token: BTreeMap<String, String>,
}

impl Permissions {
    /// What a job is given where nothing says: GitHub's restricted default,
    /// which a repository made today has.
    fn default_given() -> Permissions {
        Permissions {
            id_token: false,
            token: BTreeMap::from([("contents".to_string(), "read".to_string())]),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct Defaults {
    shell: Option<String>,
    working_directory: Option<String>,
}

#[derive(Clone, Debug)]
struct Job {
    id: String,
    runs_on: Vec<Template>,
    container: Option<Template>,
    container_env: Vec<(String, Template)>,
    services: Vec<Service>,
    needs: Vec<String>,
    condition: Option<Template>,
    /// The `if:` as it is written, for the step runner, which decides it
    /// again as the job starts, from what the jobs it needs came to.
    condition_text: Option<String>,
    /// `outputs:`, each a name and a text that may hold expressions.
    outputs: Vec<(String, String)>,
    /// How deep a checkout step of it asked the commit's history to go.
    checkout_depth: Option<u32>,
    /// The repository's secrets its steps read, by name.
    secrets: Vec<String>,
    matrix: Option<JobMatrix>,
    env: Vec<(String, Template)>,
    /// Its `env:` as written ([`late_entries`]).
    env_text: BTreeMap<String, String>,
    defaults: Defaults,
    timeout_minutes: Option<Template>,
    continue_on_error: Option<Template>,
    /// Its own `permissions:`, where it says: in place of the workflow's,
    /// as GitHub reads a job's.
    permissions: Option<Permissions>,
    steps: Vec<RawStep>,
}

#[derive(Clone, Debug)]
struct Service {
    name: String,
    image: Template,
    env: Vec<(String, Template)>,
    /// From `options: --health-cmd "…"`.
    ready: Option<String>,
}

// A matrix, its axes and each row's keys in the order they are written:
// that order is the order its jobs are made in, and named in. It is
// expanded by the expression crate, where the step runner can reach it too.
use expr::matrix::{Matrix, Row};

/// A job's matrix as its file writes it. Written out, it is expanded when
/// the run is made. Where an axis of it, or the whole of it, is an
/// expression of what an earlier job handed on, it cannot be: it is
/// expanded as the run goes, by the step runner, from this.
#[derive(Clone, Debug, Default)]
struct JobMatrix {
    /// The whole of it as one expression (`matrix: ${{ fromJSON(…) }}`).
    whole: Option<String>,
    /// Its axes, in the order written.
    axes: Vec<(String, Axis)>,
    include: Vec<Row>,
    exclude: Vec<Row>,
    /// `strategy.fail-fast`, which is read and stops nothing.
    fail_fast: bool,
}

/// An axis: its values, or the expression that will say them.
#[derive(Clone, Debug)]
enum Axis {
    Values(Vec<Value>),
    Computed(String),
}

impl JobMatrix {
    /// Whether an earlier job computes any of it.
    fn computed(&self) -> bool {
        self.whole.is_some()
            || self
                .axes
                .iter()
                .any(|(_, axis)| matches!(axis, Axis::Computed(_)))
    }

    /// The matrix, where all of it is written out.
    fn written(&self) -> Matrix {
        Matrix {
            axes: self
                .axes
                .iter()
                .filter_map(|(name, axis)| match axis {
                    Axis::Values(values) => Some((name.clone(), values.clone())),
                    Axis::Computed(_) => None,
                })
                .collect(),
            include: self.include.clone(),
            exclude: self.exclude.clone(),
        }
    }

    /// What the step runner plans the job's instances from
    /// (`CI_MATRIX_PLAN`): lists of pairs, since the order of a matrix is
    /// the order it is written in and JSON's objects have none.
    fn plan(&self) -> Value {
        if let Some(whole) = &self.whole {
            return json!({ "whole": whole, "fail_fast": self.fail_fast });
        }
        let pairs = |row: &Row| -> Value { row.iter().map(|(k, v)| json!([k, v])).collect() };
        json!({
            "axes": self
                .axes
                .iter()
                .map(|(name, axis)| match axis {
                    Axis::Values(values) => json!([name, values]),
                    Axis::Computed(source) => json!([name, { "expr": source }]),
                })
                .collect::<Vec<_>>(),
            "include": self.include.iter().map(pairs).collect::<Vec<_>>(),
            "exclude": self.exclude.iter().map(pairs).collect::<Vec<_>>(),
            "fail_fast": self.fail_fast,
        })
    }
}

/// The expression a text is, where it is one and nothing else
/// (`${{ fromJSON(needs.find.outputs.list) }}`): what an axis that is
/// computed is written as.
fn one_expression(text: &str) -> Option<String> {
    let inner = text.trim().strip_prefix("${{")?.strip_suffix("}}")?;
    let template = expr::template(text.trim()).ok()?;
    (template.exprs().count() == 1 && expr::parse(inner).is_ok()).then(|| inner.trim().to_string())
}

/// The keys of the mapping at `path` in a YAML document, in the order they
/// are written. Read as JSON, a mapping's keys are sorted.
fn written_order(document: &serde_yaml::Value, path: &[&str]) -> Vec<String> {
    let mut node = document;
    for key in path {
        match node.get(*key) {
            Some(next) => node = next,
            None => return Vec::new(),
        }
    }
    node.as_mapping()
        .into_iter()
        .flat_map(|m| m.keys())
        .filter_map(|k| k.as_str().map(str::to_string))
        .collect()
}

#[derive(Clone, Debug)]
struct RawStep {
    id: Option<String>,
    name: Option<String>,
    /// `None` for a step whose action is the workspace's own checkout, or
    /// one the repository says its images provide: nothing is run for it.
    /// `None` too for a step that uses an action that is run, which has
    /// `uses`.
    run: Option<String>,
    /// The action it uses, where one is run: `./` and a directory of the
    /// repository, or `owner/repo[/dir]@ref`.
    uses: Option<String>,
    /// The inputs it gives that action, each as written.
    with: Vec<(String, String)>,
    shell: Option<String>,
    working_directory: Option<String>,
    env: Vec<(String, String)>,
    condition: Option<String>,
    continue_on_error: bool,
    timeout_secs: Option<u64>,
}

/// Collects what is wrong with a file, each with where.
#[derive(Default)]
struct Check {
    problems: Vec<Problem>,
    /// The jobs the job being read names in its `needs:`, which are the
    /// ones an expression of it may read of `needs`.
    needed: Vec<String>,
    /// How deep the job being read asked its checkout to go
    /// (`fetch-depth`), where a checkout step of it said.
    fetch_depth: Option<u32>,
    /// The secrets the job being read's steps read (`secrets.NAME`), by
    /// their names in capitals, as GitHub keeps them; `GITHUB_TOKEN` aside.
    secrets: std::collections::BTreeSet<String>,
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
        for key in map.keys().filter(|k| !allowed.contains(&k.as_str())) {
            let at = if path.is_empty() {
                key.clone()
            } else {
                format!("{path}.{key}")
            };
            self.problem(
                &at,
                format!("is not a key this takes ({})", allowed.join(", ")),
            );
        }
    }

    /// One string or a list of them, as GitHub takes a list.
    fn strings(&mut self, path: &str, value: Option<&Value>) -> Vec<String> {
        match value {
            None | Some(Value::Null) => Vec::new(),
            Some(Value::String(one)) => vec![one.clone()],
            Some(Value::Array(items)) if items.iter().all(Value::is_string) => items
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect(),
            Some(_) => {
                self.problem(path, "is a string or a list of strings");
                Vec::new()
            }
        }
    }

    /// A feature of GitHub's that is not taken, by its name there.
    fn not_taken(&mut self, path: &str, what: &str) {
        self.problem(
            path,
            format!("{what} is not taken by dagron-ci's GitHub front-end yet (see {MATRIX})"),
        );
    }

    /// A text that may hold `${{ }}`, read; `allowed` names the contexts an
    /// expression may read here.
    fn template(&mut self, path: &str, text: &str, allowed: &[&str]) -> Option<Template> {
        match expr::template(text) {
            Ok(template) => {
                for e in template.exprs() {
                    self.contexts(path, e, allowed);
                }
                Some(template)
            }
            Err(e) => {
                self.problem(path, format!("is not an expression GitHub would take: {e}"));
                None
            }
        }
    }

    fn contexts(&mut self, path: &str, e: &expr::Expr, allowed: &[&str]) {
        for (context, property) in e.contexts() {
            let name = context.to_ascii_lowercase();
            if allowed.contains(&name.as_str()) {
                // A job is told of the jobs it names, and of no other.
                if let ("needs", Some(job)) = (name.as_str(), &property) {
                    if job != "*" && !self.needed.iter().any(|needed| needed == job) {
                        self.problem(
                            path,
                            format!(
                                "reads `needs.{job}`, and the job does not name {job} in its \
                                 `needs:`"
                            ),
                        );
                    }
                }
                continue;
            }
            match name.as_str() {
                // A step's own `GITHUB_TOKEN`, which the step runner has as
                // the job runs.
                "secrets"
                    if (allowed.contains(&"steps") || is_job_env(path))
                        && property
                            .as_deref()
                            .is_some_and(|p| p.eq_ignore_ascii_case("GITHUB_TOKEN")) => {}
                // A secret of the repository's, by its name: its
                // environment's ([`repository_environment`]), given the job
                // by the engine as it is dispatched. In a step, and in a
                // job's or the workflow's own `env:`, which the step runner
                // reads as the job starts ([`late_entries`]).
                "secrets"
                    if (allowed.contains(&"steps") && path.contains(".steps["))
                        || is_job_env(path) =>
                {
                    match property.as_deref() {
                        Some(name) if is_secret_name(name) => {
                            self.secrets.insert(name.to_ascii_uppercase());
                        }
                        Some(name) => self.problem(
                            path,
                            format!(
                                "reads `secrets.{name}`, which is no secret's name: letters, \
                                 digits and `_`, not beginning with a digit or GITHUB_"
                            ),
                        ),
                        None => self.problem(
                            path,
                            "reads `secrets` whole: a step reads a secret by its name, \
                             `secrets.NAME`",
                        ),
                    }
                }
                "secrets" => self.problem(
                    path,
                    "reads `secrets` where neither a step nor a job's `env:` does: a secret \
                     is read in a step's script, `env:` or `with:`, or in a job's or the \
                     workflow's `env:` (the rest is read when the run is made, before any \
                     secret is given)",
                ),
                "needs" => self.not_taken(
                    path,
                    "reading another job's result or outputs (`needs`) anywhere but a job's \
                     `if:`, its steps and its `outputs:`",
                ),
                "github" | "inputs" | "vars" | "matrix" | "strategy" | "env" | "runner" | "job"
                | "steps" => self.problem(
                    path,
                    format!(
                        "reads `{context}`, which GitHub does not give here ({})",
                        allowed.join(", ")
                    ),
                ),
                _ => self.problem(path, format!("reads `{context}`, which is no context")),
            }
        }
    }

    /// A scalar or a `${{ }}` text, as a template.
    fn scalar(&mut self, path: &str, value: &Value, allowed: &[&str]) -> Option<Template> {
        let text = match value {
            Value::String(s) => s.clone(),
            Value::Number(n) => n.to_string(),
            Value::Bool(b) => b.to_string(),
            _ => {
                self.problem(path, "is a value, or an expression for one");
                return None;
            }
        };
        self.template(path, &text, allowed)
    }

    /// An `env:` map, each value a text.
    fn env(
        &mut self,
        path: &str,
        value: Option<&Value>,
        allowed: &[&str],
    ) -> Vec<(String, Template)> {
        let mut out = Vec::new();
        let map = match value {
            None | Some(Value::Null) => return out,
            Some(Value::Object(map)) => map,
            Some(_) => {
                self.problem(path, "maps variable names to values");
                return out;
            }
        };
        for (name, value) in map {
            let at = format!("{path}.{name}");
            if !is_variable(name) {
                self.problem(&at, "is not a variable's name");
                continue;
            }
            if name == ACTIONS_CACHE_RESTORE_FROM {
                self.problem(&at, RESERVED_RESTORE_FROM);
                continue;
            }
            if [LATE_JOB_VARIABLE, SECRET_VARIABLE]
                .iter()
                .any(|prefix| name.starts_with(prefix))
            {
                self.problem(
                    &at,
                    "is dagron-ci's own: the variables a job is given its secrets, and its \
                     `env:` that reads them, in",
                );
                continue;
            }
            if let Some(template) = self.scalar(&at, value, allowed) {
                out.push((name.clone(), template));
            }
        }
        out
    }
}

/// Why a workflow may not name the variable dagron-ci writes a job's
/// `actions/cache` restore ref in: one it chose would read another
/// branch's entries.
pub(super) const RESERVED_RESTORE_FROM: &str =
    "is dagron-ci's own: the ref a job's actions/cache restores from after its own is the event's";

fn is_variable(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// A job's or a step's id as GitHub takes one, which is also a task's name.
fn is_id(id: &str) -> bool {
    let mut chars = id.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-')
        && id.len() <= 80
}

/// What a workflow's own `env` and `concurrency` may read.
const AT_WORKFLOW: &[&str] = &["github", "inputs", "vars"];
/// What a job's `if:` may read: that, and the jobs it needs.
const AT_JOB_IF: &[&str] = &["github", "inputs", "vars", "needs"];
/// What a job's own keys may read, its matrix among it.
const AT_JOB: &[&str] = &["github", "inputs", "vars", "matrix", "strategy", "env"];
/// What names an image: nothing an outsider chooses.
const AT_IMAGE: &[&str] = &["inputs", "vars", "matrix"];
/// What a step may read, and a job's outputs, which the step runner answers
/// as the job runs.
const AT_STEP: &[&str] = &[
    "github", "inputs", "vars", "matrix", "strategy", "env", "runner", "job", "steps", "needs",
];

/// The most outputs a job may hand on.
const MAX_OUTPUTS: usize = 100;

/// The most inputs a step may give an action, as dagron-ci-exec holds
/// them to.
const MAX_WITH: usize = 100;

/// The most tasks a job may need, matrices written out: each is two
/// variables the engine gives the job, of the 256 a task may be given.
const MAX_NEEDED: usize = 128;

/// Read a workflow file. Every problem found is returned, not just the
/// first.
pub fn parse(text: &str, provided: &BTreeSet<String>) -> Result<Workflow, Vec<Problem>> {
    let value: Value = serde_yaml::from_str(text).map_err(|e| {
        vec![Problem {
            path: String::new(),
            message: format!("not YAML a workflow can be read from: {e}"),
        }]
    })?;
    // The same text again, for the order its keys are written in.
    let written: serde_yaml::Value = serde_yaml::from_str(text).unwrap_or_default();
    let mut check = Check::default();
    let Some(top) = value.as_object() else {
        check.problem("", "a workflow is a mapping with on and jobs");
        return Err(check.problems);
    };
    check.only(
        "",
        top,
        &[
            "concurrency",
            "defaults",
            "env",
            "jobs",
            "name",
            "on",
            "permissions",
            "run-name",
        ],
    );
    let name = top.get("name").and_then(Value::as_str).map(str::to_string);
    let (on, unheard) = match top.get("on") {
        Some(on) => triggers(&mut check, on),
        None => {
            check.problem("on", "is required: which events run this workflow");
            (Triggers::default(), Vec::new())
        }
    };
    let env = check.env("env", top.get("env"), AT_WORKFLOW);
    let env_text = env_texts(top.get("env"));
    // What the workflow's `env:` reads of the repository's secrets is a
    // job's where the job keeps the variable ([`secret_names`]): not the
    // first job's.
    check.secrets.clear();
    let defaults = defaults(&mut check, "defaults", top.get("defaults"));
    let permissions = permissions(&mut check, "permissions", top.get("permissions"));
    let (concurrency, cancel_in_progress) =
        concurrency(&mut check, "concurrency", top.get("concurrency"));
    let mut jobs = Vec::new();
    match top.get("jobs") {
        Some(Value::Object(map)) if !map.is_empty() => {
            for id in written_order(&written, &["jobs"]) {
                let Some(value) = map.get(&id) else { continue };
                if let Some(job) = job(&mut check, &id, value, &written, provided) {
                    jobs.push(job);
                }
            }
        }
        _ => check.problem("jobs", "is required: a mapping of at least one job"),
    }
    let ids: BTreeSet<&str> = jobs.iter().map(|j| j.id.as_str()).collect();
    for job in &jobs {
        for need in job.needs.iter().filter(|n| !ids.contains(n.as_str())) {
            check.problem(
                &format!("jobs.{}.needs", job.id),
                format!("names no job of this workflow: {need:?}"),
            );
        }
    }
    check.done(Workflow {
        name,
        on,
        unheard,
        env,
        env_text,
        defaults,
        concurrency,
        cancel_in_progress,
        permissions,
        jobs,
    })
}

/// What `permissions:` grants ([`Permissions`]). `id-token: write` (or
/// `write-all`) lets a job ask for an OIDC token. Each other permission a
/// GitHub App has is what the job's `GITHUB_TOKEN` may do, written with `-`
/// for the App's `_` (`pull-requests`); one GitHub has and an App does not
/// (`packages`, `attestations`) gives the token nothing. `None` where
/// nothing is written.
fn permissions(check: &mut Check, path: &str, value: Option<&Value>) -> Option<Permissions> {
    let every = |level: &str| -> BTreeMap<String, String> {
        crate::checkout::FORGE_TOKEN_PERMISSIONS
            .iter()
            .filter(|name| **name != "metadata")
            .map(|name| (name.to_string(), level.to_string()))
            .collect()
    };
    match value {
        None | Some(Value::Null) => None,
        Some(Value::String(all)) => match all.as_str() {
            "write-all" => Some(Permissions {
                id_token: true,
                token: every("write"),
            }),
            "read-all" => Some(Permissions {
                id_token: false,
                token: every("read"),
            }),
            _ => {
                check.problem(path, "is read-all, write-all, or a mapping of permissions");
                None
            }
        },
        Some(Value::Object(map)) => {
            let mut given = Permissions::default();
            for (name, level) in map {
                let level = level.as_str().unwrap_or_default();
                if name == "id-token" {
                    match level {
                        "write" => given.id_token = true,
                        "none" => {}
                        _ => check.problem(&format!("{path}.id-token"), "is write or none"),
                    }
                    continue;
                }
                if !matches!(level, "read" | "write" | "none") {
                    check.problem(&format!("{path}.{name}"), "is read, write or none");
                    continue;
                }
                let app = name.replace('-', "_");
                // Of GitHub's, those an App's token has no word for give it
                // nothing; a name that is no permission is a mistake, and
                // would leave the job with less than its author meant.
                const GITHUB_ONLY: &[&str] =
                    &["attestations", "models", "packages", "repository_projects"];
                if !crate::checkout::FORGE_TOKEN_PERMISSIONS.contains(&app.as_str()) {
                    if !GITHUB_ONLY.contains(&app.as_str()) {
                        check.problem(
                            &format!("{path}.{name}"),
                            "is not a permission GitHub gives a job",
                        );
                    }
                    continue;
                }
                if level != "none" {
                    given.token.insert(app, level.to_string());
                }
            }
            Some(given)
        }
        Some(_) => {
            check.problem(path, "is read-all, write-all, or a mapping of permissions");
            None
        }
    }
}

fn triggers(check: &mut Check, on: &Value) -> (Triggers, Vec<String>) {
    // `on: push`, `on: [push, pull_request]`, or a mapping with filters.
    let map: Map<String, Value> = match on {
        Value::String(event) => Map::from_iter([(event.clone(), Value::Null)]),
        Value::Array(events) if events.iter().all(Value::is_string) => events
            .iter()
            .filter_map(Value::as_str)
            .map(|event| (event.to_string(), Value::Null))
            .collect(),
        Value::Object(map) => map.clone(),
        _ => {
            check.problem(
                "on",
                "is an event's name, a list of them, or a mapping of them",
            );
            return (Triggers::default(), Vec::new());
        }
    };
    let mut triggers = Triggers::default();
    let mut unheard = Vec::new();
    for (event, filters) in &map {
        let path = format!("on.{event}");
        if !HEARD.contains(&event.as_str()) {
            unheard.push(event.clone());
            continue;
        }
        // A list of crons, each `- cron: '…'`: read as GitHub reads them,
        // in UTC ([`crate::cron`]).
        if event == "schedule" {
            match filters {
                Value::Array(items) if !items.is_empty() => {
                    for (i, item) in items.iter().enumerate() {
                        let at = format!("{path}[{i}]");
                        match item.get("cron").and_then(Value::as_str) {
                            Some(text) => match crate::cron::Cron::parse(text) {
                                Ok(_) => triggers.schedule.push(text.trim().to_string()),
                                Err(e) => check.problem(&format!("{at}.cron"), e.0),
                            },
                            None => check.problem(&at, "is `cron: '…'`, five fields in UTC"),
                        }
                    }
                }
                _ => check.problem(&path, "is a list of `- cron: '…'`"),
            }
            continue;
        }
        let filters = match filters {
            Value::Null => Map::new(),
            Value::Object(m) => m.clone(),
            _ => {
                check.problem(&path, "is a mapping of filters, or empty");
                continue;
            }
        };
        let patterns = |check: &mut Check, name: &str| -> Option<Patterns> {
            let ignore = format!("{name}-ignore");
            let (key, compiled) = match (filters.get(name), filters.get(&ignore)) {
                (Some(_), Some(_)) => {
                    check.problem(
                        &format!("{path}.{ignore}"),
                        format!("GitHub takes {name} or {ignore}, not both"),
                    );
                    return None;
                }
                (Some(list), None) => {
                    let at = format!("{path}.{name}");
                    let strings = check.strings(&at, Some(list));
                    (at, Patterns::include(&strings))
                }
                (None, Some(list)) => {
                    let at = format!("{path}.{ignore}");
                    let strings = check.strings(&at, Some(list));
                    (at, Patterns::ignore(&strings))
                }
                (None, None) => return None,
            };
            compiled.map_err(|e| check.problem(&key, e)).ok()
        };
        match event.as_str() {
            "push" => {
                check.only(
                    &path,
                    &filters,
                    &[
                        "branches",
                        "branches-ignore",
                        "paths",
                        "paths-ignore",
                        "tags",
                        "tags-ignore",
                    ],
                );
                triggers.push = Some(PushFilter {
                    branches: patterns(check, "branches"),
                    tags: patterns(check, "tags"),
                    paths: patterns(check, "paths"),
                });
            }
            "pull_request" => {
                check.only(
                    &path,
                    &filters,
                    &[
                        "branches",
                        "branches-ignore",
                        "paths",
                        "paths-ignore",
                        "types",
                    ],
                );
                // An event is made here when a pull request is opened,
                // pushed to or reopened: GitHub's own default.
                let types = check.strings(&format!("{path}.types"), filters.get("types"));
                if let Some(other) = types
                    .iter()
                    .find(|t| !["opened", "synchronize", "reopened"].contains(&t.as_str()))
                {
                    check.not_taken(
                        &format!("{path}.types"),
                        &format!("the pull request activity `{other}`"),
                    );
                }
                triggers.pull_request = Some(PullRequestFilter {
                    branches: patterns(check, "branches"),
                    paths: patterns(check, "paths"),
                });
            }
            "merge_group" => {
                check.only(&path, &filters, &["branches", "branches-ignore", "types"]);
                triggers.merge_group = Some(MergeGroupFilter {
                    branches: patterns(check, "branches"),
                });
            }
            _ => {
                check.only(&path, &filters, &["inputs"]);
                triggers.dispatch = Some(Dispatch {
                    inputs: inputs(check, &path, filters.get("inputs")),
                });
            }
        }
    }
    (triggers, unheard)
}

/// `workflow_dispatch.inputs`. Every input is text here, whatever its
/// `type:`, as an input is when a script reads it.
fn inputs(check: &mut Check, path: &str, value: Option<&Value>) -> BTreeMap<String, Input> {
    let mut out = BTreeMap::new();
    let map = match value {
        None | Some(Value::Null) => return out,
        Some(Value::Object(map)) => map,
        Some(_) => {
            check.problem(
                &format!("{path}.inputs"),
                "maps input names to their settings",
            );
            return out;
        }
    };
    for (name, settings) in map {
        let at = format!("{path}.inputs.{name}");
        if !super::is_parameter(name) || name.starts_with("ci_") {
            check.problem(
                &at,
                "an input's name is a letter or _, then letters, digits or _, not starting ci_: \
                 it is a parameter of the run",
            );
            continue;
        }
        let settings = match settings {
            Value::Null => Map::new(),
            Value::Object(m) => m.clone(),
            _ => {
                check.problem(
                    &at,
                    "is a mapping (description, default, required, type, options)",
                );
                continue;
            }
        };
        check.only(
            &at,
            &settings,
            &["default", "description", "options", "required", "type"],
        );
        let text = |v: Option<&Value>| match v {
            Some(Value::String(s)) => Some(s.clone()),
            Some(Value::Number(n)) => Some(n.to_string()),
            Some(Value::Bool(b)) => Some(b.to_string()),
            _ => None,
        };
        out.insert(
            name.clone(),
            Input {
                description: text(settings.get("description")),
                default: text(settings.get("default")),
                required: settings.get("required") == Some(&Value::Bool(true)),
            },
        );
    }
    out
}

fn defaults(check: &mut Check, path: &str, value: Option<&Value>) -> Defaults {
    let run = match value {
        None | Some(Value::Null) => return Defaults::default(),
        Some(Value::Object(map)) => {
            check.only(path, map, &["run"]);
            map.get("run")
        }
        Some(_) => {
            check.problem(path, "is a mapping: run, with shell and working-directory");
            return Defaults::default();
        }
    };
    let Some(Value::Object(run)) = run else {
        return Defaults::default();
    };
    let at = format!("{path}.run");
    check.only(&at, run, &["shell", "working-directory"]);
    let shell = run.get("shell").and_then(Value::as_str).map(str::to_string);
    if let Some(shell) = &shell {
        shell_taken(check, &format!("{at}.shell"), shell);
    }
    Defaults {
        shell,
        working_directory: run
            .get("working-directory")
            .and_then(Value::as_str)
            .map(str::to_string),
    }
}

/// A shell a step may name: one of GitHub's by name, or a command with
/// `{0}` where the script's file goes.
fn shell_taken(check: &mut Check, path: &str, shell: &str) {
    let named = ["bash", "sh", "python", "pwsh"].contains(&shell);
    if shell.contains("${{") {
        check.problem(path, "is a shell's name, and not an expression for one");
    } else if !named && !shell.contains("{0}") {
        check.problem(
            path,
            format!(
                "{shell:?} is not a shell this takes: bash, sh, python, pwsh, or a command \
                 with {{0}} where the script's file goes"
            ),
        );
    }
}

/// `concurrency:` of a workflow: a group, as a text or under `group:`, and
/// whether a newer run of the group takes the place of the one before it
/// (`cancel-in-progress`).
fn concurrency(check: &mut Check, path: &str, value: Option<&Value>) -> (Option<Template>, bool) {
    let mut cancels = false;
    let group = match value {
        None | Some(Value::Null) => return (None, false),
        Some(Value::String(group)) => group.clone(),
        Some(Value::Object(map)) => {
            check.only(path, map, &["cancel-in-progress", "group"]);
            match map.get("cancel-in-progress") {
                None | Some(Value::Null) | Some(Value::Bool(false)) => {}
                Some(Value::Bool(true)) => cancels = true,
                // An expression is decided as the run goes there, and what
                // becomes of a run is said when it is made here.
                Some(_) => check.problem(
                    &format!("{path}.cancel-in-progress"),
                    "is true or false here: one that is an expression is not read",
                ),
            }
            match map.get("group") {
                Some(Value::String(group)) => group.clone(),
                _ => {
                    check.problem(&format!("{path}.group"), "is required: the group's name");
                    return (None, false);
                }
            }
        }
        Some(_) => {
            check.problem(path, "is a group's name, or a mapping with group");
            return (None, false);
        }
    };
    (check.template(path, &group, AT_WORKFLOW), cancels)
}

fn job(
    check: &mut Check,
    id: &str,
    value: &Value,
    written: &serde_yaml::Value,
    provided: &BTreeSet<String>,
) -> Option<Job> {
    let path = format!("jobs.{id}");
    if !is_id(id) {
        check.problem(
            &path,
            "a job's id is a letter or _, then letters, digits, _ or -",
        );
        return None;
    }
    let Some(map) = value.as_object() else {
        check.problem(&path, "a job is a mapping with runs-on and steps");
        return None;
    };
    for (key, what) in [
        ("uses", "a job that calls a reusable workflow (`uses:`)"),
        ("with", "a job that calls a reusable workflow (`with:`)"),
        ("secrets", "a job's `secrets:`"),
        ("environment", "a job's `environment:`"),
        ("concurrency", "a job's own `concurrency:`"),
    ] {
        if map.contains_key(key) {
            check.not_taken(&format!("{path}.{key}"), what);
        }
    }
    check.only(
        &path,
        map,
        &[
            "concurrency",
            "container",
            "continue-on-error",
            "defaults",
            "env",
            "environment",
            "if",
            "name",
            "needs",
            "outputs",
            "permissions",
            "runs-on",
            "secrets",
            "services",
            "steps",
            "strategy",
            "timeout-minutes",
            "uses",
            "with",
        ],
    );
    // The jobs it names, before anything of it that may read them.
    let needs = check.strings(&format!("{path}.needs"), map.get("needs"));
    check.needed = needs.clone();
    check.fetch_depth = None;
    check.secrets.clear();
    let runs_on: Vec<Template> = match map.get("runs-on") {
        Some(Value::Object(_)) => {
            check.not_taken(
                &format!("{path}.runs-on"),
                "a runner group (`runs-on.group`)",
            );
            Vec::new()
        }
        Some(Value::Bool(_) | Value::Number(_)) | None | Some(Value::Null) => Vec::new(),
        Some(other) => check
            .strings(&format!("{path}.runs-on"), Some(other))
            .iter()
            .filter_map(|label| check.template(&format!("{path}.runs-on"), label, AT_IMAGE))
            .collect(),
    };
    let (container, container_env) =
        container(check, &format!("{path}.container"), map.get("container"));
    if runs_on.is_empty() && container.is_none() && !map.contains_key("uses") {
        check.problem(
            &format!("{path}.runs-on"),
            "is required: the label of what the job runs on, or a container for it",
        );
    }
    let condition = match map.get("if") {
        None | Some(Value::Null) => None,
        Some(value) => {
            let text = match value {
                Value::String(s) => s.clone(),
                Value::Bool(b) => b.to_string(),
                other => other.to_string(),
            };
            match expr::condition(&text) {
                Ok(template) => {
                    for e in template.exprs() {
                        check.contexts(&format!("{path}.if"), e, AT_JOB_IF);
                    }
                    Some((template, text))
                }
                Err(e) => {
                    check.problem(
                        &format!("{path}.if"),
                        format!("is not an expression GitHub would take: {e}"),
                    );
                    None
                }
            }
        }
    };
    let steps = match map.get("steps") {
        Some(Value::Array(steps)) if !steps.is_empty() => steps
            .iter()
            .enumerate()
            .filter_map(|(i, step)| raw_step(check, &format!("{path}.steps[{i}]"), step, provided))
            .collect(),
        _ if map.contains_key("uses") => Vec::new(),
        _ => {
            check.problem(
                &format!("{path}.steps"),
                "is required: a list of at least one step",
            );
            Vec::new()
        }
    };
    let mut ids = BTreeSet::new();
    for step in &steps {
        if let Some(id) = &step.id {
            if !ids.insert(id.as_str()) {
                check.problem(
                    &format!("{path}.steps"),
                    format!("two steps have the id {id:?}"),
                );
            }
        }
    }
    let (condition, condition_text) = match condition {
        Some((template, text)) => (Some(template), Some(text)),
        None => (None, None),
    };
    // What it hands on to the jobs that need it: read as it ends, by the
    // step runner, which is what knows what its steps set.
    let outputs: Vec<(String, String)> = match map.get("outputs") {
        None | Some(Value::Null) => Vec::new(),
        Some(Value::Object(outputs)) => outputs
            .iter()
            .filter_map(|(name, value)| {
                let at = format!("{path}.outputs.{name}");
                if !is_id(name) {
                    check.problem(
                        &at,
                        "an output's name is a letter or _, then letters, digits, _ or -",
                    );
                    return None;
                }
                let text = match value {
                    Value::String(s) => s.clone(),
                    Value::Number(n) => n.to_string(),
                    Value::Bool(b) => b.to_string(),
                    _ => {
                        check.problem(&at, "is a value, or an expression for one");
                        return None;
                    }
                };
                check.template(&at, &text, AT_STEP)?;
                Some((name.clone(), text))
            })
            .collect(),
        Some(_) => {
            check.problem(
                &format!("{path}.outputs"),
                "maps the job's output names to their values",
            );
            Vec::new()
        }
    };
    if outputs.len() > MAX_OUTPUTS {
        check.problem(
            &format!("{path}.outputs"),
            format!("are {}, and a job may hand on {MAX_OUTPUTS}", outputs.len()),
        );
    }
    let scalar = |check: &mut Check, key: &str| {
        map.get(key)
            .filter(|v| !v.is_null())
            .and_then(|v| check.scalar(&format!("{path}.{key}"), v, AT_JOB))
    };
    // Before the secrets it reads are taken: its `env:` may read some.
    let env = check.env(&format!("{path}.env"), map.get("env"), AT_JOB);
    let job = Job {
        id: id.to_string(),
        runs_on,
        container,
        container_env,
        services: services(check, &format!("{path}.services"), map.get("services")),
        needs,
        condition,
        condition_text,
        outputs,
        checkout_depth: check.fetch_depth.take(),
        secrets: std::mem::take(&mut check.secrets).into_iter().collect(),
        matrix: strategy(
            check,
            &format!("{path}.strategy"),
            map.get("strategy"),
            written,
            id,
        ),
        env,
        env_text: env_texts(map.get("env")),
        defaults: defaults(check, &format!("{path}.defaults"), map.get("defaults")),
        timeout_minutes: scalar(check, "timeout-minutes"),
        continue_on_error: scalar(check, "continue-on-error"),
        permissions: permissions(
            check,
            &format!("{path}.permissions"),
            map.get("permissions"),
        ),
        steps,
    };
    check.needed.clear();
    Some(job)
}

/// `container:`: an image, or a mapping with one.
fn container(
    check: &mut Check,
    path: &str,
    value: Option<&Value>,
) -> (Option<Template>, Vec<(String, Template)>) {
    match value {
        None | Some(Value::Null) => (None, Vec::new()),
        Some(Value::String(image)) => (check.template(path, image, AT_IMAGE), Vec::new()),
        Some(Value::Object(map)) => {
            for (key, what) in [
                ("credentials", "a container's registry `credentials:`"),
                ("volumes", "a container's `volumes:`"),
                ("ports", "a container's `ports:`"),
                ("options", "a container's `options:`"),
            ] {
                if map.contains_key(key) {
                    check.not_taken(&format!("{path}.{key}"), what);
                }
            }
            check.only(
                path,
                map,
                &["credentials", "env", "image", "options", "ports", "volumes"],
            );
            let image = match map.get("image") {
                Some(Value::String(image)) => {
                    check.template(&format!("{path}.image"), image, AT_IMAGE)
                }
                _ => {
                    check.problem(&format!("{path}.image"), "is required: the job's image");
                    None
                }
            };
            (
                image,
                check.env(&format!("{path}.env"), map.get("env"), AT_JOB),
            )
        }
        Some(_) => {
            check.problem(path, "is an image, or a mapping with image");
            (None, Vec::new())
        }
    }
}

fn services(check: &mut Check, path: &str, value: Option<&Value>) -> Vec<Service> {
    let mut out = Vec::new();
    let map = match value {
        None | Some(Value::Null) => return out,
        Some(Value::Object(map)) => map,
        Some(_) => {
            check.problem(path, "maps a service's name to its container");
            return out;
        }
    };
    for (name, value) in map {
        let at = format!("{path}.{name}");
        if !super::is_service_name(name) {
            check.problem(
                &at,
                "a service's name is its host name here: lowercase letters, digits and '-', \
                 beginning with a letter",
            );
            continue;
        }
        let Some(service) = value.as_object() else {
            check.problem(&at, "is a mapping with image");
            continue;
        };
        for (key, what) in [
            ("credentials", "a service's registry `credentials:`"),
            ("volumes", "a service's `volumes:`"),
        ] {
            if service.contains_key(key) {
                check.not_taken(&format!("{at}.{key}"), what);
            }
        }
        check.only(
            &at,
            service,
            &["credentials", "env", "image", "options", "ports", "volumes"],
        );
        let Some(image) = service
            .get("image")
            .and_then(Value::as_str)
            .and_then(|image| check.template(&format!("{at}.image"), image, AT_IMAGE))
        else {
            check.problem(&format!("{at}.image"), "is required: the service's image");
            continue;
        };
        // `options:` are `docker create`'s. The health check is the one a
        // service is waited for by; the rest of them time it, which the
        // engine does its own way. Any other option is refused.
        let mut ready = None;
        if let Some(options) = service.get("options") {
            match options.as_str().map(health_command) {
                Some(Ok(command)) => ready = command,
                Some(Err(option)) => check.not_taken(
                    &format!("{at}.options"),
                    &format!("the container option `{option}`"),
                ),
                None => check.problem(&format!("{at}.options"), "is one line of options"),
            }
        }
        out.push(Service {
            name: name.clone(),
            image,
            env: check.env(&format!("{at}.env"), service.get("env"), AT_JOB),
            ready,
        });
    }
    out
}

/// The `--health-cmd` of a service's `options:`, where they are only a
/// health check's (`--health-interval` and its like are passed over: how
/// often a service is asked is the engine's own). `Err` names the first
/// option that is something else.
fn health_command(options: &str) -> Result<Option<String>, String> {
    let words = shell_words(options);
    let mut command = None;
    let mut i = 0;
    while i < words.len() {
        let word = &words[i];
        let (name, inline) = match word.split_once('=') {
            Some((name, value)) if name.starts_with("--") => (name, Some(value.to_string())),
            _ => (word.as_str(), None),
        };
        let health = [
            "--health-cmd",
            "--health-interval",
            "--health-timeout",
            "--health-retries",
            "--health-start-period",
        ];
        if !health.contains(&name) {
            return Err(name.to_string());
        }
        let value = match inline {
            Some(value) => value,
            None => {
                i += 1;
                words.get(i).cloned().unwrap_or_default()
            }
        };
        if name == "--health-cmd" && !value.trim().is_empty() {
            command = Some(value);
        }
        i += 1;
    }
    Ok(command)
}

/// A line split as a shell splits words: on spaces, with single and double
/// quotes holding a word together.
fn shell_words(line: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut word = String::new();
    let mut quote: Option<char> = None;
    let mut any = false;
    for c in line.chars() {
        match (quote, c) {
            (Some(q), c) if c == q => quote = None,
            (Some(_), c) => word.push(c),
            (None, '\'' | '"') => {
                quote = Some(c);
                any = true;
            }
            (None, c) if c.is_whitespace() => {
                if any || !word.is_empty() {
                    words.push(std::mem::take(&mut word));
                    any = false;
                }
            }
            (None, c) => word.push(c),
        }
    }
    if any || !word.is_empty() {
        words.push(word);
    }
    words
}

/// `strategy:`: a matrix written out, or one an earlier job computes: an
/// axis that is one expression, or the whole of it. An expression anywhere
/// else in it is not taken.
fn strategy(
    check: &mut Check,
    path: &str,
    value: Option<&Value>,
    written: &serde_yaml::Value,
    job: &str,
) -> Option<JobMatrix> {
    let map = match value {
        None | Some(Value::Null) => return None,
        Some(Value::Object(map)) => map,
        Some(_) => {
            check.problem(path, "is a mapping with matrix");
            return None;
        }
    };
    check.only(path, map, &["fail-fast", "matrix", "max-parallel"]);
    if map.contains_key("max-parallel") {
        check.not_taken(&format!("{path}.max-parallel"), "`max-parallel`");
    }
    let at = format!("{path}.matrix");
    let fail_fast = !matches!(map.get("fail-fast"), Some(Value::Bool(false)));
    let matrix = match map.get("matrix") {
        None | Some(Value::Null) => return None,
        Some(Value::Object(matrix)) => matrix,
        // The whole of it, as an earlier job handed it on.
        Some(Value::String(text)) => {
            let Some(whole) = one_expression(text) else {
                check.not_taken(
                    &at,
                    "a matrix that is more than one expression, or text with one",
                );
                return None;
            };
            check.template(&at, text, AT_JOB_IF)?;
            return Some(JobMatrix {
                whole: Some(whole),
                fail_fast,
                ..JobMatrix::default()
            });
        }
        Some(_) => {
            check.problem(&at, "maps each axis to its values");
            return None;
        }
    };
    let mut out = JobMatrix {
        fail_fast,
        ..JobMatrix::default()
    };
    let at_matrix = ["jobs", job, "strategy", "matrix"];
    // Each row of include or exclude, its keys as written.
    let rows = |check: &mut Check, key: &str| -> Vec<Row> {
        match matrix.get(key) {
            None | Some(Value::Null) => Vec::new(),
            Some(Value::Array(rows)) if rows.iter().all(Value::is_object) => {
                let mut node = written;
                for step in at_matrix.iter().chain([&key]) {
                    node = node.get(*step).unwrap_or(&serde_yaml::Value::Null);
                }
                rows.iter()
                    .filter_map(Value::as_object)
                    .enumerate()
                    .map(|(i, row)| {
                        let order: Vec<String> = node
                            .get(i)
                            .and_then(serde_yaml::Value::as_mapping)
                            .into_iter()
                            .flat_map(|m| m.keys())
                            .filter_map(|k| k.as_str().map(str::to_string))
                            .collect();
                        order
                            .iter()
                            .filter_map(|k| row.get(k).map(|v| (k.clone(), v.clone())))
                            .collect()
                    })
                    .collect()
            }
            Some(_) => {
                check.problem(&format!("{at}.{key}"), "is a list of mappings");
                Vec::new()
            }
        }
    };
    out.include = rows(check, "include");
    out.exclude = rows(check, "exclude");
    for axis in written_order(written, &at_matrix) {
        if axis == "include" || axis == "exclude" {
            continue;
        }
        let Some(values) = matrix.get(&axis) else {
            continue;
        };
        let axis = &axis;
        match values {
            Value::Array(values) if !values.is_empty() => {
                out.axes.push((axis.clone(), Axis::Values(values.clone())))
            }
            // Its values, as an earlier job handed them on.
            Value::String(text) => match one_expression(text) {
                Some(source) => {
                    if check
                        .template(&format!("{at}.{axis}"), text, AT_JOB_IF)
                        .is_some()
                    {
                        out.axes.push((axis.clone(), Axis::Computed(source)));
                    }
                }
                None => check.not_taken(
                    &format!("{at}.{axis}"),
                    "a matrix axis that is more than one expression, or text with one",
                ),
            },
            _ => check.problem(&format!("{at}.{axis}"), "is a list of at least one value"),
        }
    }
    // An axis may be one expression. One anywhere else in a matrix, among
    // an axis's values or in a row, is not taken.
    let has_expression = |value: &Value| {
        let mut texts = Vec::new();
        super::collect_strings(value, &mut texts);
        texts.iter().any(|t| t.contains("${{"))
    };
    if matrix
        .values()
        .any(|value| !value.is_string() && has_expression(value))
    {
        check.not_taken(&at, "an expression inside a matrix's values or rows");
    }
    Some(out)
}

fn raw_step(
    check: &mut Check,
    path: &str,
    value: &Value,
    provided: &BTreeSet<String>,
) -> Option<RawStep> {
    let Some(map) = value.as_object() else {
        check.problem(path, "a step is a mapping with run");
        return None;
    };
    check.only(
        path,
        map,
        &[
            "continue-on-error",
            "env",
            "id",
            "if",
            "name",
            "run",
            "shell",
            "timeout-minutes",
            "uses",
            "with",
            "working-directory",
        ],
    );
    let text = |key: &str| map.get(key).and_then(Value::as_str).map(str::to_string);
    let id = text("id");
    if id.as_deref().is_some_and(|id| !is_id(id)) {
        check.problem(
            &format!("{path}.id"),
            "is a letter or _, then letters, digits, _ or -",
        );
    }
    let mut used = None;
    let run = match (map.get("run"), map.get("uses")) {
        (Some(Value::String(run)), None) if !run.trim().is_empty() => Some(run.clone()),
        (None, Some(Value::String(action))) => {
            if action.starts_with("./") || uses(check, path, action, map.get("with"), provided) {
                used = used_action(check, path, action, map.get("with"));
                for key in ["shell", "working-directory"] {
                    if map.contains_key(key) {
                        check.problem(
                            &format!("{path}.{key}"),
                            "is a run step's, and this step uses an action",
                        );
                    }
                }
            }
            None
        }
        (Some(_), Some(_)) => {
            check.problem(path, "has run and uses: a step is one or the other");
            None
        }
        _ => {
            check.problem(&format!("{path}.run"), "is required: the step's script");
            return None;
        }
    };
    if run.is_some() && map.contains_key("with") {
        check.problem(
            &format!("{path}.with"),
            "is an action's, and this step runs a script",
        );
    }
    let shell = text("shell");
    if let Some(shell) = &shell {
        shell_taken(check, &format!("{path}.shell"), shell);
    }
    // Everything of a step is evaluated as the job runs: here it is read,
    // to find now what would not be an expression then.
    for key in ["if", "name", "run", "working-directory"] {
        if let Some(Value::String(text)) = map.get(key) {
            let read = if key == "if" {
                expr::condition(text)
            } else {
                expr::template(text)
            };
            match read {
                Ok(template) => {
                    for e in template.exprs() {
                        check.contexts(&format!("{path}.{key}"), e, AT_STEP);
                    }
                }
                Err(e) => check.problem(
                    &format!("{path}.{key}"),
                    format!("is not an expression GitHub would take: {e}"),
                ),
            }
        }
    }
    let condition = match map.get("if") {
        None | Some(Value::Null) => None,
        Some(Value::String(text)) => Some(text.clone()),
        Some(other) => Some(other.to_string()),
    };
    // Its variables, as written: a value with an expression in it is the
    // step runner's to evaluate.
    let env = check
        .env(&format!("{path}.env"), map.get("env"), AT_STEP)
        .into_iter()
        .filter_map(|(name, _)| {
            let value = map.get("env")?.get(&name)?;
            let text = match value {
                Value::String(s) => s.clone(),
                other => other.to_string(),
            };
            Some((name, text))
        })
        .collect();
    // A step's own limits are written out: one that is an expression is
    // decided as the job runs, which the step runner does not do for these.
    let continue_on_error = match map.get("continue-on-error") {
        None | Some(Value::Null) | Some(Value::Bool(false)) => false,
        Some(Value::Bool(true)) => true,
        Some(_) => {
            check.not_taken(
                &format!("{path}.continue-on-error"),
                "a step's `continue-on-error` that is an expression",
            );
            false
        }
    };
    let timeout_secs = match map.get("timeout-minutes") {
        None | Some(Value::Null) => None,
        Some(Value::Number(n)) if n.as_f64().is_some_and(|m| m > 0.0) => {
            Some((n.as_f64().unwrap_or(1.0) * 60.0).ceil() as u64)
        }
        Some(_) => {
            check.problem(
                &format!("{path}.timeout-minutes"),
                "is a number of minutes, more than none",
            );
            None
        }
    };
    let (uses, with) = used.unzip();
    Some(RawStep {
        id,
        name: text("name"),
        run,
        uses,
        with: with.unwrap_or_default(),
        shell,
        working_directory: text("working-directory"),
        env,
        condition,
        continue_on_error,
        timeout_secs,
    })
}

/// A step that uses an action that is run: a local one, `./` and a
/// directory of the repository, which the step runner reads from the
/// workspace as the step starts; or one of another repository, which the
/// job's workspace fetches before the job starts ([`ActionSource`]). Either
/// way the step runner runs a composite action's steps in its place; what
/// the action is, and whether it is one dagron-ci runs, is known only then.
/// The action, and the inputs the step gives it, each as written.
fn used_action(
    check: &mut Check,
    path: &str,
    action: &str,
    with: Option<&Value>,
) -> Option<(String, Vec<(String, String)>)> {
    if action.starts_with("./") && expr::uses::local(action).is_none() {
        check.problem(
            &format!("{path}.uses"),
            "is a local action: ./ and a directory of the repository, with no .. in it",
        );
        return None;
    }
    let mut inputs = Vec::new();
    match with {
        None | Some(Value::Null) => {}
        Some(Value::Object(map)) => {
            for (name, value) in map {
                let at = format!("{path}.with.{name}");
                let text = match value {
                    Value::String(text) => text.clone(),
                    Value::Null => String::new(),
                    Value::Number(_) | Value::Bool(_) => value.to_string(),
                    _ => {
                        check.problem(&at, "is an input's value: text, a number, true or false");
                        continue;
                    }
                };
                // Evaluated as the step starts: here it is read, to find now
                // what would not be an expression then.
                check.template(&at, &text, AT_STEP);
                inputs.push((name.clone(), text));
            }
            if inputs.len() > MAX_WITH {
                check.problem(
                    &format!("{path}.with"),
                    format!("gives the action more than {MAX_WITH} inputs"),
                );
            }
        }
        Some(_) => check.problem(
            &format!("{path}.with"),
            "is a mapping of the action's inputs",
        ),
    }
    Some((action.to_string(), inputs))
}

/// A step that uses an action of another repository: whether it is run.
/// Two are answered without running one, and the step is left out: the
/// checkout, which a job's workspace starts with, and an action the
/// repository says its images provide. A Docker action is level 2 still,
/// and refused. Any other is fetched and run ([`used_action`]).
fn uses(
    check: &mut Check,
    path: &str,
    action: &str,
    with: Option<&Value>,
    provided: &BTreeSet<String>,
) -> bool {
    let name = action.split('@').next().unwrap_or(action);
    if provided.contains(name) {
        return false;
    }
    // A step in an image of its own: the engine runs it beside the job, as
    // the step starts.
    if action.starts_with("docker://") {
        if expr::uses::docker(action).is_none() {
            check.problem(
                &format!("{path}.uses"),
                "names no image: docker:// and an image as a registry has it",
            );
            return false;
        }
        return true;
    }
    if !name.eq_ignore_ascii_case("actions/checkout") {
        if expr::uses::remote(action).is_none() {
            check.problem(
                &format!("{path}.uses"),
                format!(
                    "{action:?} is not an action GitHub would take: ./ and a directory, \
                     owner/repo@ref, or owner/repo/dir@ref"
                ),
            );
            return false;
        }
        return true;
    }
    // The workspace starts with the event's commit, detached, fetched with
    // a credential the steps never hold: alone, or with as much of its
    // history as the checkout asks for. A checkout that asks for more than
    // that is not that.
    let Some(with) = with.and_then(Value::as_object) else {
        return false;
    };
    for (key, value) in with {
        let same = match (key.as_str(), value) {
            ("persist-credentials", _) => true,
            ("fetch-depth", value) => {
                let depth = match value {
                    Value::Number(n) => n.as_u64().and_then(|n| u32::try_from(n).ok()),
                    Value::String(s) => s.trim().parse::<u32>().ok(),
                    _ => None,
                };
                // A job has one workspace: where two checkouts of it differ,
                // it comes with the more history, and 0 is all of it.
                if let Some(depth) = depth {
                    check.fetch_depth = Some(match check.fetch_depth {
                        Some(had) if had == 0 || depth == 0 => 0,
                        Some(had) => had.max(depth),
                        None => depth,
                    });
                }
                depth.is_some()
            }
            ("clean" | "show-progress" | "set-safe-directory", _) => true,
            _ => false,
        };
        if !same {
            check.not_taken(
                &format!("{path}.with.{key}"),
                &format!(
                    "a checkout with `{key}` (the workspace holds the event's commit, as deep \
                     as `fetch-depth` says, with no credential kept)"
                ),
            );
        }
    }
    false
}

// ---- for an event --------------------------------------------------------

/// A workflow as one event runs it.
#[derive(Clone, Debug, PartialEq)]
pub struct Lowered {
    /// The pipeline, for [`super::compile::compile`].
    pub pipeline: Pipeline,
    /// The jobs left out, each with why: its `if:` was false, or a job it
    /// needed was left out. GitHub shows such a job as skipped.
    pub skipped: Vec<(String, String)>,
}

/// The contexts known when a run is made.
struct Known {
    github: Ev,
    inputs: Ev,
    env: Ev,
    matrix: Option<Ev>,
    strategy: Option<Ev>,
    /// What the status functions answer, where they are asked: every needed
    /// job succeeded, or one failed.
    failed: Option<bool>,
    /// Whether the event is trusted: only a trusted run is given an OIDC
    /// token, as GitHub gives a fork's pull request none.
    trusted: bool,
    /// The owner whose actions a job fetches with a credential: the
    /// repository's, for a trusted run on github.com; none otherwise.
    owner: Option<String>,
}

impl Host for Known {
    fn context(&self, name: &str) -> Option<Ev> {
        match name.to_ascii_lowercase().as_str() {
            "github" => Some(self.github.clone()),
            "inputs" => Some(self.inputs.clone()),
            "env" => Some(self.env.clone()),
            "vars" => Some(Ev::Object(Vec::new())),
            "matrix" => self.matrix.clone(),
            "strategy" => self.strategy.clone(),
            _ => None,
        }
    }

    fn call(&self, name: &str, _args: &[Ev]) -> Option<Result<Ev, String>> {
        let failed = self.failed?;
        match name.to_ascii_lowercase().as_str() {
            "success" => Some(Ok(Ev::Bool(!failed))),
            "failure" => Some(Ok(Ev::Bool(failed))),
            "always" => Some(Ok(Ev::Bool(true))),
            "cancelled" => Some(Ok(Ev::Bool(false))),
            _ => None,
        }
    }
}

fn object(fields: Vec<(&str, Ev)>) -> Ev {
    Ev::Object(
        fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
    )
}

/// The `github` context, as far as an event tells it. What is not known
/// reads as `null`, or as empty where GitHub's is always text.
pub fn github_context(event: &Event, workflow: &str) -> Ev {
    let name = match event.kind.as_str() {
        "tag" => "push",
        "dispatch" => "workflow_dispatch",
        other => other,
    };
    let (kind, short) = match event.git_ref.strip_prefix("refs/tags/") {
        Some(tag) => ("tag", tag),
        None => (
            "branch",
            event
                .git_ref
                .strip_prefix("refs/heads/")
                .unwrap_or(&event.git_ref),
        ),
    };
    let base = event
        .base_ref
        .as_deref()
        .map(|b| b.strip_prefix("refs/heads/").unwrap_or(b))
        .unwrap_or_default();
    let owner = event.repo.split('/').next().unwrap_or_default();
    let actor = event.actor.as_deref().unwrap_or_default();
    let head_ref = match event.pr {
        Some(_) => event.head_ref.as_deref().unwrap_or_default(),
        None => "",
    };
    let mut payload = vec![(
        "repository",
        object(vec![("full_name", Ev::from(event.repo.as_str()))]),
    )];
    if !actor.is_empty() {
        payload.push(("sender", object(vec![("login", Ev::from(actor))])));
    }
    if let Some(pr) = event.pr {
        payload.push(("number", Ev::Number(pr as f64)));
        payload.push((
            "pull_request",
            object(vec![
                ("number", Ev::Number(pr as f64)),
                (
                    "base",
                    object(vec![
                        ("ref", Ev::from(base)),
                        ("sha", Ev::from(event.base_sha.clone().unwrap_or_default())),
                    ]),
                ),
                (
                    "head",
                    object(vec![
                        ("ref", Ev::from(head_ref)),
                        ("sha", Ev::from(event.sha.as_str())),
                    ]),
                ),
            ]),
        ));
    }
    // A push says where the branch was and where it is: what a job that
    // asks git what changed compares. A branch that is new was nowhere,
    // which GitHub writes as a commit id of zeros.
    if matches!(event.kind.as_str(), "push" | "tag") {
        let before = event
            .base_sha
            .clone()
            .unwrap_or_else(|| "0".repeat(event.sha.len().max(40)));
        payload.push(("before", Ev::from(before)));
        payload.push(("after", Ev::from(event.sha.as_str())));
    }
    if event.kind == "schedule" {
        let cron = event
            .inputs
            .get(SCHEDULE_INPUT)
            .cloned()
            .unwrap_or_default();
        payload.push(("schedule", Ev::from(cron)));
    }
    if event.kind == "dispatch" {
        payload.push((
            "inputs",
            Ev::Object(
                event
                    .inputs
                    .iter()
                    .map(|(k, v)| (k.clone(), Ev::from(v.as_str())))
                    .collect(),
            ),
        ));
    }
    // GitHub's own API is at a host of its own, and a server of GitHub's
    // (GitHub Enterprise) serves it under `/api/v3`. Gitea and Forgejo
    // serve theirs under `/api/v1`, and have no GraphQL.
    let host = &event.forge_host;
    let (api_url, graphql_url) = match event.forge.as_str() {
        "gitea" | "forgejo" => (format!("https://{host}/api/v1"), String::new()),
        _ if host.eq_ignore_ascii_case("github.com") => (
            "https://api.github.com".to_string(),
            "https://api.github.com/graphql".to_string(),
        ),
        _ => (
            format!("https://{host}/api/v3"),
            format!("https://{host}/api/graphql"),
        ),
    };
    // Text, as GitHub's are; empty where the run was given none.
    let number = |n: Option<i64>| Ev::from(n.map(|n| n.to_string()).unwrap_or_default());
    object(vec![
        ("event_name", Ev::from(name)),
        ("ref", Ev::from(event.git_ref.as_str())),
        ("ref_name", Ev::from(short)),
        ("ref_type", Ev::from(kind)),
        ("sha", Ev::from(event.sha.as_str())),
        ("repository", Ev::from(event.repo.as_str())),
        ("repository_owner", Ev::from(owner)),
        (
            "base_ref",
            Ev::from(if event.pr.is_some() { base } else { "" }),
        ),
        ("head_ref", Ev::from(head_ref)),
        ("actor", Ev::from(actor)),
        ("triggering_actor", Ev::from(actor)),
        ("workflow", Ev::from(workflow)),
        ("run_id", number(event.run_numbers.map(|(id, _)| id))),
        ("run_number", number(event.run_numbers.map(|(_, n)| n))),
        ("run_attempt", Ev::from("1")),
        (
            "server_url",
            Ev::from(format!("https://{}", event.forge_host)),
        ),
        ("api_url", Ev::from(api_url)),
        ("graphql_url", Ev::from(graphql_url)),
        ("event", event_payload(event, object(payload))),
    ])
}

/// `github.event`: the forge's own payload, where the webhook delivered
/// one and it was kept, with `ours` over it, a field of ours in place of
/// its own of the same name, at any depth (the commit and refs dagron-ci
/// runs, which it read from the same delivery); `ours` alone otherwise.
fn event_payload(event: &Event, ours: Ev) -> Ev {
    fn over(theirs: Ev, ours: Ev) -> Ev {
        match (theirs, ours) {
            (Ev::Object(mut theirs), Ev::Object(ours)) => {
                for (key, value) in ours {
                    match theirs.iter().position(|(k, _)| *k == key) {
                        Some(at) => {
                            let old = std::mem::replace(&mut theirs[at].1, Ev::Null);
                            theirs[at].1 = over(old, value);
                        }
                        None => theirs.push((key, value)),
                    }
                }
                Ev::Object(theirs)
            }
            (_, ours) => ours,
        }
    }
    match &event.payload {
        Some(payload @ serde_json::Value::Object(_)) if fits(payload) => {
            over(Ev::from(payload.clone()), ours)
        }
        _ => ours,
    }
}

/// The most of the forge's payload a job's context carries, as it is handed
/// on, each `{{` of it written out ([`inert`]): the context is one variable
/// of the job's, which a system holds to its limit of one (128 KiB on
/// Linux), with the rest of the context beside it.
pub const MAX_EVENT_PAYLOAD_BYTES: usize = 48 << 10;

/// Whether `payload` fits in a job's context: one that does not is left
/// out, and the run has dagron-ci's own fields of its event.
fn fits(payload: &serde_json::Value) -> bool {
    let text = payload.to_string();
    let written =
        text.len() + text.matches("{{").count() * (format!("{{{{ {BRACES} }}}}").len() - 2);
    written <= MAX_EVENT_PAYLOAD_BYTES
}

/// The parameter every run of a workflow file has, whose value is two
/// opening braces: what a `{{` of the workflow's own text is written as
/// ([`inert`]). No input can be called by it: an input's name does not
/// begin `ci_`.
pub const BRACES: &str = "ci_braces";

/// `text` as dagron hands it on unchanged: each `{{` of it written as the
/// parameter that holds one.
///
/// dagron reads a task's text once, from its start: at a `{{` it takes what
/// stands before the next `}}` for a name, fills it in if it is a
/// parameter's, and goes on after it without reading what it filled in. So
/// `{{ ci_braces }}` becomes `{{`, and whatever followed it is no longer
/// behind an opening one. The pairs are taken from the left, as dagron
/// meets them: of three braces the first two are a pair, and the third is a
/// brace.
pub fn inert(text: &str) -> String {
    Fills::Once.inert(text)
}

/// [`inert`], for the text of a task that is made as the run goes: an
/// instance of a job whose matrix an earlier job computes.
///
/// dagron fills such a task's text twice. Once with the run's parameters,
/// when the run is made, as any task's. And again as the instance is made,
/// with the instance's own `{{ item }}` and `{{ item.<key> }}`: that is how
/// it is told which instance it is. The second filling reads what the first
/// restored, so a `{{` written for one filling would be a name to the
/// second: a script's `{{ item }}` would become the instance.
///
/// So here a `{{` is written `{{ ci_braces }} item.b }}`. The first filling
/// makes `{{ ci_braces }}` two braces, and what follows them reads
/// `{{ item.b }}`. The second makes that two braces: `b` is what each
/// instance's item holds them under, which the step runner's plan gives it.
pub fn inert_twice(text: &str) -> String {
    Fills::Twice.inert(text)
}

/// How many times dagron fills a task's text before the task runs, which
/// is how a `{{` of a workflow's own is written for it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Fills {
    /// A task the run is made with.
    Once,
    /// A task that is made as the run goes ([`inert_twice`]).
    Twice,
}

impl Fills {
    fn inert(self, text: &str) -> String {
        let mut out = String::with_capacity(text.len());
        let mut rest = text;
        while let Some(at) = rest.find("{{") {
            out.push_str(&rest[..at]);
            out.push_str("{{ ");
            out.push_str(BRACES);
            out.push_str(" }}");
            if self == Fills::Twice {
                out.push_str(" item.b }}");
            }
            rest = &rest[at + 2..];
        }
        out.push_str(rest);
        out
    }

    /// [`Self::inert`], for every text in `value`.
    fn inert_value(self, value: &mut Value) {
        match value {
            Value::String(text) if text.contains("{{") => *text = self.inert(text),
            Value::Array(items) => items.iter_mut().for_each(|item| self.inert_value(item)),
            Value::Object(map) => map.values_mut().for_each(|item| self.inert_value(item)),
            _ => {}
        }
    }

    /// [`Self::inert`], for everything of a step that is text. Its name is
    /// settled first: the compiler names a step that has none by the start
    /// of its script, and would cut a `{{ ci_braces }}` in two.
    fn inert_step(self, step: &mut Step) {
        step.name = Some(self.inert(&super::compile::step_name(step)));
        step.run = self.inert(&step.run);
        for value in step.env.values_mut().chain(step.late_env.values_mut()) {
            *value = self.inert(value);
        }
        for (_, value) in &mut step.with {
            *value = self.inert(value);
        }
        for expression in &mut step.exprs {
            *expression = self.inert(expression);
        }
        let texts = [
            &mut step.shell,
            &mut step.working_directory,
            &mut step.condition,
            &mut step.uses,
        ];
        for text in texts.into_iter().flatten() {
            *text = self.inert(text);
        }
    }
}

/// A concurrency group as dagron's key for it. The key is a template of
/// dagron's own, and dagron takes none that still has a `{{` in it once it
/// is filled in: [`inert`] would hand the braces on, and the run would be
/// refused for them. A group is text, and a branch may have a `{{` in its
/// name (`ci-` and `github.ref` is how a group is commonly written), so
/// a workflow is not refused for one either. A key is only ever compared
/// with another, so its braces are written another way, which keeps two
/// groups from becoming one: `%7B` for a brace, and `%25` for a `%`.
fn group_key(group: &str) -> String {
    group.replace('%', "%25").replace('{', "%7B")
}

impl Workflow {
    /// The workflow as `event` runs it: its expressions that are known now
    /// evaluated, its matrices expanded, and the jobs whose `if:` is false
    /// left out. `stem` is the file's name without its ending, which the
    /// pipeline is called by; `settings` are the repository's.
    pub fn lower(
        &self,
        stem: &str,
        event: &Event,
        settings: &Settings,
    ) -> Result<Lowered, Vec<Problem>> {
        let mut check = Check::default();
        let workflow = self.name.clone().unwrap_or_else(|| stem.to_string());
        let event = &self.with_inputs(event);
        let mut github = github_context(event, &workflow);
        if let (Some(door), true) = (
            &settings.api_url,
            event
                .forge_host
                .eq_ignore_ascii_case(crate::github::DEFAULT_HOST),
        ) {
            if let Ev::Object(fields) = &mut github {
                if let Some((_, url)) = fields.iter_mut().find(|(k, _)| k == "api_url") {
                    *url = Ev::from(door.as_str());
                }
            }
        }
        let mut known = Known {
            github,
            inputs: Ev::Object(
                event
                    .inputs
                    .iter()
                    .map(|(k, v)| (k.clone(), Ev::from(v.as_str())))
                    .collect(),
            ),
            env: Ev::Object(Vec::new()),
            matrix: None,
            strategy: None,
            failed: None,
            trusted: event.trust == "trusted",
            owner: (event.trust == "trusted"
                && event
                    .forge_host
                    .eq_ignore_ascii_case(crate::github::DEFAULT_HOST))
            .then(|| event.repo.split('/').next().unwrap_or_default().to_string())
            .filter(|owner| !owner.is_empty()),
        };
        let workflow_env = eval_env(&mut check, "env", &self.env, &known);
        let workflow_late = late_entries(&self.env, &self.env_text);
        known.env = env_value(&workflow_env);

        let mut tasks: Vec<Task> = Vec::new();
        let mut skipped: Vec<(String, String)> = Vec::new();
        // The instances each job became, and what rule it runs by; none for
        // a job left out.
        let mut made: BTreeMap<&str, Vec<String>> = BTreeMap::new();
        for job in self.in_order(&mut check) {
            let path = format!("jobs.{}", job.id);
            // How the job stands by its `if:`: with every needed job having
            // succeeded, and with one having failed.
            let holds = |check: &mut Check, failed: bool| -> bool {
                let Some(condition) = &job.condition else {
                    return !failed;
                };
                let host = Known {
                    github: known.github.clone(),
                    inputs: known.inputs.clone(),
                    env: Ev::Object(Vec::new()),
                    matrix: None,
                    strategy: None,
                    failed: Some(failed),
                    trusted: known.trusted,
                    owner: known.owner.clone(),
                };
                let asks = condition.exprs().any(expr::Expr::asks_status);
                match condition.eval(&host) {
                    // One that asks no status function holds only where
                    // nothing before it failed.
                    Ok(value) => value.truthy() && (asks || !failed),
                    Err(e) => {
                        check.problem(&format!("{path}.if"), e.to_string());
                        false
                    }
                }
            };
            // An `if:` that reads `needs` cannot be decided here: no job has
            // run. Such a job is started whatever became of the jobs it
            // needs, and the step runner decides it as it starts. (It
            // decides every job again then, by the same `if:` and what the
            // jobs it needs came to: a job that needs one the runner
            // skipped is skipped with it, as on GitHub.)
            let reads_needs = job.condition.as_ref().is_some_and(|condition| {
                condition.exprs().any(|e| {
                    e.contexts()
                        .iter()
                        .any(|(context, _)| context.eq_ignore_ascii_case("needs"))
                })
            });
            let (clean, after_failure) = if reads_needs {
                (true, true)
            } else {
                (holds(&mut check, false), holds(&mut check, true))
            };
            let needed: Vec<&String> = job.needs.iter().collect();
            let left_out: Vec<&str> = needed
                .iter()
                .filter(|n| !made.contains_key(n.as_str()))
                .map(|n| n.as_str())
                .collect();
            let depends_on: Vec<String> = needed
                .iter()
                .filter_map(|n| made.get(n.as_str()))
                .flatten()
                .cloned()
                .collect();
            let rule = match (clean, after_failure) {
                (false, false) => {
                    skipped.push((job.id.clone(), "its if: is false".to_string()));
                    continue;
                }
                // It runs only where every job it needs ran and succeeded.
                (true, false) if !left_out.is_empty() => {
                    skipped.push((
                        job.id.clone(),
                        format!("it needs {}, which was left out", left_out.join(", ")),
                    ));
                    continue;
                }
                (true, false) => None,
                (true, true) => Some("all_done"),
                // It runs where a job it needs failed, and none of them is
                // there to fail.
                (false, true) if depends_on.is_empty() => {
                    skipped.push((
                        job.id.clone(),
                        "it runs after a failure, and no job it needs is run".to_string(),
                    ));
                    continue;
                }
                (false, true) => Some("one_failed"),
            };
            // The tasks it needs, each with the job it is: what the step
            // runner is told of, and a job left out is told as skipped.
            let needs: Vec<(String, Option<String>)> = job
                .needs
                .iter()
                .flat_map(|id| match made.get(id.as_str()) {
                    Some(tasks) if !tasks.is_empty() => tasks
                        .iter()
                        .map(|task| (id.clone(), Some(task.clone())))
                        .collect(),
                    _ => vec![(id.clone(), None)],
                })
                .collect();
            if needs.len() > MAX_NEEDED {
                check.problem(
                    &format!("{path}.needs"),
                    format!(
                        "are {} jobs, matrices written out, and a job may need {MAX_NEEDED}",
                        needs.len()
                    ),
                );
            }
            // A matrix an earlier job computes is not known here. The job is
            // two tasks: one that plans its instances from what the jobs it
            // needs came to, and the job, which the engine makes a task of
            // for each instance as the run goes.
            if let Some(matrix) = job.matrix.as_ref().filter(|m| m.computed()) {
                if !job.outputs.is_empty() {
                    check.problem(
                        &format!("{path}.outputs"),
                        "are a job's whose matrix an earlier job computes: its instances are \
                         made as the run goes, and what one of them hands on is not taken yet",
                    );
                }
                let planner = format!("{}.strategy.matrix", job.id);
                let mut waits = vec![planner.clone()];
                waits.extend(depends_on.iter().cloned());
                known.matrix = None;
                known.strategy = None;
                known.env = env_value(&workflow_env);
                let made_task = self.task(
                    &mut check,
                    job,
                    &job.id,
                    &workflow_env,
                    &workflow_late,
                    &known,
                    settings,
                    waits,
                    Some("all_done"),
                    &needs,
                    Some(&planner),
                );
                if let Some(task) = made_task {
                    tasks.push(self.planner(
                        job,
                        matrix,
                        &planner,
                        &known,
                        &task,
                        depends_on.clone(),
                        rule,
                        &needs,
                    ));
                    tasks.push(task);
                    made.insert(job.id.as_str(), vec![job.id.clone()]);
                } else {
                    made.insert(job.id.as_str(), Vec::new());
                }
                continue;
            }
            let mut names = Vec::new();
            for (label, matrix, index, total) in instances(&mut check, &path, job) {
                known.matrix = Some(matrix.clone());
                known.strategy = Some(object(vec![
                    ("fail-fast", Ev::Bool(true)),
                    ("job-index", Ev::Number(index as f64)),
                    ("job-total", Ev::Number(total as f64)),
                ]));
                known.env = env_value(&workflow_env);
                let name = match &label {
                    Some(label) => format!("{}.{label}", job.id),
                    None => job.id.clone(),
                };
                if let Some(task) = self.task(
                    &mut check,
                    job,
                    &name,
                    &workflow_env,
                    &workflow_late,
                    &known,
                    settings,
                    depends_on.clone(),
                    rule,
                    &needs,
                    None,
                ) {
                    names.push(name);
                    tasks.push(task);
                }
            }
            known.matrix = None;
            known.strategy = None;
            known.env = env_value(&workflow_env);
            made.insert(job.id.as_str(), names);
        }
        if tasks.len() > MAX_JOBS {
            check.problem(
                "jobs",
                format!(
                    "become {} jobs, matrices expanded; a workflow may have {MAX_JOBS}",
                    tasks.len()
                ),
            );
        }
        let concurrency = self
            .concurrency
            .as_ref()
            .and_then(|group| match group.text(&known) {
                Ok(key) if key.trim().is_empty() => None,
                Ok(key) => Some(Concurrency {
                    key: group_key(&key),
                    cancel_superseded: self.cancel_in_progress,
                }),
                Err(e) => {
                    check.problem("concurrency", e.to_string());
                    None
                }
            });
        // What a `{{` of the workflow's text is written as ([`inert`]).
        let mut spec = Map::new();
        spec.insert("parameters".into(), json!({ BRACES: "{{" }));
        let pipeline = Pipeline {
            name: stem.to_string(),
            on: self.on.clone(),
            concurrency,
            tasks,
            spec,
        };
        check.done(Lowered { pipeline, skipped })
    }

    /// `event`, with a dispatch's inputs as the workflow reads them: each
    /// one it declares, by the caller's value, else its default, else empty,
    /// as GitHub hands a workflow an input nobody set. The run's parameters
    /// are filled so by the compiler; without this an input left to its
    /// default had it as a parameter and read as empty in `inputs` and
    /// `github.event.inputs`, and a job whose `if:` asked for the default
    /// was left out.
    fn with_inputs(&self, event: &Event) -> Event {
        let mut event = event.clone();
        if event.kind == "dispatch" {
            for (name, input) in self.on.dispatch.iter().flat_map(|d| &d.inputs) {
                event
                    .inputs
                    .entry(name.clone())
                    .or_insert_with(|| input.default.clone().unwrap_or_default());
            }
        }
        event
    }

    /// The jobs, each after the jobs it needs. A cycle is a problem, and
    /// its jobs are left where they are.
    fn in_order(&self, check: &mut Check) -> Vec<&Job> {
        let mut ordered: Vec<&Job> = Vec::new();
        let mut placed: BTreeSet<&str> = BTreeSet::new();
        while ordered.len() < self.jobs.len() {
            let before = ordered.len();
            for job in &self.jobs {
                if !placed.contains(job.id.as_str())
                    && job.needs.iter().all(|n| {
                        placed.contains(n.as_str()) || !self.jobs.iter().any(|j| &j.id == n)
                    })
                {
                    placed.insert(&job.id);
                    ordered.push(job);
                }
            }
            if ordered.len() == before {
                let stuck: Vec<&str> = self
                    .jobs
                    .iter()
                    .filter(|j| !placed.contains(j.id.as_str()))
                    .map(|j| j.id.as_str())
                    .collect();
                check.problem(
                    "jobs",
                    format!("needs forms a cycle through {}", stuck.join(", ")),
                );
                break;
            }
        }
        ordered
    }

    #[allow(clippy::too_many_arguments)]
    fn task(
        &self,
        check: &mut Check,
        job: &Job,
        name: &str,
        workflow_env: &[(String, String)],
        workflow_late: &[(String, String)],
        known: &Known,
        settings: &Settings,
        depends_on: Vec<String>,
        rule: Option<&str>,
        needs: &[(String, Option<String>)],
        // The task that plans its instances, where it is made as the run
        // goes: a job whose matrix an earlier job computes.
        planned_by: Option<&str>,
    ) -> Option<Task> {
        let fills = match planned_by {
            Some(_) => Fills::Twice,
            None => Fills::Once,
        };
        let path = format!("jobs.{}", job.id);
        // Its variables: the workflow's, then the job's, which may read the
        // workflow's and its matrix.
        let mut env: Vec<(String, String)> = workflow_env.to_vec();
        // And those that read the repository's secrets, which the step
        // runner reads as the job starts: never in the task as values. A
        // name is the later of its two levels', whichever kind each is.
        let mut late: Vec<(String, String)> = workflow_late.to_vec();
        for (key, value) in eval_env(check, &format!("{path}.env"), &job.env, known)
            .into_iter()
            .chain(eval_env(
                check,
                &format!("{path}.container.env"),
                &job.container_env,
                known,
            ))
        {
            env.retain(|(k, _)| k != &key);
            late.retain(|(k, _)| k != &key);
            env.push((key, value));
        }
        for (key, text) in late_entries(&job.env, &job.env_text) {
            env.retain(|(k, _)| k != &key);
            late.retain(|(k, _)| k != &key);
            late.push((key, text));
        }
        let text = |check: &mut Check, at: &str, template: &Template| match template.text(known) {
            Ok(text) => Some(text),
            Err(e) => {
                check.problem(at, e.to_string());
                None
            }
        };
        // Its image: its container's, or what its label means here.
        let image = match &job.container {
            Some(container) => text(check, &format!("{path}.container"), container)?,
            None => {
                let labels: Vec<String> = job
                    .runs_on
                    .iter()
                    .filter_map(|label| text(check, &format!("{path}.runs-on"), label))
                    .collect();
                match labels.iter().find_map(|label| settings.runs_on.get(label)) {
                    Some(image) => image.clone(),
                    None => {
                        check.problem(
                            &format!("{path}.runs-on"),
                            format!(
                                "{} is no label {SETTINGS_FILE} gives an image for \
                                 (github.runs_on), and the job names no container",
                                labels.join(", ")
                            ),
                        );
                        return None;
                    }
                }
            }
        };
        if !super::is_image(&image) || image.contains("{{") {
            check.problem(&path, format!("{image:?} is not an image's name"));
            return None;
        }
        let mut fields = Map::new();
        fields.insert("docker_image".into(), json!(image));
        if let Some(rule) = rule {
            fields.insert("trigger_rule".into(), json!(rule));
        }
        let evaluated = |check: &mut Check, key: &str, template: &Option<Template>| {
            template.as_ref().and_then(|t| match t.eval(known) {
                Ok(value) => Some(value),
                Err(e) => {
                    check.problem(&format!("{path}.{key}"), e.to_string());
                    None
                }
            })
        };
        if let Some(minutes) = evaluated(check, "timeout-minutes", &job.timeout_minutes) {
            match minutes {
                Ev::Number(m) if m > 0.0 => {
                    fields.insert("timeout_secs".into(), json!((m * 60.0).ceil() as u64));
                }
                Ev::String(s) if s.trim().parse::<f64>().is_ok_and(|m| m > 0.0) => {
                    let m: f64 = s.trim().parse().unwrap_or(1.0);
                    fields.insert("timeout_secs".into(), json!((m * 60.0).ceil() as u64));
                }
                _ => check.problem(
                    &format!("{path}.timeout-minutes"),
                    "is a number of minutes, more than none",
                ),
            }
        }
        if evaluated(check, "continue-on-error", &job.continue_on_error)
            .is_some_and(|value| value.truthy() && value != Ev::from("false"))
        {
            fields.insert("allow_failure".into(), json!(true));
        }
        if !job.services.is_empty() {
            let mut services = Vec::new();
            for service in &job.services {
                let at = format!("{path}.services.{}", service.name);
                let Some(image) = text(check, &format!("{at}.image"), &service.image) else {
                    continue;
                };
                let mut entry = Map::new();
                entry.insert("name".into(), json!(service.name));
                entry.insert("image".into(), json!(image));
                let vars = eval_env(check, &format!("{at}.env"), &service.env, known);
                if !vars.is_empty() {
                    entry.insert(
                        "env".into(),
                        vars.iter()
                            .map(|(k, v)| json!({ "name": k, "value": v }))
                            .collect(),
                    );
                }
                if let Some(ready) = &service.ready {
                    entry.insert("ready".into(), json!(ready));
                }
                services.push(Value::Object(entry));
            }
            fields.insert("services".into(), Value::Array(services));
        }
        // What the step runner evaluates a step's expressions against: what
        // is known now, with the job's variables as its `env`.
        let context = json!({
            "github": serde_json::Value::from(&with_job(&known.github, &job.id)),
            "inputs": serde_json::Value::from(&known.inputs),
            "matrix": known.matrix.as_ref().map(serde_json::Value::from),
            "strategy": known.strategy.as_ref().map(serde_json::Value::from),
            "env": env.iter().map(|(k, v)| (k.clone(), json!(v))).collect::<Map<_, _>>(),
        });
        let mut list: Vec<Value> = env
            .iter()
            .map(|(k, v)| json!({ "name": k, "value": v }))
            .collect();
        list.push(json!({ "name": "CI_CONTEXT", "value": context.to_string() }));
        for (key, text) in &late {
            list.push(json!({ "name": format!("{LATE_JOB_VARIABLE}{key}"), "value": text }));
        }
        // The repository's secrets its steps and its `env:` read, on a
        // trusted run: each a reference into the repository's environment,
        // which the engine reads as the job is dispatched, and which is
        // empty where the environment holds none of the name. A run that is
        // not trusted is given none, as GitHub gives a fork's: its
        // `secrets.NAME` is empty. A job given any is given its
        // `ACTIONS_STEP_DEBUG` too, where the repository has one: what
        // turns on a step's `::debug::` lines.
        // The workflow's `env:` reads its secrets for a job only where the
        // job keeps the variable that reads them: one the job's own `env:`
        // names in its place reads none.
        let from_workflow: Vec<String> = late
            .iter()
            .filter(|(key, _)| !job.env.iter().any(|(own, _)| own == key))
            .filter_map(|(key, _)| self.env.iter().find(|(name, _)| name == key))
            .flat_map(|(_, template)| secret_names(template))
            .collect();
        let mut secrets: std::collections::BTreeSet<&str> = job
            .secrets
            .iter()
            .chain(&from_workflow)
            .map(String::as_str)
            .collect();
        if known.trusted && !secrets.is_empty() {
            secrets.insert(STEP_DEBUG);
            for name in &secrets {
                list.push(json!({
                    "name": format!("{SECRET_VARIABLE}{name}"),
                    "value_from": { "secret": name, "optional": true },
                }));
            }
            fields.insert(
                "environment".into(),
                json!(repository_environment(
                    &known.github.get("server_url").text(),
                    &known.github.get("repository").text()
                )),
            );
        }
        fields.insert("env".into(), Value::Array(list));
        // A job that may ask for an OIDC token (`id-token: write`), of a
        // trusted run, where the operator gives workflows one: the identity
        // the engine gives a task, for the forge's own audience first (the
        // one GitHub's token is for where none is asked) and then those the
        // repository names. An untrusted run's job is given none, as GitHub
        // gives a fork's none: it runs, and a step that asks is refused.
        let granted = job
            .permissions
            .clone()
            .or_else(|| self.permissions.clone())
            .unwrap_or_else(Permissions::default_given);
        if granted.id_token && settings.oidc && known.trusted {
            let own = format!(
                "{}/{}",
                known.github.get("server_url").text(),
                known.github.get("repository_owner").text()
            );
            let mut audience = vec![own.clone()];
            audience.extend(
                settings
                    .oidc_audiences
                    .iter()
                    .filter(|a| **a != own)
                    .take(MAX_AUDIENCES - 1)
                    .cloned(),
            );
            fields.insert(
                "identity".into(),
                json!({ "oidc": { "audience": audience } }),
            );
        }
        // Its `GITHUB_TOKEN`, where the operator gives workflows one: a
        // token the engine has minted for each attempt, for the repository
        // and with what its permissions grant. dagron-ci's token service
        // gives a run that is not trusted read alone. A job granted nothing
        // is given none.
        if settings.github_token && !granted.token.is_empty() {
            fields.insert(
                "forge_token".into(),
                json!({ "permissions": granted.token }),
            );
        }
        // A Docker daemon beside it, where the repository asks for one and
        // the operator lists it, on a trusted run: the daemon's container is
        // privileged, and a run that is not trusted is refused it. A job
        // that runs a daemon of its own, a service named `docker`, keeps
        // that one; one with as many services as a task may have has no
        // room for it, and says so here rather than at submit.
        if settings.docker && known.trusted {
            let services = fields
                .get("services")
                .and_then(Value::as_array)
                .map(Vec::as_slice)
                .unwrap_or_default();
            let own = services
                .iter()
                .any(|s| s.get("name").and_then(Value::as_str) == Some(super::DOCKER_SERVICE));
            if services.len() >= super::MAX_SERVICES && !own {
                check.problem(
                    &format!("{path}.services"),
                    format!(
                        "has {} services, and the repository gives each job a Docker daemon \
                         (github.docker), which is one more: a job may have {}",
                        services.len(),
                        super::MAX_SERVICES
                    ),
                );
            } else if !own {
                fields.insert("docker_daemon".into(), json!(true));
            }
        }
        // A `{{` in any of it is text, and reaches the job as it is written.
        fields
            .values_mut()
            .for_each(|value| fills.inert_value(value));
        // A task of each instance the planner prints, named by its label
        // and told which it is: these three are dagron's own templates, of
        // the instance, and are written after the workflow's text is not.
        if let Some(planner) = planned_by {
            fields.insert("with_output_of".into(), json!(planner));
            fields.insert("instance_key".into(), json!("{{ item.label }}"));
            if let Some(Value::Array(env)) = fields.get_mut("env") {
                env.push(json!({ "name": "CI_MATRIX", "value": "{{ item.matrix }}" }));
                env.push(json!({ "name": "CI_STRATEGY", "value": "{{ item.strategy }}" }));
            }
        }
        // The job's workspace starts with the event's commit, as every
        // job's does: alone, unless a checkout step asked for its history.
        if let Some(depth) = job.checkout_depth.filter(|depth| *depth != 1) {
            fields.insert(
                "workspace".into(),
                json!({ "checkout": { "depth": depth } }),
            );
        }

        let shell = job
            .defaults
            .shell
            .clone()
            .or_else(|| self.defaults.shell.clone());
        let working_directory = job
            .defaults
            .working_directory
            .clone()
            .or_else(|| self.defaults.working_directory.clone());
        let mut steps = Vec::new();
        // The actions of other repositories its steps use, each fetched
        // once, however many steps use it.
        let mut fetches: Vec<Value> = Vec::new();
        for (i, raw) in job.steps.iter().enumerate() {
            let at = format!("{path}.steps[{i}]");
            let (plain, late): (Vec<_>, Vec<_>) = raw
                .env
                .iter()
                .cloned()
                .partition(|(_, value)| !value.contains("${{"));
            let step = Step {
                name: raw.name.clone(),
                continue_on_error: raw.continue_on_error,
                timeout_secs: raw.timeout_secs,
                env: plain.into_iter().collect(),
                id: raw.id.clone(),
                condition: raw.condition.clone(),
                late_env: late.into_iter().collect(),
                ..Step::default()
            };
            // A step that uses an action has no script, shell or
            // directory: the action's steps have their own. One of another
            // repository is fetched before the job starts, where the
            // operator allows it.
            if let Some(uses) = &raw.uses {
                if let Some(remote) = expr::uses::remote(uses) {
                    if let Some(why) = settings.actions.refuses(&remote) {
                        check.problem(&format!("{at}.uses"), why);
                        continue;
                    }
                    let fetch = settings.actions.fetch(&remote, known.owner.as_deref());
                    if !fetches.contains(&fetch) {
                        fetches.push(fetch);
                    }
                }
                steps.push(Step {
                    uses: Some(uses.clone()),
                    with: raw.with.clone(),
                    ..step
                });
                continue;
            }
            let Some(run) = &raw.run else { continue };
            let shell = raw.shell.clone().or_else(|| shell.clone());
            let Some((run, exprs)) = script(check, &at, run, shell.as_deref()) else {
                continue;
            };
            steps.push(Step {
                run,
                shell: Some(shell.unwrap_or_else(|| DEFAULT_SHELL.to_string())),
                working_directory: raw
                    .working_directory
                    .clone()
                    .or_else(|| working_directory.clone()),
                exprs,
                ..step
            });
        }
        if steps.is_empty() {
            check.problem(
                &format!("{path}.steps"),
                "has no step that runs a script: every step of it uses an action that is \
                 answered without running one",
            );
            return None;
        }
        if fetches.len() > MAX_ACTIONS {
            check.problem(
                &format!("{path}.steps"),
                format!(
                    "use {} actions of other repositories, and a job fetches at most \
                     {MAX_ACTIONS}",
                    fetches.len()
                ),
            );
            return None;
        }
        if !fetches.is_empty() {
            let workspace = fields.entry("workspace").or_insert_with(|| json!({}));
            workspace["actions"] = Value::Array(fetches);
        }
        // An action of either kind may be one of JavaScript, which only the
        // step runner learns as it reads it: a job with a step that uses
        // one is given the Node.js such an action runs on.
        if steps.iter().any(|step| step.uses.is_some()) {
            let workspace = fields.entry("workspace").or_insert_with(|| json!({}));
            workspace["tools"] = json!(["node"]);
        }
        steps.iter_mut().for_each(|step| fills.inert_step(step));
        // A job's steps travel in its environment, as a native pipeline's
        // do, and are held to the same room.
        let carried: usize = steps
            .iter()
            .map(|s| {
                s.run.len()
                    + s.env.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>()
                    + s.with.iter().map(|(k, v)| k.len() + v.len()).sum::<usize>()
            })
            .sum();
        if steps.len() > super::MAX_STEPS
            || carried > super::MAX_STEPS_BYTES
            || steps.iter().any(|s| s.run.len() > super::MAX_RUN_BYTES)
        {
            check.problem(
                &format!("{path}.steps"),
                format!(
                    "are more than a job's environment can hold: at most {} steps, {} KiB a \
                     script and {} KiB together; put a long script in a file in the repository",
                    super::MAX_STEPS,
                    super::MAX_RUN_BYTES >> 10,
                    super::MAX_STEPS_BYTES >> 10
                ),
            );
            return None;
        }
        Some(Task {
            name: name.to_string(),
            steps,
            depends_on,
            fields,
            reports: super::Reports::default(),
            hooks: super::TaskHooks::default(),
            job: Some(super::JobSpec {
                condition: job.condition_text.as_deref().map(|text| fills.inert(text)),
                needs: needs.to_vec(),
                outputs: job
                    .outputs
                    .iter()
                    .map(|(name, value)| (name.clone(), fills.inert(value)))
                    .collect(),
            }),
        })
    }

    /// The task that plans the instances of `job`, whose matrix an earlier
    /// job computes: it runs in the job's image, is started as the job
    /// would be, and prints what the job is made a task of each of. It has
    /// no steps and no commit: only the step runner, from the workspace the
    /// engine installs it in.
    #[allow(clippy::too_many_arguments)]
    fn planner(
        &self,
        job: &Job,
        matrix: &JobMatrix,
        name: &str,
        known: &Known,
        of: &Task,
        depends_on: Vec<String>,
        rule: Option<&str>,
        needs: &[(String, Option<String>)],
    ) -> Task {
        let context = json!({
            "github": serde_json::Value::from(&with_job(&known.github, &job.id)),
            "inputs": serde_json::Value::from(&known.inputs),
        });
        let mut env = vec![
            json!({ "name": "CI_CONTEXT", "value": inert(&context.to_string()) }),
            json!({ "name": "CI_MATRIX_PLAN", "value": inert(&matrix.plan().to_string()) }),
        ];
        if let Some(condition) = &job.condition_text {
            env.push(json!({ "name": "CI_JOB_IF", "value": inert(condition) }));
        }
        if !needs.is_empty() {
            env.push(json!({ "name": "CI_NEED_COUNT", "value": needs.len().to_string() }));
        }
        let mut given = Vec::new();
        for (k, (id, task)) in needs.iter().enumerate() {
            let k = k + 1;
            env.push(json!({ "name": format!("CI_NEED_{k}_JOB"), "value": id }));
            match task {
                Some(task) => {
                    for (what, read) in [("STATUS", "status"), ("SAID", "last_line")] {
                        given.push(json!({
                            "name": format!("CI_NEED_{k}_{what}"),
                            "task": task,
                            "read": read,
                        }));
                    }
                }
                None => {
                    env.push(json!({ "name": format!("CI_NEED_{k}_STATUS"), "value": "skipped" }))
                }
            }
        }
        let mut fields = Map::new();
        if let Some(image) = of.fields.get("docker_image") {
            fields.insert("docker_image".into(), image.clone());
        }
        fields.insert(
            "command".into(),
            json!(["sh", "-c", super::compile::planner()]),
        );
        fields.insert("env".into(), Value::Array(env));
        if !given.is_empty() {
            fields.insert("env_from".into(), Value::Array(given));
        }
        if let Some(rule) = rule {
            fields.insert("trigger_rule".into(), json!(rule));
        }
        fields.insert("timeout_secs".into(), json!(300));
        fields.insert("workspace".into(), json!({ "checkout": false }));
        Task {
            name: name.to_string(),
            steps: Vec::new(),
            depends_on,
            fields,
            reports: super::Reports::default(),
            job: None,
            hooks: super::TaskHooks::default(),
        }
    }
}

/// `github`, with the job's id in it.
fn with_job(github: &Ev, job: &str) -> Ev {
    let Ev::Object(fields) = github else {
        return github.clone();
    };
    let mut fields = fields.clone();
    fields.push(("job".to_string(), Ev::from(job)));
    Ev::Object(fields)
}

fn env_value(env: &[(String, String)]) -> Ev {
    Ev::Object(
        env.iter()
            .map(|(k, v)| (k.clone(), Ev::from(v.as_str())))
            .collect(),
    )
}

/// An `env:`'s variables whose values are known when the run is made: all
/// but those that read the repository's secrets ([`late_entries`]).
fn eval_env(
    check: &mut Check,
    path: &str,
    env: &[(String, Template)],
    known: &Known,
) -> Vec<(String, String)> {
    env.iter()
        .filter(|(_, template)| !reads_secrets(template))
        .filter_map(|(name, template)| match template.text(known) {
            Ok(value) => Some((name.clone(), value)),
            Err(e) => {
                check.problem(&format!("{path}.{name}"), e.to_string());
                None
            }
        })
        .collect()
}

/// An `env:`'s variables that read the repository's secrets, as written
/// (`text`): what the step runner reads as the job starts, given the
/// secrets, as the variable `{LATE_JOB_VARIABLE}NAME` of the task.
fn late_entries(
    env: &[(String, Template)],
    text: &BTreeMap<String, String>,
) -> Vec<(String, String)> {
    env.iter()
        .filter(|(_, template)| reads_secrets(template))
        .filter_map(|(name, _)| text.get(name).map(|t| (name.clone(), t.clone())))
        .collect()
}

/// The names of the repository's secrets a template reads, in capitals, as
/// the job is given them.
fn secret_names(template: &Template) -> Vec<String> {
    template
        .exprs()
        .flat_map(|e| e.contexts())
        .filter(|(context, _)| context.eq_ignore_ascii_case("secrets"))
        .filter_map(|(_, property)| property)
        .filter(|name| is_secret_name(name))
        .map(|name| name.to_ascii_uppercase())
        .collect()
}

/// Whether a template reads the `secrets` context.
fn reads_secrets(template: &Template) -> bool {
    template.exprs().any(|e| {
        e.contexts()
            .iter()
            .any(|(context, _)| context.eq_ignore_ascii_case("secrets"))
    })
}

/// An `env:` map's values as written, where each is text.
fn env_texts(value: Option<&Value>) -> BTreeMap<String, String> {
    match value {
        Some(Value::Object(map)) => map
            .iter()
            .filter_map(|(k, v)| v.as_str().map(|s| (k.clone(), s.to_string())))
            .collect(),
        _ => BTreeMap::new(),
    }
}

/// Whether `path` is of a job's or the workflow's own `env:`: `env.NAME`,
/// `jobs.ID.env.NAME`. Not a container's or a service's, which the engine
/// reads, not the step runner.
fn is_job_env(path: &str) -> bool {
    if path.starts_with("env.") {
        return true;
    }
    let Some(rest) = path.strip_prefix("jobs.") else {
        return false;
    };
    rest.split_once('.')
        .is_some_and(|(_, after)| after.starts_with("env."))
}

/// A step's script, with each expression an outsider could have chosen the
/// value of replaced by a shell variable, and those expressions in order
/// ([`expr::script`]).
fn script(
    check: &mut Check,
    path: &str,
    run: &str,
    shell: Option<&str>,
) -> Option<(String, Vec<String>)> {
    let reads_variables = matches!(
        shell.unwrap_or(DEFAULT_SHELL),
        "bash" | "sh" | DEFAULT_SHELL
    );
    match expr::script(run, reads_variables) {
        Ok(Some(script)) => Some(script),
        Ok(None) => {
            check.problem(
                &format!("{path}.run"),
                "holds an expression whose value an outsider may choose, in a script for a \
                 shell that is not sh or bash: pass it through the step's env:, and read the \
                 variable",
            );
            None
        }
        Err(e) => {
            check.problem(&format!("{path}.run"), e.to_string());
            None
        }
    }
}

/// A workflow file checked as far as it can be without an event: read, and
/// then read for an event of each kind it takes (a push to `main`, a pull
/// request into it, a dispatch with each input's default, a schedule's
/// fire), so that what would stop a run is found before one is asked for: a
/// label with no image, an expression that does not evaluate. The workflow,
/// and the most jobs it became for any of them.
pub fn check(
    text: &str,
    stem: &str,
    settings: &Settings,
) -> Result<(Workflow, usize), Vec<Problem>> {
    let workflow = parse(text, &settings.provided)?;
    let on = &workflow.on;
    let event = |kind: &str| Event {
        kind: kind.to_string(),
        git_ref: "refs/heads/main".to_string(),
        sha: "0".repeat(40),
        base_sha: Some("0".repeat(40)),
        base_ref: matches!(kind, "pull_request" | "merge_group").then(|| "main".to_string()),
        pr: (kind == "pull_request").then_some(1),
        trust: "trusted".to_string(),
        repo: "owner/repository".to_string(),
        forge_host: "github.com".to_string(),
        ..Default::default()
    };
    let taken = [
        ("push", on.push.is_some()),
        ("pull_request", on.pull_request.is_some()),
        ("merge_group", on.merge_group.is_some()),
        ("dispatch", on.dispatch.is_some()),
        ("schedule", !on.schedule.is_empty()),
    ];
    let mut problems: Vec<Problem> = Vec::new();
    let mut jobs = 0;
    for (kind, _) in taken.iter().filter(|(_, takes)| *takes) {
        match workflow.lower(stem, &event(kind), settings) {
            Ok(lowered) => jobs = jobs.max(lowered.pipeline.tasks.len()),
            Err(found) => {
                for problem in found {
                    if !problems.contains(&problem) {
                        problems.push(problem);
                    }
                }
            }
        }
    }
    if problems.is_empty() {
        Ok((workflow, jobs))
    } else {
        Err(problems)
    }
}

/// A job's instances: one where it has no matrix, and else one for each
/// combination of its axes, less those `exclude` takes out, with what
/// `include` adds. Each with a label for its name, its `matrix` context,
/// and its place among them.
fn instances(check: &mut Check, path: &str, job: &Job) -> Vec<(Option<String>, Ev, usize, usize)> {
    let Some(matrix) = job.matrix.as_ref().map(JobMatrix::written) else {
        return vec![(None, Ev::Object(Vec::new()), 0, 1)];
    };
    let rows = match expr::matrix::rows(&matrix, MAX_JOBS) {
        Ok(rows) => rows,
        Err(_) => {
            check.problem(
                &format!("{path}.strategy.matrix"),
                format!("has more than {MAX_JOBS} combinations"),
            );
            return Vec::new();
        }
    };
    if rows.is_empty() {
        check.problem(
            &format!("{path}.strategy.matrix"),
            "leaves no combination to run",
        );
    }
    let total = rows.len();
    expr::matrix::labelled(rows)
        .into_iter()
        .enumerate()
        .map(|(index, (label, row))| {
            let matrix = Ev::Object(row.into_iter().map(|(k, v)| (k, Ev::from(v))).collect());
            (Some(label), matrix, index, total)
        })
        .collect()
}

#[cfg(test)]
mod tests;
