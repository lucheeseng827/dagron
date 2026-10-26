//! Running the steps of a job that came from a GitHub workflow: step
//! protocol 2.
//!
//! What such a step has that a native one does not is decided as the job
//! runs, and so it is decided here:
//!
//! - **`if:`**, against how the steps before went. A step with none runs
//!   while nothing has failed. `failure()` and `always()` say otherwise, so
//!   a step can run after one that failed; the job still fails.
//! - **Expressions** in a step's name, directory, variables and script, read
//!   by [`dagron_ci_expr`] against the contexts: `github`, `inputs`,
//!   `matrix` and `strategy` as the job was handed them (`CI_CONTEXT`), and
//!   `env`, `steps`, `job` and `runner`, which only this process knows.
//! - **A shell**: `bash` with GitHub's own flags, `sh`, `python`, `pwsh`, or
//!   a command with `{0}` where the script's file goes. A step that names
//!   none has `bash` where the image has one, and `sh` where it does not.
//! - **The runner's files.** A step appends to the files `GITHUB_ENV`,
//!   `GITHUB_PATH` and `GITHUB_OUTPUT` name. What it wrote is read when it
//!   ends: a variable for the steps after it, a directory at the front of
//!   their `PATH`, and its outputs, which later steps read as
//!   `steps.<id>.outputs.<name>`. `GITHUB_STEP_SUMMARY` and `GITHUB_STATE`
//!   are files a step may write to, and nothing reads them yet.
//! - **An action** (`uses: ./dir`, `uses: owner/repo@ref`, step protocol
//!   4): a composite action's steps, run in the step's place, or a
//!   JavaScript action's scripts, run on Node.js ([`action`]). The `post`
//!   of a JavaScript action runs when the job's steps are over, the last
//!   first, whatever became of them.
//!
//! A script is written to a file and run from there, as GitHub's runner
//! does: the shell's own variables are then the script's, and nothing of it
//! is an argument.
//!
//! What is not here: workflow commands. A line such as `::error::…` or
//! `::add-mask::…` is a line of the log, and masks nothing.

mod action;

use std::collections::BTreeMap;
use std::io::Write;
use std::os::unix::fs::DirBuilderExt;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

use dagron_ci_expr::{self as expr, Host, Value};

use crate::job::{CANCELLED, FAILURE, SKIPPED, SUCCESS};
use crate::protocol::{Job, Step};
use crate::result::{JobResult, Outcome, Said, StepResult};
use crate::run::{end_leftovers, marker, now_ms, spawn_command, take_cancel, wait, Printable, Ran};

/// Where the executor fetched the job's actions of other repositories.
const ENV_ACTIONS_DIR: &str = "DAGRON_ACTIONS_DIR";

/// The file the job's event is written to, in its own directory.
const EVENT_FILE: &str = "event.json";

/// The shell of a step that names none: `bash` where the image has one, as
/// GitHub's runner prefers it, and `sh` where it does not.
pub const AUTO_SHELL: &str = "auto";

/// Variables a step may not set for the steps after it: the runner's own,
/// the step protocol's, and GitHub's for the services a job asks (one that
/// moved `ACTIONS_ID_TOKEN_REQUEST_URL` would have the next step send its
/// request credential elsewhere), and the repository's secrets the engine
/// gave the job (`CI_SECRET_`), which no step forges for the next.
const KEPT_PREFIXES: [&str; 5] = [
    "GITHUB_",
    "RUNNER_",
    "CI_STEP_",
    "ACTIONS_",
    SECRET_VARIABLE,
];

/// What only the running job knows: what its steps have set and written, and
/// how they went.
struct Live {
    /// `github`, `inputs`, `matrix`, `strategy` and the job's `env`, as the
    /// job was handed them.
    context: serde_json::Value,
    /// Variables set through `GITHUB_ENV`, for every step after.
    env: BTreeMap<String, String>,
    /// Directories added through `GITHUB_PATH`, the last added first.
    path: Vec<String>,
    /// `steps.<id>`: each step's outputs, outcome and conclusion.
    steps: Vec<(String, Value)>,
    /// `needs`: the jobs this one needs, each with its result and outputs.
    needs: Value,
    failed: bool,
    /// Where the scripts and the runner's files are, for this job alone.
    dir: PathBuf,
    workspace: PathBuf,
    /// What each step wrote to its `GITHUB_STEP_SUMMARY`, in their order.
    summaries: Vec<String>,
    /// How many scripts have been written: each one's files are named by
    /// it, since a local action's steps share the step's number.
    scripts: usize,
    /// What the step that ran last wrote for the ones after it.
    wrote: Wrote,
    /// The `post` scripts of the JavaScript actions that ran, in the order
    /// they ran: each runs when the job's steps are over.
    posts: Vec<Step>,
}

/// What a step wrote to two of the runner's files, as it left them.
#[derive(Default)]
struct Wrote {
    /// `GITHUB_OUTPUT`: its outputs.
    outputs: Vec<(String, String)>,
    /// `GITHUB_STATE`: what a JavaScript action keeps for its next script,
    /// which reads it as `STATE_<name>`.
    state: Vec<(String, String)>,
}

/// A summary file's text, as much of it as is kept
/// ([`crate::report::SUMMARY_MAX`], and a little over so that the cut is
/// seen and said by whoever says it).
pub(crate) fn read_summary(path: &std::path::Path) -> String {
    use std::io::Read as _;
    // A step may leave anything where its file was. What is no file any
    // more (a pipe, a directory) is no summary, and is not waited on.
    let Ok(file) = crate::report::open_file(path) else {
        return String::new();
    };
    let mut bytes = Vec::new();
    let _ = file
        .take(crate::report::SUMMARY_MAX as u64 + 4096)
        .read_to_end(&mut bytes);
    String::from_utf8_lossy(&bytes).trim_end().to_string()
}

/// A runner file's text as a step left it: empty where it is not there, is
/// not text, or is no file any more. A pipe a step put there is not waited
/// on, as its summary's is not ([`read_summary`]).
fn runner_file(path: &std::path::Path) -> String {
    use std::io::Read as _;
    let mut text = String::new();
    if let Ok(mut file) = crate::report::open_file(path) {
        if file.read_to_string(&mut text).is_err() {
            text.clear();
        }
    }
    text
}

/// What the variable of each of the repository's secrets a job's steps
/// read begins with (dagron-ci's `SECRET_VARIABLE`).
pub const SECRET_VARIABLE: &str = "CI_SECRET_";

/// The repository's secrets the engine gave the job, taken out of this
/// process's environment ([`take_repository_secrets`]).
static REPOSITORY_SECRETS: std::sync::OnceLock<Vec<(String, String)>> = std::sync::OnceLock::new();

/// Take the repository's secrets the engine gave the job (each
/// `CI_SECRET_<NAME>` of this process's environment) out of it, so that no
/// step inherits one: a step has one where the workflow reads it, in its
/// script, `env:` or `with:`, and an action's own steps have none, as on
/// GitHub. Called once, first, while this process is one thread.
pub fn take_repository_secrets() {
    let taken: Vec<(String, String)> = std::env::vars_os()
        .filter_map(|(key, value)| {
            let name = key.to_str()?.strip_prefix(SECRET_VARIABLE)?;
            (!name.is_empty()).then(|| (name.to_string(), value.to_string_lossy().into_owned()))
        })
        .collect();
    for (name, _) in &taken {
        std::env::remove_var(format!("{SECRET_VARIABLE}{name}"));
    }
    // The environment this process began with is still its own under
    // `/proc` (`environ`), and a step runs as its user: not dumpable, it is
    // no step's to read. A step's own process is dumpable again: exec makes
    // it so.
    #[cfg(target_os = "linux")]
    if !taken.is_empty() {
        // SAFETY: a flag of this process's own.
        unsafe { libc::prctl(libc::PR_SET_DUMPABLE, 0, 0, 0, 0) };
    }
    let _ = REPOSITORY_SECRETS.set(taken);
}

/// The repository's secrets the engine gave the job, by name.
pub fn repository_secrets() -> Vec<(String, String)> {
    REPOSITORY_SECRETS.get().cloned().unwrap_or_default()
}

/// What the variable each of the job's and its workflow's `env:` that reads
/// a secret is handed in begins with (dagron-ci's `LATE_JOB_VARIABLE`): its
/// text as written, read as the job starts.
pub const LATE_JOB_VARIABLE: &str = "CI_JOB_ENVX_";

/// The job's `env:` that reads a secret, by name, as written.
fn late_job_env() -> Vec<(String, String)> {
    let mut late: Vec<(String, String)> = std::env::vars()
        .filter_map(|(key, text)| {
            key.strip_prefix(LATE_JOB_VARIABLE)
                .filter(|name| !name.is_empty())
                .map(|name| (name.to_string(), text))
        })
        .collect();
    late.sort();
    late
}

/// Whether a step's `::debug::` lines are printed, as GitHub's runner
/// prints them: where `ACTIONS_STEP_DEBUG` is `true` in the job's
/// environment (its or its workflow's `env:`, as GitHub's `vars` would
/// give it), or is the repository's secret of that name.
pub fn step_debug() -> bool {
    let on = |value: &str| value.trim().eq_ignore_ascii_case("true");
    std::env::var("ACTIONS_STEP_DEBUG").is_ok_and(|v| on(&v))
        || repository_secrets()
            .iter()
            .any(|(name, value)| name == "ACTIONS_STEP_DEBUG" && on(value))
}

/// The job's `GITHUB_TOKEN`, where the engine gave it one: a variable of
/// the job's own, which no step may set for the steps after it.
fn forge_token() -> Option<String> {
    std::env::var("GITHUB_TOKEN")
        .ok()
        .filter(|token| !token.is_empty())
}

/// The contexts as a step's expressions see them: [`Live`], with the step's
/// own variables over the job's.
struct Seen<'a> {
    live: &'a Live,
    step_env: &'a [(String, String)],
    /// Whether `secrets` is the repository's: in a job's own steps, and not
    /// in an action's, nor in what a job says it outputs, as on GitHub.
    secrets: bool,
}

fn object(fields: Vec<(&str, Value)>) -> Value {
    Value::Object(
        fields
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect(),
    )
}

impl Host for Seen<'_> {
    fn context(&self, name: &str) -> Option<Value> {
        let handed = |key: &str| {
            self.live
                .context
                .get(key)
                .filter(|v| !v.is_null())
                .cloned()
                .map(Value::from)
                .unwrap_or(Value::Object(Vec::new()))
        };
        Some(match name.to_ascii_lowercase().as_str() {
            "github" => {
                let mut github = handed("github");
                if let Value::Object(fields) = &mut github {
                    fields.push((
                        "workspace".to_string(),
                        Value::from(self.live.workspace.display().to_string()),
                    ));
                    if let Some(token) = forge_token() {
                        fields.push(("token".to_string(), Value::from(token)));
                    }
                }
                github
            }
            // The job's `GITHUB_TOKEN`, where the engine gave it one, and the
            // repository's secrets its steps read, which the engine gave it
            // as `CI_SECRET_<NAME>`. One it was not given is not here, and
            // reads as null, which is empty in a script, as on GitHub.
            "secrets" if !self.secrets => Value::Object(Vec::new()),
            "secrets" => {
                let mut fields: Vec<(String, Value)> = repository_secrets()
                    .into_iter()
                    .map(|(name, value)| (name, Value::from(value)))
                    .collect();
                if let Some(token) = forge_token() {
                    fields.push(("GITHUB_TOKEN".to_string(), Value::from(token)));
                }
                Value::Object(fields)
            }
            "inputs" => handed("inputs"),
            "matrix" => handed("matrix"),
            "strategy" => handed("strategy"),
            "vars" => Value::Object(Vec::new()),
            // The job's variables, then what steps have set, then the
            // step's own: the later of a name is the one read.
            "env" => {
                let mut env: BTreeMap<String, String> = BTreeMap::new();
                if let Value::Object(job) = handed("env") {
                    env.extend(job.into_iter().map(|(k, v)| (k, v.text())));
                }
                env.extend(self.live.env.clone());
                env.extend(self.step_env.iter().cloned());
                Value::Object(env.into_iter().map(|(k, v)| (k, Value::from(v))).collect())
            }
            "steps" => Value::Object(self.live.steps.clone()),
            "needs" => self.live.needs.clone(),
            "job" => object(vec![(
                "status",
                Value::from(if self.live.failed {
                    "failure"
                } else {
                    "success"
                }),
            )]),
            "runner" => object(vec![
                ("os", Value::from("Linux")),
                (
                    "arch",
                    Value::from(match std::env::consts::ARCH {
                        "aarch64" => "ARM64",
                        "x86_64" => "X64",
                        other => other,
                    }),
                ),
                ("name", Value::from("dagron-ci")),
                ("temp", Value::from(self.live.dir.display().to_string())),
            ]),
            _ => return None,
        })
    }

    fn call(&self, name: &str, args: &[Value]) -> Option<Result<Value, String>> {
        match name.to_ascii_lowercase().as_str() {
            "success" => Some(Ok(Value::Bool(!self.live.failed))),
            "failure" => Some(Ok(Value::Bool(self.live.failed))),
            "always" => Some(Ok(Value::Bool(true))),
            // A cancelled job starts no further step: none is asked this
            // once it is true.
            "cancelled" => Some(Ok(Value::Bool(false))),
            "hashfiles" => Some(crate::hash_files::hash_files(
                &self.live.workspace,
                Some(&self.live.dir),
                args,
            )),
            _ => None,
        }
    }
}

/// A directory of this job's own, for its scripts and the runner's files:
/// under the system's temporary directory, or in the working directory
/// where that cannot be written.
fn job_dir() -> std::io::Result<PathBuf> {
    let name = format!("dagron-ci-{}", std::process::id());
    let mut last = None;
    for base in [std::env::temp_dir(), PathBuf::from(".")] {
        let dir = base.join(&name);
        let _ = std::fs::remove_dir_all(&dir);
        match std::fs::DirBuilder::new().mode(0o700).create(&dir) {
            Ok(()) => return dir.canonicalize(),
            Err(e) => last = Some(e),
        }
    }
    Err(last.unwrap_or_else(|| std::io::Error::other("no directory could be made")))
}

/// Whether `program` is on `PATH`, as a file this user may execute.
fn on_path(program: &str) -> bool {
    use std::os::unix::fs::PermissionsExt;
    std::env::var_os("PATH").is_some_and(|path| {
        std::env::split_paths(&path).any(|dir| {
            std::fs::metadata(dir.join(program))
                .is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
        })
    })
}

/// The command that runs the script in `file` under `shell`, and the
/// ending a script for that shell has.
fn extension(shell: &str) -> &'static str {
    match shell {
        "python" => "py",
        "pwsh" => "ps1",
        _ => "sh",
    }
}

fn command_for(shell: &str, file: &Path) -> Result<Command, String> {
    let argv: Vec<String> = match shell {
        AUTO_SHELL if on_path("bash") => {
            vec!["bash", "--noprofile", "--norc", "-e", "-o", "pipefail"]
                .into_iter()
                .map(str::to_string)
                .chain([file.display().to_string()])
                .collect()
        }
        AUTO_SHELL | "sh" => vec!["sh".into(), "-e".into(), file.display().to_string()],
        "bash" => vec!["bash", "--noprofile", "--norc", "-e", "-o", "pipefail"]
            .into_iter()
            .map(str::to_string)
            .chain([file.display().to_string()])
            .collect(),
        "python" => {
            let python = if on_path("python") {
                "python"
            } else {
                "python3"
            };
            vec![python.into(), file.display().to_string()]
        }
        "pwsh" => vec![
            "pwsh".into(),
            "-command".into(),
            format!(". '{}'", file.display()),
        ],
        custom if custom.contains("{0}") => custom
            .split_whitespace()
            .map(|word| word.replace("{0}", &file.display().to_string()))
            .collect(),
        other => return Err(format!("{other:?} is not a shell a step can be run with")),
    };
    let mut command = Command::new(&argv[0]);
    command.args(&argv[1..]);
    Ok(command)
}

/// What a step wrote to one of the runner's files: `NAME=value` lines, and
/// `NAME<<DELIMITER`, the value's lines, then `DELIMITER` on a line of its
/// own.
pub fn file_commands(text: &str) -> Result<Vec<(String, String)>, String> {
    let mut out = Vec::new();
    let mut lines = text.lines();
    while let Some(line) = lines.next() {
        if line.trim().is_empty() {
            continue;
        }
        let equals = line.find('=');
        let heredoc = line.find("<<");
        match (equals, heredoc) {
            (_, Some(at)) if equals.is_none_or(|eq| at < eq) => {
                let (name, delimiter) = (line[..at].trim(), line[at + 2..].trim());
                if name.is_empty() || delimiter.is_empty() {
                    return Err(format!(
                        "{line:?} is neither NAME=value nor NAME<<DELIMITER"
                    ));
                }
                let mut value: Vec<&str> = Vec::new();
                loop {
                    match lines.next() {
                        Some(end) if end == delimiter => break,
                        Some(more) => value.push(more),
                        None => {
                            return Err(format!(
                                "the value of {name} is not ended by a line that is {delimiter:?}"
                            ))
                        }
                    }
                }
                out.push((name.to_string(), value.join("\n")));
            }
            (Some(at), _) if !line[..at].trim().is_empty() => {
                out.push((line[..at].trim().to_string(), line[at + 1..].to_string()));
            }
            _ => {
                return Err(format!(
                    "{line:?} is neither NAME=value nor NAME<<DELIMITER"
                ))
            }
        }
    }
    Ok(out)
}

/// Run a job's steps, under protocol 2, 3 or 4.
pub fn run(job: &Job, log: &mut impl Write) -> JobResult {
    let failed_to_start = |why: String, log: &mut dyn Write| {
        let _ = writeln!(log, "::dagron-ci::error::{why}");
        JobResult {
            protocol: job.protocol,
            exit_code: 125,
            cancelled_by: None,
            steps: job.steps.iter().map(StepResult::skipped).collect(),
            said: (job.protocol >= 3).then(|| Said {
                result: FAILURE,
                outputs: Vec::new(),
            }),
            summary: String::new(),
        }
    };
    let dir = match job_dir() {
        Ok(dir) => dir,
        Err(e) => {
            return failed_to_start(
                format!("no directory for this job's scripts could be made: {e}"),
                log,
            )
        }
    };
    let workspace = std::env::current_dir().unwrap_or_else(|_| PathBuf::from("."));
    // An instance that was made as the run went is told which it is then:
    // its matrix and its place are over what the job was handed.
    let mut context = job.context.clone().unwrap_or(serde_json::Value::Null);
    for (key, told) in [("matrix", &job.matrix), ("strategy", &job.strategy)] {
        if let Some(told) = told {
            if !context.is_object() {
                context = serde_json::json!({});
            }
            context[key] = told.clone();
        }
    }
    let mut live = Live {
        context,
        env: BTreeMap::new(),
        path: Vec::new(),
        steps: Vec::new(),
        needs: crate::job::needs_context(&job.needs),
        failed: false,
        dir,
        workspace,
        summaries: Vec::new(),
        scripts: 0,
        wrote: Wrote::default(),
        posts: Vec::new(),
    };
    // The event, as a file a JavaScript action reads it from
    // (`GITHUB_EVENT_PATH`): what of it the job's context holds.
    let event = live
        .context
        .get("github")
        .and_then(|github| github.get("event"))
        .cloned()
        .unwrap_or_else(|| serde_json::json!({}));
    let _ = std::fs::write(live.dir.join(EVENT_FILE), event.to_string());
    let late = late_job_env();
    let mut results = Vec::with_capacity(job.steps.len());
    let mut groups = Vec::new();
    let mut exit_code = 0;
    let mut cancelled_by = None;
    // Under protocol 3 whether the job runs at all is decided here, from
    // the jobs it needs: one that does not runs no step, and says so. An
    // `if:` that cannot be evaluated is no answer, and the job fails.
    let mut runs_none = false;
    if job.protocol >= 3 {
        match crate::job::runs(job.job_if.as_deref(), &live.context, &live.needs) {
            Ok(true) => {}
            Ok(false) => {
                marker(log, format_args!("job::skipped"));
                runs_none = true;
            }
            Err(why) => {
                marker(
                    log,
                    format_args!("error::the job's if: {}", Printable(&why)),
                );
                runs_none = true;
                exit_code = 1;
            }
        }
    }
    // The job's and its workflow's `env:` that read the repository's
    // secrets, read now, with them: for every step, as the rest of the
    // job's `env:` is. One that cannot be read fails the job.
    if !runs_none {
        for (key, text) in &late {
            // The runner's own, as a step's `GITHUB_ENV` may not set them.
            if KEPT_PREFIXES.iter().any(|p| key.starts_with(p)) {
                marker(
                    log,
                    format_args!(
                        "warning::the job's env.{} is the runner's own, and is not set",
                        Printable(key)
                    ),
                );
                continue;
            }
            let read = expr::template(text).and_then(|t| {
                t.text(&Seen {
                    live: &live,
                    step_env: &[],
                    secrets: true,
                })
            });
            match read {
                Ok(value) => {
                    live.env.insert(key.clone(), value);
                }
                Err(why) => {
                    marker(
                        log,
                        format_args!(
                            "error::the job's env.{key}: {}",
                            Printable(&why.to_string())
                        ),
                    );
                    runs_none = true;
                    exit_code = 1;
                    break;
                }
            }
        }
    }
    for step in &job.steps {
        if cancelled_by.is_none() {
            cancelled_by = take_cancel();
        }
        if cancelled_by.is_some() || runs_none {
            results.push(StepResult::skipped(step));
            continue;
        }
        match one(step, job, &mut live, &mut groups, log, 0) {
            Went::Skipped => results.push(StepResult::skipped(step)),
            Went::Ran(result, cancelled) => {
                let ended_job = matches!(result.conclusion, Outcome::Failure | Outcome::TimedOut);
                if ended_job && exit_code == 0 {
                    exit_code = result.exit_code.unwrap_or(1).max(1);
                }
                if ended_job {
                    live.failed = true;
                }
                if cancelled.is_some() {
                    cancelled_by = cancelled;
                }
                results.push(result);
            }
        }
    }
    // The posts of the JavaScript actions that ran, the last first, as
    // GitHub's runner runs them, whatever became of the steps: each where
    // its `post-if` holds. One that fails fails the job. A cancelled job
    // runs none.
    for post in std::mem::take(&mut live.posts).into_iter().rev() {
        if cancelled_by.is_none() {
            cancelled_by = take_cancel();
        }
        if cancelled_by.is_some() {
            break;
        }
        if let Went::Ran(result, cancelled) = one(&post, job, &mut live, &mut groups, log, 1) {
            if matches!(result.conclusion, Outcome::Failure | Outcome::TimedOut) {
                marker(
                    log,
                    format_args!(
                        "error::step {}: {} exited {}",
                        post.n,
                        Printable(&result.name),
                        result.exit_code.unwrap_or(1)
                    ),
                );
                live.failed = true;
                if exit_code == 0 {
                    exit_code = result.exit_code.unwrap_or(1).max(1);
                }
            }
            if cancelled.is_some() {
                cancelled_by = cancelled;
            }
        }
    }
    if let Some(signal) = cancelled_by {
        marker(log, format_args!("cancelled::{signal}"));
        exit_code = 128 + signal;
    }
    end_leftovers(&groups);
    // What the job says to the jobs after it. Its outputs are read whether
    // or not it failed: what its steps set before then stands, as on GitHub.
    let said = (job.protocol >= 3).then(|| {
        let result = match (cancelled_by, exit_code) {
            (Some(_), _) => CANCELLED,
            (None, 0) if runs_none => SKIPPED,
            (None, 0) => SUCCESS,
            (None, _) => FAILURE,
        };
        let mut outputs = Vec::new();
        if !runs_none {
            let seen = Seen {
                live: &live,
                step_env: &[],
                secrets: false,
            };
            for (name, text) in &job.outputs {
                let value = expr::template(text).and_then(|template| template.text(&seen));
                outputs.push((
                    name.clone(),
                    value.unwrap_or_else(|e| {
                        marker(
                            log,
                            format_args!(
                                "warning::the job's output {} could not be read, and is empty: {}",
                                Printable(name),
                                Printable(&e.to_string())
                            ),
                        );
                        String::new()
                    }),
                ));
            }
        }
        Said { result, outputs }
    });
    let _ = std::fs::remove_dir_all(&live.dir);
    JobResult {
        protocol: job.protocol,
        exit_code,
        cancelled_by,
        steps: results,
        said,
        summary: live.summaries.join("\n\n"),
    }
}

enum Went {
    /// Its `if:` was false.
    Skipped,
    /// It ran, or could not be started: its result, and the signal that
    /// cancelled the job while it ran, if one did.
    Ran(StepResult, Option<i32>),
}

/// A step that could not be run at all: it failed, with why as a marker.
/// One of a local action's steps (`depth` over 0) is no step of the job's,
/// and has no markers of its own: why is said within the step that used
/// the action.
fn could_not(step: &Step, name: &str, why: String, log: &mut impl Write, depth: usize) -> Went {
    if depth == 0 {
        marker(
            log,
            format_args!("step::{}::start::{}", step.n, Printable(name)),
        );
    }
    marker(
        log,
        format_args!("error::step {}: {}", step.n, Printable(&why)),
    );
    if depth == 0 {
        marker(log, format_args!("step::{}::end::1", step.n));
    }
    let conclusion = if step.continue_on_error {
        if depth == 0 {
            marker(log, format_args!("step::{}::continued", step.n));
        }
        Outcome::Success
    } else {
        Outcome::Failure
    };
    Went::Ran(
        StepResult {
            n: step.n,
            name: name.to_string(),
            outcome: Outcome::Failure,
            conclusion,
            exit_code: Some(1),
            signal: None,
            started_at_ms: Some(now_ms()),
            duration_ms: Some(0),
            timeout_secs: step.timeout.map(|t| t.as_secs()),
        },
        None,
    )
}

/// Run one step: one of the job's (`depth` 0), or one of a local action's,
/// `depth` actions down.
fn one(
    step: &Step,
    job: &Job,
    live: &mut Live,
    groups: &mut Vec<libc::pid_t>,
    log: &mut impl Write,
    depth: usize,
) -> Went {
    // The step's own variables, as text, over the task's.
    let mut step_env: Vec<(String, String)> = step
        .env
        .iter()
        .map(|(k, v)| {
            (
                k.to_string_lossy().into_owned(),
                v.to_string_lossy().into_owned(),
            )
        })
        .collect();
    let evaluate =
        |text: &str, live: &Live, step_env: &[(String, String)]| -> Result<String, String> {
            let template = expr::template(text).map_err(|e| e.to_string())?;
            template
                .text(&Seen {
                    live,
                    step_env,
                    secrets: depth == 0,
                })
                .map_err(|e| e.to_string())
        };
    // Its name first: a step that is skipped has one too.
    let name = evaluate(&step.name, live, &step_env).unwrap_or_else(|_| step.name.clone());

    // Whether it runs: its `if:`, or that nothing has failed.
    let runs = match &step.condition {
        None => Ok(!live.failed),
        Some(condition) => expr::condition(condition)
            .map_err(|e| e.to_string())
            .and_then(|template| {
                let asks = template.exprs().any(expr::Expr::asks_status);
                let seen = Seen {
                    live,
                    step_env: &step_env,
                    secrets: depth == 0,
                };
                let holds = template.eval(&seen).map_err(|e| e.to_string())?.truthy();
                Ok(holds && (asks || !live.failed))
            }),
    };
    match runs {
        Ok(true) => {}
        Ok(false) => return Went::Skipped,
        Err(why) => return could_not(step, &name, format!("its if: {why}"), log, depth),
    }

    // Its variables whose values hold expressions, in their order, each
    // able to read the ones before it; then what its script's variables
    // stand for.
    for (key, text) in &step.late_env {
        match evaluate(text, live, &step_env) {
            Ok(value) => step_env.push((key.clone(), value)),
            Err(why) => return could_not(step, &name, format!("env.{key}: {why}"), log, depth),
        }
    }
    // A step that uses an action runs the action's steps in its place.
    if step.uses.is_some() {
        return action::run(step, &name, job, live, &step_env, groups, log, depth);
    }
    let mut script_env: Vec<(String, String)> = Vec::new();
    for (k, source) in step.exprs.iter().enumerate() {
        let read = expr::parse(source)
            .and_then(|e| {
                e.eval(&Seen {
                    live,
                    step_env: &step_env,
                    secrets: depth == 0,
                })
            })
            .map(|value| value.text());
        match read {
            Ok(value) => script_env.push((format!("__CI_E{}", k + 1), value)),
            Err(why) => {
                return could_not(
                    step,
                    &name,
                    format!("an expression of its script: {why}"),
                    log,
                    depth,
                )
            }
        }
    }
    let shell = step.shell.as_deref().unwrap_or(AUTO_SHELL);
    live.scripts += 1;
    let tag = format!("{}-{}", step.n, live.scripts);
    // A program of the step runner's own (a JavaScript action's script, on
    // Node.js), or the step's script, with what is pasted into it pasted,
    // written to a file and run by its shell.
    let mut command = if let [program, args @ ..] = step.argv.as_slice() {
        let mut command = Command::new(program);
        command.args(args);
        command
    } else {
        let script = match evaluate(&step.run, live, &step_env) {
            Ok(script) => script,
            Err(why) => return could_not(step, &name, format!("its script: {why}"), log, depth),
        };
        let file = live.dir.join(format!("step-{tag}.{}", extension(shell)));
        if let Err(e) = std::fs::write(&file, &script) {
            return could_not(
                step,
                &name,
                format!("its script could not be written: {e}"),
                log,
                depth,
            );
        }
        match command_for(shell, &file) {
            Ok(command) => command,
            Err(why) => return could_not(step, &name, why, log, depth),
        }
    };
    let program = step.argv.first().map_or(shell, String::as_str);
    // Where it starts.
    if let Some(dir) = &step.workdir {
        let dir = match evaluate(dir, live, &step_env) {
            Ok(dir) => live.workspace.join(dir),
            Err(why) => {
                return could_not(
                    step,
                    &name,
                    format!("its working-directory: {why}"),
                    log,
                    depth,
                )
            }
        };
        if !dir.is_dir() {
            return could_not(
                step,
                &name,
                format!("its working-directory {} is not a directory", dir.display()),
                log,
                depth,
            );
        }
        command.current_dir(dir);
    }
    // The runner's files, each new and empty for this step.
    let files: Vec<(&str, PathBuf)> = ["ENV", "PATH", "OUTPUT", "STEP_SUMMARY", "STATE"]
        .into_iter()
        .map(|kind| {
            (
                kind,
                live.dir
                    .join(format!("step-{tag}.{}", kind.to_ascii_lowercase())),
            )
        })
        .collect();
    for (kind, path) in &files {
        if let Err(e) = std::fs::write(path, "") {
            return could_not(
                step,
                &name,
                format!("the file for GITHUB_{kind} could not be made: {e}"),
                log,
                depth,
            );
        }
        command.env(format!("GITHUB_{kind}"), path);
    }
    // What a script expects of the runner, then what steps have set, then
    // the step's own, then what its script's variables stand for.
    for (key, value) in runner_env(job, live) {
        command.env(key, value);
    }
    command.envs(&live.env);
    if !live.path.is_empty() {
        let mut dirs: Vec<PathBuf> = live.path.iter().rev().map(PathBuf::from).collect();
        dirs.extend(
            std::env::var_os("PATH")
                .iter()
                .flat_map(std::env::split_paths),
        );
        if let Ok(path) = std::env::join_paths(dirs) {
            command.env("PATH", path);
        }
    }
    command.envs(step_env.iter().cloned());
    command.envs(script_env);

    // One of a local action's steps is no step of the job's: it is a line
    // of the step that used the action.
    if depth == 0 {
        marker(
            log,
            format_args!("step::{}::start::{}", step.n, Printable(&name)),
        );
    } else {
        let _ = writeln!(log, "-- {}", Printable(&name));
    }
    let started_at_ms = now_ms();
    let started = Instant::now();
    let ran = match spawn_command(command) {
        Ok(group) => {
            groups.push(group);
            wait(group, step, job.grace)
        }
        Err(e) => {
            marker(
                log,
                format_args!("warning::step {} could not start {program}: {e}", step.n),
            );
            Ran {
                code: 127,
                signal: None,
                timed_out: false,
                cancelled: None,
            }
        }
    };
    // What it wrote for the steps after it, read whether or not it failed.
    let mut wrote_badly = None;
    let mut outputs = Vec::new();
    let mut state = Vec::new();
    for (kind, path) in &files {
        // Its summary is the job's to say when it is over, and is read to
        // a bound: a step may write any amount there.
        if *kind == "STEP_SUMMARY" {
            let text = read_summary(path);
            if !text.trim().is_empty() {
                live.summaries.push(text);
            }
            continue;
        }
        let text = runner_file(path);
        match *kind {
            "ENV" | "OUTPUT" => match file_commands(&text) {
                Ok(pairs) if *kind == "OUTPUT" => outputs = pairs,
                Ok(pairs) => {
                    for (key, value) in pairs {
                        if KEPT_PREFIXES.iter().any(|p| key.starts_with(p)) {
                            marker(
                                log,
                                format_args!(
                                    "warning::step {} may not set {}: it is the runner's own",
                                    step.n,
                                    Printable(&key)
                                ),
                            );
                        } else {
                            live.env.insert(key, value);
                        }
                    }
                }
                Err(why) => wrote_badly = Some(format!("GITHUB_{kind}: {why}")),
            },
            "PATH" => live.path.extend(
                text.lines()
                    .map(str::trim)
                    .filter(|l| !l.is_empty())
                    .map(str::to_string),
            ),
            "STATE" => match file_commands(&text) {
                Ok(pairs) => state = pairs,
                Err(why) => wrote_badly = Some(format!("GITHUB_STATE: {why}")),
            },
            _ => {}
        }
    }
    live.wrote = Wrote {
        outputs: outputs.clone(),
        state,
    };
    let mut code = ran.code;
    if let (Some(why), 0) = (&wrote_badly, code) {
        marker(
            log,
            format_args!("error::step {}: {}", step.n, Printable(why)),
        );
        code = 1;
    }
    if depth == 0 {
        marker(log, format_args!("step::{}::end::{code}", step.n));
    }

    let outcome = if ran.cancelled.is_some() {
        Outcome::Cancelled
    } else if code == 0 {
        Outcome::Success
    } else if ran.timed_out {
        Outcome::TimedOut
    } else {
        Outcome::Failure
    };
    let mut conclusion = outcome;
    if matches!(outcome, Outcome::Failure | Outcome::TimedOut) && step.continue_on_error {
        if depth == 0 {
            marker(log, format_args!("step::{}::continued", step.n));
        }
        conclusion = Outcome::Success;
    }
    record(live, step, outputs, outcome, conclusion);
    Went::Ran(
        StepResult {
            n: step.n,
            name,
            outcome,
            conclusion,
            exit_code: Some(code),
            signal: ran.signal,
            started_at_ms: Some(started_at_ms),
            duration_ms: Some(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)),
            timeout_secs: step.timeout.map(|t| t.as_secs()),
        },
        ran.cancelled,
    )
}

/// A step's outputs, outcome and conclusion, as later steps read them:
/// `steps.<id>`, where it has an id.
fn record(
    live: &mut Live,
    step: &Step,
    outputs: Vec<(String, String)>,
    outcome: Outcome,
    conclusion: Outcome,
) {
    let Some(id) = &step.id else { return };
    live.steps.retain(|(known, _)| known != id);
    live.steps.push((
        id.clone(),
        object(vec![
            (
                "outputs",
                Value::Object(
                    outputs
                        .into_iter()
                        .map(|(k, v)| (k, Value::from(v)))
                        .collect(),
                ),
            ),
            ("outcome", Value::from(outcome.as_str())),
            ("conclusion", Value::from(conclusion.as_str())),
        ]),
    ));
}

/// The variables a script expects of GitHub's runner, as far as the job's
/// context tells them.
fn runner_env(job: &Job, live: &Live) -> Vec<(String, String)> {
    let github = |key: &str| -> String {
        job.context
            .as_ref()
            .and_then(|c| c.get("github"))
            .and_then(|g| g.get(key))
            .and_then(serde_json::Value::as_str)
            .unwrap_or_default()
            .to_string()
    };
    let mut env = vec![
        ("CI".to_string(), "true".to_string()),
        ("GITHUB_ACTIONS".to_string(), "true".to_string()),
        (
            "GITHUB_WORKSPACE".to_string(),
            live.workspace.display().to_string(),
        ),
        ("RUNNER_OS".to_string(), "Linux".to_string()),
        (
            "RUNNER_ARCH".to_string(),
            match std::env::consts::ARCH {
                "aarch64" => "ARM64",
                "x86_64" => "X64",
                other => other,
            }
            .to_string(),
        ),
        ("RUNNER_NAME".to_string(), "dagron-ci".to_string()),
        ("RUNNER_TEMP".to_string(), live.dir.display().to_string()),
        // Where a setup action keeps what it downloads, for the job.
        (
            "RUNNER_TOOL_CACHE".to_string(),
            live.dir.join("toolcache").display().to_string(),
        ),
        (
            "GITHUB_EVENT_PATH".to_string(),
            live.dir.join(EVENT_FILE).display().to_string(),
        ),
    ];
    // A job given an OIDC identity: GitHub's two variables, from the
    // engine's. `@actions/core` adds `&audience=` to the URL, which so has a
    // query of its own already; dagron's answer is GitHub's, `{"value": …}`.
    if let (Some(url), Some(token)) = (
        std::env::var("DAGRON_OIDC_REQUEST_URL")
            .ok()
            .filter(|url| !url.is_empty()),
        std::env::var("DAGRON_OIDC_REQUEST_TOKEN")
            .ok()
            .filter(|token| !token.is_empty()),
    ) {
        let url = if url.contains('?') {
            url
        } else {
            format!("{url}?api-version=2.0")
        };
        env.push(("ACTIONS_ID_TOKEN_REQUEST_URL".to_string(), url));
        env.push(("ACTIONS_ID_TOKEN_REQUEST_TOKEN".to_string(), token));
    }
    for (variable, key) in [
        ("GITHUB_SHA", "sha"),
        ("GITHUB_REF", "ref"),
        ("GITHUB_REF_NAME", "ref_name"),
        ("GITHUB_REF_TYPE", "ref_type"),
        ("GITHUB_REPOSITORY", "repository"),
        ("GITHUB_REPOSITORY_OWNER", "repository_owner"),
        ("GITHUB_EVENT_NAME", "event_name"),
        ("GITHUB_BASE_REF", "base_ref"),
        ("GITHUB_JOB", "job"),
        ("GITHUB_WORKFLOW", "workflow"),
        ("GITHUB_SERVER_URL", "server_url"),
        ("GITHUB_API_URL", "api_url"),
        ("GITHUB_GRAPHQL_URL", "graphql_url"),
        ("GITHUB_RUN_ID", "run_id"),
        ("GITHUB_RUN_NUMBER", "run_number"),
        ("GITHUB_RUN_ATTEMPT", "run_attempt"),
        ("GITHUB_ACTOR", "actor"),
        ("GITHUB_TRIGGERING_ACTOR", "triggering_actor"),
        ("GITHUB_HEAD_REF", "head_ref"),
    ] {
        env.push((variable.to_string(), github(key)));
    }
    if step_debug() {
        env.push(("RUNNER_DEBUG".to_string(), "1".to_string()));
    }
    env
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn what_a_step_writes_to_a_runner_s_file_is_read_as_github_reads_it() {
        let read = file_commands(
            "A=1\nB=two words = and more\n\nJSON<<EOF\n{\n  \"a\": 1\n}\nEOF\nEMPTY=\nC<<ghadelimiter_1\nline\nghadelimiter_1\n",
        )
        .unwrap();
        assert_eq!(
            read,
            [
                ("A".to_string(), "1".to_string()),
                ("B".to_string(), "two words = and more".to_string()),
                ("JSON".to_string(), "{\n  \"a\": 1\n}".to_string()),
                ("EMPTY".to_string(), String::new()),
                ("C".to_string(), "line".to_string()),
            ]
        );
        assert_eq!(file_commands("").unwrap(), Vec::new());
        // A value with << in it, after its =, is a value.
        assert_eq!(
            file_commands("A=x<<y").unwrap(),
            [("A".to_string(), "x<<y".to_string())]
        );
        for (bad, why) in [
            ("no equals here", "neither NAME=value nor NAME<<DELIMITER"),
            ("=value", "neither NAME=value nor NAME<<DELIMITER"),
            (
                "A<<EOF\nnever ended",
                "is not ended by a line that is \"EOF\"",
            ),
            ("<<EOF\nx\nEOF", "neither NAME=value nor NAME<<DELIMITER"),
        ] {
            let err = file_commands(bad).unwrap_err();
            assert!(err.contains(why), "{bad:?}: {err}");
        }
    }

    /// A step may put a pipe where a file of the runner's was. It is read
    /// as holding nothing, and is not waited on: nobody writes to it once
    /// the step is over.
    #[test]
    fn a_pipe_where_a_runner_s_file_was_is_not_waited_on() {
        use std::os::unix::ffi::OsStrExt;
        let dir = std::env::temp_dir().join(format!("dagron-ci-pipe-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let pipe = dir.join("summary.md");
        let c_path = std::ffi::CString::new(pipe.as_os_str().as_bytes()).unwrap();
        // SAFETY: `c_path` is a NUL-terminated path that outlives the call.
        assert_eq!(unsafe { libc::mkfifo(c_path.as_ptr(), 0o600) }, 0);
        let file = dir.join("output");
        std::fs::write(&file, "a=1\n").unwrap();

        let (tx, rx) = std::sync::mpsc::channel();
        let (asked, also) = (pipe.clone(), file.clone());
        std::thread::spawn(move || {
            let _ = tx.send((
                read_summary(&asked),
                runner_file(&asked),
                runner_file(&also),
                runner_file(&asked.with_extension("gone")),
            ));
        });
        let read = rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("a pipe is not waited on");
        assert_eq!(
            read,
            (
                String::new(),
                String::new(),
                "a=1\n".to_string(),
                String::new()
            )
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn a_shell_is_run_as_github_s_runner_runs_it() {
        let argv = |shell: &str| {
            let command = command_for(shell, Path::new("/t/step-1.sh")).unwrap();
            std::iter::once(command.get_program())
                .chain(command.get_args())
                .map(|a| a.to_string_lossy().into_owned())
                .collect::<Vec<_>>()
                .join(" ")
        };
        assert_eq!(
            argv("bash"),
            "bash --noprofile --norc -e -o pipefail /t/step-1.sh"
        );
        assert_eq!(argv("sh"), "sh -e /t/step-1.sh");
        assert_eq!(argv("pwsh"), "pwsh -command . '/t/step-1.sh'");
        assert_eq!(argv("perl -w {0}"), "perl -w /t/step-1.sh");
        assert!(argv("python").ends_with(" /t/step-1.sh"));
        let auto = argv(AUTO_SHELL);
        assert!(
            auto == "sh -e /t/step-1.sh" || auto.starts_with("bash --noprofile"),
            "{auto}"
        );
        assert!(command_for("fish", Path::new("/x")).is_err());
        assert_eq!(
            (
                extension("python"),
                extension("pwsh"),
                extension("bash"),
                extension(AUTO_SHELL)
            ),
            ("py", "ps1", "sh", "sh")
        );
    }
}
