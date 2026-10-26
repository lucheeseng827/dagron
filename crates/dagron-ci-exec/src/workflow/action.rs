//! An action a step uses, under step protocol 4: one of the workflow's own
//! repository (`uses: ./dir`), read from the workspace, or one of another
//! (`uses: owner/repo@ref`), which the executor fetched before the job
//! started into a directory of its own under `DAGRON_ACTIONS_DIR`.
//!
//! The action is read as the step starts, as GitHub's runner reads it:
//! `action.yml` in its directory, or `action.yaml`, where that file still is
//! once every link on the way to it is followed: in the workspace, or in
//! the repository the action was fetched from. A composite action's steps
//! then run in the step's place, and a JavaScript action's scripts run on
//! Node.js ([JavaScript actions](#javascript-actions)).
//!
//! - **Its inputs** are what the step gives it with `with:`, and the
//!   default of each it is not given, read as the step's own expressions
//!   are. They are its steps' `inputs` context. An input it does not
//!   declare is warned of, and given all the same.
//! - **Its steps** are `run:` steps, each with a `shell:`, and uses of
//!   another local action. They have a `steps` context of their own, and
//!   `success()` and `failure()` ask how they went, not how the job has.
//!   Each has the step's variables and `GITHUB_ACTION_PATH`, and an action
//!   of another repository `GITHUB_ACTION_REPOSITORY` and
//!   `GITHUB_ACTION_REF`. What one sets through `GITHUB_ENV` and
//!   `GITHUB_PATH` is the job's, as on GitHub.
//! - **Its outputs** are read when its steps are over, against them, and
//!   are the step's: `steps.<id>.outputs.<name>`.
//! - **The step's `timeout-minutes`** is the action's, all its steps
//!   together.
//!
//! In a script of it an expression whose value no outsider chooses is
//! pasted, and any other is a variable the script reads, as in a
//! workflow's ([`dagron_ci_expr::script`]). An input's value is one an
//! outsider may choose, so a script for a shell that is not `sh` or `bash`
//! reads an input through its step's `env:`.
//!
//! # JavaScript actions
//!
//! An action of `runs.using: node20` or `node24` (and `node12` and
//! `node16`, which GitHub runs on a later Node.js too) runs its scripts,
//! files of its directory found as its own file is, on the Node.js the
//! engine installs beside the step runner (`/dagron/bin/node`), else one
//! on `PATH`:
//!
//! - **`pre`**, where its `pre-if` holds, as the step starts (on GitHub,
//!   as the job starts);
//! - **`main`**, with each input as `INPUT_<NAME>` and what `pre` wrote to
//!   `GITHUB_STATE` as `STATE_<name>`: what it writes to `GITHUB_OUTPUT`
//!   is the step's outputs;
//! - **`post`**, when the job's steps are over, where its `post-if` holds,
//!   with what `main` wrote to `GITHUB_STATE`.
//!
//! Each is given the runner's files and variables, as a script is, and
//! `GITHUB_ACTION`, `GITHUB_EVENT_PATH` and `RUNNER_TOOL_CACHE`.
//!
//! # Docker actions
//!
//! A step that uses an image (`uses: docker://image`, its `with.args` and
//! `with.entrypoint`), and an action of `runs.using: docker` whose image is
//! one (`runs.image: docker://image`, its `runs.args`, `runs.entrypoint`,
//! `runs.env`, `pre-entrypoint` and `post-entrypoint`), run in a container
//! of that image the engine starts beside the job ([`crate::container`]):
//! with the job's workspace at `/github/workspace`, which is where it
//! starts, the job's network, the step's variables, each input as
//! `INPUT_<NAME>`, and the runner's files there too. An action whose image
//! is a Dockerfile of its directory (`runs.image: Dockerfile`) has it built
//! first, by the engine, from the Dockerfile's directory, as GitHub builds
//! it: only for a trusted run, and only where the engine's operator lets it
//! ([`crate::container`]).
//!
//! A step of a composite action that uses an action of another repository
//! runs it where it was fetched with the job's own: dagron-ci reads the
//! actions a job's steps use as it handles the event, and fetches those
//! theirs use. One it could not read is not there, and the step says so.

use std::ffi::OsString;
use std::io::{Read as _, Write};
use std::path::Path;
use std::time::{Duration, Instant};

use dagron_ci_expr::{self as expr, uses};
use serde_yaml::Value as Yaml;

use super::{could_not, one, record, Live, Seen, Went};
use crate::protocol::{is_variable_name, Job, Step};
use crate::result::{Outcome, StepResult};
use crate::run::{marker, now_ms, take_cancel, Printable};

/// How many actions deep one local action may use another.
pub const MAX_DEPTH: usize = 10;

/// The most an action's file may be.
const MAX_ACTION_BYTES: u64 = 1 << 20;

/// Where the executor fetched a job's actions of other repositories, where
/// it says none: the place every executor but the local one fetches them.
const ACTIONS_DIR: &str = "/dagron/actions";

/// An action, as its file has it.
struct Action {
    /// The file's name: `action.yml` or `action.yaml`.
    file: &'static str,
    /// Each input it declares, and its default where it has one.
    inputs: Vec<(String, Option<String>)>,
    /// How it runs.
    runs: Runs,
    /// Each output of a composite action, and the text its value is read
    /// from. A JavaScript action's are what its `main` writes.
    outputs: Vec<(String, String)>,
}

/// How an action runs, as its `runs.using` says.
enum Runs {
    /// `composite`: its steps, as written.
    Composite(Vec<Yaml>),
    /// `node20` and its like: its scripts.
    Node(Scripts),
    /// `docker`, of an image a registry has or of a Dockerfile; or a
    /// step's `docker://`.
    Docker(Image),
}

/// A Docker action's image and what it is run with, each text as written.
struct Image {
    /// The image a registry has; empty where it is built.
    image: String,
    /// The Dockerfile it is built from, where it is: a file of the action's
    /// directory, once every link on the way is followed.
    build: Option<std::path::PathBuf>,
    entrypoint: Option<String>,
    args: Vec<String>,
    env: Vec<(String, String)>,
    pre: Option<String>,
    post: Option<String>,
    /// A step's `uses: docker://`: its args and entrypoint are its
    /// `with:`'s, and every other input of it is one.
    of_step: bool,
}

/// A JavaScript action's scripts, each a file of its directory, and when
/// `pre` and `post` run.
struct Scripts<P = String> {
    main: P,
    pre: Option<P>,
    pre_if: Option<String>,
    post: Option<P>,
    post_if: Option<String>,
}

/// What an action that is run is run with, but the job's own state.
enum Work {
    /// A composite action's steps, each with its `continue-on-error`
    /// where that is an expression.
    Steps(Vec<(Step, Option<String>)>),
    /// A JavaScript action's: the Node.js it runs on, and its scripts.
    Node(std::path::PathBuf, Scripts<std::path::PathBuf>),
    /// A Docker action's image.
    Docker,
}

/// What both kinds of action run with: the step that uses it, and the job
/// it is in.
struct Doing<'a> {
    step: &'a Step,
    job: &'a Job,
    step_env: &'a [(String, String)],
    uses: &'a str,
    /// When the step's time is up.
    deadline: Option<Instant>,
    depth: usize,
}

impl Doing<'_> {
    /// `inner` with what is left of the step's time, or `None` where none
    /// is: an action's steps share the step's.
    fn timed(&self, mut inner: Step) -> Option<Step> {
        if let Some(deadline) = self.deadline {
            match deadline.checked_duration_since(Instant::now()) {
                Some(left) if left >= Duration::from_secs(1) => inner.timeout = Some(left),
                _ => return None,
            }
        }
        Some(inner)
    }
}

/// How an action's run went.
struct Done {
    outcome: Outcome,
    /// What the step exits with, where it did not succeed.
    code: i32,
    signal: Option<i32>,
    /// The signal that cancelled the job while it ran, where one did.
    cancelled: Option<i32>,
    outputs: Vec<(String, String)>,
}

impl Done {
    /// An action whose time ran out before what it had left to run started.
    fn timed_out(outputs: Vec<(String, String)>) -> Done {
        Done {
            outcome: Outcome::TimedOut,
            code: 124,
            signal: None,
            cancelled: None,
            outputs,
        }
    }
}

/// Run the action `step` uses, in the step's place. `name` is the step's,
/// and `step_env` its variables, as [`one`] read them.
#[allow(clippy::too_many_arguments)]
pub(super) fn run(
    step: &Step,
    name: &str,
    job: &Job,
    live: &mut Live,
    step_env: &[(String, String)],
    groups: &mut Vec<libc::pid_t>,
    log: &mut impl Write,
    depth: usize,
) -> Went {
    let uses = step.uses.as_deref().unwrap_or_default();
    if depth >= MAX_DEPTH {
        let why = format!("it uses {uses}, which is more than {MAX_DEPTH} actions deep");
        return could_not(step, name, why, log, depth);
    }
    // Its directory, and where its file must still be once every link on
    // the way to it is followed: the workspace, or the repository it was
    // fetched from, each as its own links resolve.
    let resolved = |dir: &Path| dir.canonicalize().unwrap_or_else(|_| dir.to_path_buf());
    // A step in an image of its own has no action's file: what it is run
    // with is the step's `with:`.
    if let Some(image) = uses::docker(uses) {
        let action = Action {
            file: "docker://",
            inputs: Vec::new(),
            runs: Runs::Docker(Image {
                image: image.to_string(),
                build: None,
                entrypoint: None,
                args: Vec::new(),
                env: Vec::new(),
                pre: None,
                post: None,
                of_step: true,
            }),
            outputs: Vec::new(),
        };
        let path = live.workspace.clone();
        return started(
            step,
            name,
            job,
            live,
            step_env,
            groups,
            log,
            depth,
            uses,
            &action,
            &path,
            None,
            Work::Docker,
        );
    }
    let (path, within, remote) = match (uses::local(uses), uses::remote(uses)) {
        (Some(dir), _) => (
            live.workspace.join(dir),
            Within::Workspace(resolved(&live.workspace)),
            None,
        ),
        (None, Some(remote)) => {
            let actions = std::env::var_os(super::ENV_ACTIONS_DIR)
                .filter(|dir| !dir.is_empty())
                .map_or_else(|| std::path::PathBuf::from(ACTIONS_DIR), Into::into);
            let fetched = actions.join(remote.fetched_dir());
            if !fetched.is_dir() {
                // One an action's step uses is fetched where dagron-ci could
                // read the action's file as it handled the event.
                let why = if depth > 0 {
                    format!(
                        "{uses} was not fetched before the job started: dagron-ci fetches the \
                         actions an action uses where it can read the action's file (from a \
                         mirror, where CI_ACTIONS_MIRROR_RAW says where)"
                    )
                } else {
                    format!(
                        "{uses} was not fetched before the job started: a dagron-ci of before \
                         this one compiled it, or its executor fetches no actions"
                    )
                };
                return could_not(step, name, why, log, depth);
            }
            let path = match remote.path {
                "" => fetched.clone(),
                dir => fetched.join(dir),
            };
            (path, Within::Fetched(resolved(&fetched)), Some(remote))
        }
        (None, None) => {
            let why = format!("{uses:?} is not an action: ./ and a directory, or owner/repo@ref");
            return could_not(step, name, why, log, depth);
        }
    };
    let action = match read(&path, &within) {
        Ok(action) => action,
        Err(why) => return could_not(step, name, format!("{uses}: {why}"), log, depth),
    };
    let base = base_env(step_env, &path, remote);
    if let Runs::Docker(_) = &action.runs {
        return started(
            step,
            name,
            job,
            live,
            step_env,
            groups,
            log,
            depth,
            uses,
            &action,
            &path,
            remote,
            Work::Docker,
        );
    }
    // What runs, settled before anything does: a composite action's steps,
    // or the Node.js a JavaScript action runs on and each of its scripts,
    // found where its own file is.
    let work = match &action.runs {
        Runs::Composite(steps) => match plan(steps, step.n, &base) {
            Ok(planned) => Work::Steps(planned),
            Err(why) => {
                let why = format!("{uses}: {}: {why}", action.file);
                return could_not(step, name, why, log, depth);
            }
        },
        Runs::Node(scripts) => {
            let Some(node) = node_binary() else {
                let why = format!(
                    "{uses} is a JavaScript action, and this job has no Node.js to run it on: \
                     the engine installs one beside the step runner for a job whose steps use \
                     an action, and there is none there or on PATH"
                );
                return could_not(step, name, why, log, depth);
            };
            let found = |file: &String| script_path(&path, file, &within);
            let found = found(&scripts.main).and_then(|main| {
                Ok(Scripts {
                    main,
                    pre: scripts.pre.as_ref().map(found).transpose()?,
                    pre_if: scripts.pre_if.clone(),
                    post: scripts.post.as_ref().map(found).transpose()?,
                    post_if: scripts.post_if.clone(),
                })
            });
            match found {
                Ok(found) => Work::Node(node, found),
                Err(why) => {
                    let why = format!("{uses}: {}: {why}", action.file);
                    return could_not(step, name, why, log, depth);
                }
            }
        }
        Runs::Docker(_) => Work::Docker,
    };
    started(
        step, name, job, live, step_env, groups, log, depth, uses, &action, &path, remote, work,
    )
}

/// The action's run, once what runs is settled: its inputs read, and the
/// step said started, run and ended.
#[allow(clippy::too_many_arguments)]
fn started(
    step: &Step,
    name: &str,
    job: &Job,
    live: &mut Live,
    step_env: &[(String, String)],
    groups: &mut Vec<libc::pid_t>,
    log: &mut impl Write,
    depth: usize,
    uses: &str,
    action: &Action,
    path: &Path,
    remote: Option<uses::Remote<'_>>,
    work: Work,
) -> Went {
    let base = base_env(step_env, path, remote);
    let (inputs, mut undeclared) = match inputs(action, step, live, step_env, uses, depth == 0) {
        Ok(inputs) => inputs,
        Err(why) => return could_not(step, name, why, log, depth),
    };

    // A step's `docker://` declares none: each is one.
    if matches!(&action.runs, Runs::Docker(image) if image.of_step) {
        undeclared.clear();
    }
    if depth == 0 {
        marker(
            log,
            format_args!("step::{}::start::{}", step.n, Printable(name)),
        );
    } else {
        let _ = writeln!(log, "-- {}", Printable(name));
    }
    for given in &undeclared {
        marker(
            log,
            format_args!(
                "warning::step {}: {uses} declares no input {}, and is given it all the same",
                step.n,
                Printable(given)
            ),
        );
    }
    let started_at_ms = now_ms();
    let started = Instant::now();
    let doing = Doing {
        step,
        job,
        step_env,
        uses,
        deadline: step.timeout.map(|timeout| started + timeout),
        depth,
    };
    let done = match work {
        Work::Steps(planned) => composite(
            &doing,
            live,
            groups,
            log,
            planned,
            &action.outputs,
            inputs,
            path,
        ),
        Work::Node(node, scripts) => javascript(
            &doing, live, groups, log, &node, &scripts, &base, &inputs, name,
        ),
        Work::Docker => match &action.runs {
            Runs::Docker(image) => docker(&doing, live, groups, log, image, &base, inputs, name),
            _ => unreachable!("a Docker action's work is its image"),
        },
    };
    let code = match done.outcome {
        Outcome::Success => 0,
        _ => done.code,
    };

    if depth == 0 {
        marker(log, format_args!("step::{}::end::{code}", step.n));
    }
    let mut conclusion = done.outcome;
    if matches!(done.outcome, Outcome::Failure | Outcome::TimedOut) && step.continue_on_error {
        if depth == 0 {
            marker(log, format_args!("step::{}::continued", step.n));
        }
        conclusion = Outcome::Success;
    }
    record(live, step, done.outputs, done.outcome, conclusion);
    Went::Ran(
        StepResult {
            n: step.n,
            name: name.to_string(),
            outcome: done.outcome,
            conclusion,
            exit_code: Some(code),
            signal: done.signal,
            started_at_ms: Some(started_at_ms),
            duration_ms: Some(u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)),
            timeout_secs: step.timeout.map(|t| t.as_secs()),
        },
        done.cancelled,
    )
}

/// A composite action's steps, run in the step's place in a scope of the
/// action's own: its inputs, its steps, how they go. Its outputs are then
/// read against them.
#[allow(clippy::too_many_arguments)]
fn composite(
    doing: &Doing<'_>,
    live: &mut Live,
    groups: &mut Vec<libc::pid_t>,
    log: &mut impl Write,
    planned: Vec<(Step, Option<String>)>,
    declared: &[(String, String)],
    inputs: Vec<(String, serde_json::Value)>,
    path: &Path,
) -> Done {
    let (step, job, step_env, uses) = (doing.step, doing.job, doing.step_env, doing.uses);
    let saved_steps = std::mem::take(&mut live.steps);
    let saved_failed = std::mem::replace(&mut live.failed, false);
    let saved_context = live.context.clone();
    if !live.context.is_object() {
        live.context = serde_json::json!({});
    }
    live.context["inputs"] = serde_json::Value::Object(inputs.into_iter().collect());
    if !live.context["github"].is_object() {
        live.context["github"] = serde_json::json!({});
    }
    live.context["github"]["action_path"] = path.display().to_string().into();

    let mut code = None;
    let mut signal = None;
    let mut timed_out = false;
    let mut cancelled = None;
    for (inner, keep_going) in planned {
        if let Some(by) = take_cancel() {
            cancelled = Some(by);
            break;
        }
        // The step's time is the action's: each of its steps has what is
        // left of it.
        let Some(mut inner) = doing.timed(inner) else {
            timed_out = true;
            break;
        };
        if let Some(text) = keep_going {
            let seen = Seen {
                live,
                step_env,
                secrets: false,
            };
            match expr::template(&text).and_then(|t| t.eval(&seen)) {
                Ok(value) => inner.continue_on_error = value.truthy(),
                Err(e) => {
                    marker(
                        log,
                        format_args!(
                            "error::step {}: {uses}: continue-on-error: {}",
                            step.n,
                            Printable(&e.to_string())
                        ),
                    );
                    live.failed = true;
                    code.get_or_insert(1);
                    continue;
                }
            }
        }
        match one(&inner, job, live, groups, log, doing.depth + 1) {
            Went::Skipped => {}
            Went::Ran(result, by) => {
                match result.conclusion {
                    Outcome::Failure | Outcome::TimedOut => {
                        live.failed = true;
                        timed_out |= result.conclusion == Outcome::TimedOut;
                        code.get_or_insert(result.exit_code.unwrap_or(1).max(1));
                    }
                    Outcome::Cancelled => {
                        code.get_or_insert(result.exit_code.unwrap_or(1));
                        signal = result.signal;
                    }
                    _ => {}
                }
                if by.is_some() {
                    cancelled = by;
                    break;
                }
            }
        }
    }
    let outcome = if cancelled.is_some() {
        Outcome::Cancelled
    } else if timed_out {
        Outcome::TimedOut
    } else if live.failed {
        Outcome::Failure
    } else {
        Outcome::Success
    };
    // Its outputs, read against its steps.
    let mut outputs = Vec::new();
    for (output, text) in declared {
        let seen = Seen {
            live,
            step_env,
            secrets: false,
        };
        let value = expr::template(text).and_then(|t| t.text(&seen));
        outputs.push((
            output.clone(),
            value.unwrap_or_else(|e| {
                marker(
                    log,
                    format_args!(
                        "warning::step {}: {uses}: its output {} could not be read, and is \
                         empty: {}",
                        step.n,
                        Printable(output),
                        Printable(&e.to_string())
                    ),
                );
                String::new()
            }),
        ));
    }
    live.steps = saved_steps;
    live.failed = saved_failed;
    live.context = saved_context;
    Done {
        outcome,
        code: code.unwrap_or(1),
        signal,
        cancelled,
        outputs,
    }
}

/// A JavaScript action's scripts, on `node`: its `pre`, then its `main`,
/// whose outputs are the step's; its `post` is kept for when the job's
/// steps are over. Each runs whatever became of the job so far, but where
/// its own condition says otherwise: the step that uses the action ran.
#[allow(clippy::too_many_arguments)]
fn javascript(
    doing: &Doing<'_>,
    live: &mut Live,
    groups: &mut Vec<libc::pid_t>,
    log: &mut impl Write,
    node: &Path,
    scripts: &Scripts<std::path::PathBuf>,
    base: &[(OsString, OsString)],
    inputs: &[(String, serde_json::Value)],
    name: &str,
) -> Done {
    let step = doing.step;
    // Each input as `@actions/core` reads it, and which step this is.
    let mut env = base.to_vec();
    for (input, value) in inputs {
        let key = format!("INPUT_{}", input.replace(' ', "_").to_ascii_uppercase());
        env.push((key.into(), value.as_str().unwrap_or_default().into()));
    }
    let action = step
        .id
        .clone()
        .unwrap_or_else(|| format!("__run_{}", step.n));
    env.push(("GITHUB_ACTION".into(), action.into()));
    let script = |file: &Path, state: &[(String, String)], condition: Option<&String>| Step {
        n: step.n,
        name: file
            .file_name()
            .map(|f| f.to_string_lossy().into_owned())
            .unwrap_or_default(),
        argv: vec![node.display().to_string(), file.display().to_string()],
        env: env
            .iter()
            .cloned()
            .chain(
                state
                    .iter()
                    .map(|(k, v)| (format!("STATE_{k}").into(), v.into())),
            )
            .collect(),
        condition: Some(condition.cloned().unwrap_or_else(|| "always()".to_string())),
        ..Step::default()
    };
    let finished = |result: StepResult, by: Option<i32>, outputs| Done {
        outcome: result.conclusion,
        code: result.exit_code.unwrap_or(1),
        signal: result.signal,
        cancelled: by,
        outputs,
    };

    // Its `pre`, and what it keeps for `main`.
    let mut state = Vec::new();
    if let Some(pre) = &scripts.pre {
        let Some(inner) = doing.timed(script(pre, &[], scripts.pre_if.as_ref())) else {
            return Done::timed_out(Vec::new());
        };
        if let Went::Ran(result, by) = one(&inner, doing.job, live, groups, log, doing.depth + 1) {
            if by.is_some() || result.conclusion != Outcome::Success {
                return finished(result, by, Vec::new());
            }
            state = std::mem::take(&mut live.wrote.state);
        }
    }
    // Its `main`, whose outputs are the step's.
    let Some(inner) = doing.timed(script(&scripts.main, &state, None)) else {
        return Done::timed_out(Vec::new());
    };
    let Went::Ran(result, by) = one(&inner, doing.job, live, groups, log, doing.depth + 1) else {
        unreachable!("main runs always()");
    };
    let outputs = std::mem::take(&mut live.wrote.outputs);
    let kept = std::mem::take(&mut live.wrote.state);
    // Its `post`, when the job's steps are over, with what `main` kept.
    if let Some(post) = &scripts.post {
        let mut later = script(post, &kept, scripts.post_if.as_ref());
        later.name = format!("Post {name}");
        live.posts.push(later);
    }
    finished(result, by, outputs)
}

/// A Docker action: its `pre-entrypoint`, then its image's run, whose
/// outputs are the step's; its `post-entrypoint` is kept for when the job's
/// steps are over. Each is a run of this binary's `container`, which asks
/// the engine for the container and says what it said.
#[allow(clippy::too_many_arguments)]
fn docker(
    doing: &Doing<'_>,
    live: &mut Live,
    groups: &mut Vec<libc::pid_t>,
    log: &mut impl Write,
    image: &Image,
    base: &[(OsString, OsString)],
    inputs: Vec<(String, serde_json::Value)>,
    name: &str,
) -> Done {
    let step = doing.step;
    fn failed(doing: &Doing<'_>, why: String, log: &mut impl Write) -> Done {
        marker(
            log,
            format_args!(
                "error::step {}: {}: {}",
                doing.step.n,
                doing.uses,
                Printable(&why)
            ),
        );
        Done {
            outcome: Outcome::Failure,
            code: 1,
            signal: None,
            cancelled: None,
            outputs: Vec::new(),
        }
    }
    let given = |key: &str| {
        inputs
            .iter()
            .find(|(input, _)| input.eq_ignore_ascii_case(key))
            .and_then(|(_, v)| v.as_str().map(str::to_string))
            .filter(|v| !v.is_empty())
    };
    // What it is run with: a step's `with:`, or the action's own text, read
    // against its inputs.
    let read = match settle(doing, live, image, &inputs, given) {
        Ok(read) => read,
        Err(why) => return failed(doing, why, log),
    };
    // Its variables: what a script of the step would have, each input, and
    // the action's own.
    let mut env: Vec<(String, String)> = super::runner_env(doing.job, live);
    // The workflow's and the job's `env:`, which a script step has from the
    // step runner's own variables, and a container does not.
    if let Some(job_env) = live
        .context
        .get("env")
        .and_then(serde_json::Value::as_object)
    {
        env.extend(
            job_env
                .iter()
                .filter_map(|(k, v)| v.as_str().map(|v| (k.clone(), v.to_string()))),
        );
    }
    env.extend(live.env.iter().map(|(k, v)| (k.clone(), v.clone())));
    env.extend(base.iter().map(|(k, v)| {
        (
            k.to_string_lossy().into_owned(),
            v.to_string_lossy().into_owned(),
        )
    }));
    for (input, value) in &inputs {
        if image.of_step && ["args", "entrypoint"].contains(&input.to_ascii_lowercase().as_str()) {
            continue;
        }
        let key = format!("INPUT_{}", input.replace(' ', "_").to_ascii_uppercase());
        env.push((key, value.as_str().unwrap_or_default().to_string()));
    }
    let action = step
        .id
        .clone()
        .unwrap_or_else(|| format!("__run_{}", step.n));
    env.push(("GITHUB_ACTION".into(), action));
    env.extend(read.env);
    let finished = |result: StepResult, by: Option<i32>, outputs| Done {
        outcome: result.conclusion,
        code: result.exit_code.unwrap_or(1),
        signal: result.signal,
        cancelled: by,
        outputs,
    };
    let run = |live: &mut Live,
               entrypoint: Option<&String>,
               args: &[String],
               state: &[(String, String)],
               what: &str,
               condition: Option<String>|
     -> Result<Step, String> {
        let mut env = env.clone();
        env.extend(state.iter().map(|(k, v)| (format!("STATE_{k}"), v.clone())));
        let timeout_secs = doing
            .deadline
            .and_then(|deadline| deadline.checked_duration_since(Instant::now()))
            .map(|left| left.as_secs().max(1));
        let spec = crate::container::Spec {
            image: image.image.clone(),
            build: image.build.as_deref().map(|file| crate::container::Build {
                context: file
                    .parent()
                    .unwrap_or_else(|| Path::new("/"))
                    .display()
                    .to_string(),
                dockerfile: file
                    .file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .into_owned(),
            }),
            entrypoint: entrypoint.cloned(),
            args: args.to_vec(),
            env: crate::container::last_of_each(env),
            timeout_secs,
        };
        live.scripts += 1;
        let file = live
            .dir
            .join(format!("step-{}-{}.container.json", step.n, live.scripts));
        std::fs::write(&file, spec.to_json())
            .map_err(|e| format!("what its container is run with could not be written: {e}"))?;
        let me = std::env::current_exe()
            .map_err(|e| format!("the step runner could not find itself: {e}"))?;
        Ok(Step {
            n: step.n,
            name: what.to_string(),
            argv: vec![
                me.display().to_string(),
                "container".to_string(),
                file.display().to_string(),
            ],
            condition: Some(condition.unwrap_or_else(|| "always()".to_string())),
            ..Step::default()
        })
    };

    // Its `pre-entrypoint`, and what it keeps.
    let mut state = Vec::new();
    if let Some(pre) = &read.pre {
        let inner = match run(live, Some(pre), &[], &[], &format!("Pre {name}"), None) {
            Ok(inner) => inner,
            Err(why) => return failed(doing, why, log),
        };
        let Some(inner) = doing.timed(inner) else {
            return Done::timed_out(Vec::new());
        };
        if let Went::Ran(result, by) = one(&inner, doing.job, live, groups, log, doing.depth + 1) {
            if by.is_some() || result.conclusion != Outcome::Success {
                return finished(result, by, Vec::new());
            }
            state = std::mem::take(&mut live.wrote.state);
        }
    }
    // Its image's run, whose outputs are the step's.
    let inner = match run(
        live,
        read.entrypoint.as_ref(),
        &read.args,
        &state,
        name,
        None,
    ) {
        Ok(inner) => inner,
        Err(why) => return failed(doing, why, log),
    };
    let Some(inner) = doing.timed(inner) else {
        return Done::timed_out(Vec::new());
    };
    let Went::Ran(result, by) = one(&inner, doing.job, live, groups, log, doing.depth + 1) else {
        unreachable!("its run is always()");
    };
    let outputs = std::mem::take(&mut live.wrote.outputs);
    let kept = std::mem::take(&mut live.wrote.state);
    // Its `post-entrypoint`, when the job's steps are over.
    if let Some(post) = &read.post {
        match run(
            live,
            Some(post),
            &[],
            &kept,
            &format!("Post {name}"),
            Some("always()".into()),
        ) {
            Ok(later) => live.posts.push(later),
            Err(why) => {
                marker(
                    log,
                    format_args!(
                        "warning::step {}: {}: its post-entrypoint will not run: {}",
                        step.n,
                        doing.uses,
                        Printable(&why)
                    ),
                );
            }
        }
    }
    finished(result, by, outputs)
}

/// What a Docker action's image is run with, its text read.
struct Settled {
    entrypoint: Option<String>,
    args: Vec<String>,
    env: Vec<(String, String)>,
    pre: Option<String>,
    post: Option<String>,
}

/// Read what `image` is run with against its inputs: a step's `with.args`,
/// split as a shell splits words, and `with.entrypoint`; or the action's
/// `runs.args`, each one argument, and the rest of its text.
fn settle(
    doing: &Doing<'_>,
    live: &mut Live,
    image: &Image,
    inputs: &[(String, serde_json::Value)],
    given: impl Fn(&str) -> Option<String>,
) -> Result<Settled, String> {
    if image.of_step {
        let args = match given("args") {
            Some(text) => crate::container::words(&text)?,
            None => Vec::new(),
        };
        return Ok(Settled {
            entrypoint: given("entrypoint"),
            args,
            env: Vec::new(),
            pre: None,
            post: None,
        });
    }
    let saved = live.context.clone();
    if !live.context.is_object() {
        live.context = serde_json::json!({});
    }
    live.context["inputs"] = serde_json::Value::Object(inputs.iter().cloned().collect());
    let read = |live: &Live, what: &str, text: &str| -> Result<String, String> {
        let seen = Seen {
            live,
            step_env: doing.step_env,
            secrets: false,
        };
        expr::template(text)
            .and_then(|t| t.text(&seen))
            .map_err(|e| format!("runs.{what}: {e}"))
    };
    let settled = (|| {
        let args = image
            .args
            .iter()
            .map(|arg| read(live, "args", arg))
            .collect::<Result<Vec<_>, _>>()?;
        let env = image
            .env
            .iter()
            .map(|(k, v)| read(live, &format!("env.{k}"), v).map(|v| (k.clone(), v)))
            .collect::<Result<Vec<_>, _>>()?;
        let entrypoint = image
            .entrypoint
            .as_deref()
            .map(|e| read(live, "entrypoint", e))
            .transpose()?;
        let pre = image
            .pre
            .as_deref()
            .map(|e| read(live, "pre-entrypoint", e))
            .transpose()?;
        let post = image
            .post
            .as_deref()
            .map(|e| read(live, "post-entrypoint", e))
            .transpose()?;
        Ok(Settled {
            entrypoint,
            args,
            env,
            pre,
            post,
        })
    })();
    live.context = saved;
    settled
}

/// The Node.js a JavaScript action runs on: the one installed beside this
/// binary (`install --with node`), else one on `PATH`.
fn node_binary() -> Option<std::path::PathBuf> {
    use std::os::unix::fs::PermissionsExt;
    let runs = |path: &Path| {
        std::fs::metadata(path).is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
    };
    let beside = std::env::current_exe()
        .ok()
        .and_then(|me| me.parent().map(|dir| dir.join("node/bin/node")))
        .filter(|node| runs(node));
    beside.or_else(|| {
        std::env::var_os("PATH").and_then(|path| {
            std::env::split_paths(&path)
                .map(|dir| dir.join("node"))
                .find(|node| runs(node))
        })
    })
}

/// A JavaScript action's script `file`, a path of its directory `dir`,
/// where it leads once every link on the way is followed: only where that
/// is still `within` where the action's file must be.
fn script_path(dir: &Path, file: &str, within: &Within) -> Result<std::path::PathBuf, String> {
    let path = dir
        .join(file)
        .canonicalize()
        .map_err(|_| format!("its script {file} is not in its directory"))?;
    let root = match within {
        Within::Workspace(root) | Within::Fetched(root) => root,
    };
    if !path.starts_with(root) || !path.is_file() {
        return Err(format!(
            "its script {file} is no file of the workspace or of the repository the action \
             was fetched from"
        ));
    }
    Ok(path)
}

/// A Docker action's Dockerfile, `named` by its `runs.image` as a path of
/// its directory `dir`: where it leads once every link on the way is
/// followed, only where that is still `within` where the action's file must
/// be.
fn dockerfile(dir: &Path, named: &str, within: &Within) -> Result<std::path::PathBuf, String> {
    let neither = || {
        format!(
            "its runs.image is {named:?}, which is neither an image a registry has \
             (docker://image) nor a Dockerfile of its directory"
        )
    };
    let named = named.trim();
    if named.is_empty() || Path::new(named).is_absolute() {
        return Err(neither());
    }
    let path = dir.join(named).canonicalize().map_err(|_| neither())?;
    let root = match within {
        Within::Workspace(root) | Within::Fetched(root) => root,
    };
    if !path.starts_with(root) || !path.is_file() {
        return Err(neither());
    }
    Ok(path)
}

/// Where an action's file must still be once the links on the way to it
/// are followed, as those of the place itself resolve.
enum Within {
    /// The workspace: a local action's.
    Workspace(std::path::PathBuf),
    /// The directory an action of another repository was fetched into.
    Fetched(std::path::PathBuf),
}

/// The action in `dir`, read from its file. The file is read where its
/// path leads once every link on the way is followed, and only where that
/// is still `within` where it should be: a link of the repository's may not
/// make an action of a file the commit does not hold.
fn read(dir: &Path, within: &Within) -> Result<Action, String> {
    let mut found = None;
    for file in ["action.yml", "action.yaml"] {
        if let Ok(path) = dir.join(file).canonicalize() {
            found = Some((file, path));
            break;
        }
    }
    let Some((file, path)) = found else {
        return Err(
            "there is no action.yml or action.yaml in its directory, in the commit being run"
                .into(),
        );
    };
    match within {
        Within::Workspace(root) if !path.starts_with(root) => {
            return Err(format!(
                "{file} leads out of the job's workspace, and an action is the workspace's own"
            ))
        }
        Within::Fetched(root) if !path.starts_with(root) => {
            return Err(format!(
                "{file} leads out of the repository the action was fetched from"
            ))
        }
        _ => {}
    }
    let opened = crate::report::open_file(&path).map_err(|e| format!("{file}: {e}"))?;
    let mut bytes = Vec::new();
    opened
        .take(MAX_ACTION_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| format!("{file} could not be read: {e}"))?;
    if bytes.len() as u64 > MAX_ACTION_BYTES {
        return Err(format!(
            "{file} is more than {} KiB",
            MAX_ACTION_BYTES >> 10
        ));
    }
    let text = String::from_utf8(bytes).map_err(|_| format!("{file} is not UTF-8"))?;
    let mut yaml: Yaml =
        serde_yaml::from_str(&text).map_err(|e| format!("{file} is not YAML: {e}"))?;
    yaml.apply_merge()
        .map_err(|e| format!("{file} is not YAML: {e}"))?;
    let at = |what: &str| format!("{file}: {what}");

    let runs = yaml
        .get("runs")
        .ok_or_else(|| at("it has no runs:, which says how the action is run"))?;
    let using = runs
        .get("using")
        .and_then(Yaml::as_str)
        .unwrap_or_default()
        .to_ascii_lowercase();
    let runs = match using.as_str() {
        "composite" => Runs::Composite(match runs.get("steps") {
            None | Some(Yaml::Null) => Vec::new(),
            Some(Yaml::Sequence(steps)) if steps.len() <= crate::protocol::MAX_STEPS => {
                steps.clone()
            }
            Some(Yaml::Sequence(_)) => {
                return Err(at(&format!(
                    "runs.steps has more than {} steps",
                    crate::protocol::MAX_STEPS
                )))
            }
            Some(_) => return Err(at("runs.steps is not a list")),
        }),
        "node12" | "node16" | "node20" | "node24" => {
            let text = |key: &str| {
                runs.get(key)
                    .and_then(scalar)
                    .filter(|text| !text.trim().is_empty())
            };
            Runs::Node(Scripts {
                main: text("main").ok_or_else(|| {
                    at("its runs names no main: a JavaScript action names the script it runs")
                })?,
                pre: text("pre"),
                pre_if: text("pre-if"),
                post: text("post"),
                post_if: text("post-if"),
            })
        }
        "docker" => {
            let text = |key: &str| {
                runs.get(key)
                    .and_then(scalar)
                    .filter(|text| !text.trim().is_empty())
            };
            let named = text("image").unwrap_or_default();
            let (image, build) = match uses::docker(&named) {
                Some(image) => (image.to_string(), None),
                None => (
                    String::new(),
                    Some(dockerfile(dir, &named, within).map_err(|why| at(&why))?),
                ),
            };
            let args = match runs.get("args") {
                None | Some(Yaml::Null) => Vec::new(),
                Some(Yaml::Sequence(args)) => args
                    .iter()
                    .map(|arg| scalar(arg).ok_or_else(|| at("runs.args is a list of text")))
                    .collect::<Result<_, _>>()?,
                Some(_) => return Err(at("runs.args is a list of text")),
            };
            let env = mapping(runs, "env")
                .map_err(|why| at(&format!("runs.{why}")))?
                .into_iter()
                .map(|(k, v)| {
                    scalar(&v)
                        .map(|v| (k.clone(), v))
                        .ok_or_else(|| at(&format!("runs.env.{k} is not text")))
                })
                .collect::<Result<_, _>>()?;
            Runs::Docker(Image {
                image,
                build,
                entrypoint: text("entrypoint"),
                args,
                env,
                pre: text("pre-entrypoint"),
                post: text("post-entrypoint"),
                of_step: false,
            })
        }
        other => {
            return Err(at(&format!(
                "it is an action of runs.using {other:?}: dagron-ci runs a composite action, a \
                 JavaScript one and one of a Docker image"
            )))
        }
    };
    let mut inputs = Vec::new();
    for (name, input) in mapping(&yaml, "inputs").map_err(|why| at(&why))? {
        let default = match input.get("default") {
            None => None,
            Some(default) => Some(
                scalar(default).ok_or_else(|| at(&format!("inputs.{name}.default is not text")))?,
            ),
        };
        inputs.push((name, default));
    }
    let mut outputs = Vec::new();
    if let Runs::Composite(_) = runs {
        for (name, output) in mapping(&yaml, "outputs").map_err(|why| at(&why))? {
            let value = output.get("value").and_then(scalar).ok_or_else(|| {
                at(&format!(
                    "outputs.{name} has no value: a composite action's output says what it is \
                     read from"
                ))
            })?;
            outputs.push((name, value));
        }
    }
    Ok(Action {
        file,
        inputs,
        runs,
        outputs,
    })
}

/// The entries of the mapping `key` of `yaml`, by their names: none where
/// it has no such key.
fn mapping(yaml: &Yaml, key: &str) -> Result<Vec<(String, Yaml)>, String> {
    match yaml.get(key) {
        None | Some(Yaml::Null) => Ok(Vec::new()),
        Some(Yaml::Mapping(map)) => map
            .iter()
            .map(|(name, value)| match scalar(name) {
                Some(name) if !name.is_empty() => Ok((name, value.clone())),
                _ => Err(format!("{key} has a key that is not a name")),
            })
            .collect(),
        Some(_) => Err(format!("{key} is not a mapping")),
    }
}

/// A value that is text, a number, true or false, or nothing, as text.
fn scalar(value: &Yaml) -> Option<String> {
    match value {
        Yaml::Null => Some(String::new()),
        Yaml::Bool(b) => Some(b.to_string()),
        Yaml::Number(n) => Some(n.to_string()),
        Yaml::String(s) => Some(s.clone()),
        Yaml::Tagged(tagged) => scalar(&tagged.value),
        Yaml::Sequence(_) | Yaml::Mapping(_) => None,
    }
}

/// The variables each of the action's steps starts with: the step's own,
/// where the action is, and, for one of another repository, which and at
/// what ref, as GitHub's runner says them.
fn base_env(
    step_env: &[(String, String)],
    path: &Path,
    remote: Option<uses::Remote<'_>>,
) -> Vec<(OsString, OsString)> {
    const OWN: [&str; 3] = [
        "GITHUB_ACTION_PATH",
        "GITHUB_ACTION_REPOSITORY",
        "GITHUB_ACTION_REF",
    ];
    let mut base: Vec<(OsString, OsString)> = step_env
        .iter()
        .filter(|(key, _)| !OWN.contains(&key.as_str()))
        .map(|(key, value)| (key.into(), value.into()))
        .collect();
    base.push(("GITHUB_ACTION_PATH".into(), path.as_os_str().to_owned()));
    let (repository, git_ref) = remote
        .map(|r| (r.repository(), r.git_ref.to_string()))
        .unwrap_or_default();
    base.push(("GITHUB_ACTION_REPOSITORY".into(), repository.into()));
    base.push(("GITHUB_ACTION_REF".into(), git_ref.into()));
    base
}

/// The action's steps as the step runner runs them, each with its
/// `continue-on-error:` where that is an expression; or why one of them
/// cannot be run. They have the number of the step that uses the action:
/// what one says is said within that step.
fn plan(
    steps: &[Yaml],
    n: usize,
    base: &[(OsString, OsString)],
) -> Result<Vec<(Step, Option<String>)>, String> {
    let mut out = Vec::new();
    for (i, raw) in steps.iter().enumerate() {
        let at = format!("runs.steps[{i}]");
        if !raw.is_mapping() {
            return Err(format!("{at} is not a mapping"));
        }
        let text = |key: &str| -> Result<Option<String>, String> {
            match raw.get(key) {
                None => Ok(None),
                Some(value) => scalar(value)
                    .map(Some)
                    .ok_or_else(|| format!("{at}.{key} is not text")),
            }
        };
        let mut inner = Step {
            n,
            env: base.to_vec(),
            id: text("id")?.filter(|id| !id.is_empty()),
            condition: text("if")?.filter(|c| !c.trim().is_empty()),
            workdir: text("working-directory")?.filter(|d| !d.is_empty()),
            ..Step::default()
        };
        let mut keep_going = None;
        match raw.get("continue-on-error") {
            None | Some(Yaml::Null) => {}
            Some(Yaml::Bool(b)) => inner.continue_on_error = *b,
            Some(Yaml::String(s)) if s.trim() == "true" || s.trim() == "false" => {
                inner.continue_on_error = s.trim() == "true";
            }
            Some(Yaml::String(s)) if s.contains("${{") => keep_going = Some(s.clone()),
            Some(_) => {
                return Err(format!(
                    "{at}.continue-on-error is not true, false or an expression"
                ))
            }
        }
        for (key, value) in mapping(raw, "env").map_err(|why| format!("{at}.{why}"))? {
            if !is_variable_name(&key) {
                return Err(format!("{at}.env.{key} is not a variable's name"));
            }
            let value = scalar(&value).ok_or_else(|| format!("{at}.env.{key} is not text"))?;
            // Each is evaluated as the step starts: the step's own are
            // values by then, and are not read again.
            inner.late_env.push((key, value));
        }
        match (raw.get("run"), raw.get("uses")) {
            (Some(run), None) => {
                let run = scalar(run)
                    .filter(|run| !run.trim().is_empty())
                    .ok_or_else(|| format!("{at}.run is not a script"))?;
                let shell = text("shell")?
                    .filter(|shell| !shell.trim().is_empty())
                    .ok_or_else(|| {
                        format!(
                            "{at} has no shell: a run step of a composite action names its \
                             shell, as on GitHub"
                        )
                    })?;
                let reads_variables = matches!(shell.as_str(), "bash" | "sh");
                let (script, exprs) = expr::script(&run, reads_variables)
                    .map_err(|e| format!("{at}.run: {e}"))?
                    .ok_or_else(|| {
                        format!(
                            "{at}.run holds an expression whose value an outsider may choose \
                             (an input's is one), in a script for a shell that is not sh or \
                             bash: pass it through the step's env:, and read the variable"
                        )
                    })?;
                let first = run.lines().next().unwrap_or_default().trim().to_string();
                inner.name = text("name")?.unwrap_or_else(|| format!("Run {first}"));
                inner.run = script;
                inner.exprs = exprs;
                inner.shell = Some(shell);
            }
            (None, Some(used)) => {
                let used = scalar(used).unwrap_or_default();
                // A local action, one of another repository (which dagron-ci
                // fetched before the job started, as it read this action), or
                // an image: each run as a workflow's own step's.
                if uses::local(&used).is_none()
                    && uses::remote(&used).is_none()
                    && uses::docker(&used).is_none()
                {
                    return Err(format!(
                        "{at} uses {used:?}, which is no action: ./ and a directory, \
                         owner/repo@ref, or docker:// and an image"
                    ));
                }
                let uses = used;
                for (key, value) in mapping(raw, "with").map_err(|why| format!("{at}.{why}"))? {
                    let value =
                        scalar(&value).ok_or_else(|| format!("{at}.with.{key} is not text"))?;
                    inner.with.push((key, value));
                }
                inner.name = text("name")?.unwrap_or_else(|| format!("Run {uses}"));
                inner.uses = Some(uses);
            }
            (Some(_), Some(_)) => return Err(format!("{at} has run and uses: a step is one")),
            (None, None) => return Err(format!("{at} has neither run nor uses")),
        }
        out.push((inner, keep_going));
    }
    Ok(out)
}

/// The action's inputs: what the step gives it, and the default of each
/// it is not given, read as the step's own expressions are. Then the
/// names of those the step gives it that it does not declare. What the
/// step gives it (`with:`) reads `secrets` where the step is one of the
/// job's own (`secrets`); a default, the action's own words, never does.
#[allow(clippy::type_complexity)]
fn inputs(
    action: &Action,
    step: &Step,
    live: &Live,
    step_env: &[(String, String)],
    uses: &str,
    secrets: bool,
) -> Result<(Vec<(String, serde_json::Value)>, Vec<String>), String> {
    let given_seen = Seen {
        live,
        step_env,
        secrets,
    };
    let own_seen = Seen {
        live,
        step_env,
        secrets: false,
    };
    let read = |text: &str| -> Result<String, String> {
        expr::template(text)
            .and_then(|t| t.text(&given_seen))
            .map_err(|e| e.to_string())
    };
    let read_own = |text: &str| -> Result<String, String> {
        expr::template(text)
            .and_then(|t| t.text(&own_seen))
            .map_err(|e| e.to_string())
    };
    let mut out = Vec::new();
    for (name, default) in &action.inputs {
        let given = step
            .with
            .iter()
            .find(|(given, _)| given.eq_ignore_ascii_case(name));
        let value = match (given, default) {
            (Some((given, text)), _) => read(text).map_err(|why| format!("with.{given}: {why}"))?,
            (None, Some(default)) => {
                read_own(default).map_err(|why| format!("{uses}: inputs.{name}.default: {why}"))?
            }
            (None, None) => String::new(),
        };
        out.push((name.clone(), serde_json::Value::String(value)));
    }
    let mut undeclared = Vec::new();
    for (given, text) in &step.with {
        if action
            .inputs
            .iter()
            .any(|(name, _)| name.eq_ignore_ascii_case(given))
        {
            continue;
        }
        let value = read(text).map_err(|why| format!("with.{given}: {why}"))?;
        out.push((given.clone(), serde_json::Value::String(value)));
        undeclared.push(given.clone());
    }
    Ok((out, undeclared))
}
