//! `dagron-ci run`: a checkout's pipelines, here and now, on an engine of
//! their own.
//!
//! The run a push would start. The event is read from the checkout: its
//! branch, its commit, its `origin`, and for a paths filter the files the
//! working tree changed. Each pipeline compiles as the event workers compile
//! it, and each one that runs is handed to a `dagron` engine started for it
//! alone: a one-shot run on a SQLite file in a directory of its own, on the
//! Docker executor with the working tree mounted into every task at the path
//! it has here, or with `--executor local`, the tasks as processes in the
//! working tree. The engine's database is the record: it is read as the run
//! goes, each task's log is printed split by step, and the command exits
//! with the run's result.
//!
//! dagron-ci never links the engine (dagron-core takes one database backend
//! per build, and dagron-ci serves both), so it drives the `dagron` binary,
//! found beside `dagron-ci` or on `PATH`, and reads the tables it writes:
//! the two ship together.
//!
//! The engine runs with this process's environment less every setting of its
//! own: each name `dagron config` lists, and every `DAGRON_` variable. A
//! setting from a shell could make a one-shot run resident (`API_ADDR`), read
//! another configuration (`DAGRON_CONFIG`), never claim a task
//! (`RUNNER_CLASSES`), or reach past the run (`DAGRON_INSTALLATION` arms a
//! sweep that removes containers this run did not start; `GITHUB_TOKEN`
//! posts commit statuses). On the local executor a task inherits what is
//! left.

use std::collections::{BTreeMap, BTreeSet};
use std::io::{IsTerminal, Write};
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode, Stdio};
use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::Notify;

use serde_yaml::Value as Yaml;
use sqlx::sqlite::{SqliteConnectOptions, SqlitePool, SqlitePoolOptions};
use sqlx::Row;

use crate::cli::EventFlags;
use crate::pipeline::compile::{self, Changes, Event, Outcome, Settings};
use crate::pipeline::github;

/// A line on stdout. A reader that went away (`| head`) costs the lines, and
/// never the run: the engine still finishes, or is stopped, and the exit
/// code still says how it went.
macro_rules! say {
    ($($arg:tt)*) => {{
        let _ = writeln!(std::io::stdout(), $($arg)*);
    }};
}

/// A line on stderr, likewise.
macro_rules! report {
    ($($arg:tt)*) => {{
        let _ = writeln!(std::io::stderr(), $($arg)*);
    }};
}

const USAGE: &str =
    "usage: dagron-ci run [FILE]... [--executor docker|local] [--engine PATH] [--keep]
                     [--report FILE] [--docker-daemon]
                     [--event push|tag|pull_request|merge_group|dispatch] [--ref REF]
                     [--sha SHA] [--base-ref BRANCH] [--base-sha SHA] [--pr N]
                     [--repo HOST/OWNER/NAME] [--input NAME=VALUE]... [--changed PATH]...";

/// What a task runs in on the Docker executor when it names no image: the
/// engine's own default, named here so that it can be pulled first.
const DEFAULT_IMAGE: &str = "alpine:latest";

/// Where a task finds dagron-ci-exec on the Docker executor: where the
/// compiled command looks first.
const EXEC_IN_TASK: &str = "/dagron/bin/dagron-ci-exec";

/// How often the run's database is read.
const TICK: Duration = Duration::from_millis(250);

/// How long a stopped engine has to exit before its process group is killed.
const STOP_GRACE: Duration = Duration::from_secs(20);

/// The label the Docker executor puts on each task's container: the run's id.
const RUN_LABEL: &str = "dagron.dev/run-id";

/// Where the tasks run.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Executor {
    /// A container per task, from the task's `docker_image`, with the
    /// working tree mounted.
    Docker,
    /// A process per task, in the working tree.
    Local,
}

impl Executor {
    fn name(self) -> &'static str {
        match self {
            Executor::Docker => "docker",
            Executor::Local => "local",
        }
    }
}

/// `dagron-ci run`'s flags.
#[derive(Debug, PartialEq, Eq)]
struct Options<'a> {
    files: Vec<&'a str>,
    flags: EventFlags,
    executor: Executor,
    engine: Option<&'a str>,
    keep: bool,
    /// Where each pipeline's result, and each of its jobs', is written as
    /// JSON when the run is over (`--report`).
    report: Option<&'a str>,
    /// Whether a job that asks for a Docker daemon, a privileged container
    /// on this machine's Docker host, is given one (`--docker-daemon`).
    docker_daemon: bool,
    help: bool,
}

fn options<'a>(args: &'a [&'a str]) -> Result<Options<'a>, String> {
    let mut opts = Options {
        files: Vec::new(),
        flags: EventFlags::default(),
        executor: Executor::Docker,
        engine: None,
        keep: false,
        report: None,
        docker_daemon: false,
        help: false,
    };
    let mut rest = args;
    loop {
        if let Some(tail) = opts.flags.take(rest)? {
            rest = tail;
            continue;
        }
        rest = match rest {
            [] => return Ok(opts),
            ["--executor", "docker", tail @ ..] => {
                opts.executor = Executor::Docker;
                tail
            }
            ["--executor", "local", tail @ ..] => {
                opts.executor = Executor::Local;
                tail
            }
            ["--executor", other, ..] => {
                return Err(format!("--executor is docker or local, not {other:?}"))
            }
            ["--engine", path, tail @ ..] => {
                opts.engine = Some(path);
                tail
            }
            ["--keep", tail @ ..] => {
                opts.keep = true;
                tail
            }
            ["--report", path, tail @ ..] => {
                opts.report = Some(path);
                tail
            }
            ["--docker-daemon", tail @ ..] => {
                opts.docker_daemon = true;
                tail
            }
            ["--help" | "-h", tail @ ..] => {
                opts.help = true;
                tail
            }
            [path, tail @ ..] if !path.starts_with("--") => {
                opts.files.push(path);
                tail
            }
            [other, ..] => return Err(format!("{other:?} is not a flag run takes")),
        };
    }
}

/// What git says of the checkout the command was started in.
#[derive(Debug, Default, PartialEq, Eq)]
struct Checkout {
    /// The working tree's top; outside git, the current directory.
    root: PathBuf,
    git: bool,
    /// HEAD's commit; `None` before the first one.
    head: Option<String>,
    /// The branch HEAD is on, `refs/heads/…`; `None` when it is detached.
    branch: Option<String>,
    /// `origin` as a forge's host and a repository's full name.
    origin: Option<(String, String)>,
}

/// `git ARGS` in `dir`: its output, trimmed, when it succeeds.
fn git(dir: &Path, args: &[&str]) -> Option<String> {
    let out = Command::new("git")
        .args(args)
        .current_dir(dir)
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    out.status
        .success()
        .then(|| String::from_utf8_lossy(&out.stdout).trim().to_string())
}

/// `git ARGS -z` in `dir`: the paths it lists.
fn git_paths(dir: &Path, args: &[&str]) -> Option<Vec<String>> {
    let out = Command::new("git")
        .args(args)
        .current_dir(dir)
        .stdin(Stdio::null())
        .stderr(Stdio::null())
        .output()
        .ok()?;
    out.status.success().then(|| {
        out.stdout
            .split(|b| *b == 0)
            .filter(|p| !p.is_empty())
            .map(|p| String::from_utf8_lossy(p).into_owned())
            .collect()
    })
}

fn checkout(cwd: &Path) -> Checkout {
    let Some(root) = git(cwd, &["rev-parse", "--show-toplevel"]) else {
        return Checkout {
            root: cwd.to_path_buf(),
            ..Checkout::default()
        };
    };
    let root = PathBuf::from(root);
    Checkout {
        head: git(&root, &["rev-parse", "--verify", "-q", "HEAD"]),
        branch: git(&root, &["symbolic-ref", "-q", "HEAD"]),
        origin: git(&root, &["remote", "get-url", "origin"]).and_then(|url| forge_of(&url)),
        git: true,
        root,
    }
}

/// A remote's URL as a forge's host and a repository's full name:
/// `https://github.com/acme/widgets.git`, `git@github.com:acme/widgets`,
/// `ssh://git@gitlab.example:2222/group/sub/project.git`. An HTTP(S) URL
/// keeps its port, the web server's; an SSH one drops it, since it is
/// SSH's. `None` for what names no forge: a path, a `file://` URL.
fn forge_of(url: &str) -> Option<(String, String)> {
    let url = url.trim();
    let (authority, path, keep_port) = if let Some((scheme, rest)) = url.split_once("://") {
        let (authority, path) = rest.split_once('/')?;
        match scheme {
            "https" | "http" => (authority, path, true),
            "ssh" | "git" | "git+ssh" | "ssh+git" => (authority, path, false),
            _ => return None,
        }
    } else {
        // scp-like: [user@]host:path, with no slash before the colon.
        let (authority, path) = url.split_once(':')?;
        if authority.contains('/') || authority.is_empty() {
            return None;
        }
        (authority, path, false)
    };
    let host = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    let host = if keep_port {
        host
    } else {
        host.split_once(':').map_or(host, |(h, _)| h)
    };
    let path = path.trim_matches('/');
    let path = path.strip_suffix(".git").unwrap_or(path);
    let spec = format!("{host}/{path}");
    let (host, name) = crate::cli::split_spec(&spec).ok()?;
    Some((host.to_ascii_lowercase(), name.to_ascii_lowercase()))
}

/// The event the run is for: the flags, and the checkout for what they
/// leave out. A push of the branch HEAD is on, at HEAD, by default.
fn event(flags: &EventFlags, co: &Checkout) -> Result<Event, String> {
    let kind = flags.kind.as_deref().unwrap_or("push");
    if flags.git_ref.is_none() {
        if kind == "tag" {
            return Err("a tag event needs --ref refs/tags/NAME".into());
        }
        if co.branch.is_none() {
            return Err(if co.git {
                "HEAD is not on a branch: name the ref the run is for with --ref".into()
            } else {
                "this is not a git checkout: name the ref the run is for with --ref".into()
            });
        }
    }
    let (forge_host, repo) = co
        .origin
        .clone()
        .unwrap_or_else(|| ("localhost".into(), "local/checkout".into()));
    flags.event(Event {
        kind: kind.into(),
        git_ref: co.branch.clone().unwrap_or_default(),
        sha: co.head.clone().unwrap_or_else(|| "0".repeat(40)),
        trust: "trusted".into(),
        repo,
        forge_host,
        ..Event::default()
    })
}

/// The files the working tree changed, for a paths filter. Against
/// `--base-sha`, else `--base-ref` (as `origin` has it, else as it is here),
/// else the branch's upstream: everything from where the two part (their
/// merge base) to the working tree, committed or not, and the files git does
/// not track yet. When there is nothing to compare with, the files are not
/// to be had, and a paths filter lets its pipeline run.
fn changed_files(co: &Checkout, flags: &EventFlags) -> Changes {
    if !co.git {
        return Changes::Unlisted("this is not a git checkout".into());
    }
    if co.head.is_none() {
        return Changes::Unlisted("the checkout has no commit yet".into());
    }
    let root = &co.root;
    let resolve = |rev: &str| {
        git(
            root,
            &["rev-parse", "--verify", "-q", &format!("{rev}^{{commit}}")],
        )
    };
    let base = if let Some(sha) = &flags.base_sha {
        resolve(sha).ok_or_else(|| format!("--base-sha {sha} is not a commit here"))
    } else if let Some(branch) = &flags.base_ref {
        let branch = branch.strip_prefix("refs/heads/").unwrap_or(branch);
        resolve(&format!("refs/remotes/origin/{branch}"))
            .or_else(|| resolve(branch))
            .ok_or_else(|| format!("--base-ref {branch} is not a branch here"))
    } else {
        resolve("@{upstream}").ok_or_else(|| {
            "the branch has no upstream to compare the working tree with \
             (name a base with --base-ref, or the files with --changed)"
                .to_string()
        })
    };
    let base = match base {
        Ok(base) => base,
        Err(why) => return Changes::Unlisted(why),
    };
    let Some(fork) = git(root, &["merge-base", &base, "HEAD"]) else {
        return Changes::Unlisted(format!("{base} and HEAD share no history"));
    };
    let tracked = git_paths(root, &["diff", "--name-only", "--no-renames", "-z", &fork]);
    let untracked = git_paths(root, &["ls-files", "--others", "--exclude-standard", "-z"]);
    match (tracked, untracked) {
        (Some(tracked), Some(untracked)) => {
            let files: BTreeSet<String> = tracked.into_iter().chain(untracked).collect();
            Changes::Listed(files.into_iter().collect())
        }
        _ => Changes::Unlisted("git could not list the changed files".into()),
    }
}

/// The pipeline files to run: those named, else every `.yaml` and `.yml`
/// in the checkout's `.dagron/ci`, in name order, and then each workflow
/// file the repository's settings list (`github.workflows`), shown from
/// `cwd`.
fn pipeline_files(
    co: &Checkout,
    cwd: &Path,
    named: &[&str],
    workflows: &[String],
) -> Result<Vec<PathBuf>, String> {
    if !named.is_empty() {
        return Ok(named.iter().map(PathBuf::from).collect());
    }
    let shown = |p: PathBuf| p.strip_prefix(cwd).map(Path::to_path_buf).unwrap_or(p);
    let dir = co.root.join(".dagron/ci");
    let mut files: Vec<PathBuf> = match std::fs::read_dir(&dir) {
        Ok(entries) => entries
            .filter_map(Result::ok)
            .map(|e| e.path())
            .filter(|p| {
                p.is_file()
                    && matches!(p.extension().and_then(|e| e.to_str()), Some("yaml" | "yml"))
            })
            .map(shown)
            .collect(),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
        Err(e) => return Err(format!("{}: {e}", dir.display())),
    };
    files.sort();
    let listed = co.root.join(github::WORKFLOW_DIR);
    files.extend(workflows.iter().map(|name| shown(listed.join(name))));
    if files.is_empty() {
        return Err(format!(
            "no pipeline files in {}, and {} lists no workflow file",
            shown(dir).display(),
            github::SETTINGS_FILE
        ));
    }
    Ok(files)
}

/// Whether `file` is a workflow file: one of `.github/workflows`.
fn is_workflow(file: &Path) -> bool {
    let parent = file
        .parent()
        .map(|p| p.to_string_lossy().replace('\\', "/"));
    parent.is_some_and(|p| {
        p == github::WORKFLOW_DIR || p.ends_with(&format!("/{}", github::WORKFLOW_DIR))
    })
}

/// The repository's settings (`.dagron/ci.yaml`): what a workflow file's
/// `runs-on` labels mean, what its images provide, which files run. None
/// where it has none.
fn repository_settings(co: &Checkout) -> Result<github::Settings, String> {
    let path = co.root.join(github::SETTINGS_FILE);
    match std::fs::read_to_string(&path) {
        Ok(text) => github::settings(&text).map_err(|problems| {
            problems
                .iter()
                .map(|p| format!("{}: {p}", github::SETTINGS_FILE))
                .collect::<Vec<_>>()
                .join("\n")
        }),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(github::Settings::default()),
        Err(e) => Err(format!("{}: {e}", path.display())),
    }
}

/// One pipeline's line of the run's report.
fn reported_pipeline(
    file: &Path,
    name: &str,
    result: &str,
    why: Option<&str>,
    jobs: &[(String, String)],
) -> serde_json::Value {
    let mut entry = serde_json::json!({
        "file": file.display().to_string().replace('\\', "/"),
        "pipeline": name,
        "result": result,
        "jobs": jobs
            .iter()
            .map(|(job, status)| (job.clone(), serde_json::Value::String(status.clone())))
            .collect::<serde_json::Map<_, _>>(),
    });
    if let Some(why) = why {
        entry["why"] = why.into();
    }
    entry
}

/// A program: the path given (a bare name is looked up on `PATH`), else the
/// one beside this executable, else the one on `PATH`.
fn find_program(name: &str, given: Option<&str>) -> Option<PathBuf> {
    let on_path = |name: &str| {
        std::env::var_os("PATH").and_then(|path| {
            std::env::split_paths(&path)
                .map(|dir| dir.join(name))
                .find(|p| is_executable(p))
        })
    };
    match given {
        Some(given) if given.contains('/') => {
            let p = PathBuf::from(given);
            is_executable(&p).then_some(p)
        }
        Some(given) => on_path(given),
        None => std::env::current_exe()
            .ok()
            .and_then(|me| me.parent().map(|dir| dir.join(name)))
            .filter(|p| is_executable(p))
            .or_else(|| on_path(name)),
    }
}

fn is_executable(p: &Path) -> bool {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::metadata(p).is_ok_and(|m| m.is_file() && m.permissions().mode() & 0o111 != 0)
    }
    #[cfg(not(unix))]
    {
        p.is_file()
    }
}

/// Every setting the engine reads, by name, as `dagron config --json` lists
/// them. Asking also shows the binary is an engine this command can drive.
fn engine_settings(engine: &Path) -> Result<Vec<String>, String> {
    let out = Command::new(engine)
        .args(["config", "--json"])
        .env_remove("DAGRON_CONFIG")
        .env_remove("RUST_LOG")
        .env("LOG_LEVEL", "error")
        .env("LOG_ANSI", "false")
        .stdin(Stdio::null())
        .output()
        .map_err(|e| format!("{}: {e}", engine.display()))?;
    let stdout = String::from_utf8_lossy(&out.stdout);
    // Logging may start before the document, on the same stream.
    let doc = stdout
        .find("\n{")
        .map(|i| &stdout[i + 1..])
        .or_else(|| stdout.starts_with('{').then_some(&stdout[..]));
    let names = doc
        .filter(|_| out.status.success())
        .and_then(|doc| serde_json::from_str::<serde_json::Value>(doc).ok())
        .and_then(|doc| {
            doc["knobs"].as_array().map(|knobs| {
                knobs
                    .iter()
                    .filter_map(|k| k["name"].as_str().map(str::to_string))
                    .collect::<Vec<_>>()
            })
        });
    match names {
        Some(names) if !names.is_empty() => Ok(names),
        _ => Err(format!(
            "{} is not a dagron engine dagron-ci run can drive: `dagron config --json` \
             did not list its settings",
            engine.display()
        )),
    }
}

/// The compiled workflow with the run's parameters as its declared
/// defaults: the one-shot run takes the file's defaults as its parameters.
fn with_parameters(compiled: &compile::Compiled) -> Result<(String, Yaml), String> {
    let mut doc: Yaml = serde_yaml::from_str(&compiled.yaml)
        .map_err(|e| format!("the compiled workflow does not parse: {e}"))?;
    let map = doc
        .as_mapping_mut()
        .ok_or("the compiled workflow is not a mapping")?;
    let params = map
        .entry(Yaml::from("parameters"))
        .or_insert_with(|| Yaml::Mapping(Default::default()));
    let params = params
        .as_mapping_mut()
        .ok_or("the compiled workflow's parameters are not a mapping")?;
    for (name, value) in &compiled.parameters {
        params.insert(Yaml::from(name.as_str()), Yaml::from(value.as_str()));
    }
    let yaml = serde_yaml::to_string(&doc).map_err(|e| e.to_string())?;
    Ok((yaml, doc))
}

/// The images the Docker executor will start the workflow's tasks from:
/// each task's `docker_image`, else `task_defaults`', else the default.
///
/// An image a task's items fill in (`node:{{ item }}`, `{{ item.image }}`)
/// is one image for each item, and each is listed. One that still names
/// something then is known when the run is made and not before: it is not
/// listed, and the executor pulls it when it starts the task.
fn images(doc: &Yaml) -> BTreeSet<String> {
    let fallback = doc["task_defaults"]["docker_image"]
        .as_str()
        .unwrap_or(DEFAULT_IMAGE);
    let mut images = BTreeSet::new();
    let tasks = doc["tasks"].as_sequence().into_iter().flatten();
    for task in tasks.filter(|t| t.get("command").is_some()) {
        let image = task["docker_image"].as_str().unwrap_or(fallback);
        if !image.contains("{{") {
            images.insert(image.to_string());
            continue;
        }
        let items = task["with_items"].as_sequence().into_iter().flatten();
        images.extend(
            items
                .map(|item| of_item(image, item))
                .filter(|image| !image.contains("{{")),
        );
    }
    images
}

/// `text` with `{{ item }}` and `{{ item.key }}` as one of a task's items
/// has them. Any other template is left as it is written.
fn of_item(text: &str, item: &Yaml) -> String {
    let plain = |value: &Yaml| match value {
        Yaml::String(s) => Some(s.clone()),
        Yaml::Number(n) => Some(n.to_string()),
        Yaml::Bool(b) => Some(b.to_string()),
        _ => None,
    };
    let mut out = String::new();
    let mut rest = text;
    while let Some(start) = rest.find("{{") {
        let Some(end) = rest[start..].find("}}").map(|at| at + start + 2) else {
            break;
        };
        let named = rest[start + 2..end - 2].trim();
        let value = match named.strip_prefix("item") {
            Some("") => plain(item),
            Some(key) => key.strip_prefix('.').and_then(|key| item.get(key)).and_then(plain),
            None => None,
        };
        out.push_str(&rest[..start]);
        out.push_str(&value.unwrap_or_else(|| rest[start..end].to_string()));
        rest = &rest[end..];
    }
    out + rest
}

/// Why a run of `doc` does not start under `plan`: a job asks for a Docker
/// daemon, which is privileged on this machine's Docker host, and the run
/// was not told it may have one (`--docker-daemon`), or runs where none can
/// be run. `None` where none asks, or it may.
fn daemon_refusal(doc: &Yaml, plan: &Plan<'_>) -> Option<String> {
    let tasks = doc["tasks"].as_sequence().into_iter().flatten();
    let asking: Vec<&str> = tasks
        .filter(|t| t["docker_daemon"].as_bool() == Some(true))
        .filter_map(|t| t["name"].as_str())
        .collect();
    let first = asking.first()?;
    match (plan.executor, plan.docker_daemon) {
        (Executor::Docker, true) => None,
        (Executor::Docker, false) => Some(format!(
            "the job {first:?} asks for a Docker daemon, a privileged container on this \
             machine's Docker host: run again with --docker-daemon to give it one"
        )),
        (Executor::Local, _) => Some(format!(
            "the job {first:?} asks for a Docker daemon, which runs beside a job in a \
             container: run it with --executor docker --docker-daemon"
        )),
    }
}

/// `docker ARGS`, quietly: whether it succeeded, and its output.
fn docker(args: &[&str]) -> Result<(bool, String), String> {
    let out = Command::new("docker")
        .args(args)
        .stdin(Stdio::null())
        .output()
        .map_err(|e| format!("docker: {e} (the Docker executor needs Docker's command line)"))?;
    Ok((
        out.status.success(),
        String::from_utf8_lossy(&out.stdout).into_owned() + &String::from_utf8_lossy(&out.stderr),
    ))
}

/// Docker answers, and has every image the run needs, pulled now if not.
fn prepare_docker(images: &BTreeSet<String>) -> Result<(), String> {
    let (up, said) = docker(&["version", "--format", "{{.Server.Version}}"])?;
    if !up {
        return Err(format!(
            "Docker is not reachable ({}); start it, or run the steps on this machine \
             with --executor local",
            said.trim()
        ));
    }
    for image in images {
        if docker(&["image", "inspect", "--format", "{{.Id}}", image])?.0 {
            continue;
        }
        say!("pulling {image}");
        // Its progress on a terminal; elsewhere, one line for the image.
        let mut pull = Command::new("docker");
        pull.arg("pull");
        if !std::io::stdout().is_terminal() {
            pull.arg("--quiet");
        }
        let pulled = pull
            .arg(image)
            .stdin(Stdio::null())
            .status()
            .map_err(|e| format!("docker pull {image}: {e}"))?;
        if !pulled.success() {
            return Err(format!("docker pull {image} failed"));
        }
    }
    Ok(())
}

/// The mounts every task container gets: the working tree, read-write, at
/// its own path; and dagron-ci-exec, read-only, where the compiled command
/// looks for it, when there is one to give.
fn docker_mounts(root: &Path, exec: Option<&Path>) -> Result<String, String> {
    let entry = |path: &Path, what: &str| {
        let s = path.to_str().ok_or(format!("{what}'s path is not UTF-8"))?;
        if s.contains([',', ':']) {
            return Err(format!(
                "{what}'s path holds a ',' or ':', which a mount cannot carry: {s} \
                 (--executor local runs without one)"
            ));
        }
        Ok(s.to_string())
    };
    let root = entry(root, "the working tree")?;
    let mut mounts = format!("{root}:{root}");
    if let Some(exec) = exec {
        mounts.push_str(&format!(
            ",{}:{EXEC_IN_TASK}:ro",
            entry(exec, "dagron-ci-exec")?
        ));
    }
    Ok(mounts)
}

/// One task as the run's database has it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct TaskRow {
    id: String,
    name: String,
    status: String,
    attempt: i64,
    /// Both streams as they arrived, for an executor that streams; NULL for
    /// one that does not, whose `output` is read instead once it ends.
    log: Option<String>,
    output: Option<String>,
}

#[derive(Debug, Default)]
struct Snapshot {
    /// The run's id and status, once the engine has created it.
    run: Option<(String, String)>,
    tasks: Vec<TaskRow>,
}

async fn snapshot(db: &SqlitePool) -> Result<Snapshot, sqlx::Error> {
    // The first run made: a sub-workflow's runs come after their parent's.
    let run = sqlx::query("SELECT id, status FROM workflow_runs ORDER BY rowid LIMIT 1")
        .fetch_optional(db)
        .await?
        .map(|r| (r.get::<String, _>("id"), r.get::<String, _>("status")));
    let Some((id, status)) = run else {
        return Ok(Snapshot::default());
    };
    let tasks = sqlx::query(
        "SELECT id, name, status, attempt, log, output FROM task_runs \
         WHERE run_id = ? ORDER BY rowid",
    )
    .bind(&id)
    .fetch_all(db)
    .await?
    .into_iter()
    .map(|r| TaskRow {
        id: r.get("id"),
        name: r.get("name"),
        status: r.get("status"),
        attempt: r.get("attempt"),
        log: r.get("log"),
        output: r.get("output"),
    })
    .collect();
    Ok(Snapshot {
        run: Some((id, status)),
        tasks,
    })
}

/// Why a submission never became a run: the engine parks it, and its error,
/// in `dead_letters`.
async fn dead_letter(db: &SqlitePool) -> Option<String> {
    sqlx::query("SELECT error FROM dead_letters ORDER BY last_error_at DESC LIMIT 1")
        .fetch_optional(db)
        .await
        .ok()
        .flatten()
        .map(|r| r.get("error"))
}

fn is_terminal(status: &str) -> bool {
    matches!(status, "succeeded" | "failed" | "skipped" | "cancelled")
}

/// What has been printed of one task.
#[derive(Debug, Default)]
struct Seen {
    status: String,
    attempt: i64,
    /// How much of its log has been printed, in bytes, up to a newline.
    printed: usize,
    /// The end of what was printed, to tell a log that grew from one that
    /// was written anew: a retry starts its log over, and the redaction of a
    /// whole log rewrites it.
    tail: String,
    /// When it was first seen running.
    started: Option<Instant>,
}

/// How much of a log's printed end is kept to recognise it by.
const TAIL: usize = 64;

/// Turns what the database says into lines, printing each thing once.
#[derive(Debug, Default)]
struct Printer {
    seen: BTreeMap<String, Seen>,
}

impl Printer {
    /// The lines `snap` has that were not printed yet. `last` is the final
    /// look: what is left of each log is printed whole.
    fn update(&mut self, snap: &Snapshot, now: Instant, last: bool) -> Vec<String> {
        let mut lines = Vec::new();
        for task in &snap.tasks {
            let seen = self.seen.entry(task.id.clone()).or_default();
            let name = &task.name;
            if task.status == "running"
                && (seen.status != "running" || task.attempt != seen.attempt)
            {
                if seen.started.is_some() && task.attempt != seen.attempt {
                    lines.push(format!("[{name}] retrying, attempt {}", task.attempt));
                } else {
                    lines.push(format!("[{name}] started"));
                }
                seen.started = Some(now);
                seen.attempt = task.attempt;
            }
            let terminal = is_terminal(&task.status);
            // Ran and finished between two looks: it started all the same.
            if seen.started.is_none()
                && seen.status != task.status
                && matches!(task.status.as_str(), "succeeded" | "failed")
            {
                lines.push(format!("[{name}] started"));
            }
            let log = match (&task.log, terminal) {
                (Some(log), _) => Some(log.as_str()),
                (None, true) => task.output.as_deref(),
                (None, false) => None,
            };
            if let Some(log) = log {
                let grew = seen
                    .printed
                    .checked_sub(seen.tail.len())
                    .and_then(|from| log.get(from..seen.printed))
                    == Some(seen.tail.as_str());
                if !grew {
                    // Written anew: printed again, from the top.
                    seen.printed = 0;
                    seen.tail.clear();
                }
                let rest = &log[seen.printed..];
                let whole = if terminal || last {
                    rest.len()
                } else {
                    rest.rfind('\n').map_or(0, |i| i + 1)
                };
                for line in rest[..whole].lines() {
                    lines.extend(render(name, line));
                }
                seen.printed += whole;
                let from = (seen.printed.saturating_sub(TAIL)..seen.printed)
                    .find(|i| log.is_char_boundary(*i))
                    .unwrap_or(seen.printed);
                seen.tail = log[from..seen.printed].to_string();
            }
            if terminal && seen.status != task.status {
                let took = seen
                    .started
                    .map(|s| format!(" in {:.1}s", now.duration_since(s).as_secs_f64()))
                    .unwrap_or_default();
                lines.push(match task.status.as_str() {
                    "skipped" | "cancelled" => format!("[{name}] {}", task.status),
                    status => format!("[{name}] {status}{took}"),
                });
            } else if last && !terminal {
                lines.push(format!("[{name}] did not finish ({})", task.status));
            }
            seen.status = task.status.clone();
        }
        lines
    }
}

/// One line of a task's log as it is printed: a step marker as what it
/// says, what the task's reports held as their counts, each place and each
/// failure, the result (for a machine) and unknown markers not at all, and
/// anything else as it is.
fn render(task: &str, line: &str) -> Option<String> {
    let line = line.strip_suffix('\r').unwrap_or(line);
    let Some(marker) = line.strip_prefix("::dagron-ci::") else {
        return Some(format!("[{task}] {line}"));
    };
    let said = if let Some(step) = marker.strip_prefix("step::") {
        let (n, what) = step.split_once("::")?;
        if let Some(name) = what.strip_prefix("start::") {
            format!("== step {n}: {name}")
        } else if let Some(code) = what.strip_prefix("end::") {
            match code {
                "0" => format!("== step {n} ok"),
                code => format!("== step {n} failed, exit {code}"),
            }
        } else if what == "continued" {
            format!("== step {n} may fail (continue_on_error): the task goes on")
        } else {
            return None;
        }
    } else if let Some(text) = marker.strip_prefix("warning::") {
        format!("warning: {text}")
    } else if let Some(signal) = marker.strip_prefix("cancelled::") {
        format!("cancelled by signal {signal}")
    } else if ["report::", "annotation::", "failed::"]
        .iter()
        .any(|kind| marker.starts_with(kind))
    {
        reported(&crate::said::read([line]))?
    } else {
        return None;
    };
    Some(format!("[{task}] {said}"))
}

/// One line of what a task's reports held, as it is printed: the counts, a
/// place with what was found there, or a failure by name. A check run has
/// them where a forge has the run; here there is the terminal.
fn reported(said: &crate::said::Said) -> Option<String> {
    let first = |text: &str| text.lines().next().unwrap_or_default().to_string();
    if let Some(counts) = &said.counts {
        let mut line = format!(
            "report: {} tests, {} failed, {} errors, {} skipped",
            counts.tests, counts.failures, counts.errors, counts.skipped
        );
        if counts.findings > 0 {
            line.push_str(&format!(", {} findings", counts.findings));
        }
        for problem in &counts.problems {
            line.push_str(&format!("; {problem}"));
        }
        return Some(line);
    }
    if let Some(at) = said.annotations.first() {
        return Some(format!(
            "{}:{}: {}: {}: {}",
            at.path,
            at.start_line,
            at.level,
            at.title,
            first(&at.message)
        ));
    }
    let (name, message) = said.failed.first()?;
    Some(match first(message) {
        message if message.is_empty() => format!("failed: {name}"),
        message => format!("failed: {name}: {message}"),
    })
}

/// How one pipeline's run ended.
#[derive(Debug, PartialEq, Eq)]
enum Ended {
    Succeeded,
    /// The run failed, or was cancelled: its status, and the tasks that failed.
    Failed(String, Vec<String>),
    /// No run came of it, and why.
    NoRun(String),
    /// Stopped by this signal.
    Interrupted(i32),
}

/// Everything one pipeline's run needs.
struct Plan<'a> {
    engine: &'a Path,
    settings: &'a [String],
    executor: Executor,
    root: &'a Path,
    /// A dagron-ci-exec to give the tasks.
    exec: Option<&'a Path>,
    keep: bool,
    /// Whether a job that asks for a Docker daemon is given one.
    docker_daemon: bool,
}

/// Run one compiled pipeline to its end.
async fn run_one(
    plan: &Plan<'_>,
    compiled: &compile::Compiled,
    stop: &Stop,
) -> Result<(Ended, Vec<(String, String)>), String> {
    let (yaml, doc) = with_parameters(compiled)?;
    if let Some(why) = daemon_refusal(&doc, plan) {
        return Err(why);
    }
    if plan.executor == Executor::Docker {
        prepare_docker(&images(&doc))?;
    }
    let dir = run_dir().map_err(|e| format!("a directory for the run: {e}"))?;
    let workflow = dir.join("workflow.yaml");
    let db_path = dir.join("run.db");
    let log_path = dir.join("engine.log");
    std::fs::write(&workflow, yaml).map_err(|e| format!("{}: {e}", workflow.display()))?;
    let log =
        std::fs::File::create(&log_path).map_err(|e| format!("{}: {e}", log_path.display()))?;
    let mut cmd = engine_command(plan, &dir)?;
    cmd.arg(&workflow)
        .arg(&db_path)
        .stdin(Stdio::null())
        .stdout(log.try_clone().map_err(|e| e.to_string())?)
        .stderr(log);
    let mut engine = Engine(
        cmd.spawn()
            .map_err(|e| format!("{}: {e}", plan.engine.display()))?,
    );

    let mut out = std::io::stdout();
    let mut printer = Printer::default();
    let mut db: Option<SqlitePool> = None;
    let mut run_id: Option<String> = None;
    let mut handled = 0;
    let mut kill_at: Option<Instant> = None;
    let status = loop {
        if let Some(status) = engine.0.try_wait().map_err(|e| e.to_string())? {
            break status;
        }
        if db.is_none() && db_path.exists() {
            db = open(&db_path).await;
        }
        if let Some(pool) = &db {
            // The engine may still be making its tables: a read that fails
            // is tried again on the next tick.
            if let Ok(snap) = snapshot(pool).await {
                run_id = snap.run.as_ref().map(|(id, _)| id.clone()).or(run_id);
                for line in printer.update(&snap, Instant::now(), false) {
                    let _ = writeln!(out, "{line}");
                }
            }
        }
        tokio::select! {
            () = tokio::time::sleep(TICK) => {}
            () = stop.notify.notified() => {}
        }
        // The first signal asks the engine and its tasks to stop; a second,
        // or the grace running out, kills them.
        let caught = stop.count();
        if caught > handled {
            if handled == 0 {
                let _ = writeln!(
                    out,
                    "stopping the run ({})",
                    signal_name(stop.caught().unwrap_or_default())
                );
                stop_engine(engine.0.id(), false);
                kill_at = Some(Instant::now() + STOP_GRACE);
            } else {
                stop_engine(engine.0.id(), true);
            }
            handled = caught;
        }
        if kill_at.is_some_and(|at| Instant::now() >= at) {
            stop_engine(engine.0.id(), true);
            kill_at = None;
        }
    };

    let pool = match db {
        Some(pool) => Some(pool),
        None => open(&db_path).await,
    };
    let mut ended = Ended::NoRun(format!(
        "the engine exited ({status}) before it opened its database"
    ));
    // What each of its tasks came to, as the run's database last said.
    let mut jobs: Vec<(String, String)> = Vec::new();
    if let Some(pool) = &pool {
        match snapshot(pool).await {
            Ok(snap) => {
                run_id = snap.run.as_ref().map(|(id, _)| id.clone()).or(run_id);
                jobs = snap
                    .tasks
                    .iter()
                    .map(|t| (t.name.clone(), t.status.clone()))
                    .collect();
                for line in printer.update(&snap, Instant::now(), true) {
                    let _ = writeln!(out, "{line}");
                }
                ended = match &snap.run {
                    Some((_, s)) if s == "succeeded" => Ended::Succeeded,
                    Some((_, s)) if is_terminal(s) => Ended::Failed(
                        s.clone(),
                        snap.tasks
                            .iter()
                            .filter(|t| t.status == "failed")
                            .map(|t| t.name.clone())
                            .collect(),
                    ),
                    Some((_, s)) => {
                        Ended::NoRun(format!("the engine exited ({status}) with the run {s}"))
                    }
                    None => Ended::NoRun(match dead_letter(pool).await {
                        Some(why) => format!("the engine refused the workflow: {why}"),
                        None => format!("the engine started no run ({status})"),
                    }),
                };
            }
            Err(e) => ended = Ended::NoRun(format!("the run's database cannot be read: {e}")),
        }
        pool.close().await;
    }
    if let Some(signal) = stop.caught() {
        ended = Ended::Interrupted(signal);
    }
    // A stopped engine leaves its containers running, and a failed one may.
    if let (Some(id), Executor::Docker) = (&run_id, plan.executor) {
        if stop.caught().is_some() || !status.success() {
            remove_containers(id);
        }
    }
    if !status.success() && stop.caught().is_none() {
        report!("the engine exited with {status}; the end of its log:");
        let _ = write!(std::io::stderr(), "{}", tail(&log_path, 20));
    }
    if ended == Ended::Succeeded && !plan.keep {
        let _ = std::fs::remove_dir_all(&dir);
    } else {
        say!(
            "the run's files are in {}: workflow.yaml, run.db, engine.log",
            dir.display()
        );
    }
    Ok((ended, jobs))
}

/// The engine while it runs. Dropped before it exited, as when this process
/// unwinds, it is killed with every task it runs, rather than left running
/// in a process group nobody waits for.
struct Engine(std::process::Child);

impl Drop for Engine {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(None)) {
            stop_engine(self.0.id(), true);
            let _ = self.0.wait();
        }
    }
}

/// The engine, set up for one run in `dir`: this environment less its own
/// settings, then the ones the run needs; the working tree as its working
/// directory; its own process group.
fn engine_command(plan: &Plan<'_>, dir: &Path) -> Result<Command, String> {
    let mut cmd = Command::new(plan.engine);
    cmd.current_dir(plan.root);
    for name in plan.settings {
        cmd.env_remove(name);
    }
    // And every DAGRON_ variable, listed or not: the namespace is dagron's.
    for (name, _) in std::env::vars_os() {
        if name.to_str().is_some_and(|n| n.starts_with("DAGRON_")) {
            cmd.env_remove(&name);
        }
    }
    cmd.env_remove("DAGRON_CONFIG")
        .env("SOURCE", "file")
        .env("EXECUTOR", plan.executor.name())
        .env("LOG_ANSI", "false");
    match plan.executor {
        Executor::Docker => {
            cmd.env("DOCKER_IMAGE", DEFAULT_IMAGE)
                .env("DAGRON_DOCKER_MOUNTS", docker_mounts(plan.root, plan.exec)?)
                .env("DAGRON_DOCKER_WORKDIR", plan.root);
            // A Docker daemon for a job that asks for one, where the one who
            // started the run said it may have one.
            // On the runtime this machine's owner names for it, Sysbox's,
            // which the engine checks this machine's Docker has.
            if plan.docker_daemon {
                cmd.env("DAGRON_DOCKER_DAEMON", "dind");
                if let Some(runtime) = std::env::var_os("DAGRON_DOCKER_DAEMON_RUNTIME") {
                    cmd.env("DAGRON_DOCKER_DAEMON_RUNTIME", runtime);
                }
            }
        }
        Executor::Local => {
            // Every task in the tree, a job's workspace included: without
            // this, the engine would give each job an empty directory.
            cmd.env("DAGRON_LOCAL_WORKDIR", plan.root);
            // dagron-ci-exec, first on the tasks' PATH, from a directory that
            // holds it alone, so nothing beside it shadows a task's programs.
            if let Some(exec) = plan.exec {
                let bin = dir.join("bin");
                let linked = std::fs::create_dir(&bin)
                    .and_then(|()| link(exec, &bin.join("dagron-ci-exec")));
                if linked.is_ok() {
                    let path = std::env::var_os("PATH").unwrap_or_default();
                    let path = std::env::join_paths(
                        std::iter::once(bin).chain(std::env::split_paths(&path)),
                    )
                    .map_err(|e| e.to_string())?;
                    cmd.env("PATH", path);
                }
            }
        }
    }
    #[cfg(unix)]
    {
        use std::os::unix::process::CommandExt;
        // Its own process group: a stop reaches it and every task it runs,
        // and a terminal's ctrl-c reaches this process, which stops it.
        cmd.process_group(0);
    }
    Ok(cmd)
}

/// A directory for one run, this user's alone.
fn run_dir() -> std::io::Result<PathBuf> {
    let dir = std::env::temp_dir().join(format!("dagron-ci-run-{}", uuid::Uuid::now_v7().simple()));
    let mut builder = std::fs::DirBuilder::new();
    #[cfg(unix)]
    {
        use std::os::unix::fs::DirBuilderExt;
        builder.mode(0o700);
    }
    builder.create(&dir)?;
    Ok(dir)
}

#[cfg(unix)]
fn link(target: &Path, at: &Path) -> std::io::Result<()> {
    std::os::unix::fs::symlink(target, at)
}

#[cfg(not(unix))]
fn link(target: &Path, at: &Path) -> std::io::Result<()> {
    std::fs::copy(target, at).map(|_| ())
}

/// The run's database, opened to read. Not created: the engine does that.
async fn open(path: &Path) -> Option<SqlitePool> {
    let opts = SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .busy_timeout(Duration::from_secs(5));
    SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(opts)
        .await
        .ok()
}

/// The last `n` lines of a file.
fn tail(path: &Path, n: usize) -> String {
    let text = std::fs::read_to_string(path).unwrap_or_default();
    let lines: Vec<&str> = text.lines().collect();
    let mut out = String::new();
    for line in &lines[lines.len().saturating_sub(n)..] {
        out.push_str("  ");
        out.push_str(line);
        out.push('\n');
    }
    out
}

/// Remove what the Docker executor left of the run: its tasks' containers,
/// by the run's label. A stopped engine leaves them running, and one stopped
/// while it was creating or starting a container leaves Docker to finish
/// that after it died: so the sweep goes on until two looks a second apart
/// find nothing, for at most [`SWEEP_LOOKS`] looks.
fn remove_containers(run_id: &str) {
    let filter = format!("label={RUN_LABEL}={run_id}");
    let mut quiet = 0;
    for look in 0..SWEEP_LOOKS {
        if look > 0 {
            std::thread::sleep(Duration::from_secs(1));
        }
        let Ok((true, ids)) = docker(&["ps", "--all", "--quiet", "--filter", &filter]) else {
            return;
        };
        let ids: Vec<&str> = ids.split_whitespace().collect();
        if ids.is_empty() {
            quiet += 1;
            if quiet == 2 {
                return;
            }
            continue;
        }
        quiet = 0;
        // With their anonymous volumes: a Docker daemon's holds all it built.
        let mut args = vec!["rm", "--force", "--volumes"];
        args.extend(ids);
        let _ = docker(&args);
    }
}

/// How many times the run's containers are looked for after a stop.
const SWEEP_LOOKS: u32 = 8;

/// Stop the engine and the tasks it runs: SIGTERM to its process group,
/// SIGKILL when `kill`.
#[cfg(unix)]
fn stop_engine(pid: u32, kill: bool) {
    let signal = if kill { libc::SIGKILL } else { libc::SIGTERM };
    if let Ok(pid) = libc::pid_t::try_from(pid) {
        // SAFETY: kill(2) on the process group this process started; nothing
        // is shared with the call but two integers.
        unsafe {
            libc::kill(-pid, signal);
        }
    }
}

#[cfg(not(unix))]
fn stop_engine(_pid: u32, _kill: bool) {}

fn signal_name(signal: i32) -> &'static str {
    match signal {
        2 => "interrupted",
        15 => "terminated",
        1 => "hung up",
        _ => "signalled",
    }
}

/// The signals that stop a run, caught for as long as the command runs: a
/// terminal's ctrl-c, a service manager's stop, a closed terminal. Caught
/// once, at the start, so that one arriving between two pipelines, or while
/// an image is pulled, stops the command as well.
#[derive(Clone, Default)]
struct Stop {
    /// The first signal caught; 0 until one is.
    first: Arc<AtomicI32>,
    /// How many have been caught.
    count: Arc<AtomicUsize>,
    notify: Arc<Notify>,
}

impl Stop {
    fn catch() -> Result<Stop, String> {
        let stop = Stop::default();
        #[cfg(unix)]
        {
            use tokio::signal::unix::{signal, SignalKind};
            let mut int = signal(SignalKind::interrupt()).map_err(|e| format!("signals: {e}"))?;
            let mut term = signal(SignalKind::terminate()).map_err(|e| format!("signals: {e}"))?;
            let mut hup = signal(SignalKind::hangup()).map_err(|e| format!("signals: {e}"))?;
            let caught = stop.clone();
            tokio::spawn(async move {
                loop {
                    let n = tokio::select! {
                        _ = int.recv() => libc::SIGINT,
                        _ = term.recv() => libc::SIGTERM,
                        _ = hup.recv() => libc::SIGHUP,
                    };
                    let _ = caught
                        .first
                        .compare_exchange(0, n, Ordering::SeqCst, Ordering::SeqCst);
                    caught.count.fetch_add(1, Ordering::SeqCst);
                    caught.notify.notify_one();
                }
            });
        }
        Ok(stop)
    }

    fn caught(&self) -> Option<i32> {
        Some(self.first.load(Ordering::SeqCst)).filter(|n| *n != 0)
    }

    fn count(&self) -> usize {
        self.count.load(Ordering::SeqCst)
    }
}

/// `dagron-ci run`. Exits 0 when every pipeline that ran succeeded, 3 when
/// none runs for the event, 1 when one failed or could not run, 2 for a
/// usage error, and 128 plus the signal when stopped.
pub async fn run(args: &[&str]) -> ExitCode {
    let usage = |why: &str| {
        report!("dagron-ci run: {why}\n{USAGE}");
        ExitCode::from(2)
    };
    let opts = match options(args) {
        Ok(opts) => opts,
        Err(why) => return usage(&why),
    };
    if opts.help {
        say!("{USAGE}");
        return ExitCode::SUCCESS;
    }
    let cwd = match std::env::current_dir() {
        Ok(cwd) => cwd,
        Err(e) => return usage(&format!("the current directory: {e}")),
    };
    let co = checkout(&cwd);
    let mut event = match event(&opts.flags, &co) {
        Ok(event) => event,
        Err(why) => return usage(&why),
    };
    let repository = match repository_settings(&co) {
        Ok(settings) => settings,
        Err(why) => return usage(&why),
    };
    let files = match pipeline_files(&co, &cwd, &opts.files, &repository.workflows) {
        Ok(files) => files,
        Err(why) => return usage(&why),
    };
    let mut report: Vec<serde_json::Value> = Vec::new();
    let settings = match crate::config::default_timeout_secs(&|n: &str| std::env::var(n).ok()) {
        // No checkout: every task works in the tree this was run from.
        Ok(default_timeout_secs) => Settings {
            default_timeout_secs,
            checkout: false,
            patched: false,
        },
        Err(e) => return usage(&e.to_string()),
    };
    let Some(engine) = find_program("dagron", opts.engine) else {
        report!(
            "dagron-ci run: no dagron engine {}; build one (cargo build --release --bin dagron) \
             or name it with --engine",
            match opts.engine {
                Some(given) => format!("at {given}"),
                None => "beside dagron-ci or on PATH".into(),
            }
        );
        return ExitCode::FAILURE;
    };
    let engine_settings = match engine_settings(&engine) {
        Ok(names) => names,
        Err(why) => {
            report!("dagron-ci run: {why}");
            return ExitCode::FAILURE;
        }
    };
    let stop = match Stop::catch() {
        Ok(stop) => stop,
        Err(why) => {
            report!("dagron-ci run: {why}");
            return ExitCode::FAILURE;
        }
    };
    let exec = find_program("dagron-ci-exec", None);
    let plan = Plan {
        engine: &engine,
        settings: &engine_settings,
        executor: opts.executor,
        root: &co.root,
        exec: exec.as_deref(),
        keep: opts.keep,
        docker_daemon: opts.docker_daemon,
    };
    say!(
        "dagron-ci run: {} {} at {} ({}/{}), on {}{}",
        event.kind,
        event.git_ref,
        &event.sha[..12],
        event.forge_host,
        event.repo,
        opts.executor.name(),
        match opts.executor {
            Executor::Docker => format!(", with {} mounted", co.root.display()),
            Executor::Local => format!(", in {}", co.root.display()),
        }
    );

    let (mut ran, mut failed) = (0, 0);
    for file in &files {
        if let Some(signal) = stop.caught() {
            return stopped(signal);
        }
        let shown = file.display();
        // A workflow file is read for this event, as the event workers read
        // one: what it becomes is a pipeline like any other from here on,
        // and the jobs it leaves out are said to be skipped.
        let mut left_out: Vec<(String, String)> = Vec::new();
        let read = std::fs::read_to_string(file).map_err(|e| vec![e.to_string()]);
        let parsed = if is_workflow(file) {
            let stem = crate::report::pipeline_name(&file.to_string_lossy()).to_string();
            read.and_then(|text| {
                github::parse(&text, &repository.provided)
                    .and_then(|workflow| workflow.lower(&stem, &event, &repository))
                    .map(|lowered| {
                        left_out = lowered.skipped;
                        lowered.pipeline
                    })
                    .map_err(|ps| ps.iter().map(ToString::to_string).collect())
            })
        } else {
            read.and_then(|text| {
                crate::pipeline::parse(&text)
                    .map_err(|ps| ps.iter().map(ToString::to_string).collect())
            })
        };
        let pipeline = match parsed {
            Ok(p) => p,
            Err(problems) => {
                for problem in &problems {
                    report!("{shown}: {problem}");
                }
                report.push(reported_pipeline(
                    file,
                    "",
                    "refused",
                    Some(&problems.join("; ")),
                    &[],
                ));
                failed += 1;
                continue;
            }
        };
        let skipped: Vec<(String, String)> = left_out
            .iter()
            .map(|(job, _)| (job.clone(), "skipped".to_string()))
            .collect();
        let mut outcome = compile::compile(&pipeline, &event, &settings);
        if matches!(outcome, Ok(Outcome::NeedsChangedFiles)) {
            event.changed = changed_files(&co, &opts.flags);
            if let Changes::Unlisted(why) = &event.changed {
                say!("the changed files are not known, so a paths filter lets its pipeline run: {why}");
            }
            outcome = compile::compile(&pipeline, &event, &settings);
        }
        let compiled = match outcome {
            Ok(Outcome::Run(compiled)) => compiled,
            Ok(Outcome::Skip(why) | Outcome::Unchanged { why, .. }) => {
                say!("{shown}: {} does not run: {why}", pipeline.name);
                report.push(reported_pipeline(
                    file,
                    &pipeline.name,
                    "not run",
                    Some(&why),
                    &skipped,
                ));
                continue;
            }
            Ok(Outcome::NeedsChangedFiles) => unreachable!("the changed files are known"),
            Err(why) => {
                report!("{shown}: {why}");
                report.push(reported_pipeline(
                    file,
                    &pipeline.name,
                    "refused",
                    Some(&why),
                    &skipped,
                ));
                failed += 1;
                continue;
            }
        };
        say!("{shown}: {}", pipeline.name);
        ran += 1;
        let started = Instant::now();
        let (ended, mut jobs) = match run_one(&plan, &compiled, &stop).await {
            Ok(ended) => ended,
            Err(why) => {
                if let Some(signal) = stop.caught() {
                    return stopped(signal);
                }
                report!("{shown}: {why}");
                report.push(reported_pipeline(
                    file,
                    &pipeline.name,
                    "did not run",
                    Some(&why),
                    &skipped,
                ));
                failed += 1;
                continue;
            }
        };
        // A run every job of which its `when:` left out: it ended at once,
        // with nothing to have run, and is said so.
        let none_ran = jobs.is_empty();
        // And the jobs its `when:` left out are skipped, as the event workers
        // say them on the commit: of a run that was made and ended, whose
        // tasks are what dagron made of it. Where none was, no `when:` was
        // decided.
        let mut skipped = skipped;
        if matches!(ended, Ended::Succeeded | Ended::Failed(..)) {
            let made: Vec<String> = jobs.iter().map(|(job, _)| job.clone()).collect();
            for job in compile::left_out(&compiled, &made) {
                if !skipped.iter().any(|(had, _)| *had == job) {
                    skipped.push((job, "skipped".to_string()));
                }
            }
        }
        jobs.extend(skipped);
        let took = started.elapsed().as_secs_f64();
        match ended {
            Ended::Succeeded if none_ran => {
                say!(
                    "{}: succeeded in {took:.1}s, with no job: each one's `when:` leaves it out of this event",
                    pipeline.name
                );
                report.push(reported_pipeline(
                    file,
                    &pipeline.name,
                    "succeeded",
                    None,
                    &jobs,
                ));
            }
            Ended::Succeeded => {
                say!("{}: succeeded in {took:.1}s", pipeline.name);
                report.push(reported_pipeline(
                    file,
                    &pipeline.name,
                    "succeeded",
                    None,
                    &jobs,
                ));
            }
            Ended::Failed(status, tasks) => {
                failed += 1;
                let which = if tasks.is_empty() {
                    String::new()
                } else {
                    format!(" ({} failed)", tasks.join(", "))
                };
                say!("{}: {status} in {took:.1}s{which}", pipeline.name);
                report.push(reported_pipeline(
                    file,
                    &pipeline.name,
                    &status,
                    None,
                    &jobs,
                ));
            }
            Ended::NoRun(why) => {
                failed += 1;
                say!("{}: did not run: {why}", pipeline.name);
                report.push(reported_pipeline(
                    file,
                    &pipeline.name,
                    "did not run",
                    Some(&why),
                    &jobs,
                ));
            }
            Ended::Interrupted(signal) => {
                say!("{}: stopped ({})", pipeline.name, signal_name(signal));
                return stopped(signal);
            }
        }
    }
    if let Some(path) = opts.report {
        let written = serde_json::json!({
            "event": event.kind,
            "ref": event.git_ref,
            "sha": event.sha,
            "pipelines": report,
        });
        if let Err(e) = std::fs::write(path, format!("{written:#}\n")) {
            report!("dagron-ci run: the report could not be written to {path}: {e}");
            return ExitCode::FAILURE;
        }
    }
    if failed > 0 {
        ExitCode::FAILURE
    } else if ran == 0 {
        say!("no pipeline runs for {} {}", event.kind, event.git_ref);
        ExitCode::from(3)
    } else {
        ExitCode::SUCCESS
    }
}

/// What a stopped command exits with: 128 plus the signal, as a shell
/// reports one.
fn stopped(signal: i32) -> ExitCode {
    ExitCode::from(u8::try_from(128 + signal).unwrap_or(1))
}

#[cfg(test)]
mod tests;
