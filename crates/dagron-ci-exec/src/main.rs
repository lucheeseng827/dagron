//! `dagron-ci-exec`: run a dagron-ci task's steps. See the crate's README.

use std::ffi::OsString;
use std::process::ExitCode;

use dagron_ci_exec::PROTOCOLS;

const USAGE: &str = "\
usage: dagron-ci-exec              run the steps this environment describes (step protocol 1 or 2)
       dagron-ci-exec --speaks N    exit 0 if this binary speaks step protocol N
       dagron-ci-exec install DIR [--with node]
                                    copy this binary to DIR/dagron-ci-exec, and the
                                    Node.js beside it to DIR/node where asked
       dagron-ci-exec container SPEC
                                    run a step in an image of its own, as SPEC (which the
                                    step runner writes) says, in a container the engine
                                    starts beside the job
       dagron-ci-exec wait-no-network SECS
                                    exit 0 once this pod's network is seen to be gone,
                                    and 125 if it still has one after SECS seconds
       dagron-ci-exec wait-allowlist SECS
                                    exit 0 once this pod is seen to be held to its
                                    allowlist, and 125 if it is not after SECS seconds
       dagron-ci-exec --version";

/// What dagron-ci-exec exits with when it failed, rather than a step: the
/// code `timeout(1)` uses for the same.
const FAILED: u8 = 125;

/// The release this binary was built for: `APP_VERSION` as a release's build
/// sets it. `dev` is a build no release made. (As every dagron binary says
/// it; this one links none of their shared crates.)
const VERSION: &str = match option_env!("APP_VERSION") {
    Some(version) if !version.is_empty() => version,
    _ => "dev",
};

fn main() -> ExitCode {
    let args: Vec<OsString> = std::env::args_os().skip(1).collect();
    let words: Vec<Option<&str>> = args.iter().map(|a| a.to_str()).collect();
    match words.as_slice() {
        [] => run(),
        [Some("--speaks"), n] => {
            if n.and_then(|n| n.parse::<u32>().ok())
                .is_some_and(|n| PROTOCOLS.contains(&n))
            {
                ExitCode::SUCCESS
            } else {
                ExitCode::from(1)
            }
        }
        [Some("matrix")] => matrix(),
        [Some("install"), _, rest @ ..] => {
            // What is installed with it: `--with node`, and nothing else.
            let mut with_node = false;
            for pair in rest.chunks(2) {
                match pair {
                    [Some("--with"), Some("node")] => with_node = true,
                    _ => {
                        eprintln!(
                            "dagron-ci-exec: install takes --with node, and no other\n{USAGE}"
                        );
                        return ExitCode::from(FAILED);
                    }
                }
            }
            install(std::path::Path::new(&args[1]), with_node)
        }
        [Some("container"), Some(_)] => {
            let code = dagron_ci_exec::container::run(std::path::Path::new(&args[1]));
            ExitCode::from(u8::try_from(code).unwrap_or(1))
        }
        [Some("wait-no-network"), Some(secs)] => wait_no_network(secs),
        [Some("wait-allowlist"), Some(secs)] => wait_allowlist(secs),
        [Some("--version" | "-V")] => {
            println!("dagron-ci-exec {VERSION} (step protocols 1, 2, 3, 4 and 5)");
            ExitCode::SUCCESS
        }
        [Some("--help" | "-h")] => {
            println!("{USAGE}");
            ExitCode::SUCCESS
        }
        _ => {
            eprintln!("dagron-ci-exec: unknown arguments\n{USAGE}");
            ExitCode::from(FAILED)
        }
    }
}

/// Hold this pod until its network is seen to be gone: see
/// [`dagron_ci_exec::gate`]. What it says is the init container's log, which
/// the executor gives as the reason a task did not start.
fn wait_no_network(secs: &str) -> ExitCode {
    let host = std::env::var("KUBERNETES_SERVICE_HOST").ok();
    let port = std::env::var("KUBERNETES_SERVICE_PORT").ok();
    // The pod's name servers. A pod that has no such file is asked at its
    // API server alone; one whose file cannot be read is not.
    let read = std::fs::read_to_string(dagron_ci_exec::gate::RESOLV_CONF);
    let resolv_conf = match dagron_ci_exec::gate::resolv_conf(read) {
        Ok(text) => text,
        Err(why) => {
            eprintln!("{why}");
            return ExitCode::from(FAILED);
        }
    };
    match dagron_ci_exec::gate::run(
        secs,
        host.as_deref(),
        port.as_deref(),
        resolv_conf.as_deref(),
    ) {
        Ok(said) => {
            println!("{said}");
            ExitCode::SUCCESS
        }
        Err(why) => {
            eprintln!("{why}");
            ExitCode::from(FAILED)
        }
    }
}

/// Print the instances of a job whose matrix an earlier job computed: see
/// [`dagron_ci_exec::plan`]. The array is all it prints to its standard
/// output: the engine reads the whole of it.
fn matrix() -> ExitCode {
    let vars = std::env::vars_os()
        .filter_map(|(name, value)| Some((name.into_string().ok()?, value.into_string().ok()?)))
        .collect();
    match dagron_ci_exec::plan::plan(&vars) {
        Ok(instances) => {
            println!("{instances}");
            ExitCode::SUCCESS
        }
        Err(why) => {
            eprintln!("::dagron-ci::error::{why}");
            ExitCode::from(1)
        }
    }
}

/// Hold this pod until it is seen to be held to its allowlist: see
/// [`dagron_ci_exec::gate`].
fn wait_allowlist(secs: &str) -> ExitCode {
    let host = std::env::var("KUBERNETES_SERVICE_HOST").ok();
    let port = std::env::var("KUBERNETES_SERVICE_PORT").ok();
    match dagron_ci_exec::gate::run_allowlist(secs, host.as_deref(), port.as_deref()) {
        Ok(said) => {
            println!("{said}");
            ExitCode::SUCCESS
        }
        Err(why) => {
            eprintln!("{why}");
            ExitCode::from(FAILED)
        }
    }
}

#[cfg(unix)]
fn run() -> ExitCode {
    // First, while this process is one thread: no step, nor anything one
    // starts, inherits the repository's secrets. A step has one where its
    // workflow reads it, as on GitHub.
    dagron_ci_exec::workflow::take_repository_secrets();
    use std::io::Write;

    use dagron_ci_exec::run;

    let job = match dagron_ci_exec::read(std::env::vars_os()) {
        Ok(job) => job,
        Err(why) => {
            eprintln!("dagron-ci-exec: {why}");
            return ExitCode::from(FAILED);
        }
    };
    if let Err(e) = run::catch_cancel_signals() {
        eprintln!("dagron-ci-exec: the cancelling signals cannot be caught: {e}");
        return ExitCode::from(FAILED);
    }
    run::become_subreaper();

    // Under protocol 3 the job is one stream: standard error is made the
    // same descriptor as standard output, for this process and the steps it
    // starts. What a later job is given of this one is the last line it
    // printed, and two streams are put in one order only by whoever reads
    // them, which need not be the order they were written in.
    if job.protocol >= 3 {
        // SAFETY: both are this process's own standard streams, and nothing
        // else of it is running yet.
        if unsafe { libc::dup2(1, 2) } < 0 {
            // A job that cannot say its last line last would be misread by
            // the jobs after it: it does not run.
            eprintln!(
                "dagron-ci-exec: standard error could not be made one stream with standard \
                 output: {}",
                std::io::Error::last_os_error()
            );
            return ExitCode::from(FAILED);
        }
    }

    // And under protocol 3 the stream is read for GitHub's workflow
    // commands as it is copied: what a step asks to have masked
    // (`::add-mask::`), with the job's own credentials from the start, and
    // what it asks to have annotated (`::error::`). A job that cannot be
    // given that does not run: it would print what it was told would be
    // masked.
    let relay = if job.protocol >= 3 {
        use dagron_ci_exec::commands::{Commands, Relay, CREDENTIALS};
        // And the repository's secrets the engine gave it.
        let credentials = CREDENTIALS
            .iter()
            .filter_map(|name| std::env::var(name).ok())
            .chain(
                dagron_ci_exec::workflow::repository_secrets()
                    .into_iter()
                    .map(|(_, value)| value),
            );
        let workspace = std::env::current_dir()
            .ok()
            .map(|w| w.display().to_string());
        let commands = Commands::new(workspace, credentials)
            .with_debug(dagron_ci_exec::workflow::step_debug());
        match Relay::start(commands) {
            Ok(relay) => Some(relay),
            Err(e) => {
                eprintln!("dagron-ci-exec: the job's stream could not be masked: {e}");
                return ExitCode::from(FAILED);
            }
        }
    } else {
        None
    };

    // Unbuffered: each marker is written whole, in order with the steps'
    // own stderr, which is this same descriptor. Where the stream is read,
    // each of this process's own lines is printed by the relay in its place,
    // and no step's line passes for one of them.
    let mut log: Box<dyn Write> = match &relay {
        Some(relay) => Box::new(relay.own()),
        None => Box::new(std::io::stderr()),
    };
    caches("restore", &mut log);
    // A GitLab job's artifacts: those of the jobs it waits for, in the
    // workspace before its first step; and its own, uploaded when they are
    // over, as they went. One that cannot be had, or sent, fails the job.
    let artifacts = dagron_ci_exec::artifacts::asked();
    let here = std::env::current_dir().unwrap_or_default();
    // Whether its artifacts could not be fetched or sent: a failure of the
    // job's handing on, not of its own, which `allow_failure` does not end
    // as success, since the jobs after it would run without them.
    let mut handing_on_failed = false;
    let mut result = match dagron_ci_exec::artifacts::fetch(&artifacts, &here, &mut log) {
        Ok(()) => run::run(&job, &mut log),
        Err(why) => {
            handing_on_failed = true;
            let _ = writeln!(
                log,
                "::dagron-ci::error::the artifacts this job needs could not be fetched: {why}"
            );
            dagron_ci_exec::result::JobResult {
                exit_code: 1,
                cancelled_by: None,
                steps: job
                    .steps
                    .iter()
                    .map(dagron_ci_exec::result::StepResult::skipped)
                    .collect(),
                protocol: job.protocol,
                said: None,
                summary: String::new(),
            }
        }
    };
    if result.exit_code == 0 {
        caches("save", &mut log);
    }
    if let Some(upload) = &artifacts.upload {
        if result.cancelled_by.is_none() && upload.after(result.exit_code) {
            if let Err(why) = dagron_ci_exec::artifacts::send(upload, &here, &mut log) {
                handing_on_failed = true;
                let _ = writeln!(
                    log,
                    "::dagron-ci::error::the job's artifacts could not be uploaded: {why}"
                );
                if result.exit_code == 0 {
                    result.exit_code = 1;
                }
            }
        }
    }
    // A GitLab job that may fail: it failed, and is said to have, and ends
    // as succeeded, so that what comes after it runs as GitLab runs it.
    if result.exit_code != 0
        && result.cancelled_by.is_none()
        && !handing_on_failed
        && std::env::var("CI_JOB_ALLOW_FAILURE").as_deref() == Ok("true")
    {
        let _ = writeln!(
            log,
            "::dagron-ci::warning::the job failed (exit {}), and may: allow_failure",
            result.exit_code
        );
        result.exit_code = 0;
    }
    // The steps are over: every line of theirs read, and every mask known,
    // before anything of the job's own is said. What the job says of itself
    // is masked before it is cut, split or made JSON, so that a masked value
    // is masked in it whatever form it takes there.
    if let Some(relay) = &relay {
        relay.settle();
    }
    let mask = |text: &str| match &relay {
        Some(relay) => relay.mask(text),
        None => text.to_string(),
    };
    // An output that holds a masked value is not handed on, as on GitHub.
    if let (Some(relay), Some(said)) = (&relay, result.said.as_mut()) {
        let mut kept = Vec::new();
        for (name, value) in std::mem::take(&mut said.outputs) {
            if relay.masks(&value) || relay.masks(&name) {
                let _ = writeln!(
                    log,
                    "::dagron-ci::warning::the output {} holds a masked value: it is not \
                     handed on",
                    mask(&name)
                );
            } else {
                kept.push((name, value));
            }
        }
        said.outputs = kept;
    }
    // What the job has to say of its tests, and what its steps wrote to
    // their summaries: said before the result, which stays the last of the
    // job's own lines, and each in lines short enough for any reader.
    let workspace = std::env::current_dir().unwrap_or_default();
    // A report is a file the job's steps wrote, and reading one changes
    // nothing about how the job ended: not if the reading itself fails.
    let report = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        dagron_ci_exec::report::read_masked(&job.reports, &workspace, &mask)
    }))
    .unwrap_or_else(|_| {
        Some(dagron_ci_exec::report::Read {
            problems: vec!["the job's reports could not be read: the reading failed".into()],
            ..Default::default()
        })
    });
    if let Some(report) = report {
        for line in report.lines() {
            let _ = writeln!(log, "{line}");
        }
    }
    for line in dagron_ci_exec::report::summary_lines(&mask(&result.summary)) {
        let _ = writeln!(log, "{line}");
    }
    let json = masked_json(result.to_json(), &mask).to_string();
    if let Some(path) = &job.result_file {
        if let Err(e) = write_result(path, &json) {
            let _ = writeln!(
                log,
                "::dagron-ci::warning::the result could not be written to {}: {e}",
                path.display()
            );
        }
    }
    let _ = writeln!(log, "::dagron-ci::result::{json}");
    // And last, under protocol 3, the line the jobs after this one read.
    if let Some(said) = &result.said {
        let (line, left_out) = dagron_ci_exec::job::said(said.result, &said.outputs);
        if left_out {
            let _ = writeln!(
                log,
                "::dagron-ci::warning::the job's outputs are more than a job hands on ({} bytes \
                 with its result, on one line): the jobs after it are given none of them",
                dagron_ci_exec::job::SAID_MAX
            );
        }
        let _ = writeln!(log, "{line}");
    }
    // Every line of the job's, its last among them, has gone into the pipe
    // in its order: all of it is printed, and nothing after.
    drop(log);
    if let Some(relay) = relay {
        relay.finish();
    }
    ExitCode::from(u8::try_from(result.exit_code).unwrap_or(FAILED))
}

/// `value` with each of its strings and keys through `mask`, its shape as
/// it was: JSON still, however short a masked value.
#[cfg(unix)]
fn masked_json(value: serde_json::Value, mask: &dyn Fn(&str) -> String) -> serde_json::Value {
    use serde_json::Value;
    match value {
        Value::String(s) => Value::String(mask(&s)),
        Value::Array(items) => {
            Value::Array(items.into_iter().map(|v| masked_json(v, mask)).collect())
        }
        Value::Object(map) => Value::Object(
            map.into_iter()
                .map(|(k, v)| (mask(&k), masked_json(v, mask)))
                .collect(),
        ),
        other => other,
    }
}

/// The task's path caches (`workspace.cache`), where the engine gave it some
/// (`DAGRON_CACHE`): `dagron-ci-cache restore` before the steps, and `save`
/// after they all succeed. It is installed beside this binary; it is its own
/// so that this one stays a runner of steps with nothing to fetch. A cache
/// that is not restored or saved never changes how the job ends.
#[cfg(unix)]
fn caches(what: &str, log: &mut impl std::io::Write) {
    if std::env::var_os("DAGRON_CACHE").is_none() {
        return;
    }
    let beside = std::env::current_exe()
        .ok()
        .and_then(|me| me.parent().map(|dir| dir.join("dagron-ci-cache")))
        .filter(|path| path.is_file());
    let program = beside.unwrap_or_else(|| "dagron-ci-cache".into());
    // What it says goes where the job's markers go, standard error, and not
    // after the steps' own output, where a later job reads the last line.
    let said = std::process::Stdio::from(std::io::stderr());
    if let Err(e) = std::process::Command::new(&program)
        .arg(what)
        .stdout(said)
        .status()
    {
        let _ = writeln!(
            log,
            "::dagron-ci::warning::the job's caches were not {}: dagron-ci-cache could not be started ({e})",
            if what == "restore" { "restored" } else { "saved" }
        );
    }
}

/// Write the result beside where it goes, then move it there, so a reader
/// never sees half of one.
#[cfg(unix)]
fn write_result(path: &std::path::Path, json: &str) -> std::io::Result<()> {
    if let Some(dir) = path.parent().filter(|d| !d.as_os_str().is_empty()) {
        std::fs::create_dir_all(dir)?;
    }
    let name = path.file_name().unwrap_or_default().to_string_lossy();
    let partial = path.with_file_name(format!(".{name}.partial"));
    std::fs::write(&partial, format!("{json}\n"))?;
    std::fs::rename(&partial, path)
}

/// Copy this binary into `dir`: what an init container runs to put it in a
/// task's workspace, from an image with nothing else in it. `dagron-ci-cache`
/// goes with it where the image has it beside this one, and, `with_node`,
/// the Node.js a JavaScript action runs on (`node` beside this binary, a
/// directory), which an image that holds none fails to install.
#[cfg(unix)]
fn install(dir: &std::path::Path, with_node: bool) -> ExitCode {
    use std::os::unix::fs::PermissionsExt;

    let put = |from: &std::path::Path, name: &str| -> std::io::Result<std::path::PathBuf> {
        let installed = dir.join(name);
        let partial = dir.join(format!(".{name}.partial"));
        std::fs::copy(from, &partial)?;
        std::fs::set_permissions(&partial, std::fs::Permissions::from_mode(0o755))?;
        std::fs::rename(&partial, &installed)?;
        Ok(installed)
    };
    let copy = || -> std::io::Result<std::path::PathBuf> {
        let me = std::env::current_exe()?;
        std::fs::create_dir_all(dir)?;
        let installed = put(&me, "dagron-ci-exec")?;
        if let Some(cache) = me
            .parent()
            .map(|d| d.join("dagron-ci-cache"))
            .filter(|p| p.is_file())
        {
            put(&cache, "dagron-ci-cache")?;
        }
        if with_node {
            let node = me
                .parent()
                .map(|d| d.join("node"))
                .filter(|p| p.join("bin/node").is_file())
                .ok_or_else(|| {
                    std::io::Error::other(
                        "this image holds no Node.js to install (a node directory beside \
                         dagron-ci-exec)",
                    )
                })?;
            // Whole, or not at all: copied beside, then put in its place.
            let partial = dir.join(".node.partial");
            let _ = std::fs::remove_dir_all(&partial);
            copy_tree(&node, &partial)?;
            // One installed before goes; one that cannot go is said, not
            // left to make the rename below fail for another reason.
            match std::fs::remove_dir_all(dir.join("node")) {
                Err(e) if e.kind() != std::io::ErrorKind::NotFound => return Err(e),
                _ => {}
            }
            std::fs::rename(&partial, dir.join("node"))?;
        }
        Ok(installed)
    };
    match copy() {
        Ok(installed) => {
            println!("{}", installed.display());
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("dagron-ci-exec: cannot install into {}: {e}", dir.display());
            ExitCode::from(FAILED)
        }
    }
}

#[cfg(not(unix))]
fn run() -> ExitCode {
    eprintln!("dagron-ci-exec runs on Unix only");
    ExitCode::from(FAILED)
}

/// Copy the tree at `from` to `to`, files with their modes and links as
/// links: the Node.js of an image, whose loader its binary names by path.
#[cfg(unix)]
fn copy_tree(from: &std::path::Path, to: &std::path::Path) -> std::io::Result<()> {
    std::fs::create_dir_all(to)?;
    for entry in std::fs::read_dir(from)? {
        let entry = entry?;
        let kind = entry.file_type()?;
        let target = to.join(entry.file_name());
        if kind.is_symlink() {
            std::os::unix::fs::symlink(std::fs::read_link(entry.path())?, &target)?;
        } else if kind.is_dir() {
            copy_tree(&entry.path(), &target)?;
        } else {
            std::fs::copy(entry.path(), &target)?;
        }
    }
    Ok(())
}

#[cfg(not(unix))]
fn install(_: &std::path::Path, _: bool) -> ExitCode {
    run()
}
