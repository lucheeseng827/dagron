//! The `dagron-ci` command line: the service, and the commands that look
//! after it. It lives in the library so that an alternate build's binary is
//! the same commands with its own [`Seams`].
//!
//! ```text
//! dagron-ci [serve]                  serve until SIGTERM or ctrl-c
//! dagron-ci repo add HOST/NAME       connect a repository (--forge github by default,
//!                                    --mode native), optionally with the workflow
//!                                    its events run
//! dagron-ci repo set HOST/NAME       change the workflow, the cell its events run
//!                                    on, how often it is polled, or its mode
//! dagron-ci repo list                list connected repositories, and how polling goes
//! dagron-ci repo show HOST/NAME      one repository, connected or not, and who
//!                                    connected, changed and disconnected it
//! dagron-ci repo rm HOST/NAME        disconnect one: its events and runs are kept,
//!                                    and the events that wait are not run
//! dagron-ci events                   the newest events, their state and why
//! dagron-ci fleet usage              what fleet mode's runners did, by repository,
//!                                    team or label, over whole days (UTC)
//! dagron-ci validate [PATH…]         check pipeline files (.dagron/ci by default)
//! dagron-ci compile FILE --event …   the workflow a pipeline compiles to for an event
//! dagron-ci run [FILE…]              run a checkout's pipelines on a local engine
//! dagron-ci run --remote …           ask a server to run a repository's pipelines at a ref
//! dagron-ci rerun ID [--all]         run a run's failed jobs again, or every job, on a server
//! dagron-ci cancel ID                stop a run, on a server
//! dagron-ci logs ID                  what a run's jobs wrote, from a server
//! dagron-ci import [FILE…]           another CI's files as pipelines, held to the reader
//!                                    and compiler (`import.rs`)
//! dagron-ci login                    keep the caller's token for a server
//! dagron-ci logout                   forget it
//! dagron-ci schema                   print the pipeline format's JSON Schema
//! dagron-ci version
//! ```
//!
//! `serve`, `repo`, `events` and `fleet` read the service's environment
//! (CI_DATABASE_URL and the rest; see the README). `validate`, `compile`,
//! `run`, `import` and `schema` need no database: they run in a developer's
//! checkout, or a pre-commit hook (`run` needs a `dagron` engine, and Docker
//! unless told otherwise). `run --remote`, `rerun`, `cancel` and `logs` need
//! a server and the caller's token for it, and nothing else; `login` keeps
//! that token, once the server has taken it (`remote.rs`).

use std::collections::BTreeMap;
use std::process::ExitCode;

use crate::config::{MAX_POLL_SECS, MIN_POLL_SECS};
use crate::db::{Connected, Db, Mode};
use crate::pipeline::compile::{Changes, Event, Outcome, Settings};
use crate::{Config, Seams};

const USAGE: &str = "usage: dagron-ci [serve]
       dagron-ci repo add HOST/NAME [--forge github|gitlab|gitea|forgejo|bitbucket|generic]
                                    [--workflow ID] [--cell NAME] [--poll-secs N|default]
                                    [--mode native|compat|fleet]
       dagron-ci repo set HOST/NAME [--workflow ID | --no-workflow] [--cell NAME]
                                    [--poll-secs N|default] [--mode native|compat|fleet]
       dagron-ci repo list
       dagron-ci repo show HOST/NAME
       dagron-ci repo rm HOST/NAME
       dagron-ci events [--state new|claimed|done|skipped|failed] [--limit N]
       dagron-ci fleet usage [--since YYYY-MM-DD] [--until YYYY-MM-DD]
                             [--by repo|team|label]
       dagron-ci validate [FILE|DIR]...
       dagron-ci compile FILE --event push|tag|pull_request|merge_group|dispatch|failed --ref REF
                         [--sha SHA] [--base-ref BRANCH] [--base-sha SHA] [--pr N]
                         [--repo HOST/OWNER/NAME] [--input NAME=VALUE]... [--changed PATH]...
                         [--json]
       dagron-ci run [FILE]... [--executor docker|local] [--engine PATH] [--keep]
                     [--event …] [--ref …] and compile's other event flags
       dagron-ci schema
       dagron-ci version";

/// The usage, with `import` and the commands that speak to a server.
fn usage() -> String {
    format!(
        "{USAGE}\n{}\n{}",
        crate::import::USAGE,
        crate::remote::USAGE
    )
}

const FORGES: &[&str] = &[
    "github",
    "gitlab",
    "gitea",
    "forgejo",
    "bitbucket",
    "generic",
];

/// Run one command. `binary` names the program in its logs and its version
/// line; `seams` is called once, after logging starts, and only to serve.
pub async fn run(binary: &str, args: &[String], seams: impl FnOnce() -> Seams) -> ExitCode {
    let args: Vec<&str> = args.iter().map(String::as_str).collect();
    let result = match args.as_slice() {
        [] | ["serve"] => serve(binary, seams).await,
        ["repo", rest @ ..] => repo(rest).await,
        ["events", rest @ ..] => events(rest).await,
        ["fleet", "usage", rest @ ..] => fleet_usage(rest).await,
        ["validate", paths @ ..] => return validate(paths),
        ["compile", rest @ ..] => return compile(rest),
        ["run", rest @ ..] if rest.contains(&"--remote") => {
            return crate::remote::run("run", rest).await
        }
        ["run", rest @ ..] => return crate::run_local::run(rest).await,
        ["import", rest @ ..] => return crate::import::run(rest),
        [command @ ("rerun" | "cancel" | "logs" | "login" | "logout"), rest @ ..] => {
            return crate::remote::run(command, rest).await
        }
        ["schema"] => {
            print!("{}", crate::pipeline::SCHEMA);
            Ok(())
        }
        // The release's version, not this crate's: the crates of a release
        // are not numbered with it.
        ["version" | "--version" | "-V"] => {
            println!("{}", dagron_logging::version_line(binary));
            Ok(())
        }
        ["help" | "--help" | "-h"] => {
            println!("{}", usage());
            Ok(())
        }
        _ => {
            eprintln!("{}", usage());
            return ExitCode::from(2);
        }
    };
    match result {
        Ok(()) => ExitCode::SUCCESS,
        Err(e) => {
            eprintln!("{binary}: {e:#}");
            ExitCode::FAILURE
        }
    }
}

async fn serve(binary: &str, seams: impl FnOnce() -> Seams) -> anyhow::Result<()> {
    dagron_logging::init(binary);
    let seams = seams();
    let result = match Config::from_env() {
        Ok(config) => crate::serve(config, seams).await,
        Err(e) => Err(e),
    };
    dagron_logging::shutdown();
    result
}

/// The flags `repo add` and `repo set` take, each at most once.
#[derive(Debug, Default, PartialEq, Eq)]
struct RepoFlags<'a> {
    forge: Option<&'a str>,
    /// `Some(None)` is `--no-workflow`.
    workflow: Option<Option<&'a str>>,
    cell: Option<&'a str>,
    /// `Some(None)` is `--poll-secs default`: CI_POLL_SECS's interval.
    poll_secs: Option<Option<u64>>,
    mode: Option<Mode>,
}

fn repo_flags<'a>(mut flags: &[&'a str]) -> anyhow::Result<RepoFlags<'a>> {
    let mut out = RepoFlags::default();
    loop {
        flags = match flags {
            [] => return Ok(out),
            ["--forge", forge, rest @ ..] if out.forge.is_none() && FORGES.contains(forge) => {
                out.forge = Some(forge);
                rest
            }
            ["--workflow", id, rest @ ..] if out.workflow.is_none() && is_name(id) => {
                out.workflow = Some(Some(id));
                rest
            }
            ["--no-workflow", rest @ ..] if out.workflow.is_none() => {
                out.workflow = Some(None);
                rest
            }
            ["--cell", cell, rest @ ..] if out.cell.is_none() && is_name(cell) => {
                out.cell = Some(cell);
                rest
            }
            ["--poll-secs", secs, rest @ ..] if out.poll_secs.is_none() => {
                out.poll_secs = Some(poll_secs(secs)?);
                rest
            }
            ["--mode", mode, rest @ ..] if out.mode.is_none() && Mode::parse(mode).is_some() => {
                out.mode = Mode::parse(mode);
                rest
            }
            _ => anyhow::bail!("{USAGE}"),
        };
    }
}

/// `--poll-secs`: `default`, 0 (never), or an interval CI_POLL_SECS could
/// take.
fn poll_secs(v: &str) -> anyhow::Result<Option<u64>> {
    if v == "default" {
        return Ok(None);
    }
    match v.parse::<u64>() {
        Ok(n) if n == 0 || (MIN_POLL_SECS..=MAX_POLL_SECS).contains(&n) => Ok(Some(n)),
        _ => anyhow::bail!(
            "--poll-secs takes default, 0 (never), or {MIN_POLL_SECS} to {MAX_POLL_SECS}, not {v:?}"
        ),
    }
}

/// A workflow id or a cell name: what goes into a URL path or a lookup key.
fn is_name(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= 128
        && s.bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
}

/// Who a change made at a terminal is recorded as: `cli:` and the
/// operator's name, as the environment has it (`DAGRON_CI_ACTOR`, else the
/// user's). `dagron-ci repo` writes the database with the database's own
/// credentials, so this is who the operator says they are, and no more: a
/// change by a caller dagron-api has identified goes through
/// `/ci/v1/repos`.
fn cli_actor() -> String {
    let name = ["DAGRON_CI_ACTOR", "USER", "LOGNAME", "USERNAME"]
        .iter()
        .filter_map(|key| std::env::var(key).ok())
        .map(|name| {
            name.chars()
                .filter(|c| !c.is_control())
                .take(128)
                .collect::<String>()
                .trim()
                .to_string()
        })
        .find(|name| !name.is_empty());
    match name {
        Some(name) => format!("cli:{name}"),
        None => "cli".to_string(),
    }
}

async fn repo(args: &[&str]) -> anyhow::Result<()> {
    let db = Db::connect(&Config::from_env()?).await?;
    let actor = cli_actor();
    let result = match args {
        ["list"] => {
            for listed in db.list_repos().await? {
                let r = &listed.repo;
                let poll = match listed.poll_secs {
                    None => "default".to_string(),
                    Some(0) => "off".to_string(),
                    Some(n) => format!("{n}s"),
                };
                let error = listed
                    .poll_error
                    .map(|e| format!("\tpoll_error={e}"))
                    .unwrap_or_default();
                println!(
                    "{}/{}\t{}\tmode={}\tworkflow={}\tcell={}\tpoll={poll}\tpolled={}\t{}{error}",
                    r.forge_host,
                    r.full_name,
                    r.forge,
                    r.mode.as_str(),
                    r.workflow.as_deref().unwrap_or("-"),
                    r.cell,
                    listed.polled_at.as_deref().unwrap_or("never"),
                    r.id
                );
            }
            Ok(())
        }
        ["add", spec, flags @ ..] => {
            let (host, name) = split_spec(spec)?;
            let flags = repo_flags(flags)?;
            if flags.workflow == Some(None) {
                anyhow::bail!("--no-workflow is for `repo set`; leave --workflow out instead");
            }
            // Connected again, a repository is on the forge it was on, unless
            // this says otherwise.
            let forge = match flags.forge {
                Some(forge) => forge.to_string(),
                None => db
                    .repo_any(host, name)
                    .await?
                    .map_or_else(|| "github".to_string(), |(repo, _)| repo.forge),
            };
            let workflow = flags.workflow.flatten();
            let cell = flags.cell.unwrap_or("default");
            let poll = flags.poll_secs.flatten();
            let mode = flags.mode.unwrap_or_default();
            let made = db
                .connect_repo(&forge, host, name, workflow, cell, poll, mode, &actor)
                .await?;
            match made {
                Connected::New(r) => {
                    println!("connected {}/{} ({})", r.forge_host, r.full_name, r.id)
                }
                // It was disconnected: its events and runs are under it still.
                Connected::Again(r) => {
                    println!(
                        "connected again {}/{} ({})",
                        r.forge_host, r.full_name, r.id
                    )
                }
                Connected::Already => println!("{host}/{name} is connected already"),
            }
            Ok(())
        }
        ["set", spec, flags @ ..] => {
            let (host, name) = split_spec(spec)?;
            let flags = repo_flags(flags)?;
            let binds = flags.workflow.is_some() || flags.cell.is_some();
            if flags.forge.is_some()
                || (!binds && flags.poll_secs.is_none() && flags.mode.is_none())
            {
                anyhow::bail!("{USAGE}");
            }
            let found = db
                .change_repo(
                    host,
                    name,
                    flags.workflow,
                    flags.cell,
                    flags.poll_secs,
                    flags.mode,
                    &actor,
                )
                .await?;
            match found {
                Some(true) => println!("updated {host}/{name}"),
                Some(false) => println!("{host}/{name} is as it was"),
                None => anyhow::bail!("{host}/{name} is not connected"),
            }
            Ok(())
        }
        ["show", spec] => {
            let (host, name) = split_spec(spec)?;
            match db.repo_any(host, name).await? {
                Some((r, since)) => {
                    let state = match since {
                        Some(at) => format!("disconnected since {at}"),
                        None => "connected".to_string(),
                    };
                    println!(
                        "{}/{}\t{}\tmode={}\tworkflow={}\tcell={}\t{state}\t{}",
                        r.forge_host,
                        r.full_name,
                        r.forge,
                        r.mode.as_str(),
                        r.workflow.as_deref().unwrap_or("-"),
                        r.cell,
                        r.id
                    );
                    for change in db.repo_changes(&r.id).await? {
                        println!(
                            "{}\t{}\t{}\t{}",
                            change.at, change.actor, change.action, change.detail
                        );
                    }
                    Ok(())
                }
                None => Err(anyhow::anyhow!("{host}/{name} was never connected")),
            }
        }
        ["rm", spec] => {
            let (host, name) = split_spec(spec)?;
            if db.disconnect_repo(host, name, &actor).await?.is_some() {
                println!("disconnected {host}/{name}: its events and runs are kept");
                Ok(())
            } else {
                anyhow::bail!("{host}/{name} is not connected")
            }
        }
        _ => Err(anyhow::anyhow!("{USAGE}")),
    };
    db.close().await;
    result
}

/// What `fleet usage` is asked: the days, and what it is by.
fn usage_flags<'a>(
    args: &[&'a str],
) -> anyhow::Result<(Option<&'a str>, Option<&'a str>, crate::fleet::usage::By)> {
    let (mut since, mut until, mut by) = (None, None, crate::fleet::usage::By::Repository);
    let mut rest = args;
    loop {
        rest = match rest {
            [] => break,
            ["--since", day, tail @ ..] if since.is_none() => {
                since = Some(*day);
                tail
            }
            ["--until", day, tail @ ..] if until.is_none() => {
                until = Some(*day);
                tail
            }
            ["--by", what, tail @ ..] => {
                by = crate::fleet::usage::By::parse(what)
                    .ok_or_else(|| anyhow::anyhow!("--by is repo, team or label, not {what:?}"))?;
                tail
            }
            _ => anyhow::bail!("{USAGE}"),
        };
    }
    Ok((since, until, by))
}

async fn fleet_usage(args: &[&str]) -> anyhow::Result<()> {
    let (since, until, by) = usage_flags(args)?;
    let (since, until) = crate::fleet::usage::window(since, until, crate::db::Day::today())
        .map_err(|why| anyhow::anyhow!(why))?;
    let db = Db::connect(&Config::from_env()?).await?;
    let report = crate::fleet::usage::usage(&db, since, until, by).await;
    db.close().await;
    print!("{}", report?.table());
    Ok(())
}

async fn events(args: &[&str]) -> anyhow::Result<()> {
    let (mut state, mut limit) = ("", 50_i64);
    let mut rest = args;
    loop {
        rest = match rest {
            [] => break,
            ["--state", s, tail @ ..]
                if ["new", "claimed", "done", "skipped", "failed"].contains(s) =>
            {
                state = s;
                tail
            }
            ["--limit", n, tail @ ..] => {
                limit = n
                    .parse()
                    .ok()
                    .filter(|n| (1..=1000).contains(n))
                    .ok_or_else(|| anyhow::anyhow!("--limit must be 1 to 1000"))?;
                tail
            }
            _ => anyhow::bail!("{USAGE}"),
        };
    }
    let db = Db::connect(&Config::from_env()?).await?;
    let result = db.list_events(state, limit).await;
    db.close().await;
    for e in result? {
        println!(
            "{}\t{}\t{}\t{} {}\t{}\tattempts={}\t{}",
            e.id,
            e.state,
            e.repo,
            e.kind,
            e.git_ref,
            &e.sha[..e.sha.len().min(12)],
            e.attempts,
            e.reason.as_deref().unwrap_or("")
        );
    }
    Ok(())
}

/// `dagron-ci validate`: each pipeline file, `ok` or every problem in it.
/// Exits 1 when any file has a problem, 2 when there is nothing to check.
///
/// A file under `.github/workflows` is checked as a GitHub workflow, by
/// the front-end that would run it, with the repository's settings
/// (`.dagron/ci.yaml` of the directory this is run in, where there is
/// one); and that settings file is checked as itself. A `.gitlab-ci.yml`
/// (or the file the settings name) is checked as a GitLab pipeline, with
/// the files it includes read from here, and made for a push to `main`.
fn validate(paths: &[&str]) -> ExitCode {
    use crate::pipeline::{github, gitlab};
    // The repository's settings, for what a workflow file is checked
    // against. One with a mistake in it is said where it is named.
    let settings = std::fs::read_to_string(github::SETTINGS_FILE)
        .ok()
        .and_then(|text| github::settings(&text).ok())
        .unwrap_or_default();
    let is_settings = |file: &std::path::Path| {
        file.file_name().is_some_and(|n| n == "ci.yaml")
            && file
                .parent()
                .and_then(|p| p.file_name())
                .is_some_and(|n| n == ".dagron")
    };
    let is_gitlab = |file: &std::path::Path| is_gitlab(file, &settings);
    let paths: Vec<&str> = if paths.is_empty() {
        vec![".dagron/ci"]
    } else {
        paths.to_vec()
    };
    let mut files = Vec::new();
    for path in &paths {
        let path = std::path::Path::new(path);
        if path.is_dir() {
            let mut found: Vec<_> = match std::fs::read_dir(path) {
                Ok(entries) => entries
                    .filter_map(Result::ok)
                    .map(|e| e.path())
                    .filter(|p| {
                        p.is_file()
                            && matches!(
                                p.extension().and_then(|e| e.to_str()),
                                Some("yaml" | "yml")
                            )
                    })
                    .collect(),
                Err(e) => {
                    eprintln!("{}: {e}", path.display());
                    return ExitCode::from(2);
                }
            };
            found.sort();
            files.extend(found);
        } else {
            files.push(path.to_path_buf());
        }
    }
    if files.is_empty() {
        eprintln!("no pipeline files in {}", paths.join(", "));
        return ExitCode::from(2);
    }
    let mut clean = true;
    for file in files {
        let shown = file.display();
        let text = match std::fs::read_to_string(&file) {
            Ok(text) => text,
            Err(e) => {
                println!("{shown}: {e}");
                clean = false;
                continue;
            }
        };
        if is_settings(&file) {
            match github::settings(&text) {
                Ok(read) => println!(
                    "{shown}: ok (settings: {} workflow file{} listed, {} label{} given an image)",
                    read.workflows.len(),
                    if read.workflows.len() == 1 { "" } else { "s" },
                    read.runs_on.len(),
                    if read.runs_on.len() == 1 { "" } else { "s" },
                ),
                Err(problems) => {
                    clean = false;
                    for problem in problems {
                        println!("{shown}: {problem}");
                    }
                }
            }
            continue;
        }
        if is_gitlab(&file) {
            // What it includes, from the directory this is run in.
            // What it includes is the project's, from its root: the checkout the
            // file is in, else the directory this is run in.
            let root = file
                .canonicalize()
                .ok()
                .and_then(|f| {
                    f.ancestors()
                        .skip(1)
                        .find(|d| d.join(".git").exists())
                        .map(std::path::Path::to_path_buf)
                })
                .unwrap_or_else(|| std::path::PathBuf::from("."));
            let mut included = std::collections::BTreeMap::new();
            let mut queue = gitlab::local_includes(&text).unwrap_or_default();
            while let Some(path) = queue.pop() {
                if included.contains_key(&path) || included.len() >= gitlab::MAX_FILES {
                    continue;
                }
                // From the checkout, and from nowhere a link in it leads.
                let read = crate::import::within(&root, &path)
                    .and_then(|file| std::fs::read_to_string(file).map_err(|e| e.to_string()))
                    .map_err(|why| format!("cannot be read: {why}"));
                if let Ok(text) = &read {
                    queue.extend(gitlab::local_includes(text).unwrap_or_default());
                }
                included.insert(path, read);
            }
            let read = gitlab::parse(&text, &included);
            let push = crate::pipeline::compile::Event {
                kind: "push".into(),
                git_ref: "refs/heads/main".into(),
                sha: "0".repeat(40),
                trust: "trusted".into(),
                repo: "group/project".into(),
                forge: "gitlab".into(),
                forge_host: "gitlab.example".into(),
                default_branch: Some("main".into()),
                changed: crate::pipeline::compile::Changes::Unlisted("validate".into()),
                ..Default::default()
            };
            let made = read.and_then(|ci| {
                ci.lower("gitlab-ci", &push, &settings.gitlab)
                    .map(|lowered| (ci, lowered))
                    .map_err(|problems| {
                        problems
                            .into_iter()
                            .map(|p| crate::pipeline::Problem {
                                path: p.path,
                                message: format!("for a push to main: {}", p.message),
                            })
                            .collect()
                    })
            });
            match made {
                Ok((ci, lowered)) => {
                    let runs = match &lowered.passed_over {
                        Some(why) => format!("a push to main runs none: {why}"),
                        None => format!(
                            "a push to main runs {} task{}",
                            lowered.pipeline.tasks.len(),
                            if lowered.pipeline.tasks.len() == 1 {
                                ""
                            } else {
                                "s"
                            }
                        ),
                    };
                    println!(
                        "{shown}: ok (GitLab pipeline: {} job{}, {} file{} included; {runs})",
                        ci.jobs.len(),
                        if ci.jobs.len() == 1 { "" } else { "s" },
                        included.len(),
                        if included.len() == 1 { "" } else { "s" },
                    );
                }
                Err(problems) => {
                    clean = false;
                    for problem in problems {
                        println!("{shown}: {problem}");
                    }
                }
            }
            continue;
        }
        if is_workflow(&file) {
            let stem = file
                .file_stem()
                .map(|s| s.to_string_lossy().into_owned())
                .unwrap_or_default();
            match github::check(&text, &stem, &settings) {
                Ok((workflow, jobs)) => {
                    let on = [
                        ("push", workflow.on.push.is_some()),
                        ("pull_request", workflow.on.pull_request.is_some()),
                        ("merge_group", workflow.on.merge_group.is_some()),
                        ("workflow_dispatch", workflow.on.dispatch.is_some()),
                        ("schedule", !workflow.on.schedule.is_empty()),
                    ]
                    .iter()
                    .filter(|(_, set)| *set)
                    .map(|(name, _)| *name)
                    .collect::<Vec<_>>()
                    .join(", ");
                    let never = if workflow.unheard.is_empty() {
                        String::new()
                    } else {
                        format!("; never for {}", workflow.unheard.join(", "))
                    };
                    let listed = if settings.runs(&format!(
                        "{}",
                        file.file_name().unwrap_or_default().to_string_lossy()
                    )) {
                        ""
                    } else {
                        "; not listed in .dagron/ci.yaml, so not run"
                    };
                    println!(
                        "{shown}: ok (workflow {stem}: {jobs} job{}; on {}{never}{listed})",
                        if jobs == 1 { "" } else { "s" },
                        if on.is_empty() {
                            "nothing dagron-ci makes an event for"
                        } else {
                            &on
                        },
                    );
                }
                Err(problems) => {
                    clean = false;
                    for problem in problems {
                        println!("{shown}: {problem}");
                    }
                }
            }
            continue;
        }
        match crate::pipeline::parse(&text) {
            Ok(p) => {
                let on = [
                    ("push", p.on.push.is_some()),
                    ("pull_request", p.on.pull_request.is_some()),
                    ("merge_group", p.on.merge_group.is_some()),
                    ("dispatch", p.on.dispatch.is_some()),
                    ("schedule", !p.on.schedule.is_empty()),
                    ("failed", p.on.failed.is_some()),
                ]
                .iter()
                .filter(|(_, set)| *set)
                .map(|(name, _)| *name)
                .collect::<Vec<_>>()
                .join(", ");
                println!(
                    "{shown}: ok ({}: {} task{}; on {on})",
                    p.name,
                    p.tasks.len(),
                    if p.tasks.len() == 1 { "" } else { "s" }
                );
            }
            Err(problems) => {
                clean = false;
                for problem in problems {
                    println!("{shown}: {problem}");
                }
            }
        }
    }
    if clean {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// Whether `file` is a GitHub workflow: a file of `.github/workflows`.
pub(crate) fn is_workflow(file: &std::path::Path) -> bool {
    let mut parts: Vec<_> = file.components().rev().skip(1).take(2).collect();
    parts.reverse();
    parts.len() == 2 && parts[0].as_os_str() == ".github" && parts[1].as_os_str() == "workflows"
}

/// Whether `file` is a GitLab pipeline: a `.gitlab-ci.yml`, or the file the
/// repository's settings name for one.
pub(crate) fn is_gitlab(
    file: &std::path::Path,
    settings: &crate::pipeline::github::Settings,
) -> bool {
    use crate::pipeline::gitlab;
    let name = file.to_string_lossy().replace('\\', "/");
    let name = name.trim_start_matches("./");
    name.ends_with(gitlab::DEFAULT_FILE) || settings.gitlab.pipeline.as_deref() == Some(name)
}

/// The flags `compile` and `run` describe an event with, as given. Each
/// command fills in what is not given its own way, and checks the result.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct EventFlags {
    pub kind: Option<String>,
    pub git_ref: Option<String>,
    pub sha: Option<String>,
    pub base_sha: Option<String>,
    pub base_ref: Option<String>,
    pub pr: Option<i64>,
    /// The forge's host and the repository's full name, lowercased.
    pub repo: Option<(String, String)>,
    pub inputs: BTreeMap<String, String>,
    /// `--changed`, each file once per flag.
    pub changed: Option<Vec<String>>,
}

impl EventFlags {
    /// Take the event flag `args` starts with, if it starts with one: the
    /// arguments after it, `None` when it starts with something else, or
    /// why the flag's value is refused.
    pub(crate) fn take<'a>(
        &mut self,
        args: &'a [&'a str],
    ) -> Result<Option<&'a [&'a str]>, String> {
        let (flag, value, tail) = match args {
            [flag, value, tail @ ..] if flag.starts_with("--") => (*flag, *value, tail),
            _ => return Ok(None),
        };
        match flag {
            "--event" => {
                if !EVENTS.contains(&value) {
                    return Err(format!(
                        "--event {value:?} is not an event a pipeline runs on"
                    ));
                }
                self.kind = Some(value.into());
            }
            "--ref" => self.git_ref = Some(value.into()),
            "--sha" => self.sha = Some(value.into()),
            "--base-sha" => self.base_sha = Some(value.into()),
            "--base-ref" => self.base_ref = Some(value.into()),
            "--pr" => match value.parse() {
                Ok(n) if n > 0 => self.pr = Some(n),
                _ => return Err("--pr takes a pull request's number".into()),
            },
            "--repo" => {
                let (host, name) = split_spec(value).map_err(|e| e.to_string())?;
                self.repo = Some((host.to_ascii_lowercase(), name.to_ascii_lowercase()));
            }
            "--input" => match value.split_once('=') {
                Some((name, v)) => {
                    self.inputs.insert(name.into(), v.into());
                }
                None => return Err("--input takes NAME=VALUE".into()),
            },
            "--changed" => self.changed.get_or_insert_with(Vec::new).push(value.into()),
            _ => return Ok(None),
        }
        Ok(Some(tail))
    }

    /// The event these flags describe, over `defaults`, checked: a full
    /// commit id for each sha, a ref that can be stored.
    pub(crate) fn event(&self, defaults: Event) -> Result<Event, String> {
        let mut event = defaults;
        if let Some(kind) = &self.kind {
            event.kind = kind.clone();
        }
        if let Some(r) = &self.git_ref {
            event.git_ref = r.clone();
        }
        if let Some(sha) = &self.sha {
            event.sha = sha.clone();
        }
        event.base_sha = self.base_sha.clone().or(event.base_sha);
        event.base_ref = self.base_ref.clone().or(event.base_ref);
        event.pr = self.pr.or(event.pr);
        if let Some((host, name)) = &self.repo {
            event.forge_host = host.clone();
            event.repo = name.clone();
        }
        event.inputs.extend(self.inputs.clone());
        if let Some(files) = &self.changed {
            event.changed = Changes::Listed(files.clone());
        }
        for sha in std::iter::once(&event.sha).chain(event.base_sha.iter()) {
            if !crate::event::is_commit_id(sha) {
                return Err(format!("{sha:?} is not a full commit id"));
            }
        }
        if !crate::event::is_storable_ref(&event.git_ref) {
            return Err("the ref is empty, too long, or holds a control character".into());
        }
        Ok(event)
    }
}

/// The events a pipeline runs on.
/// `failed` takes the failed run's facts as `--input`s (`ci_failed_pipeline`
/// and the rest), as the event that raised it carries them.
const EVENTS: &[&str] = &[
    "push",
    "tag",
    "pull_request",
    "merge_group",
    "dispatch",
    "failed",
];

/// `dagron-ci compile`: what FILE compiles to for the event the flags
/// describe. Exits 0 with the workflow (`--json`: the body `POST /api/runs`
/// takes), 3 when the pipeline does not run for the event, 1 when the
/// pipeline or the request is refused, 2 for a usage error.
fn compile(args: &[&str]) -> ExitCode {
    let usage = |why: &str| {
        eprintln!("dagron-ci compile: {why}\n{USAGE}");
        ExitCode::from(2)
    };
    let mut file = None;
    let mut json = false;
    let mut flags = EventFlags::default();
    let mut rest = args;
    loop {
        match flags.take(rest) {
            Ok(Some(tail)) => {
                rest = tail;
                continue;
            }
            Ok(None) => {}
            Err(why) => return usage(&why),
        }
        rest = match rest {
            [] => break,
            ["--json", tail @ ..] => {
                json = true;
                tail
            }
            [path, tail @ ..] if file.is_none() && !path.starts_with("--") => {
                file = Some(*path);
                tail
            }
            [other, ..] => return usage(&format!("{other:?} is not a flag compile takes")),
        };
    }
    let Some(file) = file else {
        return usage("name the pipeline file");
    };
    if flags.kind.is_none() || flags.git_ref.is_none() {
        return usage("--event and --ref are required");
    }
    let event = match flags.event(Event {
        sha: "0".repeat(40),
        trust: "trusted".into(),
        repo: "local/checkout".into(),
        forge_host: "localhost".into(),
        ..Event::default()
    }) {
        Ok(event) => event,
        Err(why) => return usage(&why),
    };
    let settings = match crate::config::default_timeout_secs(&|n: &str| std::env::var(n).ok()) {
        Ok(default_timeout_secs) => Settings {
            default_timeout_secs,
            ..Settings::default()
        },
        Err(e) => {
            eprintln!("dagron-ci compile: {e}");
            return ExitCode::from(2);
        }
    };

    let text = match std::fs::read_to_string(file) {
        Ok(text) => text,
        Err(e) => {
            eprintln!("{file}: {e}");
            return ExitCode::FAILURE;
        }
    };
    let pipeline = match crate::pipeline::parse(&text) {
        Ok(p) => p,
        Err(problems) => {
            for problem in problems {
                eprintln!("{file}: {problem}");
            }
            return ExitCode::FAILURE;
        }
    };
    match crate::pipeline::compile::compile(&pipeline, &event, &settings) {
        Ok(Outcome::Run(c)) => {
            if json {
                let body = serde_json::json!({ "yaml": c.yaml, "parameters": c.parameters });
                println!("{body}");
            } else {
                println!(
                    "# {} runs for {} {} at {}.",
                    pipeline.name, event.kind, event.git_ref, event.sha
                );
                println!("# Its parameters (POST /api/runs takes them with the workflow):");
                for (name, value) in &c.parameters {
                    println!("#   {name}: {}", serde_json::Value::from(value.as_str()));
                }
                print!("{}", c.yaml);
            }
            ExitCode::SUCCESS
        }
        Ok(Outcome::Skip(why) | Outcome::Unchanged { why, .. }) => {
            eprintln!("{file}: {} does not run: {why}", pipeline.name);
            ExitCode::from(3)
        }
        Ok(Outcome::NeedsChangedFiles) => {
            eprintln!(
                "{file}: {} filters on paths: name the files the event changed with --changed",
                pipeline.name
            );
            ExitCode::from(2)
        }
        Err(why) => {
            eprintln!("{file}: {why}");
            ExitCode::FAILURE
        }
    }
}

/// `github.com/acme/widgets` into its host and its full name. GitLab's nested
/// groups keep every segment after the host: `gitlab.example/g/sub/project`.
/// Each segment is a name a forge allows (letters, digits, `-`, `_`, `.`),
/// since the poll sweep puts them into URLs; the host may carry a port.
pub(crate) fn split_spec(spec: &str) -> anyhow::Result<(&str, &str)> {
    let segment = |s: &str| {
        !s.is_empty()
            && s != "."
            && s != ".."
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_' | b'.'))
    };
    let host_ok = |host: &str| match host.split_once(':') {
        Some((name, port)) => {
            segment(name) && !port.is_empty() && port.bytes().all(|b| b.is_ascii_digit())
        }
        None => segment(host),
    };
    match spec.trim_matches('/').split_once('/') {
        Some((host, name))
            if host_ok(host) && name.contains('/') && name.split('/').all(segment) =>
        {
            Ok((host, name))
        }
        _ => anyhow::bail!("{spec:?} is not HOST/OWNER/NAME, such as github.com/acme/widgets"),
    }
}

#[cfg(test)]
mod tests {
    use super::{repo_flags, split_spec, usage_flags, RepoFlags};
    use crate::db::Mode;
    use crate::fleet::usage::By;

    #[test]
    fn fleet_usage_is_asked_for_days_and_by_what() {
        assert_eq!(usage_flags(&[]).unwrap(), (None, None, By::Repository));
        assert_eq!(
            usage_flags(&["--by", "team", "--since", "2026-10-01"]).unwrap(),
            (Some("2026-10-01"), None, By::Team)
        );
        for bad in [
            &["--by", "cost"][..],
            &["--since", "a", "--since", "b"],
            &["--until"],
            &["--per", "day"],
        ] {
            assert!(usage_flags(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn repo_flags_are_each_taken_once() {
        assert_eq!(repo_flags(&[]).unwrap(), RepoFlags::default());
        assert_eq!(
            repo_flags(&["--workflow", "wf-1", "--cell", "eu", "--forge", "gitlab"]).unwrap(),
            RepoFlags {
                forge: Some("gitlab"),
                workflow: Some(Some("wf-1")),
                cell: Some("eu"),
                poll_secs: None,
                mode: None,
            }
        );
        assert_eq!(
            repo_flags(&["--mode", "fleet"]).unwrap().mode,
            Some(Mode::Fleet)
        );
        assert_eq!(
            repo_flags(&["--poll-secs", "600"]).unwrap().poll_secs,
            Some(Some(600))
        );
        assert_eq!(
            repo_flags(&["--poll-secs", "0"]).unwrap().poll_secs,
            Some(Some(0))
        );
        assert_eq!(
            repo_flags(&["--poll-secs", "default"]).unwrap().poll_secs,
            Some(None)
        );
        assert_eq!(
            repo_flags(&["--no-workflow"]).unwrap(),
            RepoFlags {
                workflow: Some(None),
                ..RepoFlags::default()
            }
        );
        for bad in [
            &["--workflow"][..],
            &["--workflow", "a", "--workflow", "b"],
            &["--workflow", "a", "--no-workflow"],
            &["--workflow", "../admin"],
            &["--cell", "eu west"],
            &["--forge", "svn"],
            &["--color"],
            &["--poll-secs", "10"],
            &["--poll-secs", "5m"],
            &["--poll-secs", "60", "--poll-secs", "60"],
            &["--mode", "push"],
            &["--mode", "fleet", "--mode", "native"],
        ] {
            assert!(repo_flags(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn a_spec_is_host_then_the_full_name() {
        assert_eq!(
            split_spec("github.com/acme/widgets").unwrap(),
            ("github.com", "acme/widgets")
        );
        assert_eq!(
            split_spec("gitlab.example/group/sub/project").unwrap(),
            ("gitlab.example", "group/sub/project")
        );
        assert_eq!(
            split_spec("ghe.example:8443/acme/widgets.js").unwrap(),
            ("ghe.example:8443", "acme/widgets.js")
        );
        for bad in [
            "github.com",
            "github.com/acme",
            "/acme/widgets",
            "github.com/acme/",
            "github.com/acme/../admin",
            "github.com/acme/w?x=1",
            "github.com/acme//widgets",
            "user@github.com/acme/widgets",
            "github.com:/acme/widgets",
        ] {
            assert!(split_spec(bad).is_err(), "{bad:?}");
        }
    }
}
