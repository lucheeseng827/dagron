//! The commands that speak to a dagron-ci server: `run --remote`, `rerun`,
//! `cancel` and `logs`. Each asks the run API's door of that name (`api.rs`)
//! as the person at the keyboard. And `login` and `logout`, which keep the
//! person's token for a server, and forget it (`remote/login.rs`).
//!
//! **Where, and as whom.** The server is `--server`, DAGRON_CI_URL, or the
//! one logged in to last. The credential is the run API's, a dagron-api
//! session or personal access token, read from `--token-file`,
//! DAGRON_CI_TOKEN_FILE or DAGRON_CI_TOKEN, in that order, else the one
//! `dagron-ci login` kept for that server and no other. No flag takes the
//! token itself: a process list would show it.
//!
//! **Never in the clear off this machine.** The server is `https://`, or
//! `http://` to this machine, as a cell's dagron-api is (`config.rs`): the
//! token goes there. And nowhere else: a redirect is not followed, and is
//! said as the answer it is.
//!
//! **Which run.** `ID` is what `run --remote` printed, an event's id; or an
//! engine run's, the one a commit status links to, which means that run
//! alone.
//!
//! **A change nobody pushed.** `run --remote --bundle-from DIR` makes a patch
//! bundle of the git working tree at DIR (`remote/bundle.rs`), uploads it,
//! and runs the pipelines on its base with it laid over. Such a run is
//! untrusted, and the forge is told nothing of it.
//!
//! **Exit codes.** 0 when the server did what was asked. 1 when it refused,
//! could not be reached, or, for `run --wait`, something did not succeed. 2
//! for a usage error. 124 when `--timeout` ran out, as timeout(1) says it.

use std::io::Write;
use std::process::ExitCode;
use std::time::{Duration, Instant};

use reqwest::Method;
use serde_json::{json, Value};

mod bundle;
mod login;

pub(crate) const USAGE: &str =
    "       dagron-ci run --remote --repo HOST/OWNER/NAME --ref REF [--sha SHA]
                     [--bundle-from DIR] [--pipeline NAME] [--input NAME=VALUE]...
                     [--key KEY] [--wait [--timeout SECS]]
       dagron-ci rerun ID [--all]
       dagron-ci cancel ID
       dagron-ci logs ID [--job NAME]... [--failed] [--grep TEXT] [--tail N] [--json]
                     [--follow [--from-start]]
         each of those four with [--server URL] [--token-file PATH], or
         DAGRON_CI_URL and DAGRON_CI_TOKEN_FILE or DAGRON_CI_TOKEN, or what
         `dagron-ci login` kept; ID is what `run --remote` printed, or the
         run a commit status links to
       dagron-ci login [--server URL] [--token-file PATH]
         the token from the file, those variables, or standard input
       dagron-ci logout [--server URL]";

/// How long one call to the server may take: a log is read from its cell
/// behind it.
const TIMEOUT: Duration = Duration::from_secs(60);

/// How long a followed log may say nothing before its connection is taken
/// for lost: the server says something every 15 s while it waits.
const QUIET: Duration = Duration::from_secs(60);

/// How many times a followed log may be cut off, one after another, before
/// the follow gives up on it.
const MAX_DROPS: u32 = 10;
/// How long a bundle's upload may take: up to `bundles::MAX_INLINE`.
const UPLOAD_TIMEOUT: Duration = Duration::from_secs(300);

/// How many times in a row `run --wait` may fail to read its event before it
/// gives up, and how often `cancel` asks again of an event that is still
/// being dispatched.
const MAX_MISSES: u32 = 5;
const MAX_ASKS: u32 = 30;

/// Where a command reads its environment, and what it prints and says to.
type Env<'a> = &'a (dyn Fn(&str) -> Option<String> + Sync);
type Out<'a> = &'a mut (dyn Write + Send);

/// How long the commands wait between looks.
#[derive(Clone, Copy)]
struct Pace {
    /// Between two reads of a run that is waited for.
    poll: Duration,
    /// Before a cancel is asked again.
    again: Duration,
}

const PACE: Pace = Pace {
    poll: Duration::from_secs(2),
    again: Duration::from_secs(1),
};

/// Run `command`: `run` (with `--remote` among `args`), `rerun`, `cancel`,
/// `logs`, `login` or `logout`.
pub async fn run(command: &str, args: &[&str]) -> ExitCode {
    let env = |name: &str| std::env::var(name).ok().filter(|v| !v.is_empty());
    let (mut out, mut err) = (std::io::stdout(), std::io::stderr());
    let mut ask = login::typed;
    ExitCode::from(dispatch(command, args, &env, &mut ask, PACE, &mut out, &mut err).await)
}

async fn dispatch(
    command: &str,
    args: &[&str],
    env: Env<'_>,
    ask: login::Ask<'_>,
    pace: Pace,
    out: Out<'_>,
    err: Out<'_>,
) -> u8 {
    let done = match command {
        "run" => dispatch_run(args, env, pace, out, err).await,
        "rerun" => rerun(args, env, out, err).await,
        "cancel" => cancel(args, env, pace, out, err).await,
        "logs" => logs(args, env, pace, out, err).await,
        "login" => login::login(args, env, ask, out, err).await,
        "logout" => login::logout(args, env, out, err),
        _ => Err(Stop::Usage(format!("{command:?} is not a command"))),
    };
    match done {
        Ok(code) => code,
        Err(Stop::Usage(why)) => {
            let _ = writeln!(err, "dagron-ci {command}: {why}\nusage:\n{USAGE}");
            2
        }
        Err(Stop::Failed(why)) => {
            let _ = writeln!(err, "dagron-ci {command}: {why}");
            1
        }
    }
}

/// Why a command stopped short.
#[derive(Debug)]
enum Stop {
    /// Its arguments or its environment: exit 2, with the usage.
    Usage(String),
    /// The server's refusal, or that it could not be reached: exit 1.
    Failed(String),
}

fn usage<T>(why: impl Into<String>) -> Result<T, Stop> {
    Err(Stop::Usage(why.into()))
}

/// `--server` and `--token-file`, which every command here takes.
#[derive(Debug, Default)]
struct Target<'a> {
    server: Option<&'a str>,
    token_file: Option<&'a str>,
}

impl<'a> Target<'a> {
    /// Take the flag `args` starts with, if it is one of the two: what is
    /// left after it.
    fn take(&mut self, args: &'a [&'a str]) -> Option<&'a [&'a str]> {
        match args {
            ["--server", url, tail @ ..] if self.server.is_none() => {
                self.server = Some(url);
                Some(tail)
            }
            ["--token-file", path, tail @ ..] if self.token_file.is_none() => {
                self.token_file = Some(path);
                Some(tail)
            }
            _ => None,
        }
    }

    /// The token the flag or the environment gives, if either does.
    fn given(&self, env: Env<'_>) -> Result<Option<String>, Stop> {
        let file = self
            .token_file
            .map(str::to_string)
            .or_else(|| env("DAGRON_CI_TOKEN_FILE"));
        match (file, env("DAGRON_CI_TOKEN")) {
            (Some(path), _) => match std::fs::read_to_string(&path) {
                Ok(text) => Ok(Some(text)),
                Err(e) => usage(format!("the token file {path}: {e}")),
            },
            (None, token) => Ok(token),
        }
    }

    /// The server these flags and the environment name, or the one logged in
    /// to last, and the caller's token for it: from the flag or the
    /// environment, else the one kept for that server.
    fn server(&self, env: Env<'_>) -> Result<Server, Stop> {
        let named = self
            .server
            .map(str::to_string)
            .or_else(|| env("DAGRON_CI_URL"));
        let given = self.given(env)?;
        // What `login` kept is read only where it is wanted.
        let stored = match (&named, &given) {
            (Some(_), Some(_)) => None,
            _ => login::read(env)?,
        };
        let Some(url) = named.or_else(|| stored.as_ref().and_then(|s| s.default.clone())) else {
            return usage(
                "name the dagron-ci server with --server or DAGRON_CI_URL, or log in to one \
                 with `dagron-ci login`",
            );
        };
        let base = checked(&url)?;
        let kept = stored.and_then(|mut stored| stored.servers.remove(&base));
        let Some(token) = given.or(kept.map(|kept| kept.token)) else {
            return usage(format!(
                "give the caller's dagron-api token with --token-file, DAGRON_CI_TOKEN_FILE or \
                 DAGRON_CI_TOKEN, or log in to {base} with `dagron-ci login`"
            ));
        };
        Server::new(base, &token)
    }
}

/// The server `url` names, as the commands send to it and `login` keeps a
/// token for it: `https://`, or `http://` to this machine, since the token
/// goes there.
fn checked(url: &str) -> Result<String, Stop> {
    let given = url.trim().trim_end_matches('/');
    let Ok(parsed) = reqwest::Url::parse(given) else {
        return usage(format!("the server {given:?} is not a URL"));
    };
    match parsed.scheme() {
        "https" => {}
        "http" if crate::config::to_this_machine(&parsed) => {}
        _ => {
            return usage(format!(
                "the server {given:?} must be https://, or http:// to this machine: the \
                 caller's token is sent there"
            ))
        }
    }
    if parsed.host_str().unwrap_or("").is_empty()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return usage(format!(
            "the server {given:?} takes no user, query or fragment"
        ));
    }
    // As the URL reads parsed: one server is one key of what `login` keeps,
    // however it was written.
    Ok(parsed.as_str().trim_end_matches('/').to_string())
}

/// A dagron-ci server, and the caller's token for it.
struct Server {
    base: String,
    token: String,
    http: crate::config::Clients,
    stream: crate::config::Clients,
}

impl Server {
    /// `base`, checked, with `token`: one word.
    fn new(base: String, token: &str) -> Result<Server, Stop> {
        let token = token.trim();
        if token.is_empty() || token.contains(char::is_whitespace) {
            return usage("the token is empty, or is more than one word");
        }
        Ok(Server {
            base,
            token: token.to_string(),
            // The token is for the server that was named: for no place that
            // server points at, and, where the server is this machine, for
            // no proxy the environment names.
            http: crate::config::Clients::new(|| {
                reqwest::Client::builder()
                    .timeout(TIMEOUT)
                    .redirect(reqwest::redirect::Policy::none())
                    .user_agent(concat!("dagron-ci/", env!("CARGO_PKG_VERSION")))
            }),
            // A followed log lasts as long as its run: no bound on the whole,
            // one on a silence.
            stream: crate::config::Clients::new(|| {
                reqwest::Client::builder()
                    .connect_timeout(Duration::from_secs(15))
                    .read_timeout(QUIET)
                    .redirect(reqwest::redirect::Policy::none())
                    .user_agent(concat!("dagron-ci/", env!("CARGO_PKG_VERSION")))
            }),
        })
    }
}

/// What the server answered.
struct Answer {
    status: u16,
    /// Its body: JSON, or its text as a string when it is something else.
    body: Value,
    /// Whether it said when to ask again (`Retry-After`).
    retry: bool,
}

impl Answer {
    /// The server's refusal, in its words: a problem document's title and
    /// detail, or whatever else it sent.
    fn refusal(&self) -> String {
        let status = self.status;
        match (&self.body["title"], &self.body["detail"], &self.body) {
            (Value::String(title), Value::String(detail), _) => {
                format!("{title}: {detail} (HTTP {status})")
            }
            (_, _, Value::String(text)) if !text.trim().is_empty() => {
                format!("{} (HTTP {status})", text.trim())
            }
            _ => format!("the server answered HTTP {status}"),
        }
    }
}

/// An error with what caused it: reqwest's own text stops at "error sending
/// request", and the cause is what a person needs.
fn chain(e: &dyn std::error::Error) -> String {
    let mut said = e.to_string();
    let mut cause = e.source();
    while let Some(inner) = cause {
        said.push_str(": ");
        said.push_str(&inner.to_string());
        cause = inner.source();
    }
    said
}

impl Server {
    fn request(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
    ) -> reqwest::RequestBuilder {
        self.http
            .to(&self.base)
            .request(method, format!("{}{path}", self.base))
            .bearer_auth(&self.token)
            .query(query)
    }

    async fn ask(
        &self,
        method: Method,
        path: &str,
        query: &[(&str, &str)],
        body: Option<&Value>,
        key: Option<&str>,
    ) -> Result<Answer, Stop> {
        let mut request = self.request(method, path, query);
        if let Some(body) = body {
            request = request.json(body);
        }
        if let Some(key) = key {
            request = request.header("idempotency-key", key);
        }
        self.answer(request).await
    }

    /// Upload a bundle (`POST /ci/v1/bundles`), its bytes as the body.
    async fn upload(&self, query: &[(&str, &str)], bytes: Vec<u8>) -> Result<Answer, Stop> {
        let request = self
            .request(Method::POST, "/ci/v1/bundles", query)
            .timeout(UPLOAD_TIMEOUT)
            .header("content-type", "application/octet-stream")
            .body(bytes);
        self.answer(request).await
    }

    async fn answer(&self, request: reqwest::RequestBuilder) -> Result<Answer, Stop> {
        let unreachable = |e: reqwest::Error| Stop::Failed(chain(&e));
        let response = request.send().await.map_err(unreachable)?;
        let status = response.status().as_u16();
        let retry = response.headers().contains_key("retry-after");
        let text = response.text().await.map_err(unreachable)?;
        Ok(Answer {
            status,
            body: serde_json::from_str(&text).unwrap_or(Value::String(text)),
            retry,
        })
    }
}

/// A followed log, opened: its stream, or why it was not, and when to ask
/// again (a server busy, or not reached).
enum Opened {
    Stream(reqwest::Response),
    /// Not now: after this long; and whether its server is there and busy
    /// (it asked to be asked again), or not there at all.
    Later(Duration, bool),
}

impl Server {
    /// Open `run`'s followed log (`GET /ci/v1/runs/{run}/logs?follow=true`).
    async fn follow(&self, run: &str, query: &[(&str, &str)]) -> Result<Opened, Stop> {
        let response = self
            .stream
            .to(&self.base)
            .get(format!("{}/ci/v1/runs/{run}/logs", self.base))
            .bearer_auth(&self.token)
            .header("accept", "text/event-stream")
            .query(&[("follow", "true")])
            .query(query)
            .send()
            .await;
        let response = match response {
            Ok(response) => response,
            Err(_) => return Ok(Opened::Later(Duration::from_secs(2), false)),
        };
        let status = response.status().as_u16();
        if status == 200 {
            return Ok(Opened::Stream(response));
        }
        let after = response
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.trim().parse::<u64>().ok());
        let text = response.text().await.unwrap_or_default();
        let answer = Answer {
            status,
            body: serde_json::from_str(&text).unwrap_or(Value::String(text)),
            retry: after.is_some(),
        };
        match status {
            429 | 409 | 503 => Ok(Opened::Later(
                Duration::from_secs(after.unwrap_or(2).clamp(1, 60)),
                status == 429,
            )),
            _ => Err(Stop::Failed(answer.refusal())),
        }
    }
}

/// An id a door's path takes: an event's, or an engine run's.
fn run_id<'a>(id: Option<&'a str>, command: &str) -> Result<&'a str, Stop> {
    match id {
        Some(id) if crate::rerun::is_run_id(id) => Ok(id),
        Some(id) => usage(format!("{id:?} is not a run's id")),
        None => usage(format!("name the run to {command}")),
    }
}

/// A count and its noun: `1 job`, `3 jobs`.
fn jobs(n: &Value) -> String {
    match n.as_u64() {
        Some(1) => "1 job".into(),
        Some(n) => format!("{n} jobs"),
        None => "its jobs".into(),
    }
}

fn text(v: &Value) -> &str {
    v.as_str().unwrap_or("")
}

/// `left` of a rerun's or a cancel's answer, each said on `err`.
fn say_left(answer: &Value, err: Out<'_>) {
    for run in answer["left"].as_array().into_iter().flatten() {
        let _ = writeln!(
            err,
            "left {} ({}): {}",
            text(&run["id"]),
            text(&run["pipeline"]),
            text(&run["why"])
        );
    }
}

/// `dagron-ci run --remote`: queue a dispatch (`POST /ci/v1/runs`) and print
/// its event's id. With `--wait`, wait for what it runs, and say how each
/// run ended.
async fn dispatch_run(
    args: &[&str],
    env: Env<'_>,
    pace: Pace,
    out: Out<'_>,
    err: Out<'_>,
) -> Result<u8, Stop> {
    let mut target = Target::default();
    let (mut repo, mut git_ref, mut sha, mut pipeline, mut key) = (None, None, None, None, None);
    let mut bundle_from = None;
    let mut inputs = serde_json::Map::new();
    let (mut wait, mut timeout) = (false, None);
    let mut rest = args;
    loop {
        if let Some(tail) = target.take(rest) {
            rest = tail;
            continue;
        }
        rest = match rest {
            [] => break,
            ["--remote", tail @ ..] => tail,
            ["--wait", tail @ ..] => {
                wait = true;
                tail
            }
            ["--timeout", secs, tail @ ..] => {
                match secs.parse::<u64>() {
                    Ok(secs) if secs > 0 => timeout = Some(Duration::from_secs(secs)),
                    _ => return usage("--timeout takes a number of seconds"),
                }
                tail
            }
            ["--input", pair, tail @ ..] => {
                let Some((name, value)) = pair.split_once('=') else {
                    return usage("--input takes NAME=VALUE");
                };
                inputs.insert(name.into(), value.into());
                tail
            }
            [flag, value, tail @ ..] => {
                let slot = match *flag {
                    "--repo" => &mut repo,
                    "--ref" => &mut git_ref,
                    "--sha" => &mut sha,
                    "--pipeline" => &mut pipeline,
                    "--key" => &mut key,
                    "--bundle-from" => &mut bundle_from,
                    other => return usage(format!("{other:?} is not a flag run --remote takes")),
                };
                if slot.replace(*value).is_some() {
                    return usage(format!("{flag} is given twice"));
                }
                tail
            }
            [other] => return usage(format!("{other:?} is not a flag run --remote takes")),
        };
    }
    let (Some(repo), Some(git_ref)) = (repo, git_ref) else {
        return usage("--repo and --ref are required");
    };
    if timeout.is_some() && !wait {
        return usage("--timeout is how long --wait waits");
    }
    if key.is_some() && bundle_from.is_some() {
        return usage(
            "--key is not taken with --bundle-from: each upload is a bundle of its own, so a \
             retry under the key would name another",
        );
    }
    let server = target.server(env)?;

    let mut body = json!({ "repo": repo, "ref": git_ref });
    match bundle_from {
        // The run is of the bundle's base, which `--sha` names where it is given.
        Some(dir) => body["bundle"] = json!(upload_bundle(&server, repo, dir, sha, err).await?),
        None => {
            if let Some(sha) = sha {
                body["sha"] = json!(sha);
            }
        }
    }
    if let Some(pipeline) = pipeline {
        body["pipeline"] = json!(pipeline);
    }
    if !inputs.is_empty() {
        body["inputs"] = Value::Object(inputs);
    }
    let answer = server
        .ask(Method::POST, "/ci/v1/runs", &[], Some(&body), key)
        .await?;
    if !matches!(answer.status, 200 | 202) {
        return Err(Stop::Failed(answer.refusal()));
    }
    let Some(id) = answer.body["id"]
        .as_str()
        .filter(|id| crate::rerun::is_run_id(id))
    else {
        return Err(Stop::Failed(
            "the server queued something, and gave no id".into(),
        ));
    };
    let _ = writeln!(out, "{id}");
    let _ = writeln!(
        err,
        "{}: {} at {}",
        if answer.status == 200 {
            "queued before, under the same key"
        } else {
            "queued"
        },
        text(&answer.body["ref"]),
        text(&answer.body["sha"])
    );
    if !wait {
        return Ok(0);
    }
    waited(&server, id, pace, timeout, out, err).await
}

/// Make the bundle of the working tree at `dir` since `base` and upload it
/// for `repo`: its id.
async fn upload_bundle(
    server: &Server,
    repo: &str,
    dir: &str,
    base: Option<&str>,
    err: Out<'_>,
) -> Result<String, Stop> {
    let made = bundle::make(dir, base).map_err(Stop::Failed)?;
    let answer = server
        .upload(&[("repo", repo), ("base", &made.base)], made.bytes)
        .await?;
    if answer.status != 201 {
        return Err(Stop::Failed(answer.refusal()));
    }
    let Some(id) = answer.body["id"]
        .as_str()
        .filter(|id| crate::bundles::is_bundle_id(id))
    else {
        return Err(Stop::Failed(
            "the server kept the bundle, and gave no id".into(),
        ));
    };
    let _ = writeln!(
        err,
        "bundle {id}: {} changed and {} deleted since {}, sha256 {}",
        plural(made.files, "file"),
        plural(made.deleted, "file"),
        made.base,
        text(&answer.body["sha256"])
    );
    Ok(id.to_string())
}

fn plural(n: usize, noun: &str) -> String {
    if n == 1 {
        format!("1 {noun}")
    } else {
        format!("{n} {noun}s")
    }
}

/// Wait for the event `id` to settle and each run it submitted to end, saying
/// each change on `err`, then each run and how it ended on `out`.
async fn waited(
    server: &Server,
    id: &str,
    pace: Pace,
    timeout: Option<Duration>,
    out: Out<'_>,
    err: Out<'_>,
) -> Result<u8, Stop> {
    let started = Instant::now();
    let path = format!("/ci/v1/runs/{id}");
    let mut seen: Vec<(String, String)> = Vec::new();
    let mut misses = 0;
    let event = loop {
        match server.ask(Method::GET, &path, &[], None, None).await {
            Ok(answer) if answer.status == 200 => {
                misses = 0;
                let event = answer.body;
                let state = text(&event["state"]).to_string();
                let runs = event["engine_runs"].as_array().cloned().unwrap_or_default();
                let mut now = vec![("event".to_string(), state.clone())];
                now.extend(runs.iter().map(|run| {
                    (
                        format!("{} ({})", text(&run["id"]), text(&run["pipeline"])),
                        text(&run["status"]).to_string(),
                    )
                }));
                for change in now.iter().filter(|change| !seen.contains(change)) {
                    let _ = writeln!(err, "{}: {}", change.0, change.1);
                }
                seen = now;
                let settled = matches!(state.as_str(), "done" | "skipped" | "failed");
                if settled && runs.iter().all(|run| run["status"] != "submitted") {
                    break event;
                }
            }
            // The server's own refusal ends the wait; one it passes on from
            // behind it, or none at all, is worth another look.
            Ok(answer) if matches!(answer.status, 401 | 403 | 404) => {
                return Err(Stop::Failed(answer.refusal()));
            }
            Ok(answer) => {
                misses += 1;
                if misses >= MAX_MISSES {
                    return Err(Stop::Failed(answer.refusal()));
                }
            }
            Err(stop) => {
                misses += 1;
                if misses >= MAX_MISSES {
                    return Err(stop);
                }
            }
        }
        if timeout.is_some_and(|limit| started.elapsed() >= limit) {
            let _ = writeln!(err, "dagron-ci run: {id} was not over in time; it runs on");
            return Ok(124);
        }
        tokio::time::sleep(pace.poll).await;
    };
    let runs = event["engine_runs"].as_array().cloned().unwrap_or_default();
    for run in &runs {
        let _ = writeln!(
            out,
            "{}\t{}\t{}",
            text(&run["id"]),
            text(&run["pipeline"]),
            text(&run["status"])
        );
    }
    let succeeded = event["state"] == "done"
        && !runs.is_empty()
        && runs.iter().all(|run| run["status"] == "succeeded");
    if !succeeded {
        let _ = writeln!(
            err,
            "dagron-ci run: {id} is {}: {}",
            text(&event["state"]),
            text(&event["reason"])
        );
    }
    Ok(u8::from(!succeeded))
}

/// The one `ID` a command takes, and its target flags; nothing else.
fn id_and_target<'a>(args: &'a [&'a str], command: &str) -> Result<(&'a str, Target<'a>), Stop> {
    let mut target = Target::default();
    let mut id = None;
    let mut rest = args;
    loop {
        if let Some(tail) = target.take(rest) {
            rest = tail;
            continue;
        }
        rest = match rest {
            [] => break,
            [value, tail @ ..] if id.is_none() && !value.starts_with("--") => {
                id = Some(*value);
                tail
            }
            [other, ..] => return usage(format!("{other:?} is not something {command} takes")),
        };
    }
    Ok((run_id(id, command)?, target))
}

/// `dagron-ci rerun ID`: run its failed and cancelled runs again, each from
/// its failed jobs (`POST /ci/v1/runs/{id}/rerun`). With `--all`, each of its
/// runs that is over, whole: every job (`?failed=false`).
async fn rerun(args: &[&str], env: Env<'_>, out: Out<'_>, err: Out<'_>) -> Result<u8, Stop> {
    let mut target = Target::default();
    let (mut id, mut all) = (None, false);
    let mut rest = args;
    loop {
        if let Some(tail) = target.take(rest) {
            rest = tail;
            continue;
        }
        rest = match rest {
            [] => break,
            ["--all", tail @ ..] if !all => {
                all = true;
                tail
            }
            [value, tail @ ..] if id.is_none() && !value.starts_with("--") => {
                id = Some(*value);
                tail
            }
            [other, ..] => return usage(format!("{other:?} is not something rerun takes")),
        };
    }
    let id = run_id(id, "rerun")?;
    let server = target.server(env)?;
    let path = format!("/ci/v1/runs/{id}/rerun");
    let query: &[(&str, &str)] = if all { &[("failed", "false")] } else { &[] };
    let answer = server.ask(Method::POST, &path, query, None, None).await?;
    if answer.status != 200 {
        return Err(Stop::Failed(answer.refusal()));
    }
    for run in answer.body["rerun"].as_array().into_iter().flatten() {
        let _ = writeln!(
            out,
            "{}\t{}\trerun\t{}",
            text(&run["id"]),
            text(&run["pipeline"]),
            jobs(&run["jobs"])
        );
    }
    say_left(&answer.body, err);
    Ok(0)
}

/// `dagron-ci cancel ID`: stop its runs, and the event if it has not been
/// dispatched (`POST /ci/v1/runs/{id}/cancel`). An event a worker is
/// dispatching is asked about again until it is not, so that a run submitted
/// a moment after the first cancel is stopped too.
async fn cancel(
    args: &[&str],
    env: Env<'_>,
    pace: Pace,
    out: Out<'_>,
    err: Out<'_>,
) -> Result<u8, Stop> {
    let (id, target) = id_and_target(args, "cancel")?;
    let server = target.server(env)?;
    let path = format!("/ci/v1/runs/{id}/cancel");
    let mut stopped = false;
    for _ in 0..MAX_ASKS {
        let answer = server.ask(Method::POST, &path, &[], None, None).await?;
        let again = match answer.status {
            200 => {
                stopped = true;
                for run in answer.body["cancelled"].as_array().into_iter().flatten() {
                    let _ = writeln!(
                        out,
                        "{}\t{}\tcancelled\t{}, {} of them still stopping",
                        text(&run["id"]),
                        text(&run["pipeline"]),
                        jobs(&run["jobs"]),
                        run["stopping"].as_u64().unwrap_or(0)
                    );
                }
                say_left(&answer.body, err);
                if answer.body["event"] == "withdrawn" {
                    let _ = writeln!(
                        err,
                        "{} was withdrawn: it had not been dispatched",
                        text(&answer.body["id"])
                    );
                }
                answer.body["event"] == "dispatching"
            }
            409 if answer.retry => true,
            // Nothing more to stop. That is the end of a cancel that stopped
            // something, and a refusal of one that did not.
            409 if stopped => return Ok(0),
            _ => return Err(Stop::Failed(answer.refusal())),
        };
        if !again {
            return Ok(0);
        }
        tokio::time::sleep(pace.again).await;
    }
    Err(Stop::Failed(format!(
        "{id} is still being dispatched: what it has submitted {}, and what it submits next is \
         not. Ask again",
        if stopped {
            "is cancelled"
        } else {
            "was not there to cancel"
        }
    )))
}

/// `dagron-ci logs ID`: what its runs wrote (`GET /ci/v1/runs/{id}/logs`),
/// a line a line as `[job] text`, or the door's answer as it came (`--json`).
async fn logs(
    args: &[&str],
    env: Env<'_>,
    pace: Pace,
    out: Out<'_>,
    err: Out<'_>,
) -> Result<u8, Stop> {
    let mut target = Target::default();
    let mut id = None;
    let mut query: Vec<(&str, &str)> = Vec::new();
    let (mut tail_of, mut raw) = (false, false);
    let (mut follow_it, mut from_start) = (false, false);
    let mut rest = args;
    loop {
        if let Some(tail) = target.take(rest) {
            rest = tail;
            continue;
        }
        rest = match rest {
            [] => break,
            ["--json", tail @ ..] => {
                raw = true;
                tail
            }
            ["--follow", tail @ ..] => {
                follow_it = true;
                tail
            }
            ["--from-start", tail @ ..] => {
                from_start = true;
                tail
            }
            ["--failed", tail @ ..] => {
                query.push(("status", "failed"));
                tail
            }
            ["--job", name, tail @ ..] => {
                query.push(("job", name));
                tail
            }
            ["--grep", text, tail @ ..] => {
                query.push(("q", text));
                tail
            }
            ["--tail", n, tail @ ..] if !tail_of => {
                if !n.parse::<u32>().is_ok_and(|n| n > 0) {
                    return usage("--tail takes a number of lines");
                }
                tail_of = true;
                query.push(("limit", n));
                query.push(("tail", "true"));
                tail
            }
            [value, tail @ ..] if id.is_none() && !value.starts_with("--") => {
                id = Some(*value);
                tail
            }
            [other, ..] => return usage(format!("{other:?} is not something logs takes")),
        };
    }
    let id = run_id(id, "read the logs of")?;
    if from_start && !follow_it {
        return usage("--from-start is where a followed log begins: it goes with --follow");
    }
    let server = target.server(env)?;
    if follow_it {
        return follow(&server, id, &query, from_start, raw, pace, out, err).await;
    }
    let path = format!("/ci/v1/runs/{id}/logs");
    let answer = server.ask(Method::GET, &path, &query, None, None).await?;
    if answer.status != 200 {
        return Err(Stop::Failed(answer.refusal()));
    }
    if raw {
        let _ = writeln!(out, "{}", answer.body);
        return Ok(0);
    }
    let runs = answer.body["runs"].as_array().cloned().unwrap_or_default();
    if runs.is_empty() {
        let _ = writeln!(
            err,
            "{} is {}, and has run nothing: {}",
            text(&answer.body["id"]),
            text(&answer.body["state"]),
            text(&answer.body["reason"])
        );
    }
    let mut unread = false;
    for run in &runs {
        let name = format!("{} ({})", text(&run["id"]), text(&run["pipeline"]));
        if let Some(why) = run["error"].as_str() {
            unread = true;
            let _ = writeln!(err, "== {name}: not read: {why}");
            continue;
        }
        let log = &run["logs"];
        let _ = writeln!(err, "== {name}: {}", text(&run["status"]));
        for line in log["lines"].as_array().into_iter().flatten() {
            let _ = writeln!(out, "[{}] {}", text(&line["task"]), text(&line["text"]));
        }
        if log["truncated"] == true {
            let _ = writeln!(
                err,
                "== {name}: {} of {} lines that match are shown; --tail N shows the last N",
                log["lines"].as_array().map_or(0, Vec::len),
                log["matched"]
            );
        }
        if log["eof"] == false {
            let _ = writeln!(err, "== {name}: still running; read again for more");
        }
    }
    Ok(u8::from(unread))
}

/// One run a follow follows: where it is in each of its jobs (their
/// cursors), each job's line not yet ended, the jobs whose end it said, and
/// whether its log is over.
struct Followed {
    pipeline: String,
    parser: crate::sse::Parser,
    /// The connection read now: what comes of one given up is not read.
    conn: u64,
    cursors: std::collections::BTreeMap<String, String>,
    /// Each job's line not yet ended: the label it is shown under, and it.
    partial: std::collections::BTreeMap<String, (String, String)>,
    /// The jobs whose end was said, at the cursor it was said at: a stream
    /// opened again says it again.
    ended: std::collections::BTreeMap<String, String>,
    /// Jobs whose ended log a stream opened again sends once more (their
    /// cursors were not sent): not shown twice.
    replaying: std::collections::BTreeSet<String>,
    done: bool,
    failed: bool,
    drops: u32,
}

/// What a followed run's connection gave: some of its bytes; its end, once,
/// whether it ended or failed; or that its server asked to be asked again
/// later, which is no drop.
enum Item {
    Bytes(axum::body::Bytes),
    End,
    /// The wait after a connection ended is over: open another.
    Open,
    /// The wait a server asked for is over (`busy`), or the one after it
    /// could not be reached or refused for now.
    Again {
        busy: bool,
    },
}

/// A cursor's writing: all of it but its byte.
fn writing(cursor: &str) -> &str {
    cursor
        .rsplit_once(':')
        .map_or(cursor, |(writing, _)| writing)
}

/// `dagron-ci logs ID --follow`: each of the event's runs followed through the
/// door, new ones as the event submits them, a line a line as `[job] text`
/// (`[pipeline job]` with more than one), or each event as it came, a JSON
/// object a line (`--json`). A stream cut off is opened again from where it
/// was. It ends once every run's log has ended and the event is over.
#[allow(clippy::too_many_arguments)]
async fn follow(
    server: &Server,
    id: &str,
    query: &[(&str, &str)],
    from_start: bool,
    raw: bool,
    pace: Pace,
    out: Out<'_>,
    err: Out<'_>,
) -> Result<u8, Stop> {
    use futures_util::StreamExt;
    let event_path = format!("/ci/v1/runs/{id}");
    let mut runs: std::collections::BTreeMap<String, Followed> = Default::default();
    let mut streams = futures_util::stream::SelectAll::new();
    let mut settled = false;
    let mut misses = 0;
    let mut look = Instant::now();
    // A connection's bytes, and then its end: one, whether its body ended or
    // failed (a body whose read timed out fails at every read after).
    let piece = |run: String, conn: u64, response: reqwest::Response| {
        futures_util::stream::unfold(Some(response.bytes_stream().boxed()), move |body| {
            let run = run.clone();
            async move {
                let mut body = body?;
                match body.next().await {
                    Some(Ok(bytes)) => Some(((run, conn, Item::Bytes(bytes)), Some(body))),
                    _ => Some(((run, conn, Item::End), None)),
                }
            }
        })
        .boxed()
    };
    // What a server asked to be waited, at the follow's pace (a second's
    // pace is the server's own).
    let asked = |after: Duration| after.mul_f64(pace.again.as_secs_f64());
    // A wait of one run's own, among its streams: the loop reads the other
    // runs' meanwhile.
    let later = |run: String, conn: u64, wait: Duration, item: Item| {
        futures_util::stream::once(async move {
            tokio::time::sleep(wait).await;
            (run, conn, item)
        })
        .boxed()
    };
    let open = |run: &str, followed: &Followed| {
        let mut ask: Vec<(&str, String)> = query.iter().map(|(k, v)| (*k, v.to_string())).collect();
        if from_start {
            ask.push(("from", "start".to_string()));
        }
        // Each job's cursor; past what a request carries, only those of the
        // jobs still going (an ended one sent again is not shown again).
        let all = followed.cursors.len() <= 1000;
        ask.extend(
            followed
                .cursors
                .iter()
                .filter(|(task, _)| all || !followed.ended.contains_key(*task))
                .map(|(_, c)| ("cursor", c.clone())),
        );
        let run = run.to_string();
        async move {
            let ask: Vec<(&str, &str)> = ask.iter().map(|(k, v)| (*k, v.as_str())).collect();
            server.follow(&run, &ask).await
        }
    };
    loop {
        if !settled && Instant::now() >= look {
            look = Instant::now() + pace.poll.max(Duration::from_millis(1)) * 2;
            match server.ask(Method::GET, &event_path, &[], None, None).await {
                Ok(answer) if answer.status == 200 => {
                    misses = 0;
                    let state = text(&answer.body["state"]).to_string();
                    let all = answer.body["engine_runs"]
                        .as_array()
                        .cloned()
                        .unwrap_or_default();
                    // An engine run's id names that run alone.
                    let named: Vec<&Value> = match all.iter().any(|run| run["id"] == id) {
                        true => all.iter().filter(|run| run["id"] == id).collect(),
                        false => all.iter().collect(),
                    };
                    for run in named {
                        let run_id = text(&run["id"]).to_string();
                        if runs.contains_key(&run_id) {
                            continue;
                        }
                        let followed = Followed {
                            pipeline: text(&run["pipeline"]).to_string(),
                            parser: Default::default(),
                            conn: 0,
                            cursors: Default::default(),
                            partial: Default::default(),
                            ended: Default::default(),
                            replaying: Default::default(),
                            done: false,
                            failed: false,
                            drops: 0,
                        };
                        match open(&run_id, &followed).await? {
                            Opened::Stream(response) => {
                                streams.push(piece(run_id.clone(), 0, response))
                            }
                            Opened::Later(after, busy) => streams.push(later(
                                run_id.clone(),
                                0,
                                asked(after),
                                Item::Again { busy },
                            )),
                        }
                        runs.insert(run_id, followed);
                    }
                    settled = matches!(state.as_str(), "done" | "skipped" | "failed");
                    if runs.is_empty() && !raw {
                        let _ = writeln!(err, "== {id}: {state}, and nothing run yet");
                    }
                }
                Ok(answer) if matches!(answer.status, 401 | 403 | 404) => {
                    return Err(Stop::Failed(answer.refusal()));
                }
                Ok(answer) => {
                    misses += 1;
                    if misses >= MAX_MISSES {
                        return Err(Stop::Failed(answer.refusal()));
                    }
                }
                Err(stop) => {
                    misses += 1;
                    if misses >= MAX_MISSES {
                        return Err(stop);
                    }
                }
            }
        }
        if settled && runs.values().all(|run| run.done) {
            break;
        }
        let many = runs.len() > 1;
        let next = tokio::select! {
            next = streams.next(), if !streams.is_empty() => next,
            _ = tokio::time::sleep_until(look.into()), if !settled => continue,
            else => break,
        };
        let Some((run_id, conn, item)) = next else {
            continue;
        };
        let Some(run) = runs.get_mut(&run_id) else {
            continue;
        };
        if conn != run.conn || run.done {
            continue;
        }
        match item {
            Item::Bytes(bytes) => {
                let frames = match run.parser.push(&bytes) {
                    Ok(frames) => frames,
                    Err(why) => {
                        let _ = writeln!(err, "== {run_id}: {why}");
                        run.failed = true;
                        run.done = true;
                        flush_all(run, raw, out);
                        continue;
                    }
                };
                if !frames.is_empty() {
                    run.drops = 0;
                }
                for frame in frames {
                    show(&run_id, run, frame, many, raw, out, err);
                }
            }
            // Cut off before its end, or not opened: again, from its cursors,
            // on a connection of its own.
            Item::End | Item::Open | Item::Again { .. } => {
                // A connection cut off, and an open its server refused or
                // could not be asked: counted, so that a server that never
                // comes back ends the follow. A busy one that asked to be
                // asked again later is not.
                if matches!(item, Item::End | Item::Again { busy: false }) {
                    run.drops += 1;
                    if run.drops > MAX_DROPS {
                        let _ = writeln!(
                            err,
                            "== {run_id}: its log could not be followed {MAX_DROPS} times; it is left"
                        );
                        run.failed = true;
                        run.done = true;
                        flush_all(run, raw, out);
                        continue;
                    }
                }
                run.conn += 1;
                if let Item::End = item {
                    let wait = pace.again * 2u32.saturating_pow(run.drops.min(5));
                    streams.push(later(
                        run_id.clone(),
                        run.conn,
                        wait.min(Duration::from_secs(30)),
                        Item::Open,
                    ));
                    continue;
                }
                run.parser = Default::default();
                match open(&run_id, run).await? {
                    Opened::Stream(response) => {
                        streams.push(piece(run_id.clone(), run.conn, response))
                    }
                    Opened::Later(after, busy) => streams.push(later(
                        run_id.clone(),
                        run.conn,
                        asked(after),
                        Item::Again { busy },
                    )),
                }
            }
        }
    }
    for run in runs.values_mut() {
        flush_all(run, raw, out);
    }
    Ok(u8::from(runs.values().any(|run| run.failed)))
}

/// Each of a run's lines not yet ended, shown as they are: its log is over,
/// or left.
fn flush_all(run: &mut Followed, raw: bool, out: Out<'_>) {
    for (_, (label, line)) in std::mem::take(&mut run.partial) {
        if !raw && !line.is_empty() {
            let _ = writeln!(out, "[{label}] {line}");
        }
    }
}

/// One event of a followed run's log, shown: its lines under their job,
/// what began again said so, or, with `--json`, the event as it came.
fn show(
    run_id: &str,
    run: &mut Followed,
    frame: crate::sse::Frame,
    many: bool,
    raw: bool,
    out: Out<'_>,
    err: Out<'_>,
) {
    let data: Value =
        serde_json::from_str(&frame.data).unwrap_or(Value::String(frame.data.clone()));
    if raw {
        let _ = writeln!(
            out,
            "{}",
            json!({ "run": run_id, "event": frame.event, "data": data })
        );
    }
    let task = text(&data["task_id"]).to_string();
    let cursor = data["cursor"].as_str().unwrap_or_default().to_string();
    let held = match cursor.is_empty() {
        true => None,
        false => run.cursors.insert(task.clone(), cursor.clone()),
    };
    let label = match many {
        true => format!("{} {}", run.pipeline, text(&data["task"])),
        false => text(&data["task"]).to_string(),
    };
    let flush = |run: &mut Followed, out: Out<'_>| {
        if let Some((label, line)) = run.partial.remove(&task) {
            if !raw && !line.is_empty() {
                let _ = writeln!(out, "[{label}] {line}");
            }
        }
    };
    match frame.event.as_str() {
        "start" => {
            // Where the stream was: go on with what is held. Anywhere else:
            // what is held of the task is replaced, not finished.
            if held.as_deref() != Some(cursor.as_str()) {
                run.partial.remove(&task);
            }
            // A job whose end was said, sent again from where it was: not
            // shown again.
            match run.ended.get(&task) {
                Some(end) if writing(end) == writing(&cursor) => {
                    run.replaying.insert(task.clone());
                }
                Some(_) => {
                    run.ended.remove(&task);
                    run.replaying.remove(&task);
                }
                None => {}
            }
            if !raw && !run.replaying.contains(&task) {
                match data["why"].as_str() {
                    Some("attempt") => {
                        let _ = writeln!(err, "== {label}: attempt {}", data["attempt"]);
                    }
                    Some("rewritten") => {
                        let _ =
                            writeln!(err, "== {label}: its log was written again from its start");
                    }
                    _ => {}
                }
            }
        }
        "log" if run.replaying.contains(&task) => {}
        "log" => {
            let text: String = match data["lines"].as_array() {
                Some(lines) => lines
                    .iter()
                    .map(|line| format!("{}\n", line["text"].as_str().unwrap_or_default()))
                    .collect(),
                None => data["text"].as_str().unwrap_or_default().to_string(),
            };
            let (_, pending) = run
                .partial
                .entry(task.clone())
                .or_insert_with(|| (label.clone(), String::new()));
            pending.push_str(&text);
            while let Some(at) = pending.find('\n') {
                let line: String = pending.drain(..=at).collect();
                if !raw {
                    let _ = writeln!(out, "[{label}] {}", &line[..line.len() - 1]);
                }
            }
        }
        "end" => {
            flush(run, &mut *out);
            let again = run.replaying.remove(&task) || run.ended.get(&task) == Some(&cursor);
            run.ended.insert(task.clone(), cursor.clone());
            let status = text(&data["status"]);
            if !raw && !again && status != "succeeded" {
                let _ = writeln!(err, "== {label}: {status}");
            }
        }
        "eof" => {
            run.done = true;
            flush_all(run, raw, out);
        }
        _ => {}
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, VecDeque};
    use std::sync::{Arc, Mutex};

    use axum::body::Bytes;
    use axum::extract::State;
    use axum::http::{HeaderMap, StatusCode, Uri};
    use axum::response::{IntoResponse, Response};
    use axum::Router;

    use super::*;

    /// A request a stub server took: its method, its path and query, its
    /// credential, its `Idempotency-Key` and its body.
    type Seen = (String, String, String, String, String);

    /// A stand-in for a dagron-ci server: it answers what it is given, in
    /// order, and keeps what it was asked. A string is answered as the text
    /// it holds (a page, say), and anything else as JSON.
    #[derive(Default)]
    struct Stub {
        answers: VecDeque<(u16, bool, Value)>,
        seen: Vec<Seen>,
    }

    type Shared = Arc<Mutex<Stub>>;

    async fn stub(answers: Vec<(u16, bool, Value)>) -> (String, Shared) {
        async fn any(
            State(stub): State<Shared>,
            method: axum::http::Method,
            uri: Uri,
            headers: HeaderMap,
            body: Bytes,
        ) -> Response {
            let header = |name: &str| {
                headers
                    .get(name)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("")
                    .to_string()
            };
            let mut stub = stub.lock().unwrap();
            stub.seen.push((
                method.to_string(),
                uri.to_string(),
                header("authorization"),
                header("idempotency-key"),
                String::from_utf8_lossy(&body).into_owned(),
            ));
            let Some((status, retry, body)) = stub.answers.pop_front() else {
                return StatusCode::INTERNAL_SERVER_ERROR.into_response();
            };
            let mut response = match body {
                Value::String(stream) if stream.starts_with("event: ") => (
                    StatusCode::from_u16(status).unwrap(),
                    [("content-type", "text/event-stream")],
                    stream,
                )
                    .into_response(),
                Value::String(text) => (StatusCode::from_u16(status).unwrap(), text).into_response(),
                body => (StatusCode::from_u16(status).unwrap(), body.to_string()).into_response(),
            };
            if retry {
                response
                    .headers_mut()
                    .insert("retry-after", "1".parse().unwrap());
            }
            response
        }
        let shared: Shared = Arc::new(Mutex::new(Stub {
            answers: answers.into(),
            seen: Vec::new(),
        }));
        let app = Router::new().fallback(any).with_state(shared.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (base, shared)
    }

    /// Run `command` against `base` as `dgr_dev`, quickly: its exit code,
    /// what it printed, and what it said.
    async fn run(command: &str, base: &str, args: &[&str]) -> (u8, String, String) {
        let base = base.to_string();
        let env = move |name: &str| match name {
            "DAGRON_CI_URL" => Some(base.clone()),
            "DAGRON_CI_TOKEN" => Some("dgr_dev".to_string()),
            _ => None,
        };
        let pace = Pace {
            poll: Duration::from_millis(5),
            again: Duration::from_millis(5),
        };
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let mut ask = |_: &str| -> std::io::Result<String> { panic!("a token was asked for") };
        let code = dispatch(command, args, &env, &mut ask, pace, &mut out, &mut err).await;
        (
            code,
            String::from_utf8(out).unwrap(),
            String::from_utf8(err).unwrap(),
        )
    }

    /// Run `command` with a home of its own (`XDG_CONFIG_HOME`), the
    /// variables `vars` and, when it asks for a token, `typed`.
    async fn at_home(
        home: &std::path::Path,
        command: &str,
        args: &[&str],
        vars: &[(&str, &str)],
        typed: Option<&str>,
    ) -> (u8, String, String) {
        let mut vars: Vec<(String, String)> = vars
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect();
        vars.push(("XDG_CONFIG_HOME".into(), home.to_str().unwrap().into()));
        let env = move |name: &str| {
            vars.iter()
                .find(|(n, _)| n == name)
                .map(|(_, value)| value.clone())
        };
        let typed = typed.map(str::to_string);
        let mut ask = move |prompt: &str| -> std::io::Result<String> {
            assert!(prompt.starts_with("dagron-api API token for "), "{prompt}");
            typed
                .clone()
                .ok_or_else(|| std::io::Error::other("nothing to read"))
        };
        let pace = Pace {
            poll: Duration::from_millis(5),
            again: Duration::from_millis(5),
        };
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let code = dispatch(command, args, &env, &mut ask, pace, &mut out, &mut err).await;
        (
            code,
            String::from_utf8(out).unwrap(),
            String::from_utf8(err).unwrap(),
        )
    }

    /// The file `login` keeps tokens in, under `home`.
    fn kept(home: &std::path::Path) -> std::path::PathBuf {
        home.join("dagron-ci/credentials.json")
    }

    fn seen(stub: &Shared) -> Vec<Seen> {
        std::mem::take(&mut stub.lock().unwrap().seen)
    }

    fn problem(title: &str, detail: &str) -> Value {
        json!({ "type": "about:blank", "title": title, "detail": detail })
    }

    /// The server is https, or this machine; and the token comes from a
    /// file or the environment, the file first, and never from a flag.
    #[test]
    fn the_token_goes_to_https_or_to_this_machine_and_is_never_a_flag() {
        let dir = tempfile::tempdir().unwrap();
        let file = dir.path().join("token");
        std::fs::write(&file, "dgr_from_file\n").unwrap();
        let file = file.to_str().unwrap().to_string();
        let with = |server: Option<&str>, token_file: Option<&str>, env: &[(&str, &str)]| {
            let target = Target { server, token_file };
            let env: Vec<(String, String)> = env
                .iter()
                .map(|(name, value)| (name.to_string(), value.to_string()))
                .collect();
            target
                .server(&move |name: &str| {
                    env.iter()
                        .find(|(n, _)| n == name)
                        .map(|(_, value)| value.clone())
                })
                .map(|server| (server.base, server.token))
                .map_err(|stop| format!("{stop:?}"))
        };
        let token = [("DAGRON_CI_TOKEN", "dgr_from_env")];

        assert_eq!(
            with(Some("https://ci.example/"), None, &token).unwrap(),
            ("https://ci.example".to_string(), "dgr_from_env".to_string())
        );
        // A flag is before the environment, and a file before the variable
        // that holds a token.
        let both = [
            ("DAGRON_CI_URL", "https://env.example"),
            ("DAGRON_CI_TOKEN_FILE", file.as_str()),
            ("DAGRON_CI_TOKEN", "dgr_from_env"),
        ];
        assert_eq!(
            with(None, None, &both).unwrap(),
            (
                "https://env.example".to_string(),
                "dgr_from_file".to_string()
            )
        );
        assert_eq!(
            with(Some("http://127.0.0.1:8085"), Some(&file), &token)
                .unwrap()
                .1,
            "dgr_from_file"
        );
        assert!(with(Some("http://localhost:8085/ci"), None, &token).is_ok());

        for (server, says) in [
            (
                "http://ci.example",
                "must be https://, or http:// to this machine",
            ),
            ("ftp://ci.example", "must be https://"),
            ("ci.example", "is not a URL"),
            (
                "https://dev:pw@ci.example",
                "takes no user, query or fragment",
            ),
            (
                "https://ci.example/?x=1",
                "takes no user, query or fragment",
            ),
        ] {
            let refused = with(Some(server), None, &token).unwrap_err();
            assert!(refused.contains(says), "{server}: {refused}");
        }
        assert!(with(None, None, &token)
            .unwrap_err()
            .contains("--server or DAGRON_CI_URL"));
        assert!(with(Some("https://ci.example"), None, &[])
            .unwrap_err()
            .contains("--token-file, DAGRON_CI_TOKEN_FILE or DAGRON_CI_TOKEN"));
        assert!(
            with(Some("https://ci.example"), Some("/no/such/file"), &token)
                .unwrap_err()
                .contains("the token file /no/such/file")
        );
        assert!(with(
            Some("https://ci.example"),
            None,
            &[("DAGRON_CI_TOKEN", "a b")]
        )
        .unwrap_err()
        .contains("more than one word"));
    }

    #[tokio::test]
    async fn run_remote_queues_a_dispatch_and_waits_for_what_it_runs() {
        let event = |state: &str, statuses: &[&str]| {
            let runs: Vec<Value> = statuses
                .iter()
                .enumerate()
                .map(|(n, status)| {
                    json!({ "id": format!("run-{n}"), "pipeline": format!("p{n}.yaml"), "status": status })
                })
                .collect();
            json!({
                "id": "ev-1", "ref": "refs/heads/main", "sha": "abc", "state": state,
                "reason": "p1.yaml: failed", "engine_runs": runs,
            })
        };
        let (base, server) = stub(vec![(202, false, event("new", &[]))]).await;
        let (code, out, err) = run(
            "run",
            &base,
            &[
                "--remote",
                "--repo",
                "github.com/acme/widgets",
                "--ref",
                "refs/heads/main",
                "--sha",
                "abc",
                "--pipeline",
                "deploy",
                "--input",
                "env=prod",
                "--input",
                "dry=1",
                "--key",
                "k-1",
            ],
        )
        .await;
        assert_eq!((code, out.as_str()), (0, "ev-1\n"), "{err}");
        assert_eq!(err, "queued: refs/heads/main at abc\n");
        let asked = seen(&server);
        assert_eq!(asked.len(), 1);
        assert_eq!(
            (
                asked[0].0.as_str(),
                asked[0].1.as_str(),
                asked[0].2.as_str(),
                asked[0].3.as_str()
            ),
            ("POST", "/ci/v1/runs", "Bearer dgr_dev", "k-1")
        );
        assert_eq!(
            serde_json::from_str::<Value>(&asked[0].4).unwrap(),
            json!({
                "repo": "github.com/acme/widgets", "ref": "refs/heads/main", "sha": "abc",
                "pipeline": "deploy", "inputs": { "env": "prod", "dry": "1" },
            })
        );

        // Waited for: each change is said once, a read that fails is tried
        // again, and the end is each run and how it ended.
        let (base, server) = stub(vec![
            (200, false, event("new", &[])),
            (200, false, event("claimed", &[])),
            (503, false, problem("unavailable", "try again")),
            (200, false, event("done", &["submitted", "submitted"])),
            (200, false, event("done", &["succeeded", "submitted"])),
            (200, false, event("done", &["succeeded", "succeeded"])),
        ])
        .await;
        let wait = [
            "--remote",
            "--repo",
            "github.com/acme/widgets",
            "--ref",
            "refs/heads/main",
            "--wait",
        ];
        let (code, out, err) = run("run", &base, &wait).await;
        assert_eq!(code, 0, "{err}");
        assert_eq!(
            out,
            "ev-1\nrun-0\tp0.yaml\tsucceeded\nrun-1\tp1.yaml\tsucceeded\n"
        );
        assert_eq!(
            err,
            "queued before, under the same key: refs/heads/main at abc\n\
             event: claimed\n\
             event: done\n\
             run-0 (p0.yaml): submitted\n\
             run-1 (p1.yaml): submitted\n\
             run-0 (p0.yaml): succeeded\n\
             run-1 (p1.yaml): succeeded\n"
        );
        assert_eq!(seen(&server).len(), 6);

        // A run that did not succeed is the command's failure, and says why.
        let (base, _) = stub(vec![
            (202, false, event("new", &[])),
            (200, false, event("done", &["succeeded", "failed"])),
        ])
        .await;
        let (code, out, err) = run("run", &base, &wait).await;
        assert_eq!(code, 1, "{err}");
        assert!(out.ends_with("run-1\tp1.yaml\tfailed\n"), "{out}");
        assert!(
            err.ends_with("dagron-ci run: ev-1 is done: p1.yaml: failed\n"),
            "{err}"
        );
        // So is an event that ran nothing.
        let (base, _) = stub(vec![
            (202, false, event("new", &[])),
            (200, false, event("skipped", &[])),
        ])
        .await;
        assert_eq!(run("run", &base, &wait).await.0, 1);

        // A wait that runs out says so, as timeout(1) does.
        let (base, _) = stub(
            std::iter::once((202, false, event("new", &[])))
                .chain((0..400).map(|_| (200, false, event("claimed", &[]))))
                .collect(),
        )
        .await;
        let mut bounded = wait.to_vec();
        bounded.extend(["--timeout", "1"]);
        let (code, _, err) = run("run", &base, &bounded).await;
        assert_eq!(code, 124, "{err}");

        // The server's refusal is said in its words.
        let (base, _) = stub(vec![(
            404,
            false,
            problem(
                "repository not connected",
                "github.com/acme/widgets is not connected",
            ),
        )])
        .await;
        let (code, out, err) = run("run", &base, &wait).await;
        assert_eq!((code, out.as_str()), (1, ""));
        assert_eq!(
            err,
            "dagron-ci run: repository not connected: github.com/acme/widgets is not connected \
             (HTTP 404)\n"
        );
    }

    /// `--bundle-from` uploads the working tree's change, then runs the
    /// bundle on its base; a bundle the server refuses runs nothing.
    #[tokio::test]
    async fn run_remote_bundle_from_uploads_the_change_and_runs_it() {
        let dir = tempfile::tempdir().unwrap();
        let top = dir.path();
        let git = |args: &[&str]| {
            let out = std::process::Command::new("git")
                .args([
                    "-c",
                    "user.name=t",
                    "-c",
                    "user.email=t@t",
                    "-c",
                    "commit.gpgsign=false",
                ])
                .args(args)
                .current_dir(top)
                .output()
                .unwrap();
            assert!(out.status.success(), "git {args:?}");
            String::from_utf8(out.stdout).unwrap().trim().to_string()
        };
        git(&["init", "-q", "-b", "main"]);
        std::fs::write(top.join("lib.rs"), "a\n").unwrap();
        git(&["add", "-A"]);
        git(&["commit", "-qm", "base"]);
        let head = git(&["rev-parse", "HEAD"]);
        std::fs::write(top.join("lib.rs"), "b\n").unwrap();

        let id = "0190f0a0-0000-7000-8000-000000000001";
        let (base, server) = stub(vec![
            (
                201,
                false,
                json!({ "id": id, "base": head, "sha256": "ab".repeat(32) }),
            ),
            (
                202,
                false,
                json!({ "id": "ev-1", "ref": "refs/heads/main", "sha": head }),
            ),
        ])
        .await;
        let args = [
            "--remote",
            "--repo",
            "github.com/acme/widgets",
            "--ref",
            "refs/heads/main",
            "--bundle-from",
            top.to_str().unwrap(),
        ];
        let (code, out, err) = run("run", &base, &args).await;
        assert_eq!((code, out.as_str()), (0, "ev-1\n"), "{err}");
        assert!(
            err.starts_with(&format!(
                "bundle {id}: 1 file changed and 0 files deleted since {head}, sha256 abab"
            )),
            "{err}"
        );
        let asked = seen(&server);
        assert_eq!(
            (asked[0].0.as_str(), asked[0].1.as_str()),
            (
                "POST",
                format!("/ci/v1/bundles?repo=github.com%2Facme%2Fwidgets&base={head}").as_str()
            )
        );
        assert_eq!(asked[0].2, "Bearer dgr_dev");
        let run_body: Value = serde_json::from_str(&asked[1].4).unwrap();
        assert_eq!(
            run_body,
            json!({ "repo": "github.com/acme/widgets", "ref": "refs/heads/main", "bundle": id }),
            "the run is the bundle's base's, which it names no other way"
        );

        let (base, server) = stub(vec![(
            422,
            false,
            problem("invalid bundle", "\"x\" is not a file"),
        )])
        .await;
        let (code, _, err) = run("run", &base, &args).await;
        assert_eq!(code, 1);
        assert!(
            err.contains("invalid bundle: \"x\" is not a file (HTTP 422)"),
            "{err}"
        );
        assert_eq!(seen(&server).len(), 1, "no run is asked for");
    }

    #[tokio::test]
    async fn rerun_and_cancel_say_what_was_done_and_what_was_left() {
        let (base, server) = stub(vec![(
            200,
            false,
            json!({
                "id": "ev-1",
                "rerun": [{ "id": "run-1", "cell": "default", "pipeline": "ci.yaml", "jobs": 1 }],
                "left": [{ "id": "run-2", "pipeline": "docs.yaml", "why": "it succeeded" }],
            }),
        )])
        .await;
        let (code, out, err) = run("rerun", &base, &["ev-1"]).await;
        assert_eq!(code, 0, "{err}");
        assert_eq!(out, "run-1\tci.yaml\trerun\t1 job\n");
        assert_eq!(err, "left run-2 (docs.yaml): it succeeded\n");
        let asked = seen(&server);
        assert_eq!(
            (
                asked[0].0.as_str(),
                asked[0].1.as_str(),
                asked[0].2.as_str()
            ),
            ("POST", "/ci/v1/runs/ev-1/rerun", "Bearer dgr_dev")
        );

        // `--all`: every job of each run that is over.
        let (base, server) = stub(vec![(
            200,
            false,
            json!({
                "id": "ev-1",
                "rerun": [{ "id": "run-2", "cell": "default", "pipeline": "docs.yaml", "jobs": 4 }],
                "left": [],
            }),
        )])
        .await;
        let (code, out, err) = run("rerun", &base, &["--all", "ev-1"]).await;
        assert_eq!((code, err.as_str()), (0, ""));
        assert_eq!(out, "run-2\tdocs.yaml\trerun\t4 jobs\n");
        let asked = seen(&server);
        assert_eq!(
            (asked[0].0.as_str(), asked[0].1.as_str()),
            ("POST", "/ci/v1/runs/ev-1/rerun?failed=false")
        );

        let (base, _) = stub(vec![(
            409,
            false,
            problem("nothing to rerun", "run run-1 (ci.yaml): it is running"),
        )])
        .await;
        let (code, _, err) = run("rerun", &base, &["ev-1"]).await;
        assert_eq!(code, 1);
        assert_eq!(
            err,
            "dagron-ci rerun: nothing to rerun: run run-1 (ci.yaml): it is running (HTTP 409)\n"
        );

        // A cancel of an event that is being dispatched is asked again
        // until it is not: a run submitted after the first cancel is
        // stopped by the second.
        let cancelled = |run: &str, event: &str| {
            json!({
                "id": "ev-1",
                "event": event,
                "cancelled": [{ "id": run, "cell": "default", "pipeline": "ci.yaml", "jobs": 3, "stopping": 1 }],
                "left": [],
            })
        };
        let (base, server) = stub(vec![
            (409, true, problem("being dispatched", "ask again")),
            (200, false, cancelled("run-1", "dispatching")),
            (200, false, cancelled("run-2", "settled")),
        ])
        .await;
        let (code, out, err) = run("cancel", &base, &["ev-1"]).await;
        assert_eq!((code, err.as_str()), (0, ""));
        assert_eq!(
            out,
            "run-1\tci.yaml\tcancelled\t3 jobs, 1 of them still stopping\n\
             run-2\tci.yaml\tcancelled\t3 jobs, 1 of them still stopping\n"
        );
        let asked = seen(&server);
        assert_eq!(asked.len(), 3);
        assert!(asked
            .iter()
            .all(|a| a.0 == "POST" && a.1 == "/ci/v1/runs/ev-1/cancel"));

        // One run, by its own id: its event may be dispatching still, and
        // the second answer, that nothing of the run is left, is the end.
        let (base, _) = stub(vec![
            (200, false, cancelled("run-1", "dispatching")),
            (
                409,
                false,
                problem("nothing to cancel", "nothing of it was left"),
            ),
        ])
        .await;
        assert_eq!(run("cancel", &base, &["run-1"]).await.0, 0);

        // An event that had not been dispatched is withdrawn, and said so.
        let (base, _) = stub(vec![(
            200,
            false,
            json!({ "id": "ev-1", "event": "withdrawn", "cancelled": [], "left": [] }),
        )])
        .await;
        let (code, out, err) = run("cancel", &base, &["ev-1"]).await;
        assert_eq!((code, out.as_str()), (0, ""));
        assert_eq!(err, "ev-1 was withdrawn: it had not been dispatched\n");

        // Nothing to cancel is a refusal, and so is a wait that never ends.
        let (base, _) = stub(vec![(
            409,
            false,
            problem(
                "nothing to cancel",
                "event ev-1 is done, and submitted no run",
            ),
        )])
        .await;
        assert_eq!(run("cancel", &base, &["ev-1"]).await.0, 1);
        let (base, server) = stub(
            (0..MAX_ASKS)
                .map(|_| (409, true, problem("being dispatched", "ask again")))
                .collect(),
        )
        .await;
        let (code, _, err) = run("cancel", &base, &["ev-1"]).await;
        assert_eq!(code, 1);
        assert!(err.contains("ev-1 is still being dispatched"), "{err}");
        assert_eq!(seen(&server).len(), MAX_ASKS as usize);
    }

    #[tokio::test]
    async fn logs_prints_each_run_s_lines_under_their_jobs() {
        let body = json!({
            "id": "ev-1", "state": "done", "reason": null,
            "runs": [
                {
                    "id": "run-1", "pipeline": "ci.yaml", "status": "failed",
                    "logs": {
                        "lines": [
                            { "task": "build", "text": "cargo build" },
                            { "task": "test", "text": "1 test failed" },
                        ],
                        "matched": 9, "truncated": true, "eof": true,
                    },
                },
                { "id": "run-2", "pipeline": "docs.yaml", "status": "submitted",
                  "logs": { "lines": [], "matched": 0, "truncated": false, "eof": false } },
                { "id": "run-3", "pipeline": "old.yaml", "status": "gone", "error": "its cell has no such run" },
            ],
        });
        let (base, server) =
            stub(vec![(200, false, body.clone()), (200, false, body.clone())]).await;
        let (code, out, err) = run(
            "logs",
            &base,
            &[
                "ev-1",
                "--job",
                "build",
                "--job",
                "unit tests",
                "--failed",
                "--grep",
                "a&b",
                "--tail",
                "50",
            ],
        )
        .await;
        assert_eq!(code, 1, "a run whose log was not read: {err}");
        assert_eq!(out, "[build] cargo build\n[test] 1 test failed\n");
        assert_eq!(
            err,
            "== run-1 (ci.yaml): failed\n\
             == run-1 (ci.yaml): 2 of 9 lines that match are shown; --tail N shows the last N\n\
             == run-2 (docs.yaml): submitted\n\
             == run-2 (docs.yaml): still running; read again for more\n\
             == run-3 (old.yaml): not read: its cell has no such run\n"
        );
        let (code, out, _) = run("logs", &base, &["--json", "ev-1"]).await;
        assert_eq!(code, 0);
        assert_eq!(serde_json::from_str::<Value>(&out).unwrap(), body);
        let asked = seen(&server);
        assert_eq!(
            (asked[0].0.as_str(), asked[0].1.as_str(), asked[0].2.as_str()),
            (
                "GET",
                "/ci/v1/runs/ev-1/logs?job=build&job=unit+tests&status=failed&q=a%26b&limit=50&tail=true",
                "Bearer dgr_dev"
            )
        );
        assert_eq!(asked[1].1, "/ci/v1/runs/ev-1/logs");

        // An event that has run nothing says where it stands.
        let (base, _) = stub(vec![(
            200,
            false,
            json!({ "id": "ev-1", "state": "new", "reason": "waiting for a slot", "runs": [] }),
        )])
        .await;
        let (code, out, err) = run("logs", &base, &["ev-1"]).await;
        assert_eq!((code, out.as_str()), (0, ""));
        assert_eq!(
            err,
            "ev-1 is new, and has run nothing: waiting for a slot\n"
        );
    }

    /// The token is for the server that was named: a redirect is not
    /// followed, and where it points hears nothing.
    #[tokio::test]
    async fn a_redirect_is_not_followed() {
        let (elsewhere, other) = stub(vec![(200, false, json!({ "runs": [] }))]).await;
        let app = Router::new().fallback(move || {
            let to = format!("{elsewhere}/ci/v1/runs/ev-1/logs");
            async move { (StatusCode::TEMPORARY_REDIRECT, [("location", to)]) }
        });
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });

        let (code, out, err) = run("logs", &base, &["ev-1"]).await;
        assert_eq!((code, out.as_str()), (1, ""), "{err}");
        assert_eq!(err, "dagron-ci logs: the server answered HTTP 307\n");
        assert!(seen(&other).is_empty(), "where it pointed was not asked");
    }

    /// What a command does not take is said before the server hears of it.
    #[tokio::test]
    async fn a_usage_error_asks_the_server_nothing() {
        let (base, server) = stub(Vec::new()).await;
        for (command, args) in [
            ("run", &["--remote", "--ref", "refs/heads/main"][..]),
            (
                "run",
                &["--remote", "--repo", "h/o/n", "--ref", "r", "--ref", "r"],
            ),
            (
                "run",
                &[
                    "--remote",
                    "--repo",
                    "h/o/n",
                    "--ref",
                    "r",
                    "--timeout",
                    "5",
                ],
            ),
            (
                "run",
                &["--remote", "--repo", "h/o/n", "--ref", "r", "--input", "x"],
            ),
            (
                "run",
                &["--remote", "--repo", "h/o/n", "--ref", "r", "--token", "t"],
            ),
            (
                "run",
                &[
                    "--remote",
                    "--repo",
                    "h/o/n",
                    "--ref",
                    "r",
                    "--bundle-from",
                    ".",
                    "--key",
                    "k",
                ],
            ),
            ("rerun", &[]),
            ("rerun", &["ev-1", "ev-2"]),
            ("rerun", &["--all"]),
            ("rerun", &["ev-1", "--all", "--all"]),
            ("rerun", &["ev-1", "--failed"]),
            ("cancel", &["../admin"]),
            ("cancel", &["ev-1", "--force"]),
            ("logs", &["ev-1", "--tail", "all"]),
            ("logs", &["ev-1", "--from-start"]),
            ("logs", &["--job", "build"]),
            ("login", &["--token", "dgp_x"]),
            ("login", &["dgp_x"]),
            ("logout", &["--token-file", "/tmp/t"]),
        ] {
            let (code, out, err) = run(command, &base, args).await;
            assert_eq!((code, out.as_str()), (2, ""), "{command} {args:?}: {err}");
            assert!(err.contains("usage:"), "{err}");
        }
        assert!(seen(&server).is_empty());
    }

    /// A token is kept once the server took it, and one it refused is not;
    /// what is kept is the user's alone, and is what the commands send to
    /// that server when nothing else gives a token.
    #[tokio::test]
    async fn login_keeps_a_token_the_server_took_for_that_server() {
        let home = tempfile::tempdir().unwrap();
        let me =
            json!({ "sub": "u-1", "email": "dev@example.com", "groups": [], "trust": "trusted" });
        let (base, server) = stub(vec![
            (
                401,
                false,
                problem(
                    "not authenticated",
                    "dagron-api did not accept the credential",
                ),
            ),
            (200, false, me.clone()),
            (
                200,
                false,
                json!({ "id": "ev-1", "state": "done", "reason": null, "runs": [] }),
            ),
        ])
        .await;
        let url = [("DAGRON_CI_URL", base.as_str())];

        let (code, out, err) = at_home(home.path(), "login", &[], &url, Some("dgp_wrong\n")).await;
        assert_eq!((code, out.as_str()), (1, ""));
        assert_eq!(
            err,
            "dagron-ci login: nothing was kept: not authenticated: dagron-api did not accept \
             the credential (HTTP 401)\n"
        );
        assert!(!kept(home.path()).exists());

        let (code, out, err) = at_home(home.path(), "login", &[], &url, Some("dgp_right\n")).await;
        assert_eq!(code, 0, "{err}");
        assert_eq!(
            out,
            format!("logged in to {base} as dev@example.com (u-1)\n")
        );
        let asked = seen(&server);
        assert_eq!(
            (
                asked[1].0.as_str(),
                asked[1].1.as_str(),
                asked[1].2.as_str()
            ),
            ("GET", "/ci/v1/me", "Bearer dgp_right")
        );
        let stored: Value =
            serde_json::from_str(&std::fs::read_to_string(kept(home.path())).unwrap()).unwrap();
        assert_eq!(
            stored,
            json!({
                "default": base,
                "servers": { base.as_str(): { "token": "dgp_right", "as": "dev@example.com (u-1)" } },
            })
        );
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let mode =
                |p: &std::path::Path| std::fs::metadata(p).unwrap().permissions().mode() & 0o777;
            assert_eq!(mode(&kept(home.path())), 0o600);
            assert_eq!(mode(&home.path().join("dagron-ci")), 0o700);
        }
        // No temporary file is left beside it, only the lock.
        let mut beside: Vec<String> = std::fs::read_dir(home.path().join("dagron-ci"))
            .unwrap()
            .map(|entry| entry.unwrap().file_name().to_string_lossy().into_owned())
            .collect();
        beside.sort();
        #[cfg(unix)]
        assert_eq!(beside, ["credentials.json", "credentials.lock"]);
        #[cfg(not(unix))]
        assert_eq!(beside, ["credentials.json"]);

        // With no server and no token named, the one logged in to and its
        // token are used.
        let (code, _, err) = at_home(home.path(), "logs", &["ev-1"], &[], None).await;
        assert_eq!(code, 0, "{err}");
        let asked = seen(&server);
        assert_eq!(
            (asked[0].1.as_str(), asked[0].2.as_str()),
            ("/ci/v1/runs/ev-1/logs", "Bearer dgp_right")
        );
    }

    /// A 200 that names no one (a page served for every path, or JSON with
    /// no subject) is not the run API's answer: nothing is kept.
    #[tokio::test]
    async fn a_200_that_names_no_one_keeps_no_token() {
        let home = tempfile::tempdir().unwrap();
        let (base, _) = stub(vec![
            (200, false, json!("<!doctype html><title>console</title>")),
            (
                200,
                false,
                json!({ "email": "dev@example.com", "groups": [] }),
            ),
        ])
        .await;
        let url = [("DAGRON_CI_URL", base.as_str())];
        for _ in 0..2 {
            let (code, out, err) =
                at_home(home.path(), "login", &[], &url, Some("dgp_right\n")).await;
            assert_eq!((code, out.as_str()), (1, ""));
            assert_eq!(
                err,
                format!(
                    "dagron-ci login: nothing was kept: {base} answered GET /ci/v1/me without \
                     saying whose the token is: name a dagron-ci server\n"
                )
            );
            assert!(!kept(home.path()).exists());
        }
    }

    /// A subject or an email that is no one's name (one with a control
    /// character, which would be written to the terminal as it is, or one
    /// longer than any name) keeps nothing; and nothing is kept that would
    /// be too large to read back.
    #[tokio::test]
    async fn a_200_that_names_no_one_s_name_keeps_no_token() {
        let home = tempfile::tempdir().unwrap();
        let (base, _) = stub(vec![
            (
                200,
                false,
                json!({ "sub": "u-1\r\u{1b}[2Klogged in to https://ci.corp.example as alice" }),
            ),
            (200, false, json!({ "sub": "u".repeat(257) })),
            (
                200,
                false,
                json!({ "sub": "u-1", "email": "dev@example.com\u{7}" }),
            ),
        ])
        .await;
        let url = [("DAGRON_CI_URL", base.as_str())];
        for _ in 0..3 {
            let (code, out, err) =
                at_home(home.path(), "login", &[], &url, Some("dgp_right\n")).await;
            assert_eq!((code, out.as_str()), (1, ""));
            assert_eq!(
                err,
                format!(
                    "dagron-ci login: nothing was kept: {base} answered GET /ci/v1/me with a \
                     subject or an email of more than 256 bytes, or with a control character \
                     in it: name a dagron-ci server\n"
                )
            );
            assert!(!kept(home.path()).exists());
        }

        let xdg = home.path().to_str().unwrap().to_string();
        let env = move |name: &str| (name == "XDG_CONFIG_HOME").then(|| xdg.clone());
        let large = login::Stored {
            default: None,
            servers: BTreeMap::from([(
                "https://ci.example".to_string(),
                login::Kept {
                    token: "t".repeat(1 << 20),
                    who: "u-1".into(),
                },
            )]),
        };
        let refused = login::write(&env, &large).unwrap_err();
        assert!(
            format!("{refused:?}").contains("more than 1 MiB"),
            "{refused:?}"
        );
        assert!(!kept(home.path()).exists());
    }

    /// What another login or logout kept while this one waited for its
    /// token, or for the server's answer, is not undone: what is kept is
    /// read again, under a lock, once the server has answered.
    #[tokio::test]
    async fn a_login_keeps_what_another_kept_meanwhile() {
        let home = tempfile::tempdir().unwrap();
        let xdg = home.path().to_str().unwrap().to_string();
        let env = move |name: &str| (name == "XDG_CONFIG_HOME").then(|| xdg.clone());
        let kept_for = |server: &str| login::Stored {
            default: Some(server.to_string()),
            servers: BTreeMap::from([(
                server.to_string(),
                login::Kept {
                    token: "dgp_kept".into(),
                    who: "u-1".into(),
                },
            )]),
        };
        login::write(&env, &kept_for("https://c.example")).unwrap();
        let (base, _) = stub(vec![(200, false, json!({ "sub": "u-2", "groups": [] }))]).await;
        // While the token is typed, c.example is logged out of and b.example
        // logged in to.
        let mut ask = |_: &str| -> std::io::Result<String> {
            login::write(&env, &kept_for("https://b.example")).unwrap();
            Ok("dgp_typed\n".to_string())
        };
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let code = dispatch(
            "login",
            &["--server", &base],
            &env,
            &mut ask,
            PACE,
            &mut out,
            &mut err,
        )
        .await;
        assert_eq!(code, 0, "{}", String::from_utf8_lossy(&err));
        let stored: Value =
            serde_json::from_str(&std::fs::read_to_string(kept(home.path())).unwrap()).unwrap();
        assert_eq!(
            stored,
            json!({
                "default": base,
                "servers": {
                    "https://b.example": { "token": "dgp_kept", "as": "u-1" },
                    base.as_str(): { "token": "dgp_typed", "as": "u-2" },
                },
            })
        );
    }

    /// The token comes from the flag, then the variables, then what was
    /// kept for that server, and what was kept for one server is sent to no
    /// other.
    #[tokio::test]
    async fn a_kept_token_is_the_last_one_asked_and_for_its_server_alone() {
        let home = tempfile::tempdir().unwrap();
        let file = home.path().join("token");
        std::fs::write(&file, "dgr_from_file\n").unwrap();
        let file = file.to_str().unwrap().to_string();
        let stored = login::Stored {
            default: Some("https://ci.example".into()),
            servers: BTreeMap::from([(
                "https://ci.example".to_string(),
                login::Kept {
                    token: "dgp_kept".into(),
                    who: "dev@example.com".into(),
                },
            )]),
        };
        let xdg = home.path().to_str().unwrap().to_string();
        let target = |server: Option<&str>, token_file: Option<&str>, vars: &[(&str, &str)]| {
            let mut vars: Vec<(String, String)> = vars
                .iter()
                .map(|(n, v)| (n.to_string(), v.to_string()))
                .collect();
            vars.push(("XDG_CONFIG_HOME".into(), xdg.clone()));
            Target { server, token_file }
                .server(&move |name: &str| {
                    vars.iter().find(|(n, _)| n == name).map(|(_, v)| v.clone())
                })
                .map(|server| (server.base, server.token))
                .map_err(|stop| format!("{stop:?}"))
        };
        let env = |name: &str| (name == "XDG_CONFIG_HOME").then(|| xdg.clone());
        login::write(&env, &stored).unwrap();

        assert_eq!(
            target(None, None, &[]).unwrap(),
            ("https://ci.example".to_string(), "dgp_kept".to_string())
        );
        // Written another way, it is the same server.
        assert_eq!(
            target(Some("https://CI.example:443/"), None, &[])
                .unwrap()
                .1,
            "dgp_kept"
        );
        assert_eq!(
            target(None, None, &[("DAGRON_CI_TOKEN", "dgr_env")])
                .unwrap()
                .1,
            "dgr_env"
        );
        assert_eq!(
            target(
                None,
                None,
                &[
                    ("DAGRON_CI_TOKEN_FILE", &file),
                    ("DAGRON_CI_TOKEN", "dgr_env")
                ]
            )
            .unwrap()
            .1,
            "dgr_from_file"
        );
        assert_eq!(target(None, Some(&file), &[]).unwrap().1, "dgr_from_file");
        // Another server is not sent the kept token.
        let refused =
            target(None, None, &[("DAGRON_CI_URL", "https://other.example")]).unwrap_err();
        assert!(
            refused.contains("or log in to https://other.example with `dagron-ci login`"),
            "{refused}"
        );

        // A server kept is held to the rule as one named is.
        let plain = login::Stored {
            default: Some("http://ci.example".into()),
            servers: BTreeMap::from([(
                "http://ci.example".to_string(),
                login::Kept {
                    token: "dgp_kept".into(),
                    who: String::new(),
                },
            )]),
        };
        login::write(&env, &plain).unwrap();
        assert!(target(None, None, &[])
            .unwrap_err()
            .contains("must be https://, or http:// to this machine"));

        // A file others may read is refused, as ssh refuses a key.
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            login::write(&env, &stored).unwrap();
            std::fs::set_permissions(kept(home.path()), std::fs::Permissions::from_mode(0o640))
                .unwrap();
            let refused = target(None, None, &[]).unwrap_err();
            assert!(
                refused.contains("holds tokens, and others may read it (mode 640)"),
                "{refused}"
            );
            // What is named in full needs nothing kept, and does not read it.
            assert!(target(Some("https://ci.example"), Some(&file), &[]).is_ok());
        }
        std::fs::write(kept(home.path()), "{\"servers\": 1}").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(kept(home.path()), std::fs::Permissions::from_mode(0o600))
                .unwrap();
        }
        assert!(target(None, None, &[])
            .unwrap_err()
            .contains("is not a file `dagron-ci login` wrote"));
    }

    /// The server is checked before a token is asked for, and the token is
    /// never an argument; a logout forgets the server's token and no other.
    #[tokio::test]
    async fn login_asks_only_a_server_it_may_send_to_and_logout_forgets() {
        let home = tempfile::tempdir().unwrap();
        let (code, _, err) = at_home(
            home.path(),
            "login",
            &["--server", "http://ci.example"],
            &[],
            None,
        )
        .await;
        assert_eq!(code, 2);
        assert!(err.contains("must be https://"), "{err}");
        let (code, _, err) = at_home(home.path(), "login", &[], &[], None).await;
        assert_eq!(code, 2);
        assert!(err.contains("--server or DAGRON_CI_URL"), "{err}");

        let me = |email: &str| json!({ "sub": "u-1", "email": email, "groups": ["viewer"] });
        let (one, _) = stub(vec![(200, false, me("dev@example.com"))]).await;
        let (two, _) = stub(vec![(200, false, me(""))]).await;
        let (code, _, err) = at_home(
            home.path(),
            "login",
            &["--server", &one],
            &[("DAGRON_CI_TOKEN", "dgp_one")],
            None,
        )
        .await;
        assert_eq!(code, 0, "{err}");
        assert!(
            err.starts_with("a viewer: runs are read, and none is started\n"),
            "{err}"
        );
        let (code, out, _) = at_home(
            home.path(),
            "login",
            &["--server", &two],
            &[],
            Some("dgp_two"),
        )
        .await;
        assert_eq!((code, out), (0, format!("logged in to {two} as u-1\n")));

        // DAGRON_CI_URL names the server before the one logged in to last.
        let (code, out, err) = at_home(
            home.path(),
            "logout",
            &[],
            &[("DAGRON_CI_URL", "https://other.example")],
            None,
        )
        .await;
        assert_eq!((code, out.as_str()), (0, ""));
        assert_eq!(err, "no token is kept for https://other.example\n");

        // The one logged in to last is the one a logout forgets.
        let (code, out, err) = at_home(home.path(), "logout", &[], &[], None).await;
        assert_eq!((code, out), (0, format!("logged out of {two}\n")));
        assert!(
            err.contains("it works until it is revoked in dagron-api"),
            "{err}"
        );
        let stored: Value =
            serde_json::from_str(&std::fs::read_to_string(kept(home.path())).unwrap()).unwrap();
        assert_eq!(
            stored,
            json!({ "servers": { one.as_str(): { "token": "dgp_one", "as": "dev@example.com (u-1)" } } })
        );
        let (code, out, err) = at_home(home.path(), "logout", &["--server", &two], &[], None).await;
        assert_eq!((code, out.as_str()), (0, ""));
        assert_eq!(err, format!("no token is kept for {two}\n"));
        // With nothing left, nothing is kept.
        let (code, _, _) = at_home(home.path(), "logout", &["--server", &one], &[], None).await;
        assert_eq!(code, 0);
        assert!(!kept(home.path()).exists());
        let (code, _, err) = at_home(home.path(), "logout", &[], &[], None).await;
        assert_eq!((code, err.as_str()), (0, "no token is kept here\n"));
    }

    /// `logs --follow`: each run's log through the door, its lines under
    /// their job as they come, a line cut off finished when the rest comes; a
    /// stream cut off opened again from its cursors, after the time a busy
    /// server asks for; done once the event is over and each log has ended.
    #[tokio::test]
    async fn logs_follow_goes_on_from_its_cursors_after_a_drop() {
        let start = "event: start\ndata: {\"task_id\":\"t-1\",\"task\":\"build\",\"attempt\":1,\"cursor\":\"t-1:3:1:0\",\"why\":\"open\"}\n\n";
        let first = format!("{start}event: log\ndata: {{\"task_id\":\"t-1\",\"task\":\"build\",\"cursor\":\"t-1:3:1:6\",\"text\":\"one\\ntw\"}}\n\n");
        // The stream opened again begins with the job's start, at the cursor it was
        // asked from: what is held of the line goes on.
        let rest = "event: start\ndata: {\"task_id\":\"t-1\",\"task\":\"build\",\"attempt\":1,\"cursor\":\"t-1:3:1:6\",\"why\":\"open\"}\n\nevent: log\ndata: {\"task_id\":\"t-1\",\"task\":\"build\",\"cursor\":\"t-1:3:1:13\",\"text\":\"o\\nthree\\n\"}\n\nevent: end\ndata: {\"task_id\":\"t-1\",\"task\":\"build\",\"status\":\"failed\",\"cursor\":\"t-1:3:1:13\",\"complete\":true}\n\nevent: eof\ndata: {\"run_id\":\"run-a\"}\n\n";
        let (base, server) = stub(vec![
            (
                200,
                false,
                json!({ "id": "ev-1", "state": "done",
                        "engine_runs": [{ "id": "run-a", "pipeline": ".dagron/ci/a.yaml", "status": "failed" }] }),
            ),
            (200, false, Value::String(first)),
            (429, true, problem("dagron-api is busy", "ask again")),
            (200, false, Value::String(rest.to_string())),
        ])
        .await;
        let (code, out, err) = run(
            "logs",
            &base,
            &["ev-1", "--follow", "--from-start", "--job", "build"],
        )
        .await;
        assert_eq!(code, 0, "{err}");
        assert_eq!(out, "[build] one\n[build] two\n[build] three\n", "{err}");
        assert!(err.contains("== build: failed"), "{err}");
        let asked: Vec<String> = seen(&server).into_iter().map(|s| s.1).collect();
        assert_eq!(
            asked,
            [
                "/ci/v1/runs/ev-1",
                "/ci/v1/runs/run-a/logs?follow=true&job=build&from=start",
                "/ci/v1/runs/run-a/logs?follow=true&job=build&from=start&cursor=t-1%3A3%3A1%3A6",
                "/ci/v1/runs/run-a/logs?follow=true&job=build&from=start&cursor=t-1%3A3%3A1%3A6",
            ]
        );
    }

    /// A follow of a server that only says it cannot follow now ends, after
    /// so many tries: it does not ask for ever.
    #[tokio::test]
    async fn logs_follow_of_a_server_that_never_comes_back_ends() {
        let mut answers = vec![(
            200,
            false,
            json!({ "id": "ev-1", "state": "done",
                    "engine_runs": [{ "id": "run-a", "pipeline": ".dagron/ci/a.yaml", "status": "failed" }] }),
        )];
        answers.extend((0..=MAX_DROPS).map(|_| {
            (
                503,
                true,
                problem("dagron-api could not be asked", "try again"),
            )
        }));
        let (base, server) = stub(answers).await;
        let (code, out, err) = run("logs", &base, &["ev-1", "--follow"]).await;
        assert_eq!(code, 1, "{err}");
        assert_eq!(out, "");
        assert!(err.contains("could not be followed 10 times"), "{err}");
        assert_eq!(
            seen(&server).len(),
            2 + MAX_DROPS as usize,
            "the event, and each try"
        );
    }
}
