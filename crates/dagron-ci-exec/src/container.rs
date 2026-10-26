//! **A step in an image of its own** (a workflow's Docker action): `dagron-ci-exec
//! container <spec>`, which the step runner runs in the step's place.
//!
//! It asks the engine's results service (`ACTIONS_RESULTS_URL`, with the
//! job's `ACTIONS_RUNTIME_TOKEN`) for a container of the image, which the
//! job's executor starts beside the job: the job's workspace at
//! `/github/workspace`, where it starts, and the job's network. It waits for
//! the container to end, says what it said, and exits as it exited.
//!
//! The runner's files (`GITHUB_ENV`, `GITHUB_OUTPUT`, `GITHUB_PATH`,
//! `GITHUB_STATE`, `GITHUB_STEP_SUMMARY`) are the step runner's own, where
//! no other container sees them. The container is given files of its own, in
//! a directory of the workspace that is the step's alone, and what it wrote
//! there is added to the step runner's when it has ended; the event's file
//! is copied there, and `HOME` and `RUNNER_TEMP` are there too. The
//! directory is gone after.
//!
//! An action whose image is built from a Dockerfile has it built first: a
//! tar of the Dockerfile's directory is sent to the results service, whose
//! executor builds it (for a trusted run, where its operator lets it), and
//! what the build said is the step's to say before what its container
//! says. The image is named for what it was built from, so a second step of
//! the same action's has it already.
//!
//! A cancelled step (the step runner's `SIGTERM` at its time, or the job's
//! cancel) stops waiting; its container runs on until the engine's time for
//! it is up, or the job ends, whichever is first.

use std::io::Write;
use std::path::{Path, PathBuf};
use std::time::Duration;

use serde_json::{json, Value};

/// Where the job's workspace is in a step's container.
pub const WORKSPACE: &str = "/github/workspace";

/// The runner's files a step's container is given its own of.
const FILES: [&str; 5] = ["ENV", "PATH", "OUTPUT", "STEP_SUMMARY", "STATE"];

/// The most an image may be built from: the engine's own bound.
const MAX_CONTEXT_BYTES: usize = 64 << 20;

/// An image to build, from a Dockerfile.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Build {
    /// The Dockerfile's directory, which it is built from.
    pub context: String,
    /// The Dockerfile, a file of that directory.
    pub dockerfile: String,
}

/// What a step's container is run with, as the step runner writes it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Spec {
    /// The image a registry has; empty where it is built.
    pub image: String,
    /// What it is built from, where it is.
    pub build: Option<Build>,
    pub entrypoint: Option<String>,
    pub args: Vec<String>,
    pub env: Vec<(String, String)>,
    /// What is left of the step's time.
    pub timeout_secs: Option<u64>,
}

impl Spec {
    pub fn to_json(&self) -> String {
        json!({
            "image": self.image,
            "build": self.build.as_ref().map(|b| json!({
                "context": b.context,
                "dockerfile": b.dockerfile,
            })),
            "entrypoint": self.entrypoint,
            "args": self.args,
            "env": self.env,
            "timeout_secs": self.timeout_secs,
        })
        .to_string()
    }

    fn from_json(text: &str) -> Result<Spec, String> {
        let v: Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
        let texts = |key: &str| -> Vec<String> {
            v[key]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|s| s.as_str().map(str::to_string))
                .collect()
        };
        let build = match (
            v["build"]["context"].as_str(),
            v["build"]["dockerfile"].as_str(),
        ) {
            (Some(context), Some(dockerfile)) => Some(Build {
                context: context.to_string(),
                dockerfile: dockerfile.to_string(),
            }),
            _ => None,
        };
        Ok(Spec {
            image: v["image"].as_str().unwrap_or_default().to_string(),
            build,
            entrypoint: v["entrypoint"].as_str().map(str::to_string),
            args: texts("args"),
            env: v["env"]
                .as_array()
                .into_iter()
                .flatten()
                .filter_map(|pair| {
                    Some((pair[0].as_str()?.to_string(), pair[1].as_str()?.to_string()))
                })
                .collect(),
            timeout_secs: v["timeout_secs"].as_u64(),
        })
    }
}

/// Each variable once, as the last of it says, in the order they were first
/// named: a step's own after the runner's, as a script reads them.
pub fn last_of_each(env: Vec<(String, String)>) -> Vec<(String, String)> {
    let mut out: Vec<(String, String)> = Vec::with_capacity(env.len());
    for (key, value) in env {
        match out.iter_mut().find(|(k, _)| *k == key) {
            Some(had) => had.1 = value,
            None => out.push((key, value)),
        }
    }
    // What is the container's own, not the step runner's.
    out.retain(|(key, _)| !matches!(key.as_str(), "PATH" | "HOME" | "HOSTNAME" | "PWD"));
    out
}

/// `text` as a shell splits it into words: on spaces, but within quotes,
/// with a backslash keeping the next character as it is. As GitHub reads a
/// `docker://` step's `with.args`.
pub fn words(text: &str) -> Result<Vec<String>, String> {
    let mut words = Vec::new();
    let mut word = String::new();
    let mut started = false;
    let mut quote: Option<char> = None;
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        match (quote, c) {
            (Some(q), c) if c == q => quote = None,
            (Some('"'), '\\') | (None, '\\') => match chars.next() {
                Some(next) => word.push(next),
                None => return Err("with.args ends with a backslash".into()),
            },
            (Some(_), c) => word.push(c),
            (None, '"' | '\'') => {
                quote = Some(c);
                started = true;
            }
            (None, c) if c.is_whitespace() => {
                if started || !word.is_empty() {
                    words.push(std::mem::take(&mut word));
                }
                started = false;
            }
            (None, c) => word.push(c),
        }
    }
    if quote.is_some() {
        return Err("with.args has a quote that is not closed".into());
    }
    if started || !word.is_empty() {
        words.push(word);
    }
    Ok(words)
}

/// What a client of the engine's service says when the engine does not.
fn spoken(e: ureq::Error) -> String {
    match e {
        ureq::Error::Status(code, response) => {
            let said = response.into_string().unwrap_or_default();
            let why = serde_json::from_str::<Value>(&said)
                .ok()
                .and_then(|v| v["error"].as_str().map(str::to_string))
                .unwrap_or(said);
            format!("the engine answered {code}: {why}")
        }
        other => format!("the engine could not be reached: {other}"),
    }
}

/// The step's directory in the workspace: `<workspace>/.dagron-ci-step-…`,
/// open to whatever user the container runs as.
fn step_dir(workspace: &Path) -> std::io::Result<PathBuf> {
    use std::os::unix::fs::PermissionsExt;
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.subsec_nanos())
        .unwrap_or_default();
    let dir = workspace.join(format!(".dagron-ci-step-{}-{nanos}", std::process::id()));
    std::fs::create_dir(&dir)?;
    for sub in ["home", "tmp"] {
        std::fs::create_dir(dir.join(sub))?;
        std::fs::set_permissions(dir.join(sub), std::fs::Permissions::from_mode(0o777))?;
    }
    for kind in FILES {
        let file = dir.join(kind.to_ascii_lowercase());
        std::fs::write(&file, "")?;
        std::fs::set_permissions(&file, std::fs::Permissions::from_mode(0o666))?;
    }
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o777))?;
    Ok(dir)
}

/// Run the step's container as `spec_file` says: what it exits with.
pub fn run(spec_file: &Path) -> i32 {
    let _ = crate::run::catch_cancel_signals();
    let mut spec = match std::fs::read_to_string(spec_file)
        .map_err(|e| e.to_string())
        .and_then(|text| Spec::from_json(&text))
    {
        Ok(spec) => spec,
        Err(why) => {
            eprintln!("dagron-ci-exec container: {}: {why}", spec_file.display());
            return 1;
        }
    };
    let (Some(base), Some(token)) = (
        std::env::var("ACTIONS_RESULTS_URL")
            .ok()
            .filter(|u| !u.is_empty()),
        std::env::var("ACTIONS_RUNTIME_TOKEN")
            .ok()
            .filter(|t| !t.is_empty()),
    ) else {
        eprintln!(
            "this step runs in an image of its own, which the engine's results service starts \
             (DAGRON_ACTIONS_CACHE_LISTEN), and this job was given none"
        );
        return 1;
    };
    let workspace = std::env::var_os("GITHUB_WORKSPACE")
        .map(PathBuf::from)
        .or_else(|| std::env::current_dir().ok())
        .unwrap_or_else(|| PathBuf::from("/workspace"));
    let dir = match step_dir(&workspace) {
        Ok(dir) => dir,
        Err(e) => {
            eprintln!("the step's directory in the workspace could not be made: {e}");
            return 1;
        }
    };
    let agent = ureq::AgentBuilder::new()
        .timeout_connect(Duration::from_secs(15))
        .timeout_read(Duration::from_secs(90))
        .build();
    let bearer = format!("Bearer {token}");
    if let Some(build) = spec.build.take() {
        let started = std::time::Instant::now();
        match built(&agent, &build, &base, &bearer, spec.timeout_secs) {
            Ok(image) => spec.image = image,
            Err(code) => {
                let _ = std::fs::remove_dir_all(&dir);
                return code;
            }
        }
        // The build is of the step's time: its container has what is left.
        if let Some(secs) = spec.timeout_secs {
            let left = secs.saturating_sub(started.elapsed().as_secs());
            if left == 0 {
                eprintln!("the step's time was spent building its image");
                let _ = std::fs::remove_dir_all(&dir);
                return 124;
            }
            spec.timeout_secs = Some(left);
        }
    }
    let code = ask(&agent, &spec, &base, &bearer, &dir);
    relay(&dir);
    let _ = std::fs::remove_dir_all(&dir);
    code
}

/// The container's own files, where it wrote for the steps after it, said
/// to the step runner in its own.
fn relay(dir: &Path) {
    for kind in FILES {
        let Ok(text) = std::fs::read(dir.join(kind.to_ascii_lowercase())) else {
            continue;
        };
        let Some(own) = std::env::var_os(format!("GITHUB_{kind}")) else {
            continue;
        };
        if text.is_empty() {
            continue;
        }
        if let Ok(mut file) = std::fs::OpenOptions::new().append(true).open(own) {
            let _ = file.write_all(&text);
        }
    }
}

/// A `.dockerignore`'s patterns, as Docker's command line reads them before
/// it sends a build its context: a line each, `#` a comment, `!` an
/// exception; `*` and `?` within a name, `**` across names. A path is left
/// out where the last pattern that matches it, or a directory it is in, is
/// no exception.
struct Ignore(Vec<(bool, String)>);

impl Ignore {
    fn read(dir: &Path) -> Ignore {
        let text = std::fs::read_to_string(dir.join(".dockerignore")).unwrap_or_default();
        Ignore(
            text.lines()
                .map(str::trim)
                .filter(|line| !line.is_empty() && !line.starts_with('#'))
                .map(|line| {
                    let (except, pattern) = match line.strip_prefix('!') {
                        Some(rest) => (true, rest.trim()),
                        None => (false, line),
                    };
                    let pattern = pattern
                        .split('/')
                        .filter(|part| !part.is_empty() && *part != ".")
                        .collect::<Vec<_>>()
                        .join("/");
                    (except, pattern)
                })
                .filter(|(_, pattern)| !pattern.is_empty())
                .collect(),
        )
    }

    fn has_exceptions(&self) -> bool {
        self.0.iter().any(|(except, _)| *except)
    }

    /// Whether `path`, relative to the context with `/` between names, is
    /// left out.
    fn excludes(&self, path: &str) -> bool {
        let parts: Vec<&str> = path.split('/').collect();
        let mut out = false;
        for (except, pattern) in &self.0 {
            let hit = (1..=parts.len()).any(|n| glob(pattern, &parts[..n].join("/")));
            if hit {
                out = !except;
            }
        }
        out
    }
}

/// Whether `text` is matched by `pattern`: `*` any run of a name, `?` one
/// character of one, `**` any run of names.
fn glob(pattern: &str, text: &str) -> bool {
    fn go(p: &[u8], t: &[u8]) -> bool {
        match p {
            [] => t.is_empty(),
            [b'*', b'*', rest @ ..] => match rest.strip_prefix(b"/") {
                // `**/` and a name: that name at any depth.
                Some(rest) => (0..=t.len())
                    .filter(|&i| i == 0 || t[i - 1] == b'/')
                    .any(|i| go(rest, &t[i..])),
                None => (0..=t.len()).any(|i| go(rest, &t[i..])),
            },
            [b'*', rest @ ..] => (0..=t.len())
                .take_while(|&i| i == 0 || t[i - 1] != b'/')
                .any(|i| go(rest, &t[i..])),
            [b'?', rest @ ..] => matches!(t, [c, ..] if *c != b'/') && go(rest, &t[1..]),
            [c, rest @ ..] => matches!(t, [d, ..] if d == c) && go(rest, &t[1..]),
        }
    }
    go(pattern.as_bytes(), text.as_bytes())
}

/// A tar of `dir`, as Docker's command line sends a build its context: its
/// links as links, with no owner or time, less what its `.dockerignore`
/// leaves out, but `dockerfile` and the `.dockerignore`, which are always
/// sent; and never the step runner's own directories of the workspace.
fn context_of(dir: &Path, dockerfile: &str) -> std::io::Result<Vec<u8>> {
    struct Bounded(Vec<u8>);
    impl Write for Bounded {
        fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
            if self.0.len() + data.len() > MAX_CONTEXT_BYTES {
                return Err(std::io::Error::other(format!(
                    "it is more than the {} MiB an image is built from",
                    MAX_CONTEXT_BYTES >> 20
                )));
            }
            self.0.extend_from_slice(data);
            Ok(data.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let ignore = Ignore::read(dir);
    let mut tar = tar::Builder::new(Bounded(Vec::new()));
    tar.follow_symlinks(false);
    tar.mode(tar::HeaderMode::Deterministic);
    let mut dirs = vec![std::path::PathBuf::new()];
    while let Some(rel) = dirs.pop() {
        let mut entries = std::fs::read_dir(dir.join(&rel))?.collect::<Result<Vec<_>, _>>()?;
        entries.sort_by_key(std::fs::DirEntry::file_name);
        for entry in entries {
            let name = rel.join(entry.file_name());
            let text = name.to_string_lossy().replace('\\', "/");
            if text
                .split('/')
                .any(|part| part.starts_with(".dagron-ci-step-"))
            {
                continue;
            }
            let kept = text == dockerfile || text == ".dockerignore" || !ignore.excludes(&text);
            if entry.file_type()?.is_dir() {
                if kept {
                    tar.append_dir(&name, entry.path())?;
                }
                // A directory left out may hold an exception.
                if kept || ignore.has_exceptions() {
                    dirs.push(name);
                }
            } else if kept {
                tar.append_path_with_name(entry.path(), &name)?;
            }
        }
    }
    Ok(tar.into_inner()?.0)
}

/// `text` as a URL's query has it.
fn query_text(text: &str) -> String {
    text.bytes()
        .map(|b| match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'.' | b'_' | b'~' => {
                char::from(b).to_string()
            }
            _ => format!("%{b:02X}"),
        })
        .collect()
}

/// The image `build` makes, built by the engine: its name, once what the
/// build said is said; else the step's exit code.
fn built(
    agent: &ureq::Agent,
    build: &Build,
    base: &str,
    bearer: &str,
    timeout_secs: Option<u64>,
) -> Result<String, i32> {
    let context = match context_of(Path::new(&build.context), &build.dockerfile) {
        Ok(context) => context,
        Err(e) => {
            eprintln!("the action's image could not be built from its directory: {e}");
            return Err(1);
        }
    };
    let mut url = format!(
        "{}/dagron/v1/steps/images?dockerfile={}",
        base.trim_end_matches('/'),
        query_text(&build.dockerfile)
    );
    if let Some(secs) = timeout_secs {
        url.push_str(&format!("&timeout_secs={secs}"));
    }
    let asked = agent
        .post(&url)
        .set("authorization", bearer)
        .set("content-type", "application/x-tar")
        .send_bytes(&context)
        .map_err(spoken)
        .and_then(|r| r.into_string().map_err(|e| e.to_string()));
    let id = match id_of(asked) {
        Ok(id) => id,
        Err(why) => {
            eprintln!("the action's image was not built: {why}");
            return Err(1);
        }
    };
    let at = format!("{}/dagron/v1/steps/{id}", base.trim_end_matches('/'));
    let v = waited(agent, &at, bearer, "while its image was built")?;
    let said = v["output"].as_str().unwrap_or_default();
    say(said);
    if let Some(why) = v["error"].as_str() {
        eprintln!("the action's image was not built: {why}");
        return Err(1);
    }
    match v["image"].as_str() {
        Some(image) if !image.is_empty() => Ok(image.to_string()),
        _ => {
            eprintln!("the engine said the action's image was built, and named none");
            Err(1)
        }
    }
}

/// The id of what the engine was asked for, from its answer.
fn id_of(answer: Result<String, String>) -> Result<String, String> {
    answer
        .and_then(|text| serde_json::from_str::<Value>(&text).map_err(|e| e.to_string()))
        .and_then(|v| {
            v["id"]
                .as_str()
                .map(str::to_string)
                .ok_or_else(|| "the engine gave no id".into())
        })
}

/// What the engine says of the run at `at` once it has ended; else the
/// step's exit code, where it was cancelled (`during` what) or could not be
/// asked.
fn waited(agent: &ureq::Agent, at: &str, bearer: &str, during: &str) -> Result<Value, i32> {
    loop {
        if crate::run::take_cancel().is_some() {
            let _ = agent.delete(at).set("authorization", bearer).call();
            eprintln!("the step was cancelled {during}");
            return Err(143);
        }
        let polled = agent
            .get(&format!("{at}?wait=5"))
            .set("authorization", bearer)
            .call()
            .map_err(spoken)
            .and_then(|r| r.into_string().map_err(|e| e.to_string()))
            .and_then(|text| serde_json::from_str::<Value>(&text).map_err(|e| e.to_string()));
        match polled {
            Ok(v) if v["done"] == true => return Ok(v),
            Ok(_) => continue,
            Err(why) => {
                eprintln!("what became of the step's container could not be asked: {why}");
                return Err(1);
            }
        }
    }
}

/// What the engine said a step's container or build said, as the step's.
fn say(said: &str) {
    let mut out = std::io::stdout();
    let _ = out.write_all(said.as_bytes());
    if !said.is_empty() && !said.ends_with('\n') {
        let _ = out.write_all(b"\n");
    }
    let _ = out.flush();
}

fn ask(agent: &ureq::Agent, spec: &Spec, base: &str, bearer: &str, dir: &Path) -> i32 {
    let within = |name: &str| {
        let rel = dir.file_name().unwrap_or_default().to_string_lossy();
        format!("{WORKSPACE}/{rel}/{name}")
    };
    let mut env = spec.env.clone();
    env.retain(|(key, _)| {
        !FILES.iter().any(|kind| *key == format!("GITHUB_{kind}"))
            && !matches!(
                key.as_str(),
                "GITHUB_WORKSPACE" | "GITHUB_EVENT_PATH" | "RUNNER_TEMP"
            )
    });
    for kind in FILES {
        env.push((format!("GITHUB_{kind}"), within(&kind.to_ascii_lowercase())));
    }
    if let Some(event) = std::env::var_os("GITHUB_EVENT_PATH") {
        if std::fs::copy(&event, dir.join("event.json")).is_ok() {
            env.push(("GITHUB_EVENT_PATH".into(), within("event.json")));
        }
    }
    env.push(("GITHUB_WORKSPACE".into(), WORKSPACE.into()));
    env.push(("RUNNER_TEMP".into(), within("tmp")));
    env.push(("HOME".into(), within("home")));

    let url = format!("{}/dagron/v1/steps", base.trim_end_matches('/'));
    let body = json!({
        "image": spec.image,
        "entrypoint": spec.entrypoint,
        "args": spec.args,
        "env": env,
        "timeout_secs": spec.timeout_secs,
    });
    let asked = agent
        .post(&url)
        .set("authorization", bearer)
        .set("content-type", "application/json")
        .send_string(&body.to_string())
        .map_err(spoken)
        .and_then(|r| r.into_string().map_err(|e| e.to_string()));
    let id = match id_of(asked) {
        Ok(id) => id,
        Err(why) => {
            eprintln!("the step's container was not started: {why}");
            return 1;
        }
    };
    let at = format!("{url}/{id}");
    let v = match waited(agent, &at, bearer, "while its container ran") {
        Ok(v) => v,
        Err(code) => return code,
    };
    if let Some(why) = v["error"].as_str() {
        eprintln!("the step's container did not run: {why}");
        return 1;
    }
    say(v["output"].as_str().unwrap_or_default());
    if v["timed_out"] == true {
        eprintln!("the step's container ran out of time, and was stopped");
        return 124;
    }
    let code = v["exit_code"].as_i64().unwrap_or(1);
    i32::try_from(code.clamp(0, 255))
        .unwrap_or(1)
        .max(i32::from(code != 0))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn with_args_is_split_as_a_shell_splits_words() {
        assert_eq!(words("a b  c").unwrap(), ["a", "b", "c"]);
        assert_eq!(
            words(r#"-c "echo hi; echo there" 'a b' """#).unwrap(),
            ["-c", "echo hi; echo there", "a b", ""]
        );
        assert_eq!(words(r#"a\ b "c\"d""#).unwrap(), ["a b", "c\"d"]);
        assert!(words("\"open").is_err());
        assert!(words("ends\\").is_err());
        assert!(words("").unwrap().is_empty());
    }

    #[test]
    fn each_variable_is_the_last_of_it_and_the_containers_own_are_left_out() {
        let env = last_of_each(vec![
            ("A".into(), "1".into()),
            ("PATH".into(), "/bin".into()),
            ("B".into(), "2".into()),
            ("A".into(), "3".into()),
            ("HOME".into(), "/root".into()),
        ]);
        assert_eq!(env, [("A".into(), "3".into()), ("B".into(), "2".into())]);
    }

    #[test]
    fn a_spec_reads_back_as_it_was_written() {
        let mut spec = Spec {
            image: "alpine:3.20".into(),
            build: None,
            entrypoint: Some("/bin/sh".into()),
            args: vec!["-c".into(), "echo hi".into()],
            env: vec![("INPUT_WHO".into(), "rig".into())],
            timeout_secs: Some(60),
        };
        assert_eq!(Spec::from_json(&spec.to_json()).unwrap(), spec);
        spec.image = String::new();
        spec.build = Some(Build {
            context: "/dagron/actions/octo/hello/v1".into(),
            dockerfile: "Dockerfile".into(),
        });
        assert_eq!(Spec::from_json(&spec.to_json()).unwrap(), spec);
    }

    /// What an image is built from is the directory as it is, the same
    /// each time, with its links as links; and no more than the engine
    /// takes.
    #[test]
    fn an_image_is_built_from_its_directory_as_it_is() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("Dockerfile"), "FROM alpine:3.20\n").unwrap();
        std::fs::create_dir(dir.path().join("bin")).unwrap();
        std::fs::write(dir.path().join("bin/run.sh"), "echo hi\n").unwrap();
        std::os::unix::fs::symlink("/etc/passwd", dir.path().join("away")).unwrap();
        let once = context_of(dir.path(), "Dockerfile").unwrap();
        assert_eq!(
            context_of(dir.path(), "Dockerfile").unwrap(),
            once,
            "the same each time"
        );
        let mut read = tar::Archive::new(once.as_slice());
        let mut names = Vec::new();
        for entry in read.entries().unwrap() {
            let entry = entry.unwrap();
            let path = entry.path().unwrap().display().to_string();
            if path.ends_with("away") {
                assert_eq!(
                    entry.header().entry_type(),
                    tar::EntryType::Symlink,
                    "a link is not followed"
                );
            }
            names.push(path);
        }
        names.sort();
        assert!(names.iter().any(|n| n.ends_with("Dockerfile")), "{names:?}");
        assert!(names.iter().any(|n| n.ends_with("bin/run.sh")), "{names:?}");

        let big = vec![0u8; MAX_CONTEXT_BYTES + 1];
        std::fs::write(dir.path().join("big"), big).unwrap();
        let err = context_of(dir.path(), "Dockerfile").unwrap_err();
        assert!(err.to_string().contains("64 MiB"), "{err}");
    }

    #[test]
    fn a_dockerignore_leaves_out_what_docker_s_command_line_does() {
        for (pattern, path, matched) in [
            ("*.md", "README.md", true),
            ("*.md", "docs/README.md", false),
            ("**/*.md", "docs/README.md", true),
            ("**/foo", "a/b/foo", true),
            ("**/foo", "a/xfoo", false),
            ("docs", "docs", true),
            ("d?cs", "docs", true),
            ("d?cs", "d/cs", false),
        ] {
            assert_eq!(glob(pattern, path), matched, "{pattern} {path}");
        }
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join(".dockerignore"),
            "# what the image needs not\n*.md\n!KEEP.md\nsecret/\n/node_modules\n",
        )
        .unwrap();
        for file in [
            "Dockerfile",
            "README.md",
            "KEEP.md",
            "run.sh",
            "secret/key",
            "node_modules/x/y.js",
            ".dagron-ci-step-1/output",
        ] {
            let path = dir.path().join(file);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, "x").unwrap();
        }
        let tar = context_of(dir.path(), "Dockerfile").unwrap();
        let mut names: Vec<String> = tar::Archive::new(tar.as_slice())
            .entries()
            .unwrap()
            .map(|e| e.unwrap().path().unwrap().display().to_string())
            .collect();
        names.sort();
        assert_eq!(
            names,
            [".dockerignore", "Dockerfile", "KEEP.md", "run.sh"],
            "what the .dockerignore leaves out, and the step runner's own, are not sent"
        );
    }

    #[test]
    fn a_dockerfile_s_path_is_put_in_a_query_as_it_is() {
        assert_eq!(query_text("Dockerfile"), "Dockerfile");
        assert_eq!(query_text("a b/c&d.x"), "a%20b%2Fc%26d.x");
    }
}
