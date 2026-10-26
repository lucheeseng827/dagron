//! `dagron-ci import`: another CI's files as pipelines of `.dagron/ci`, by
//! the importers of `dagron-import` (GitHub Actions workflows, GitLab CI
//! pipelines and Jenkins Declarative Pipelines), and each held to
//! dagron-ci's own reader and compiler before it is called ok.
//!
//! **What is imported.** Each FILE, or, with none, what the checkout holds:
//! each workflow of `.github/workflows`, its GitLab pipeline
//! (`.gitlab-ci.yml`, or the file `.dagron/ci.yaml` names) and its
//! `Jenkinsfile`. A file's kind is told by where it is and what it is
//! called; `--from` says it for a file called something else. A pipeline is
//! named for its file ([`dagron_import::pipeline_name`]), unless `--name`.
//!
//! **The checkout is the directory this is run in.** A GitLab pipeline's
//! includes are read from it and from nowhere else: a path up out of it, or
//! a link that leads out, is not read, and is left to do by hand. A file
//! found rather than named is read from within it too. The images the
//! repository's settings give a `runs-on` label or a GitLab runner tag are
//! the importer's, under those `--image` gives.
//!
//! **What is printed.** Without `--write`, the one file's pipeline under
//! its report, on standard output, as `dagron-import` prints it. With it,
//! each file's to `.dagron/ci/<name>.yaml`, never over a file that is there
//! unless `--force`. Either way a line for each file says whether its
//! pipeline is one dagron-ci reads, and compiles for every event: its tasks,
//! the events it runs for, and how much of its report is to do by hand, was
//! changed on the way and was not carried over. A job with something left
//! to do is a pipeline all the same: its first step fails, and says what.
//!
//! **Exit codes.** 0 when each file was imported as a pipeline dagron-ci
//! reads and compiles (and, with `--write`, written); 1 when an importer
//! refused one, what it made is no such pipeline, or a file could not be
//! read or written; 2 for a usage error, or nothing to import.

use std::collections::BTreeMap;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use dagron_import::{Imported, Kind};

use crate::pipeline::compile::{compile, Changes, Event, Outcome, Settings};
use crate::pipeline::{github, gitlab, Pipeline};

pub(crate) const USAGE: &str =
    "       dagron-ci import [FILE]... [--from github|gitlab|jenkins] [--name NAME]
                        [--image LABEL=IMAGE]... [--default-branch BRANCH] [--write [--force]]";

/// The importers, by the name `--from` takes.
const FROM: [&str; 3] = ["github", "gitlab", "jenkins"];

/// Where `--write` writes, in the checkout.
const DIR: &str = ".dagron/ci";

/// The events an imported pipeline is compiled for.
const EVENTS: [&str; 5] = ["push", "tag", "pull_request", "merge_group", "dispatch"];

/// What `dagron-ci import` is asked.
#[derive(Debug, Default, PartialEq, Eq)]
struct Asked<'a> {
    files: Vec<&'a str>,
    from: Option<&'static str>,
    name: Option<&'a str>,
    images: BTreeMap<String, String>,
    default_branch: Option<&'a str>,
    write: bool,
    force: bool,
}

fn asked<'a>(args: &[&'a str]) -> Result<Asked<'a>, String> {
    let mut asked = Asked::default();
    let mut rest = args;
    loop {
        rest = match rest {
            [] => break,
            ["--from", from, tail @ ..] if asked.from.is_none() => {
                let Some(from) = FROM.iter().find(|f| *f == from) else {
                    return Err(format!("--from is github, gitlab or jenkins, not {from:?}"));
                };
                asked.from = Some(from);
                tail
            }
            ["--name", name, tail @ ..] if asked.name.is_none() => {
                if !is_file_name(name) {
                    return Err(format!(
                        "--name names a file of {DIR}: letters, digits, '.', '_' and '-', not \
                         first a '.', not {name:?}"
                    ));
                }
                asked.name = Some(name);
                tail
            }
            ["--image", pair, tail @ ..] => {
                match pair.split_once('=') {
                    Some((label, image)) if !label.is_empty() && !image.is_empty() => {
                        asked.images.insert(label.into(), image.into());
                    }
                    _ => return Err(format!("--image takes LABEL=IMAGE, not {pair:?}")),
                }
                tail
            }
            ["--default-branch", branch, tail @ ..] if asked.default_branch.is_none() => {
                asked.default_branch = Some(branch);
                tail
            }
            ["--write", tail @ ..] if !asked.write => {
                asked.write = true;
                tail
            }
            ["--force", tail @ ..] if !asked.force => {
                asked.force = true;
                tail
            }
            [file, tail @ ..] if !file.starts_with("--") => {
                asked.files.push(file);
                tail
            }
            [other, ..] => return Err(format!("{other:?} is not something import takes")),
        };
    }
    if asked.force && !asked.write {
        return Err("--force is for --write, which it lets write over a file".into());
    }
    if asked.name.is_some() && asked.files.len() != 1 {
        return Err("--name names one file's pipeline: name the file".into());
    }
    Ok(asked)
}

/// A name a file of `.dagron/ci` may have, as the run API's `pipeline`
/// may name it.
fn is_file_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 128
        && !name.starts_with('.')
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
}

/// A file to import, and its importer.
#[derive(Debug, PartialEq, Eq)]
struct Input {
    /// As named, or as found from the checkout: `.github/workflows/ci.yml`.
    path: String,
    from: &'static str,
    /// Found rather than named: read only where it is within the checkout.
    found: bool,
}

/// The importer for a file, told by where it is and what it is called.
fn from_of(path: &Path, settings: &github::Settings) -> Option<&'static str> {
    if crate::cli::is_workflow(path) {
        return Some("github");
    }
    if crate::cli::is_gitlab(path, settings) {
        return Some("gitlab");
    }
    let name = path.file_name()?.to_str()?;
    let jenkins = name.eq_ignore_ascii_case("jenkinsfile")
        || path
            .extension()
            .and_then(|e| e.to_str())
            .is_some_and(|e| e.eq_ignore_ascii_case("jenkinsfile"));
    jenkins.then_some("jenkins")
}

/// What the checkout at `root` holds to import: each workflow of
/// `.github/workflows`, its GitLab pipeline, and its `Jenkinsfile`.
fn found(root: &Path, settings: &github::Settings) -> Vec<Input> {
    let mut inputs = Vec::new();
    let workflows = root.join(".github/workflows");
    let mut names: Vec<String> = std::fs::read_dir(&workflows)
        .into_iter()
        .flatten()
        .filter_map(Result::ok)
        .map(|entry| entry.path())
        .filter(|p| {
            p.is_file() && matches!(p.extension().and_then(|e| e.to_str()), Some("yml" | "yaml"))
        })
        .filter_map(|p| p.file_name()?.to_str().map(str::to_string))
        .collect();
    names.sort();
    for name in names {
        inputs.push(Input {
            path: format!(".github/workflows/{name}"),
            from: "github",
            found: true,
        });
    }
    let gitlab = settings
        .gitlab
        .pipeline
        .clone()
        .unwrap_or_else(|| gitlab::DEFAULT_FILE.to_string());
    for (path, from) in [(gitlab, "gitlab"), ("Jenkinsfile".to_string(), "jenkins")] {
        if root.join(&path).is_file() {
            inputs.push(Input {
                path,
                from,
                found: true,
            });
        }
    }
    inputs
}

/// `dagron-ci import`, in the directory it is run in.
pub(crate) fn run(args: &[&str]) -> ExitCode {
    let (mut out, mut err) = (std::io::stdout(), std::io::stderr());
    ExitCode::from(import(args, Path::new("."), &mut out, &mut err))
}

fn import(args: &[&str], root: &Path, out: &mut dyn Write, err: &mut dyn Write) -> u8 {
    let usage = |err: &mut dyn Write, why: &str| {
        let _ = writeln!(err, "dagron-ci import: {why}\nusage:\n{USAGE}");
        2
    };
    let asked = match asked(args) {
        Ok(asked) => asked,
        Err(why) => return usage(err, &why),
    };
    // The repository's settings, for which file is its GitLab pipeline and
    // the images its labels and tags mean. One with a mistake in it is
    // `validate`'s to say.
    let settings = within(root, github::SETTINGS_FILE)
        .ok()
        .and_then(|file| std::fs::read_to_string(file).ok())
        .and_then(|text| github::settings(&text).ok())
        .unwrap_or_default();
    let mut inputs = Vec::new();
    for path in &asked.files {
        let Some(from) = asked.from.or_else(|| from_of(Path::new(path), &settings)) else {
            return usage(
                err,
                &format!("{path}: say what it is with --from github, gitlab or jenkins"),
            );
        };
        inputs.push(Input {
            path: path.to_string(),
            from,
            found: false,
        });
    }
    if inputs.is_empty() {
        inputs = found(root, &settings);
        if let Some(from) = asked.from {
            inputs.retain(|input| input.from == from);
        }
    }
    if inputs.is_empty() {
        return usage(
            err,
            "nothing to import here: no .github/workflows, .gitlab-ci.yml or Jenkinsfile; name \
             the file",
        );
    }
    if !asked.write && inputs.len() > 1 {
        let names: Vec<&str> = inputs.iter().map(|i| i.path.as_str()).collect();
        return usage(
            err,
            &format!(
                "{} files to import ({}): name one, or --write writes each to {DIR}",
                names.len(),
                names.join(", ")
            ),
        );
    }
    // Each file's pipeline name, which with --write names its file: two
    // files are never written to one.
    let mut names: Vec<Option<String>> = Vec::new();
    let mut written: BTreeMap<&str, &str> = BTreeMap::new();
    for input in &inputs {
        let name = asked
            .name
            .map(str::to_string)
            .or_else(|| dagron_import::pipeline_name(input.from, Path::new(&input.path)));
        if asked.write && !name.as_deref().is_some_and(is_file_name) {
            return usage(
                err,
                &format!(
                    "{}: its name is no file of {DIR}: import it alone, with --name",
                    input.path
                ),
            );
        }
        names.push(name);
    }
    if asked.write {
        for (input, name) in inputs.iter().zip(&names) {
            let name = name.as_deref().unwrap_or_default();
            if let Some(other) = written.insert(name, &input.path) {
                return usage(
                    err,
                    &format!(
                        "{other} and {} would both be {DIR}/{name}.yaml: import one of them \
                         alone, with --name",
                        input.path
                    ),
                );
            }
        }
    }

    let mut clean = true;
    for (input, name) in inputs.iter().zip(names) {
        let (ok, said) = one(input, name, &asked, &settings, root, out);
        clean &= ok;
        let _ = match asked.write {
            true => out.write_all(said.as_bytes()),
            false => err.write_all(said.as_bytes()),
        };
    }
    u8::from(!clean)
}

/// Import one file, print its pipeline (without `--write`) or write it
/// (with): whether all went well, and the lines that say how it went.
fn one(
    input: &Input,
    name: Option<String>,
    asked: &Asked,
    settings: &github::Settings,
    root: &Path,
    out: &mut dyn Write,
) -> (bool, String) {
    let path = &input.path;
    let file = name.clone().unwrap_or_default();
    let imported = match imported(input, name, asked, settings, root) {
        Ok(imported) => imported,
        Err(why) => return (false, format!("{path}: not imported: {why}\n")),
    };
    let text = imported.with_report();
    if !asked.write {
        let _ = write!(out, "{text}");
    }
    let summary = match check(&text) {
        Ok(summary) => summary,
        Err(problems) => {
            let said = problems
                .iter()
                .map(|problem| {
                    format!("{path}: imported, and not a pipeline dagron-ci reads: {problem}\n")
                })
                .collect();
            return (false, said);
        }
    };
    let count = |kind| imported.notes.iter().filter(|n| n.kind == kind).count();
    let report = format!(
        "{summary}; {} to do by hand, {} changed on the way, {} not carried over",
        count(Kind::Todo),
        count(Kind::Changed),
        count(Kind::Left)
    );
    if !asked.write {
        return (true, format!("{path}: ok ({report})\n"));
    }
    match write(root, &file, &text, asked.force) {
        Ok(written) => (true, format!("{path}: ok, as {written} ({report})\n")),
        Err(why) => (false, format!("{path}: not written: {why}\n")),
    }
}

/// `path` of the checkout at `root`, where it is within it once its links
/// are followed: a file is read from the checkout this is run in, and from
/// nowhere else it points.
pub(crate) fn within(root: &Path, path: &str) -> Result<PathBuf, String> {
    let root = root.canonicalize().map_err(|e| e.to_string())?;
    let file = root.join(path).canonicalize().map_err(|e| e.to_string())?;
    if file.starts_with(&root) {
        Ok(file)
    } else {
        Err("it leads out of the checkout".into())
    }
}

/// The file `input` names, imported by its importer.
fn imported(
    input: &Input,
    name: Option<String>,
    asked: &Asked,
    settings: &github::Settings,
    root: &Path,
) -> Result<Imported, String> {
    let file = if input.found {
        within(root, &input.path)?
    } else {
        root.join(&input.path)
    };
    let text = std::fs::read_to_string(&file).map_err(|e| format!("cannot be read: {e}"))?;
    // The settings' images, under those the command line gives.
    let images = |given: &BTreeMap<String, String>| {
        let mut images = given.clone();
        images.extend(asked.images.clone());
        images
    };
    let made = match input.from {
        "github" => dagron_import::github_to_pipeline(
            &text,
            &dagron_import::github::Options {
                name,
                images: images(&settings.runs_on),
            },
        ),
        "gitlab" => dagron_import::gitlab_to_pipeline(
            &text,
            &dagron_import::gitlab::Options {
                name,
                images: images(&settings.gitlab.tags),
                default_branch: asked.default_branch.map(str::to_string),
                included: dagron_import::gitlab::read_included(root, &text),
            },
        ),
        _ => dagron_import::jenkins_to_pipeline(
            &text,
            &dagron_import::jenkins::Options {
                name,
                images: images(&BTreeMap::new()),
            },
        ),
    };
    made.map_err(|e| format!("{e:#}"))
}

/// What dagron-ci makes of an imported pipeline, `text` as it would be
/// written: read by its own reader and compiled for every kind of event, its
/// name, its tasks and the events it runs for; or every problem it has.
fn check(text: &str) -> Result<String, Vec<String>> {
    let pipeline = crate::pipeline::parse(text)
        .map_err(|problems| problems.iter().map(ToString::to_string).collect::<Vec<_>>())?;
    let mut runs = Vec::new();
    let mut problems = Vec::new();
    for kind in EVENTS {
        match compile(&pipeline, &event(kind, &pipeline), &Settings::default()) {
            Ok(Outcome::Run(_)) => runs.push(kind),
            Ok(_) => {}
            Err(why) => problems.push(format!("a {kind} is refused: {why}")),
        }
    }
    if !problems.is_empty() {
        return Err(problems);
    }
    let tasks = pipeline.tasks.len();
    Ok(format!(
        "{}: {tasks} task{}; runs for {}",
        pipeline.name,
        if tasks == 1 { "" } else { "s" },
        if runs.is_empty() {
            "no event dagron-ci makes".to_string()
        } else {
            runs.join(", ")
        }
    ))
}

/// An event of `kind` for `pipeline`, with each input it requires sent.
fn event(kind: &str, pipeline: &Pipeline) -> Event {
    let inputs = match (kind, &pipeline.on.dispatch) {
        ("dispatch", Some(dispatch)) => dispatch
            .inputs
            .iter()
            .filter(|(_, input)| input.required)
            .map(|(name, _)| (name.clone(), "sent".to_string()))
            .collect(),
        _ => BTreeMap::new(),
    };
    Event {
        kind: kind.into(),
        git_ref: match kind {
            "pull_request" => "refs/pull/1/head".into(),
            "tag" => "refs/tags/v1.0.0".into(),
            _ => "refs/heads/main".into(),
        },
        sha: "0".repeat(40),
        base_ref: matches!(kind, "pull_request" | "merge_group").then(|| "main".to_string()),
        pr: (kind == "pull_request").then_some(1),
        trust: "trusted".into(),
        repo: "local/checkout".into(),
        forge_host: "localhost".into(),
        inputs,
        // A paths filter lets its pipeline run where the files are not
        // listed.
        changed: Changes::Unlisted("import".into()),
        ..Event::default()
    }
}

/// Write `text` as `.dagron/ci/<name>.yaml` of the checkout at `root`: never
/// over a file that is there unless `force`, and then by a rename, so that a
/// link there is replaced and not written through. The file it wrote, from
/// the checkout.
fn write(root: &Path, name: &str, text: &str, force: bool) -> Result<String, String> {
    // A directory at a time, each found within the checkout before anything
    // is made in it: a `.dagron` that is a link out is not made into.
    for made in [".dagron", DIR] {
        match std::fs::create_dir(root.join(made)) {
            Err(e) if e.kind() != std::io::ErrorKind::AlreadyExists => {
                return Err(format!("{made}: {e}"))
            }
            _ => {}
        }
        within(root, made).map_err(|why| format!("{made}: {why}"))?;
    }
    let dir = root.join(DIR);
    let shown = format!("{DIR}/{name}.yaml");
    if dir.join(format!("{name}.yml")).symlink_metadata().is_ok() {
        return Err(format!(
            "{DIR}/{name}.yml is there already: import it under another name, with --name"
        ));
    }
    let target = dir.join(format!("{name}.yaml"));
    let fresh = |path: &Path| {
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)?;
        file.write_all(text.as_bytes())?;
        file.sync_all()
    };
    if !force {
        return match fresh(&target) {
            Ok(()) => Ok(shown),
            Err(e) if e.kind() == std::io::ErrorKind::AlreadyExists => {
                Err(format!("{shown} is there already: --force writes over it"))
            }
            Err(e) => {
                let _ = std::fs::remove_file(&target);
                Err(format!("{shown}: {e}"))
            }
        };
    }
    let temp = dir.join(format!(".{name}.yaml.{}.tmp", std::process::id()));
    let done = fresh(&temp).and_then(|()| std::fs::rename(&temp, &target));
    match done {
        Ok(()) => Ok(shown),
        Err(e) => {
            let _ = std::fs::remove_file(&temp);
            Err(format!("{shown}: {e}"))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const WORKFLOW: &str = "name: build\non: [push, pull_request]\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/checkout@v4\n      - run: make test\n";
    const GITLAB: &str = "include:\n  - local: ci/inside.yml\n  - local: ci/escape.yml\n  - local: ../outside.yml\ntest:\n  image: rust:1\n  script: [cargo test]\n";
    const INSIDE: &str = "lint:\n  image: rust:1\n  script: [cargo clippy]\n";
    const JENKINSFILE: &str = "pipeline {\n  agent any\n  stages {\n    stage('Build') {\n      steps {\n        sh 'make'\n      }\n    }\n  }\n}\n";

    /// Run `import` in `root`: its exit code, what it printed and what it
    /// said.
    fn run(root: &Path, args: &[&str]) -> (u8, String, String) {
        let (mut out, mut err) = (Vec::new(), Vec::new());
        let code = import(args, root, &mut out, &mut err);
        (
            code,
            String::from_utf8(out).unwrap(),
            String::from_utf8(err).unwrap(),
        )
    }

    /// A checkout with a GitHub workflow, a GitLab pipeline that includes a
    /// file of its own and two from outside it, and a Jenkinsfile.
    fn checkout() -> (tempfile::TempDir, PathBuf) {
        let base = tempfile::tempdir().unwrap();
        let root = base.path().join("repo");
        std::fs::create_dir_all(root.join(".github/workflows")).unwrap();
        std::fs::create_dir_all(root.join("ci")).unwrap();
        std::fs::write(root.join(".github/workflows/build.yml"), WORKFLOW).unwrap();
        std::fs::write(root.join(".github/workflows/notes.txt"), "no workflow").unwrap();
        std::fs::write(root.join(".gitlab-ci.yml"), GITLAB).unwrap();
        std::fs::write(root.join("ci/inside.yml"), INSIDE).unwrap();
        std::fs::write(
            base.path().join("outside.yml"),
            "secret:\n  script: [cat /etc/shadow]\n",
        )
        .unwrap();
        #[cfg(unix)]
        std::os::unix::fs::symlink(base.path().join("outside.yml"), root.join("ci/escape.yml"))
            .unwrap();
        std::fs::write(root.join("Jenkinsfile"), JENKINSFILE).unwrap();
        (base, root)
    }

    #[test]
    fn what_import_is_asked_is_each_flag_once() {
        let asked = asked(&[
            "a.yml",
            "--from",
            "github",
            "--image",
            "self-hosted=debian:12",
            "--name",
            "build",
            "--write",
            "--force",
        ])
        .unwrap();
        assert_eq!(asked.files, ["a.yml"]);
        assert_eq!((asked.from, asked.name), (Some("github"), Some("build")));
        assert_eq!(asked.images["self-hosted"], "debian:12");
        assert!(asked.write && asked.force);
        for bad in [
            &["--from", "circleci"][..],
            &["--from", "github", "--from", "gitlab"],
            &["--image", "debian:12"],
            &["--name", "../escape", "a.yml"],
            &["--name", ".hidden", "a.yml"],
            &["--name", "x"],
            &["--name", "x", "a.yml", "b.yml"],
            &["--force"],
            &["--write", "--write"],
            &["--token", "t"],
        ] {
            assert!(super::asked(bad).is_err(), "{bad:?}");
        }
    }

    #[test]
    fn a_file_s_importer_is_told_by_where_it_is_and_its_name() {
        let mut settings = github::Settings::default();
        let from = |path: &str, settings: &github::Settings| from_of(Path::new(path), settings);
        assert_eq!(from(".github/workflows/ci.yml", &settings), Some("github"));
        assert_eq!(from("./.gitlab-ci.yml", &settings), Some("gitlab"));
        assert_eq!(from("Jenkinsfile", &settings), Some("jenkins"));
        assert_eq!(from("ci/deploy.Jenkinsfile", &settings), Some("jenkins"));
        assert_eq!(from("ci/main.yml", &settings), None);
        settings.gitlab.pipeline = Some("ci/main.yml".into());
        assert_eq!(from("ci/main.yml", &settings), Some("gitlab"));
    }

    #[test]
    fn one_file_is_printed_under_its_report_and_held_to_the_reader() {
        let (_base, root) = checkout();
        let (code, out, err) = run(&root, &[".github/workflows/build.yml"]);
        assert_eq!(code, 0, "{err}");
        assert!(
            out.starts_with("# Imported from a GitHub Actions workflow"),
            "{out}"
        );
        assert!(crate::pipeline::parse(&out).is_ok(), "{out}");
        assert_eq!(
            err,
            ".github/workflows/build.yml: ok (build: 1 task; runs for push, tag, \
             pull_request; 0 to do by hand, 3 changed on the way, 0 not carried over)\n"
        );
        assert!(!root.join(DIR).exists(), "nothing is written");

        // Several are not printed one after another: they are written.
        let (code, out, err) = run(&root, &[]);
        assert_eq!((code, out.as_str()), (2, ""));
        assert!(
            err.contains(
                "3 files to import (.github/workflows/build.yml, .gitlab-ci.yml, Jenkinsfile)"
            ),
            "{err}"
        );
        // A file whose kind is not told needs --from.
        std::fs::write(root.join("release.groovy"), JENKINSFILE).unwrap();
        let (code, _, err) = run(&root, &["release.groovy"]);
        assert_eq!(code, 2);
        assert!(err.contains("--from github, gitlab or jenkins"), "{err}");
        let (code, out, err) = run(&root, &["release.groovy", "--from", "jenkins"]);
        assert_eq!(code, 0, "{err}");
        assert!(out.contains("\nname: release\n"), "{out}");

        // An importer's refusal is the command's failure.
        std::fs::write(root.join("Jenkinsfile"), "node {\n  sh 'make'\n}\n").unwrap();
        let (code, out, err) = run(&root, &["Jenkinsfile"]);
        assert_eq!((code, out.as_str()), (1, ""));
        assert!(err.starts_with("Jenkinsfile: not imported: "), "{err}");
    }

    /// The images the repository's settings give a label or a tag are the
    /// importer's, under those the command line gives; settings that lead
    /// out of the checkout are not read.
    #[test]
    fn the_settings_images_are_the_importer_s_under_the_command_line_s() {
        let (base, root) = checkout();
        std::fs::create_dir_all(root.join(".dagron")).unwrap();
        std::fs::write(
            root.join(".dagron/ci.yaml"),
            "github:\n  runs_on: { ubuntu-latest: \"registry.example/ubuntu:24.04\" }\n\
             gitlab:\n  tags: { docker: \"registry.example/alpine:3.20\" }\n",
        )
        .unwrap();
        let (_, out, _) = run(&root, &[".github/workflows/build.yml"]);
        assert!(out.contains("registry.example/ubuntu:24.04"), "{out}");
        let (_, out, _) = run(
            &root,
            &[
                ".github/workflows/build.yml",
                "--image",
                "ubuntu-latest=registry.example/given:1",
            ],
        );
        assert!(
            out.contains("registry.example/given:1") && !out.contains("ubuntu:24.04"),
            "{out}"
        );
        std::fs::write(
            root.join(".gitlab-ci.yml"),
            "test:\n  tags: [docker]\n  script: [make]\n",
        )
        .unwrap();
        let (_, out, _) = run(&root, &[".gitlab-ci.yml"]);
        assert!(out.contains("registry.example/alpine:3.20"), "{out}");

        #[cfg(unix)]
        {
            std::fs::rename(root.join(".dagron/ci.yaml"), base.path().join("ci.yaml")).unwrap();
            std::os::unix::fs::symlink(base.path().join("ci.yaml"), root.join(".dagron/ci.yaml"))
                .unwrap();
            let (_, out, _) = run(&root, &[".github/workflows/build.yml"]);
            assert!(!out.contains("registry.example"), "{out}");
        }
    }

    #[test]
    fn write_writes_each_pipeline_and_reads_nothing_outside_the_checkout() {
        let (_base, root) = checkout();
        let (code, out, err) = run(&root, &["--write"]);
        assert_eq!((code, err.as_str()), (0, ""), "{out}");
        let lines: Vec<&str> = out.lines().collect();
        assert_eq!(lines.len(), 3, "{out}");
        assert!(
            lines[0].starts_with(
                ".github/workflows/build.yml: ok, as .dagron/ci/build.yaml (build: 1 task;"
            ),
            "{out}"
        );
        assert!(
            lines[1].starts_with(".gitlab-ci.yml: ok, as .dagron/ci/gitlab-ci.yaml (gitlab-ci: "),
            "{out}"
        );
        assert!(
            lines[2].starts_with("Jenkinsfile: ok, as .dagron/ci/ci.yaml (ci: 1 task;"),
            "{out}"
        );
        let gitlab = std::fs::read_to_string(root.join(".dagron/ci/gitlab-ci.yaml")).unwrap();
        assert!(
            gitlab.contains("cargo clippy"),
            "the include within: {gitlab}"
        );
        assert!(
            !gitlab.contains("/etc/shadow"),
            "neither a link out of the checkout nor a path up from it is read: {gitlab}"
        );
        assert!(
            gitlab.contains("ci/escape.yml") && gitlab.contains("../outside.yml"),
            "each is left to do: {gitlab}"
        );
        for file in ["build.yaml", "gitlab-ci.yaml", "ci.yaml"] {
            let text = std::fs::read_to_string(root.join(DIR).join(file)).unwrap();
            assert!(crate::pipeline::parse(&text).is_ok(), "{file}: {text}");
        }

        // Again: nothing is written over without --force.
        let (code, out, _) = run(&root, &["--write", "--from", "github"]);
        assert_eq!(code, 1, "{out}");
        assert_eq!(
            out,
            ".github/workflows/build.yml: not written: .dagron/ci/build.yaml is there already: \
             --force writes over it\n"
        );
        std::fs::write(root.join(".dagron/ci/build.yaml"), "edited").unwrap();
        let (code, out, _) = run(&root, &["--write", "--force", "--from", "github"]);
        assert_eq!(code, 0, "{out}");
        assert_ne!(
            std::fs::read_to_string(root.join(".dagron/ci/build.yaml")).unwrap(),
            "edited"
        );
        // A file of the name with the other ending is a pipeline of that
        // name already: another is not written beside it.
        std::fs::write(root.join(".dagron/ci/gitlab-ci.yml"), "mine").unwrap();
        let (code, out, _) = run(&root, &["--write", "--force", ".gitlab-ci.yml"]);
        assert_eq!(code, 1);
        assert!(
            out.contains(".dagron/ci/gitlab-ci.yml is there already"),
            "{out}"
        );
    }

    #[test]
    fn two_files_of_one_name_are_refused_before_anything_is_written() {
        let (_base, root) = checkout();
        std::fs::write(root.join(".github/workflows/ci.yml"), WORKFLOW).unwrap();
        let (code, out, err) = run(&root, &["--write"]);
        assert_eq!((code, out.as_str()), (2, ""));
        assert!(
            err.contains(
                ".github/workflows/ci.yml and Jenkinsfile would both be .dagron/ci/ci.yaml"
            ),
            "{err}"
        );
        assert!(!root.join(DIR).exists());
        let (code, out, err) = run(&root, &["--write", "Jenkinsfile", "--name", "jenkins"]);
        assert_eq!(code, 0, "{err}");
        assert!(
            out.starts_with("Jenkinsfile: ok, as .dagron/ci/jenkins.yaml"),
            "{out}"
        );
    }

    #[cfg(unix)]
    #[test]
    fn a_found_file_that_leads_out_of_the_checkout_is_not_read() {
        let (base, root) = checkout();
        std::fs::remove_file(root.join(".gitlab-ci.yml")).unwrap();
        std::fs::remove_file(root.join("Jenkinsfile")).unwrap();
        std::fs::write(base.path().join("elsewhere.yml"), WORKFLOW).unwrap();
        std::os::unix::fs::symlink(
            base.path().join("elsewhere.yml"),
            root.join(".github/workflows/linked.yml"),
        )
        .unwrap();
        let (code, out, _) = run(&root, &["--write"]);
        assert_eq!(code, 1, "{out}");
        assert!(
            out.contains(
                ".github/workflows/linked.yml: not imported: it leads out of the checkout"
            ),
            "{out}"
        );
        assert!(!root.join(".dagron/ci/linked.yaml").exists());
        // `.dagron/ci` that leads out of the checkout is not written into.
        std::fs::create_dir_all(base.path().join("elsewhere")).unwrap();
        std::fs::create_dir_all(root.join(".dagron")).unwrap();
        std::fs::remove_dir_all(root.join(DIR)).unwrap();
        std::os::unix::fs::symlink(base.path().join("elsewhere"), root.join(DIR)).unwrap();
        let (code, out, _) = run(&root, &["--write", ".github/workflows/build.yml"]);
        assert_eq!(code, 1, "{out}");
        assert!(out.contains("it leads out of the checkout"), "{out}");
        assert_eq!(
            std::fs::read_dir(base.path().join("elsewhere"))
                .unwrap()
                .count(),
            0
        );
        // Nor is a `.dagron` that leads out: no `ci` is made in it.
        std::fs::remove_file(root.join(DIR)).unwrap();
        std::fs::remove_dir(root.join(".dagron")).unwrap();
        std::os::unix::fs::symlink(base.path().join("elsewhere"), root.join(".dagron")).unwrap();
        let (code, out, _) = run(&root, &["--write", ".github/workflows/build.yml"]);
        assert_eq!(code, 1, "{out}");
        assert_eq!(
            out,
            ".github/workflows/build.yml: not written: .dagron: it leads out of the checkout\n"
        );
        assert_eq!(
            std::fs::read_dir(base.path().join("elsewhere"))
                .unwrap()
                .count(),
            0
        );
    }
}
