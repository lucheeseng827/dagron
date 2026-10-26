//! The golden-file corpus: what dagron-ci makes of a pipeline, or of a
//! `.github/workflows` file, for an event. Case by case, each beside a file
//! that holds the answer in full.
//!
//! A case is a file of `tests/corpus/cases`: what it shows, in a sentence,
//! an event, and a source. Its golden file, of the same name in
//! `tests/corpus/golden`, is all of what came of it: the problems of a file
//! that is no pipeline, why an event is refused or passed over, or the
//! workflow that is submitted, with its parameters.
//!
//! The answer is generated, and read by a person when it changes. After a
//! change that means to change one:
//!
//! ```sh
//! DAGRON_CI_BLESS=1 cargo test -p dagron-ci --test corpus
//! git diff crates/dagron-ci/tests/corpus/golden
//! ```
//!
//! Three things are checked of every case beside its golden file: that the
//! same inputs give the same bytes twice (the compiler is pure), that a
//! workflow which comes of it is one dagron's own parser takes, and that no
//! golden file is left without its case.

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use dagron_ci::pipeline::compile::{self, compile, Changes, Event, Outcome};
use dagron_ci::pipeline::{self, github, gitlab, Problem};
use serde::Deserialize;

/// The fewest cases the corpus holds.
const AT_LEAST: usize = 100;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    /// What the case shows, in a sentence.
    shows: String,
    /// `native`, a pipeline of `.dagron/ci`; or `github`, a workflow file.
    #[serde(default)]
    front: Front,
    /// The file's name without its ending: what a workflow file's pipeline
    /// is called by.
    #[serde(default = "ci")]
    file: String,
    #[serde(default)]
    event: CaseEvent,
    #[serde(default)]
    settings: CaseSettings,
    /// A repository's settings for its workflow files (`.dagron/ci.yaml`).
    #[serde(default)]
    github: CaseGithub,
    /// A GitLab project's (`.dagron/ci.yaml`'s `gitlab:`), and the files its
    /// pipeline includes.
    #[serde(default)]
    gitlab: CaseGitlab,
    source: String,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct CaseGitlab {
    #[serde(default)]
    tags: BTreeMap<String, String>,
    #[serde(default)]
    included: BTreeMap<String, String>,
}

fn ci() -> String {
    "ci".into()
}

#[derive(Default, Deserialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum Front {
    #[default]
    Native,
    Github,
    Gitlab,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct CaseEvent {
    #[serde(default = "push")]
    kind: String,
    #[serde(default, rename = "ref")]
    git_ref: Option<String>,
    #[serde(default)]
    sha: Option<String>,
    #[serde(default)]
    base_sha: Option<String>,
    #[serde(default)]
    base_ref: Option<String>,
    #[serde(default)]
    pr: Option<i64>,
    #[serde(default)]
    trust: Option<String>,
    #[serde(default)]
    inputs: BTreeMap<String, String>,
    /// The files the event changed. Absent: not looked up.
    #[serde(default)]
    changed: Option<Vec<String>>,
    /// Looked up, and not to be had, for this reason.
    #[serde(default)]
    unlisted: Option<String>,
}

fn push() -> String {
    "push".into()
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct CaseSettings {
    #[serde(default)]
    default_timeout_secs: Option<u64>,
    #[serde(default)]
    checkout: Option<bool>,
}

#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct CaseGithub {
    #[serde(default)]
    runs_on: BTreeMap<String, String>,
    #[serde(default)]
    provided: BTreeSet<String>,
    /// The operator's `CI_ACTIONS_MIRROR` and `CI_ACTIONS_ALLOW`.
    #[serde(default)]
    actions_mirror: String,
    #[serde(default)]
    actions_allow: String,
    /// The operator's `CI_GITHUB_OIDC`, and the repository's
    /// `github.oidc_audiences`.
    #[serde(default)]
    oidc: bool,
    #[serde(default)]
    oidc_audiences: Vec<String>,
    /// The operator's `CI_GITHUB_TOKEN`.
    #[serde(default)]
    github_token: bool,
    /// The repository's `github.docker`.
    #[serde(default)]
    docker: bool,
}

impl Case {
    fn event(&self) -> Event {
        let e = &self.event;
        let kind = if e.kind.is_empty() { "push" } else { &e.kind };
        Event {
            kind: kind.into(),
            git_ref: e.git_ref.clone().unwrap_or_else(|| match kind {
                "pull_request" => "refs/pull/7/head".into(),
                "tag" => "refs/tags/v1.2.3".into(),
                "merge_group" => "refs/heads/gh-readonly-queue/main/pr-7-abc".into(),
                _ => "refs/heads/main".into(),
            }),
            sha: e
                .sha
                .clone()
                .unwrap_or_else(|| "4b4708e725f29891d22b4c96af9f0a403ff51c7c".into()),
            base_sha: e.base_sha.clone(),
            base_ref: e.base_ref.clone().or_else(|| {
                matches!(kind, "pull_request" | "merge_group").then(|| "main".to_string())
            }),
            pr: e.pr.or((kind == "pull_request").then_some(7)),
            trust: e.trust.clone().unwrap_or_else(|| "trusted".into()),
            repo: "acme/widgets".into(),
            forge: String::new(),
            forge_host: "github.com".into(),
            inputs: e.inputs.clone(),
            actor: None,
            head_ref: None,
            default_branch: None,
            run_numbers: None,
            payload: None,
            changed: match (&e.changed, &e.unlisted) {
                (Some(files), _) => Changes::Listed(files.clone()),
                (None, Some(why)) => Changes::Unlisted(why.clone()),
                (None, None) => Changes::Unknown,
            },
        }
    }

    fn settings(&self) -> compile::Settings {
        let default = compile::Settings::default();
        compile::Settings {
            default_timeout_secs: self
                .settings
                .default_timeout_secs
                .unwrap_or(default.default_timeout_secs),
            checkout: self.settings.checkout.unwrap_or(default.checkout),
            patched: false,
        }
    }
}

fn problems(out: &mut String, problems: &[Problem]) {
    out.push_str("not a pipeline:\n");
    for p in problems {
        match p.path.is_empty() {
            true => out.push_str(&format!("  {}\n", p.message)),
            false => out.push_str(&format!("  {}: {}\n", p.path, p.message)),
        }
    }
}

/// All of what comes of `case`, as its golden file has it; and the workflow
/// it compiles to, where it runs.
fn outcome(case: &Case) -> (String, Option<compile::Compiled>) {
    let mut event = case.event();
    if case.front == Front::Gitlab {
        event.forge = "gitlab".into();
        event.forge_host = "gitlab.example".into();
        event.repo = "group/app".into();
        event.default_branch = Some("main".into());
    }
    let mut out = String::new();
    let pipeline = match case.front {
        Front::Gitlab => {
            let settings = gitlab::Settings {
                pipeline: Some(".gitlab-ci.yml".into()),
                tags: case.gitlab.tags.clone(),
                included: case
                    .gitlab
                    .included
                    .iter()
                    .map(|(path, text)| (path.clone(), Ok(text.clone())))
                    .collect(),
            };
            let lowered = gitlab::parse(&case.source, &settings.included)
                .and_then(|ci| ci.lower(&case.file, &event, &settings));
            match lowered {
                Ok(lowered)
                    if lowered.needs_changes && event.changed == compile::Changes::Unknown =>
                {
                    out.push_str("needs the files the event changed\n");
                    return (out, None);
                }
                Ok(lowered) => {
                    if let Some(why) = &lowered.passed_over {
                        out.push_str(&format!("passed over: {why}\n"));
                        return (out, None);
                    }
                    lowered.pipeline
                }
                Err(found) => {
                    problems(&mut out, &found);
                    return (out, None);
                }
            }
        }
        Front::Native => match pipeline::parse(&case.source) {
            Ok(pipeline) => pipeline,
            Err(found) => {
                problems(&mut out, &found);
                return (out, None);
            }
        },
        Front::Github => {
            let settings = github::Settings {
                workflows: vec![format!("{}.yml", case.file)],
                runs_on: case.github.runs_on.clone(),
                provided: case.github.provided.clone(),
                actions: github::ActionSource::parse(
                    &case.github.actions_mirror,
                    &case.github.actions_allow,
                )
                .unwrap_or_else(|why| panic!("{}: {why}", case.file)),
                oidc: case.github.oidc,
                oidc_audiences: case.github.oidc_audiences.clone(),
                github_token: case.github.github_token,
                docker: case.github.docker,
                api_url: None,
                gitlab: Default::default(),
            };
            let lowered = github::parse(&case.source, &settings.provided)
                .and_then(|workflow| workflow.lower(&case.file, &event, &settings));
            match lowered {
                Ok(lowered) => {
                    if !lowered.skipped.is_empty() {
                        out.push_str("jobs left out:\n");
                        for (job, why) in &lowered.skipped {
                            out.push_str(&format!("  {job}: {why}\n"));
                        }
                        out.push('\n');
                    }
                    lowered.pipeline
                }
                Err(found) => {
                    problems(&mut out, &found);
                    return (out, None);
                }
            }
        }
    };
    match compile(&pipeline, &event, &case.settings()) {
        Err(why) => out.push_str(&format!("refused: {why}\n")),
        Ok(Outcome::Skip(why)) => out.push_str(&format!("passed over: {why}\n")),
        Ok(Outcome::Unchanged { why, jobs }) => {
            out.push_str(&format!("unchanged: {why}\njobs: {}\n", jobs.join(", ")))
        }
        Ok(Outcome::NeedsChangedFiles) => out.push_str("needs the files the event changed\n"),
        Ok(Outcome::Run(compiled)) => {
            out.push_str("runs\n\nparameters:\n");
            for (name, value) in &compiled.parameters {
                out.push_str(&format!("  {name}: {value:?}\n"));
            }
            out.push_str("\nworkflow:\n");
            out.push_str(&compiled.yaml);
            if !compiled.yaml.ends_with('\n') {
                out.push('\n');
            }
            return (out, Some(compiled));
        }
    }
    (out, None)
}

fn files(dir: &Path, ending: &str) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .map(|entry| entry.unwrap().path())
        .filter(|path| path.extension().and_then(|e| e.to_str()) == Some(ending))
        .collect();
    files.sort();
    files
}

/// A file's text, with a checkout's line endings read as the tree's.
fn read(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("{}: {e}", path.display()))
        .replace("\r\n", "\n")
}

#[test]
fn each_case_comes_to_what_its_golden_file_says() {
    let corpus = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/corpus");
    let (cases, golden) = (corpus.join("cases"), corpus.join("golden"));
    let bless = std::env::var_os("DAGRON_CI_BLESS").is_some();
    let mut changed = Vec::new();
    let mut seen = BTreeSet::new();
    let mut shown = BTreeSet::new();
    let mut ran = 0;
    for path in files(&cases, "yaml") {
        let name = path.file_stem().unwrap().to_str().unwrap().to_string();
        let case: Case = serde_yaml::from_str(&read(&path))
            .unwrap_or_else(|e| panic!("{}: not a case: {e}", path.display()));
        assert!(
            case.shows.trim().len() > 20 && shown.insert(case.shows.trim().to_string()),
            "{name}: a case says what it shows, and no two say the same"
        );
        let (out, compiled) = outcome(&case);
        assert_eq!(
            out,
            outcome(&case).0,
            "{name}: the same inputs came to two answers"
        );
        if let Some(compiled) = compiled {
            ran += 1;
            // Every one is said, and not the first alone: a change to the
            // compiler may break many at once.
            let taken = dagron_core::dag::DagGraph::from_yaml_with_params(
                &compiled.yaml,
                &compiled.parameters,
            );
            if let Err(e) = taken {
                changed.push(format!(
                    "{name}: dagron refuses the workflow it compiles to: {e:#}"
                ));
            }
        }
        let said = golden.join(format!("{name}.golden"));
        if bless {
            std::fs::write(&said, &out).unwrap();
        }
        match said.is_file() {
            true if read(&said) == out => {}
            true => changed.push(format!(
                "{name}: no longer what its golden file says:\n{out}"
            )),
            false => changed.push(format!("{name}: has no golden file")),
        }
        seen.insert(name);
    }
    for path in files(&golden, "golden") {
        let name = path.file_stem().unwrap().to_str().unwrap().to_string();
        if !seen.contains(&name) {
            match bless {
                true => std::fs::remove_file(&path).unwrap(),
                false => changed.push(format!("{name}: a golden file with no case")),
            }
        }
    }
    assert!(
        changed.is_empty(),
        "{} of {} cases differ (DAGRON_CI_BLESS=1 rewrites the golden files; read the \
         diff):\n\n{}",
        changed.len(),
        seen.len(),
        changed.join("\n")
    );
    assert!(
        seen.len() >= AT_LEAST,
        "the corpus holds {} cases, and is to hold {AT_LEAST}",
        seen.len()
    );
    // A corpus of files that are no pipeline would check no compiler.
    assert!(ran * 2 >= seen.len(), "{ran} of {} cases run", seen.len());
}
