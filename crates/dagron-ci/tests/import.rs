//! What `dagron-import` makes of a GitHub Actions workflow, a GitLab CI
//! pipeline and a Jenkinsfile: case by case, each beside the pipeline it
//! comes to, and each of those a pipeline dagron-ci reads and compiles.
//!
//! A case is a workflow file of `tests/import/cases` (GitHub's),
//! `tests/import/gitlab/cases` or `tests/import/jenkins/cases`. Its golden
//! file, of the same name in the `golden` beside it, is what
//! `dagron-import github`, `gitlab` or `jenkins` prints for it: the
//! pipeline, under the report of what was changed on the way, what was not
//! carried over, and what is left to do by hand. A GitLab case's
//! `<name>.files` directory holds the files it includes, by their path in
//! its repository.
//!
//! The importer is a crate of its own and knows nothing of dagron-ci, so
//! it is here that what it writes is held to being a pipeline: read by
//! dagron-ci's own reader, compiled for every kind of event, run by at
//! least one, and taken by dagron's parser as the workflow it compiles to.
//!
//! After a change that means to change what is imported:
//!
//! ```sh
//! DAGRON_CI_BLESS=1 cargo test -p dagron-ci --test import
//! git diff crates/dagron-ci/tests/import/golden
//! ```

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};

use dagron_ci::pipeline::compile::{compile, Changes, Event, Outcome, Settings};
use dagron_ci::pipeline::{self, Pipeline};
use dagron_import::github::{github_to_pipeline, Imported, Options};

/// The fewest workflows the corpus holds.
const AT_LEAST: usize = 20;

/// The fewest pipelines of GitLab's, and Jenkinsfiles, it holds: as many
/// as it does, so that a case taken away with its golden file is missed.
const AT_LEAST_GITLAB: usize = 18;
const AT_LEAST_JENKINS: usize = 18;

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

/// An event of `kind` for `pipeline`, with every input it requires sent.
fn event(kind: &str, pipeline: &Pipeline) -> Event {
    let inputs: BTreeMap<String, String> = match (kind, &pipeline.on.dispatch) {
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
            "pull_request" => "refs/pull/7/head".into(),
            "tag" => "refs/tags/v1.2.3".into(),
            _ => "refs/heads/main".into(),
        },
        sha: "4b4708e725f29891d22b4c96af9f0a403ff51c7c".into(),
        base_sha: None,
        base_ref: matches!(kind, "pull_request" | "merge_group").then(|| "main".to_string()),
        pr: (kind == "pull_request").then_some(7),
        trust: "trusted".into(),
        repo: "acme/widgets".into(),
        forge: String::new(),
        forge_host: "github.com".into(),
        inputs,
        actor: None,
        head_ref: None,
        default_branch: None,
        run_numbers: None,
        // A paths filter lets its pipeline run where the files cannot be had.
        changed: Changes::Unlisted("not looked up".into()),
        payload: None,
    }
}

/// Every file of a directory and those below it, by its path from there.
fn tree(dir: &Path) -> BTreeMap<String, String> {
    let mut out = BTreeMap::new();
    let mut wanted = vec![dir.to_path_buf()];
    while let Some(at) = wanted.pop() {
        for entry in std::fs::read_dir(&at).unwrap_or_else(|e| panic!("{}: {e}", at.display())) {
            let path = entry.unwrap().path();
            if path.is_dir() {
                wanted.push(path);
            } else {
                let name = path.strip_prefix(dir).unwrap().to_string_lossy().replace('\\', "/");
                out.insert(name, read(&path));
            }
        }
    }
    out
}

/// Each case of `corpus` imported, and held to its golden file and to being
/// a pipeline: read by dagron-ci's own reader, compiled for every kind of
/// event, run by at least one, and taken by dagron's parser as the workflow
/// it compiles to.
fn each_case(corpus: &Path, ending: &str, at_least: usize, import: &dyn Fn(&str, &str) -> Imported) {
    let (cases, golden) = (corpus.join("cases"), corpus.join("golden"));
    let bless = std::env::var_os("DAGRON_CI_BLESS").is_some();
    let mut wrong = Vec::new();
    let mut seen = BTreeSet::new();
    for path in files(&cases, ending) {
        let name = path.file_stem().unwrap().to_str().unwrap().to_string();
        let text = read(&path);
        let imported = import(&name, &text);
        let printed = imported.with_report();
        assert_eq!(
            printed,
            import(&name, &text).with_report(),
            "{name}: the same file was imported two ways"
        );

        // What is printed is a file of `.dagron/ci`, comment and all.
        match pipeline::parse(&printed) {
            Err(problems) => wrong.push(format!(
                "{name}: what was imported is no pipeline: {problems:#?}\n{printed}"
            )),
            Ok(pipeline) => {
                let mut ran = Vec::new();
                for kind in ["push", "tag", "pull_request", "merge_group", "dispatch"] {
                    match compile(&pipeline, &event(kind, &pipeline), &Settings::default()) {
                        Ok(Outcome::Run(compiled)) => {
                            let taken = dagron_core::dag::DagGraph::from_yaml_with_params(
                                &compiled.yaml,
                                &compiled.parameters,
                            );
                            if let Err(e) = taken {
                                wrong.push(format!(
                                    "{name}: dagron refuses what it compiles to for a {kind}: {e:#}"
                                ));
                            }
                            ran.push(kind);
                        }
                        Ok(_) => {}
                        Err(why) => wrong.push(format!("{name}: a {kind} is refused: {why}")),
                    }
                }
                if ran.is_empty() {
                    wrong.push(format!("{name}: no event runs what was imported"));
                }
            }
        }

        let said = golden.join(format!("{name}.yaml"));
        if bless {
            std::fs::write(&said, &printed).unwrap();
        }
        match said.is_file() {
            true if read(&said) == printed => {}
            true => wrong.push(format!(
                "{name}: no longer what its golden file says:\n{printed}"
            )),
            false => wrong.push(format!("{name}: has no golden file")),
        }
        seen.insert(name);
    }
    for path in files(&golden, "yaml") {
        let name = path.file_stem().unwrap().to_str().unwrap().to_string();
        if !seen.contains(&name) {
            match bless {
                true => std::fs::remove_file(&path).unwrap(),
                false => wrong.push(format!("{name}: a golden file with no case")),
            }
        }
    }
    // What else lies among the cases would be no case at all, and pass
    // unread: a file of another ending (`.yaml`, `.Jenkinsfile`), or the
    // `.files` of no case.
    for entry in std::fs::read_dir(&cases).unwrap() {
        let path = entry.unwrap().path();
        let file = path.file_name().unwrap().to_string_lossy().into_owned();
        let case = match file.strip_suffix(".files") {
            Some(name) if path.is_dir() => seen.contains(name),
            _ => path.is_file() && path.extension().and_then(|e| e.to_str()) == Some(ending),
        };
        if !case {
            wrong.push(format!("{file}: a file among the cases that is none of them"));
        }
    }
    for entry in std::fs::read_dir(&golden).unwrap() {
        let path = entry.unwrap().path();
        if !(path.is_file() && path.extension().and_then(|e| e.to_str()) == Some("yaml")) {
            let file = path.file_name().unwrap().to_string_lossy().into_owned();
            wrong.push(format!("{file}: a file among the golden ones that is none of them"));
        }
    }
    assert!(
        wrong.is_empty(),
        "{} problems in {} cases (DAGRON_CI_BLESS=1 rewrites the golden files; read the \
         diff):\n\n{}",
        wrong.len(),
        seen.len(),
        wrong.join("\n\n")
    );
    assert!(
        seen.len() >= at_least,
        "the corpus holds {} cases, and is to hold {at_least}",
        seen.len()
    );
}

#[test]
fn each_workflow_is_imported_as_its_golden_file_says_and_is_a_pipeline() {
    let corpus = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/import");
    each_case(&corpus, "yml", AT_LEAST, &|name, text| {
        let options = Options {
            name: Some(name.to_string()),
            // A team's own runners, as `--image self-hosted=…` would say.
            images: BTreeMap::from([(
                "self-hosted".to_string(),
                "docker.io/library/debian:12".to_string(),
            )]),
        };
        github_to_pipeline(text, &options).unwrap_or_else(|e| panic!("{name}: not imported: {e:#}"))
    });
}

#[test]
fn each_gitlab_pipeline_is_imported_as_its_golden_file_says_and_is_a_pipeline() {
    let corpus = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/import/gitlab");
    let cases = corpus.join("cases");
    each_case(&corpus, "yml", AT_LEAST_GITLAB, &|name, text| {
        let files = cases.join(format!("{name}.files"));
        let options = dagron_import::gitlab::Options {
            name: Some(name.to_string()),
            // A team's own runners, as `--image docker=…` would say.
            images: BTreeMap::from([(
                "docker".to_string(),
                "docker.io/library/debian:12".to_string(),
            )]),
            default_branch: None,
            included: match files.is_dir() {
                true => tree(&files),
                false => BTreeMap::new(),
            },
        };
        dagron_import::gitlab_to_pipeline(text, &options)
            .unwrap_or_else(|e| panic!("{name}: not imported: {e:#}"))
    });
}

#[test]
fn each_jenkinsfile_is_imported_as_its_golden_file_says_and_is_a_pipeline() {
    let corpus = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/import/jenkins");
    each_case(&corpus, "jenkinsfile", AT_LEAST_JENKINS, &|name, text| {
        let options = dagron_import::jenkins::Options {
            name: Some(name.to_string()),
            // A team's own agents, as `--image linux=…` would say.
            images: BTreeMap::from([(
                "linux".to_string(),
                "docker.io/library/debian:12".to_string(),
            )]),
        };
        dagron_import::jenkins_to_pipeline(text, &options)
            .unwrap_or_else(|e| panic!("{name}: not imported: {e:#}"))
    });
}

/// The migration guide shows a workflow and the pipeline the importer writes
/// for it. Both are the first case here, and the guide holds them as they
/// are: a change to what the importer writes shows in the guide too, or
/// this says that the guide is out of date. Skipped where the documents are
/// not beside the crate.
#[test]
fn the_migration_guide_shows_what_the_importer_writes() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let Ok(guide) = std::fs::read_to_string(root.join("../../docs/CI_MIGRATING.md")) else {
        return;
    };
    let guide = guide.replace("\r\n", "\n");
    let corpus = root.join("tests/import");
    let name = "01-node-lint-test-publish";
    let workflow = read(&corpus.join(format!("cases/{name}.yml")));
    let written = read(&corpus.join(format!("golden/{name}.yaml")));
    let from = |text: &str| text[text.find("name: ci\n").expect("the example's first key")..].to_string();
    assert!(
        guide.contains(&from(&workflow)),
        "docs/CI_MIGRATING.md no longer shows the workflow of tests/import/cases/{name}.yml"
    );
    assert!(
        guide.contains(&from(&written)),
        "docs/CI_MIGRATING.md no longer shows the pipeline dagron-import writes for its own \
         example: copy it from tests/import/golden/{name}.yaml"
    );
}
