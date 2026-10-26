//! The pipelines this tree ships are ones dagron runs: the starters in
//! `examples/ci`, and the repository's own in `.dagron/ci`.
//!
//! `dagron-ci validate` checks a pipeline's own keys. Dagron's pass through
//! as written, and dagron-ci cannot link dagron-core to check them (one
//! database backend per build): one written wrongly is refused when its run
//! is submitted. A task's `env:` is the one people get wrong, a list of
//! names and values where a step's is a map. So each shipped pipeline is
//! compiled here and read by dagron's own parser, as dagron-api reads a run
//! that is submitted.

use std::path::{Path, PathBuf};

use dagron_ci::pipeline::compile::{compile, Changes, Event, Outcome, Settings};

/// The pipeline files in `dir`, in order.
fn pipelines(dir: &Path) -> Vec<PathBuf> {
    let mut files: Vec<PathBuf> = std::fs::read_dir(dir)
        .unwrap_or_else(|e| panic!("{}: {e}", dir.display()))
        .map(|entry| entry.unwrap().path())
        .filter(|path| {
            matches!(
                path.extension().and_then(|e| e.to_str()),
                Some("yaml" | "yml")
            )
        })
        .collect();
    files.sort();
    files
}

/// An event of `kind` that a pipeline with no filter of its own would take.
fn event(kind: &str) -> Event {
    Event {
        kind: kind.into(),
        git_ref: match kind {
            "pull_request" => "refs/pull/7/head".into(),
            _ => "refs/heads/main".into(),
        },
        sha: "4b4708e725f29891d22b4c96af9f0a403ff51c7c".into(),
        base_ref: (kind == "pull_request").then(|| "main".into()),
        pr: (kind == "pull_request").then_some(7),
        trust: "trusted".into(),
        repo: "acme/widgets".into(),
        forge_host: "github.com".into(),
        // Which files changed is not known here: a paths filter lets its
        // pipeline run, as it does for a change too large for a forge to
        // list.
        changed: Changes::Unlisted("not looked up".into()),
        // A run of `ci` failed: what the event that says so carries.
        inputs: match kind {
            "failed" => [
                ("ci_failed_pipeline", "ci"),
                ("ci_failed_run", "0190f0a0-0000-7000-8000-000000000009"),
                ("ci_failed_job", "test"),
            ]
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
            _ => Default::default(),
        },
        ..Event::default()
    }
}

/// Compile the pipeline at `path` for each kind of event, and hand every
/// workflow that comes of it to dagron's own parse, expand and validate. At
/// least one kind must run it: a pipeline no event here runs was not checked.
/// Returns the kinds that run it, in the order they are tried.
fn dagron_takes(path: &Path) -> Vec<&'static str> {
    let shown = path.display();
    let text = std::fs::read_to_string(path).unwrap_or_else(|e| panic!("{shown}: {e}"));
    let pipeline = dagron_ci::pipeline::parse(&text)
        .unwrap_or_else(|problems| panic!("{shown} is not a pipeline: {problems:?}"));
    let mut ran = Vec::new();
    for kind in ["push", "pull_request", "merge_group", "dispatch", "failed"] {
        match compile(&pipeline, &event(kind), &Settings::default()) {
            Ok(Outcome::Run(compiled)) => {
                dagron_core::dag::DagGraph::from_yaml_with_params(
                    &compiled.yaml,
                    &compiled.parameters,
                )
                .unwrap_or_else(|e| {
                    panic!(
                        "{shown}: dagron refuses the workflow it compiles to for a {kind}: \
                         {e:#}\n{}",
                        compiled.yaml
                    )
                });
                ran.push(kind);
            }
            Ok(_) => {}
            Err(why) => panic!("{shown}: a {kind} is refused: {why}"),
        }
    }
    assert!(!ran.is_empty(), "{shown}: no event here runs it");
    eprintln!("{shown}: dagron takes it, for {}", ran.join(", "));
    ran
}

#[test]
fn dagron_runs_each_starter_pipeline() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/ci");
    let files = pipelines(&dir);
    let names: Vec<_> = files
        .iter()
        .map(|path| path.file_name().unwrap().to_str().unwrap())
        .collect();
    assert_eq!(
        names,
        [
            "dbt.yaml",
            "go.yaml",
            "java.yaml",
            "node.yaml",
            "python.yaml",
            "rust.yaml"
        ],
        "the starters the documents name"
    );
    for file in &files {
        // What the README and each starter's own header say runs it. One
        // that lost either would still be taken, for the other.
        assert_eq!(
            dagron_takes(file),
            ["push", "pull_request"],
            "{}: a starter runs on a push and on a pull request, and on nothing else",
            file.display()
        );
    }
}

/// The agent recipes in `examples/ci/agents`: the review is run for a pull
/// request alone, the fix for a failed run of `ci` alone, and dagron takes
/// what each compiles to.
#[test]
fn dagron_runs_each_agent_recipe() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../examples/ci/agents");
    let files = pipelines(&dir);
    let names: Vec<_> = files
        .iter()
        .map(|path| path.file_name().unwrap().to_str().unwrap())
        .collect();
    assert_eq!(
        names,
        ["fix.yaml", "review.yaml"],
        "the recipes its README names"
    );
    for (file, runs_on) in files.iter().zip([&["failed"][..], &["pull_request"][..]]) {
        assert_eq!(
            dagron_takes(file),
            runs_on,
            "{}: what the recipe is run for, and nothing else",
            file.display()
        );
    }
}

/// The repository's own pipelines, where this tree is inside the repository
/// that has them. A copy of the tree on its own has none, and nothing to
/// check.
#[test]
fn dagron_runs_this_repository_s_own_pipelines() {
    let dir = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../../../../.dagron/ci");
    if !dir.is_dir() {
        eprintln!("no {} here - skipping", dir.display());
        return;
    }
    let files = pipelines(&dir);
    assert!(!files.is_empty(), "{} holds no pipeline", dir.display());
    for file in &files {
        dagron_takes(file);
    }
}
