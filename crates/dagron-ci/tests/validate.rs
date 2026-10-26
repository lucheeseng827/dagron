//! `dagron-ci validate` and `dagron-ci schema` through the shipped binary:
//! they run in a checkout, with no database and no configuration.

use std::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_dagron-ci");

const GOOD: &str = "name: build\non: { push: { branches: [main] } }\ntasks:\n  - { name: test, steps: [{ run: make test }] }\n";
const BAD: &str =
    "name: build\non: { push: }\ntasks:\n  - { name: test, stpes: [{ run: make }] }\n";

fn run(dir: &std::path::Path, args: &[&str]) -> (i32, String) {
    let out = Command::new(BIN)
        .args(args)
        .current_dir(dir)
        .env_remove("CI_DATABASE_URL")
        .output()
        .unwrap();
    let text = String::from_utf8(out.stdout).unwrap() + &String::from_utf8(out.stderr).unwrap();
    (out.status.code().unwrap_or(-1), text)
}

#[test]
fn validate_checks_a_checkout_without_a_database() {
    let dir = tempfile::tempdir().unwrap();
    let pipelines = dir.path().join(".dagron/ci");
    std::fs::create_dir_all(&pipelines).unwrap();

    // Nothing to check is its own answer.
    let (code, text) = run(dir.path(), &["validate"]);
    assert_eq!(code, 2, "{text}");
    assert!(text.contains("no pipeline files"), "{text}");

    std::fs::write(pipelines.join("a-good.yaml"), GOOD).unwrap();
    std::fs::write(pipelines.join("b-bad.yml"), BAD).unwrap();
    std::fs::write(pipelines.join("notes.txt"), "not a pipeline").unwrap();
    let (code, text) = run(dir.path(), &["validate"]);
    assert_eq!(code, 1, "{text}");
    assert!(
        text.contains(".dagron/ci/a-good.yaml: ok (build: 1 task; on push)"),
        "{text}"
    );
    assert!(
        text.contains(".dagron/ci/b-bad.yml: tasks[0] (test).stpes:")
            && text.contains("did you mean \"steps\""),
        "{text}"
    );
    assert!(!text.contains("notes.txt"), "{text}");

    let (code, text) = run(dir.path(), &["validate", ".dagron/ci/a-good.yaml"]);
    assert_eq!(code, 0, "{text}");
}

#[test]
fn schema_prints_the_json_schema() {
    let dir = tempfile::tempdir().unwrap();
    let (code, text) = run(dir.path(), &["schema"]);
    assert_eq!(code, 0);
    let schema: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert_eq!(
        schema["$schema"],
        "https://json-schema.org/draft/2020-12/schema"
    );
}

#[test]
fn compile_prints_what_a_pipeline_runs_for_an_event() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("build.yaml"), GOOD).unwrap();
    let sha = "4b4708e725f29891d22b4c96af9f0a403ff51c7c";

    let (code, text) = run(
        dir.path(),
        &[
            "compile",
            "build.yaml",
            "--event",
            "push",
            "--ref",
            "refs/heads/main",
            "--sha",
            sha,
        ],
    );
    assert_eq!(code, 0, "{text}");
    assert!(text.contains(&format!("#   ci_sha: \"{sha}\"")), "{text}");
    assert!(
        text.contains("\nname: build\n") && text.contains("CI_STEP_1_RUN"),
        "{text}"
    );

    // The body POST /api/runs takes.
    let (code, text) = run(
        dir.path(),
        &[
            "compile",
            "build.yaml",
            "--event",
            "push",
            "--ref",
            "refs/heads/main",
            "--json",
        ],
    );
    assert_eq!(code, 0, "{text}");
    let body: serde_json::Value = serde_json::from_str(&text).unwrap();
    assert!(body["yaml"].as_str().unwrap().starts_with("name: build\n"));
    assert_eq!(body["parameters"]["ci_event"], "push");

    // Not for this event: 3, and why.
    let (code, text) = run(
        dir.path(),
        &[
            "compile",
            "build.yaml",
            "--event",
            "push",
            "--ref",
            "refs/heads/dev",
        ],
    );
    assert_eq!(code, 3, "{text}");
    assert!(
        text.contains("build does not run: branch \"dev\" is not in on.push.branches"),
        "{text}"
    );

    let (code, _) = run(
        dir.path(),
        &["compile", "build.yaml", "--ref", "refs/heads/main"],
    );
    assert_eq!(code, 2, "no --event");
    std::fs::write(dir.path().join("bad.yaml"), BAD).unwrap();
    let (code, text) = run(
        dir.path(),
        &[
            "compile",
            "bad.yaml",
            "--event",
            "push",
            "--ref",
            "refs/heads/main",
        ],
    );
    assert_eq!(code, 1, "{text}");
}

/// A workflow file is checked by the front-end that would run it, against
/// the repository's settings: what it uses that is not taken, a label with
/// no image, and whether it is listed to run at all. And the settings file
/// is checked as itself.
#[test]
fn validate_checks_a_workflow_file_against_the_repository_s_settings() {
    let dir = tempfile::tempdir().unwrap();
    let workflows = dir.path().join(".github/workflows");
    std::fs::create_dir_all(&workflows).unwrap();
    std::fs::create_dir_all(dir.path().join(".dagron")).unwrap();
    std::fs::write(
        dir.path().join(".dagron/ci.yaml"),
        "github:\n  workflows: [build.yml]\n  runs_on: { ubuntu-latest: \"docker.io/library/ubuntu:24.04\" }\n",
    )
    .unwrap();
    let job = |runs_on: &str, step: &str| {
        format!("jobs:\n  a:\n    runs-on: {runs_on}\n    steps:\n      - {step}\n")
    };
    std::fs::write(
        workflows.join("build.yml"),
        format!(
            "on:\n  push:\n  workflow_dispatch:\n  schedule: [{{ cron: \"0 3 * * *\" }}]\n{}  \
             b:\n    needs: a\n    strategy: {{ matrix: {{ v: [1, 2] }} }}\n    runs-on: ubuntu-latest\n    \
             steps:\n      - run: echo ${{{{ matrix.v }}}}\n",
            job("ubuntu-latest", "run: make")
        ),
    )
    .unwrap();
    std::fs::write(
        workflows.join("unlisted.yml"),
        format!("on: push\n{}", job("ubuntu-latest", "run: make")),
    )
    .unwrap();
    std::fs::write(
        workflows.join("action.yml"),
        format!(
            "on: push\n{}",
            job("ubuntu-latest", "uses: \"docker://\"")
        ),
    )
    .unwrap();
    std::fs::write(
        workflows.join("mac.yml"),
        format!("on: push\n{}", job("macos-latest", "run: make")),
    )
    .unwrap();
    // A workflow that runs only on a schedule is read for a fire of it.
    std::fs::write(
        workflows.join("nightly.yml"),
        format!(
            "on:\n  schedule: [{{ cron: \"0 3 * * *\" }}]\n{}",
            job("windows-latest", "run: make")
        ),
    )
    .unwrap();

    let (code, text) = run(
        dir.path(),
        &["validate", ".github/workflows", ".dagron/ci.yaml"],
    );
    assert_eq!(code, 1, "{text}");
    for said in [
        ".github/workflows/build.yml: ok (workflow build: 3 jobs; on push, workflow_dispatch, schedule)",
        ".github/workflows/unlisted.yml: ok (workflow unlisted: 1 job; on push; not listed in .dagron/ci.yaml, so not run)",
        ".github/workflows/action.yml: jobs.a.steps[0].uses: names no image: docker:// and an image",
        ".github/workflows/mac.yml: jobs.a.runs-on: macos-latest is no label .dagron/ci.yaml gives an image for",
        ".github/workflows/nightly.yml: jobs.a.runs-on: windows-latest is no label .dagron/ci.yaml gives an image for",
        ".dagron/ci.yaml: ok (settings: 1 workflow file listed, 1 label given an image)",
    ] {
        assert!(text.contains(said), "{said}\nnot in\n{text}");
    }

    let (code, text) = run(dir.path(), &["validate", ".github/workflows/build.yml"]);
    assert_eq!(code, 0, "{text}");
    std::fs::write(
        dir.path().join(".dagron/ci.yaml"),
        "github: { workflws: [] }",
    )
    .unwrap();
    let (code, text) = run(dir.path(), &["validate", ".dagron/ci.yaml"]);
    assert_eq!(code, 1, "{text}");
    assert!(
        text.contains(".dagron/ci.yaml: github.workflws: is not a key this takes"),
        "{text}"
    );
}

/// A `.gitlab-ci.yml` is checked as a GitLab pipeline: the files it
/// includes read from the checkout, and made for a push to `main`, where a
/// job's tag maps to no image is said.
#[test]
fn validate_checks_a_gitlab_pipeline_with_what_it_includes() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("ci")).unwrap();
    std::fs::create_dir_all(dir.path().join(".dagron")).unwrap();
    std::fs::write(
        dir.path().join(".dagron/ci.yaml"),
        "gitlab:\n  pipeline: .gitlab-ci.yml\n  tags: { docker: \"alpine:3.20\" }\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join("ci/common.yml"),
        ".test:\n  tags: [docker]\n  script: [make test]\n",
    )
    .unwrap();
    std::fs::write(
        dir.path().join(".gitlab-ci.yml"),
        "include: [{local: ci/common.yml}]\nunit:\n  extends: .test\n\
         lint:\n  extends: .test\n  rules: [{if: $CI_COMMIT_TAG}]\n",
    )
    .unwrap();
    let (code, text) = run(dir.path(), &["validate", ".gitlab-ci.yml"]);
    assert_eq!(code, 0, "{text}");
    assert!(
        text.contains("ok (GitLab pipeline: 2 jobs, 1 file included; a push to main runs 1 task)"),
        "{text}"
    );

    std::fs::write(
        dir.path().join(".gitlab-ci.yml"),
        "include: [{local: ci/missing.yml}]\nunit:\n  tags: [shell]\n  script: [x]\n",
    )
    .unwrap();
    let (code, text) = run(dir.path(), &["validate", ".gitlab-ci.yml"]);
    assert_eq!(code, 1, "{text}");
    assert!(text.contains("ci/missing.yml"), "{text}");

    // A link in the checkout that leads out of it is not read.
    #[cfg(unix)]
    {
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(
            outside.path().join("secret.yml"),
            "secret:\n  tags: [docker]\n  script: [x]\n",
        )
        .unwrap();
        std::os::unix::fs::symlink(
            outside.path().join("secret.yml"),
            dir.path().join("ci/escape.yml"),
        )
        .unwrap();
        std::fs::write(
            dir.path().join(".gitlab-ci.yml"),
            "include: [{local: ci/escape.yml}]\nunit:\n  tags: [docker]\n  script: [x]\n",
        )
        .unwrap();
        let (code, text) = run(dir.path(), &["validate", ".gitlab-ci.yml"]);
        assert_eq!(code, 1, "{text}");
        assert!(
            text.contains("ci/escape.yml cannot be read: it leads out of the checkout"),
            "{text}"
        );
    }
}
