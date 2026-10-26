//! `dagron-ci import` through the shipped binary: what it prints for each
//! case of the importers' corpus (`tests/import.rs`) is that case's golden
//! file, so the command and `dagron-import` never write a pipeline two
//! ways; and in a checkout, what `--write` writes is what `validate` reads.

use std::path::{Path, PathBuf};
use std::process::Command;

const BIN: &str = env!("CARGO_BIN_EXE_dagron-ci");

/// `dagron-ci ARGS` in `dir`: its exit code, what it printed, and what it
/// said.
fn run(dir: &Path, args: &[&str]) -> (i32, String, String) {
    let out = Command::new(BIN)
        .args(args)
        .current_dir(dir)
        .env_remove("CI_DATABASE_URL")
        .output()
        .unwrap();
    (
        out.status.code().unwrap_or(-1),
        String::from_utf8(out.stdout).unwrap(),
        String::from_utf8(out.stderr).unwrap(),
    )
}

fn read(path: &Path) -> String {
    std::fs::read_to_string(path)
        .unwrap_or_else(|e| panic!("{}: {e}", path.display()))
        .replace("\r\n", "\n")
}

/// Copy the tree at `from` into `to`.
fn copy(from: &Path, to: &Path) {
    for entry in std::fs::read_dir(from).unwrap() {
        let path = entry.unwrap().path();
        let into = to.join(path.file_name().unwrap());
        if path.is_dir() {
            std::fs::create_dir_all(&into).unwrap();
            copy(&path, &into);
        } else {
            std::fs::write(&into, read(&path)).unwrap();
        }
    }
}

/// Each case of `corpus` imported by `dagron-ci import --from from`, with
/// the name and the image its golden file was made with: its golden file,
/// on standard output, and an ok for it.
fn each_case(corpus: &str, ending: &str, from: &str, image: &str) {
    let corpus = Path::new(env!("CARGO_MANIFEST_DIR")).join(corpus);
    let mut cases: Vec<PathBuf> = std::fs::read_dir(corpus.join("cases"))
        .unwrap()
        .map(|entry| entry.unwrap().path())
        .filter(|p| p.extension().and_then(|e| e.to_str()) == Some(ending))
        .collect();
    cases.sort();
    assert!(!cases.is_empty());
    for case in cases {
        let name = case.file_stem().unwrap().to_str().unwrap().to_string();
        // The checkout: where the files a GitLab case includes are.
        let checkout = tempfile::tempdir().unwrap();
        let files = corpus.join("cases").join(format!("{name}.files"));
        if files.is_dir() {
            copy(&files, checkout.path());
        }
        let file = checkout.path().join(format!("pipeline.{ending}"));
        std::fs::write(&file, read(&case)).unwrap();
        let file = file.to_str().unwrap();
        let (code, out, err) = run(
            checkout.path(),
            &[
                "import", "--from", from, file, "--name", &name, "--image", image,
            ],
        );
        assert_eq!(code, 0, "{name}: {err}");
        assert_eq!(
            out,
            read(&corpus.join("golden").join(format!("{name}.yaml"))),
            "{name}: not what its golden file says"
        );
        assert!(err.starts_with(&format!("{file}: ok (")), "{name}: {err}");
    }
}

#[test]
fn each_workflow_is_imported_as_dagron_import_imports_it() {
    each_case(
        "tests/import",
        "yml",
        "github",
        "self-hosted=docker.io/library/debian:12",
    );
}

#[test]
fn each_gitlab_pipeline_is_imported_as_dagron_import_imports_it() {
    each_case(
        "tests/import/gitlab",
        "yml",
        "gitlab",
        "docker=docker.io/library/debian:12",
    );
}

#[test]
fn each_jenkinsfile_is_imported_as_dagron_import_imports_it() {
    each_case(
        "tests/import/jenkins",
        "jenkinsfile",
        "jenkins",
        "linux=docker.io/library/debian:12",
    );
}

/// A checkout's CI files written as its pipelines, which `validate` then
/// reads; nothing written over; a file no importer takes is the command's
/// failure, and what it is not asked is refused before anything is read.
#[test]
fn import_writes_a_checkout_s_pipelines_for_validate_to_read() {
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir_all(root.join(".github/workflows")).unwrap();
    std::fs::write(
        root.join(".github/workflows/build.yml"),
        "on: push\njobs:\n  test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make test\n",
    )
    .unwrap();
    std::fs::write(
        root.join(".gitlab-ci.yml"),
        "test:\n  image: rust:1\n  script: [cargo test]\n",
    )
    .unwrap();
    std::fs::write(
        root.join("Jenkinsfile"),
        "pipeline {\n  agent any\n  stages {\n    stage('Build') {\n      steps { sh 'make' }\n    }\n  }\n}\n",
    )
    .unwrap();

    let (code, out, err) = run(root, &["import", "--write"]);
    assert_eq!((code, err.as_str()), (0, ""), "{out}");
    for line in [
        ".github/workflows/build.yml: ok, as .dagron/ci/build.yaml (",
        ".gitlab-ci.yml: ok, as .dagron/ci/gitlab-ci.yaml (",
        "Jenkinsfile: ok, as .dagron/ci/ci.yaml (",
    ] {
        assert!(out.contains(line), "{line}: {out}");
    }
    let (code, out, err) = run(root, &["validate"]);
    assert_eq!(code, 0, "{out}{err}");
    for file in ["build.yaml", "ci.yaml", "gitlab-ci.yaml"] {
        assert!(
            out.contains(&format!(".dagron/ci/{file}: ok (")),
            "{file}: {out}"
        );
    }

    let (code, out, _) = run(root, &["import", "--write"]);
    assert_eq!(code, 1);
    assert_eq!(
        out.matches("is there already: --force writes over it")
            .count(),
        3,
        "{out}"
    );

    std::fs::write(root.join("Jenkinsfile"), "node {\n  sh 'make'\n}\n").unwrap();
    let (code, out, err) = run(root, &["import", "Jenkinsfile"]);
    assert_eq!((code, out.as_str()), (1, ""));
    assert!(err.starts_with("Jenkinsfile: not imported: "), "{err}");

    for args in [
        &["import", "--token", "t"][..],
        &["import", "--force"],
        &["import", "--from", "circleci"],
        &["import", "notes.txt"],
    ] {
        let (code, out, err) = run(root, args);
        assert_eq!((code, out.as_str()), (2, ""), "{args:?}: {err}");
        assert!(err.contains("usage:"), "{err}");
    }
}
