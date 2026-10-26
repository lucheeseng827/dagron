//! dagron-import — convert another orchestrator's workflow to dagron DAG YAML.
//!
//!   dagron-import argo <workflow.yaml>   # prints dagron YAML to stdout
//!   dagron-import github <workflow.yml> [--name NAME] [--image LABEL=IMAGE]...
//!                                        # prints a dagron-ci pipeline
//!   dagron-import gitlab <.gitlab-ci.yml> [--name NAME] [--image TAG=IMAGE]...
//!                        [--default-branch NAME] [--root DIR]
//!   dagron-import jenkins <Jenkinsfile> [--name NAME] [--image LABEL=IMAGE]...
//!
//! `argo`: pipe it into a file or `POST /api/runs` to migrate.
//!
//! `github`, `gitlab` and `jenkins`: a CI's file as a pipeline of
//! `.dagron/ci`, under a comment that says what was changed on the way,
//! what was not carried over, and what is left to do by hand. A job with
//! something left to do starts with a step that fails and says what.
//!
//! `gitlab` reads the files `include: local` names from the repository the
//! pipeline is in: `--root`, or the directory the file is in.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use dagron_import::{Imported, Kind};

const USAGE: &str = "usage: dagron-import argo <workflow.yaml>\n       \
                     dagron-import github <workflow.yml> [--name NAME] [--image LABEL=IMAGE]...\n       \
                     dagron-import gitlab <.gitlab-ci.yml> [--name NAME] [--image TAG=IMAGE]... [--default-branch NAME] [--root DIR]\n       \
                     dagron-import jenkins <Jenkinsfile> [--name NAME] [--image LABEL=IMAGE]...";

fn main() -> ExitCode {
    // `--version`: the release this binary was built for, on one line, and
    // nothing else is done. Every dagron binary answers so. This one links none
    // of their shared crates, so the answer is its own; `dev` is a build no
    // release made.
    const VERSION: &str = match option_env!("APP_VERSION") {
        Some(version) if !version.is_empty() => version,
        _ => "dev",
    };
    let mut asked = std::env::args_os().skip(1);
    if matches!(asked.next(), Some(arg) if arg == "--version" || arg == "-V")
        && asked.next().is_none()
    {
        println!("dagron-import {VERSION}");
        return ExitCode::SUCCESS;
    }
    // Every argument is text: a path that is none (not UTF-8) is said so,
    // where reading it as text would stop the program.
    let args: Vec<String> = match std::env::args_os()
        .map(|arg| arg.into_string())
        .collect::<Result<_, _>>()
    {
        Ok(args) => args,
        Err(arg) => {
            eprintln!("error: {arg:?} is not UTF-8: give a path that is\n{USAGE}");
            return ExitCode::from(2);
        }
    };
    let pipelines = ["github", "gitlab", "jenkins"];
    if args.len() < 3 || (!pipelines.contains(&args[1].as_str()) && args.len() != 3) {
        eprintln!("{USAGE}");
        return ExitCode::from(2);
    }
    let kind = args[1].as_str();
    let path = &args[2];
    if pipelines.contains(&kind) {
        return pipeline(kind, path, &args[3..]);
    }

    let input = match std::fs::read_to_string(path) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("error: cannot read '{path}': {e}");
            return ExitCode::FAILURE;
        }
    };

    let result = match kind {
        "argo" => dagron_import::argo_to_dagron(&input),
        other => {
            eprintln!(
                "error: unknown importer '{other}' (supported: argo, github, gitlab, jenkins)"
            );
            return ExitCode::from(2);
        }
    };

    match result {
        Ok(yaml) => {
            print!("{yaml}");
            ExitCode::SUCCESS
        }
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

/// What a pipeline importer is told beside its file.
#[derive(Default)]
struct Flags {
    name: Option<String>,
    images: BTreeMap<String, String>,
    default_branch: Option<String>,
    root: Option<String>,
}

/// `dagron-import github|gitlab|jenkins`: the pipeline on standard output,
/// under its report; and how much of each kind the report holds on standard
/// error.
fn pipeline(kind: &str, path: &str, rest: &[String]) -> ExitCode {
    // The file's own name is the pipeline's, unless another is given
    // ([`dagron_import::pipeline_name`]).
    let mut flags = Flags {
        name: dagron_import::pipeline_name(kind, Path::new(path)),
        ..Flags::default()
    };
    let mut rest = rest.iter();
    while let Some(flag) = rest.next() {
        match (flag.as_str(), rest.next()) {
            ("--name", Some(name)) => flags.name = Some(name.clone()),
            ("--image", Some(pair)) => match pair.split_once('=') {
                Some((label, image)) if !label.is_empty() && !image.is_empty() => {
                    flags.images.insert(label.to_string(), image.to_string());
                }
                _ => {
                    eprintln!("error: --image takes LABEL=IMAGE, not {pair:?}");
                    return ExitCode::from(2);
                }
            },
            ("--default-branch", Some(branch)) if kind == "gitlab" => {
                flags.default_branch = Some(branch.clone())
            }
            ("--root", Some(root)) if kind == "gitlab" => flags.root = Some(root.clone()),
            _ => {
                eprintln!("{USAGE}");
                return ExitCode::from(2);
            }
        }
    }
    let input = match std::fs::read_to_string(path) {
        Ok(text) => text,
        Err(e) => {
            eprintln!("error: cannot read '{path}': {e}");
            return ExitCode::FAILURE;
        }
    };
    let imported = match kind {
        "github" => {
            let options = dagron_import::github::Options {
                name: flags.name,
                images: flags.images,
            };
            dagron_import::github_to_pipeline(&input, &options)
        }
        "gitlab" => {
            let root = root_of(path, flags.root.as_deref());
            let options = dagron_import::gitlab::Options {
                name: flags.name,
                images: flags.images,
                default_branch: flags.default_branch,
                included: dagron_import::gitlab::read_included(&root, &input),
            };
            dagron_import::gitlab_to_pipeline(&input, &options)
        }
        _ => {
            let options = dagron_import::jenkins::Options {
                name: flags.name,
                images: flags.images,
            };
            dagron_import::jenkins_to_pipeline(&input, &options)
        }
    };
    match imported {
        Ok(imported) => report(path, &imported),
        Err(e) => {
            eprintln!("error: {e:#}");
            ExitCode::FAILURE
        }
    }
}

/// The repository a GitLab pipeline is in: `--root`, else the directory its
/// file is in. A file named bare (`.gitlab-ci.yml`) is in the current
/// directory, whose path is no name at all.
fn root_of(path: &str, root: Option<&str>) -> PathBuf {
    match root {
        Some(root) => PathBuf::from(root),
        None => Path::new(path)
            .parent()
            .filter(|dir| !dir.as_os_str().is_empty())
            .unwrap_or(Path::new("."))
            .to_path_buf(),
    }
}

fn report(path: &str, imported: &Imported) -> ExitCode {
    print!("{}", imported.with_report());
    let count = |kind| imported.notes.iter().filter(|n| n.kind == kind).count();
    eprintln!(
        "{path}: {} to do by hand, {} changed on the way, {} not carried over",
        count(Kind::Todo),
        count(Kind::Changed),
        count(Kind::Left)
    );
    ExitCode::SUCCESS
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_file_named_bare_is_in_the_current_directory() {
        assert_eq!(root_of(".gitlab-ci.yml", None), PathBuf::from("."));
        assert_eq!(root_of("./.gitlab-ci.yml", None), PathBuf::from("."));
        assert_eq!(root_of("repo/.gitlab-ci.yml", None), PathBuf::from("repo"));
        assert_eq!(
            root_of(".gitlab-ci.yml", Some("elsewhere")),
            PathBuf::from("elsewhere")
        );
    }
}
