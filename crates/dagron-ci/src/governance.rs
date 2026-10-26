//! Rules a build holds the repositories it serves to, for a build that has
//! any.
//!
//! The open build has none: a repository runs what it writes, and nothing
//! else. A build that composes this crate can pass a [`Governance`] in its
//! [`crate::Seams`], and a worker then asks it three things of each event:
//! - **which pipelines the build requires** of the event's repository,
//!   beside the repository's own ([`Governance::required`]). Each is read
//!   and run as one of the repository's, at the event's commit, under its
//!   own `on:`. It is not the repository's to change: a file of the
//!   repository's of the same name is refused, and says why;
//! - **whether each run may be submitted**, once it is compiled
//!   ([`Governance::review`]). A refusal is the run's, as dagron-api's would
//!   be: the pipeline is not run, the event fails, and the commit says why
//!   once the event has ended;
//! - **who the event's actor is** to dagron ([`Governance::actor`]): a forge
//!   login read as a person of the build's directory. The run's record names
//!   that person, so a gate that its triggerer may not approve compares
//!   people, and not a login with an email.
//!
//! Each is asked as the event is handled, inside no transaction: it must
//! answer at once, from memory.

use crate::forge::PipelineFile;

/// Where a required pipeline's file is said to be. No repository's file is
/// read from there (theirs are in `.dagron/ci`), so the two never share a
/// path, an idempotency key or a run.
pub const REQUIRED_DIR: &str = ".dagron/required";

/// A run about to be submitted, for a [`Governance`] to look at.
#[derive(Clone, Copy, Debug)]
pub struct Proposed<'a> {
    /// `forge host/owner/name`.
    pub repo: &'a str,
    /// The pipeline's file: `.dagron/ci/build.yaml`, or one under
    /// [`REQUIRED_DIR`] for a pipeline the build requires.
    pub pipeline: &'a str,
    /// The kind of event: `push`, `pull_request`, `dispatch`, …
    pub kind: &'a str,
    pub git_ref: &'a str,
    pub sha: &'a str,
    /// `trusted`, `untrusted` or `agent`.
    pub trust: &'a str,
    /// The forge login, or the caller, that caused the event.
    pub actor: Option<&'a str>,
    pub pr: Option<i64>,
    /// The workflow it compiled to, as it is submitted.
    pub yaml: &'a str,
}

impl Proposed<'_> {
    /// Whether the build requires this pipeline, rather than the repository
    /// writing it.
    pub fn is_required(&self) -> bool {
        self.pipeline.starts_with(REQUIRED_DIR)
    }
}

/// What a build holds the repositories it serves to.
pub trait Governance: Send + Sync + 'static {
    /// The pipelines the build requires of the repository `forge_host` /
    /// `full_name`, each with a path under [`REQUIRED_DIR`]. None by
    /// default.
    fn required(&self, forge_host: &str, full_name: &str) -> Vec<PipelineFile> {
        let _ = (forge_host, full_name);
        Vec::new()
    }

    /// Whether `run` may be submitted. `Err` refuses it; its text is the
    /// reason, on the event and on the commit, so it says which rule and
    /// what would pass it.
    fn review(&self, run: &Proposed<'_>) -> Result<(), String> {
        let _ = run;
        Ok(())
    }

    /// Who `login` on `forge_host` is to dagron, where the build knows: the
    /// identity a person signs in to dagron with. `None` keeps the login.
    fn actor(&self, forge_host: &str, login: &str) -> Option<String> {
        let _ = (forge_host, login);
        None
    }
}

/// `files` with the pipelines `required` names added. A file of the
/// repository's that a required pipeline shares its name with is read as
/// refused, so that a check the build requires is never one the repository
/// wrote.
pub fn with_required(files: &mut Vec<PipelineFile>, required: Vec<PipelineFile>) {
    if required.is_empty() {
        return;
    }
    let names: std::collections::BTreeSet<String> = required
        .iter()
        .map(|f| crate::report::pipeline_name(&f.path).to_string())
        .collect();
    for file in files.iter_mut() {
        let name = crate::report::pipeline_name(&file.path);
        if names.contains(name) {
            file.text = Err(format!(
                "this installation requires a pipeline called {name} of every repository it \
                 names, and it runs in this one's place: rename this one"
            ));
        }
    }
    files.extend(required);
    files.sort_by(|a, b| a.path.cmp(&b.path));
}

#[cfg(test)]
mod tests {
    use super::*;

    fn file(path: &str, text: &str) -> PipelineFile {
        PipelineFile {
            path: path.into(),
            text: Ok(text.into()),
        }
    }

    #[test]
    fn a_required_pipeline_is_added_and_takes_its_name_from_the_repository() {
        let mut files = vec![
            file(".dagron/ci/build.yaml", "own"),
            file(".dagron/ci/scan.yaml", "own"),
        ];
        with_required(&mut files, vec![file(".dagron/required/scan.yaml", "org")]);
        let paths: Vec<&str> = files.iter().map(|f| f.path.as_str()).collect();
        assert_eq!(
            paths,
            [
                ".dagron/ci/build.yaml",
                ".dagron/ci/scan.yaml",
                ".dagron/required/scan.yaml"
            ]
        );
        assert_eq!(files[0].text.as_deref(), Ok("own"));
        assert!(files[1]
            .text
            .as_ref()
            .is_err_and(|why| why.contains("requires a pipeline called scan")));
        assert_eq!(files[2].text.as_deref(), Ok("org"));
    }

    #[test]
    fn nothing_required_changes_nothing() {
        let mut files = vec![file(".dagron/ci/build.yaml", "own")];
        with_required(&mut files, Vec::new());
        assert_eq!(files, vec![file(".dagron/ci/build.yaml", "own")]);
    }

    #[test]
    fn a_required_pipeline_is_known_by_its_path() {
        let run = |pipeline| Proposed {
            repo: "github.com/acme/widgets",
            pipeline,
            kind: "push",
            git_ref: "refs/heads/main",
            sha: "abc",
            trust: "trusted",
            actor: None,
            pr: None,
            yaml: "",
        };
        assert!(run(".dagron/required/scan.yaml").is_required());
        assert!(!run(".dagron/ci/scan.yaml").is_required());
    }
}
