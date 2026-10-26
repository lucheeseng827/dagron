//! **Signed provenance of each workflow artifact**: as an
//! upload is finalized, the engine says what made it, in an SLSA v1
//! provenance statement of the zip's SHA-256, and signs it
//! ([`dagron_crypto::provenance`]). The envelope is kept with the artifact,
//! made its run's in the same statement, and served beside it by dagron-api
//! (`GET /api/runs/{run_id}/workflow-artifacts/{name}/provenance`), for
//! `cosign verify-blob-attestation` to check against the zip.
//!
//! What it says is the run's record: the repository, ref and commit it was
//! started for, its trust class, the workflow, the job and its attempt. Not
//! the job's own word: a job says nothing here but its upload.
//!
//! Where a key is given, every artifact is signed or is not one: an upload
//! whose provenance could not be signed is refused, and the job is told so.
//! Where none is given, artifacts are kept without provenance, as before.

use std::sync::Arc;

use anyhow::{Context, Result};
use dagron_core::{db, envpolicy};
use dagron_crypto::provenance::{envelope, statement, Signer, Subject};
use serde_json::{json, Value};

/// What a provenance's `builder.id` is, unless the operator says.
pub const ENV_BUILDER_ID: &str = "DAGRON_PROVENANCE_BUILDER_ID";
pub const BUILDER_ID: &str = "https://dagron.dev/builders/dagron-engine";
/// How the predicate's `buildDefinition` is to be read.
pub const BUILD_TYPE: &str = "https://dagron.dev/buildtypes/workflow-job/v1";

/// The key a workflow artifact's provenance is signed with, and who the
/// builder says it is.
pub struct Provenance {
    signer: Arc<dyn Signer>,
    builder_id: String,
}

impl Provenance {
    pub fn new(signer: Box<dyn Signer>, builder_id: Option<String>) -> Provenance {
        Provenance {
            signer: Arc::from(signer),
            builder_id: builder_id
                .map(|b| b.trim().to_string())
                .filter(|b| !b.is_empty())
                .unwrap_or_else(|| BUILDER_ID.to_string()),
        }
    }

    /// The signer the environment names, if any (`DAGRON_PROVENANCE_KEY`, or
    /// a KMS command in the enterprise build), with its builder.
    pub fn from_env() -> Result<Option<Provenance>> {
        Ok(dagron_crypto::provenance::signer_from_env()?
            .map(|signer| Provenance::new(signer, std::env::var(ENV_BUILDER_ID).ok())))
    }

    pub fn public_key_pem(&self) -> &str {
        self.signer.public_key_pem()
    }

    /// The signed provenance of the artifact `name` of `run`, uploaded by
    /// `task`, whose zip's SHA-256 is `digest` (`sha256:<hex>`): a DSSE
    /// envelope, as JSON text.
    pub(super) async fn sign(
        &self,
        pool: &db::Pool,
        run: &str,
        task: &str,
        name: &str,
        digest: &str,
    ) -> Result<String> {
        let trigger = db::run_trigger(pool, run)
            .await
            .context("loading what started the run, for its provenance")?;
        let (job, attempt, workflow) = db::task_in_workflow(pool, task)
            .await
            .context("loading the task, for its provenance")?
            .with_context(|| format!("task {task} is not in the database"))?;
        let facts = Facts {
            builder_id: &self.builder_id,
            run,
            task,
            job: &job,
            // The attempt that is running, as its task's kept attempts are
            // numbered: claimed, it is 1 on a first run, and on from the
            // rounds before it after a rerun. A task no worker has claimed
            // reads 0, and is its first.
            attempt: attempt.max(1),
            workflow: workflow.as_deref(),
            trigger: trigger.as_ref(),
        };
        let sha256 = digest.strip_prefix("sha256:").unwrap_or(digest);
        let statement = statement(
            &[Subject {
                name: format!("{name}.zip"),
                sha256: sha256.to_string(),
            }],
            predicate(&facts),
        );
        // A KMS command may take its time: not on the service's threads.
        let signer = Arc::clone(&self.signer);
        let signed = tokio::task::spawn_blocking(move || envelope(signer.as_ref(), &statement))
            .await
            .context("the signing task")??;
        Ok(serde_json::to_string(&signed)?)
    }
}

/// What the provenance of one artifact says, read from the run's record.
struct Facts<'a> {
    builder_id: &'a str,
    run: &'a str,
    task: &'a str,
    job: &'a str,
    /// The attempt that uploaded it, from 1, by its task's count for its
    /// life: what `…/attempts` numbers it by.
    attempt: i64,
    workflow: Option<&'a str>,
    trigger: Option<&'a dagron_core::models::RunTrigger>,
}

/// The SLSA v1 predicate: the build is the job of the workflow, its external
/// parameters what the run was started for, and its one resolved dependency
/// the commit it was started at, where the run names one.
fn predicate(f: &Facts) -> Value {
    let trigger = f.trigger;
    let repository = trigger.and_then(|t| {
        let repo = t.repo.as_deref().filter(|r| !r.is_empty())?;
        let host = t.forge_host.as_deref().filter(|h| !h.is_empty())?;
        Some(format!("https://{host}/{repo}"))
    });
    let git_ref = trigger
        .and_then(|t| t.git_ref.clone())
        .filter(|r| !r.is_empty());
    let sha = trigger
        .and_then(|t| t.sha.clone())
        .filter(|s| !s.is_empty());

    let mut external = json!({ "job": f.job });
    if let Some(workflow) = f.workflow {
        external["workflow"] = json!(workflow);
    }
    if let Some(repository) = &repository {
        external["repository"] = json!(repository);
    }
    if let Some(git_ref) = &git_ref {
        external["ref"] = json!(git_ref);
    }
    if let Some(t) = trigger {
        external["trigger"] = json!(t.kind);
        if let Some(pr) = t.pr {
            external["pull_request"] = json!(pr);
        }
    }

    let mut dependencies = Vec::new();
    if let (Some(repository), Some(sha)) = (&repository, &sha) {
        let mut uri = format!("git+{repository}");
        if let Some(git_ref) = &git_ref {
            uri.push('@');
            uri.push_str(git_ref);
        }
        dependencies.push(json!({ "uri": uri, "digest": { "gitCommit": sha } }));
    }

    json!({
        "buildDefinition": {
            "buildType": BUILD_TYPE,
            "externalParameters": external,
            "internalParameters": {
                "trust": envpolicy::class_of(trigger),
                "attempt": f.attempt,
            },
            "resolvedDependencies": dependencies,
        },
        "runDetails": {
            "builder": {
                "id": f.builder_id,
                "version": { "dagron-engine": dagron_logging::VERSION },
            },
            "metadata": {
                "invocationId": format!("{}/{}/{}", f.run, f.task, f.attempt),
            },
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use dagron_core::models::RunTrigger;

    fn trigger() -> RunTrigger {
        RunTrigger {
            kind: "push".into(),
            forge_host: Some("github.com".into()),
            repo: Some("acme/widgets".into()),
            git_ref: Some("refs/heads/main".into()),
            sha: Some("0123456789abcdef0123456789abcdef01234567".into()),
            ..RunTrigger::default()
        }
    }

    #[test]
    fn the_predicate_is_the_run_s_record_and_the_commit_it_was_started_at() {
        let t = trigger();
        let p = predicate(&Facts {
            builder_id: BUILDER_ID,
            run: "r1",
            task: "t1",
            job: "build",
            attempt: 2,
            workflow: Some("ci"),
            trigger: Some(&t),
        });
        let def = &p["buildDefinition"];
        assert_eq!(def["buildType"], BUILD_TYPE);
        assert_eq!(
            def["externalParameters"],
            json!({
                "job": "build", "workflow": "ci", "trigger": "push",
                "repository": "https://github.com/acme/widgets", "ref": "refs/heads/main",
            })
        );
        assert_eq!(
            def["resolvedDependencies"],
            json!([{
                "uri": "git+https://github.com/acme/widgets@refs/heads/main",
                "digest": { "gitCommit": "0123456789abcdef0123456789abcdef01234567" },
            }])
        );
        assert_eq!(def["internalParameters"]["attempt"], 2);
        assert_eq!(p["runDetails"]["builder"]["id"], BUILDER_ID);
        assert_eq!(p["runDetails"]["metadata"]["invocationId"], "r1/t1/2");
    }

    #[test]
    fn a_run_started_for_no_commit_resolves_nothing() {
        let p = predicate(&Facts {
            builder_id: "https://ci.example/builder",
            run: "r1",
            task: "t1",
            job: "build",
            attempt: 1,
            workflow: None,
            trigger: None,
        });
        assert_eq!(p["buildDefinition"]["resolvedDependencies"], json!([]));
        assert_eq!(
            p["buildDefinition"]["externalParameters"],
            json!({ "job": "build" })
        );
        assert_eq!(
            p["runDetails"]["builder"]["id"],
            "https://ci.example/builder"
        );
    }
}
