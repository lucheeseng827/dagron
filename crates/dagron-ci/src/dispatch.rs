//! What a worker does with an event: run the repository's pipelines for it,
//! on the repository's cell, through dagron-api, each once.
//!
//! **Which pipelines.** Those at the event's commit: the `*.yaml` files in
//! `.dagron/ci`, read through the forge's API (GitHub's, so far). Each is
//! parsed, and its `on:` decides whether this event runs it. A paths filter
//! is decided with the files the event changed, asked of the forge once, and
//! only when a pipeline needs them. Each pipeline that runs is compiled
//! ([`crate::pipeline::compile`]) and submitted as its own run
//! (`POST /api/runs`). The engine sees ordinary runs.
//!
//! **Without pipelines** (none at the commit, or a forge whose files
//! dagron-ci cannot read yet), the workflow bound to the repository runs
//! (`dagron-ci repo add … --workflow ID`), with the event as parameters.
//!
//! **Once.** Each run's idempotency key derives from the event's commit key
//! and the pipeline (its path, or the bound workflow), so a webhook and a poll
//! that see one commit share it. dagron-ci records each run it submits under
//! it (`ci_runs`) and submits nothing for a key it has. dagron-api gets the
//! key as `Idempotency-Key`, so a worker that died after submitting, and
//! before recording, gets the same run back.
//!
//! **How an event ends.**
//! - Done, once every pipeline that runs is submitted, now or before.
//! - Skipped, when none runs: each one's reason is kept. One that took the
//!   event and was skipped for its `paths:` says so on the commit, with a
//!   passing status for each of its jobs: a check a branch requires would
//!   otherwise wait for ever on a pipeline that had nothing to run for.
//! - Skipped, and said on the commit, when a pull request is refused for
//!   where it is from (a fork, a non-member). Its head is given one status,
//!   `error`, under [`REFUSED_CONTEXT`], which says it was not run and why:
//!   nobody waits on a check that is not coming, and nothing reads as passed.
//!   A closed pull request is skipped for being closed, whoever opened it.
//! - Where such pull requests are run ([`crate::forks`]): waiting, while
//!   the approval one needs is not given, with no pipeline of it submitted; and
//!   then as any other event, each run of it recorded as `untrusted`, which
//!   is what dagron holds it by. Never submitted without that record.
//! - Failed, and said on the commit, when a pipeline of it is invalid or
//!   refused: one `error` status under the pipeline's own name, which says it
//!   was not run and why. Posted once the event ends for good, and not at
//!   each turn of one that is retried or waits.
//! - Failed, when a pipeline cannot run as written, or dagron-api refuses its
//!   run, after every other pipeline is submitted.
//! - Retried, when the forge or dagron-api cannot be reached.
//! - It waits, without spending an attempt, while a run's concurrency key
//!   has a run active (dagron-api's 429), the forge's rate limit is spent, or
//!   the build's limit on runs in flight is reached ([`crate::admission`]).
//!
//! **Marked as CI's.** Each run is submitted with a trigger record: that it
//! is CI's, and the repository, ref, commit, pull request, actor and event it
//! is for ([`trigger`]). dagron-api keeps it with the run, reads the run's
//! trigger as `ci`, and lets a list be narrowed to such runs. It takes one
//! only from a caller in its `ci:trigger` group, and refuses any other
//! caller's submit that carries one. So dagron-ci asks the cell who its token
//! is first (`GET /api/me`), and sends none where the token is not in the
//! group: the runs are made all the same, and read as manual, as they did
//! before there was a record.

use std::collections::BTreeMap;
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use sha2::{Digest, Sha256};

use crate::admission::{self, Admission};
use crate::audit::{self as audit_trail, Audit};
use crate::db::{Claimed, Db, EventPayload, Finish, Mode, Repo, SubmittedRun};
use crate::forge::{CommitStatus, Forge, ForgeError, PipelineFile, StatusError, PIPELINE_DIR};
use crate::governance::{self, Governance, Proposed};
use crate::meter::{MeteredRun, RunMeter};
use crate::metrics::{Metrics, StatusOutcome};
use crate::pipeline;
use crate::pipeline::compile::{self, Changes, Compiled, Outcome};
use crate::pipeline::github;
use crate::pipeline::gitlab;

/// What a worker hands each event it claims to. The open build's is
/// [`Dispatcher`]; a test gives its own.
pub trait Handler: Send + Sync + 'static {
    /// How to leave the event. A `Retry`'s `after` is the worker's to set (its
    /// backoff); a `Wait`'s is the handler's, within the worker's bounds.
    fn handle(&self, event: &Claimed) -> impl Future<Output = Finish> + Send;
}

/// How long one request to dagron-api may take. Well inside the event lease,
/// so a hung dagron-api cannot outlive the claim.
const REQUEST_TIMEOUT: Duration = Duration::from_secs(15);

/// The most of a dagron-api error body kept in an event's reason.
const REASON_EXCERPT: usize = 300;

/// How long an event waits before asking again whether its concurrency key
/// has room.
const CONCURRENCY_WAIT: Duration = Duration::from_secs(30);

/// The longest a forge's rate limit holds an event.
const MAX_RATE_LIMIT_WAIT: Duration = Duration::from_secs(3600);

/// How long a skipped status holds the lease on its context
/// ([`Db::lease_context`]): longer than any post of one, which asks the
/// forge at most five things in turn, each within
/// [`crate::forge::REQUEST_TIMEOUT`]: whether the repository takes check
/// runs (the App's installation, and a token for its checks), a token for
/// its calls (the installation again, and the token), and the post. One
/// timeout more is the margin.
const SKIPPED_LEASE: Duration = Duration::from_secs(6 * crate::forge::REQUEST_TIMEOUT.as_secs());

/// The dagron-api group whose members may say what started a run.
pub(crate) const TRIGGER_GROUP: &str = "ci:trigger";

/// How long dagron-ci goes on what a cell last said of its token's groups. An
/// operator who puts the token's user in the group sees runs marked within
/// this long, with no restart.
const TRIGGER_RECHECK: Duration = Duration::from_secs(300);

/// The longest text a trigger record's field may hold, as dagron-api has it.
const TRIGGER_FIELD_MAX: usize = 512;

/// The context of the status a refused pull request's head is given. It names
/// no pipeline and no job: none of the pull request's was read. A job's
/// context is `dagron-ci/{pipeline}/{job} (pull_request)`, so this is none of
/// them, and a run of the same commit never writes over it.
pub(crate) const REFUSED_CONTEXT: &str = "dagron-ci (pull_request)";

/// What that status says, within the 140 characters GitHub shows.
const REFUSED_DESCRIPTION: &str =
    "not run: pull requests from forks and from non-members are refused here (CI_FORK_PRS=refuse)";

/// And where they are run, but the cell does not record what started a run:
/// nothing would say this one is untrusted, so it is not made.
const UNRECORDED_DESCRIPTION: &str =
    "not run: this cell records no run as untrusted (dagron-ci's token is not in the ci:trigger group)";

/// How long an event waits between looks at its approval.
const APPROVAL_WAIT: Duration = Duration::from_secs(20);

pub struct Dispatcher {
    db: Db,
    cells: BTreeMap<String, String>,
    token: String,
    http: crate::config::Clients,
    forge: Arc<Forge>,
    settings: compile::Settings,
    admission: Option<Arc<dyn Admission>>,
    meter: Option<Arc<dyn RunMeter>>,
    governance: Option<Arc<dyn Governance>>,
    audit: Option<Arc<dyn Audit>>,
    /// Where a status this posts is counted, beside the reporter's.
    metrics: Option<Arc<Metrics>>,
    /// The attempt a worker ends an event at, when it is still to be retried
    /// ([`Dispatcher::with_max_attempts`]).
    max_attempts: Option<i64>,
    /// Whether each cell's dagron-api takes a trigger record from the token,
    /// and when it was asked ([`Dispatcher::marks`]).
    marks: Mutex<BTreeMap<String, (Instant, bool)>>,
    /// What a pull request from a fork or a non-member gets, where it is run
    /// ([`Dispatcher::with_forks`]). `None` refuses it.
    forks: Option<crate::forks::Forks>,
    /// Whether a run an agent asks for is run, as an agent run
    /// ([`Dispatcher::with_agent_runs`]).
    agents: bool,
    /// Whether a repository's `.github/workflows` files are read
    /// ([`Dispatcher::with_github_workflows`]).
    github: bool,
    /// Whether a GitLab project's `.gitlab-ci.yml` is read
    /// ([`Dispatcher::with_gitlab_pipelines`]).
    gitlab: bool,
    /// Where payloads too large for their event's row were kept
    /// (CI_OBJECT_STORE_URL).
    payloads: Option<crate::payloads::Payloads>,
    /// Where the actions their steps use are fetched from, and which may be
    /// ([`Dispatcher::with_actions`]).
    actions: github::ActionSource,
    /// Whether a job that may ask for an OIDC token is given one
    /// ([`Dispatcher::with_github_oidc`]).
    oidc: bool,
    /// Whether a workflow's job is given a `GITHUB_TOKEN`
    /// ([`Dispatcher::with_github_token`]).
    job_token: bool,
    /// Where jobs reach GitHub's REST API: dagron-ci's door, where one is
    /// served ([`Dispatcher::with_github_api_for_jobs`]).
    api_for_jobs: Option<String>,
    /// The repositories whose jobs may have a Docker daemon
    /// ([`Dispatcher::with_github_docker`]).
    docker: github::DockerRepos,
}

/// How long a meter may take to take a submitted run.
const METER_TIMEOUT: Duration = Duration::from_secs(5);

/// What became of one pipeline.
#[derive(Debug)]
enum Ran {
    /// Submitted now: the run.
    Submitted(String),
    /// Submitted before for this commit: the run.
    Recorded(String),
    /// Its `on:` does not take this event, and why.
    Skipped(String),
    /// Its `on:` takes the event and its `paths:` name none of the changed
    /// files: why, and its jobs, each of which is owed a status that says so.
    Unchanged { why: String, jobs: Vec<String> },
    /// It cannot run as written, or dagron-api refused its run, and why.
    Refused(String),
    /// The database or dagron-api could not be reached, and why.
    Retry(String),
    /// Its run's concurrency key has a run active: dagron-api's answer.
    Waiting(String),
}

/// What became of saying, on a commit, that a pipeline was not run.
enum Said {
    Posted,
    /// The repository's commits take no statuses: nobody was asked.
    NotAsked,
    /// The forge refused it, and would again, and why.
    Cannot(String),
    /// Not now: how the event is to be handled again, to say it then.
    Later(Finish),
}

/// The words of dagron-api's `403` where it is an environment's policy that
/// refused the run, and not dagron-ci's token. They are dagron's own
/// (`dagron_core::envpolicy::REFUSAL_MARK`), written again here because this
/// binary does not link dagron-core. A test holds the two to each other.
const POLICY_REFUSAL: &str = "may not be used by this run";

/// And where it is a run that is not trusted naming what such a run may not
/// use (`dagron_core::confine::REFUSAL_MARK`), held to dagron's likewise.
const CONFINED_REFUSAL: &str = "a run that is not trusted may not use";

/// How dagron-api answered a submit.
enum Answer {
    Run(String),
    Refused(String),
    Retry(String),
    Waiting(String),
}

/// Whether a run may be submitted now.
enum Admitted {
    /// Yes: no limit is set, or a slot is held for the run.
    Yes,
    /// Not until a run ends: the limit is reached, and why.
    Wait(String),
    /// The slot could not be asked for.
    Retry(String),
}

impl Dispatcher {
    pub fn new(
        db: Db,
        cells: BTreeMap<String, String>,
        token: String,
        forge: Arc<Forge>,
        settings: compile::Settings,
    ) -> Dispatcher {
        Dispatcher {
            http: crate::config::Clients::new(|| {
                reqwest::Client::builder()
                    .timeout(REQUEST_TIMEOUT)
                    .user_agent(concat!("dagron-ci/", env!("CARGO_PKG_VERSION")))
            }),
            db,
            cells,
            token,
            forge,
            settings,
            admission: None,
            meter: None,
            governance: None,
            audit: None,
            metrics: None,
            max_attempts: None,
            marks: Mutex::default(),
            forks: None,
            agents: false,
            github: false,
            gitlab: false,
            payloads: None,
            actions: github::ActionSource::default(),
            oidc: false,
            job_token: false,
            docker: github::DockerRepos::default(),
            api_for_jobs: None,
        }
    }

    /// Let the jobs of the repositories `repos` lists have a Docker daemon
    /// where their repository asks (`CI_GITHUB_DOCKER`).
    pub fn with_github_docker(mut self, repos: github::DockerRepos) -> Dispatcher {
        self.docker = repos;
        self
    }

    /// Have a workflow's job reach GitHub's REST API at dagron-ci's door,
    /// `url` (`CI_GITHUB_API_FOR_JOBS`): its `GITHUB_API_URL`.
    pub fn with_github_api_for_jobs(mut self, url: Option<String>) -> Dispatcher {
        self.api_for_jobs = url;
        self
    }

    /// Give a workflow's job a `GITHUB_TOKEN` the engine mints for each
    /// attempt (`CI_GITHUB_TOKEN=on`).
    pub fn with_github_token(mut self, on: bool) -> Dispatcher {
        self.job_token = on;
        self
    }

    /// Give a workflow's job that may ask for an OIDC token
    /// (`id-token: write`) the engine's, on a trusted run
    /// (`CI_GITHUB_OIDC=on`).
    pub fn with_github_oidc(mut self, on: bool) -> Dispatcher {
        self.oidc = on;
        self
    }

    /// Fetch the actions of other repositories that workflows' steps use
    /// from where `actions` says, and allow those it allows
    /// (`CI_ACTIONS_MIRROR`, `CI_ACTIONS_ALLOW`).
    pub fn with_actions(mut self, actions: github::ActionSource) -> Dispatcher {
        self.actions = actions;
        self
    }

    /// Read the workflow files a repository lists in its
    /// `.dagron/ci.yaml` (`CI_GITHUB_WORKFLOWS=on`), and run them as
    /// pipelines. Off, no repository is asked for that file.
    pub fn with_github_workflows(mut self, on: bool) -> Dispatcher {
        self.github = on;
        self
    }

    /// Read the `.gitlab-ci.yml` a GitLab project names in its
    /// `.dagron/ci.yaml` (`CI_GITLAB_PIPELINES=on`), with the files it
    /// includes, and run it as a pipeline. Off, none is read.
    pub fn with_gitlab_pipelines(mut self, on: bool) -> Dispatcher {
        self.gitlab = on;
        self
    }

    /// Log a status posted on the event's commit, and what came of it
    /// (`ci_report_log`). A log that cannot be written is said, and the
    /// event goes on: the post was made.
    async fn log_post(
        &self,
        ev: &Claimed,
        status: &CommitStatus,
        posted: &Result<(), StatusError>,
    ) {
        let row = crate::report::logged(&ev.id, None, &ev.repo.id, &ev.sha, status, posted);
        if let Err(e) = self.db.log_reports(std::slice::from_ref(&row)).await {
            tracing::warn!(event = %ev.id, context = %status.context, error = %e, "a status posted could not be logged");
        }
    }

    /// Read the payloads kept in CI_OBJECT_STORE_URL ([`crate::payloads`]).
    pub fn with_payloads(mut self, payloads: Option<crate::payloads::Payloads>) -> Dispatcher {
        self.payloads = payloads;
        self
    }

    /// Run pull requests from forks and from non-members as `forks` says
    /// (`CI_FORK_PRS=untrusted`), where it says anything: they are refused
    /// otherwise.
    pub fn with_forks(mut self, forks: Option<crate::forks::Forks>) -> Dispatcher {
        self.forks = forks;
        self
    }

    /// Run what an agent asks for (`CI_AGENT_RUNS=untrusted`), as an agent
    /// run, on a cell that records it so: it is refused otherwise.
    pub fn with_agent_runs(mut self, on: bool) -> Dispatcher {
        self.agents = on;
        self
    }

    /// Whether the dagron-api at `base` takes a trigger record from
    /// dagron-ci's token: whether the token's user is in [`TRIGGER_GROUP`].
    /// Asked of the cell (`GET /api/me`) and kept for [`TRIGGER_RECHECK`].
    ///
    /// A cell that cannot be asked counts as one that does not, for this
    /// submit alone: a run made unmarked is a run, and a submit dagron-api
    /// refuses for its trigger is none. That is for a trusted event. One that
    /// is not is never submitted unmarked ([`Dispatcher::marked`]).
    async fn marks(&self, base: &str) -> bool {
        self.takes_record(base).await.unwrap_or(false)
    }

    /// [`Dispatcher::marks`], with a cell that could not be asked told apart
    /// from one that said no: `None`.
    async fn takes_record(&self, base: &str) -> Option<bool> {
        if let Some((asked, takes)) = self.marks.lock().ok().and_then(|m| m.get(base).copied()) {
            if asked.elapsed() < TRIGGER_RECHECK {
                return Some(takes);
            }
        }
        let answer = self
            .http
            .to(base)
            .get(format!("{base}/api/me"))
            .bearer_auth(&self.token)
            .send()
            .await;
        let groups = match answer {
            Ok(response) if response.status().is_success() => response
                .json::<serde_json::Value>()
                .await
                .ok()
                .map(|me| me["groups"].clone()),
            _ => None,
        };
        let groups = groups?;
        let takes = groups
            .as_array()
            .is_some_and(|groups| groups.iter().any(|g| g == TRIGGER_GROUP));
        let before = self
            .marks
            .lock()
            .ok()
            .and_then(|mut m| m.insert(base.to_string(), (Instant::now(), takes)))
            .map(|(_, takes)| takes);
        if before != Some(takes) {
            if takes {
                tracing::info!(cell = %base, "runs are submitted with a trigger record, and read as CI's");
            } else {
                tracing::warn!(
                    cell = %base,
                    "dagron-api takes no trigger record from dagron-ci's token (CI_API_TOKEN_FILE): \
                     its user is not in the `{TRIGGER_GROUP}` group. Runs are submitted without one, \
                     and read as manual"
                );
            }
        }
        Some(takes)
    }

    /// `body`, with the event's trigger record where the cell takes one.
    ///
    /// An event that is not trusted always carries it. A run made without
    /// one reads as a person's own, which is trusted: so where the cell
    /// would not take the record, the submit is refused, and is no run.
    async fn marked(&self, base: &str, ev: &Claimed, mut body: serde_json::Value) -> serde_json::Value {
        if ev.trust != "trusted" || self.marks(base).await {
            body["trigger"] = self.trigger(ev);
        }
        body
    }

    /// The event's trigger record, its actor read as a person of the build's
    /// directory where the build knows the login ([`Governance::actor`]).
    fn trigger(&self, ev: &Claimed) -> serde_json::Value {
        let mut record = trigger(ev);
        if let (Some(governance), Some(login)) = (&self.governance, ev.actor.as_deref()) {
            if let Some(person) = governance.actor(&ev.repo.forge_host, login) {
                if person.len() <= TRIGGER_FIELD_MAX && !person.chars().any(char::is_control) {
                    record["actor"] = person.into();
                }
            }
        }
        record
    }

    /// Tell `meter` of each run submitted.
    pub fn with_run_meter(mut self, meter: Arc<dyn RunMeter>) -> Dispatcher {
        self.meter = Some(meter);
        self
    }

    /// Hold each event's repository to `governance`: the pipelines it
    /// requires, its word on each run, and who an actor is.
    pub fn with_governance(mut self, governance: Arc<dyn Governance>) -> Dispatcher {
        self.governance = Some(governance);
        self
    }

    /// Tell `audit` of each run submitted and each pipeline refused.
    pub fn with_audit(mut self, audit: Arc<dyn Audit>) -> Dispatcher {
        self.audit = Some(audit);
        self
    }

    /// Ask `admission` before each run is submitted.
    pub fn with_admission(mut self, admission: Arc<dyn Admission>) -> Dispatcher {
        self.admission = Some(admission);
        self
    }

    /// Count each status this posts in `metrics`, as the reporter counts its
    /// own (`dagron_ci_commit_statuses_total`).
    pub fn with_metrics(mut self, metrics: Arc<Metrics>) -> Dispatcher {
        self.metrics = Some(metrics);
        self
    }

    /// The attempt at which the worker gives an event up: one still to be
    /// retried then is failed (`worker::Settings::max_attempts`). The
    /// dispatcher is told, so that what it says once an event has ended it can
    /// say on that attempt, which is the last turn the event gets.
    pub fn with_max_attempts(mut self, max_attempts: i64) -> Dispatcher {
        self.max_attempts = Some(max_attempts);
        self
    }

    /// What bundle `id` changes ([`crate::bundles::Patch`]), from the store.
    async fn patch(&self, id: &str) -> Result<crate::bundles::Patch, Finish> {
        let Some(store) = &self.payloads else {
            return Err(Finish::Failed(
                "this event names a patch bundle, and this dagron-ci keeps none (CI_OBJECT_STORE_URL)".into(),
            ));
        };
        let bundle = match self.db.bundle(id).await {
            Ok(Some(b)) if b.ready => b,
            Ok(_) => {
                return Err(Finish::Failed(format!(
                    "the patch bundle {id} is no longer kept"
                )))
            }
            Err(e) => {
                return Err(Finish::Retry {
                    reason: format!("the database could not be read: {e}"),
                    after: std::time::Duration::from_secs(5),
                })
            }
        };
        let bytes = match store.get_bundle(&bundle.object_name).await {
            Ok(bytes) => bytes,
            Err(e) => {
                return Err(Finish::Retry {
                    reason: format!("{e:#}"),
                    after: std::time::Duration::from_secs(5),
                })
            }
        };
        crate::bundles::read(&bytes)
            .map_err(|why| Finish::Failed(format!("the patch bundle {id} cannot be read: {why}")))
    }

    /// A file of the event's commit, as its patch bundle has it where it has
    /// one, else as the forge has it.
    async fn file(
        &self,
        ev: &Claimed,
        path: &str,
    ) -> Result<Option<Result<String, String>>, crate::forge::ForgeError> {
        if let Some(over) = ev.patch.as_ref().and_then(|p| p.file(path)) {
            return Ok(over.map(|bytes| {
                String::from_utf8(bytes.to_vec()).map_err(|_| "is not UTF-8 text".to_string())
            }));
        }
        self.forge.file(&ev.repo, path, &ev.sha).await
    }

    async fn dispatch(&self, ev: &Claimed) -> Finish {
        // A run on a patch bundle reads what it changes before the forge.
        let with_patch;
        let ev = match &ev.bundle_id {
            Some(id) => match self.patch(id).await {
                Ok(patch) => {
                    with_patch = Claimed {
                        patch: Some(std::sync::Arc::new(patch)),
                        ..ev.clone()
                    };
                    &with_patch
                }
                Err(finish) => return finish,
            },
            None => ev,
        };
        // What the event is comes before whose it is: these run nothing
        // whoever they are from, and are not refused for it.
        match ev.kind.as_str() {
            "delete" => return Finish::Skipped("a deleted ref runs nothing".into()),
            "pull_request_closed" => {
                return Finish::Skipped("a closed pull request runs nothing".into())
            }
            "merge_group_destroyed" => return self.drop_group(ev).await,
            _ => {}
        }
        // Its forge runs a fleet-mode repository's pipelines: what of it
        // waited when it was put in that mode runs nothing here.
        if !ev.repo.mode.runs_pipelines() {
            return Finish::Skipped(crate::db::FLEET_MODE.into());
        }
        match ev.trust.as_str() {
            "trusted" => {}
            // Run, where this dagron-ci runs them, as an agent run: held to
            // the floor of a run that is not trusted, which the engine reads
            // from the run's record.
            "agent" if self.agents => {}
            "agent" => {
                return Finish::Skipped(
                    "a run an agent asks for is refused here (CI_AGENT_RUNS=refuse): this \
                     dagron-ci runs no work of the agent trust class"
                        .into(),
                )
            }
            // Run, where this dagron-ci runs them, and only a pull request:
            // nothing else is made untrusted by where it is from.
            _ if self.forks.is_some() && ev.kind == "pull_request" => {}
            // A run on a patch bundle: untrusted for what it is, a change
            // nobody reviewed, whoever asked. Run where the run's record
            // says so (below).
            _ if ev.bundle_id.is_some() => {}
            _ => {
                let why = format!(
                    "a {} event from a fork or a non-member is refused (CI_FORK_PRS=refuse)",
                    ev.kind
                );
                return self.refused(ev, why, REFUSED_DESCRIPTION).await;
            }
        }
        let repo = &ev.repo;
        let Some(base) = self.cells.get(&repo.cell) else {
            return Finish::Failed(format!("cell {:?} is not in CI_CELLS", repo.cell));
        };
        if ev.trust == "agent" || ev.bundle_id.is_some() {
            match self.records(ev, base).await {
                Ok(true) => {}
                // Not said on the commit: an agent's dispatch is of a ref that
                // others build on, and its refusal is the agent's to read.
                Ok(false) => {
                    return Finish::Skipped(format!(
                        "an agent's dispatch is not run on cell {:?}: its dagron-api takes no \
                         record of what started a run from dagron-ci's token (the \
                         `{TRIGGER_GROUP}` group), so nothing would say the run is an agent's",
                        ev.repo.cell
                    ))
                }
                Err(finish) => return finish,
            }
        } else if let (Some(forks), true, None) =
            (&self.forks, ev.trust != "trusted", &ev.bundle_id)
        {
            if let Err(finish) = self.untrusted(ev, base, forks).await {
                return finish;
            }
        }
        let reads = Forge::reads_pipelines(repo);
        let named = ev.pipeline.as_deref();
        if reads {
            let mut files = match self.forge.pipelines(repo, &ev.sha).await {
                Ok(files) => files.unwrap_or_default(),
                Err(e) => return forge_finish(e),
            };
            // A patch bundle's own pipelines, and none it deletes.
            if let Some(patch) = &ev.patch {
                files.retain(|f| patch.file(&f.path).is_none());
                for (path, bytes) in patch.in_dir(crate::forge::PIPELINE_DIR) {
                    if path.ends_with(".yaml") || path.ends_with(".yml") {
                        files.push(PipelineFile {
                            path: path.to_string(),
                            text: String::from_utf8(bytes.to_vec())
                                .map_err(|_| "is not UTF-8 text".to_string()),
                        });
                    }
                }
                files.sort_by(|a, b| a.path.cmp(&b.path));
            }
            // And the workflow files the repository says run here, where
            // this dagron-ci reads any.
            let settings = match self.workflows(ev, &mut files).await {
                Ok(settings) => github::Settings {
                    actions: self.actions.clone(),
                    oidc: self.oidc,
                    github_token: self.job_token,
                    // The repository asks; the operator says whether it may.
                    docker: settings.docker && self.docker.allows(&repo.full_name),
                    // The door, where it is served and also answers.
                    api_url: self.api_for_jobs.clone(),
                    ..settings
                },
                Err(finish) => return finish,
            };
            // Whether the repository has pipelines of its own, before the
            // build's are added: one with none still runs its bound workflow,
            // and a patch bundle is laid over its own alone.
            let own = !files.is_empty();
            // And the pipelines the build requires of the repository, which
            // are not the repository's to change.
            if let Some(governance) = &self.governance {
                governance::with_required(
                    &mut files,
                    governance.required(&repo.forge_host, &repo.full_name),
                );
            }
            if !files.is_empty() && (own || ev.bundle_id.is_none()) {
                // Read of every pipeline before one is picked by name: a
                // schedule's run, or a dispatch's, of a pipeline another waits
                // on the failure of raises its `failed` event all the same.
                let listened = listened(&files);
                let files = match named {
                    Some(name) => match pick(files, name, &ev.sha) {
                        Ok(file) => vec![file],
                        Err(why) => return Finish::Failed(why),
                    },
                    None => files,
                };
                let ran = self
                    .run_pipelines(ev, base, files, &settings, &listened)
                    .await;
                // The build's pipelines beside the bound workflow, never in
                // its place.
                return match repo.workflow.as_deref() {
                    Some(workflow) if !own && named.is_none() => {
                        self.beside_workflow(ev, base, workflow, ran).await
                    }
                    _ => ran,
                };
            }
        }
        // A patch is laid over a pipeline's checkout, which is dagron-ci's to
        // make: a repository bound to a workflow of its own has none.
        if ev.bundle_id.is_some() {
            return Finish::Failed(format!(
                "a patch bundle runs the repository's own pipelines ({}), and the commit has none",
                crate::forge::PIPELINE_DIR
            ));
        }
        // A pipeline asked for by name, and none to be had: never the bound
        // workflow in its place.
        if let Some(name) = named {
            return Finish::Failed(if reads {
                format!(
                    "pipeline {name:?} was asked for, and {} has no pipelines in {PIPELINE_DIR}",
                    short(&ev.sha)
                )
            } else {
                format!(
                    "pipeline {name:?} was asked for, and pipelines are read from GitHub \
                     and GitLab repositories only so far"
                )
            });
        }
        let Some(workflow) = repo.workflow.as_deref() else {
            let why = if reads && !self.forge.has_token(repo) && repo.forge == "gitlab" {
                format!(
                    "{} has no pipelines in {PIPELINE_DIR} that GitLab shows without a \
                     credential (a private project's need a token for its host in \
                     CI_GITLAB_TOKENS_DIR)",
                    short(&ev.sha)
                )
            } else if reads && !self.forge.has_token(repo) {
                format!(
                    "{} has no pipelines in {PIPELINE_DIR} that GitHub shows without a \
                     credential (a private repository's need the App installed on it, \
                     CI_GITHUB_APP_ID, or CI_GITHUB_TOKEN_FILE)",
                    short(&ev.sha)
                )
            } else if reads && self.reads_forge_files(repo) && repo.mode != Mode::Compat {
                format!(
                    "{} has no pipelines in {PIPELINE_DIR}, and the forge's own files are read \
                     for a repository in compat mode alone (`dagron-ci repo set {}/{} --mode \
                     compat`)",
                    short(&ev.sha),
                    repo.forge_host,
                    repo.full_name
                )
            } else if reads {
                format!("{} has no pipelines in {PIPELINE_DIR}", short(&ev.sha))
            } else {
                format!(
                    "pipelines are read from GitHub and GitLab repositories only so far, and \
                     {} is on {}",
                    repo.full_name, repo.forge
                )
            };
            return Finish::Skipped(format!(
                "{why}, and no workflow is bound to {}/{}; bind one with \
                 `dagron-ci repo set {0}/{1} --workflow ID`",
                repo.forge_host, repo.full_name
            ));
        };
        self.run_workflow(ev, base, workflow).await
    }

    /// Whether this dagron-ci reads `repo`'s forge's own pipeline files
    /// where its `.dagron/ci.yaml` names them (CI_GITHUB_WORKFLOWS,
    /// CI_GITLAB_PIPELINES), its mode aside.
    fn reads_forge_files(&self, repo: &Repo) -> bool {
        self.github || (self.gitlab && crate::forge::gitlab::is_gitlab(repo))
    }

    /// The repository's settings for the GitHub Actions front-end at the
    /// event's commit, with the workflow files they list added to `files`.
    /// No settings, and nothing added, where this dagron-ci reads no
    /// workflow files, the repository is not in compat mode, or the commit
    /// has no `.dagron/ci.yaml`.
    ///
    /// Settings that cannot be read, and a listed file the commit does not
    /// have, are added as files that failed to be read: the event then says
    /// so, on the commit, as it does of a pipeline that is invalid.
    async fn workflows(
        &self,
        ev: &Claimed,
        files: &mut Vec<PipelineFile>,
    ) -> Result<github::Settings, Finish> {
        let gitlab = self.gitlab && crate::forge::gitlab::is_gitlab(&ev.repo);
        // In native mode a repository is run as its `.dagron/ci` says,
        // whatever the build reads.
        if !self.reads_forge_files(&ev.repo) || ev.repo.mode != Mode::Compat {
            return Ok(github::Settings::default());
        }
        let read = match self.file(ev, github::SETTINGS_FILE).await {
            Ok(None) => return Ok(github::Settings::default()),
            Ok(Some(read)) => read,
            Err(e) => return Err(forge_finish(e)),
        };
        // Where this dagron-ci reads no workflow files, the settings' `gitlab:`
        // alone is read: the rest is no business of its, and a mistake there
        // is not made the project's events' failure.
        let settings = match read.and_then(|text| {
            if self.github {
                github::settings(&text).map_err(|p| invalid(&p))
            } else {
                gitlab::settings_in(&text)
                    .map(|gitlab| github::Settings {
                        gitlab,
                        ..github::Settings::default()
                    })
                    .map_err(|p| invalid(&p))
            }
        }) {
            Ok(settings) => settings,
            Err(why) => {
                files.push(PipelineFile {
                    path: github::SETTINGS_FILE.into(),
                    text: Err(why),
                });
                return Ok(github::Settings::default());
            }
        };
        let mut settings = settings;
        if gitlab {
            self.gitlab_files(ev, &mut settings.gitlab, files).await?;
        } else {
            settings.gitlab = Default::default();
        }
        if !self.github {
            return Ok(github::Settings {
                gitlab: settings.gitlab,
                ..github::Settings::default()
            });
        }
        for name in &settings.workflows {
            let path = format!("{}/{name}", github::WORKFLOW_DIR);
            let text = match self.file(ev, &path).await {
                Ok(Some(text)) => text,
                Ok(None) => Err(format!(
                    "is listed in {}, and the commit has no such file",
                    github::SETTINGS_FILE
                )),
                Err(e) => return Err(forge_finish(e)),
            };
            files.push(PipelineFile { path, text });
        }
        Ok(settings)
    }

    /// A GitLab project's pipeline file, which its settings name, added to
    /// `files`, and every file it includes from the project read into
    /// `settings.included`, as far as GitLab reads them: each file's own
    /// includes in turn, at most [`gitlab::MAX_FILES`] in all.
    async fn gitlab_files(
        &self,
        ev: &Claimed,
        settings: &mut gitlab::Settings,
        files: &mut Vec<PipelineFile>,
    ) -> Result<(), Finish> {
        let Some(path) = settings.pipeline.clone() else {
            return Ok(());
        };
        let text = match self.file(ev, &path).await {
            Ok(Some(text)) => text,
            Ok(None) => Err(format!(
                "is named in {}, and the commit has no such file",
                github::SETTINGS_FILE
            )),
            Err(e) => return Err(forge_finish(e)),
        };
        let mut queue: Vec<String> = match &text {
            Ok(text) => gitlab::local_includes(text).unwrap_or_default(),
            Err(_) => Vec::new(),
        };
        while let Some(include) = queue.pop() {
            if settings.included.contains_key(&include)
                || settings.included.len() >= gitlab::MAX_FILES
            {
                continue;
            }
            let read = match self.file(ev, &include).await {
                Ok(Some(read)) => read,
                Ok(None) => Err("is included, and the commit has no such file".to_string()),
                Err(e) => return Err(forge_finish(e)),
            };
            if let Ok(text) = &read {
                queue.extend(gitlab::local_includes(text).unwrap_or_default());
            }
            settings.included.insert(include, read);
        }
        files.push(PipelineFile { path, text });
        Ok(())
    }

    /// The repository's default branch, for a GitLab pipeline's
    /// `CI_DEFAULT_BRANCH`: the webhook's word where it gave one, else the
    /// forge's `HEAD`.
    /// A forge that cannot be read is tried again, and not taken for a
    /// repository with no default branch: a rule that reads it would make
    /// another pipeline of the same event.
    async fn default_branch(
        &self,
        ev: &Claimed,
        payload: Option<&serde_json::Value>,
    ) -> Result<Option<String>, Finish> {
        if let Some(branch) = payload
            .and_then(|p| p["project"]["default_branch"].as_str())
            .filter(|b| !b.is_empty())
        {
            return Ok(Some(branch.to_string()));
        }
        match self.forge.list_refs(&ev.repo, &[]).await {
            Ok(refs) => Ok(refs.head.map(|head| {
                head.strip_prefix("refs/heads/")
                    .unwrap_or(&head)
                    .to_string()
            })),
            Err(e) => Err(forge_finish(e)),
        }
    }

    /// The bound workflow of a repository with no pipelines of its own, run
    /// once the pipelines the build requires of it have `ran`. One that has
    /// to be tried again is, both together: each run is kept under its own
    /// key, so what was submitted is found and not submitted twice.
    async fn beside_workflow(
        &self,
        ev: &Claimed,
        base: &str,
        workflow: &str,
        ran: Finish,
    ) -> Finish {
        let (required, failed) = match ran {
            Finish::Done(why) | Finish::Skipped(why) => (why, false),
            Finish::Failed(why) => (why, true),
            again @ (Finish::Retry { .. } | Finish::Wait { .. }) => return again,
        };
        match self.run_workflow(ev, base, workflow).await {
            Finish::Done(why) | Finish::Skipped(why) if !failed => {
                Finish::Done(format!("{required}; workflow {workflow}: {why}"))
            }
            Finish::Done(why) | Finish::Skipped(why) | Finish::Failed(why) => {
                Finish::Failed(format!("{required}; workflow {workflow}: {why}"))
            }
            again => again,
        }
    }

    /// The workflow bound to the repository, run with the event as its
    /// parameters.
    async fn run_workflow(&self, ev: &Claimed, base: &str, workflow: &str) -> Finish {
        let pipeline = format!("workflow:{workflow}");
        let key = idempotency_key(&ev.commit_key, &pipeline);
        match self.db.find_run(&key).await {
            Ok(Some(run_id)) => {
                return Finish::Done(format!(
                    "run {run_id} was submitted for this commit already"
                ))
            }
            Ok(None) => {}
            Err(e) => {
                return Finish::Retry {
                    reason: format!("reading ci_runs: {e}"),
                    after: Duration::ZERO,
                }
            }
        }
        match self.admit(&key, ev).await {
            Admitted::Yes => {}
            Admitted::Wait(why) => {
                return Finish::Wait {
                    reason: format!("waiting: {why}"),
                    after: CONCURRENCY_WAIT,
                }
            }
            Admitted::Retry(why) => {
                return Finish::Retry {
                    reason: why,
                    after: Duration::ZERO,
                }
            }
        }
        let url = format!("{base}/api/workflows/{workflow}/run");
        let body = self
            .marked(base, ev, serde_json::json!({ "parameters": parameters(ev) }))
            .await;
        let answer = self.submit(&url, &key, &body).await;
        self.settle(&key, &answer).await;
        let run_id = match answer {
            Answer::Run(run_id) => run_id,
            Answer::Refused(why) => return Finish::Failed(why),
            Answer::Retry(why) => {
                return Finish::Retry {
                    reason: why,
                    after: Duration::ZERO,
                }
            }
            Answer::Waiting(why) => {
                return Finish::Wait {
                    reason: format!("waiting: {why}"),
                    after: CONCURRENCY_WAIT,
                }
            }
        };
        match self.record(ev, &pipeline, &key, &run_id, false).await {
            // A retry of an event that died after submitting: dagron-api gave
            // the same run back, and it is recorded already, or now.
            Ok(()) => Finish::Done(format!("submitted run {run_id} on cell {}", ev.repo.cell)),
            // The run exists. Retrying replays the same key: the same run comes
            // back and is recorded then.
            Err(why) => Finish::Retry {
                reason: why,
                after: Duration::ZERO,
            },
        }
    }

    /// The repository's pipelines at the event's commit. Every pipeline is
    /// decided before any is submitted, so a forge that cannot list the
    /// changed files leaves nothing half done.
    ///
    /// A workflow file is read for this event ([`github::Workflow::lower`]):
    /// what it becomes is a pipeline like any other from here on. The jobs
    /// it leaves out (an `if:` that is false) say so on the commit once its
    /// run is made, as GitHub shows such a job skipped.
    async fn run_pipelines(
        &self,
        ev: &Claimed,
        base: &str,
        files: Vec<PipelineFile>,
        settings: &github::Settings,
        listened: &std::collections::BTreeSet<String>,
    ) -> Finish {
        // A patch bundle's runs lay it over their checkout.
        let settings_of_event = compile::Settings {
            patched: ev.bundle_id.is_some(),
            ..self.settings
        };
        let mut event = compile_event(ev);
        // The forge's own payload, which a workflow reads as `github.event`.
        // One that cannot be had is as one never kept: the event is run with
        // what dagron-ci knows of it. One kept in the store that the store
        // does not give now is asked for again: the event is tried again.
        let text = match self.db.event_payload(&ev.id).await {
            Ok(Some(EventPayload::Inline(text))) => Some(text),
            Ok(Some(EventPayload::Stored(name))) => {
                let read = match &self.payloads {
                    Some(payloads) => payloads.get(&name).await,
                    None => Err(anyhow::anyhow!(
                        "it was kept in a payload store, and CI_OBJECT_STORE_URL is not set"
                    )),
                };
                match read {
                    Ok(bytes) => Some(String::from_utf8_lossy(&bytes).into_owned()),
                    Err(e) => {
                        return Finish::Retry {
                            reason: format!("reading the event's payload: {e:#}"),
                            after: Duration::ZERO,
                        }
                    }
                }
            }
            Ok(None) => None,
            Err(e) => {
                tracing::warn!(event = %ev.id, error = %e, "the event's payload could not be read; its run has dagron-ci's own fields of it");
                None
            }
        };
        event.payload = text.and_then(|text| serde_json::from_str(&text).ok());
        let is_gitlab = |path: &str| settings.gitlab.pipeline.as_deref() == Some(path);
        let is_workflow = |path: &str| path.starts_with(github::WORKFLOW_DIR);
        // A commit's checks are named by a pipeline's file, so a workflow
        // file and a pipeline of one name would report under one another's.
        let native: std::collections::BTreeSet<String> = files
            .iter()
            .filter(|f| !is_workflow(&f.path) && !is_gitlab(&f.path))
            .map(|f| crate::report::pipeline_name(&f.path).to_string())
            .collect();
        // Every other file's name, which a GitLab pipeline's may not share.
        let elsewhere: std::collections::BTreeSet<String> = files
            .iter()
            .filter(|f| !is_gitlab(&f.path))
            .map(|f| crate::report::pipeline_name(&f.path).to_string())
            .collect();
        // A GitLab pipeline: read once, for every event, and made for this
        // one; where its `rules:changes` ask, the changed files are looked
        // up first.
        let mut gitlab_read: BTreeMap<String, Result<gitlab::Ci, String>> = BTreeMap::new();
        for file in files.iter().filter(|f| is_gitlab(&f.path)) {
            let read = file.text.clone().and_then(|text| {
                gitlab::parse(&text, &settings.gitlab.included).map_err(|p| invalid(&p))
            });
            gitlab_read.insert(file.path.clone(), read);
        }
        if !gitlab_read.is_empty() {
            event.default_branch = match self.default_branch(ev, event.payload.as_ref()).await {
                Ok(branch) => branch,
                Err(finish) => return finish,
            };
            let asks = gitlab_read.iter().any(|(path, ci)| {
                ci.as_ref().is_ok_and(|ci| {
                    let stem = crate::report::pipeline_name(path);
                    ci.lower(stem, &event, &settings.gitlab)
                        .is_ok_and(|l| l.needs_changes)
                })
            });
            if asks {
                event.changed = match self.changes(ev).await {
                    Ok(changes) => changes,
                    Err(finish) => return finish,
                };
            }
        }
        // Why a GitLab pipeline makes no run of this event, where it makes
        // none.
        let mut passed: BTreeMap<String, String> = BTreeMap::new();
        let mut left_out: BTreeMap<String, Vec<(String, String)>> = BTreeMap::new();
        // Each workflow file's text, to be read again with its run's numbers.
        let mut texts: BTreeMap<String, String> = BTreeMap::new();
        let read: Vec<(String, Result<pipeline::Pipeline, String>)> = files
            .into_iter()
            .map(|file| {
                let parsed = if is_gitlab(&file.path) {
                    let stem = crate::report::pipeline_name(&file.path).to_string();
                    let ci = gitlab_read
                        .get(&file.path)
                        .cloned()
                        .unwrap_or_else(|| Err("was not read".into()));
                    ci.and_then(|ci| {
                        if elsewhere.contains(&stem) {
                            return Err(format!(
                                "a pipeline in {PIPELINE_DIR}, or a workflow file, is called \
                                 {stem} too, and a commit's checks are named by it: rename one \
                                 of the two"
                            ));
                        }
                        if !pipeline::is_name(&stem) {
                            return Err(format!(
                                "its file's name is what its checks are called by, and \
                                 {stem:?} has more than letters, digits, '.', '_' and '-' in it"
                            ));
                        }
                        let lowered = ci
                            .lower(&stem, &event, &settings.gitlab)
                            .map_err(|problems| invalid(&problems))?;
                        if let Some(why) = lowered.passed_over {
                            passed.insert(file.path.clone(), why);
                        }
                        Ok(lowered.pipeline)
                    })
                } else if is_workflow(&file.path) {
                    let stem = crate::report::pipeline_name(&file.path).to_string();
                    if let Ok(text) = &file.text {
                        texts.insert(file.path.clone(), text.clone());
                    }
                    file.text.and_then(|text| {
                        if native.contains(&stem) {
                            return Err(format!(
                                "a pipeline in {PIPELINE_DIR} is called {stem} too, and a \
                                 commit's checks are named by it: rename one of the two"
                            ));
                        }
                        if !pipeline::is_name(&stem) {
                            return Err(format!(
                                "its file's name is what its checks are called by, and \
                                 {stem:?} has more than letters, digits, '.', '_' and '-' in it"
                            ));
                        }
                        let lowered = github::parse(&text, &settings.provided)
                            .and_then(|workflow| workflow.lower(&stem, &event, settings))
                            .map_err(|problems| invalid(&problems))?;
                        left_out.insert(file.path.clone(), lowered.skipped);
                        Ok(lowered.pipeline)
                    })
                } else {
                    file.text.and_then(|text| {
                        pipeline::parse(&text).map_err(|problems| invalid(&problems))
                    })
                };
                (file.path, parsed)
            })
            .collect();
        let decide = |event: &compile::Event| -> Vec<Result<Outcome, String>> {
            read.iter()
                .map(|(path, p)| match (p, passed.get(path)) {
                    (Ok(_), Some(why)) => Ok(Outcome::Skip(why.clone())),
                    (Ok(p), None) => compile::compile(p, event, &settings_of_event),
                    (Err(why), _) => Err(why.clone()),
                })
                .collect()
        };
        let mut decided = decide(&event);
        // Which pipelines a paths filter decides: asked of the forge once.
        let asked: Vec<bool> = decided
            .iter()
            .map(|d| matches!(d, Ok(Outcome::NeedsChangedFiles)))
            .collect();
        if asked.contains(&true) {
            event.changed = match self.changes(ev).await {
                Ok(changes) => changes,
                Err(finish) => return finish,
            };
            decided = decide(&event);
        }
        // What reading actions' files has spent for this event.
        let mut reading = crate::nested::Reading::default();
        // Whose actions are fetched, and read, with a credential: the
        // repository's owner's, for a trusted run on github.com.
        let owner = (ev.trust == "trusted"
            && ev
                .repo
                .forge_host
                .eq_ignore_ascii_case(crate::github::DEFAULT_HOST))
        .then(|| {
            ev.repo
                .full_name
                .split('/')
                .next()
                .unwrap_or_default()
                .to_string()
        })
        .filter(|owner| !owner.is_empty());
        // A workflow that runs is given its run's numbers, and is read again
        // with them: `github.run_id` and `github.run_number` in what it says,
        // a concurrency group among them. One that does not run is given
        // none, so a workflow's count is of its runs.
        for (at, (path, _)) in read.iter().enumerate() {
            let gitlab_ci = gitlab_read.get(path).and_then(|ci| ci.as_ref().ok());
            let text = texts.get(path);
            if text.is_none() && gitlab_ci.is_none() {
                continue;
            }
            if !matches!(decided[at], Ok(Outcome::Run(_))) {
                continue;
            }
            let key = idempotency_key(&ev.commit_key, path);
            let numbers = match self.db.run_numbers(&ev.repo.id, path, &key).await {
                Ok(numbers) => numbers,
                Err(e) => {
                    return Finish::Retry {
                        reason: format!("giving {path}'s run its numbers: {e}"),
                        after: Duration::ZERO,
                    }
                }
            };
            let mut numbered = event.clone();
            numbered.run_numbers = Some(numbers);
            let stem = crate::report::pipeline_name(path).to_string();
            if let Some(ci) = gitlab_ci {
                // Read again with its numbers, a rule may read them: one
                // that then holds for no job passes the pipeline over.
                decided[at] = ci
                    .lower(&stem, &numbered, &settings.gitlab)
                    .map_err(|problems| invalid(&problems))
                    .and_then(|l| match l.passed_over {
                        Some(why) => Ok(Outcome::Skip(why)),
                        None => compile::compile(&l.pipeline, &numbered, &settings_of_event),
                    });
                continue;
            }
            let Some(text) = text else { continue };
            let lowered = github::parse(text, &settings.provided)
                .and_then(|workflow| workflow.lower(&stem, &numbered, settings))
                .map_err(|problems| invalid(&problems));
            decided[at] = match lowered {
                Err(why) => Err(why),
                Ok(mut lowered) => {
                    left_out.insert(path.clone(), lowered.skipped);
                    // And the actions its actions use, fetched before the
                    // job starts as its own steps' are.
                    match crate::nested::resolve(
                        &mut lowered.pipeline,
                        &self.forge,
                        &settings.actions,
                        &ev.repo,
                        &ev.sha,
                        owner.as_deref(),
                        &mut reading,
                    )
                    .await
                    {
                        Ok(()) => {
                            compile::compile(&lowered.pipeline, &numbered, &settings_of_event)
                        }
                        Err(crate::nested::Nested::Refused(why)) => Err(why),
                        Err(crate::nested::Nested::Forge(e)) => return forge_finish(e),
                    }
                }
            };
        }
        let unchecked = match &event.changed {
            Changes::Unlisted(why) => Some(why.as_str()),
            _ => None,
        };
        // An agent's dispatch says nothing on the commit of what it did not
        // run, or left out: the commit is one others build on, and what became
        // of the dispatch is the agent's to read, on the event.
        // Nor does a schedule's: its commit is the default branch's head,
        // which a push of its own has said everything of.
        // Nor does a failure's: the commit says the failure already, and what
        // answers it is the answering pipeline's to say.
        let says = ev.trust != "agent"
            && !matches!(ev.kind.as_str(), "schedule" | "failed")
            && ev.bundle_id.is_none();

        // A trusted run of a pipeline another of this commit's waits on the
        // failure of (`listened`) raises a `failed` event as it fails. A
        // failure's own run raises none, nor a run that is not trusted or is
        // of a patch bundle: what it ran is no one's to answer with a trusted
        // pipeline.
        let raises = ev.trust == "trusted" && ev.bundle_id.is_none() && ev.kind != "failed";
        let mut ran = Vec::with_capacity(read.len());
        for (((path, _), outcome), asked) in read.iter().zip(decided).zip(asked) {
            let on_failed = raises && listened.contains(crate::report::pipeline_name(path));
            let result = match outcome {
                // The build's rules have their word before dagron-api has its.
                Ok(Outcome::Run(compiled)) => match self.review(ev, path, &compiled) {
                    Err(why) => Ran::Refused(why),
                    Ok(()) => {
                        let mut result = self
                            .run_pipeline(ev, base, path, &compiled, on_failed)
                            .await;
                        // The jobs its run left out for their `when:`, said
                        // skipped as a workflow's left out by `if:` are.
                        if let (true, Ran::Submitted(run) | Ran::Recorded(run)) = (says, &result) {
                            let jobs = left_out.entry(path.clone()).or_default();
                            for job in self.left_by_when(ev, base, run, &compiled).await {
                                if !jobs.iter().any(|(had, _)| *had == job) {
                                    jobs.push((job, compile::LEFT_OUT_BY_WHEN.into()));
                                }
                            }
                        }
                        if let (true, Some(why), Ran::Submitted(run)) = (asked, unchecked, &result)
                        {
                            result = Ran::Submitted(format!("{run} (paths not checked: {why})"));
                        }
                        result
                    }
                },
                // One asked for by name runs, or the caller is told why not.
                // A schedule's workflow that no longer takes its cron is
                // passed over: nobody asked.
                Ok(Outcome::Skip(why) | Outcome::Unchanged { why, .. })
                    if ev.pipeline.is_some() && ev.kind != "schedule" =>
                {
                    Ran::Refused(format!("asked for by name, and {why}"))
                }
                Ok(Outcome::Skip(why)) => Ran::Skipped(why),
                Ok(Outcome::Unchanged { why, jobs }) => Ran::Unchanged { why, jobs },
                // The changed files were asked for above; the compiler never
                // asks twice.
                Ok(Outcome::NeedsChangedFiles) => {
                    Ran::Refused("the compiler asked for the changed files twice".into())
                }
                Err(why) => Ran::Refused(why),
            };
            ran.push((path.as_str(), result));
        }
        // An event that ends here for good says, on the commit, which of its
        // pipelines did not run and why. One that will be handled again says
        // nothing yet: it would say it at every turn. A retry on the event's
        // last attempt is an end too: the worker fails it, and there is no
        // later turn to say it at.
        let ends = match finish(&ran) {
            Finish::Failed(_) => true,
            Finish::Retry { .. } => self.max_attempts.is_some_and(|max| ev.attempts >= max),
            _ => false,
        };
        if ends {
            for (path, result) in &ran {
                if let Ran::Refused(why) = result {
                    audit_trail::record(self.audit.as_ref(), || {
                        audit_trail::Entry::new(audit_trail::kind::PIPELINE_REFUSED)
                            .actor(ev.actor.as_deref())
                            .repo(&ev.repo.forge_host, &ev.repo.full_name)
                            .with("pipeline", *path)
                            .with("event_id", ev.id.as_str())
                            .with("event", ev.kind.as_str())
                            .with("ref", ev.git_ref.as_str())
                            .with("sha", ev.sha.as_str())
                            .with("trust", ev.trust.as_str())
                            .with("why", why.as_str())
                    });
                }
            }
        }
        if ends && says {
            let mut later = None;
            for (path, result) in &mut ran {
                let Ran::Refused(why) = result else {
                    continue;
                };
                match self.say_not_run(ev, path, why).await {
                    Said::Posted => why.push_str("; the commit says so"),
                    Said::Cannot(refusal) => {
                        why.push_str(&format!("; no status says so: {refusal}"));
                    }
                    Said::NotAsked => {}
                    // A rate limit holds the rest as well: they are said
                    // when it is over.
                    Said::Later(finish @ Finish::Wait { .. }) => return finish,
                    // One that could not be posted does not keep the others
                    // from being said: on an event's last attempt there is
                    // no later turn for them. The event is handled again for
                    // this one.
                    Said::Later(finish) => {
                        later.get_or_insert(finish);
                    }
                }
            }
            if let Some(finish) = later {
                return finish;
            }
        }
        // And one that was skipped for its paths says so, once the event is
        // settled: not at each turn of one that is handled again, when its
        // other pipelines' runs are not all submitted yet.
        let settled =
            says && (ends || matches!(finish(&ran), Finish::Done(_) | Finish::Skipped(_)));
        if settled {
            let mut later = None;
            for (path, result) in &mut ran {
                let Ran::Unchanged { why, jobs } = result else {
                    continue;
                };
                match self.say_skipped(ev, path, why, jobs).await {
                    Said::Posted => why.push_str("; the commit says so"),
                    Said::Cannot(refusal) => {
                        why.push_str(&format!("; no status says so: {refusal}"));
                    }
                    Said::NotAsked => {}
                    Said::Later(finish @ Finish::Wait { .. }) => return finish,
                    Said::Later(finish) => {
                        later.get_or_insert(finish);
                    }
                }
            }
            if let Some(finish) = later {
                return finish;
            }
            // And the jobs a run left out, once it is made (a workflow's for
            // their `if:`, any pipeline's for their `when:`): a passing status
            // each, under the job's own context, that says it was skipped and
            // why.
            for (path, result) in &ran {
                if !matches!(result, Ran::Submitted(_) | Ran::Recorded(_)) {
                    continue;
                }
                let Some(jobs) = left_out.get(*path).filter(|jobs| !jobs.is_empty()) else {
                    continue;
                };
                if let Some(finish) = self.say_left_out(ev, path, jobs).await {
                    return finish;
                }
            }
        }
        finish(&ran)
    }

    /// Say on the event's commit which jobs of the workflow or pipeline at
    /// `path` this event left out (`jobs`, each with why): a `success` status
    /// for each, under the context the job's own status would have had, which
    /// says it was skipped. As GitHub shows such a job, and so that a branch
    /// that requires its check is not left waiting on a job with nothing to
    /// run. `Some` where the event is to be handled again for it.
    ///
    /// Not under a context another event's run of the workflow has at this
    /// commit. Two events of one commit can leave different jobs out (a job
    /// with `if: github.ref == 'refs/heads/main'`, or `when: "{{ ci_ref }} ==
    /// refs/heads/main"`, and a second branch at the commit), a commit's
    /// statuses are read by every branch and pull request it is the head of,
    /// and this event's passing "skipped" must not stand over a job that
    /// failed in the other's run.
    ///
    /// As in [`Self::say_skipped`], looking and then posting is two steps:
    /// a run can post between them, and one just recorded has posted nothing
    /// yet. Each status goes up under a lease on its context, which a run
    /// that posts under it waits out ([`Self::post_skipped`]): the run's
    /// word is the later, and stands. And after the last post the other
    /// events' runs are told to post their statuses afresh
    /// ([`Db::report_pipeline_again`]), for a run whose record says it
    /// posted under a context it had no need to own then. They are told so
    /// even where nothing was posted here: an earlier handling of the event
    /// may have, before it was cut short. And where a post could not be
    /// made: one before it may have been, and an event on its last attempt
    /// is not handled again. This event's own run is not among them, and
    /// need not be: it has none of these jobs.
    async fn say_left_out(
        &self,
        ev: &Claimed,
        path: &str,
        jobs: &[(String, String)],
    ) -> Option<Finish> {
        if !self.forge.posts_statuses(&ev.repo) {
            return None;
        }
        let kind = match ev.kind.as_str() {
            "merge_group" => "pull_request",
            kind => kind,
        };
        let also = match kind {
            "pull_request" => "merge_group",
            kind => kind,
        };
        let again = |what: &str, e: sqlx::Error| {
            Some(Finish::Retry {
                reason: format!("{what} ci_runs: {e}"),
                after: Duration::ZERO,
            })
        };
        let theirs = match self
            .db
            .posted_by_others(&ev.repo.id, path, &ev.sha, kind, also, &ev.id)
            .await
        {
            Ok(theirs) => theirs,
            Err(e) => return again("reading", e),
        };
        let pipeline = crate::report::pipeline_name(path);
        let mut later = None;
        for (job, why) in jobs {
            let named = crate::report::status_kind(&ev.repo, kind);
            if theirs.contains(&format!("dagron-ci/{pipeline}/{job} ({named})")) {
                continue;
            }
            let one = std::slice::from_ref(job);
            if let Said::Later(finish) = self.post_skipped(ev, path, why, one, kind).await {
                later = Some(finish);
                break;
            }
        }
        let told = self
            .db
            .report_pipeline_again(&ev.repo.id, path, &ev.sha, kind, also, Some(&ev.id))
            .await;
        match (later, told) {
            // To be handled again for the post that could not be made.
            (Some(finish), _) => Some(finish),
            (None, Ok(_)) => None,
            (None, Err(e)) => again("writing", e),
        }
    }

    /// Say on the event's commit that the pipeline at `path` was skipped for
    /// its paths: a `success` status for each of its jobs, under the context
    /// the job's own status would have had, which says it was skipped and
    /// `why`. A branch that requires one of those checks is then not left
    /// waiting on a pipeline that had nothing to run for.
    ///
    /// The jobs are the ones the pipeline names. One that fans out at run
    /// time has a status for its name, and none for instances nobody made.
    ///
    /// Not where the pipeline has a run of its own at this commit, from an
    /// event that changed what its paths name. A commit's statuses are read
    /// by every branch and pull request it is the head of, and this event's
    /// passing "skipped" must not stand over a job that failed there.
    ///
    /// Looking for such a run and then posting is two steps, and a run can be
    /// recorded between them. Each status goes up under a lease on its
    /// context, which the run waits out before it posts under it
    /// ([`Self::post_skipped`]), so the run's are the later. And the run is
    /// looked for again after the last post, and each time it is found it is
    /// told to post its own statuses afresh ([`Db::report_pipeline_again`]).
    async fn say_skipped(&self, ev: &Claimed, path: &str, why: &str, jobs: &[String]) -> Said {
        if !self.forge.posts_statuses(&ev.repo) {
            return Said::NotAsked;
        }
        let kind = match ev.kind.as_str() {
            "merge_group" => "pull_request",
            kind => kind,
        };
        let also = match kind {
            "pull_request" => "merge_group",
            kind => kind,
        };
        let own = || {
            self.db
                .report_pipeline_again(&ev.repo.id, path, &ev.sha, kind, also, None)
        };
        let unwritten = |e: sqlx::Error| {
            Said::Later(Finish::Retry {
                reason: format!("writing ci_runs: {e}"),
                after: Duration::ZERO,
            })
        };
        // Told to post again even here, where this handling has posted
        // nothing: an earlier one of the event may have, before it was cut
        // short.
        match own().await {
            Ok(0) => {}
            Ok(_) => {
                return Said::Cannot(
                    "it has a run of its own at this commit, whose checks stand".into(),
                )
            }
            Err(e) => return unwritten(e),
        }
        let said = self.post_skipped(ev, path, why, jobs, kind).await;
        match (own().await, said) {
            (Ok(0), said) => said,
            // To be handled again, and looked for again then.
            (_, later @ Said::Later(_)) => later,
            (Ok(_), _) => Said::Cannot(
                "a run of its own started at this commit meanwhile: its checks are posted \
                 again, and stand"
                    .into(),
            ),
            (Err(e), _) => unwritten(e),
        }
    }

    /// Post the `success` status of each of a skipped pipeline's jobs: a
    /// check run that is over, where the repository's jobs are check runs
    /// ([`Forge::post_skipped_job`]).
    ///
    /// Each under a lease on its context ([`Db::lease_context`]), and none
    /// under one a run owns: a run of the job posts under it only once it
    /// owns it, which waits the lease out, so what the run says is the later
    /// of the two and stands. One whose lease is not had is not posted: a
    /// run has it, or another event is saying it skipped.
    async fn post_skipped(
        &self,
        ev: &Claimed,
        path: &str,
        why: &str,
        jobs: &[String],
        kind: &str,
    ) -> Said {
        let pipeline = crate::report::pipeline_name(path);
        let named = crate::report::status_kind(&ev.repo, kind);
        let count = |outcome| {
            if let Some(metrics) = &self.metrics {
                metrics.commit_status(outcome);
            }
        };
        let mut cannot = None;
        let (mut posted_any, mut taken) = (false, false);
        for job in jobs {
            let status = CommitStatus {
                state: "success",
                context: format!("dagron-ci/{pipeline}/{job} ({named})"),
                description: crate::report::clamp(&format!("skipped: {why}")),
                target_url: None,
                pipeline_ref: crate::forge::gitlab::status_ref(
                    &ev.git_ref,
                    ev.head_ref.as_deref(),
                    &ev.trust,
                ),
            };
            let leased = self
                .db
                .lease_context(&ev.repo.id, &ev.sha, &status.context, &ev.id, SKIPPED_LEASE)
                .await;
            match leased {
                Ok(true) => {}
                Ok(false) => {
                    taken = true;
                    continue;
                }
                Err(e) => {
                    return Said::Later(Finish::Retry {
                        reason: format!("writing ci_contexts: {e}"),
                        after: Duration::ZERO,
                    })
                }
            }
            let posted = self
                .forge
                .post_skipped_job(&ev.repo, &ev.sha, &status)
                .await;
            // Given back as the post is answered, however it was: a run
            // waiting to own the context posts after it. One not given back
            // lapses.
            if let Err(e) = self
                .db
                .release_context(&ev.repo.id, &ev.sha, &status.context, &ev.id)
                .await
            {
                tracing::warn!(
                    context = %status.context,
                    "a skipped status's lease on its context was not given back, and lapses: {e}"
                );
            }
            self.log_post(ev, &status, &posted).await;
            match posted {
                Ok(()) => {
                    posted_any = true;
                    count(StatusOutcome::Posted)
                }
                Err(StatusError::RateLimited { wait, why }) => {
                    count(StatusOutcome::RateLimited);
                    return Said::Later(Finish::Wait {
                        reason: why,
                        after: wait.clamp(Duration::from_secs(1), MAX_RATE_LIMIT_WAIT),
                    });
                }
                Err(StatusError::Failed(why)) => {
                    count(StatusOutcome::Failed);
                    return Said::Later(Finish::Retry {
                        reason: why,
                        after: Duration::ZERO,
                    });
                }
                // The repository's, or this one job's: the others are still
                // said, and the event keeps the reason.
                Err(StatusError::Refused(why) | StatusError::ThisStatus(why)) => {
                    count(StatusOutcome::Refused);
                    cannot.get_or_insert(why);
                }
            }
        }
        match cannot {
            Some(why) => Said::Cannot(why),
            None if taken && !posted_any => Said::Cannot(
                "its checks at this commit are a run's own, or another event's saying them \
                 skipped"
                    .into(),
            ),
            None => Said::Posted,
        }
    }

    /// Say on the event's commit that the pipeline at `path` was not run,
    /// and `why`: one `error` status under the pipeline's own name, where
    /// its jobs' statuses would have been (`dagron-ci/{pipeline} ({event})`,
    /// which is no job's context). A pipeline that is invalid has no job to
    /// name, and one dagron-api refused has none that ran.
    async fn say_not_run(&self, ev: &Claimed, path: &str, why: &str) -> Said {
        if !self.forge.posts_statuses(&ev.repo) {
            return Said::NotAsked;
        }
        // As the reporter names them: a merge group's checks by its pull
        // requests', and a merge request's as GitLab calls it.
        let kind = crate::report::status_kind(&ev.repo, ev.kind.as_str());
        let status = CommitStatus {
            state: "error",
            context: format!("dagron-ci/{} ({kind})", crate::report::pipeline_name(path)),
            description: crate::report::clamp(&format!("not run: {why}")),
            target_url: None,
            pipeline_ref: crate::forge::gitlab::status_ref(
                &ev.git_ref,
                ev.head_ref.as_deref(),
                &ev.trust,
            ),
        };
        let count = |outcome| {
            if let Some(metrics) = &self.metrics {
                metrics.commit_status(outcome);
            }
        };
        let posted = self.forge.post_status(&ev.repo, &ev.sha, &status).await;
        self.log_post(ev, &status, &posted).await;
        match posted {
            Ok(()) => {
                count(StatusOutcome::Posted);
                Said::Posted
            }
            Err(StatusError::RateLimited { wait, why }) => {
                count(StatusOutcome::RateLimited);
                Said::Later(Finish::Wait {
                    reason: why,
                    after: wait.clamp(Duration::from_secs(1), MAX_RATE_LIMIT_WAIT),
                })
            }
            Err(StatusError::Failed(why)) => {
                count(StatusOutcome::Failed);
                Said::Later(Finish::Retry {
                    reason: why,
                    after: Duration::ZERO,
                })
            }
            Err(StatusError::Refused(why) | StatusError::ThisStatus(why)) => {
                count(StatusOutcome::Refused);
                Said::Cannot(why)
            }
        }
    }

    /// Submit one compiled pipeline, unless its run is recorded already.
    /// The build's word on one compiled run ([`Governance::review`]): `Err`
    /// is why the pipeline is not run. Without rules, every run may be.
    fn review(&self, ev: &Claimed, path: &str, compiled: &Compiled) -> Result<(), String> {
        let Some(governance) = &self.governance else {
            return Ok(());
        };
        let repo = format!("{}/{}", ev.repo.forge_host, ev.repo.full_name);
        // As the run's record will say it: a patch bundle's run is untrusted,
        // whoever asked (`trigger`).
        let trust = if ev.bundle_id.is_some() {
            "untrusted"
        } else {
            ev.trust.as_str()
        };
        governance
            .review(&Proposed {
                repo: &repo,
                pipeline: path,
                kind: &ev.kind,
                git_ref: &ev.git_ref,
                sha: &ev.sha,
                trust,
                actor: ev.actor.as_deref(),
                pr: ev.pr,
                yaml: &compiled.yaml,
            })
            .map_err(|why| format!("refused by this installation's policy: {why}"))
    }

    async fn run_pipeline(
        &self,
        ev: &Claimed,
        base: &str,
        path: &str,
        compiled: &Compiled,
        on_failed: bool,
    ) -> Ran {
        let key = idempotency_key(&ev.commit_key, path);
        match self.db.find_run(&key).await {
            Ok(Some(run_id)) => return Ran::Recorded(run_id),
            Ok(None) => {}
            Err(e) => return Ran::Retry(format!("reading ci_runs: {e}")),
        }
        match self.admit(&key, ev).await {
            Admitted::Yes => {}
            Admitted::Wait(why) => return Ran::Waiting(why),
            Admitted::Retry(why) => return Ran::Retry(why),
        }
        let body = serde_json::json!({
            "yaml": compiled.yaml,
            "parameters": compiled.parameters,
        });
        let body = self.marked(base, ev, body).await;
        let answer = self.submit(&format!("{base}/api/runs"), &key, &body).await;
        self.settle(&key, &answer).await;
        match answer {
            Answer::Run(run_id) => match self.record(ev, path, &key, &run_id, on_failed).await {
                Ok(()) => Ran::Submitted(run_id),
                Err(why) => Ran::Retry(why),
            },
            Answer::Refused(why) => Ran::Refused(why),
            Answer::Retry(why) => Ran::Retry(why),
            Answer::Waiting(why) => Ran::Waiting(why),
        }
    }

    /// The jobs of `compiled` its run `run` left out for their `when:`: those
    /// dagron made no task of, as the run's graph shows them, which it has
    /// from the moment it is made. Nothing where no job has a `when:`, or the
    /// commit takes no statuses, or the graph is not read: a job left out is
    /// then said nothing of, as before, and the run is not held back for it.
    async fn left_by_when(
        &self,
        ev: &Claimed,
        base: &str,
        run: &str,
        compiled: &Compiled,
    ) -> Vec<String> {
        if !compile::can_leave_out(compiled) || !self.forge.posts_statuses(&ev.repo) {
            return Vec::new();
        }
        #[derive(serde::Deserialize)]
        struct Graph {
            nodes: Vec<Node>,
        }
        #[derive(serde::Deserialize)]
        struct Node {
            name: String,
        }
        let url = format!("{base}/api/runs/{run}/graph");
        let read = async {
            let response = self
                .http
                .to(&url)
                .get(&url)
                .bearer_auth(&self.token)
                .timeout(Duration::from_secs(10))
                .send()
                .await
                .map_err(|e| e.to_string())?;
            match response.status().as_u16() {
                200 => response.json::<Graph>().await.map_err(|e| e.to_string()),
                code => Err(format!("HTTP {code}")),
            }
        };
        match read.await {
            Ok(graph) => {
                let made: Vec<String> = graph.nodes.into_iter().map(|node| node.name).collect();
                compile::left_out(compiled, &made)
            }
            Err(why) => {
                tracing::warn!(
                    run_id = %run,
                    "the jobs its `when:` left out are not said skipped: reading {url}: {why}"
                );
                Vec::new()
            }
        }
    }

    /// Take a slot for the run under `key`, of the event's repository and its
    /// caller, when the build limits runs in flight.
    async fn admit(&self, key: &str, ev: &Claimed) -> Admitted {
        let Some(admission) = &self.admission else {
            return Admitted::Yes;
        };
        let repo = format!("{}/{}", ev.repo.forge_host, ev.repo.full_name);
        let caller = ev.actor.as_deref();
        match self
            .db
            .admit_for(
                key,
                Some(&repo),
                caller,
                admission::HOLD,
                admission.by_repository(),
                admission.by_caller(),
                |load| admission.admit_caller(&repo, caller, &ev.trust, load),
            )
            .await
        {
            Ok(Ok(())) => Admitted::Yes,
            Ok(Err(why)) => Admitted::Wait(why),
            Err(e) => Admitted::Retry(format!("taking an admission slot: {e}")),
        }
    }

    /// Let go of the run's slot when dagron-api surely did not create it: it
    /// refused the run, or made it wait its turn. After any other answer the
    /// run may exist, so the slot is kept until the run is recorded, or it
    /// lapses.
    async fn settle(&self, key: &str, answer: &Answer) {
        if self.admission.is_none() || !matches!(answer, Answer::Refused(_) | Answer::Waiting(_)) {
            return;
        }
        if let Err(e) = self.db.release_admission(key).await {
            tracing::warn!(error = %e, "an admission slot was not let go of; it lapses on its own");
        }
    }

    /// The files the event changed, for a paths filter: from the forge,
    /// comparing the event's commit with its base (its compare API, or a
    /// shallow fetch of the two where that cannot list them). An event with
    /// no base (a new branch) has none to list.
    async fn changes(&self, ev: &Claimed) -> Result<Changes, Finish> {
        let Some(base) = ev.base_sha.as_deref() else {
            return Ok(Changes::Unlisted(match ev.kind.as_str() {
                "push" => "a new branch has no earlier commit to compare with".into(),
                kind => format!("a {kind} event names no commit to compare with"),
            }));
        };
        self.forge
            .changed_files(&ev.repo, base, &ev.sha)
            .await
            .map_err(forge_finish)
    }

    /// Record a run dagron-api created (or gave back) under `key`, and tell
    /// the meter, if there is one, once it is recorded.
    async fn record(
        &self,
        ev: &Claimed,
        pipeline: &str,
        key: &str,
        run_id: &str,
        on_failed: bool,
    ) -> Result<(), String> {
        let run = SubmittedRun {
            run_id: run_id.to_string(),
            event_id: ev.id.clone(),
            repo_id: ev.repo.id.clone(),
            cell: ev.repo.cell.clone(),
            pipeline: pipeline.to_string(),
            idempotency_key: key.to_string(),
            on_failed,
        };
        self.db
            .record_run(&run)
            .await
            .map_err(|e| format!("run {run_id} was submitted, recording it failed: {e}"))?;
        audit_trail::record(self.audit.as_ref(), || {
            audit_trail::Entry::new(audit_trail::kind::RUN_SUBMITTED)
                .actor(ev.actor.as_deref())
                .repo(&ev.repo.forge_host, &ev.repo.full_name)
                .with("run_id", run_id)
                .with("cell", ev.repo.cell.as_str())
                .with("pipeline", pipeline)
                .with("event_id", ev.id.as_str())
                .with("event", ev.kind.as_str())
                .with("ref", ev.git_ref.as_str())
                .with("sha", ev.sha.as_str())
                .with("trust", ev.trust.as_str())
        });
        if let Some(meter) = &self.meter {
            let metered = MeteredRun {
                run_id: run_id.to_string(),
                cell: ev.repo.cell.clone(),
                repo: format!("{}/{}", ev.repo.forge_host, ev.repo.full_name),
                pipeline: pipeline.to_string(),
            };
            // Best effort: the meter still hears how the run ends.
            match tokio::time::timeout(METER_TIMEOUT, meter.submitted(&metered)).await {
                Ok(Ok(())) => {}
                Ok(Err(why)) => {
                    tracing::warn!(run_id, error = %why, "the meter refused a submitted run")
                }
                Err(_) => tracing::warn!(run_id, "the meter did not take a submitted run in time"),
            }
        }
        Ok(())
    }

    /// POST `body` to dagron-api at `url` under the key, and read its answer.
    async fn submit(&self, url: &str, key: &str, body: &serde_json::Value) -> Answer {
        self.submit_made(url, key, body).await.0
    }

    /// [`Dispatcher::submit`], with whether the run was made by this submit
    /// (`201`) and not given back for a key seen before (`200`).
    async fn submit_made(&self, url: &str, key: &str, body: &serde_json::Value) -> (Answer, bool) {
        let made = std::sync::atomic::AtomicBool::new(false);
        let answer = self.submit_as(url, key, body, &made).await;
        (answer, made.load(std::sync::atomic::Ordering::Relaxed))
    }

    async fn submit_as(
        &self,
        url: &str,
        key: &str,
        body: &serde_json::Value,
        made: &std::sync::atomic::AtomicBool,
    ) -> Answer {
        let response = match self
            .http
            .to(url)
            .post(url)
            .bearer_auth(&self.token)
            .header("Idempotency-Key", key)
            .json(body)
            .send()
            .await
        {
            Ok(response) => response,
            Err(e) => return Answer::Retry(format!("dagron-api at {url}: {e}")),
        };
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        let excerpt = excerpt(&body);
        made.store(status.as_u16() == 201, std::sync::atomic::Ordering::Relaxed);
        match status.as_u16() {
            200 | 201 => match serde_json::from_str::<serde_json::Value>(&body)
                .ok()
                .and_then(|v| v["run_id"].as_str().map(str::to_string))
            {
                Some(run_id) => Answer::Run(run_id),
                None => Answer::Retry(format!("dagron-api answered {status} without a run id")),
            },
            // The run is not for this event to make: the spec does not validate,
            // or a parameter is refused. Retrying changes nothing.
            400 => Answer::Refused(format!("dagron-api refused the run: {excerpt}")),
            // An environment the pipeline names does not admit this run: its
            // policy's answer, which asking again does not change. It is not
            // dagron-ci's token that was refused.
            403 if body.contains(POLICY_REFUSAL) => {
                Answer::Refused(format!("dagron-api refused the run: {excerpt}"))
            }
            // A run that is not trusted named what such a run may not use:
            // the pipeline's to change, and likewise no fault of the token.
            403 if body.contains(CONFINED_REFUSAL) => {
                Answer::Refused(format!("dagron-api refused the run: {excerpt}"))
            }
            // A token problem is the operator's to fix, and events keep until
            // then: retried with backoff, failed once attempts run out.
            401 | 403 => Answer::Retry(format!(
                "dagron-api answered {status} to dagron-ci's token (CI_API_TOKEN_FILE)"
            )),
            404 => Answer::Refused(format!("dagron-api at {url} answered 404: {excerpt}")),
            // An identical submit still in flight: its run comes back on retry.
            409 if body.contains("in flight") || body.contains("raced") => {
                Answer::Retry(format!("dagron-api: {excerpt}"))
            }
            // A paused or retired workflow, or the key used for other parameters.
            409 => Answer::Refused(format!("dagron-api: {excerpt}")),
            // The run's concurrency key has `max_active_runs` runs active: it
            // waits its turn.
            429 if body.contains("max_active_runs") => Answer::Waiting(excerpt),
            _ => Answer::Retry(format!("dagron-api answered {status}: {excerpt}")),
        }
    }
}

impl Dispatcher {
    /// Leave an event that is refused for `why`, and say so where the person
    /// who opened the pull request will look: one status on its head commit,
    /// `error`, under [`REFUSED_CONTEXT`]. Never one that passes, and nothing
    /// of the pull request is read to post it.
    ///
    /// - Posted: the event is skipped, and its reason says the commit says so.
    /// - GitHub's rate limit is spent: the event waits it out, and posts then.
    /// - GitHub could not take it: the event is tried again.
    /// - GitHub refuses it, and would again (the token, the repository, the
    ///   commit): no status can say so, and the event's own reason does.
    ///
    /// Each event of a refused pull request says it, under the one context: a
    /// webhook and a sweep that both see a head post it twice, and the commit
    /// shows one. Only a pull request has a head whose author is waiting, and
    /// only a repository whose commits take statuses is told
    /// ([`Forge::posts_statuses`]); any other refused event is skipped as it
    /// was.
    async fn refused(&self, ev: &Claimed, why: String, description: &str) -> Finish {
        let status = CommitStatus {
            state: "error",
            context: REFUSED_CONTEXT.into(),
            description: description.into(),
            target_url: None,
            pipeline_ref: crate::forge::gitlab::status_ref(
                &ev.git_ref,
                ev.head_ref.as_deref(),
                &ev.trust,
            ),
        };
        match self.say(ev, &status).await {
            Ok(Some(())) => Finish::Skipped(format!(
                "{why}; its head commit says so, under {REFUSED_CONTEXT:?}"
            )),
            Ok(None) => Finish::Skipped(why),
            Err(Unsaid::Never(refusal)) => {
                Finish::Skipped(format!("{why}; no status says so: {refusal}"))
            }
            Err(Unsaid::Later(finish)) => finish,
        }
    }

    /// Post `status` on the head of the pull request `ev` is for.
    /// `Ok(None)` where nobody is told: the event is no pull request's, or
    /// its repository's commits take no statuses.
    async fn say(&self, ev: &Claimed, status: &CommitStatus) -> Result<Option<()>, Unsaid> {
        if ev.kind != "pull_request" || !self.forge.posts_statuses(&ev.repo) {
            return Ok(None);
        }
        let count = |outcome| {
            if let Some(metrics) = &self.metrics {
                metrics.commit_status(outcome);
            }
        };
        let posted = self.forge.post_status(&ev.repo, &ev.sha, status).await;
        self.log_post(ev, status, &posted).await;
        match posted {
            Ok(()) => {
                count(StatusOutcome::Posted);
                Ok(Some(()))
            }
            Err(StatusError::RateLimited { wait, why: limit }) => {
                count(StatusOutcome::RateLimited);
                Err(Unsaid::Later(Finish::Wait {
                    reason: limit,
                    after: wait.clamp(Duration::from_secs(1), MAX_RATE_LIMIT_WAIT),
                }))
            }
            Err(StatusError::Failed(failed)) => {
                count(StatusOutcome::Failed);
                Err(Unsaid::Later(Finish::Retry {
                    reason: failed,
                    after: Duration::ZERO,
                }))
            }
            Err(StatusError::Refused(refusal) | StatusError::ThisStatus(refusal)) => {
                count(StatusOutcome::Refused);
                Err(Unsaid::Never(refusal))
            }
        }
    }

    /// What stands in front of an event that is not trusted, where such
    /// events are run. `Ok` once its pipelines may be submitted; `Err` is
    /// how the event is left until then, or for good.
    ///
    /// - The cell records what started a run. Nothing else would say the run
    ///   is untrusted, and dagron holds it by that alone: on a cell that does
    ///   not, the pull request is refused, and its head says so.
    /// - The approval, where the pull request needs one
    ///   ([`crate::forks::Forks::gated`]).
    async fn untrusted(
        &self,
        ev: &Claimed,
        base: &str,
        forks: &crate::forks::Forks,
    ) -> Result<(), Finish> {
        if !self.records(ev, base).await? {
            let why = format!(
                "a {} event from a fork or a non-member is not run on cell {:?}: its \
                 dagron-api takes no record of what started a run from dagron-ci's token \
                 (the `{TRIGGER_GROUP}` group), so nothing would say the run is untrusted",
                ev.kind, ev.repo.cell
            );
            return Err(self.refused(ev, why, UNRECORDED_DESCRIPTION).await);
        }
        if forks.gated(ev.association.as_deref()) {
            self.approval(ev, base, forks).await?;
        }
        Ok(())
    }

    /// A merge-queue group the queue let go of without merging it
    /// (`merge_group` `destroyed`, invalidated or dequeued; a GitLab merge
    /// train's pipeline cancelled, as the train drops it or makes it again).
    /// Nobody waits on its checks now: its events still waiting are
    /// withdrawn, and its runs not seen to end are cancelled through their
    /// cells, as dagron-ci. Its jobs' checks then say they were cancelled. A
    /// group is its ref and its head: a merge train's ref is its merge
    /// request's, and a pipeline the train made again of it is not this one.
    ///
    /// A worker that holds one of the group's events may submit a run after
    /// this has looked: the group is looked at again once it is through.
    async fn drop_group(&self, ev: &Claimed) -> Finish {
        let again = |reason: String| Finish::Retry {
            reason,
            after: Duration::from_secs(5),
        };
        let events = match self
            .db
            .group_events(&ev.repo.id, &ev.git_ref, &ev.sha)
            .await
        {
            Ok(events) => events,
            Err(e) => return again(format!("reading ci_events: {e}")),
        };
        if events.iter().any(|(_, state)| state == "claimed") {
            return again("a worker is dispatching the group's checks now".into());
        }
        let queue = if crate::forge::gitlab::is_gitlab(&ev.repo) {
            "merge train"
        } else {
            "merge queue"
        };
        let why = format!(
            "the {queue} let go of {} at {} without merging it",
            ev.git_ref,
            short(&ev.sha)
        );
        let mut withdrawn = 0;
        for (id, state) in &events {
            if state != "new" {
                continue;
            }
            match self.db.withdraw_event(id, &why).await {
                Ok(true) => withdrawn += 1,
                Ok(false) => {}
                Err(e) => return again(format!("writing ci_events: {e}")),
            }
        }
        let runs = match self.db.group_runs(&ev.repo.id, &ev.git_ref, &ev.sha).await {
            Ok(runs) => runs,
            Err(e) => return again(format!("reading ci_runs: {e}")),
        };
        // A run that cannot be cancelled keeps none of the others from being:
        // each is asked, and what could not be is said once all have been.
        let mut cancelled = 0;
        let mut left: Vec<String> = Vec::new();
        for (run_id, cell) in &runs {
            let Some(base) = self.cells.get(cell) else {
                left.push(format!("run {run_id}: cell {cell:?} is not in CI_CELLS"));
                continue;
            };
            match crate::api::cancel::ask(self.http.to(base), base, run_id, &self.token).await {
                Ok(crate::api::cancel::Stopped::Cancelled { .. }) => cancelled += 1,
                Ok(crate::api::cancel::Stopped::Nothing | crate::api::cancel::Stopped::Gone) => {}
                Ok(crate::api::cancel::Stopped::Denied(code)) => left.push(format!(
                    "run {run_id}: dagron-api refused dagron-ci's token its cancel (HTTP {code})"
                )),
                Err(e) => return again(e),
            }
        }
        if !left.is_empty() {
            return Finish::Failed(format!(
                "{why}: {cancelled} run(s) cancelled, {withdrawn} event(s) withdrawn, and not \
                 cancelled: {}",
                left.join("; ")
            ));
        }
        if withdrawn == 0 && cancelled == 0 {
            return Finish::Skipped(format!("{why}: nothing of it was going"));
        }
        Finish::Done(format!(
            "{why}: {cancelled} run(s) cancelled, {withdrawn} event(s) withdrawn"
        ))
    }

    /// Whether the event's cell takes the record of what started a run from
    /// dagron-ci's token, which is what holds a run that is not trusted to
    /// its floor. `Err` to try again, where the cell could not be asked.
    async fn records(&self, ev: &Claimed, base: &str) -> Result<bool, Finish> {
        self.takes_record(base).await.ok_or_else(|| Finish::Retry {
            reason: format!(
                "cell {:?} could not be asked whether it records what started a run",
                ev.repo.cell
            ),
            after: Duration::ZERO,
        })
    }

    /// The approval in front of a pull request that needs one: a run of its
    /// own, of one approval task that dagron-ci writes
    /// ([`crate::forks::Forks::gate_workflow`]). `Ok` once it has succeeded.
    ///
    /// Nothing is kept of it here. The run is found again by its key, which
    /// is the commit's and the workflow's. The workflow is the same for a
    /// commit whichever door its event came through, so two events for one
    /// head wait on one approval; and an operator who changes who may
    /// approve asks again, of them.
    ///
    /// The pull request's head says where it stands, under
    /// [`crate::forks::GATE_CONTEXT`]: `pending` when the approval is asked
    /// for, and then how it ended.
    async fn approval(
        &self,
        ev: &Claimed,
        base: &str,
        forks: &crate::forks::Forks,
    ) -> Result<(), Finish> {
        let yaml = forks.gate_workflow(ev);
        let digest = hex::encode(Sha256::digest(yaml.as_bytes()));
        let key = idempotency_key(
            &ev.commit_key,
            &format!("{}:{}", crate::forks::GATE_PIPELINE, &digest[..16]),
        );
        let mut body = serde_json::json!({ "yaml": yaml, "parameters": {} });
        // The approval is this installation's own workflow, every word of it
        // dagron-ci's. What it stands in front of is not.
        let mut record = self.trigger(ev);
        record["trust"] = serde_json::json!("trusted");
        body["trigger"] = record;
        let (answer, made) = self
            .submit_made(&format!("{base}/api/runs"), &key, &body)
            .await;
        let run_id = match answer {
            Answer::Run(run_id) => run_id,
            Answer::Refused(why) => {
                return Err(Finish::Failed(format!(
                    "the approval of this pull request could not be asked for: {why}"
                )))
            }
            Answer::Retry(why) => {
                return Err(Finish::Retry {
                    reason: format!("asking for the approval of this pull request: {why}"),
                    after: Duration::ZERO,
                })
            }
            Answer::Waiting(why) => {
                return Err(Finish::Wait {
                    reason: why,
                    after: CONCURRENCY_WAIT,
                })
            }
        };
        let said = |state: &'static str, description: String| CommitStatus {
            state,
            context: crate::forks::GATE_CONTEXT.into(),
            description,
            target_url: None,
            pipeline_ref: crate::forge::gitlab::status_ref(
                &ev.git_ref,
                ev.head_ref.as_deref(),
                &ev.trust,
            ),
        };
        let number = ev.pr.map(|n| format!(" #{n}")).unwrap_or_default();
        if made {
            // Asked for now. Said once, and not waited on: the approval does
            // not depend on the forge taking a status.
            let pending = said(
                "pending",
                "waiting for an approval: this pull request is from outside the repository".into(),
            );
            if let Err(Unsaid::Later(_) | Unsaid::Never(_)) = self.say(ev, &pending).await {
                tracing::warn!(event_id = %ev.id, run_id, "the approval's pending status was not posted");
            }
        }
        let status = self
            .run_status(base, &run_id)
            .await
            .map_err(|why| Finish::Retry {
                reason: why,
                after: Duration::ZERO,
            })?;
        match status.as_str() {
            "succeeded" => {
                let approved = said(
                    "success",
                    "approved: its pipelines are run as untrusted".into(),
                );
                match self.say(ev, &approved).await {
                    Err(Unsaid::Later(finish)) => Err(finish),
                    _ => Ok(()),
                }
            }
            "failed" | "cancelled" => {
                let why = format!(
                    "pull request{number} was not approved (run {run_id} is {status}): nothing \
                     of it was run"
                );
                let refused = said(
                    "failure",
                    "not approved: nothing of this pull request was run".into(),
                );
                Err(match self.say(ev, &refused).await {
                    Ok(Some(())) => Finish::Skipped(format!(
                        "{why}; its head commit says so, under {:?}",
                        crate::forks::GATE_CONTEXT
                    )),
                    Ok(None) => Finish::Skipped(why),
                    Err(Unsaid::Never(refusal)) => {
                        Finish::Skipped(format!("{why}; no status says so: {refusal}"))
                    }
                    Err(Unsaid::Later(finish)) => finish,
                })
            }
            _ => Err(Finish::Wait {
                reason: format!(
                    "pull request{number} waits for an approval: run {run_id} on cell {}",
                    ev.repo.cell
                ),
                after: APPROVAL_WAIT,
            }),
        }
    }

    /// The status of run `run_id` on the cell at `base`, as dagron-api has
    /// it.
    async fn run_status(&self, base: &str, run_id: &str) -> Result<String, String> {
        let url = format!("{base}/api/runs/{run_id}");
        let response = self
            .http
            .to(base)
            .get(&url)
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| format!("dagron-api at {url}: {e}"))?;
        let status = response.status();
        let body = response.text().await.unwrap_or_default();
        if !status.is_success() {
            return Err(format!(
                "dagron-api answered {status} for run {run_id}: {}",
                excerpt(&body)
            ));
        }
        serde_json::from_str::<serde_json::Value>(&body)
            .ok()
            .and_then(|run| run["status"].as_str().map(str::to_string))
            .ok_or_else(|| format!("dagron-api's answer for run {run_id} names no status"))
    }
}

/// A status that was not posted.
enum Unsaid {
    /// The forge refuses it, and would again: why.
    Never(String),
    /// Not now: how the event is to be handled again, to say it then.
    Later(Finish),
}

impl Handler for Dispatcher {
    fn handle(&self, event: &Claimed) -> impl Future<Output = Finish> + Send {
        self.dispatch(event)
    }
}

/// How an event ends, from what became of each of its pipelines: retried if
/// any could not be submitted, waiting if any waits its turn, failed if any
/// was refused, done if any ran, and skipped when none did. The reason names
/// each pipeline and what became of it.
fn finish(ran: &[(&str, Ran)]) -> Finish {
    let summary = ran
        .iter()
        .map(|(path, ran)| {
            let name = path
                .strip_prefix(PIPELINE_DIR)
                .and_then(|p| p.strip_prefix('/'))
                .unwrap_or(path);
            let what = match ran {
                Ran::Submitted(run) => format!("submitted run {run}"),
                Ran::Recorded(run) => format!("run {run} was submitted for this commit already"),
                Ran::Skipped(why) | Ran::Unchanged { why, .. } => format!("skipped, {why}"),
                Ran::Refused(why) => format!("refused, {why}"),
                Ran::Retry(why) => format!("not submitted, {why}"),
                Ran::Waiting(why) => format!("waiting, {why}"),
            };
            format!("{name}: {what}")
        })
        .collect::<Vec<_>>()
        .join("; ");
    let any = |pick: fn(&Ran) -> bool| ran.iter().any(|(_, r)| pick(r));
    if any(|r| matches!(r, Ran::Retry(_))) {
        Finish::Retry {
            reason: summary,
            after: Duration::ZERO,
        }
    } else if any(|r| matches!(r, Ran::Waiting(_))) {
        Finish::Wait {
            reason: summary,
            after: CONCURRENCY_WAIT,
        }
    } else if any(|r| matches!(r, Ran::Refused(_))) {
        Finish::Failed(summary)
    } else if any(|r| matches!(r, Ran::Submitted(_) | Ran::Recorded(_))) {
        Finish::Done(summary)
    } else {
        Finish::Skipped(summary)
    }
}

/// The pipelines another of `files` waits on the failure of (`on.failed`):
/// only a pipeline of `.dagron/ci` waits on one. One that does not
/// read is refused where it runs, and waits on nothing.
fn listened(files: &[PipelineFile]) -> std::collections::BTreeSet<String> {
    files
        .iter()
        .filter(|f| f.path.starts_with(PIPELINE_DIR))
        .filter_map(|f| f.text.as_ref().ok())
        .filter_map(|text| pipeline::parse(text).ok())
        .filter_map(|p| p.on.failed)
        .flat_map(|f| f.pipelines)
        .collect()
}

/// The pipeline file `name` names: `deploy` or `deploy.yaml` for
/// `.dagron/ci/deploy.yaml`, and likewise a workflow file the repository
/// lists.
fn pick(files: Vec<PipelineFile>, name: &str, sha: &str) -> Result<PipelineFile, String> {
    let mut picked: Vec<PipelineFile> = files
        .into_iter()
        .filter(|f| {
            let file = f
                .path
                .strip_prefix(PIPELINE_DIR)
                .or_else(|| f.path.strip_prefix(github::WORKFLOW_DIR))
                .and_then(|p| p.strip_prefix('/'))
                .unwrap_or(&f.path);
            let stem = file
                .strip_suffix(".yaml")
                .or_else(|| file.strip_suffix(".yml"));
            f.path == name
                || file == name
                || stem == Some(name)
                || crate::report::pipeline_name(&f.path) == name
        })
        .collect();
    match picked.len() {
        1 => Ok(picked.remove(0)),
        0 => Err(format!(
            "{} has no pipeline {name:?} in {PIPELINE_DIR}",
            short(sha)
        )),
        _ => Err(format!(
            "pipeline {name:?} names {}; name the file",
            picked
                .iter()
                .map(|f| f.path.as_str())
                .collect::<Vec<_>>()
                .join(" and ")
        )),
    }
}

/// A pipeline's problems, as one reason: the first, and how many more.
fn invalid(problems: &[pipeline::Problem]) -> String {
    match problems {
        [] => "invalid".into(),
        [only] => format!("invalid: {only}"),
        [first, rest @ ..] => format!(
            "invalid: {first} (and {} more; `dagron-ci validate` lists them)",
            rest.len()
        ),
    }
}

/// How a forge that could not be read leaves the event: a spent rate limit
/// waits for it, anything else is retried.
fn forge_finish(e: ForgeError) -> Finish {
    match e {
        ForgeError::RateLimited { wait, why } => Finish::Wait {
            reason: why,
            after: wait.clamp(Duration::from_secs(1), MAX_RATE_LIMIT_WAIT),
        },
        ForgeError::Failed(why) => Finish::Retry {
            reason: why,
            after: Duration::ZERO,
        },
    }
}

/// The run's `Idempotency-Key`: `ci-` and the SHA-256 of
/// `{commit key}:{pipeline}`. Hashed because a ref can be a kilobyte long and
/// dagron-api takes 255 characters.
pub fn idempotency_key(commit_key: &str, pipeline: &str) -> String {
    let digest = Sha256::digest(format!("{commit_key}:{pipeline}").as_bytes());
    format!("ci-{}", hex::encode(digest))
}

/// The event, as the compiler reads it.
fn compile_event(ev: &Claimed) -> compile::Event {
    compile::Event {
        kind: ev.kind.clone(),
        git_ref: ev.git_ref.clone(),
        sha: ev.sha.clone(),
        base_sha: ev.base_sha.clone(),
        base_ref: ev.base_ref.clone(),
        pr: ev.pr,
        trust: ev.trust.clone(),
        repo: ev.repo.full_name.clone(),
        forge: ev.repo.forge.clone(),
        forge_host: ev.repo.forge_host.clone(),
        inputs: ev.inputs.clone(),
        actor: ev.actor.clone(),
        head_ref: ev.head_ref.clone(),
        default_branch: None,
        run_numbers: None,
        changed: Changes::Unknown,
        payload: None,
    }
}

/// The event, as the bound workflow's parameters: a dispatch's inputs under
/// their own names, then the `ci_*` parameters a compiled pipeline gets too
/// ([`compile::ci_parameters`]). The run API refuses a `ci_` input name, and
/// the `ci_*` values would override one anyway.
///
/// `ci_ref` is a branch name, which its pusher chose, and an input is what its
/// caller typed. Read them in a task's `env:`, never in a command or a
/// `when:`, as the compiled pipelines do.
pub fn parameters(ev: &Claimed) -> BTreeMap<String, String> {
    let mut parameters = ev.inputs.clone();
    parameters.extend(compile::ci_parameters(&compile_event(ev)));
    parameters
}

/// The event, as the trigger record of a run submitted for it: that the run
/// is CI's, and what it is for. dagron-api sets the record's principal itself,
/// from the token.
///
/// A field dagron-api would refuse (over [`TRIGGER_FIELD_MAX`] bytes, or
/// holding a control character) is left out: a record with a gap in it, where
/// the alternative is no run.
pub fn trigger(ev: &Claimed) -> serde_json::Value {
    let fits = |s: &str| s.len() <= TRIGGER_FIELD_MAX && !s.chars().any(char::is_control);
    let text = |s: &str| fits(s).then(|| s.to_string());
    let optional = |s: &Option<String>| s.as_deref().and_then(text);
    // A run on a patch bundle is untrusted to the engine whoever asked: an
    // agent's is recorded as an agent's (CI_AGENT_RUNS still holds), but an
    // environment's policy that names agents gives it nothing.
    let trust = if ev.bundle_id.is_some() {
        "untrusted"
    } else {
        ev.trust.as_str()
    };
    serde_json::json!({
        "kind": "ci",
        "actor": optional(&ev.actor),
        "trust": text(trust),
        "forge_host": text(&ev.repo.forge_host),
        "repo": text(&ev.repo.full_name),
        "ref": text(&ev.git_ref),
        "sha": text(&ev.sha),
        "base_sha": optional(&ev.base_sha),
        "pr": ev.pr,
        "event_id": text(&ev.id),
    })
}

/// A commit id cut to the twelve digits a person reads.
fn short(sha: &str) -> &str {
    &sha[..sha.len().min(12)]
}

fn excerpt(body: &str) -> String {
    let body = body.trim();
    if body.chars().count() <= REASON_EXCERPT {
        return body.to_string();
    }
    let mut cut: String = body.chars().take(REASON_EXCERPT).collect();
    cut.push('…');
    cut
}

#[cfg(test)]
pub(crate) mod tests {
    use std::sync::Mutex;

    use axum::extract::{Path, State};
    use axum::http::{HeaderMap, StatusCode};
    use axum::routing::post;
    use axum::{Json, Router};

    use super::*;
    use crate::db::tests::{event, sqlite};
    use crate::db::Repo;
    use crate::event::{Kind, Trust};
    use crate::forge::tests::{stub_github, StubGithub};

    /// A submit the stub dagron-api answered.
    #[derive(Clone, Debug)]
    pub(crate) struct Submitted {
        /// The stored workflow run, or `None` for `POST /api/runs`.
        pub(crate) workflow: Option<String>,
        /// What `POST /api/runs` was given to run.
        pub(crate) yaml: Option<String>,
        pub(crate) key: String,
        pub(crate) auth: String,
        pub(crate) params: BTreeMap<String, String>,
        /// The trigger record the submit carried, if it carried one.
        pub(crate) trigger: Option<serde_json::Value>,
    }

    pub(crate) type Seen = Arc<Mutex<Vec<Submitted>>>;

    /// A dagron-api that answers each submit with the next of `answers` (the
    /// last one repeating), and replays a key it has seen with 200. A workflow
    /// `POST /api/runs` is given goes through dagron's own parse, as
    /// dagron-api's does, and one dagron refuses is answered 400. The token is
    /// a user's with no groups, so no submit carries a trigger record.
    pub(crate) async fn stub_api(answers: Vec<(u16, &'static str)>) -> (String, Seen) {
        stub_api_as(answers, Some(&[])).await
    }

    /// [`stub_api`], whose `/api/me` says the token's user is in `groups`, or
    /// fails when there are none to say.
    pub(crate) async fn stub_api_as(
        answers: Vec<(u16, &'static str)>,
        groups: Option<&'static [&'static str]>,
    ) -> (String, Seen) {
        #[derive(Clone)]
        struct St {
            seen: Seen,
            answers: Arc<Mutex<Vec<(u16, &'static str)>>>,
            runs: Arc<Mutex<BTreeMap<String, String>>>,
            /// What each run's status is said to be: `running` until a test
            /// says otherwise (`PUT /_stub/runs/{id}/{status}`).
            statuses: Arc<Mutex<BTreeMap<String, String>>>,
            groups: Option<&'static [&'static str]>,
        }
        async fn status_of(State(st): State<St>, Path(id): Path<String>) -> (StatusCode, String) {
            let status = st
                .statuses
                .lock()
                .unwrap()
                .get(&id)
                .cloned()
                .unwrap_or_else(|| "running".to_string());
            (
                StatusCode::OK,
                serde_json::json!({ "id": id, "status": status }).to_string(),
            )
        }
        async fn set_status(
            State(st): State<St>,
            Path((id, status)): Path<(String, String)>,
        ) -> StatusCode {
            st.statuses.lock().unwrap().insert(id, status);
            StatusCode::NO_CONTENT
        }
        /// A cancel, as dagron-api's: one job called off of a run that is
        /// running, and nothing of one that is not.
        async fn cancel(State(st): State<St>, Path(id): Path<String>) -> (StatusCode, String) {
            let mut statuses = st.statuses.lock().unwrap();
            let running = statuses.get(&id).is_none_or(|s| s == "running");
            statuses.insert(id, "cancelled".into());
            let cancelled = u64::from(running);
            (
                StatusCode::OK,
                serde_json::json!({ "cancelled": cancelled, "stopping": 0 }).to_string(),
            )
        }
        async fn me(State(st): State<St>) -> (StatusCode, String) {
            match st.groups {
                Some(groups) => (
                    StatusCode::OK,
                    serde_json::json!({ "sub": "u-ci", "email": "dagron-ci@example.com", "groups": groups })
                        .to_string(),
                ),
                None => (StatusCode::INTERNAL_SERVER_ERROR, String::new()),
            }
        }
        /// A run's tasks, as dagron makes them of what was submitted for it.
        async fn graph(State(st): State<St>, Path(id): Path<String>) -> (StatusCode, String) {
            let runs = st.runs.lock().unwrap().clone();
            let Some(key) = runs.iter().find(|(_, run)| **run == id).map(|(key, _)| key) else {
                return (StatusCode::NOT_FOUND, String::new());
            };
            let seen = st.seen.lock().unwrap();
            let Some(asked) = seen.iter().rev().find(|s| &s.key == key) else {
                return (StatusCode::NOT_FOUND, String::new());
            };
            let names: Vec<String> = match &asked.yaml {
                Some(yaml) => {
                    dagron_core::dag::DagGraph::from_yaml_with_params(yaml, &asked.params)
                        .unwrap()
                        .spec
                        .tasks
                        .iter()
                        .map(|task| task.name.clone())
                        .collect()
                }
                None => Vec::new(),
            };
            let nodes: Vec<serde_json::Value> = names
                .iter()
                .map(|name| serde_json::json!({ "id": name, "name": name, "status": "pending" }))
                .collect();
            (
                StatusCode::OK,
                serde_json::json!({ "nodes": nodes }).to_string(),
            )
        }
        fn trigger_of(body: &serde_json::Value) -> Option<serde_json::Value> {
            body.get("trigger").filter(|t| !t.is_null()).cloned()
        }
        fn answer(st: &St, submitted: Submitted) -> (StatusCode, String) {
            let key = submitted.key.clone();
            st.seen.lock().unwrap().push(submitted);
            if let Some(run) = st.runs.lock().unwrap().get(&key) {
                return (StatusCode::OK, format!(r#"{{"run_id":"{run}"}}"#));
            }
            let (code, text) = {
                let mut answers = st.answers.lock().unwrap();
                if answers.len() > 1 {
                    answers.remove(0)
                } else {
                    answers[0]
                }
            };
            if code == 201 {
                let run = format!("run-{}", st.runs.lock().unwrap().len() + 1);
                st.runs.lock().unwrap().insert(key, run.clone());
                return (StatusCode::CREATED, format!(r#"{{"run_id":"{run}"}}"#));
            }
            (StatusCode::from_u16(code).unwrap(), text.to_string())
        }
        fn header(headers: &HeaderMap, name: &str) -> String {
            headers
                .get(name)
                .and_then(|v| v.to_str().ok())
                .unwrap_or("")
                .to_string()
        }
        async fn workflow_run(
            State(st): State<St>,
            Path(workflow): Path<String>,
            headers: HeaderMap,
            Json(body): Json<serde_json::Value>,
        ) -> (StatusCode, String) {
            let submitted = Submitted {
                workflow: Some(workflow),
                yaml: None,
                key: header(&headers, "idempotency-key"),
                auth: header(&headers, "authorization"),
                params: serde_json::from_value(body["parameters"].clone()).unwrap(),
                trigger: trigger_of(&body),
            };
            answer(&st, submitted)
        }
        async fn run(
            State(st): State<St>,
            headers: HeaderMap,
            Json(body): Json<serde_json::Value>,
        ) -> (StatusCode, String) {
            let yaml = body["yaml"].as_str().unwrap_or_default().to_string();
            let params: BTreeMap<String, String> =
                serde_json::from_value(body["parameters"].clone()).unwrap();
            if let Err(e) = dagron_core::dag::DagGraph::from_yaml_with_params(&yaml, &params) {
                return (StatusCode::BAD_REQUEST, format!("invalid DAG: {e}"));
            }
            let submitted = Submitted {
                workflow: None,
                yaml: Some(yaml),
                key: header(&headers, "idempotency-key"),
                auth: header(&headers, "authorization"),
                params,
                trigger: trigger_of(&body),
            };
            answer(&st, submitted)
        }
        let st = St {
            seen: Arc::default(),
            answers: Arc::new(Mutex::new(answers)),
            runs: Arc::default(),
            statuses: Arc::default(),
            groups,
        };
        let seen = st.seen.clone();
        let app = Router::new()
            .route("/api/workflows/{workflow}/run", post(workflow_run))
            .route("/api/runs", post(run))
            .route("/api/runs/{id}", axum::routing::get(status_of))
            .route("/api/runs/{id}/graph", axum::routing::get(graph))
            .route("/_stub/runs/{id}/{status}", axum::routing::put(set_status))
            .route("/api/runs/{id}/cancel", post(cancel))
            .route("/api/me", axum::routing::get(me))
            .with_state(st);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (base, seen)
    }

    /// A repository bound to `workflow` on the `default` cell, with one queued
    /// event of `kind`/`trust`, claimed.
    pub(crate) async fn claimed(
        db: &Db,
        workflow: Option<&str>,
        kind: Kind,
        trust: Trust,
    ) -> (Repo, Claimed) {
        let repo = db
            .add_repo("github", "github.com", "acme/widgets", workflow, "default")
            .await
            .unwrap()
            .unwrap();
        let mut ev = event();
        ev.kind = kind;
        ev.trust = trust;
        ev.base_sha = Some("c".repeat(40));
        if kind == Kind::PullRequest {
            ev.git_ref = "refs/pull/7/head".into();
            ev.pr = Some(7);
            ev.base_sha = Some("b".repeat(40));
            ev.base_ref = Some("main".into());
        }
        db.record_delivery(&repo, &uuid::Uuid::now_v7().to_string(), &ev, b"{}")
            .await
            .unwrap();
        let c = db
            .claim_event("w", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
        (repo, c)
    }

    /// A dispatcher for the `default` cell at `base`, reading a stub GitHub
    /// that holds no pipelines until a test gives it some.
    pub(crate) async fn dispatcher(db: &Db, base: &str) -> (Dispatcher, StubGithub) {
        let (origin, gh) = stub_github().await;
        let d = Dispatcher::new(
            db.clone(),
            BTreeMap::from([("default".into(), base.into())]),
            "dgr_token".into(),
            Arc::new(Forge::new(None).with_origin(&origin)),
            compile::Settings::default(),
        );
        (d, gh)
    }

    /// Every waiting event due now.
    async fn due_now(db: &Db) {
        match db {
            Db::Sqlite(p) => {
                sqlx::query("UPDATE ci_events SET next_attempt_at = '2000-01-01T00:00:00Z'")
                    .execute(p)
                    .await
                    .map(drop)
            }
            Db::Postgres(p) => sqlx::query("UPDATE ci_events SET next_attempt_at = now()")
                .execute(p)
                .await
                .map(drop),
        }
        .unwrap();
    }

    pub(crate) fn pipelines(gh: &StubGithub, files: &[(&str, &str)]) {
        gh.lock().unwrap().pipelines = Some(
            files
                .iter()
                .map(|(name, text)| (name.to_string(), text.to_string()))
                .collect(),
        );
    }

    const CI: &str = "name: ci\non: { push: { branches: [main] }, pull_request: {} }\n\
                      concurrency: { key: \"{{ ci_group }}\" }\n\
                      tasks:\n  - { name: test, docker_image: \"rust:1\", steps: [{ run: cargo test }] }\n";
    const RELEASE: &str = "name: release\non: { push: { branches: [\"release/*\"] } }\n\
                           tasks:\n  - { name: build, steps: [{ run: make }] }\n";
    const LINT: &str = "name: lint\non: { push: }\n\
                        tasks:\n  - { name: fmt, steps: [{ run: cargo fmt --check }] }\n";

    // ---- pull requests from forks, where they are run ------------------------

    /// Say what the stand-in dagron-api answers for run `id`'s status.
    async fn run_is(base: &str, id: &str, status: &str) {
        let answer = reqwest::Client::new()
            .put(format!("{base}/_stub/runs/{id}/{status}"))
            .send()
            .await
            .unwrap();
        assert!(answer.status().is_success());
    }

    /// A dispatcher that runs fork pull requests with this approval, by one
    /// group, reading a stub GitHub that takes statuses.
    async fn fork_dispatcher(
        db: &Db,
        base: &str,
        approval: crate::forks::Approval,
    ) -> (Dispatcher, StubGithub) {
        let (origin, gh) = stub_github().await;
        let d = Dispatcher::new(
            db.clone(),
            BTreeMap::from([("default".into(), base.into())]),
            "dgr_token".into(),
            Arc::new(Forge::new(Some("ghp_test".into())).with_origin(&origin)),
            compile::Settings::default(),
        )
        .with_forks(Some(crate::forks::Forks {
            approval,
            approvers: vec!["group:maintainers".into()],
            approval_timeout_secs: 3600,
        }));
        pipelines(&gh, &[("ci.yaml", CI)]);
        (d, gh)
    }

    /// A fork's pull request, by an author the forge says this of, claimed.
    async fn claimed_pull(db: &Db, association: Option<&str>) -> Claimed {
        let repo = db
            .add_repo("github", "github.com", "acme/widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        let mut ev = event();
        ev.kind = Kind::PullRequest;
        ev.trust = Trust::Untrusted;
        ev.git_ref = "refs/pull/7/head".into();
        ev.pr = Some(7);
        ev.base_sha = Some("b".repeat(40));
        ev.base_ref = Some("main".into());
        ev.actor = Some("mallory".into());
        ev.association = association.map(str::to_string);
        db.record_delivery(&repo, &uuid::Uuid::now_v7().to_string(), &ev, b"{}")
            .await
            .unwrap();
        db.claim_event("w", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap()
    }

    pub(crate) const TAKES_RECORDS: Option<&[&str]> = Some(&["ci:trigger"]);

    /// Where fork pull requests are run, one is run as an untrusted run, and
    /// never as anything else: on a cell that would not record it so, it is
    /// not run, and says so; on one that cannot be asked, not yet.
    #[tokio::test]
    async fn a_fork_s_pull_request_is_run_as_untrusted_or_not_at_all() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api_as(vec![(201, "")], TAKES_RECORDS).await;
        let (d, gh) = fork_dispatcher(&db, &base, crate::forks::Approval::Never).await;
        let fork = claimed_pull(&db, None).await;

        let finish = d.handle(&fork).await;
        assert!(
            matches!(&finish, Finish::Done(what) if what.contains("ci.yaml: submitted run run-1")),
            "{finish:?}"
        );
        {
            let seen = seen.lock().unwrap();
            assert_eq!(seen.len(), 1);
            let record = seen[0].trigger.as_ref().expect("the run is recorded");
            assert_eq!(
                (
                    &record["kind"],
                    &record["trust"],
                    &record["pr"],
                    &record["ref"]
                ),
                (
                    &serde_json::json!("ci"),
                    &serde_json::json!("untrusted"),
                    &serde_json::json!(7),
                    &serde_json::json!("refs/pull/7/head"),
                )
            );
            assert!(seen[0].yaml.as_deref().unwrap().contains("name: ci"));
        }
        assert!(
            gh.lock().unwrap().statuses.is_empty(),
            "its jobs' statuses are the reporter's to post"
        );

        // A cell whose dagron-api takes no record from dagron-ci's token:
        // the run would read as a person's own, which is trusted.
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api_as(vec![(201, "")], Some(&[])).await;
        let (d, gh) = fork_dispatcher(&db, &base, crate::forks::Approval::Never).await;
        let fork = claimed_pull(&db, None).await;
        let Finish::Skipped(why) = d.handle(&fork).await else {
            panic!("a pull request that cannot be recorded as untrusted is not run");
        };
        assert!(
            why.contains("takes no record of what started a run")
                && why.contains("its head commit says so"),
            "{why}"
        );
        assert!(seen.lock().unwrap().is_empty(), "nothing is submitted");
        {
            let statuses = gh.lock().unwrap().statuses.clone();
            assert_eq!(statuses.len(), 1, "{statuses:?}");
            let status = &statuses[0].2;
            assert_eq!(
                (status["state"].as_str(), status["context"].as_str()),
                (Some("error"), Some(REFUSED_CONTEXT))
            );
            let said = status["description"].as_str().unwrap();
            assert!(
                said.starts_with("not run:")
                    && said.contains("ci:trigger")
                    && said.chars().count() <= 140,
                "{said}"
            );
        }

        // A cell that cannot be asked: not run unmarked, and not given up on.
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api_as(vec![(201, "")], None).await;
        let (d, gh) = fork_dispatcher(&db, &base, crate::forks::Approval::Never).await;
        let fork = claimed_pull(&db, None).await;
        let finish = d.handle(&fork).await;
        assert!(
            matches!(&finish, Finish::Retry { reason, .. } if reason.contains("could not be asked")),
            "{finish:?}"
        );
        assert!(seen.lock().unwrap().is_empty());
        assert!(gh.lock().unwrap().statuses.is_empty());

        // Only a pull request is made untrusted by where it is from: nothing
        // else of that class is run, whatever the setting.
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api_as(vec![(201, "")], TAKES_RECORDS).await;
        let (d, _gh) = fork_dispatcher(&db, &base, crate::forks::Approval::Never).await;
        let (_, push) = claimed(&db, None, Kind::Push, Trust::Untrusted).await;
        let finish = d.handle(&push).await;
        assert!(
            matches!(&finish, Finish::Skipped(why) if why.contains("is refused")),
            "{finish:?}"
        );
        assert!(seen.lock().unwrap().is_empty());
    }

    /// A dispatch that the agent `agent:fixer` asked for, queued and claimed.
    async fn agents_dispatch(db: &Db) -> Claimed {
        agents_dispatch_of(db, None).await
    }

    /// The same, naming `pipeline` where it says one.
    async fn agents_dispatch_of(db: &Db, pipeline: Option<&str>) -> Claimed {
        let repo = db
            .add_repo("github", "github.com", "acme/widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        let dispatch = crate::event::NewEvent {
            kind: Kind::Dispatch,
            trust: Trust::Agent,
            actor: Some("agent:fixer".into()),
            ..event()
        };
        db.record_dispatch(&repo, &dispatch, &BTreeMap::new(), pipeline, None, None)
            .await
            .unwrap();
        db.claim_event("w", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap()
    }

    /// Where this dagron-ci runs an agent's work (CI_AGENT_RUNS=untrusted),
    /// an agent's dispatch is run as an agent's, and never as anything else:
    /// on a cell that would not record it so, it is not run. Where it does
    /// not, the dispatch is refused. Nothing is said on the commit of either.
    #[tokio::test]
    async fn an_agent_s_dispatch_is_run_as_an_agent_s_or_not_at_all() {
        const FIX: &str = "name: fix\non: { dispatch: {} }\n\
                           tasks:\n  - { name: test, docker_image: \"rust:1\", steps: [{ run: cargo test }] }\n";

        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api_as(vec![(201, "")], TAKES_RECORDS).await;
        let (d, gh) = dispatcher(&db, &base).await;
        let d = d.with_agent_runs(true);
        pipelines(&gh, &[("fix.yaml", FIX)]);
        let finish = d.handle(&agents_dispatch(&db).await).await;
        assert!(
            matches!(&finish, Finish::Done(what) if what.contains("fix.yaml: submitted run run-1")),
            "{finish:?}"
        );
        {
            let seen = seen.lock().unwrap();
            let record = seen[0].trigger.as_ref().expect("the run is recorded");
            assert_eq!(
                (&record["trust"], &record["actor"]),
                (
                    &serde_json::json!("agent"),
                    &serde_json::json!("agent:fixer")
                )
            );
        }
        assert!(gh.lock().unwrap().statuses.is_empty());

        // A cell whose dagron-api takes no record from dagron-ci's token:
        // the run would read as a person's own, which is trusted.
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api_as(vec![(201, "")], Some(&[])).await;
        let (d, gh) = dispatcher(&db, &base).await;
        let d = d.with_agent_runs(true);
        pipelines(&gh, &[("fix.yaml", FIX)]);
        let Finish::Skipped(why) = d.handle(&agents_dispatch(&db).await).await else {
            panic!("an agent's dispatch that cannot be recorded as an agent's is not run");
        };
        assert!(
            why.contains("nothing would say the run is an agent's"),
            "{why}"
        );
        assert!(seen.lock().unwrap().is_empty(), "nothing is submitted");
        assert!(gh.lock().unwrap().statuses.is_empty());

        // A pipeline it names that does not take a dispatch: the event fails
        // and says why, and the commit is told nothing of it, though it would
        // be of a person's.
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api_as(vec![(201, "")], TAKES_RECORDS).await;
        let (origin, gh) = stub_github().await;
        let d = Dispatcher::new(
            db.clone(),
            BTreeMap::from([("default".into(), base.clone())]),
            "dgr_token".into(),
            Arc::new(Forge::new(Some("ghp_test".into())).with_origin(&origin)),
            compile::Settings::default(),
        )
        .with_agent_runs(true);
        pipelines(&gh, &[("fix.yaml", FIX), ("ci.yaml", CI)]);
        let Finish::Failed(why) = d.handle(&agents_dispatch_of(&db, Some("ci")).await).await else {
            panic!("a pipeline that does not take dispatch is not run");
        };
        assert!(why.contains("asked for by name"), "{why}");
        assert!(!why.contains("the commit says so"), "{why}");
        assert!(seen.lock().unwrap().is_empty());
        assert!(
            gh.lock().unwrap().statuses.is_empty(),
            "nothing is said on the commit"
        );

        // Where an agent's work is not run, which is the default.
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api_as(vec![(201, "")], TAKES_RECORDS).await;
        let (d, gh) = dispatcher(&db, &base).await;
        pipelines(&gh, &[("fix.yaml", FIX)]);
        let Finish::Skipped(why) = d.handle(&agents_dispatch(&db).await).await else {
            panic!("an agent's dispatch is refused");
        };
        assert!(why.contains("CI_AGENT_RUNS=refuse"), "{why}");
        assert!(seen.lock().unwrap().is_empty());
        assert!(gh.lock().unwrap().statuses.is_empty());
    }

    /// A merge-queue group the queue lets go of without merging it has its
    /// runs cancelled, as dagron-ci, and its events still waiting withdrawn;
    /// once nothing of it is going, a second word of it does nothing.
    #[tokio::test]
    async fn a_merge_group_let_go_of_unmerged_has_its_runs_cancelled() {
        const QUEUE: &str = "name: queue\non: { merge_group: {} }\n\
                             tasks:\n  - { name: test, steps: [{ run: make test }] }\n";
        const GROUP: &str =
            "refs/heads/gh-readonly-queue/main/pr-42-2222222222222222222222222222222222222222";
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api_as(vec![(201, "")], TAKES_RECORDS).await;
        let (d, gh) = dispatcher(&db, &base).await;
        pipelines(&gh, &[("queue.yaml", QUEUE)]);
        let repo = db
            .add_repo("github", "github.com", "acme/widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        let of_group = |kind: Kind, sha: char| crate::event::NewEvent {
            kind,
            git_ref: GROUP.into(),
            sha: sha.to_string().repeat(40),
            base_sha: Some("b".repeat(40)),
            base_ref: Some("refs/heads/main".into()),
            ..event()
        };
        let next = || async {
            db.claim_event("w", Duration::from_secs(60))
                .await
                .unwrap()
                .unwrap()
        };

        // The group asks for its checks, and its run is submitted.
        db.record_delivery(&repo, "g-1", &of_group(Kind::MergeGroup, 'd'), b"{}")
            .await
            .unwrap();
        let asked = next().await;
        let finish = d.handle(&asked).await;
        assert!(
            matches!(&finish, Finish::Done(what) if what.contains("queue.yaml: submitted run run-1")),
            "{finish:?}"
        );
        assert!(db.finish_event(&asked.id, "w", &finish).await.unwrap());
        // The queue lets go of it; a second ask of it is still waiting.
        db.record_delivery(
            &repo,
            "g-2",
            &of_group(Kind::MergeGroupDestroyed, 'd'),
            b"{}",
        )
        .await
        .unwrap();
        db.record_delivery(&repo, "g-3", &of_group(Kind::MergeGroup, 'd'), b"{}")
            .await
            .unwrap();
        let destroyed = next().await;
        assert_eq!(destroyed.kind, "merge_group_destroyed");
        let Finish::Done(what) = d.handle(&destroyed).await else {
            panic!("the group's run is cancelled");
        };
        assert!(
            what.ends_with("1 run(s) cancelled, 1 event(s) withdrawn"),
            "{what}"
        );
        let status: serde_json::Value = reqwest::get(format!("{base}/api/runs/run-1"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(status["status"], "cancelled");
        assert_eq!(seen.lock().unwrap().len(), 1, "nothing more is submitted");
        let waiting = db.claim_event("w", Duration::from_secs(60)).await.unwrap();
        assert!(waiting.is_none(), "the group's waiting ask was withdrawn");

        // Said again, it finds nothing of the group going.
        assert!(matches!(
            d.handle(&destroyed).await,
            Finish::Skipped(why) if why.ends_with("nothing of it was going")
        ));
    }

    /// A GitLab merge train's pipeline let go of has its own runs cancelled,
    /// and no other's: a train's ref is its merge request's, and the pipeline
    /// the train makes again of it, behind one that failed, is of another
    /// commit under the same ref.
    #[tokio::test]
    async fn a_merge_train_s_pipeline_let_go_of_cancels_its_own_runs_alone() {
        const TRAIN: &str = "refs/merge-requests/12/train";
        let (db, _dir) = sqlite().await;
        let (base, _seen) = stub_api_as(vec![(201, "")], TAKES_RECORDS).await;
        let (d, _gh) = dispatcher(&db, &base).await;
        let repo = db
            .add_repo("gitlab", "gitlab.example", "group/app", None, "default")
            .await
            .unwrap()
            .unwrap();
        let of_train = |kind: Kind, sha: char| crate::event::NewEvent {
            kind,
            git_ref: TRAIN.into(),
            sha: sha.to_string().repeat(40),
            base_ref: Some("main".into()),
            ..event()
        };
        // The train's first pipeline, and the one it made again: a run each.
        for (sha, run) in [('d', "run-d"), ('e', "run-e")] {
            db.record_delivery(&repo, run, &of_train(Kind::MergeGroup, sha), b"{}")
                .await
                .unwrap();
            let held = db
                .claim_event("w", Duration::from_secs(60))
                .await
                .unwrap()
                .unwrap();
            assert!(db
                .record_run(&crate::db::SubmittedRun {
                    run_id: run.into(),
                    event_id: held.id.clone(),
                    repo_id: repo.id.clone(),
                    cell: "default".into(),
                    pipeline: ".dagron/ci/queue.yaml".into(),
                    idempotency_key: format!("ci-{run}"),
                    on_failed: false,
                })
                .await
                .unwrap());
            assert!(db
                .finish_event(&held.id, "w", &Finish::Done("submitted".into()))
                .await
                .unwrap());
        }
        db.record_delivery(
            &repo,
            "cancel-d",
            &of_train(Kind::MergeGroupDestroyed, 'd'),
            b"{}",
        )
        .await
        .unwrap();
        let destroyed = db
            .claim_event("w", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
        let Finish::Done(what) = d.handle(&destroyed).await else {
            panic!("the first pipeline's run is cancelled");
        };
        assert!(
            what.starts_with(&format!(
                "the merge train let go of {TRAIN} at dddddddddddd without merging it: 1 run(s) \
                 cancelled"
            )),
            "{what}"
        );
        let status = |run: &'static str| {
            let base = base.clone();
            async move {
                reqwest::get(format!("{base}/api/runs/{run}"))
                    .await
                    .unwrap()
                    .json::<serde_json::Value>()
                    .await
                    .unwrap()["status"]
                    .clone()
            }
        };
        assert_eq!(status("run-d").await, "cancelled");
        assert_eq!(
            status("run-e").await,
            "running",
            "the train's new pipeline's"
        );
    }

    /// A run of the group on a cell this dagron-ci no longer has keeps none
    /// of the others from being cancelled, and is said once they have been.
    #[tokio::test]
    async fn a_run_that_cannot_be_cancelled_keeps_none_of_the_others_from_it() {
        const GROUP: &str =
            "refs/heads/gh-readonly-queue/main/pr-43-3333333333333333333333333333333333333333";
        let (db, _dir) = sqlite().await;
        let (base, _seen) = stub_api_as(vec![(201, "")], TAKES_RECORDS).await;
        let (d, _gh) = dispatcher(&db, &base).await;
        let repo = db
            .add_repo("github", "github.com", "acme/widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        let asked = crate::event::NewEvent {
            kind: Kind::MergeGroup,
            git_ref: GROUP.into(),
            base_ref: Some("refs/heads/main".into()),
            ..event()
        };
        db.record_delivery(&repo, "g-1", &asked, b"{}")
            .await
            .unwrap();
        let held = db
            .claim_event("w", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
        // Two runs of the group: one on a cell that has gone, one on its own.
        for (run, cell) in [("run-gone", "elsewhere"), ("run-here", "default")] {
            assert!(db
                .record_run(&crate::db::SubmittedRun {
                    run_id: run.into(),
                    event_id: held.id.clone(),
                    repo_id: repo.id.clone(),
                    cell: cell.into(),
                    pipeline: ".dagron/ci/queue.yaml".into(),
                    idempotency_key: format!("ci-{run}"),
                    on_failed: false,
                })
                .await
                .unwrap());
        }
        assert!(db
            .finish_event(&held.id, "w", &Finish::Done("submitted".into()))
            .await
            .unwrap());
        let destroyed = crate::event::NewEvent {
            kind: Kind::MergeGroupDestroyed,
            ..asked
        };
        db.record_delivery(&repo, "g-2", &destroyed, b"{}")
            .await
            .unwrap();
        let destroyed = db
            .claim_event("w", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
        let Finish::Failed(why) = d.handle(&destroyed).await else {
            panic!("a run that could not be cancelled is said");
        };
        assert!(why.contains("1 run(s) cancelled"), "{why}");
        assert!(
            why.contains("run run-gone: cell \"elsewhere\" is not in CI_CELLS"),
            "{why}"
        );
        let status: serde_json::Value = reqwest::get(format!("{base}/api/runs/run-here"))
            .await
            .unwrap()
            .json()
            .await
            .unwrap();
        assert_eq!(status["status"], "cancelled", "the other run was cancelled");
    }

    /// A pull request that needs an approval has no pipeline of it submitted
    /// until the approval is given. The approval is a run of its own, every
    /// word of it dagron-ci's, and the pull request's head says where it
    /// stands. Two events for one head, as a delivery and a sweep make, wait
    /// on the one approval.
    #[tokio::test]
    async fn a_pull_request_that_needs_an_approval_runs_once_it_is_given() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api_as(vec![(201, "")], TAKES_RECORDS).await;
        let (d, gh) = fork_dispatcher(&db, &base, crate::forks::Approval::Always).await;
        let fork = claimed_pull(&db, Some("CONTRIBUTOR")).await;
        let statuses = || {
            gh.lock()
                .unwrap()
                .statuses
                .iter()
                .map(|(_, _, s)| {
                    (
                        s["state"].as_str().unwrap().to_string(),
                        s["context"].as_str().unwrap().to_string(),
                    )
                })
                .collect::<Vec<_>>()
        };
        let waits = |finish: &Finish| {
            matches!(finish, Finish::Wait { reason, after }
                if reason.contains("pull request #7 waits for an approval: run run-1")
                    && *after == APPROVAL_WAIT)
        };

        // Asked for: the approval's run, and nothing else.
        let finish = d.handle(&fork).await;
        assert!(waits(&finish), "{finish:?}");
        let key = {
            let seen = seen.lock().unwrap();
            assert_eq!(seen.len(), 1);
            let asked = &seen[0];
            let dag =
                dagron_core::dag::DagGraph::from_yaml(asked.yaml.as_deref().unwrap()).unwrap();
            assert_eq!(dag.spec.name, crate::forks::GATE_WORKFLOW);
            assert_eq!(dag.spec.tasks.len(), 1);
            let task = &dag.spec.tasks[0];
            assert!(task.is_approval());
            assert_eq!(task.approvers, ["group:maintainers"]);
            assert_eq!(task.approval_on_timeout.as_deref(), Some("reject"));
            // The approval is this installation's own run, for the pull
            // request: nothing in it is the fork's.
            let record = asked.trigger.as_ref().unwrap();
            assert_eq!(
                (&record["trust"], &record["pr"]),
                (&serde_json::json!("trusted"), &serde_json::json!(7))
            );
            asked.key.clone()
        };
        assert_eq!(
            statuses(),
            [(
                "pending".to_string(),
                crate::forks::GATE_CONTEXT.to_string()
            )]
        );

        // Still waiting: the same run is found again, and nothing is said twice.
        let finish = d.handle(&fork).await;
        assert!(waits(&finish), "{finish:?}");
        {
            let seen = seen.lock().unwrap();
            assert_eq!(seen.len(), 2);
            assert_eq!(seen[1].key, key, "the approval is found by its key");
        }
        assert_eq!(statuses().len(), 1);

        // The same head again, as the other door sees it: it names another
        // person, and the forge says something else of the author. One
        // commit has one approval, and this event waits on it.
        let other = {
            let mut ev = event();
            ev.kind = Kind::PullRequest;
            ev.trust = Trust::Untrusted;
            ev.git_ref = "refs/pull/7/head".into();
            ev.pr = Some(7);
            ev.base_sha = Some("b".repeat(40));
            ev.base_ref = Some("main".into());
            ev.actor = Some("a-maintainer".into());
            ev.association = Some("NONE".into());
            db.record_delivery(&fork.repo, &uuid::Uuid::now_v7().to_string(), &ev, b"{}")
                .await
                .unwrap();
            db.claim_event("w", Duration::from_secs(60))
                .await
                .unwrap()
                .expect("a second event for the head")
        };
        assert_eq!(
            (other.commit_key.as_str(), other.sha.as_str()),
            (fork.commit_key.as_str(), fork.sha.as_str())
        );
        assert_ne!(other.id, fork.id);
        let finish = d.handle(&other).await;
        assert!(waits(&finish), "{finish:?}");
        {
            let seen = seen.lock().unwrap();
            assert_eq!(seen.len(), 3);
            assert_eq!(
                (&seen[2].key, &seen[2].yaml),
                (&key, &seen[0].yaml),
                "the same approval, asked for in the same words"
            );
        }
        assert_eq!(statuses().len(), 1, "and no second request is announced");

        // Given: the pull request's pipeline is submitted, as untrusted.
        run_is(&base, "run-1", "succeeded").await;
        let finish = d.handle(&fork).await;
        assert!(
            matches!(&finish, Finish::Done(what) if what.contains("ci.yaml: submitted run run-2")),
            "{finish:?}"
        );
        {
            let seen = seen.lock().unwrap();
            assert_eq!(seen.len(), 5, "the approval again, then the pipeline");
            let run = &seen[4];
            assert!(run.yaml.as_deref().unwrap().contains("name: ci"));
            assert_ne!(run.key, key);
            assert_eq!(
                run.trigger.as_ref().unwrap()["trust"],
                serde_json::json!("untrusted")
            );
        }
        assert_eq!(
            statuses()[1],
            (
                "success".to_string(),
                crate::forks::GATE_CONTEXT.to_string()
            )
        );

        // Refused, or never answered: nothing of the pull request is run, and
        // its head says so.
        for ended in ["failed", "cancelled"] {
            let (db, _dir) = sqlite().await;
            let (base, seen) = stub_api_as(vec![(201, "")], TAKES_RECORDS).await;
            let (d, gh) = fork_dispatcher(&db, &base, crate::forks::Approval::Always).await;
            let fork = claimed_pull(&db, None).await;
            assert!(matches!(d.handle(&fork).await, Finish::Wait { .. }));
            run_is(&base, "run-1", ended).await;
            let Finish::Skipped(why) = d.handle(&fork).await else {
                panic!("a pull request that was not approved is not run ({ended})");
            };
            assert!(
                why.contains("pull request #7 was not approved")
                    && why.contains("nothing of it was run")
                    && why.contains("its head commit says so"),
                "{why}"
            );
            let seen = seen.lock().unwrap();
            assert_eq!(seen.len(), 2, "the approval, asked for and looked at");
            assert!(seen.iter().all(|s| s
                .yaml
                .as_deref()
                .unwrap()
                .contains(crate::forks::GATE_WORKFLOW)));
            let last = gh.lock().unwrap().statuses.last().unwrap().2.clone();
            assert_eq!(
                (last["state"].as_str(), last["context"].as_str()),
                (Some("failure"), Some(crate::forks::GATE_CONTEXT))
            );
        }
    }

    /// Where only a first contribution waits, an author with a commit in the
    /// repository does not, and one with none does, and so does one of whom
    /// the forge said nothing.
    #[tokio::test]
    async fn only_a_first_contribution_waits_where_that_is_the_setting() {
        for (association, waits) in [
            (Some("CONTRIBUTOR"), false),
            (Some("FIRST_TIME_CONTRIBUTOR"), true),
            (Some("NONE"), true),
            (None, true),
        ] {
            let (db, _dir) = sqlite().await;
            let (base, seen) = stub_api_as(vec![(201, "")], TAKES_RECORDS).await;
            let (d, _gh) = fork_dispatcher(&db, &base, crate::forks::Approval::FirstTime).await;
            let fork = claimed_pull(&db, association).await;
            let finish = d.handle(&fork).await;
            let seen = seen.lock().unwrap();
            let first = seen[0].yaml.as_deref().unwrap();
            if waits {
                assert!(
                    matches!(finish, Finish::Wait { .. }),
                    "{association:?}: {finish:?}"
                );
                assert!(
                    first.contains(crate::forks::GATE_WORKFLOW),
                    "{association:?}"
                );
                assert_eq!(seen.len(), 1);
            } else {
                assert!(
                    matches!(finish, Finish::Done(_)),
                    "{association:?}: {finish:?}"
                );
                assert!(first.contains("name: ci"), "{association:?}");
                assert_eq!(
                    seen[0].trigger.as_ref().unwrap()["trust"],
                    serde_json::json!("untrusted")
                );
            }
        }
    }

    #[tokio::test]
    async fn a_trusted_push_runs_the_bound_workflow_once() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, "")]).await;
        let (repo, ev) = claimed(&db, Some("wf-1"), Kind::Push, Trust::Trusted).await;
        let (d, gh) = dispatcher(&db, &base).await;

        let finish = d.handle(&ev).await;
        assert_eq!(
            finish,
            Finish::Done("submitted run run-1 on cell default".into())
        );
        {
            let seen = seen.lock().unwrap();
            let s = &seen[0];
            assert_eq!(s.workflow.as_deref(), Some("wf-1"));
            assert_eq!(s.key, idempotency_key(&ev.commit_key, "workflow:wf-1"));
            assert_eq!(s.auth, "Bearer dgr_token");
            assert_eq!(s.params["ci_sha"], "a".repeat(40));
            assert_eq!(s.params["ci_event"], "push");
            assert_eq!(s.params["ci_group"], "refs/heads/main");
            assert_eq!(s.params["ci_repo"], repo.full_name);
            assert_eq!(s.params["ci_base_sha"], "c".repeat(40));
            assert_eq!(s.params["ci_pr"], "", "absent is empty, never unresolved");
        }
        assert_eq!(
            gh.lock().unwrap().seen.len(),
            1,
            "the commit was asked for its pipelines, and had none"
        );

        // The same commit again (a poll that saw it, a redelivered event):
        // found in ci_runs, and dagron-api is not asked.
        assert_eq!(
            d.handle(&ev).await,
            Finish::Done("run run-1 was submitted for this commit already".into())
        );
        assert_eq!(seen.lock().unwrap().len(), 1);
        assert_eq!(
            seen.lock().unwrap()[0].trigger,
            None,
            "a token outside the trigger group sends no record, and its run is made"
        );
    }

    /// A token whose user is in dagron-api's trigger group marks its runs: a
    /// bound workflow's and a compiled pipeline's, each with what the event
    /// was. The cell is asked who the token is once, not once a run.
    #[tokio::test]
    async fn a_token_in_the_trigger_group_marks_its_runs() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api_as(vec![(201, "")], Some(&["ci:trigger"])).await;
        let (repo, ev) = claimed(&db, Some("wf-1"), Kind::PullRequest, Trust::Trusted).await;
        let (d, gh) = dispatcher(&db, &base).await;

        assert!(matches!(d.handle(&ev).await, Finish::Done(_)));
        let want = serde_json::json!({
            "kind": "ci",
            "actor": ev.actor,
            "trust": "trusted",
            "forge_host": "github.com",
            "repo": repo.full_name,
            "ref": "refs/pull/7/head",
            "sha": ev.sha,
            "base_sha": "b".repeat(40),
            "pr": 7,
            "event_id": ev.id,
        });
        assert_eq!(seen.lock().unwrap()[0].trigger.as_ref(), Some(&want));
        assert_eq!(seen.lock().unwrap()[0].workflow.as_deref(), Some("wf-1"));

        // The same repository's own pipeline, at another commit: marked too.
        pipelines(&gh, &[("ci.yaml", CI)]);
        let mut next = event();
        next.sha = "d".repeat(40);
        db.record_delivery(&repo, "d-2", &next, b"{}").await.unwrap();
        let ev2 = db.claim_event("w", Duration::from_secs(60)).await.unwrap().unwrap();
        assert!(matches!(d.handle(&ev2).await, Finish::Done(_)));
        let seen = seen.lock().unwrap();
        let s = seen.last().unwrap();
        assert!(s.yaml.is_some(), "the pipeline was compiled and submitted inline");
        let t = s.trigger.as_ref().expect("a compiled pipeline's run is marked");
        assert_eq!(
            (&t["kind"], &t["sha"], &t["ref"], &t["event_id"], &t["pr"]),
            (
                &serde_json::json!("ci"),
                &serde_json::json!("d".repeat(40)),
                &serde_json::json!("refs/heads/main"),
                &serde_json::json!(ev2.id),
                &serde_json::Value::Null,
            )
        );
        assert_eq!(d.marks.lock().unwrap().len(), 1, "one cell, asked once and kept");
    }

    /// An annotated tag's event is keyed by the tag's own id and runs at the
    /// commit the tag names: its pipelines are read there, and `ci_sha` (which
    /// the checkout is made at) and the trigger record say that commit.
    #[tokio::test]
    async fn an_annotated_tag_runs_at_the_commit_it_names() {
        const TAGS: &str = "name: tags\non: { push: { tags: [\"v*\"] } }\n\
                            tasks:\n  - { name: release, docker_image: \"rust:1\", steps: [{ run: make }] }\n";
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api_as(vec![(201, "")], Some(&["ci:trigger"])).await;
        let repo = db
            .add_repo("github", "github.com", "acme/widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        let (tag, commit) = ("d".repeat(40), "c".repeat(40));
        let annotated = crate::event::NewEvent {
            kind: Kind::Tag,
            git_ref: "refs/tags/v1.0.0".into(),
            sha: tag.clone(),
            commit: Some(commit.clone()),
            ..event()
        };
        db.record_delivery(&repo, "d-tag", &annotated, b"{}")
            .await
            .unwrap();
        let ev = db
            .claim_event("w", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
        let (d, gh) = dispatcher(&db, &base).await;
        pipelines(&gh, &[("tags.yaml", TAGS)]);

        assert_eq!(
            d.handle(&ev).await,
            Finish::Done("tags.yaml: submitted run run-1".into())
        );
        let read = gh.lock().unwrap().seen.clone();
        assert!(
            !read.is_empty() && read.iter().all(|(path, _, _)| path.ends_with(&format!("?ref={commit}"))),
            "the pipelines are read at the commit: {read:?}"
        );
        let s = seen.lock().unwrap()[0].clone();
        assert_eq!(s.params["ci_sha"], commit);
        assert_eq!(s.params["ci_ref"], "refs/tags/v1.0.0");
        assert!(
            s.yaml.as_deref().unwrap().contains("commit: '{{ ci_sha }}'"),
            "the checkout is made at ci_sha"
        );
        assert_eq!(s.trigger.as_ref().unwrap()["sha"], serde_json::json!(commit));
        // The key is the tag's own id's, as a sweep that lists the tag
        // derives it: the two make one run.
        assert_eq!(
            s.key,
            idempotency_key(
                &format!("ci:{}:tag:refs/tags/v1.0.0:{tag}", repo.id),
                ".dagron/ci/tags.yaml"
            )
        );
    }

    /// A cell that cannot say who the token is gets no record, and its run all
    /// the same: nothing about marking a run may cost the run.
    #[tokio::test]
    async fn a_cell_that_cannot_say_who_the_token_is_gets_an_unmarked_run() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api_as(vec![(201, "")], None).await;
        let (_repo, ev) = claimed(&db, Some("wf-1"), Kind::Push, Trust::Trusted).await;
        let (d, _gh) = dispatcher(&db, &base).await;

        assert_eq!(d.handle(&ev).await, Finish::Done("submitted run run-1 on cell default".into()));
        assert_eq!(seen.lock().unwrap()[0].trigger, None);
        assert!(d.marks.lock().unwrap().is_empty(), "not kept: the next submit asks again");
    }

    /// dagron-api refuses a record with a field it would not store. Such a
    /// field is left out, so the record has a gap and the run is still made.
    #[test]
    fn a_field_dagron_api_would_refuse_is_left_out_of_the_record() {
        let repo = Repo {
            id: "r".into(),
            forge: "github".into(),
            forge_host: "github.com".into(),
            full_name: "acme/widgets".into(),
            workflow: None,
            cell: "default".into(),
            mode: crate::db::Mode::Native,
        };
        let ev = Claimed {
            id: "e-1".into(),
            repo,
            kind: "push".into(),
            git_ref: format!("refs/heads/{}", "x".repeat(600)),
            sha: "a".repeat(40),
            base_sha: None,
            base_ref: None,
            pr: None,
            actor: Some("octo\ncat".into()),
            trust: "trusted".into(),
            association: None,
            head_ref: None,
            commit_key: "k".into(),
            attempts: 1,
            inputs: BTreeMap::new(),
            pipeline: None,
            bundle_id: None,
            patch: None,
        };
        let t = trigger(&ev);
        assert_eq!(t["trust"], "trusted");
        // An agent's bundle is untrusted to the engine.
        let bundled = Claimed {
            trust: "agent".into(),
            bundle_id: Some("b-1".into()),
            ..ev.clone()
        };
        assert_eq!(trigger(&bundled)["trust"], "untrusted");
        assert_eq!(t["ref"], serde_json::Value::Null, "over 512 bytes");
        assert_eq!(t["actor"], serde_json::Value::Null, "a control character");
        assert_eq!(t["base_sha"], serde_json::Value::Null, "absent");
        assert_eq!(
            (&t["kind"], &t["repo"], &t["sha"], &t["event_id"]),
            (
                &serde_json::json!("ci"),
                &serde_json::json!("acme/widgets"),
                &serde_json::json!("a".repeat(40)),
                &serde_json::json!("e-1"),
            )
        );
    }

    /// A worker that died after dagron-api created the run, before recording
    /// it, loses nothing and duplicates nothing: the retry replays the key and
    /// records the run it gets back.
    #[tokio::test]
    async fn a_retry_after_an_unrecorded_submit_gets_the_same_run() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, "")]).await;
        let (_repo, ev) = claimed(&db, Some("wf-1"), Kind::Push, Trust::Trusted).await;
        let (d, _gh) = dispatcher(&db, &base).await;
        let key = idempotency_key(&ev.commit_key, "workflow:wf-1");
        let body = serde_json::json!({ "parameters": parameters(&ev) });
        let Answer::Run(first) = d
            .submit(&format!("{base}/api/workflows/wf-1/run"), &key, &body)
            .await
        else {
            panic!("dagron-api made the run");
        };
        assert_eq!(
            db.find_run(&key).await.unwrap(),
            None,
            "the worker died here"
        );

        assert_eq!(
            d.handle(&ev).await,
            Finish::Done(format!("submitted run {first} on cell default"))
        );
        assert_eq!(db.find_run(&key).await.unwrap(), Some(first));
        assert_eq!(seen.lock().unwrap().len(), 2, "asked twice, one run");
    }

    /// A pull request refused for where it is from leaves one status on its
    /// head, which says it was not run and why, and is never one that passes
    /// (rule 17: no check waits on silence). Under a context of its own, since
    /// no pipeline of the pull request was read.
    #[tokio::test]
    async fn a_refused_pull_request_s_head_says_it_was_not_run() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, "")]).await;
        let (origin, gh) = stub_github().await;
        let metrics = Arc::new(Metrics::default());
        let d = Dispatcher::new(
            db.clone(),
            BTreeMap::from([("default".into(), base)]),
            "dgr_token".into(),
            Arc::new(Forge::new(Some("ghp_test".into())).with_origin(&origin)),
            compile::Settings::default(),
        )
        .with_metrics(metrics.clone());
        let posted = || gh.lock().unwrap().statuses.clone();
        let answers = |with: Option<(u16, Vec<(&'static str, String)>)>| {
            gh.lock().unwrap().answer = with;
        };

        let (_, fork) = claimed(&db, Some("wf-1"), Kind::PullRequest, Trust::Untrusted).await;
        let Finish::Skipped(why) = d.handle(&fork).await else {
            panic!("a refused pull request is skipped");
        };
        assert!(
            why.contains("CI_FORK_PRS=refuse") && why.contains("its head commit says so"),
            "{why}"
        );
        let statuses = posted();
        assert_eq!(statuses.len(), 1, "{statuses:?}");
        let (sha, auth, status) = &statuses[0];
        assert_eq!((sha, auth.as_str()), (&fork.sha, "Bearer ghp_test"));
        assert_eq!(
            (status["state"].as_str(), status["context"].as_str()),
            (Some("error"), Some("dagron-ci (pull_request)")),
            "an error, never a success, and under no job's name"
        );
        let said = status["description"].as_str().unwrap();
        assert!(
            said.starts_with("not run:")
                && said.contains("CI_FORK_PRS=refuse")
                && said.chars().count() <= 140,
            "{said}"
        );
        assert_eq!(
            status.get("target_url"),
            None,
            "there is no run to point at"
        );
        assert_eq!(
            gh.lock().unwrap().seen.len(),
            1,
            "the status, and nothing of the pull request was read"
        );
        assert!(seen.lock().unwrap().is_empty(), "nor anything submitted");

        // GitHub's rate limit is spent: the event waits it out, and is not
        // skipped with nothing said.
        answers(Some((403, vec![("retry-after", "120".into())])));
        assert!(
            matches!(d.handle(&fork).await, Finish::Wait { after, .. } if after == Duration::from_secs(120))
        );
        // GitHub cannot take it now: tried again.
        answers(Some((502, vec![])));
        assert!(
            matches!(d.handle(&fork).await, Finish::Retry { reason, .. } if reason.contains("HTTP 502"))
        );
        // GitHub refuses the token, and would again: no status can say so,
        // and the event's own reason does.
        answers(Some((403, vec![])));
        let Finish::Skipped(why) = d.handle(&fork).await else {
            panic!("a status that cannot be posted does not hold the event");
        };
        assert!(
            why.contains("CI_FORK_PRS=refuse")
                && why.contains("no status says so")
                && why.contains("statuses: write"),
            "{why}"
        );
        // A secondary rate limit can answer 403 too, with no wait in its
        // headers. GitHub's message says which it is: that one is waited out,
        // for the minute GitHub asks, and not taken for a refusal.
        gh.lock().unwrap().answer_body =
            r#"{"message":"You have exceeded a secondary rate limit. Please wait a few minutes before you try again."}"#
                .into();
        let Finish::Wait { reason, after } = d.handle(&fork).await else {
            panic!("a rate limit that names no wait is still waited out");
        };
        assert_eq!(after, Duration::from_secs(60), "{reason}");
        assert!(reason.contains("named no wait"), "{reason}");
        gh.lock().unwrap().answer_body.clear();
        answers(None);
        assert_eq!(posted().len(), 1);
        let counted = metrics.render(None, &[]);
        for (outcome, times) in [
            ("posted", 1),
            ("rate_limited", 2),
            ("failed", 1),
            ("refused", 1),
        ] {
            assert!(
                counted.contains(&format!(
                    "dagron_ci_commit_statuses_total{{outcome=\"{outcome}\"}} {times}\n"
                )),
                "{outcome}: {counted}"
            );
        }

        // A closed pull request is skipped for being closed, whoever opened
        // it: it is not refused, and nothing is said on its head.
        db.remove_repo("github.com", "acme/widgets").await.unwrap();
        let (_, closed) =
            claimed(&db, Some("wf-1"), Kind::PullRequestClosed, Trust::Untrusted).await;
        assert_eq!(
            d.handle(&closed).await,
            Finish::Skipped("a closed pull request runs nothing".into())
        );
        assert_eq!(posted().len(), 1);
    }

    #[tokio::test]
    async fn what_cannot_run_is_skipped_with_the_reason() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, "")]).await;
        let (d, gh) = dispatcher(&db, &base).await;

        // Refused, and with no token for statuses there is no commit to tell.
        let (_, fork) = claimed(&db, Some("wf-1"), Kind::PullRequest, Trust::Untrusted).await;
        assert_eq!(
            d.handle(&fork).await,
            Finish::Skipped(
                "a pull_request event from a fork or a non-member is refused (CI_FORK_PRS=refuse)"
                    .into()
            )
        );
        assert!(gh.lock().unwrap().seen.is_empty(), "GitHub was not asked");
        db.remove_repo("github.com", "acme/widgets").await.unwrap();

        let (_, unbound) = claimed(&db, None, Kind::Push, Trust::Trusted).await;
        let Finish::Skipped(why) = d.handle(&unbound).await else {
            panic!("no pipelines and no workflow: nothing runs");
        };
        assert!(
            why.contains("aaaaaaaaaaaa has no pipelines in .dagron/ci")
                && why.contains("CI_GITHUB_TOKEN_FILE")
                && why.contains("--workflow ID"),
            "the forge is read without a token, which a private repository needs: {why}"
        );
        db.remove_repo("github.com", "acme/widgets").await.unwrap();

        for (kind, why) in [
            (Kind::Delete, "deleted ref"),
            (Kind::PullRequestClosed, "closed pull request"),
        ] {
            let (_, ev) = claimed(&db, Some("wf-1"), kind, Trust::Trusted).await;
            assert!(matches!(d.handle(&ev).await, Finish::Skipped(r) if r.contains(why)));
            db.remove_repo("github.com", "acme/widgets").await.unwrap();
        }
        assert!(
            seen.lock().unwrap().is_empty(),
            "dagron-api was never asked"
        );
        assert_eq!(
            gh.lock().unwrap().seen.len(),
            1,
            "only the trusted push's commit was read"
        );

        // A cell the operator never named is a failure, not a skip.
        let d = Dispatcher::new(
            db.clone(),
            BTreeMap::new(),
            "t".into(),
            Arc::new(Forge::new(None)),
            compile::Settings::default(),
        );
        let (_, ev) = claimed(&db, Some("wf-1"), Kind::Push, Trust::Trusted).await;
        assert!(matches!(d.handle(&ev).await, Finish::Failed(r) if r.contains("CI_CELLS")));
    }

    #[tokio::test]
    async fn dagron_api_answers_decide_retry_wait_or_failure() {
        let (db, _dir) = sqlite().await;
        // An environment's policy refusing a run, in the refusal's own words:
        // whatever they come to be, this is what dagron-api answers with.
        let refused: &'static str = Box::leak(
            dagron_core::envpolicy::admits(
                "release",
                None,
                &dagron_core::envpolicy::RunFacts {
                    trust: dagron_core::envpolicy::UNTRUSTED.to_string(),
                    git_ref: None,
                    pipeline: "ci".to_string(),
                },
            )
            .expect_err("an environment with no policy admits trusted runs alone")
            .to_string()
            .into_boxed_str(),
        );
        // The words dagron-ci knows one by are dagron's.
        assert_eq!(POLICY_REFUSAL, dagron_core::envpolicy::REFUSAL_MARK);
        assert!(refused.contains(POLICY_REFUSAL), "{refused}");
        // And a run that is not trusted naming what such a run may not use.
        let confined: &'static str = Box::leak(
            dagron_core::confine::ConfinedRefused {
                class: dagron_core::envpolicy::UNTRUSTED.to_string(),
                what: "`notify`".to_string(),
                why: "the engine itself would post where the workflow names",
            }
            .to_string()
            .into_boxed_str(),
        );
        assert_eq!(CONFINED_REFUSAL, dagron_core::confine::REFUSAL_MARK);
        assert!(confined.contains(CONFINED_REFUSAL), "{confined}");
        for (answer, want) in [
            ((503, "busy"), "retry"),
            ((429, "slow down"), "retry"),
            ((401, "who"), "retry"),
            // A 403 is dagron-ci's token's, which an operator can mend and
            // the event waits for. But for a policy's, which waiting does
            // not change: the commit is told.
            ((403, "forbidden"), "retry"),
            ((403, refused), "fail"),
            ((403, confined), "fail"),
            (
                (
                    409,
                    "Idempotency-Key 'k': a submission with this key is in flight; retry",
                ),
                "retry",
            ),
            (
                (
                    429,
                    "max_active_runs reached for workflow 'w' and concurrency_key 'k' (1 active, cap 1)",
                ),
                "wait",
            ),
            ((400, "invalid DAG: cycle"), "fail"),
            ((404, "workflow 'wf-1' not found"), "fail"),
            (
                (
                    409,
                    "workflow 'wf-1' is paused — set it active before running it",
                ),
                "fail",
            ),
        ] {
            let (base, _seen) = stub_api(vec![answer]).await;
            let (_, ev) = claimed(&db, Some("wf-1"), Kind::Push, Trust::Trusted).await;
            let finish = dispatcher(&db, &base).await.0.handle(&ev).await;
            match (want, &finish) {
                ("retry", Finish::Retry { .. })
                | ("fail", Finish::Failed(_))
                | ("wait", Finish::Wait { .. }) => {}
                _ => panic!("{answer:?} gave {finish:?}"),
            }
            db.remove_repo("github.com", "acme/widgets").await.unwrap();
        }

        // Nobody listening: retried.
        let (_, ev) = claimed(&db, Some("wf-1"), Kind::Push, Trust::Trusted).await;
        let finish = dispatcher(&db, "http://127.0.0.1:9")
            .await
            .0
            .handle(&ev)
            .await;
        assert!(matches!(finish, Finish::Retry { .. }), "{finish:?}");
    }

    #[tokio::test]
    async fn a_pull_request_is_grouped_by_number_and_a_ref_is_made_safe() {
        let (db, _dir) = sqlite().await;
        let (_, mut ev) = claimed(&db, Some("wf-1"), Kind::PullRequest, Trust::Trusted).await;
        let params = parameters(&ev);
        assert_eq!(
            (params["ci_group"].as_str(), params["ci_pr"].as_str()),
            ("pr-7", "7")
        );
        assert_eq!(params["ci_base_sha"], "b".repeat(40));

        ev.kind = "push".into();
        ev.pr = None;
        ev.git_ref = "refs/heads/x;rm -rf /$(id){{ a }}".into();
        assert_eq!(
            parameters(&ev)["ci_group"],
            "refs/heads/x_rm_-rf_/__id____a___"
        );
        ev.git_ref = format!("refs/heads/{}", "é".repeat(300));
        assert_eq!(parameters(&ev)["ci_group"].chars().count(), 128);
    }

    #[test]
    fn the_key_is_short_and_stable() {
        let k = idempotency_key(
            &format!("ci:r:push:refs/heads/{}:abc", "x".repeat(1024)),
            "workflow:wf",
        );
        assert_eq!(k.len(), 3 + 64);
        assert_eq!(
            k,
            idempotency_key(
                &format!("ci:r:push:refs/heads/{}:abc", "x".repeat(1024)),
                "workflow:wf"
            )
        );
        assert_ne!(
            k,
            idempotency_key("ci:r:push:refs/heads/main:abc", "workflow:wf")
        );
    }

    const SETTINGS: &str = "github:\n  workflows: [build.yml, absent.yml]\n  \
                            runs_on: { ubuntu-latest: \"docker.io/library/ubuntu:24.04\" }\n";
    const BUILD: &str = "on: [push, pull_request]\njobs:\n  \
                         test:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/checkout@v4\n      \
                         - run: make test\n  \
                         release:\n    needs: test\n    if: github.event_name == 'workflow_dispatch'\n    \
                         runs-on: ubuntu-latest\n    steps:\n      - run: make release\n";

    /// A dispatcher that reads workflow files, with a token to say things
    /// on a commit with.
    async fn github_dispatcher(db: &Db, base: &str) -> (Dispatcher, StubGithub) {
        let (origin, gh) = stub_github().await;
        let d = Dispatcher::new(
            db.clone(),
            BTreeMap::from([("default".into(), base.into())]),
            "dgr_token".into(),
            Arc::new(Forge::new(Some("ghp_test".into())).with_origin(&origin)),
            compile::Settings::default(),
        )
        .with_github_workflows(true);
        (d, gh)
    }

    fn files(gh: &StubGithub, files: &[(&str, &str)]) {
        gh.lock().unwrap().files = files
            .iter()
            .map(|(path, text)| (path.to_string(), text.to_string()))
            .collect();
    }

    /// A schedule's event runs the one workflow file it names, as GitHub
    /// runs a schedule. One whose workflow no longer takes its cron is
    /// passed over: nothing runs, and nothing is said on the commit.
    #[tokio::test]
    async fn a_schedule_runs_the_workflow_it_names() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, "")]).await;
        let (_repo, mut ev) = claimed(&db, None, Kind::Schedule, Trust::Trusted).await;
        ev.pipeline = Some(".github/workflows/nightly.yml".into());
        ev.inputs = BTreeMap::from([(compile::SCHEDULE_INPUT.to_string(), "0 3 * * *".into())]);
        let (d, gh) = github_dispatcher(&db, &base).await;
        pipelines(&gh, &[]);
        let job = "jobs:\n  a:\n    runs-on: ubuntu-latest\n    steps: [{ run: make nightly }]\n";
        files(
            &gh,
            &[
                (
                    ".dagron/ci.yaml",
                    "github:\n  workflows: [nightly.yml, build.yml]\n  \
                     runs_on: { ubuntu-latest: \"docker.io/library/ubuntu:24.04\" }\n",
                ),
                (
                    ".github/workflows/nightly.yml",
                    &format!("on:\n  schedule:\n    - cron: '0 3 * * *'\n{job}"),
                ),
                (".github/workflows/build.yml", &format!("on: push\n{job}")),
            ],
        );
        let Finish::Done(why) = d.handle(&ev).await else {
            panic!("the schedule runs nightly.yml");
        };
        assert!(why.contains("nightly.yml: submitted"), "{why}");
        assert_eq!(seen.lock().unwrap().len(), 1, "build.yml is not run");

        // The cron is gone from the file: passed over, quietly.
        ev.inputs = BTreeMap::from([(compile::SCHEDULE_INPUT.to_string(), "0 4 * * *".into())]);
        ev.commit_key = format!("{}-again", ev.commit_key);
        let finish = d.handle(&ev).await;
        assert!(
            !matches!(finish, Finish::Failed(_)),
            "a schedule its workflow no longer has fails nothing"
        );
        assert_eq!(seen.lock().unwrap().len(), 1, "nothing more runs");
        assert!(
            gh.lock().unwrap().statuses.is_empty(),
            "nothing said on the commit"
        );
    }

    /// A workflow that runs is given its run's numbers and is read with
    /// them: `github.run_number` counts its runs, `github.run_id` every
    /// run's, and a concurrency group made of them is the run's own. One its
    /// `on:` leaves out is given none, so its count is of its runs.
    #[tokio::test]
    async fn a_workflow_that_runs_is_given_its_run_s_numbers() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, "")]).await;
        let (_repo, ev) = claimed(&db, None, Kind::Push, Trust::Trusted).await;
        let (d, gh) = github_dispatcher(&db, &base).await;
        pipelines(&gh, &[]);
        files(
            &gh,
            &[
                (
                    ".dagron/ci.yaml",
                    "github:\n  workflows: [build.yml, prs.yml]\n  \
                     runs_on: { ubuntu-latest: \"docker.io/library/ubuntu:24.04\" }\n",
                ),
                (
                    ".github/workflows/build.yml",
                    "on: push\nconcurrency: \"${{ github.workflow }}-${{ github.head_ref || github.run_id }}\"\n\
                     jobs:\n  test:\n    runs-on: ubuntu-latest\n    \
                     env: { N: \"${{ github.run_number }}\" }\n    steps: [{ run: make test }]\n",
                ),
                (
                    ".github/workflows/prs.yml",
                    "on: pull_request\njobs:\n  test:\n    runs-on: ubuntu-latest\n    \
                     steps: [{ run: make test }]\n",
                ),
            ],
        );
        let Finish::Done(_) = d.handle(&ev).await else {
            panic!("the push runs build.yml");
        };
        let submitted = seen.lock().unwrap().clone();
        assert_eq!(submitted.len(), 1, "prs.yml does not run for a push");
        let yaml = submitted[0].yaml.as_deref().unwrap();
        let dag: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();
        let env = dag["tasks"][0]["env"].as_sequence().unwrap();
        assert!(
            env.iter().any(|e| e["name"] == "N" && e["value"] == "1"),
            "its first run: {yaml}"
        );
        assert!(yaml.contains("build-1"), "a group of the run's own: {yaml}");
        // The event handled again (its run's record lost, say) gives the run
        // the numbers it had: what it submits is what it submitted.
        let build = ".github/workflows/build.yml";
        let key = idempotency_key(&ev.commit_key, build);
        assert_eq!(
            db.run_numbers(&ev.repo.id, build, &key).await.unwrap(),
            (1, 1)
        );
        // prs.yml took no number: the next of its count is its first.
        assert_eq!(
            db.run_numbers(&ev.repo.id, ".github/workflows/prs.yml", "another run")
                .await
                .unwrap(),
            (2, 1)
        );
        assert_eq!(
            db.run_numbers(&ev.repo.id, build, "a later run")
                .await
                .unwrap(),
            (3, 2)
        );
        // Another repository counts its own runs.
        assert_eq!(
            db.run_numbers("another-repo", build, "its run")
                .await
                .unwrap(),
            (1, 1)
        );
    }

    /// The actions a workflow's actions use are fetched with its own: a
    /// local composite action whose step uses one of another repository,
    /// which uses another in turn, each read where the operator's mirror
    /// says its files are; held to the same allow-list and pins.
    #[tokio::test]
    async fn the_actions_an_action_uses_are_fetched_with_the_job() {
        const PIN: &str = "1111111111111111111111111111111111111111";
        const INNER: &str = "2222222222222222222222222222222222222222";
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, "")]).await;
        let (_repo, ev) = claimed(&db, None, Kind::Push, Trust::Trusted).await;
        let (origin, gh) = crate::forge::tests::stub_github().await;
        let source = |allow: &str| {
            github::ActionSource::parse(&origin, allow)
                .unwrap()
                .with_raw(&format!("{origin}/raw/{{owner}}/{{repo}}/{{ref}}/{{path}}"))
                .unwrap()
        };
        let d = Dispatcher::new(
            db.clone(),
            BTreeMap::from([("default".into(), base)]),
            "dgr_token".into(),
            Arc::new(Forge::new(Some("ghp_test".into())).with_origin(&origin)),
            compile::Settings::default(),
        )
        .with_github_workflows(true)
        .with_actions(source(""));
        pipelines(&gh, &[]);
        let workflow = "on: push\njobs:\n  a:\n    runs-on: ubuntu-latest\n    steps:\n      \
                        - uses: ./.github/actions/outer\n      - run: make\n";
        files(
            &gh,
            &[
                (
                    ".dagron/ci.yaml",
                    "github:\n  workflows: [build.yml]\n  \
                     runs_on: { ubuntu-latest: \"docker.io/library/ubuntu:24.04\" }\n",
                ),
                (".github/workflows/build.yml", workflow),
                (
                    ".github/actions/outer/action.yml",
                    &format!(
                        "runs:\n  using: composite\n  steps:\n    - uses: octo/tool@{PIN}\n    \
                         - uses: docker://alpine:3.20\n"
                    ),
                ),
            ],
        );
        gh.lock().unwrap().raw = vec![(
            format!("octo/tool/{PIN}/action.yml"),
            format!("runs:\n  using: composite\n  steps:\n    - uses: octo/inner/sub@{INNER}\n"),
        )];
        let Finish::Done(why) = d.handle(&ev).await else {
            panic!("the workflow runs");
        };
        assert!(why.contains("build.yml: submitted"), "{why}");
        let yaml = seen.lock().unwrap()[0].yaml.clone().unwrap();
        let dag: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();
        let fetched: Vec<&str> = dag["tasks"][0]["workspace"]["actions"]
            .as_sequence()
            .unwrap()
            .iter()
            .map(|a| a["repository"].as_str().unwrap())
            .collect();
        assert_eq!(
            fetched,
            [
                format!("{origin}/octo/tool.git"),
                format!("{origin}/octo/inner.git")
            ],
            "{yaml}"
        );

        // Where only pinned actions of octo's are allowed, an inner one at
        // a tag is refused, as a workflow's own step's would be.
        let mut ev = ev;
        ev.commit_key = format!("{}-allow", ev.commit_key);
        gh.lock().unwrap().raw = vec![(
            format!("octo/tool/{PIN}/action.yml"),
            "runs:\n  using: composite\n  steps:\n    - uses: octo/inner@v1\n".to_string(),
        )];
        let d = d.with_actions(source("octo/*"));
        let Finish::Failed(why) = d.handle(&ev).await else {
            panic!("an unpinned inner action is refused");
        };
        assert!(
            why.contains("./.github/actions/outer: a step of it") || why.contains("octo/tool@"),
            "{why}"
        );
        assert!(why.contains("pinned to a commit's full id"), "{why}");

        // A raw host that cannot answer now has the event handled again,
        // with nothing submitted; one that refuses for good leaves the
        // action to the step runner, and the run is made.
        let d = d.with_actions(source(""));
        for (answer, retried) in [("status:503", true), ("status:403", false)] {
            ev.commit_key = format!("{}-{answer}", ev.commit_key);
            gh.lock().unwrap().raw =
                vec![(format!("octo/tool/{PIN}/action.yml"), answer.to_string())];
            let submitted = seen.lock().unwrap().len();
            match d.handle(&ev).await {
                Finish::Retry { reason, .. } if retried => {
                    assert!(reason.contains("503"), "{reason}");
                    assert_eq!(
                        seen.lock().unwrap().len(),
                        submitted,
                        "nothing was submitted"
                    );
                }
                Finish::Done(why) if !retried => {
                    assert!(why.contains("build.yml: submitted"), "{why}")
                }
                other => panic!("{answer}: {other:?}"),
            }
        }
        // A raw host that cannot be reached at all is where dagron-ci runs:
        // the action is left to the step runner, and the run is made.
        ev.commit_key = format!("{}-unreachable", ev.commit_key);
        let d = d.with_actions(
            github::ActionSource::parse(&origin, "")
                .unwrap()
                .with_raw("http://127.0.0.1:1/{owner}/{repo}/{ref}/{path}")
                .unwrap(),
        );
        let Finish::Done(why) = d.handle(&ev).await else {
            panic!("a raw host that cannot be reached holds up no run");
        };
        assert!(why.contains("build.yml: submitted"), "{why}");
    }

    /// In a trusted run, an action of the repository's own owner's is
    /// fetched with a credential, as it may be private, and its file is read
    /// through the forge as the repository's own are, at the action's ref,
    /// where its repository shares its actions as GitHub lets a workflow use
    /// them: what it uses of the owner's is fetched the same. One whose
    /// repository shares none is not read.
    #[tokio::test]
    async fn the_owner_s_own_actions_are_fetched_and_read_with_a_credential() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, "")]).await;
        let (_repo, ev) = claimed(&db, None, Kind::Push, Trust::Trusted).await;
        let (origin, gh) = crate::forge::tests::stub_github().await;
        let Some(forge) = crate::forge::tests::app_forge(&origin, Some("ghp_test")) else {
            return;
        };
        {
            let mut gh = gh.lock().unwrap();
            gh.installed = ["acme/widgets", "acme/tool", "acme/other"]
                .into_iter()
                .map(String::from)
                .collect();
            gh.shared = vec![
                ("acme/tool".into(), true, "organization".into()),
                ("acme/other".into(), true, "organization".into()),
            ];
        }
        let d = Dispatcher::new(
            db.clone(),
            BTreeMap::from([("default".into(), base)]),
            "dgr_token".into(),
            Arc::new(forge),
            compile::Settings::default(),
        )
        .with_github_workflows(true);
        pipelines(&gh, &[]);
        files(
            &gh,
            &[
                (
                    ".dagron/ci.yaml",
                    "github:\n  workflows: [build.yml]\n  \
                     runs_on: { ubuntu-latest: \"docker.io/library/ubuntu:24.04\" }\n",
                ),
                (
                    ".github/workflows/build.yml",
                    "on: push\njobs:\n  a:\n    runs-on: ubuntu-latest\n    steps:\n      \
                     - uses: acme/tool/greet@v1\n",
                ),
            ],
        );
        gh.lock().unwrap().theirs = vec![
            (
                "acme/tool/greet/action.yml".into(),
                "v1".into(),
                "runs:\n  using: composite\n  steps:\n    - uses: acme/other@v2\n    \
                 - uses: docker://alpine:3.20\n"
                    .into(),
            ),
            (
                "acme/other/action.yml".into(),
                "v2".into(),
                "runs:\n  using: node20\n  main: index.js\n".into(),
            ),
        ];
        let Finish::Done(why) = d.handle(&ev).await else {
            panic!("the workflow runs");
        };
        assert!(why.contains("build.yml: submitted"), "{why}");
        let yaml = seen.lock().unwrap()[0].yaml.clone().unwrap();
        let dag: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();
        let fetched: Vec<(&str, bool)> = dag["tasks"][0]["workspace"]["actions"]
            .as_sequence()
            .unwrap()
            .iter()
            .map(|a| {
                (
                    a["repository"].as_str().unwrap(),
                    a["credential"].as_bool().unwrap_or(false),
                )
            })
            .collect();
        assert_eq!(
            fetched,
            [
                ("https://github.com/acme/tool.git", true),
                ("https://github.com/acme/other.git", true)
            ],
            "{yaml}"
        );

        // A repository that shares none of its actions is not read: what
        // its action uses is not fetched.
        let mut ev = ev;
        ev.commit_key = format!("{}-unshared", ev.commit_key);
        gh.lock().unwrap().shared[0].2 = "none".into();
        let Finish::Done(why) = d.handle(&ev).await else {
            panic!("the workflow runs");
        };
        assert!(why.contains("build.yml: submitted"), "{why}");
        let yaml = seen.lock().unwrap().last().unwrap().yaml.clone().unwrap();
        let dag: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();
        let fetched: Vec<&str> = dag["tasks"][0]["workspace"]["actions"]
            .as_sequence()
            .unwrap()
            .iter()
            .map(|a| a["repository"].as_str().unwrap())
            .collect();
        assert_eq!(fetched, ["https://github.com/acme/tool.git"], "{yaml}");
        gh.lock().unwrap().shared[0].2 = "organization".into();

        // The forge failing as it answers for the owner's action has the
        // event handled again, with nothing submitted.
        ev.commit_key = format!("{}-503", ev.commit_key);
        gh.lock().unwrap().theirs[0].2 = "status:503".into();
        let submitted = seen.lock().unwrap().len();
        let Finish::Retry { .. } = d.handle(&ev).await else {
            panic!("the forge's failure is retried");
        };
        assert_eq!(
            seen.lock().unwrap().len(),
            submitted,
            "nothing was submitted"
        );
    }

    /// Reading actions' files is bounded: an action that uses itself is
    /// read once, a job fetches no more than [`github::MAX_ACTIONS`], and an
    /// event reads no more than [`crate::nested::MAX_READS`] files.
    #[tokio::test]
    async fn an_action_s_actions_are_read_within_bounds() {
        const PIN: &str = "1111111111111111111111111111111111111111";
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, "")]).await;
        let (_repo, ev) = claimed(&db, None, Kind::Push, Trust::Trusted).await;
        let (origin, gh) = crate::forge::tests::stub_github().await;
        let d = Dispatcher::new(
            db.clone(),
            BTreeMap::from([("default".into(), base)]),
            "dgr_token".into(),
            Arc::new(Forge::new(Some("ghp_test".into())).with_origin(&origin)),
            compile::Settings::default(),
        )
        .with_github_workflows(true)
        .with_actions(
            github::ActionSource::parse(&origin, "")
                .unwrap()
                .with_raw(&format!("{origin}/raw/{{owner}}/{{repo}}/{{ref}}/{{path}}"))
                .unwrap(),
        );
        pipelines(&gh, &[]);
        let settings = "github:\n  workflows: [build.yml]\n  \
                        runs_on: { ubuntu-latest: \"docker.io/library/ubuntu:24.04\" }\n";
        let workflow = "on: push\njobs:\n  a:\n    runs-on: ubuntu-latest\n    steps:\n      \
                        - uses: ./.github/actions/outer\n";
        let composite = |uses: &[String]| {
            let steps: String = uses.iter().map(|u| format!("    - uses: {u}\n")).collect();
            format!("runs:\n  using: composite\n  steps:\n{steps}")
        };

        // An action that uses itself, however it is spelled, is read once.
        let outer = composite(&[
            "./.github/actions/outer/".to_string(),
            format!("octo/loop@{PIN}"),
        ]);
        files(
            &gh,
            &[
                (".dagron/ci.yaml", settings),
                (".github/workflows/build.yml", workflow),
                (".github/actions/outer/action.yml", &outer),
            ],
        );
        gh.lock().unwrap().raw = vec![(
            format!("octo/loop/{PIN}/action.yml"),
            composite(&[format!("octo/loop@{PIN}")]),
        )];
        let Finish::Done(why) = d.handle(&ev).await else {
            panic!("the workflow runs");
        };
        assert!(why.contains("build.yml: submitted"), "{why}");
        let yaml = seen.lock().unwrap()[0].yaml.clone().unwrap();
        let dag: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();
        let fetched = dag["tasks"][0]["workspace"]["actions"]
            .as_sequence()
            .unwrap()
            .len();
        assert_eq!(fetched, 1, "{yaml}");

        // An action that uses more than a job fetches is refused.
        let mut ev = ev;
        ev.commit_key = format!("{}-wide", ev.commit_key);
        let wide: Vec<String> = (0..=github::MAX_ACTIONS)
            .map(|i| format!("octo/a{i}@{PIN}"))
            .collect();
        gh.lock().unwrap().raw = vec![(format!("octo/loop/{PIN}/action.yml"), composite(&wide))];
        let Finish::Failed(why) = d.handle(&ev).await else {
            panic!("too many actions are refused");
        };
        assert!(why.contains("more than 32 actions"), "{why}");

        // Local actions with no file each cost two reads, and the event's
        // reads run out.
        ev.commit_key = format!("{}-deep", ev.commit_key);
        let many: Vec<String> = (0..40).map(|i| format!("./missing/{i}")).collect();
        let outer = composite(&many);
        files(
            &gh,
            &[
                (".dagron/ci.yaml", settings),
                (".github/workflows/build.yml", workflow),
                (".github/actions/outer/action.yml", &outer),
            ],
        );
        let Finish::Failed(why) = d.handle(&ev).await else {
            panic!("reading stops at the event's bound");
        };
        assert!(why.contains("for one event (64)"), "{why}");
    }

    /// An action is read for the least deep place it runs, whatever else
    /// reaches it deeper first: what it uses is fetched. And a step that
    /// asks nobody for a file (an image) spends none of the event's reads.
    #[tokio::test]
    async fn an_action_reached_deep_first_is_read_for_its_shallowest_place() {
        const PIN: &str = "1111111111111111111111111111111111111111";
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, ""), (201, "")]).await;
        let (_repo, ev) = claimed(&db, None, Kind::Push, Trust::Trusted).await;
        let (origin, gh) = crate::forge::tests::stub_github().await;
        let d = Dispatcher::new(
            db.clone(),
            BTreeMap::from([("default".into(), base)]),
            "dgr_token".into(),
            Arc::new(Forge::new(Some("ghp_test".into())).with_origin(&origin)),
            compile::Settings::default(),
        )
        .with_github_workflows(true)
        .with_actions(
            github::ActionSource::parse(&origin, "")
                .unwrap()
                .with_raw(&format!("{origin}/raw/{{owner}}/{{repo}}/{{ref}}/{{path}}"))
                .unwrap(),
        );
        pipelines(&gh, &[]);
        let settings = "github:\n  workflows: [build.yml]\n  \
                        runs_on: { ubuntu-latest: \"docker.io/library/ubuntu:24.04\" }\n";
        let composite =
            |uses: &str| format!("runs:\n  using: composite\n  steps:\n    - uses: {uses}\n");
        // ./c1 uses ./c2 … ./c9 uses ./x: nine deep. The job's own step
        // uses ./x too, which uses octo/inner.
        let mut owned: Vec<(String, String)> = (1..=9)
            .map(|i| {
                let next = if i == 9 {
                    "./x".to_string()
                } else {
                    format!("./c{}", i + 1)
                };
                (format!("c{i}/action.yml"), composite(&next))
            })
            .collect();
        owned.push((
            "x/action.yml".into(),
            composite(&format!("octo/inner@{PIN}")),
        ));
        let workflow = "on: push\njobs:\n  a:\n    runs-on: ubuntu-latest\n    steps:\n      \
                        - uses: ./x\n      - uses: ./c1\n";
        let mut all: Vec<(&str, &str)> = vec![
            (".dagron/ci.yaml", settings),
            (".github/workflows/build.yml", workflow),
        ];
        all.extend(owned.iter().map(|(p, t)| (p.as_str(), t.as_str())));
        files(&gh, &all);
        gh.lock().unwrap().raw = vec![(
            format!("octo/inner/{PIN}/action.yml"),
            "runs:\n  using: node20\n  main: index.js\n".to_string(),
        )];
        let Finish::Done(why) = d.handle(&ev).await else {
            panic!("the workflow runs");
        };
        assert!(why.contains("build.yml: submitted"), "{why}");
        let yaml = seen.lock().unwrap()[0].yaml.clone().unwrap();
        let dag: serde_yaml::Value = serde_yaml::from_str(&yaml).unwrap();
        let fetched: Vec<&str> = dag["tasks"][0]["workspace"]["actions"]
            .as_sequence()
            .unwrap()
            .iter()
            .map(|a| a["repository"].as_str().unwrap())
            .collect();
        assert_eq!(fetched, [format!("{origin}/octo/inner.git")], "{yaml}");

        // Seventy images: nothing is read for them, and the job runs.
        let mut ev = ev;
        ev.commit_key = format!("{}-images", ev.commit_key);
        let steps: String = (0..70)
            .map(|i| format!("      - uses: docker://alpine:3.{i}\n"))
            .collect();
        let workflow =
            format!("on: push\njobs:\n  a:\n    runs-on: ubuntu-latest\n    steps:\n{steps}");
        files(
            &gh,
            &[
                (".dagron/ci.yaml", settings),
                (".github/workflows/build.yml", &workflow),
            ],
        );
        let Finish::Done(why) = d.handle(&ev).await else {
            panic!("the images' workflow runs");
        };
        assert!(why.contains("build.yml: submitted"), "{why}");
    }

    /// A workflow file the repository lists in its settings is run as a
    /// pipeline, beside its native ones: read for the event, compiled, and
    /// submitted under its own path. A job its `if:` leaves out says so on
    /// the commit, as a job GitHub skipped does. A file that is listed and
    /// is not there fails the event, which says so.
    #[tokio::test]
    async fn a_listed_workflow_file_is_run_as_a_pipeline() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, "")]).await;
        let (_repo, ev) = claimed(&db, None, Kind::Push, Trust::Trusted).await;
        let (d, gh) = github_dispatcher(&db, &base).await;
        pipelines(&gh, &[("ci.yaml", CI)]);
        files(
            &gh,
            &[
                (".dagron/ci.yaml", SETTINGS),
                (".github/workflows/build.yml", BUILD),
                // Not listed: it is not read, whatever it holds.
                (".github/workflows/other.yml", "not: a workflow"),
            ],
        );

        let Finish::Failed(why) = d.handle(&ev).await else {
            panic!("a listed file that is not there fails the event");
        };
        assert!(
            why.starts_with("ci.yaml: submitted run run-1; ")
                && why.contains(".github/workflows/build.yml: submitted run run-2; ")
                && why.contains(
                    ".github/workflows/absent.yml: refused, is listed in .dagron/ci.yaml, and \
                     the commit has no such file"
                ),
            "{why}"
        );
        let submitted = seen.lock().unwrap().clone();
        assert_eq!(submitted.len(), 2);
        let build = &submitted[1];
        assert_eq!(
            build.key,
            idempotency_key(&ev.commit_key, ".github/workflows/build.yml")
        );
        let yaml = build.yaml.as_deref().unwrap();
        assert!(yaml.starts_with("name: build\n"), "{yaml}");
        let dag: serde_yaml::Value = serde_yaml::from_str(yaml).unwrap();
        let tasks = dag["tasks"].as_sequence().unwrap();
        assert_eq!(tasks.len(), 1, "release is left out for a push: {yaml}");
        assert_eq!(tasks[0]["name"], "test");
        assert_eq!(tasks[0]["docker_image"], "docker.io/library/ubuntu:24.04");
        assert!(
            tasks[0]["workspace"]["checkout"]["commit"].is_string(),
            "the event's commit, which its checkout step asked for"
        );
        assert!(
            !gh.lock()
                .unwrap()
                .seen
                .iter()
                .any(|(path, _, _)| path.contains("other.yml")),
            "a file that is not listed is not asked for"
        );
        // The job left out has a passing status that says it was skipped,
        // and the file that is not there one that says it was not run.
        let said: Vec<(String, String, String)> = gh
            .lock()
            .unwrap()
            .statuses
            .iter()
            .map(|(_, _, s)| {
                (
                    s["context"].as_str().unwrap().to_string(),
                    s["state"].as_str().unwrap().to_string(),
                    s["description"].as_str().unwrap().to_string(),
                )
            })
            .collect();
        assert!(
            said.contains(&(
                "dagron-ci/build/release (push)".to_string(),
                "success".to_string(),
                "skipped: its if: is false".to_string()
            )),
            "{said:#?}"
        );
        assert!(
            said.iter()
                .any(|(context, state, _)| context == "dagron-ci/absent (push)" && state == "error"),
            "{said:#?}"
        );
    }

    /// Two events of one commit can leave different jobs of a workflow out:
    /// here `deploy` is main's alone, and the commit is on a second branch
    /// too. The second branch's event says `deploy` skipped only while no
    /// run has it: once main's run has posted under it, that run's word
    /// stands, a failure above all, and it is told to post again each time
    /// in case a "skipped" reached the forge after it. A job no event runs
    /// is said skipped.
    #[tokio::test]
    async fn a_job_left_out_is_not_said_skipped_over_another_event_s_run_of_it() {
        const ONE: &str = "github:\n  workflows: [build.yml]\n  \
                           runs_on: { ubuntu-latest: \"docker.io/library/ubuntu:24.04\" }\n";
        const WORKFLOW: &str = "on: push\njobs:\n  \
             test:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make test\n  \
             deploy:\n    needs: test\n    if: github.ref == 'refs/heads/main'\n    \
             runs-on: ubuntu-latest\n    steps:\n      - run: make deploy\n  \
             never:\n    if: github.event_name == 'release'\n    \
             runs-on: ubuntu-latest\n    steps:\n      - run: make never\n";
        let (db, _dir) = sqlite().await;
        let (base, _) = stub_api(vec![(201, "")]).await;
        // main's event, and its run of the workflow, which has `deploy`.
        let (repo, main) = claimed(&db, None, Kind::Push, Trust::Trusted).await;
        assert!(db
            .record_run(&SubmittedRun {
                run_id: "run-main".into(),
                event_id: main.id.clone(),
                repo_id: repo.id.clone(),
                cell: "default".into(),
                pipeline: ".github/workflows/build.yml".into(),
                idempotency_key: "ci-main".into(),
                on_failed: false,
            })
            .await
            .unwrap());
        // The same commit, on another branch.
        let mut pushed = event();
        pushed.git_ref = "refs/heads/release".into();
        pushed.base_sha = Some("c".repeat(40));
        db.record_delivery(&repo, &uuid::Uuid::now_v7().to_string(), &pushed, b"{}")
            .await
            .unwrap();
        let ev = db
            .claim_event("w", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (ev.git_ref.as_str(), &ev.sha),
            ("refs/heads/release", &main.sha)
        );
        let (d, gh) = github_dispatcher(&db, &base).await;
        files(
            &gh,
            &[
                (".dagron/ci.yaml", ONE),
                (".github/workflows/build.yml", WORKFLOW),
            ],
        );
        let said = |gh: &StubGithub| -> Vec<(String, String)> {
            let mut gh = gh.lock().unwrap();
            let said = gh
                .statuses
                .iter()
                .map(|(_, _, s)| {
                    (
                        s["context"].as_str().unwrap().to_string(),
                        s["description"].as_str().unwrap().to_string(),
                    )
                })
                .collect();
            gh.statuses.clear();
            said
        };
        let skipped = |job: &str, why: &str| {
            (
                format!("dagron-ci/build/{job} (push)"),
                format!("skipped: {why}"),
            )
        };
        let reporter = Duration::from_secs(60);

        // main's run has posted nothing yet: nothing says it has `deploy`,
        // so both jobs this event leaves out are said skipped. And main's
        // run is told to post, so that what it posts is the last word.
        let Finish::Done(why) = d.handle(&ev).await else {
            panic!("the workflow runs for a push to any branch");
        };
        assert!(why.contains("build.yml: submitted run run-1"), "{why}");
        assert_eq!(
            said(&gh),
            [
                skipped("deploy", "its if: is false"),
                skipped("never", "its if: is false")
            ]
        );
        let mut due = BTreeMap::new();
        while let Some((run, report)) = db.claim_report("r", reporter).await.unwrap() {
            due.insert(run, report.expect("its event is there").reported);
        }
        assert_eq!(
            due.keys().map(String::as_str).collect::<Vec<_>>(),
            ["run-1", "run-main"]
        );

        // main's run is looked at: its `deploy` failed, and the commit says so.
        let failed = crate::db::ReportWrite {
            reported: BTreeMap::from([
                (
                    "dagron-ci/build/test (push)".to_string(),
                    "success:succeeded".to_string(),
                ),
                (
                    "dagron-ci/build/deploy (push)".to_string(),
                    "failure:failed".to_string(),
                ),
            ]),
            status: Some("failed".into()),
            ..Default::default()
        };
        assert!(db.finish_report("run-main", "r", &failed).await.unwrap());

        // The event is handled again, as one cut short is. `deploy` has a
        // run's word now, and no "skipped" is put over it; `never` has
        // none. main's run is due again with nothing on record as posted:
        // whatever an earlier handling said under `deploy`, its failure is
        // posted after. This event's own run is left as it is.
        let Finish::Done(why) = d.handle(&ev).await else {
            panic!("handled again, it is done again");
        };
        assert!(why.contains("build.yml: run run-1"), "{why}");
        assert_eq!(said(&gh), [skipped("never", "its if: is false")]);
        let mut due = BTreeMap::new();
        while let Some((run, report)) = db.claim_report("r", reporter).await.unwrap() {
            due.insert(run, report.expect("its event is there").reported);
        }
        assert_eq!(
            due.keys().map(String::as_str).collect::<Vec<_>>(),
            ["run-main"]
        );
        assert!(due["run-main"].is_empty(), "{due:?}");

        // A "skipped" that cannot be posted has the event handled again. If
        // this was its last attempt it is not, and a "skipped" of an earlier
        // handling may be on the commit: main's run is told to post again
        // all the same.
        assert!(db.finish_report("run-main", "r", &failed).await.unwrap());
        assert!(db.claim_report("r", reporter).await.unwrap().is_none());
        gh.lock().unwrap().status_answer = Some((502, vec![]));
        assert!(matches!(d.handle(&ev).await, Finish::Retry { .. }));
        let (run, due) = db.claim_report("r", reporter).await.unwrap().unwrap();
        assert_eq!(run, "run-main");
        assert!(due.expect("its event is there").reported.is_empty());
    }

    /// With the setting off, which it is unless an operator turns it on, no
    /// repository is asked for its settings, and no workflow file is read.
    #[tokio::test]
    async fn no_workflow_file_is_read_unless_the_operator_says_so() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, "")]).await;
        let (_repo, ev) = claimed(&db, None, Kind::Push, Trust::Trusted).await;
        let (d, gh) = dispatcher(&db, &base).await;
        pipelines(&gh, &[("ci.yaml", CI)]);
        files(
            &gh,
            &[
                (".dagron/ci.yaml", SETTINGS),
                (".github/workflows/build.yml", BUILD),
            ],
        );
        let Finish::Done(why) = d.handle(&ev).await else {
            panic!("the push runs ci");
        };
        assert_eq!(why, "ci.yaml: submitted run run-1");
        assert_eq!(seen.lock().unwrap().len(), 1);
        assert!(
            !gh.lock()
                .unwrap()
                .seen
                .iter()
                .any(|(path, _, _)| path.contains(".dagron/ci.yaml") || path.contains("workflows")),
            "{:?}",
            gh.lock().unwrap().seen
        );
    }

    /// A repository's mode says which of its files are run. In native
    /// mode its workflow files are not read, though the build reads them and
    /// its settings list them; with no pipelines of its own, it says how
    /// they would be. In fleet mode, its forge runs its pipelines: an event
    /// of it that waited runs nothing.
    #[tokio::test]
    async fn a_repository_s_mode_says_which_of_its_files_are_run() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, "")]).await;
        let (_repo, mut ev) = claimed(&db, None, Kind::Push, Trust::Trusted).await;
        ev.repo.mode = Mode::Native;
        let (d, gh) = github_dispatcher(&db, &base).await;
        pipelines(&gh, &[("ci.yaml", CI)]);
        files(
            &gh,
            &[
                (".dagron/ci.yaml", SETTINGS),
                (".github/workflows/build.yml", BUILD),
            ],
        );
        let Finish::Done(why) = d.handle(&ev).await else {
            panic!("the push runs ci");
        };
        assert_eq!(why, "ci.yaml: submitted run run-1");
        assert_eq!(seen.lock().unwrap().len(), 1);
        assert!(
            !gh.lock()
                .unwrap()
                .seen
                .iter()
                .any(|(path, _, _)| path.contains(".dagron/ci.yaml") || path.contains("workflows")),
            "{:?}",
            gh.lock().unwrap().seen
        );

        pipelines(&gh, &[]);
        let Finish::Skipped(why) = d.handle(&ev).await else {
            panic!("nothing of its own runs");
        };
        assert!(
            why.contains(
                "the forge's own files are read for a repository in compat mode alone \
                 (`dagron-ci repo set github.com/acme/widgets --mode compat`)"
            ),
            "{why}"
        );

        ev.repo.mode = Mode::Fleet;
        pipelines(&gh, &[("ci.yaml", CI)]);
        assert_eq!(
            d.handle(&ev).await,
            Finish::Skipped(crate::db::FLEET_MODE.into())
        );
        assert_eq!(seen.lock().unwrap().len(), 1, "no run was asked for");
    }

    /// A workflow file and a pipeline of one name would report under one
    /// another's checks: the workflow file is not run, and says why. And
    /// settings that cannot be read fail the event the same way.
    #[tokio::test]
    async fn a_workflow_file_named_as_a_pipeline_is_not_run_beside_it() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, "")]).await;
        let (_repo, ev) = claimed(&db, None, Kind::Push, Trust::Trusted).await;
        let (d, gh) = github_dispatcher(&db, &base).await;
        pipelines(&gh, &[("ci.yaml", CI)]);
        files(
            &gh,
            &[
                (
                    ".dagron/ci.yaml",
                    "github:\n  workflows: [ci.yml]\n  runs_on: { ubuntu-latest: \"ubuntu:24.04\" }\n",
                ),
                (".github/workflows/ci.yml", BUILD),
            ],
        );
        let Finish::Failed(why) = d.handle(&ev).await else {
            panic!("the workflow file is refused");
        };
        assert!(
            why.contains(
                ".github/workflows/ci.yml: refused, a pipeline in .dagron/ci is called ci too"
            ),
            "{why}"
        );
        assert_eq!(seen.lock().unwrap().len(), 1, "the pipeline itself ran");

        let (db, _dir) = sqlite().await;
        let (_repo, ev) = claimed(&db, None, Kind::Push, Trust::Trusted).await;
        let (d, gh) = github_dispatcher(&db, &base).await;
        files(
            &gh,
            &[(".dagron/ci.yaml", "github: { workflws: [ci.yml] }")],
        );
        let Finish::Failed(why) = d.handle(&ev).await else {
            panic!("settings with a mistake in them fail the event");
        };
        assert!(
            why.contains("invalid: github.workflws: is not a key this takes"),
            "{why}"
        );
    }

    const FIX: &str = "name: fix\non: { failed: [ci] }\n\
                       tasks:\n  - { name: agent, steps: [{ run: \"echo $CI_FAILED_JOB\" }] }\n";

    /// A run asked for by name, as a dispatch's or a schedule's is, raises
    /// its failure as a push's does: the pipeline that waits on it is read
    /// with the rest, not passed over with the pipelines not asked for.
    #[tokio::test]
    async fn a_run_asked_for_by_name_raises_its_failure_too() {
        const NAMED: &str = "name: ci\non: { push: {}, dispatch: {} }\n\
                             tasks:\n  - { name: test, docker_image: \"rust:1\", steps: [{ run: cargo test }] }\n";
        let (db, _dir) = sqlite().await;
        let (base, _seen) = stub_api(vec![(201, "")]).await;
        let (_repo, mut ev) = claimed(&db, None, Kind::Dispatch, Trust::Trusted).await;
        ev.pipeline = Some("ci".into());
        let (d, gh) = dispatcher(&db, &base).await;
        pipelines(&gh, &[("ci.yaml", NAMED), ("fix.yaml", FIX)]);
        let Finish::Done(why) = d.handle(&ev).await else {
            panic!("the dispatch runs ci");
        };
        assert_eq!(why, "ci.yaml: submitted run run-1");
        let (run, _) = db
            .claim_report("r", Duration::from_secs(60))
            .await
            .unwrap()
            .expect("a run is due");
        let write = crate::db::ReportWrite {
            status: Some("failed".to_string()),
            ..crate::db::ReportWrite::default()
        };
        assert!(db.finish_report(&run, "r", &write).await.unwrap());
        let due = db
            .claim_failure(Duration::from_secs(60))
            .await
            .unwrap()
            .expect("ci's failure is due, though fix was not asked for");
        assert_eq!(due.run_id, "run-1");
    }

    /// A trusted run of a pipeline another of its commit's takes
    /// `on.failed` for raises a `failed` event as it fails, once; that event
    /// runs the pipeline that waits on it, and that alone, with the failed
    /// run's facts; and what it runs raises no failure of its own.
    #[tokio::test]
    async fn a_failed_run_runs_the_pipeline_that_waits_on_its_failure() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, "")]).await;
        let (_repo, ev) = claimed(&db, None, Kind::Push, Trust::Trusted).await;
        let (d, gh) = dispatcher(&db, &base).await;
        pipelines(&gh, &[("ci.yaml", CI), ("fix.yaml", FIX)]);
        let Finish::Done(why) = d.handle(&ev).await else {
            panic!("the push runs ci");
        };
        assert_eq!(
            why,
            "ci.yaml: submitted run run-1; fix.yaml: skipped, on: does not take push"
        );
        // Each run ends as the reporter sees it end: the failure of ci's is
        // due to raise its event, and nothing else is.
        let end = |status: &'static str| {
            let db = db.clone();
            async move {
                let (run, _) = db
                    .claim_report("r", Duration::from_secs(60))
                    .await
                    .unwrap()
                    .expect("a run is due");
                let write = crate::db::ReportWrite {
                    status: Some(status.to_string()),
                    ..crate::db::ReportWrite::default()
                };
                assert!(db.finish_report(&run, "r", &write).await.unwrap());
                run
            }
        };
        assert_eq!(end("failed").await, "run-1");
        let due = db
            .claim_failure(Duration::from_secs(60))
            .await
            .unwrap()
            .expect("ci's failure is due");
        assert_eq!(
            (due.run_id.as_str(), due.pipeline.as_str()),
            ("run-1", ".dagron/ci/ci.yaml")
        );
        assert!(
            db.claim_failure(Duration::from_secs(60))
                .await
                .unwrap()
                .is_none(),
            "held"
        );
        let inputs = BTreeMap::from([
            ("ci_failed_run".to_string(), "run-1".to_string()),
            (compile::FAILED_PIPELINE.to_string(), "ci".to_string()),
            ("ci_failed_job".to_string(), "test".to_string()),
        ]);
        let raised = db
            .raise_failure(&due, &inputs)
            .await
            .unwrap()
            .expect("raised");
        assert_eq!(db.raise_failure(&due, &inputs).await.unwrap(), None, "once");

        let failed = db
            .claim_event("w", Duration::from_secs(60))
            .await
            .unwrap()
            .expect("the failure's event is queued");
        assert_eq!(
            (
                failed.id.as_str(),
                failed.kind.as_str(),
                failed.trust.as_str()
            ),
            (raised.as_str(), "failed", "trusted")
        );
        assert_eq!(
            (failed.sha.as_str(), failed.git_ref.as_str()),
            (ev.sha.as_str(), ev.git_ref.as_str())
        );
        let Finish::Done(why) = d.handle(&failed).await else {
            panic!("the failure runs fix");
        };
        assert_eq!(
            why,
            "ci.yaml: skipped, on: does not take failed; fix.yaml: submitted run run-2"
        );
        let submitted = seen.lock().unwrap().clone();
        let fix = &submitted[1];
        assert_eq!(fix.params["ci_failed_pipeline"], "ci");
        assert_eq!(fix.params["ci_failed_run"], "run-1");
        assert_eq!(fix.params["ci_failed_job"], "test");
        assert_eq!(
            fix.params["ci_failed_fault_class"], "",
            "where the engine said none"
        );
        assert!(
            fix.yaml.as_deref().unwrap().contains("name: CI_FAILED_JOB"),
            "{:?}",
            fix.yaml
        );
        assert!(
            !submitted[0].yaml.as_deref().unwrap().contains("CI_FAILED"),
            "a pipeline that waits on no failure is given none of it"
        );
        assert!(
            gh.lock().unwrap().statuses.is_empty(),
            "nothing said on the commit of it"
        );
        // fix's own failure raises nothing, and a run that succeeded raises
        // nothing either.
        assert_eq!(end("failed").await, "run-2");
        assert!(db
            .claim_failure(Duration::from_secs(60))
            .await
            .unwrap()
            .is_none());
    }

    /// A commit with pipelines runs them, each as its own run, in place of
    /// the bound workflow; and a second event for the commit runs nothing
    /// more.
    #[tokio::test]
    async fn a_commit_s_pipelines_run_in_place_of_the_bound_workflow() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, "")]).await;
        let (repo, ev) = claimed(&db, Some("wf-1"), Kind::Push, Trust::Trusted).await;
        let (d, gh) = dispatcher(&db, &base).await;
        pipelines(
            &gh,
            &[
                ("ci.yaml", CI),
                ("lint.yaml", LINT),
                ("release.yml", RELEASE),
            ],
        );

        let Finish::Done(why) = d.handle(&ev).await else {
            panic!("the push runs ci and lint");
        };
        assert_eq!(
            why,
            "ci.yaml: submitted run run-1; lint.yaml: submitted run run-2; release.yml: \
             skipped, branch \"main\" is not in on.push.branches"
        );
        let submitted = seen.lock().unwrap().clone();
        assert_eq!(
            submitted.len(),
            2,
            "a run each, and the bound workflow not asked"
        );
        assert_eq!(
            submitted[1].key,
            idempotency_key(&ev.commit_key, ".dagron/ci/lint.yaml"),
            "each pipeline's run has its own key"
        );
        assert!(submitted[1]
            .yaml
            .as_deref()
            .unwrap()
            .starts_with("name: lint\n"));
        let s = &submitted[0];
        assert_eq!(s.workflow, None);
        assert_eq!(s.key, idempotency_key(&ev.commit_key, ".dagron/ci/ci.yaml"));
        let compiled = match compile::compile(
            &pipeline::parse(CI).unwrap(),
            &compile_event(&ev),
            &compile::Settings::default(),
        ) {
            Ok(Outcome::Run(c)) => c,
            other => panic!("{other:?}"),
        };
        assert_eq!(
            s.yaml.as_deref(),
            Some(compiled.yaml.as_str()),
            "what the compiler wrote"
        );
        assert_eq!(s.params, compiled.parameters);
        assert_eq!(s.params["ci_repo"], repo.full_name);

        let view = db.event_view(&ev.id).await.unwrap().unwrap();
        // Each as it stands: followed, until the reporter sees it end.
        let runs: Vec<_> = view
            .runs
            .iter()
            .map(|r| (&*r.id, &*r.cell, &*r.pipeline, &*r.status))
            .collect();
        assert_eq!(
            runs,
            [
                ("run-1", "default", ".dagron/ci/ci.yaml", "submitted"),
                ("run-2", "default", ".dagron/ci/lint.yaml", "submitted")
            ]
        );
        let keyed = view.runs.iter().all(|r| r.key.starts_with("ci-"));
        assert!(keyed, "{:?}", view.runs);

        // The commit again: its runs are recorded, and nothing is submitted.
        assert_eq!(
            d.handle(&ev).await,
            Finish::Done(
                "ci.yaml: run run-1 was submitted for this commit already; lint.yaml: run run-2 \
                 was submitted for this commit already; release.yml: skipped, branch \"main\" \
                 is not in on.push.branches"
                    .into()
            )
        );
        assert_eq!(seen.lock().unwrap().len(), 2);
    }

    /// A build's rules, for a test: a pipeline it requires of every
    /// repository, an image it refuses, and one forge login it knows.
    struct Rules;

    const SCAN: &str = "name: scan\non: { push: }\n\
                        tasks:\n  - { name: audit, steps: [{ run: cargo audit }] }\n";

    impl Governance for Rules {
        fn required(&self, forge_host: &str, full_name: &str) -> Vec<PipelineFile> {
            assert_eq!((forge_host, full_name), ("github.com", "acme/widgets"));
            vec![PipelineFile {
                path: format!("{}/scan.yaml", governance::REQUIRED_DIR),
                text: Ok(SCAN.into()),
            }]
        }

        fn review(&self, run: &Proposed<'_>) -> Result<(), String> {
            assert_eq!(run.repo, "github.com/acme/widgets");
            if run.yaml.contains("rust:1") {
                return Err("images-from-registry: images only from registry.acme.internal".into());
            }
            Ok(())
        }

        fn actor(&self, forge_host: &str, login: &str) -> Option<String> {
            (forge_host == "github.com" && login == "octocat").then(|| "alice@acme.example".into())
        }
    }

    /// Through the open build's seam: a pipeline the
    /// build requires runs beside the repository's own, under its own key; a
    /// run the build's rules refuse is not submitted, and says why on the
    /// commit as any refused pipeline does; and the audit hears of each.
    #[tokio::test]
    async fn a_build_s_rules_add_a_pipeline_and_refuse_a_run() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, "")]).await;
        let (_, ev) = claimed(&db, None, Kind::Push, Trust::Trusted).await;
        let (d, gh) = dispatcher(&db, &base).await;
        let kept = Arc::new(crate::audit::tests::Kept::default());
        let d = d.with_governance(Arc::new(Rules)).with_audit(kept.clone());
        pipelines(&gh, &[("ci.yaml", CI), ("lint.yaml", LINT)]);

        let Finish::Failed(why) = d.handle(&ev).await else {
            panic!("ci.yaml is refused");
        };
        assert!(
            why.contains(
                "ci.yaml: refused, refused by this installation's policy: \
                          images-from-registry: images only from registry.acme.internal"
            ) && why.contains("lint.yaml: submitted run run-1")
                && why.contains("scan.yaml: submitted run run-2"),
            "{why}"
        );
        let submitted = seen.lock().unwrap().clone();
        assert_eq!(submitted.len(), 2, "lint and the required scan");
        assert_eq!(
            submitted[1].key,
            idempotency_key(&ev.commit_key, ".dagron/required/scan.yaml")
        );
        assert!(submitted[1]
            .yaml
            .as_deref()
            .unwrap()
            .starts_with("name: scan\n"));
        assert_eq!(
            kept.kinds(),
            [
                crate::audit::kind::RUN_SUBMITTED,
                crate::audit::kind::RUN_SUBMITTED,
                crate::audit::kind::PIPELINE_REFUSED
            ]
        );
        let entries = kept.0.lock().unwrap().clone();
        assert_eq!(entries[0].actor.as_deref(), Some("octocat"));
        assert_eq!(entries[0].repo.as_deref(), Some("github.com/acme/widgets"));
        assert_eq!(entries[2].detail["pipeline"], ".dagron/ci/ci.yaml");
    }

    /// A file of the repository's that a required pipeline is named as is
    /// not run in its place, and says why.
    #[tokio::test]
    async fn a_repository_cannot_write_over_a_required_pipeline() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, "")]).await;
        let (_, ev) = claimed(&db, None, Kind::Push, Trust::Trusted).await;
        let (d, gh) = dispatcher(&db, &base).await;
        let d = d.with_governance(Arc::new(Rules));
        pipelines(&gh, &[("scan.yaml", LINT)]);

        let Finish::Failed(why) = d.handle(&ev).await else {
            panic!("the repository's scan.yaml is refused");
        };
        assert!(
            why.contains("requires a pipeline called scan") && why.contains("submitted run run-1"),
            "{why}"
        );
        let submitted = seen.lock().unwrap().clone();
        assert_eq!(submitted.len(), 1);
        assert!(submitted[0]
            .yaml
            .as_deref()
            .unwrap()
            .contains("cargo audit"));
    }

    /// A repository with no pipelines of its own runs its bound workflow
    /// still, beside the pipeline the build requires of it; and the event
    /// tried again submits neither twice.
    #[tokio::test]
    async fn a_required_pipeline_runs_beside_the_bound_workflow() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, "")]).await;
        let (_, ev) = claimed(&db, Some("wf-1"), Kind::Push, Trust::Trusted).await;
        let (d, _gh) = dispatcher(&db, &base).await;
        let d = d.with_governance(Arc::new(Rules));

        let Finish::Done(why) = d.handle(&ev).await else {
            panic!("the scan and wf-1 both run");
        };
        assert_eq!(
            why,
            ".dagron/required/scan.yaml: submitted run run-1; workflow wf-1: submitted run \
             run-2 on cell default"
        );
        let submitted = seen.lock().unwrap().clone();
        assert_eq!(submitted.len(), 2);
        assert_eq!(
            submitted[0].key,
            idempotency_key(&ev.commit_key, ".dagron/required/scan.yaml")
        );
        assert_eq!(submitted[1].workflow.as_deref(), Some("wf-1"));

        assert!(matches!(d.handle(&ev).await, Finish::Done(_)));
        assert_eq!(seen.lock().unwrap().len(), 2, "nothing submitted twice");
    }

    /// The run's record names the person a forge login is to the
    /// build, so an approval its triggerer may not give compares people.
    #[tokio::test]
    async fn the_record_names_the_person_a_login_is() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api_as(vec![(201, "")], TAKES_RECORDS).await;
        let (_, ev) = claimed(&db, None, Kind::Push, Trust::Trusted).await;
        assert_eq!(ev.actor.as_deref(), Some("octocat"));
        let (d, gh) = dispatcher(&db, &base).await;
        let d = d.with_governance(Arc::new(Rules));
        pipelines(&gh, &[("lint.yaml", LINT)]);

        assert!(matches!(d.handle(&ev).await, Finish::Done(_)));
        let seen = seen.lock().unwrap();
        let required_and_own: Vec<_> = seen
            .iter()
            .map(|s| s.trigger.as_ref().unwrap()["actor"].clone())
            .collect();
        assert_eq!(
            required_and_own,
            [
                serde_json::json!("alice@acme.example"),
                serde_json::json!("alice@acme.example")
            ]
        );
    }

    /// A paths filter is decided with the files the event changed, asked of
    /// the forge once for every pipeline that needs them.
    #[tokio::test]
    async fn a_paths_filter_asks_the_forge_once() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, "")]).await;
        let (_, ev) = claimed(&db, None, Kind::PullRequest, Trust::Trusted).await;
        let (d, gh) = dispatcher(&db, &base).await;
        let paths = |name: &str, glob: &str| {
            format!(
                "name: {name}\non: {{ pull_request: {{ branches: [main], paths: [\"{glob}\"] }} }}\n\
                 tasks:\n  - {{ name: t, steps: [{{ run: make }}] }}\n"
            )
        };
        let (code, docs) = (paths("code", "src/**"), paths("docs", "docs/**"));
        pipelines(&gh, &[("code.yaml", &code), ("docs.yaml", &docs)]);
        gh.lock().unwrap().changed = vec![("src/lib.rs".into(), None)];

        let Finish::Done(why) = d.handle(&ev).await else {
            panic!("code's paths match");
        };
        assert_eq!(
            why,
            "code.yaml: submitted run run-1; docs.yaml: skipped, no changed file matches \
             on.pull_request.paths"
        );
        let compares: Vec<String> = gh
            .lock()
            .unwrap()
            .seen
            .iter()
            .map(|s| s.0.clone())
            .filter(|path| path.contains("/compare/"))
            .collect();
        assert_eq!(
            compares,
            [format!(
                "/repos/acme/widgets/compare/{}...{}",
                "b".repeat(40),
                "a".repeat(40)
            )],
            "once, for both"
        );
        assert_eq!(seen.lock().unwrap().len(), 1);
    }

    /// A change larger than the compare API lists is decided all the same:
    /// its files come from a shallow fetch of its two commits, and a
    /// pipeline whose paths they miss is skipped, not run unchecked, with a
    /// passing status for each of its jobs.
    #[tokio::test]
    async fn a_change_too_large_to_compare_is_decided_from_a_shallow_fetch() {
        use crate::git::pack::tests::{a_large_change, Scratch};
        let Some(scratch) = Scratch::new(crate::git::pack::ObjectFormat::Sha1) else {
            return;
        };
        let (base, head) = a_large_change(&scratch);
        let (db, _dir) = sqlite().await;
        let (api, seen) = stub_api(vec![(201, "")]).await;
        let repo = db
            .add_repo("github", "github.com", "acme/widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        let pull = crate::event::NewEvent {
            kind: Kind::PullRequest,
            git_ref: "refs/pull/7/head".into(),
            sha: head.clone(),
            base_sha: Some(base.clone()),
            base_ref: Some("main".into()),
            pr: Some(7),
            ..event()
        };
        db.record_delivery(&repo, "d-1", &pull, b"{}")
            .await
            .unwrap();
        let ev = db
            .claim_event("w", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
        let (origin, gh) = stub_github().await;
        let d = Dispatcher::new(
            db.clone(),
            BTreeMap::from([("default".into(), api)]),
            "dgr_token".into(),
            Arc::new(Forge::new(Some("ghp_test".into())).with_origin(&origin)),
            compile::Settings::default(),
        );
        let paths = |name: &str, glob: &str| {
            format!(
                "name: {name}\non: {{ pull_request: {{ paths: [\"{glob}\"] }} }}\n\
                 tasks:\n  - {{ name: t, steps: [{{ run: make }}] }}\n"
            )
        };
        let (code, docs) = (paths("code", "src/**"), paths("docs", "docs/**"));
        pipelines(&gh, &[("code.yaml", &code), ("docs.yaml", &docs)]);
        {
            let mut gh = gh.lock().unwrap();
            gh.git = Some(scratch.path().to_path_buf());
            // As many as the compare API lists, and so maybe not all.
            gh.changed = (0..300).map(|n| (format!("gen/{n}.txt"), None)).collect();
            gh.merge_base = Some(base.clone());
        }

        assert_eq!(
            d.handle(&ev).await,
            Finish::Done(
                "code.yaml: submitted run run-1; docs.yaml: skipped, no changed file matches \
                 on.pull_request.paths; the commit says so"
                    .into()
            )
        );
        assert_eq!(seen.lock().unwrap().len(), 1);
        let posted: Vec<(String, String, String)> = gh
            .lock()
            .unwrap()
            .statuses
            .iter()
            .map(|(sha, _, status)| {
                assert_eq!(sha, &head);
                let field = |name: &str| status[name].as_str().unwrap().to_string();
                (field("context"), field("state"), field("description"))
            })
            .collect();
        assert_eq!(
            posted,
            [(
                "dagron-ci/docs/t (pull_request)".to_string(),
                "success".to_string(),
                "skipped: no changed file matches on.pull_request.paths".to_string()
            )]
        );
    }

    /// A pipeline that took the event and was skipped for its paths says so
    /// on the commit: a passing status for each of its jobs, under the
    /// context the job's own would have had, so that a branch that requires
    /// the check is not left waiting. Once the event is settled, and
    /// not at a turn that will be handled again; and nothing for a pipeline
    /// whose `on:` did not take the event at all.
    #[tokio::test]
    async fn a_pipeline_skipped_for_its_paths_says_so_on_the_commit() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(503, "busy"), (201, "")]).await;
        let (_, ev) = claimed(&db, None, Kind::PullRequest, Trust::Trusted).await;
        let (origin, gh) = stub_github().await;
        let d = Dispatcher::new(
            db.clone(),
            BTreeMap::from([("default".into(), base)]),
            "dgr_token".into(),
            Arc::new(Forge::new(Some("ghp_test".into())).with_origin(&origin)),
            compile::Settings::default(),
        );
        let code = "name: code\non: { pull_request: { paths: [\"src/**\"] } }\n\
                    tasks:\n  - { name: t, steps: [{ run: make }] }\n";
        let docs = "name: docs\non: { pull_request: { paths: [\"docs/**\"] } }\n\
                    tasks:\n  - { name: build, steps: [{ run: make docs }] }\n  \
                    - { name: links, depends_on: [build], steps: [{ run: make links }] }\n";
        let pushes = "name: pushes\non: { push: }\n\
                      tasks:\n  - { name: t, steps: [{ run: make }] }\n";
        pipelines(
            &gh,
            &[
                ("code.yaml", code),
                ("docs.yaml", docs),
                ("pushes.yaml", pushes),
            ],
        );
        gh.lock().unwrap().changed = vec![("src/lib.rs".into(), None)];
        let said = || gh.lock().unwrap().statuses.clone();

        // code's run could not be submitted: the event is handled again, and
        // nothing is said of docs yet.
        assert!(matches!(d.handle(&ev).await, Finish::Retry { .. }));
        assert!(said().is_empty(), "{:?}", said());

        let Finish::Done(why) = d.handle(&ev).await else {
            panic!("code's paths match");
        };
        assert_eq!(
            why,
            "code.yaml: submitted run run-1; docs.yaml: skipped, no changed file matches \
             on.pull_request.paths; the commit says so; pushes.yaml: skipped, on: does not \
             take pull_request"
        );
        let statuses = said();
        let posted: Vec<(String, String, String)> = statuses
            .iter()
            .map(|(sha, _, status)| {
                assert_eq!(sha, &ev.sha);
                (
                    status["context"].as_str().unwrap().to_string(),
                    status["state"].as_str().unwrap().to_string(),
                    status["description"].as_str().unwrap().to_string(),
                )
            })
            .collect();
        let skipped = "skipped: no changed file matches on.pull_request.paths".to_string();
        assert_eq!(
            posted,
            [
                (
                    "dagron-ci/docs/build (pull_request)".to_string(),
                    "success".to_string(),
                    skipped.clone()
                ),
                (
                    "dagron-ci/docs/links (pull_request)".to_string(),
                    "success".to_string(),
                    skipped
                ),
            ],
            "one for each of docs's jobs, and none for a pipeline that takes no pull request"
        );
        assert_eq!(
            seen.lock().unwrap().len(),
            2,
            "code, refused once, and code"
        );

        // A repository whose commits take no statuses is not asked, and the
        // event's reason does not say the commit says so.
        let (db, _dir) = sqlite().await;
        let (base, _) = stub_api(vec![(201, "")]).await;
        let (_, ev) = claimed(&db, None, Kind::PullRequest, Trust::Trusted).await;
        let (d, gh) = dispatcher(&db, &base).await;
        pipelines(&gh, &[("code.yaml", code), ("docs.yaml", docs)]);
        gh.lock().unwrap().changed = vec![("src/lib.rs".into(), None)];
        let Finish::Done(why) = d.handle(&ev).await else {
            panic!("code's paths match");
        };
        assert!(
            why.ends_with("docs.yaml: skipped, no changed file matches on.pull_request.paths"),
            "{why}"
        );
        assert!(gh.lock().unwrap().statuses.is_empty());

        // A pipeline with a run of its own at the commit keeps that run's
        // word. Another event of the commit, which changed other files,
        // does not put a passing "skipped" over what its jobs came to.
        let (db, _dir) = sqlite().await;
        let (base, _) = stub_api(vec![(201, "")]).await;
        let (repo, ev) = claimed(&db, None, Kind::PullRequest, Trust::Trusted).await;
        let (origin, gh) = stub_github().await;
        let d = Dispatcher::new(
            db.clone(),
            BTreeMap::from([("default".into(), base)]),
            "dgr_token".into(),
            Arc::new(Forge::new(Some("ghp_test".into())).with_origin(&origin)),
            compile::Settings::default(),
        );
        assert!(db
            .record_run(&SubmittedRun {
                run_id: "run-earlier".into(),
                event_id: ev.id.clone(),
                repo_id: repo.id.clone(),
                cell: "default".into(),
                pipeline: ".dagron/ci/docs.yaml".into(),
                idempotency_key: "ci-earlier".into(),
                on_failed: false,
            })
            .await
            .unwrap());
        // It has ended, and its job's failure is on the commit.
        let failed = crate::db::ReportWrite {
            reported: BTreeMap::from([(
                "dagron-ci/docs/build (pull_request)".to_string(),
                "failure:failed".to_string(),
            )]),
            status: Some("failed".into()),
            ..Default::default()
        };
        let reporter = Duration::from_secs(60);
        let (due, _) = db.claim_report("r", reporter).await.unwrap().unwrap();
        assert_eq!(due, "run-earlier");
        assert!(db.finish_report("run-earlier", "r", &failed).await.unwrap());
        assert!(db.claim_report("r", reporter).await.unwrap().is_none());

        pipelines(&gh, &[("code.yaml", code), ("docs.yaml", docs)]);
        gh.lock().unwrap().changed = vec![("src/lib.rs".into(), None)];
        let Finish::Done(why) = d.handle(&ev).await else {
            panic!("code's paths match");
        };
        assert!(
            why.ends_with(
                "docs.yaml: skipped, no changed file matches on.pull_request.paths; no status \
                 says so: it has a run of its own at this commit, whose checks stand"
            ),
            "{why}"
        );
        assert!(gh.lock().unwrap().statuses.is_empty());
        // And the run is told to post its statuses afresh, in case an earlier
        // handling of this event had said "skipped" before it was cut short:
        // due again, with nothing on record as posted. (So is code's new
        // run, which is due as any new run is.)
        let mut due = BTreeMap::new();
        while let Some((run, report)) = db.claim_report("r", reporter).await.unwrap() {
            due.insert(run, report.expect("its event is there").reported);
        }
        assert_eq!(
            due.keys().map(String::as_str).collect::<Vec<_>>(),
            ["run-1", "run-earlier"]
        );
        assert!(due["run-earlier"].is_empty(), "{due:?}");
    }

    /// A job its `when:` leaves out of the event has no task in the run, and
    /// so nothing of the run's own says anything of it. It has a passing
    /// status that says it was skipped, as a workflow's job left out by its
    /// `if:` has, so that a branch that requires its check is not left
    /// waiting: one each where some jobs run, and where none does.
    #[tokio::test]
    async fn a_job_its_when_leaves_out_is_said_skipped_on_the_commit() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, ""), (201, "")]).await;
        let (_, ev) = claimed(&db, None, Kind::Push, Trust::Trusted).await;
        let (origin, gh) = stub_github().await;
        let d = Dispatcher::new(
            db.clone(),
            BTreeMap::from([("default".into(), base)]),
            "dgr_token".into(),
            Arc::new(Forge::new(Some("ghp_test".into())).with_origin(&origin)),
            compile::Settings::default(),
        );
        let release = "name: release\non: { push: }\ntasks:\n  \
                       - { name: build, steps: [{ run: make }] }\n  \
                       - { name: publish, depends_on: [build], when: \"{{ ci_event }} == tag\", \
                       steps: [{ run: make publish }] }\n";
        let tags = "name: tags\non: { push: }\ntasks:\n  \
                    - { name: a, when: \"{{ ci_event }} == tag\", steps: [{ run: make a }] }\n  \
                    - { name: b, when: \"{{ ci_event }} == tag\", steps: [{ run: make b }] }\n";
        pipelines(&gh, &[("release.yaml", release), ("tags.yaml", tags)]);

        let Finish::Done(why) = d.handle(&ev).await else {
            panic!("both pipelines take a push");
        };
        assert_eq!(
            why,
            "release.yaml: submitted run run-1; tags.yaml: submitted run run-2"
        );
        assert_eq!(seen.lock().unwrap().len(), 2, "a run of no task is a run");
        let posted: Vec<(String, String, String)> = gh
            .lock()
            .unwrap()
            .statuses
            .iter()
            .map(|(sha, _, status)| {
                assert_eq!(sha, &ev.sha);
                let field = |name: &str| status[name].as_str().unwrap().to_string();
                (field("context"), field("state"), field("description"))
            })
            .collect();
        let skipped = |context: &str| {
            (
                context.to_string(),
                "success".to_string(),
                "skipped: its when: leaves it out of this event".to_string(),
            )
        };
        assert_eq!(
            posted,
            [
                skipped("dagron-ci/release/publish (push)"),
                skipped("dagron-ci/tags/a (push)"),
                skipped("dagron-ci/tags/b (push)"),
            ],
            "nothing yet for build, which runs: its run says how it went"
        );
    }

    /// A job a run owns the context of (its reporter is posting under it, and
    /// has not yet put that on the run's record) is not said skipped: a
    /// passing "skipped" would stand over what the run said until the run
    /// posted again. Its other jobs are, each under a lease that is given
    /// back once its post is answered, so that a run of one posts after.
    #[tokio::test]
    async fn a_job_whose_context_a_run_owns_is_not_said_skipped_over_it() {
        let (db, _dir) = sqlite().await;
        let (base, _) = stub_api(vec![(201, "")]).await;
        let (repo, ev) = claimed(&db, None, Kind::Push, Trust::Trusted).await;
        let (origin, gh) = stub_github().await;
        let d = Dispatcher::new(
            db.clone(),
            BTreeMap::from([("default".into(), base)]),
            "dgr_token".into(),
            Arc::new(Forge::new(Some("ghp_test".into())).with_origin(&origin)),
            compile::Settings::default(),
        );
        let tags = "name: tags\non: { push: }\ntasks:\n  \
                    - { name: a, when: \"{{ ci_event }} == tag\", steps: [{ run: make a }] }\n  \
                    - { name: b, when: \"{{ ci_event }} == tag\", steps: [{ run: make b }] }\n";
        pipelines(&gh, &[("tags.yaml", tags)]);
        // Another event's run of `a` at the commit, mid-post: it owns `a`'s
        // context, and its record says nothing of it yet.
        let a = "dagron-ci/tags/a (push)";
        assert!(db
            .own_context(&repo.id, &ev.sha, a, "run-elsewhere")
            .await
            .unwrap());

        let Finish::Done(why) = d.handle(&ev).await else {
            panic!("the pipeline takes a push");
        };
        assert_eq!(why, "tags.yaml: submitted run run-1");
        let posted: Vec<String> = gh
            .lock()
            .unwrap()
            .statuses
            .iter()
            .map(|(_, _, s)| s["context"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(posted, ["dagron-ci/tags/b (push)"], "a is the run's");
        // `b`'s lease was given back: a run of it would own it now.
        let b = "dagron-ci/tags/b (push)";
        assert!(db.own_context(&repo.id, &ev.sha, b, "run-2").await.unwrap());
    }

    /// A run of the pipeline that is recorded while the pipeline is being said
    /// skipped, by another event of the commit. The dispatcher looked before
    /// it posted, and found none. It looks again after its last post, and
    /// has the run post its own statuses afresh: they are then the last word,
    /// whichever reached the forge first.
    #[tokio::test]
    async fn a_run_recorded_while_its_pipeline_is_said_skipped_posts_again() {
        let (db, _dir) = sqlite().await;
        let (base, _) = stub_api(vec![(201, "")]).await;
        let (repo, ev) = claimed(&db, None, Kind::PullRequest, Trust::Trusted).await;
        let (origin, gh) = stub_github().await;
        let d = Dispatcher::new(
            db.clone(),
            BTreeMap::from([("default".into(), base)]),
            "dgr_token".into(),
            Arc::new(Forge::new(Some("ghp_test".into())).with_origin(&origin)),
            compile::Settings::default(),
        );
        let docs = "name: docs\non: { pull_request: { paths: [\"docs/**\"] } }\n\
                    tasks:\n  - { name: build, steps: [{ run: make docs }] }\n";
        pipelines(&gh, &[("docs.yaml", docs)]);
        gh.lock().unwrap().changed = vec![("src/lib.rs".into(), None)];
        // The forge holds the status at its door until it is let in.
        let reached = Arc::new(tokio::sync::Notify::new());
        let go = Arc::new(tokio::sync::Notify::new());
        gh.lock().unwrap().status_gate = Some((reached.clone(), go.clone()));

        let handling = tokio::spawn({
            let ev = ev.clone();
            async move { d.handle(&ev).await }
        });
        // The "skipped" is on its way: the dispatcher has looked, and found
        // no run of docs at this commit. Now there is one, which fails and
        // says so before the "skipped" lands.
        reached.notified().await;
        assert!(db
            .record_run(&SubmittedRun {
                run_id: "run-meanwhile".into(),
                event_id: ev.id.clone(),
                repo_id: repo.id.clone(),
                cell: "default".into(),
                pipeline: ".dagron/ci/docs.yaml".into(),
                idempotency_key: "ci-meanwhile".into(),
                on_failed: false,
            })
            .await
            .unwrap());
        let failed = crate::db::ReportWrite {
            reported: BTreeMap::from([(
                "dagron-ci/docs/build (pull_request)".to_string(),
                "failure:failed".to_string(),
            )]),
            status: Some("failed".into()),
            ..Default::default()
        };
        let reporter = Duration::from_secs(60);
        let (due, _) = db.claim_report("r", reporter).await.unwrap().unwrap();
        assert_eq!(due, "run-meanwhile");
        assert!(db
            .finish_report("run-meanwhile", "r", &failed)
            .await
            .unwrap());
        gh.lock().unwrap().status_gate = None;
        go.notify_one();

        let (Finish::Skipped(why) | Finish::Done(why)) = handling.await.unwrap() else {
            panic!("the event is settled");
        };
        assert!(
            why.ends_with(
                "no status says so: a run of its own started at this commit meanwhile: its \
                 checks are posted again, and stand"
            ),
            "{why}"
        );
        // The "skipped" did land, over the failure. So the run is due for a
        // report again, with nothing on record as posted: its failure is
        // posted afresh, after it.
        let posted = gh.lock().unwrap().statuses.clone();
        assert_eq!(posted.len(), 1, "{posted:?}");
        assert_eq!(posted[0].2["state"], "success");
        let (due, report) = db
            .claim_report("r", reporter)
            .await
            .unwrap()
            .expect("the run is due again");
        assert_eq!(due, "run-meanwhile");
        assert!(report.unwrap().reported.is_empty());
    }

    /// A new branch has nothing to compare with, and a change can be larger
    /// than the forge lists: a paths filter then lets its pipeline run, and
    /// the event says why it was not checked.
    #[tokio::test]
    async fn unlisted_changes_run_the_pipeline_and_say_so() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, "")]).await;
        let repo = db
            .add_repo("github", "github.com", "acme/widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        let new_branch = crate::event::NewEvent {
            git_ref: "refs/heads/topic".into(),
            base_sha: None,
            ..event()
        };
        db.record_delivery(&repo, "d-1", &new_branch, b"{}")
            .await
            .unwrap();
        let ev = db
            .claim_event("w", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
        let (d, gh) = dispatcher(&db, &base).await;
        let paths = "name: p\non: { push: { paths: [\"src/**\"] } }\n\
                     tasks:\n  - { name: t, steps: [{ run: make }] }\n";
        pipelines(&gh, &[("p.yaml", paths)]);

        assert_eq!(
            d.handle(&ev).await,
            Finish::Done(
                "p.yaml: submitted run run-1 (paths not checked: a new branch has no earlier \
                 commit to compare with)"
                    .into()
            )
        );
        assert!(
            gh.lock()
                .unwrap()
                .seen
                .iter()
                .all(|s| !s.0.contains("/compare/")),
            "nothing to compare"
        );
        assert_eq!(seen.lock().unwrap().len(), 1);
    }

    /// A pipeline that cannot run as written fails the event, once every
    /// other pipeline has run.
    #[tokio::test]
    async fn an_invalid_pipeline_fails_the_event_after_the_others_run() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, "")]).await;
        let (_, ev) = claimed(&db, None, Kind::Push, Trust::Trusted).await;
        let (d, gh) = dispatcher(&db, &base).await;
        let broken = "name: broken\non: { push: }\ntasks:\n  - { name: t, steps: [{ run: \"echo {{ ci_ref }}\" }] }\n";
        pipelines(&gh, &[("broken.yaml", broken), ("ci.yaml", CI)]);

        let Finish::Failed(why) = d.handle(&ev).await else {
            panic!("broken.yaml cannot run");
        };
        assert!(
            why.starts_with("broken.yaml: refused, invalid: tasks[0] (t).steps[0].run: ")
                && why.ends_with("; ci.yaml: submitted run run-1"),
            "{why}"
        );
        assert_eq!(seen.lock().unwrap().len(), 1, "ci ran");
    }

    /// A pipeline that is invalid, or that dagron-api refuses, leaves one
    /// status on the event's commit, under the pipeline's own name, which says
    /// it was not run and why (rule 17: no check waits on silence). Once the
    /// event ends for good, and not at each turn of one that is retried.
    #[tokio::test]
    async fn a_pipeline_that_cannot_run_says_so_on_the_commit() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, ""), (503, "busy"), (201, "")]).await;
        let (_, ev) = claimed(&db, None, Kind::Push, Trust::Trusted).await;
        let (origin, gh) = stub_github().await;
        let metrics = Arc::new(Metrics::default());
        let d = Dispatcher::new(
            db.clone(),
            BTreeMap::from([("default".into(), base)]),
            "dgr_token".into(),
            Arc::new(Forge::new(Some("ghp_test".into())).with_origin(&origin)),
            compile::Settings::default(),
        )
        .with_metrics(metrics.clone());
        let broken = "name: broken\non: { push: }\ntasks:\n  - { name: t, steps: [{ run: \"echo {{ ci_ref }}\" }] }\n";
        pipelines(
            &gh,
            &[
                ("broken.yaml", broken),
                ("ci.yaml", CI),
                ("lint.yaml", LINT),
            ],
        );
        let said = || gh.lock().unwrap().statuses.clone();

        // lint could not be submitted: the event is handled again, and nothing
        // is said of broken yet.
        assert!(matches!(d.handle(&ev).await, Finish::Retry { .. }));
        assert!(said().is_empty(), "{:?}", said());

        // This time it ends: failed, for broken, which the commit now says.
        let Finish::Failed(why) = d.handle(&ev).await else {
            panic!("broken.yaml cannot run");
        };
        assert!(
            why.contains("broken.yaml: refused, invalid: tasks[0] (t).steps[0].run: ")
                && why.contains("; the commit says so; ci.yaml: run run-1 was submitted"),
            "{why}"
        );
        let statuses = said();
        assert_eq!(
            statuses.len(),
            1,
            "one status, for the one that did not run: {statuses:?}"
        );
        let (sha, _, status) = &statuses[0];
        assert_eq!(sha, &ev.sha);
        assert_eq!(
            (status["state"].as_str(), status["context"].as_str()),
            (Some("error"), Some("dagron-ci/broken (push)")),
            "under the pipeline's name, which is no job's"
        );
        let description = status["description"].as_str().unwrap();
        assert!(
            description.starts_with("not run: invalid: tasks[0] (t).steps[0].run: ")
                && description.chars().count() <= 140,
            "{description}"
        );
        assert_eq!(seen.lock().unwrap().len(), 3, "ci, lint refused once, lint");

        // A status that cannot be posted now holds the event to say it later;
        // one GitHub refuses leaves the reason on the event.
        gh.lock().unwrap().answer = None;
        let refuse_statuses = |code: u16, headers: Vec<(&'static str, String)>| {
            let mut gh = gh.lock().unwrap();
            gh.status_answer = Some((code, headers));
        };
        refuse_statuses(403, vec![("retry-after", "30".into())]);
        assert!(
            matches!(d.handle(&ev).await, Finish::Wait { after, .. } if after == Duration::from_secs(30))
        );
        refuse_statuses(502, vec![]);
        assert!(matches!(d.handle(&ev).await, Finish::Retry { .. }));
        refuse_statuses(403, vec![]);
        let Finish::Failed(why) = d.handle(&ev).await else {
            panic!("the event still ends failed");
        };
        assert!(
            why.contains("; no status says so: ") && why.contains("statuses: write"),
            "{why}"
        );
        assert_eq!(said().len(), 1);
    }

    /// A status that cannot be posted for one pipeline does not keep the
    /// event's other pipelines from being said: the event is handled again for
    /// the one, and on its last attempt there would be no turn for the rest.
    #[tokio::test]
    async fn one_status_that_fails_does_not_keep_the_others_from_being_said() {
        let (db, _dir) = sqlite().await;
        let (base, _seen) = stub_api(vec![(201, "")]).await;
        let (_, ev) = claimed(&db, None, Kind::Push, Trust::Trusted).await;
        let (origin, gh) = stub_github().await;
        let d = Dispatcher::new(
            db.clone(),
            BTreeMap::from([("default".into(), base)]),
            "dgr_token".into(),
            Arc::new(Forge::new(Some("ghp_test".into())).with_origin(&origin)),
            compile::Settings::default(),
        );
        let broken = "name: broken\non: { push: }\ntasks: []\n";
        pipelines(&gh, &[("a.yaml", broken), ("b.yaml", broken)]);
        // GitHub fails the first status it is sent, and takes the next.
        gh.lock().unwrap().status_answers.push_back(502);

        let Finish::Retry { reason, .. } = d.handle(&ev).await else {
            panic!("the status that failed is tried again");
        };
        assert!(reason.contains("HTTP 502"), "{reason}");
        let contexts: Vec<String> = gh
            .lock()
            .unwrap()
            .statuses
            .iter()
            .map(|(_, _, status)| status["context"].as_str().unwrap().to_string())
            .collect();
        assert_eq!(
            contexts,
            ["dagron-ci/b (push)"],
            "the other was said all the same"
        );
    }

    /// An event still to be retried on its last attempt is ended by the worker,
    /// which fails it. Its pipelines that cannot run are said on that attempt,
    /// and not left for a turn that will not come.
    #[tokio::test]
    async fn an_event_on_its_last_attempt_says_what_cannot_run_before_it_is_given_up() {
        let (db, _dir) = sqlite().await;
        let (base, _seen) = stub_api(vec![(503, "busy")]).await;
        let (_, ev) = claimed(&db, None, Kind::Push, Trust::Trusted).await;
        let (origin, gh) = stub_github().await;
        let dispatcher = |max_attempts: i64| {
            Dispatcher::new(
                db.clone(),
                BTreeMap::from([("default".into(), base.clone())]),
                "dgr_token".into(),
                Arc::new(Forge::new(Some("ghp_test".into())).with_origin(&origin)),
                compile::Settings::default(),
            )
            .with_max_attempts(max_attempts)
        };
        let broken = "name: broken\non: { push: }\ntasks: []\n";
        pipelines(&gh, &[("broken.yaml", broken), ("ci.yaml", CI)]);
        let said = || gh.lock().unwrap().statuses.clone();

        // With an attempt to spare, ci's submit is retried and nothing is said.
        let spare = dispatcher(ev.attempts + 1);
        assert!(matches!(spare.handle(&ev).await, Finish::Retry { .. }));
        assert!(said().is_empty(), "{:?}", said());

        // On the last one the answer is still a retry, which the worker turns
        // into the event's end. So it is said now.
        let Finish::Retry { reason, .. } = dispatcher(ev.attempts).handle(&ev).await else {
            panic!("ci could not be submitted");
        };
        assert!(
            reason.contains("broken.yaml: refused, invalid: ")
                && reason.contains("; the commit says so"),
            "{reason}"
        );
        let statuses = said();
        assert_eq!(statuses.len(), 1, "{statuses:?}");
        let status = &statuses[0].2;
        assert_eq!(
            (status["state"].as_str(), status["context"].as_str()),
            (Some("error"), Some("dagron-ci/broken (push)"))
        );
    }

    /// A pipeline dagron-api could not take now has the event retried, ahead
    /// of a refusal or a success: the retry submits what is missing, finds
    /// what was submitted, and only then does the event end.
    #[tokio::test]
    async fn a_pipeline_not_yet_submitted_retries_the_event_first() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, ""), (503, "busy"), (201, "")]).await;
        let (_, ev) = claimed(&db, None, Kind::Push, Trust::Trusted).await;
        let (d, gh) = dispatcher(&db, &base).await;
        let broken = "name: broken\non: { push: }\ntasks: []\n";
        pipelines(
            &gh,
            &[
                ("broken.yaml", broken),
                ("ci.yaml", CI),
                ("lint.yaml", LINT),
            ],
        );

        let Finish::Retry { reason, .. } = d.handle(&ev).await else {
            panic!("lint was not submitted: the event is retried");
        };
        assert!(
            reason.contains("ci.yaml: submitted run run-1")
                && reason.contains("lint.yaml: not submitted, dagron-api answered 503"),
            "{reason}"
        );

        let Finish::Failed(why) = d.handle(&ev).await else {
            panic!("everything that can run has; broken cannot");
        };
        assert!(
            why.starts_with("broken.yaml: refused, invalid: ")
                && why.contains("ci.yaml: run run-1 was submitted for this commit already")
                && why.ends_with("lint.yaml: submitted run run-2"),
            "{why}"
        );
        assert_eq!(seen.lock().unwrap().len(), 3, "ci once, lint twice");
    }

    /// While a run's concurrency key is full the event waits, and a wait
    /// spends no attempt: the event never runs out of them while it waits.
    #[tokio::test]
    async fn a_full_concurrency_key_waits_without_spending_an_attempt() {
        let (db, _dir) = sqlite().await;
        let full = "max_active_runs reached for workflow 'ci' and concurrency_key \
                    'github.com/acme/widgets:refs/heads/main' (1 active, cap 1)";
        let (base, seen) = stub_api(vec![(429, full), (201, "")]).await;
        let (_, ev) = claimed(&db, None, Kind::Push, Trust::Trusted).await;
        let (d, gh) = dispatcher(&db, &base).await;
        pipelines(&gh, &[("ci.yaml", CI)]);

        let finish = d.handle(&ev).await;
        assert_eq!(
            finish,
            Finish::Wait {
                reason: format!("ci.yaml: waiting, {full}"),
                after: CONCURRENCY_WAIT,
            }
        );
        assert_eq!(ev.attempts, 1);
        assert!(db.finish_event(&ev.id, "w", &finish).await.unwrap());
        due_now(&db).await;
        let again = db
            .claim_event("w", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(again.attempts, 1, "the wait gave its attempt back");
        assert_eq!(
            d.handle(&again).await,
            Finish::Done("ci.yaml: submitted run run-1".into())
        );
        assert_eq!(seen.lock().unwrap().len(), 2);
    }

    /// A forge whose rate limit is spent holds the event until it resets,
    /// and one that cannot be read is retried.
    #[tokio::test]
    async fn an_unreadable_forge_waits_or_retries() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, "")]).await;
        let (_, ev) = claimed(&db, Some("wf-1"), Kind::Push, Trust::Trusted).await;
        let (d, gh) = dispatcher(&db, &base).await;
        gh.lock().unwrap().answer = Some((429, vec![("retry-after", "90".into())]));
        let Finish::Wait { reason, after } = d.handle(&ev).await else {
            panic!("a spent rate limit waits");
        };
        assert_eq!(after, Duration::from_secs(90));
        assert!(reason.contains("listing .dagron/ci: HTTP 429"), "{reason}");

        gh.lock().unwrap().answer = Some((502, vec![]));
        let finish = d.handle(&ev).await;
        assert!(
            matches!(&finish, Finish::Retry { reason, .. } if reason == "listing .dagron/ci: HTTP 502"),
            "{finish:?}"
        );
        assert!(
            seen.lock().unwrap().is_empty(),
            "nothing ran in place of the pipelines"
        );
    }

    /// A dispatch runs the pipelines that take dispatch, with the inputs they
    /// declare; one sent an input its pipeline does not take is refused.
    #[tokio::test]
    async fn a_dispatch_runs_with_the_inputs_its_pipeline_declares() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, "")]).await;
        let repo = db
            .add_repo("github", "github.com", "acme/widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        let (d, gh) = dispatcher(&db, &base).await;
        let deploy = "name: deploy\non: { dispatch: { inputs: { channel: { default: beta } } } }\n\
                      tasks:\n  - { name: t, steps: [{ run: make, env: { CHANNEL: \"{{ channel }}\" } }] }\n";
        pipelines(&gh, &[("ci.yaml", CI), ("deploy.yaml", deploy)]);
        let dispatch = crate::event::NewEvent {
            kind: Kind::Dispatch,
            ..event()
        };
        let queue = |inputs: BTreeMap<String, String>| {
            let (db, repo, dispatch) = (db.clone(), repo.clone(), dispatch.clone());
            async move {
                db.record_dispatch(&repo, &dispatch, &inputs, None, None, None)
                    .await
                    .unwrap();
                db.claim_event("w", Duration::from_secs(60))
                    .await
                    .unwrap()
                    .unwrap()
            }
        };

        let ev = queue(BTreeMap::from([("channel".into(), "stable".into())])).await;
        let Finish::Done(why) = d.handle(&ev).await else {
            panic!("deploy takes dispatch");
        };
        assert!(why.contains("deploy.yaml: submitted run run-1"), "{why}");
        assert!(
            why.contains("ci.yaml: skipped, on: does not take dispatch"),
            "{why}"
        );
        assert_eq!(seen.lock().unwrap()[0].params["channel"], "stable");

        let ev = queue(BTreeMap::from([("chanel".into(), "stable".into())])).await;
        let Finish::Failed(why) = d.handle(&ev).await else {
            panic!("an undeclared input is refused");
        };
        assert!(
            why.contains("pipeline deploy takes no input \"chanel\""),
            "{why}"
        );
        assert_eq!(seen.lock().unwrap().len(), 1);
    }

    /// A dispatch the caller queued, naming `pipeline` or not, claimed.
    async fn queued(db: &Db, repo: &Repo, pipeline: Option<&str>) -> Claimed {
        let dispatch = crate::event::NewEvent {
            kind: Kind::Dispatch,
            ..event()
        };
        db.record_dispatch(repo, &dispatch, &BTreeMap::new(), pipeline, None, None)
            .await
            .unwrap();
        db.claim_event("w", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap()
    }

    /// A dispatch may name the one pipeline it runs; one asked for by name
    /// runs, or the event fails saying why, and never runs anything else in
    /// its place.
    #[tokio::test]
    async fn a_dispatch_may_name_the_one_pipeline_it_runs() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, "")]).await;
        let repo = db
            .add_repo(
                "github",
                "github.com",
                "acme/widgets",
                Some("wf-1"),
                "default",
            )
            .await
            .unwrap()
            .unwrap();
        let (d, gh) = dispatcher(&db, &base).await;
        let dispatchable = |name: &str| {
            format!("name: {name}\non: {{ dispatch: }}\ntasks:\n  - {{ name: t, steps: [{{ run: make }}] }}\n")
        };
        let (deploy, nightly) = (dispatchable("deploy"), dispatchable("nightly"));
        pipelines(
            &gh,
            &[
                ("ci.yaml", CI),
                ("deploy.yaml", &deploy),
                ("nightly.yml", &nightly),
            ],
        );

        let ev = queued(&db, &repo, Some("deploy")).await;
        assert_eq!(ev.pipeline.as_deref(), Some("deploy"));
        assert_eq!(
            d.handle(&ev).await,
            Finish::Done("deploy.yaml: submitted run run-1".into())
        );
        assert_eq!(
            seen.lock().unwrap()[0].key,
            idempotency_key(&ev.commit_key, ".dagron/ci/deploy.yaml")
        );
        let ev = queued(&db, &repo, Some("nightly.yml")).await;
        assert_eq!(
            d.handle(&ev).await,
            Finish::Done("nightly.yml: submitted run run-2".into())
        );

        for (name, why) in [
            (
                "ci",
                "ci.yaml: refused, asked for by name, and on: does not take dispatch",
            ),
            (
                "missing",
                "aaaaaaaaaaaa has no pipeline \"missing\" in .dagron/ci",
            ),
        ] {
            let ev = queued(&db, &repo, Some(name)).await;
            assert_eq!(d.handle(&ev).await, Finish::Failed(why.into()), "{name}");
        }
        assert_eq!(seen.lock().unwrap().len(), 2);

        // Unnamed, every pipeline that takes dispatch runs.
        let ev = queued(&db, &repo, None).await;
        assert_eq!(
            d.handle(&ev).await,
            Finish::Done(
                "ci.yaml: skipped, on: does not take dispatch; deploy.yaml: submitted run run-3; \
                 nightly.yml: submitted run run-4"
                    .into()
            )
        );

        // Two files for one name: the caller names the file.
        pipelines(&gh, &[("deploy.yaml", &deploy), ("deploy.yml", &deploy)]);
        let ev = queued(&db, &repo, Some("deploy")).await;
        assert_eq!(
            d.handle(&ev).await,
            Finish::Failed(
                "pipeline \"deploy\" names .dagron/ci/deploy.yaml and .dagron/ci/deploy.yml; \
                 name the file"
                    .into()
            )
        );

        // No pipelines at all: the bound workflow does not stand in for one
        // asked for by name.
        gh.lock().unwrap().pipelines = None;
        let ev = queued(&db, &repo, Some("deploy")).await;
        assert_eq!(
            d.handle(&ev).await,
            Finish::Failed(
                "pipeline \"deploy\" was asked for, and aaaaaaaaaaaa has no pipelines in \
                 .dagron/ci"
                    .into()
            )
        );
        assert_eq!(seen.lock().unwrap().len(), 4, "wf-1 never ran");
    }

    /// dagron-ci reads pipelines through GitHub's and GitLab's APIs so far:
    /// another forge's repository runs its bound workflow, and its forge is
    /// not asked.
    #[tokio::test]
    async fn another_forge_runs_its_bound_workflow() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, "")]).await;
        let repo = db
            .add_repo(
                "gitea",
                "gitea.com",
                "acme/widgets",
                Some("wf-1"),
                "default",
            )
            .await
            .unwrap()
            .unwrap();
        db.record_delivery(&repo, "d-1", &event(), b"{}")
            .await
            .unwrap();
        let ev = db
            .claim_event("w", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
        let (d, gh) = dispatcher(&db, &base).await;
        assert_eq!(
            d.handle(&ev).await,
            Finish::Done("submitted run run-1 on cell default".into())
        );
        assert!(gh.lock().unwrap().seen.is_empty());
        assert_eq!(seen.lock().unwrap()[0].workflow.as_deref(), Some("wf-1"));
    }

    /// A limit of so many runs at once, as a build's admission seam sets one.
    struct AtMost(u64);

    impl Admission for AtMost {
        fn admit(&self, active: u64) -> Result<(), String> {
            if active < self.0 {
                Ok(())
            } else {
                Err(format!(
                    "{active} runs are active, and at most {} may be",
                    self.0
                ))
            }
        }
    }

    /// A limit of one run at once for each caller, which keeps what it was
    /// asked with.
    #[derive(Default)]
    struct OnePerCaller(std::sync::Mutex<Vec<(u64, u64, Option<String>, String)>>);

    impl Admission for OnePerCaller {
        fn admit(&self, _active: u64) -> Result<(), String> {
            Ok(())
        }

        fn by_caller(&self) -> bool {
            true
        }

        fn admit_caller(
            &self,
            _repo: &str,
            caller: Option<&str>,
            trust: &str,
            load: &admission::Load,
        ) -> Result<(), String> {
            let theirs = load
                .by_caller
                .get(caller.unwrap_or(""))
                .copied()
                .unwrap_or(0);
            self.0.lock().unwrap().push((
                load.active,
                theirs,
                caller.map(str::to_string),
                trust.to_string(),
            ));
            match theirs {
                0 => Ok(()),
                n => Err(format!("{} has {n} runs active", caller.unwrap_or("?"))),
            }
        }
    }

    /// Admission is asked with the run's caller, the actor its event names,
    /// and each caller's runs: one caller held to its limit holds back no
    /// other's run.
    #[tokio::test]
    async fn a_caller_at_its_limit_waits_and_holds_back_no_one_else() {
        let (db, _dir) = sqlite().await;
        let (base, _seen) = stub_api(vec![(201, ""), (201, "")]).await;
        let (d, _gh) = dispatcher(&db, &base).await;
        let caps = Arc::new(OnePerCaller::default());
        let d = d.with_admission(caps.clone());
        let repo = db
            .add_repo(
                "github",
                "github.com",
                "acme/widgets",
                Some("wf-1"),
                "default",
            )
            .await
            .unwrap()
            .unwrap();
        let mut evs = Vec::new();
        for (i, actor) in ["agent:fixer", "agent:fixer", "octocat"].iter().enumerate() {
            let mut ev = event();
            ev.sha = format!("{i:040x}");
            ev.actor = Some(actor.to_string());
            db.record_delivery(&repo, &uuid::Uuid::now_v7().to_string(), &ev, b"{}")
                .await
                .unwrap();
            evs.push(
                db.claim_event("w", Duration::from_secs(60))
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        assert_eq!(
            d.handle(&evs[0]).await,
            Finish::Done("submitted run run-1 on cell default".into())
        );
        assert_eq!(
            d.handle(&evs[1]).await,
            Finish::Wait {
                reason: "waiting: agent:fixer has 1 runs active".into(),
                after: CONCURRENCY_WAIT,
            }
        );
        assert!(matches!(d.handle(&evs[2]).await, Finish::Done(_)));
        let asked = caps.0.lock().unwrap().clone();
        assert_eq!(
            asked,
            [
                (0, 0, Some("agent:fixer".into()), "trusted".into()),
                (1, 1, Some("agent:fixer".into()), "trusted".into()),
                (1, 0, Some("octocat".into()), "trusted".into()),
            ]
        );
    }

    /// A repository bound to `wf-1` with `n` events queued, one per commit,
    /// each claimed.
    async fn events(db: &Db, n: usize) -> Vec<Claimed> {
        let repo = db
            .add_repo(
                "github",
                "github.com",
                "acme/widgets",
                Some("wf-1"),
                "default",
            )
            .await
            .unwrap()
            .unwrap();
        let mut claimed = Vec::with_capacity(n);
        for i in 0..n {
            let mut ev = event();
            ev.sha = format!("{i:040x}");
            db.record_delivery(&repo, &uuid::Uuid::now_v7().to_string(), &ev, b"{}")
                .await
                .unwrap();
            claimed.push(
                db.claim_event("w", Duration::from_secs(60))
                    .await
                    .unwrap()
                    .unwrap(),
            );
        }
        claimed
    }

    #[tokio::test]
    async fn a_run_over_the_limit_waits_its_turn_and_starts_when_one_ends() {
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, "")]).await;
        let (d, _gh) = dispatcher(&db, &base).await;
        let d = d.with_admission(Arc::new(AtMost(1)));
        let evs = events(&db, 2).await;

        assert_eq!(
            d.handle(&evs[0]).await,
            Finish::Done("submitted run run-1 on cell default".into())
        );
        // The second waits, without asking dagron-api, for as long as a
        // concurrency key would make it.
        assert_eq!(
            d.handle(&evs[1]).await,
            Finish::Wait {
                reason: "waiting: 1 runs are active, and at most 1 may be".into(),
                after: CONCURRENCY_WAIT,
            }
        );
        assert_eq!(seen.lock().unwrap().len(), 1);
        // A run recorded already is no new run, limit or not.
        assert_eq!(
            d.handle(&evs[0]).await,
            Finish::Done("run run-1 was submitted for this commit already".into())
        );
        // Once the reporter sees the first end, the second starts.
        crate::db::tests::end_run(&db, "run-1").await;
        assert_eq!(
            d.handle(&evs[1]).await,
            Finish::Done("submitted run run-2 on cell default".into())
        );
        assert_eq!(seen.lock().unwrap().len(), 2);
    }

    #[tokio::test]
    async fn a_run_dagron_api_did_not_create_frees_its_slot_and_one_it_may_have_keeps_it() {
        for (answer, freed) in [
            ((400, "invalid DAG: cycle"), true),
            (
                (
                    429,
                    "max_active_runs reached for workflow 'w' and concurrency_key 'k' (1 active, cap 1)",
                ),
                true,
            ),
            ((503, "busy"), false),
        ] {
            let (db, _dir) = sqlite().await;
            let (base, seen) = stub_api(vec![answer, (201, "")]).await;
            let (d, _gh) = dispatcher(&db, &base).await;
            let d = d.with_admission(Arc::new(AtMost(1)));
            let evs = events(&db, 2).await;
            let first = d.handle(&evs[0]).await;
            assert!(
                !matches!(first, Finish::Done(_)),
                "{answer:?} gave {first:?}"
            );
            let second = d.handle(&evs[1]).await;
            if freed {
                assert!(
                    matches!(&second, Finish::Done(why) if why.contains("run-1")),
                    "{answer:?} left its slot taken: {second:?}"
                );
            } else {
                // dagron-api may have made the first run: its slot holds, and
                // the first's retry takes it up again.
                assert!(
                    matches!(second, Finish::Wait { .. }),
                    "{answer:?} let its slot go: {second:?}"
                );
                assert_eq!(seen.lock().unwrap().len(), 1);
                assert_eq!(
                    d.handle(&evs[0]).await,
                    Finish::Done("submitted run run-1 on cell default".into())
                );
            }
        }
    }

    /// The licence's cap on runs at once, on dagron-ci's side: with a limit of 25, the 26th of 26
    /// runs asked for at once waits, spends no attempt waiting, and starts
    /// once a slot frees. Every pipeline of an event counts as a run.
    async fn the_twenty_sixth_run_waits_and_never_fails(db: &Db) {
        let (base, seen) = stub_api(vec![(201, "")]).await;
        let (d, gh) = dispatcher(db, &base).await;
        let d = Arc::new(d.with_admission(Arc::new(AtMost(25))));
        pipelines(&gh, &[("ci.yaml", CI)]);
        let evs = events(db, 26).await;
        let handled: Vec<_> = evs
            .into_iter()
            .map(|ev| {
                let d = d.clone();
                tokio::spawn(async move {
                    let finish = d.handle(&ev).await;
                    (ev, finish)
                })
            })
            .collect();
        let mut waiting = Vec::new();
        for task in handled {
            let (ev, finish) = task.await.unwrap();
            match finish {
                Finish::Done(_) => {}
                Finish::Wait { .. } => waiting.push((ev, finish)),
                other => panic!("{other:?}"),
            }
        }
        assert_eq!(waiting.len(), 1, "{waiting:?}");
        assert_eq!(seen.lock().unwrap().len(), 25);

        let (ev, wait) = waiting.remove(0);
        assert!(db.finish_event(&ev.id, "w", &wait).await.unwrap());
        due_now(db).await;
        let again = db
            .claim_event("w", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
        assert_eq!((again.id.as_str(), again.attempts), (ev.id.as_str(), 1));
        assert!(matches!(d.handle(&again).await, Finish::Wait { .. }));

        let first = seen.lock().unwrap()[0].key.clone();
        let run = db.find_run(&first).await.unwrap().unwrap();
        crate::db::tests::end_run(db, &run).await;
        assert!(matches!(d.handle(&again).await, Finish::Done(_)));
        assert_eq!(seen.lock().unwrap().len(), 26);
    }

    #[tokio::test]
    async fn the_twenty_sixth_run_waits_and_never_fails_on_sqlite() {
        let (db, _dir) = sqlite().await;
        the_twenty_sixth_run_waits_and_never_fails(&db).await;
    }

    #[tokio::test]
    async fn the_twenty_sixth_run_waits_and_never_fails_on_postgres() {
        let Some((db, schema, admin)) = crate::db::tests::postgres().await else {
            return;
        };
        the_twenty_sixth_run_waits_and_never_fails(&db).await;
        db.close().await;
        crate::db::tests::drop_schema(&admin, &schema).await;
    }

    /// A meter is told of each run once it is recorded, with what it is for;
    /// a run found recorded already is told nothing again.
    #[tokio::test]
    async fn the_meter_is_told_of_each_run_once_it_is_recorded() {
        let (db, _dir) = sqlite().await;
        let (base, _seen) = stub_api(vec![(201, "")]).await;
        let (d, gh) = dispatcher(&db, &base).await;
        let meter = Arc::new(crate::report::tests::StubMeter::default());
        let d = d.with_run_meter(meter.clone());
        pipelines(&gh, &[("ci.yaml", CI), ("lint.yaml", LINT)]);
        let evs = events(&db, 1).await;
        assert!(matches!(d.handle(&evs[0]).await, Finish::Done(_)));
        let told = meter.submitted.lock().unwrap().clone();
        assert_eq!(
            told,
            vec![
                MeteredRun {
                    run_id: "run-1".into(),
                    cell: "default".into(),
                    repo: "github.com/acme/widgets".into(),
                    pipeline: ".dagron/ci/ci.yaml".into(),
                },
                MeteredRun {
                    run_id: "run-2".into(),
                    cell: "default".into(),
                    repo: "github.com/acme/widgets".into(),
                    pipeline: ".dagron/ci/lint.yaml".into(),
                },
            ]
        );
        assert!(matches!(d.handle(&evs[0]).await, Finish::Done(_)));
        assert_eq!(meter.submitted.lock().unwrap().len(), 2);
    }

    /// A GitLab project's `.gitlab-ci.yml`, which its settings name, runs
    /// with the file it includes, as one pipeline: `gitlab-ci`, its jobs
    /// tasks, its variables GitLab's. Off (`CI_GITLAB_PIPELINES`), nothing
    /// of it is read.
    #[tokio::test]
    async fn a_gitlab_project_s_pipeline_runs_with_what_it_includes() {
        use crate::forge::gitlab::tests::stub_gitlab;
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api(vec![(201, "")]).await;
        let repo = db
            .add_repo("gitlab", "gitlab.example", "group/app", None, "default")
            .await
            .unwrap()
            .unwrap();
        db.record_delivery(
            &repo,
            "u-1",
            &event(),
            br#"{"project": {"default_branch": "main"}}"#,
        )
        .await
        .unwrap();
        let ev = db
            .claim_event("w", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
        let (origin, gl) = stub_gitlab().await;
        let raw = |path: &str| {
            format!(
                "/projects/group%2Fapp/repository/files/{}/raw?ref={}",
                path.replace('/', "%2F"),
                ev.sha
            )
        };
        for (path, text) in [
            (".dagron/ci.yaml", "gitlab:\n  pipeline: .gitlab-ci.yml\n"),
            (
                ".gitlab-ci.yml",
                "include: [{local: ci/common.yml}]\nimage: alpine:3.20\nstages: [build, test]\n\
                 build:\n  stage: build\n  script: [make]\n  artifacts: {paths: [out/]}\n\
                 test:\n  extends: .tested\n  stage: test\n",
            ),
            (
                "ci/common.yml",
                ".tested:\n  script: [make test]\n  rules:\n    - if: $CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH\n",
            ),
        ] {
            gl.lock()
                .unwrap()
                .answers
                .insert(raw(path), (200, Vec::new(), text.to_string()));
        }
        let tokens = BTreeMap::from([("gitlab.example".to_string(), "glpat-x".to_string())]);
        let dispatcher = |on: bool| {
            Dispatcher::new(
                db.clone(),
                BTreeMap::from([("default".into(), base.clone())]),
                "dgr_token".into(),
                Arc::new(
                    Forge::new(None)
                        .with_origin(&origin)
                        .with_gitlab_tokens(&tokens),
                ),
                compile::Settings::default(),
            )
            .with_gitlab_pipelines(on)
        };

        // Off: no settings file is asked for, and the project's bound
        // workflow is all it has (none).
        let finish = dispatcher(false).handle(&ev).await;
        assert!(!matches!(finish, Finish::Done(_)), "{finish:?}");
        assert!(seen.lock().unwrap().is_empty());
        assert!(
            !gl.lock()
                .unwrap()
                .seen
                .iter()
                .any(|(_, p, _, _)| p.contains("ci.yaml")),
            "nothing of it was read"
        );

        let finish = dispatcher(true).handle(&ev).await;
        let Finish::Done(why) = finish else {
            panic!("{finish:?}");
        };
        assert!(why.contains(".gitlab-ci.yml: submitted"), "{why}");
        let sent = seen.lock().unwrap();
        assert_eq!(sent.len(), 1);
        let yaml = sent[0].yaml.as_deref().unwrap();
        assert!(
            yaml.contains("name: build") && yaml.contains("name: test"),
            "{yaml}"
        );
        assert!(yaml.contains("CI_ARTIFACT_DOWNLOAD"), "{yaml}");
        assert!(yaml.contains("CI_DEFAULT_BRANCH"), "{yaml}");
        assert!(yaml.contains("make test"), "the included template: {yaml}");
        assert!(sent[0].key.starts_with("ci-"), "{}", sent[0].key);
    }
}

#[cfg(test)]
mod bundle_tests {
    use std::collections::BTreeMap;
    use std::time::Duration;

    use super::tests::{dispatcher, pipelines, stub_api_as, TAKES_RECORDS};
    use super::*;
    use crate::db::tests::{event, sqlite};
    use crate::event::{Kind, Trust};

    fn tar_of(entries: &[(&str, &[u8])]) -> Vec<u8> {
        let mut b = tar::Builder::new(Vec::new());
        for (path, bytes) in entries {
            let mut h = tar::Header::new_gnu();
            h.set_size(bytes.len() as u64);
            h.set_mode(0o644);
            h.set_cksum();
            b.append_data(&mut h, path, *bytes).unwrap();
        }
        b.into_inner().unwrap()
    }

    /// A dispatch on a patch bundle runs the bundle's pipelines, not
    /// the base's where it changes them, and none it deletes; untrusted, its
    /// checkout patched; and the forge is told nothing of it.
    #[tokio::test]
    async fn a_bundle_s_dispatch_runs_its_own_pipelines_untrusted_patched_and_says_nothing() {
        const BASE: &str = "name: ci\non: { dispatch: {} }\n\
                            tasks:\n  - { name: test, docker_image: \"rust:1\", steps: [{ run: echo base }] }\n";
        const GONE: &str = "name: gone\non: { dispatch: {} }\n\
                            tasks:\n  - { name: test, docker_image: \"rust:1\", steps: [{ run: echo gone }] }\n";
        const PATCHED: &str = "name: ci\non: { dispatch: {} }\n\
                               tasks:\n  - { name: test, docker_image: \"rust:1\", steps: [{ run: echo patched }] }\n";
        let (db, _dir) = sqlite().await;
        let (base, seen) = stub_api_as(vec![(201, "")], TAKES_RECORDS).await;
        let (d, gh) = dispatcher(&db, &base).await;
        let store_dir = tempfile::tempdir().unwrap();
        let store =
            crate::payloads::Payloads::from_url(&format!("file://{}", store_dir.path().display()))
                .unwrap();
        let d = d.with_payloads(Some(store.clone()));
        pipelines(&gh, &[("ci.yaml", BASE), ("gone.yaml", GONE)]);

        let repo = db
            .add_repo("github", "github.com", "acme/widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        let id = uuid::Uuid::now_v7().to_string();
        let name = crate::payloads::bundle_name(&id);
        let tar = tar_of(&[
            (".dagron/ci/ci.yaml", PATCHED.as_bytes()),
            (crate::bundles::DELETED, b".dagron/ci/gone.yaml\n"),
        ]);
        store.put_bundle(&name, &tar).await.unwrap();
        let bundle = crate::db::Bundle {
            id: id.clone(),
            repo_id: repo.id.clone(),
            base_sha: event().sha,
            principal: "u-1".into(),
            object_name: name,
            size_bytes: Some(tar.len() as i64),
            sha256: Some("x".into()),
            ready: true,
            expires_at: String::new(),
        };
        db.insert_bundle(&bundle, 7).await.unwrap();
        let asked = crate::event::NewEvent {
            kind: Kind::Dispatch,
            trust: Trust::Untrusted,
            actor: Some("dev@example.com".into()),
            ..event()
        };
        db.record_dispatch(&repo, &asked, &BTreeMap::new(), None, Some(&id), None)
            .await
            .unwrap();
        let ev = db
            .claim_event("w", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ev.bundle_id.as_deref(), Some(id.as_str()));

        let finish = d.handle(&ev).await;
        assert!(
            matches!(&finish, Finish::Done(what) if what.contains("ci.yaml: submitted run")),
            "{finish:?}"
        );
        let seen = seen.lock().unwrap();
        assert_eq!(
            seen.len(),
            1,
            "one run, of the bundle's pipeline: the one it deletes is not"
        );
        let yaml = seen[0].yaml.as_deref().unwrap();
        assert!(
            yaml.contains("echo patched") && !yaml.contains("echo base"),
            "{yaml}"
        );
        assert!(
            yaml.contains("patched: true"),
            "its checkout lays the patch over the base: {yaml}"
        );
        let record = seen[0].trigger.as_ref().expect("the run is recorded");
        assert_eq!(record["trust"], serde_json::json!("untrusted"));
        assert!(
            gh.lock().unwrap().statuses.is_empty(),
            "nothing said on the base commit"
        );
    }
}
