//! The look over a repository's fleet: start a runner for each job that
//! waits, take back each runner nothing waits for, and clear up after each
//! that is over.
//!
//! **A runner is a dagron run of one task**: the class's image, started
//! with a just-in-time configuration GitHub mints for it. GitHub's runner
//! takes one job that asks for the runner's labels, runs it, and exits; the
//! registration goes with it. It is registered for the repository, or for
//! its owner where the class's `scope` is the organization's, in the runner
//! group the class names.
//!
//! **GitLab's runner is GitLab's own**, `gitlab-runner run-single`, made for
//! the project (or its group) with its tags just before it is started, and
//! given its token as GitHub's is given its configuration; it takes one job
//! (`--max-builds 1`) and exits, and the look removes it. A GitLab project's
//! Job Hook says a job waits and names none of its tags, so the look asks
//! GitLab for the project's jobs that wait, writes down those the fleet
//! serves, and tells a merge request's job from a fork by the merge request.
//! What follows holds of either forge.
//!
//! **A runner is not one job's.** It is registered for labels, and GitHub
//! gives it whichever waiting job asks for them: an organization's runner,
//! a job of any repository its group lets in. So the look counts: as many
//! runners out, and not yet busy, as jobs waiting, by class. Which runner
//! took which job is known when GitHub says so, and is recorded then, by the
//! runner's name, whichever repository the job is of. A class's quota
//! (`max_runners`, `max_per_repository`) is held as a runner is wanted: a
//! job past it waits for one of the class's runners to end.
//!
//! **A fork's job.** Where a class tells them apart, a job is told trusted
//! or not by its workflow run's head, which GitHub is asked for once a run
//! (`actions: read`). Until it answers, the job is taken for a fork's. A
//! runner for such a job is started as the class says: none
//! (`untrusted: refuse`, the default), or as a run recorded as `untrusted`
//! (`floor`), which the engine holds to its floor for such runs, with its
//! configuration in an environment that admits them
//! (CI_FLEET_UNTRUSTED_ENVIRONMENT). A trusted runner and an untrusted one
//! are counted apart.
//!
//! GitHub gives a waiting job to any runner with its labels, so a fork's
//! job may still be taken by a runner started for another. An
//! organization's runner groups can keep that from happening (a group
//! restricted to the workflows of a branch is given no pull request's
//! job). Where it happens all the same, the look stops that runner's run as
//! it hears of it, and the job fails; and where the job could not be told
//! apart in [`UNTOLD_FOR`], as for a fork's. Not before: what the job ran
//! until then, it ran.
//!
//! **The configuration is a secret**: whoever holds it is the runner, and
//! is handed the job's own secrets by GitHub. It is never in the run's spec,
//! its parameters or a task's `env`, which a reader of the run can see. It
//! is written as a secret of an environment of the cell's dagron-api
//! (CI_FLEET_ENVIRONMENT), which keeps it encrypted and gives it back to
//! nobody, and the run's task names it (`value_from`). The engine resolves
//! it as the task starts. The secret is deleted once the runner is over.
//!
//! **Taking a runner back is done in an order**: its registration first.
//! Once GitHub has none by its id it can be given no job, and its run is
//! then cancelled. Where GitHub answers that it is running a job, it has
//! one the look had not heard of, and it is left to it.
//!
//! **Warm capacity.** A label's `warm` runners are kept out beyond the jobs
//! that wait, for a repository that has had a job of it within its
//! `warm_for_secs`: a job queued then is given one at once, and another is
//! started in its place. They are trusted runs, counted against the label's
//! quota, and GitHub would give an idle one a fork's job: a label that
//! tells a fork's job apart keeps none warm unless it names a runner group,
//! which the organization restricts ([`crate::fleet`]). One that has waited
//! half an hour, or a quarter of its label's
//! time where that is less, is taken back and started anew, one a look, so
//! that a job given it has the most of its time. The runner's image is
//! pulled where it first runs; the chart pulls each label's image onto
//! each node before that (`ci.fleet.prepull`).
//!
//! **A job whose runner died is run again, once.** A job that failed on a
//! runner of the fleet's whose run did not succeed failed because its
//! runner died (its pod was evicted, its node lost, its process killed),
//! and not because of what it ran: GitHub's runner ends well after its one
//! job, however the job ended. It is run again through GitHub's API
//! (`POST …/actions/jobs/{id}/rerun`, `actions: write`), as a new attempt
//! of its workflow run, once its workflow run is over, which is when GitHub
//! runs a job again: asked each minute for an hour. Not for a runner that
//! had its label's whole time, which the next would have too; not for one
//! the look stopped itself; and never twice for a job of one name of a
//! workflow run. A label says `retry: false` to have none of it. Its run
//! says whether the runner died, whether or not GitHub said first that the
//! job had ended. A job of which a later attempt is heard of was run again,
//! whoever asked, and is not asked for again.
//!
//! **What a delivery did not say.** A job is heard of by delivery, and a
//! delivery can be lost. A job that has waited a minute, and one whose
//! runner was over a minute ago with no end of the job heard, is asked
//! about (`GET …/actions/jobs/{id}`), a few a look and each at most every
//! five minutes, and its row moved on to what GitHub says.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use serde::Deserialize;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use super::store::{Claimed, Quota, Runner, Waiting, JOB_ENDED, TRUSTED, UNTRUSTED};
use super::{Class, Fleet, Untrusted};
use crate::db::{Db, Repo};
use crate::forge::gitlab::is_gitlab;
use crate::forge::{owner_of, Forge, Retried};
use crate::rerun::{Reruns, Sight};

/// How long a look holds a repository's fleet. One that takes longer loses
/// it to another looker.
const LEASE: Duration = Duration::from_secs(60);

/// How soon a fleet with a job waiting or a runner out is looked over again.
pub(crate) const AGAIN: Duration = Duration::from_secs(5);

/// How long a runner that could not be started holds back the next of its
/// label and trust: the reason is one an operator changes (a permission, a
/// token, the cell). The repository is looked over as often as ever.
pub(crate) const BACKOFF: Duration = Duration::from_secs(60);

/// How long a looker with nothing due waits before it asks again.
const IDLE: Duration = Duration::from_secs(2);

/// How long a runner nothing waits for is left before it is taken back:
/// long enough for the delivery of a job queued a moment after another's
/// end to arrive, and short enough that, with a look every [`AGAIN`], a
/// job cancelled as it waits has its runner taken back inside fifteen
/// seconds.
pub(crate) const SPARE_FOR: i64 = 5;

/// How long a runner may be `wanted`, with no run submitted, before its
/// launch is taken for one that was cut short.
pub(crate) const WANTED_FOR: i64 = 120;

/// Why a runner nothing waited for ended. Not a start that failed.
pub(crate) const NO_JOB: &str = "no job waited for it";

/// Why a runner started as a trusted job's was stopped: GitHub gave it a
/// fork's job, which its class gives no such runner, or one that could not
/// be told apart in [`UNTOLD_FOR`].
pub(crate) const FORK_JOB: &str = "GitHub gave it a job of a fork's pull request, or one not \
     told apart in time, which its label gives no trusted runner";

/// How long a job the fleet keeps may run on a trusted runner without being
/// told apart, in seconds, before it is taken for a fork's and its runner
/// stopped: a look tells a few runs apart at a time, and a forge that
/// answers no more (an outage, a rate limit, an App without `actions:
/// read`) does not keep a fork's job on a trusted runner.
pub(crate) const UNTOLD_FOR: i64 = 30;

/// How long a runner whose job has ended is left to exit by itself, in
/// seconds, before its run is asked to stop. GitHub says a job has ended
/// while its runner is still finishing: it sends what is left of the job's
/// log, and then exits. Stopped at once, the runner would be killed in that
/// moment, and the run that hosted a job to its end would read as cancelled.
pub(crate) const EXIT_FOR: i64 = 30;

/// How long a job waits, or a runner that took one is over with no end of
/// the job heard, before GitHub is asked how the job stands.
pub(crate) const UNHEARD_AFTER: Duration = Duration::from_secs(60);

/// How soon a job GitHub was asked about is asked about again.
pub(crate) const ASK_AGAIN: Duration = Duration::from_secs(5 * 60);

/// The most jobs one look asks GitHub about, and the most workflow runs it
/// asks whether they are a fork's. The rest wait for the next look.
const ASKS: i64 = 4;
const TELLS: usize = 8;

/// How long after a job whose runner died has ended it may still be run
/// again: GitHub runs it once its workflow run is over, and a run that goes
/// on longer than this is let go.
pub(crate) const RETRY_WITHIN: Duration = Duration::from_secs(60 * 60);

/// How soon GitHub is asked again to run a job that it would not run yet.
pub(crate) const RETRY_EVERY: Duration = Duration::from_secs(60);

/// The most jobs one look asks GitHub to run again.
const RETRIES: i64 = 4;

/// How long a runner kept warm waits for a job before it is taken back and
/// another started in its place, at most: its label's time runs from its
/// start, and not its job's.
pub(crate) const RECYCLE_AFTER: i64 = 30 * 60;

/// How long a runner of `class` kept warm waits for a job before it is
/// started anew: [`RECYCLE_AFTER`], or a quarter of its time if that is less.
pub(crate) fn recycle_after(class: &Class) -> i64 {
    RECYCLE_AFTER.min((class.timeout_secs / 4) as i64)
}

/// How long before its label's time is out a runner that died is taken to
/// have had it, and its job not run again.
const TIME_LEFT: u64 = 60;

/// How long what a cell said of a fork's job's runner is believed: whether
/// it records a run as untrusted, and whether the environment admits one.
const RECHECK: Duration = Duration::from_secs(5 * 60);

/// Whether a runner that is over never came to run a job, for a reason that
/// would be the next one's too: it could not be registered, kept or
/// submitted, or its run failed before GitHub gave it anything. Its row
/// then holds its place for [`BACKOFF`], and no runner is started in it:
/// a job that waits on something an operator has to change is not given a
/// new runner every five seconds.
pub(crate) fn failed_to_start(runner: &Runner) -> bool {
    runner.job_id.is_none()
        && !matches!(
            runner.reason.as_deref(),
            None | Some(NO_JOB)
                | Some(JOB_ENDED)
                | Some("its run succeeded")
                | Some("its run cancelled")
        )
}

/// The most runners one look starts for a repository.
pub(crate) const MAX_LAUNCHES: usize = 8;

/// How long one call to a cell's dagron-api may take.
const CALL_TIMEOUT: Duration = Duration::from_secs(10);

/// The variable GitHub's runner reads its just-in-time configuration from.
pub const JIT_VARIABLE: &str = "ACTIONS_RUNNER_INPUT_JITCONFIG";

/// What a runner's task runs, unless its class says: GitHub's own runner
/// image's start.
pub const RUN_SCRIPT: &str = "/home/runner/run.sh";

/// The variable GitLab's runner reads its token from, and the instance it
/// is a runner of.
pub const GITLAB_TOKEN_VARIABLE: &str = "CI_SERVER_TOKEN";
pub const GITLAB_URL_VARIABLE: &str = "CI_SERVER_URL";

/// What a GitLab runner's task runs, unless its class says: one job, and
/// out. The rest it is told by its environment. On the Docker executor an
/// image's entrypoint is kept and this is given to it, so a label of an
/// image whose entrypoint is GitLab's own (`gitlab/gitlab-runner`) says
/// `command: [run-single, --max-builds, "1"]` there.
pub const GITLAB_RUN: [&str; 4] = ["gitlab-runner", "run-single", "--max-builds", "1"];

/// The pipelines the environment of a fork's job's runners admits, where
/// dagron-ci writes its policy: the runs it submits for them.
const FLEET_PIPELINES: &str = "fleet-*";

/// Looks over each repository's fleet. One per replica; replicas share the
/// repositories through their claims.
pub struct Lookout {
    db: Db,
    /// GitHub's label map, and GitLab's: one with no label serves nothing.
    fleet: Fleet,
    gitlab: Fleet,
    forge: Arc<Forge>,
    /// Each cell's dagron-api, without a trailing slash.
    cells: BTreeMap<String, String>,
    /// dagron-ci's dagron-api token.
    token: String,
    http: crate::config::Clients,
    reruns: Arc<Reruns>,
    /// CI_FLEET_ENVIRONMENT: the environment a runner's configuration is a
    /// secret of.
    environment: String,
    /// CI_FLEET_UNTRUSTED_ENVIRONMENT: the same, of a runner started for a
    /// fork's job, which admits runs that are not trusted.
    untrusted_environment: String,
    /// Each environment's id in each cell's dagron-api, once it is known, by
    /// the cell and the environment's name.
    environment_ids: Mutex<HashMap<(String, String), String>>,
    /// Whether each cell may be given a fork's job's runner, as it last
    /// said, and when.
    floors: Mutex<HashMap<String, Said>>,
}

/// What a cell said, and when: `Err` is why not.
type Said = (Instant, Result<(), String>);

#[derive(Deserialize)]
struct Environment {
    id: String,
    name: String,
    /// Which runs it admits, where an admin has said; `None` admits
    /// trusted runs alone.
    #[serde(default)]
    policy: Option<Policy>,
}

#[derive(Deserialize)]
struct Policy {
    #[serde(default)]
    allowed_trust: Vec<String>,
}

#[derive(Deserialize)]
struct Submitted {
    run_id: String,
}

/// What came of starting a runner.
enum Launch {
    Started,
    /// The class has as many runners out as its quota lets it: the job
    /// waits for one of them to end.
    Full,
    /// None was started, and why.
    Refused(String),
}

/// Whether `name` is one the fleet gives its runners (`dagron-` and twelve
/// hexadecimal digits), as a delivery names the runner that took a job:
/// what the fleet's rows are asked about.
pub(crate) fn is_runner_name(name: &str) -> bool {
    name.strip_prefix("dagron-")
        .is_some_and(|id| id.len() == 12 && id.bytes().all(|b| b.is_ascii_hexdigit()))
}

/// The name of the secret a runner's configuration is kept as: letters,
/// digits and `_`, as an environment's secrets are named.
pub(crate) fn secret_name(runner: &str) -> String {
    let name: String = runner
        .chars()
        .map(|c| match c {
            c if c.is_ascii_alphanumeric() => c.to_ascii_uppercase(),
            _ => '_',
        })
        .collect();
    format!("FLEET_{name}")
}

/// The run that hosts a runner of `class`: one task, the class's image,
/// with the runner's configuration named and not written.
///
/// A runner for a fork's job (`trust` [`UNTRUSTED`]) asks for the class's
/// `untrusted_runner_class`, and for none of what a run that is not trusted
/// is refused or not given: no pool, no priority, no Docker daemon.
///
/// A GitLab runner (`gitlab`, the instance's URL) is told by its
/// environment where its instance is, its token, that it runs a job's
/// script in its own container, and its name.
pub(crate) fn run_spec(
    label: &str,
    class: &Class,
    environment: &str,
    runner: &str,
    trust: &str,
    gitlab: Option<&str>,
) -> String {
    let untrusted = trust == UNTRUSTED;
    let secret = serde_json::json!({ "secret": secret_name(runner) });
    let (command, env) = match gitlab {
        None => (
            vec![RUN_SCRIPT.to_string()],
            serde_json::json!([{ "name": JIT_VARIABLE, "value_from": secret }]),
        ),
        Some(url) => (
            GITLAB_RUN.map(str::to_string).to_vec(),
            serde_json::json!([
                { "name": GITLAB_TOKEN_VARIABLE, "value_from": secret },
                { "name": GITLAB_URL_VARIABLE, "value": url },
                { "name": "RUNNER_EXECUTOR", "value": "shell" },
                { "name": "RUNNER_NAME", "value": runner },
            ]),
        ),
    };
    let mut task = serde_json::json!({
        "name": "runner",
        "docker_image": class.image,
        "command": class.command.clone().unwrap_or(command),
        "env": env,
        "timeout_secs": class.timeout_secs,
        // A configuration is good for one start: a second attempt would be
        // refused by GitHub, and a job is not given to a runner twice. A
        // GitLab runner's token is the runner's, which the look removes.
        "max_attempts": 1,
    });
    let runner_class = match untrusted {
        true => class
            .untrusted_runner_class
            .as_ref()
            .or(class.runner_class.as_ref()),
        false => class.runner_class.as_ref(),
    };
    if let Some(runner_class) = runner_class {
        task["runner_class"] = serde_json::json!(runner_class);
    }
    if !untrusted {
        if let Some(pool) = &class.pool {
            task["pool"] = serde_json::json!(pool);
        }
        if let Some(priority) = class.priority {
            task["priority"] = serde_json::json!(priority);
        }
        if class.docker {
            task["docker_daemon"] = serde_json::json!(true);
        }
    }
    let spec = serde_json::json!({
        "name": format!("fleet-{label}"),
        "environment": environment,
        "tasks": [task],
    });
    serde_yaml::to_string(&spec).unwrap_or_default()
}

/// The trigger record a fork's job's runner is submitted with: a CI run that
/// is not trusted, which is what the engine holds it by. A runner is no one
/// job's, so it names the repository it was started for, and no commit.
pub(crate) fn floor_trigger(repo: &Repo, runner: &str) -> serde_json::Value {
    serde_json::json!({
        "kind": "ci",
        "trust": UNTRUSTED,
        "forge_host": repo.forge_host,
        "repo": repo.full_name,
        "event_id": format!("fleet:{runner}"),
    })
}

/// The labels of `fleet` whose jobs are told trusted or not.
fn forks_told(fleet: &Fleet) -> Vec<&str> {
    fleet
        .labels()
        .filter(|label| fleet.class(label).is_some_and(Class::tells_forks_apart))
        .collect()
}

/// Why a runner's row says it ended, where its run ended first.
pub(crate) fn run_ended(status: &str) -> String {
    format!("its run {status}")
}

/// Whether a run's status is one it does not leave.
fn ended(status: &str) -> bool {
    matches!(status, "succeeded" | "failed" | "cancelled")
}

impl Lookout {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        db: Db,
        fleet: Fleet,
        gitlab: Fleet,
        forge: Arc<Forge>,
        cells: BTreeMap<String, String>,
        token: String,
        reruns: Arc<Reruns>,
        environment: String,
        untrusted_environment: String,
    ) -> Lookout {
        Lookout {
            db,
            fleet,
            gitlab,
            forge,
            cells: cells
                .into_iter()
                .map(|(cell, base)| (cell, base.trim_end_matches('/').to_string()))
                .collect(),
            token,
            http: crate::config::Clients::new(|| {
                reqwest::Client::builder()
                    .timeout(CALL_TIMEOUT)
                    .user_agent(concat!("dagron-ci/", env!("CARGO_PKG_VERSION")))
            }),
            reruns,
            environment,
            untrusted_environment,
            environment_ids: Mutex::default(),
            floors: Mutex::default(),
        }
    }

    /// Claim the repository whose fleet is most overdue, look it over, and
    /// say when to look again. `false` when none was due.
    pub async fn look_one(&self, claim: &str) -> sqlx::Result<bool> {
        let Some(claimed) = self.db.claim_fleet(claim, LEASE).await? else {
            return Ok(false);
        };
        let again = self.look(&claimed).await?;
        self.db.finish_fleet(&claimed.repo.id, claim, again).await?;
        Ok(true)
    }

    /// The label map of `repo`'s forge.
    fn fleet_for(&self, repo: &Repo) -> &Fleet {
        match is_gitlab(repo) {
            true => &self.gitlab,
            false => &self.fleet,
        }
    }

    /// One look at a repository's fleet. How soon the next one is.
    async fn look(&self, claimed: &Claimed) -> sqlx::Result<Duration> {
        let repo = &claimed.repo;
        let fleet = self.fleet_for(repo);
        // What deliveries have not said, the forge is asked. Not of a
        // repository that was disconnected: its jobs were closed with it.
        if claimed.connected {
            match is_gitlab(repo) {
                true => self.gitlab_queue(repo, fleet).await?,
                false => self.tell_apart(repo, fleet).await?,
            }
            self.ask_unheard(repo).await?;
        }
        let view = self.db.fleet_view(&repo.id).await?;

        // What is over, or has come to nothing, first: each such runner is
        // one fewer the counting below takes for a runner out. Runners are
        // counted by their class and by what their run was submitted as.
        let mut out: BTreeMap<(&str, &str), Vec<&Runner>> = BTreeMap::new();
        // The places held by runners that failed to start.
        let mut held: BTreeMap<(&str, &str), usize> = BTreeMap::new();
        for runner in &view.runners {
            let kind = (runner.class.as_str(), runner.trust.as_str());
            match runner.state.as_str() {
                "ended"
                    if failed_to_start(runner)
                        && runner.ended_secs.unwrap_or(i64::MAX) < BACKOFF.as_secs() as i64 =>
                {
                    *held.entry(kind).or_default() += 1;
                }
                "ended" => {
                    self.sweep(repo, runner).await?;
                }
                "wanted" if runner.age_secs >= WANTED_FOR => {
                    self.db
                        .fleet_ended(&runner.name, "its launch was cut short")
                        .await?;
                }
                "wanted" => out.entry(kind).or_default().push(runner),
                "busy" if has_a_fork_s_job(fleet, runner) => {
                    tracing::warn!(
                        repo = %repo.full_name, runner = %runner.name,
                        job = runner.job_id.as_deref().unwrap_or("-"),
                        told = runner.job_trust.is_some(),
                        "fleet: the forge gave a runner started as a trusted job's a fork's job, \
                         or one not told apart in time; its run is stopped, and the job fails"
                    );
                    if let Some(run_id) = &runner.run_id {
                        self.cancel(&runner.cell, run_id).await;
                    }
                    self.db.fleet_ended(&runner.name, FORK_JOB).await?;
                }
                state => match self.over(runner).await {
                    Some(how) => {
                        self.db.fleet_ended(&runner.name, &how).await?;
                    }
                    None if state == "launched" => out.entry(kind).or_default().push(runner),
                    None => {}
                },
            }
        }
        // A job whose runner died is run again once that is seen, which the
        // runners above have just been looked at for: a runner swept here is
        // not looked at again, nor is a repository with nothing else open.
        if claimed.connected {
            self.retry_died(repo, fleet).await?;
        }

        // Then, by class and by trust: a runner for each job that waits, the
        // label's warm ones besides, and none other.
        let mut launches = 0;
        for label in fleet.labels() {
            let Some(class) = fleet.class(label) else {
                continue;
            };
            let waiting = match claimed.connected {
                true => view.queued.get(label).copied().unwrap_or_default(),
                // A repository that was disconnected is started nothing.
                false => Waiting::default(),
            };
            let (trusted, untrusted) = wanted(class, waiting);
            let warm = match claimed.connected
                && view
                    .newest
                    .get(label)
                    .is_some_and(|age| *age < class.warm_for() as i64)
            {
                true => class.warm as usize,
                false => 0,
            };
            for (trust, jobs, keep) in [(TRUSTED, trusted, warm), (UNTRUSTED, untrusted, 0)] {
                let waiting = jobs + keep;
                let spare = out
                    .get(&(label, trust))
                    .map(Vec::as_slice)
                    .unwrap_or_default();
                let held = held.get(&(label, trust)).copied().unwrap_or(0);
                let places = spare.len() + held;
                if waiting > places {
                    // What kept a runner of these from starting a moment ago
                    // would keep the next: none is started until it has
                    // held its place for its while.
                    if held > 0 {
                        continue;
                    }
                    for _ in places..waiting {
                        if launches == MAX_LAUNCHES {
                            break;
                        }
                        launches += 1;
                        match self.launch(repo, fleet, label, class, trust).await? {
                            Launch::Started => {}
                            Launch::Full => {
                                tracing::debug!(
                                    repo = %repo.full_name, label,
                                    "fleet: the label has as many runners out as its quota \
                                     lets it; its jobs wait"
                                );
                                break;
                            }
                            Launch::Refused(why) => {
                                tracing::warn!(
                                    repo = %repo.full_name, label, trust, %why,
                                    "fleet: a runner could not be started; its job waits"
                                );
                                break;
                            }
                        }
                    }
                } else {
                    // The newest first: the oldest has waited longest for the
                    // engine to start it, and is the likeliest to be up.
                    for runner in spare.iter().rev().take(spare.len().saturating_sub(waiting)) {
                        if runner.age_secs >= SPARE_FOR {
                            self.take_back(repo, runner).await?;
                        }
                    }
                    // Of those kept warm (the oldest are taken to be the
                    // jobs'), the oldest that has waited too long, one a
                    // look, is started anew at the next.
                    let kept = &spare[jobs.min(spare.len())..waiting.min(spare.len())];
                    if let Some(stale) = kept
                        .iter()
                        .find(|runner| runner.age_secs >= recycle_after(class))
                    {
                        tracing::info!(
                            repo = %repo.full_name, runner = %stale.name, label,
                            "fleet: a runner kept warm has waited its while; it is started anew"
                        );
                        self.take_back(repo, stale).await?;
                    }
                }
            }
        }
        // A runner of a label the file no longer has is nobody's to wait for.
        for ((label, _), spare) in &out {
            if fleet.class(label).is_none() {
                for runner in spare.iter().filter(|r| r.age_secs >= SPARE_FOR) {
                    self.take_back(repo, runner).await?;
                }
            }
        }
        Ok(AGAIN)
    }

    /// Ask GitHub whether the workflow runs of `repo`'s jobs not yet told
    /// apart are a fork's, for the classes that tell them apart. A run
    /// GitHub has no more is taken for a fork's: its jobs will not run. One
    /// it does not answer for is asked again at the next look.
    async fn tell_apart(&self, repo: &Repo, fleet: &Fleet) -> sqlx::Result<()> {
        let classes = forks_told(fleet);
        if classes.is_empty() {
            return Ok(());
        }
        for run in self.db.fleet_untold(&repo.id, &classes, TELLS).await? {
            let trust = match self.forge.workflow_run_is_own(repo, &run).await {
                Ok(Some(true)) => TRUSTED,
                Ok(Some(false) | None) => UNTRUSTED,
                Err(e) => {
                    tracing::debug!(
                        repo = %repo.full_name, workflow_run = %run, error = ?e,
                        "fleet: GitHub could not be asked whether a workflow run is a fork's; \
                         its jobs are taken for a fork's until it answers"
                    );
                    break;
                }
            };
            self.db.fleet_told(&repo.id, &run, trust).await?;
        }
        Ok(())
    }

    /// GitLab's queue: the jobs of `repo`'s project that wait, as GitLab lists
    /// them, each the fleet serves written down; and, where a class tells a
    /// fork's job apart, each pipeline not yet told, by its ref: a merge
    /// request's is the project's own where the merge request is, as GitLab
    /// says of it, and any other ref is a branch's or a tag's of the project.
    /// A pipeline none of whose jobs waits (a runner of the fleet's took one
    /// before a look listed it), or whose jobs named no ref, is asked of
    /// GitLab for its ref, and one GitLab no longer has, or gives no ref of,
    /// is taken for a fork's.
    async fn gitlab_queue(&self, repo: &Repo, fleet: &Fleet) -> sqlx::Result<()> {
        let pending = match self.forge.gitlab_pending_jobs(repo).await {
            Ok((pending, all)) => {
                if !all {
                    tracing::warn!(
                        repo = %repo.full_name,
                        "fleet: GitLab lists more jobs that wait than a look reads; those past \
                         them are read once the first are given runners or end"
                    );
                }
                pending
            }
            Err(e) => {
                tracing::debug!(
                    repo = %repo.full_name, error = ?e,
                    "fleet: GitLab could not be asked which of the project's jobs wait"
                );
                return Ok(());
            }
        };
        let mut refs: HashMap<String, Option<String>> = HashMap::new();
        for (job, git_ref) in pending {
            let Some((class, _)) = fleet.serves(&job.labels) else {
                continue;
            };
            self.db.fleet_job(repo, &job, class).await?;
            refs.insert(job.run_id.to_string(), git_ref);
        }
        let classes = forks_told(fleet);
        if classes.is_empty() {
            return Ok(());
        }
        for pipeline in self.db.fleet_untold(&repo.id, &classes, TELLS).await? {
            // A pipeline none of whose jobs waits, or whose jobs named no
            // ref, is asked of GitLab: a ref it does not give is a fork's.
            let git_ref = match refs.remove(&pipeline).flatten() {
                Some(git_ref) => Some(git_ref),
                None => match self.forge.gitlab_pipeline_ref(repo, &pipeline).await {
                    Ok(git_ref) => git_ref,
                    Err(e) => {
                        tracing::debug!(
                            repo = %repo.full_name, pipeline = %pipeline, error = ?e,
                            "fleet: GitLab could not be asked a pipeline's ref; its jobs are \
                             taken for a fork's until it answers"
                        );
                        break;
                    }
                },
            };
            let trust = match git_ref.as_deref().map(merge_request_of) {
                None => UNTRUSTED,
                Some(None) => TRUSTED,
                Some(Some(iid)) => match self.forge.gitlab_merge_request_is_own(repo, iid).await {
                    Ok(Some(true)) => TRUSTED,
                    Ok(Some(false) | None) => UNTRUSTED,
                    Err(e) => {
                        tracing::debug!(
                            repo = %repo.full_name, merge_request = iid, error = ?e,
                            "fleet: GitLab could not be asked whether a merge request is a \
                             fork's; its jobs are taken for a fork's until it answers"
                        );
                        break;
                    }
                },
            };
            self.db.fleet_told(&repo.id, &pipeline, trust).await?;
        }
        Ok(())
    }

    /// Ask GitHub how the jobs of `repo` stand that no delivery has moved on
    /// for a while ([`crate::fleet::store`]'s `fleet_unheard`), and write
    /// what it says.
    async fn ask_unheard(&self, repo: &Repo) -> sqlx::Result<()> {
        let unheard = self
            .db
            .fleet_unheard(&repo.id, UNHEARD_AFTER, ASK_AGAIN, ASKS)
            .await?;
        for (job_id, class) in unheard {
            self.db.fleet_asked(&repo.forge_host, &job_id).await?;
            let asked = match is_gitlab(repo) {
                true => self.forge.gitlab_job(repo, &job_id).await,
                false => self.forge.workflow_job(repo, &job_id).await,
            };
            match asked {
                Ok(Some(job)) => {
                    if job.action != crate::github::JobAction::Queued {
                        tracing::info!(
                            repo = %repo.full_name, job = %job_id,
                            "fleet: a job no delivery said had moved on has, as its forge says"
                        );
                        self.db.fleet_job(repo, &job, &class).await?;
                    }
                }
                Ok(None) => self.db.fleet_job_gone(&repo.forge_host, &job_id).await?,
                Err(e) => {
                    tracing::debug!(
                        repo = %repo.full_name, job = %job_id, error = ?e,
                        "fleet: the forge could not be asked how a job stands"
                    );
                    break;
                }
            }
        }
        Ok(())
    }

    /// Run again the jobs of `repo` whose runner died, once each, as the
    /// module says. A job that is not to be is marked so, with why, and is
    /// not looked at again.
    async fn retry_died(&self, repo: &Repo, fleet: &Fleet) -> sqlx::Result<()> {
        for died in self.db.fleet_died(&repo.id, RETRY_WITHIN, RETRIES).await? {
            let job = died.job_id.as_str();
            let refuse = |why: String| async move {
                tracing::info!(
                    repo = %repo.full_name, job, %why,
                    "fleet: a job whose runner died is not run again"
                );
                self.db
                    .fleet_retry(&repo.forge_host, job, "refused", None, Some(&why))
                    .await
            };
            // Run again already: GitHub took an ask whose answer was lost,
            // or someone ran it again by hand.
            if self.db.fleet_run_again(&repo.id, job).await? {
                tracing::info!(
                    repo = %repo.full_name, job,
                    "fleet: a job whose runner died was run again already"
                );
                self.db
                    .fleet_retry(
                        &repo.forge_host,
                        job,
                        "asked",
                        None,
                        Some("a later attempt of it was queued"),
                    )
                    .await?;
                continue;
            }
            let Some(class) = fleet.class(&died.class) else {
                refuse(format!(
                    "its label {:?} is no longer the fleet's",
                    died.class
                ))
                .await?;
                continue;
            };
            if !class.retry {
                refuse("its label says `retry: false`".into()).await?;
                continue;
            }
            if died
                .lived_secs
                .is_some_and(|lived| lived as u64 + TIME_LEFT >= class.timeout_secs)
            {
                refuse(format!(
                    "its runner had its label's time ({} s), which another would have too",
                    class.timeout_secs
                ))
                .await?;
                continue;
            }
            if died.ended_secs as u64 > RETRY_WITHIN.as_secs() {
                refuse(format!(
                    "GitHub would not run it again within {} minutes of its end: its workflow \
                     run was still going",
                    RETRY_WITHIN.as_secs() / 60
                ))
                .await?;
                continue;
            }
            if self
                .db
                .fleet_retried(&repo.id, &died.workflow_run, &died.name, job)
                .await?
            {
                refuse("a job of its name in its workflow run was run again once already".into())
                    .await?;
                continue;
            }
            let said = match is_gitlab(repo) {
                true => self.forge.gitlab_retry_job(repo, &died.job_id).await,
                false => self.forge.retry_job(repo, &died.job_id).await,
            };
            let said = match said {
                Ok(said) => said,
                Err(e) => Retried::NotYet(format!("{e:?}")),
            };
            match said {
                Retried::Asked => {
                    tracing::info!(
                        repo = %repo.full_name, job = %died.job_id, name = %died.name,
                        "fleet: a job whose runner died is run again"
                    );
                    self.db
                        .fleet_retry(&repo.forge_host, &died.job_id, "asked", None, None)
                        .await?;
                }
                Retried::NotYet(why) => {
                    tracing::debug!(
                        repo = %repo.full_name, job = %died.job_id, %why,
                        "fleet: GitHub would not run a job whose runner died again yet"
                    );
                    self.db
                        .fleet_retry(
                            &repo.forge_host,
                            &died.job_id,
                            "due",
                            Some(RETRY_EVERY),
                            Some(&why),
                        )
                        .await?;
                }
                Retried::Refused(why) => refuse(why).await?,
            }
        }
        Ok(())
    }

    /// How the run that hosts `runner` ended, if it has: the runner is then
    /// over, whatever GitHub last said of it. `None` while it runs, and
    /// where its cell could not be asked.
    async fn over(&self, runner: &Runner) -> Option<String> {
        let run_id = runner.run_id.as_deref()?;
        match self.reruns.sight(&runner.cell, run_id).await {
            Ok(Sight::Running) => None,
            // Not running yet is not over: a run waits for the engine.
            Ok(Sight::Over(status)) if !ended(&status) => None,
            Ok(Sight::Over(status)) => Some(run_ended(&status)),
            Ok(Sight::Gone) => Some("its cell has no such run".to_string()),
            Err(why) => {
                tracing::debug!(runner = %runner.name, %why, "fleet: a runner's run could not be looked at");
                None
            }
        }
    }

    /// The environment a runner's configuration is kept in, by what its run
    /// is submitted as.
    fn environment_for(&self, trust: &str) -> &str {
        match trust {
            UNTRUSTED => &self.untrusted_environment,
            _ => &self.environment,
        }
    }

    /// Start a runner of `class` for `repo`, as `trust`: its row, its
    /// registration, its configuration kept as a secret, and the run that
    /// hosts it. Whatever of it was made is taken back where none is
    /// started.
    async fn launch(
        &self,
        repo: &Repo,
        fleet: &Fleet,
        label: &str,
        class: &Class,
        trust: &str,
    ) -> sqlx::Result<Launch> {
        let name = format!(
            "dagron-{}",
            &uuid::Uuid::now_v7().simple().to_string()[20..]
        );
        debug_assert!(is_runner_name(&name));
        // Its row first: one that comes to nothing holds its place, and says
        // why ([`failed_to_start`]).
        let quota = Quota {
            all: class.max_runners,
            repository: class.max_per_repository,
        };
        let org = class.group().map(|_| owner_of(repo));
        if !self
            .db
            .fleet_want(repo, label, &name, org.as_deref(), trust, quota)
            .await?
        {
            return Ok(Launch::Full);
        }
        let Some(base) = self.cells.get(&repo.cell) else {
            let why = format!("its cell {:?} is not in CI_CELLS", repo.cell);
            return self.give_up(&name, why).await;
        };
        // Somewhere to keep the configuration, before one is asked for.
        let environment_name = self.environment_for(trust);
        let environment = match self.environment(&repo.cell, base, environment_name).await {
            Ok(id) => id,
            Err(why) => return self.give_up(&name, why).await,
        };
        if trust == UNTRUSTED {
            if let Err(why) = self.floor(&repo.cell, base).await {
                return self.give_up(&name, why).await;
            }
        }
        let labels = fleet.registered(label);
        let registered = match is_gitlab(repo) {
            true => {
                let group = class.scope == super::Scope::Organization;
                self.forge
                    .gitlab_create_runner(repo, &name, &labels, group, class.timeout_secs)
                    .await
            }
            false => {
                self.forge
                    .jit_runner(repo, &name, &labels, class.group())
                    .await
            }
        };
        let jit = match registered {
            Ok(jit) => jit,
            Err(
                crate::forge::ForgeError::Failed(why)
                | crate::forge::ForgeError::RateLimited { why, .. },
            ) => return self.give_up(&name, why).await,
        };
        self.db
            .fleet_registered(&name, &jit.runner_id.to_string())
            .await?;
        let secret = secret_name(&name);
        if let Err(why) = self
            .secret(base, &environment, &secret, Some(&jit.config))
            .await
        {
            return self
                .give_up(&name, format!("its configuration could not be kept: {why}"))
                .await;
        }
        let gitlab = is_gitlab(repo).then(|| format!("https://{}", repo.forge_host));
        let spec = run_spec(
            label,
            class,
            environment_name,
            &name,
            trust,
            gitlab.as_deref(),
        );
        let trigger = (trust == UNTRUSTED).then(|| floor_trigger(repo, &name));
        match self.submit(base, &name, &spec, trigger).await {
            Ok(run_id) => {
                self.db.fleet_launched(&name, &run_id).await?;
                tracing::info!(
                    repo = %repo.full_name, label, trust, runner = %name, run_id = %run_id,
                    org = org.as_deref().unwrap_or("-"),
                    "fleet: a runner was started"
                );
                Ok(Launch::Started)
            }
            Err(why) => {
                self.give_up(&name, format!("its run could not be submitted: {why}"))
                    .await
            }
        }
    }

    /// A launch that came to nothing: the runner is over, and says why.
    /// What was made of it (a registration, a secret) is cleared up as any
    /// ended runner is, at the next look.
    async fn give_up(&self, name: &str, why: String) -> sqlx::Result<Launch> {
        self.db.fleet_ended(name, &why).await?;
        Ok(Launch::Refused(why))
    }

    /// Take back a runner nothing waits for: its registration, and then its
    /// run. Where GitHub says it is running a job, it is left to it.
    async fn take_back(&self, repo: &Repo, runner: &Runner) -> sqlx::Result<()> {
        if let Some(forge_id) = &runner.forge_id {
            match self.remove(repo, runner, forge_id).await {
                Ok(true) => {}
                // It has a job the forge has not told of yet.
                Ok(false) => return Ok(()),
                Err(e) => {
                    tracing::warn!(runner = %runner.name, error = ?e, "fleet: a spare runner could not be removed; it is tried again");
                    return Ok(());
                }
            }
        }
        if let Some(run_id) = &runner.run_id {
            self.cancel(&runner.cell, run_id).await;
        }
        if self.db.fleet_ended(&runner.name, NO_JOB).await? {
            tracing::info!(repo = %repo.full_name, runner = %runner.name, "fleet: a spare runner was taken back");
        }
        Ok(())
    }

    /// Clear up after a runner that is over: its registration, where GitHub
    /// still has one; its run, where it still runs; and its secret. `false`
    /// where something of it could not be taken back yet, and it is looked
    /// at again.
    ///
    /// A runner whose job has ended is expected to exit by itself, and its
    /// run to end with it: the run is left for [`EXIT_FOR`] before it is
    /// asked to stop, and the runner is looked at again meanwhile.
    async fn sweep(&self, repo: &Repo, runner: &Runner) -> sqlx::Result<bool> {
        // Whether its run still runs, as far as can be seen: looked at first,
        // because a runner that is still exiting is left whole.
        let running = match &runner.run_id {
            None => None,
            // How it ended is kept, whatever the runner's row says ended it
            // first: whether the runner died is its run's to say.
            Some(run_id) => match self.reruns.sight(&runner.cell, run_id).await {
                Ok(Sight::Gone) => {
                    self.db.fleet_run_end(&runner.name, "gone").await?;
                    None
                }
                Ok(Sight::Over(status)) if ended(&status) => {
                    self.db.fleet_run_end(&runner.name, &status).await?;
                    None
                }
                // Running, waiting, or not known.
                _ => Some(run_id),
            },
        };
        // Its job has just ended: it is given time to exit by itself, with
        // its registration and its secret as they are.
        if running.is_some()
            && runner.reason.as_deref() == Some(JOB_ENDED)
            && runner.ended_secs.unwrap_or(i64::MAX) < EXIT_FOR
        {
            return Ok(false);
        }
        // A runner stopped for a fork's job has its run asked to stop again,
        // first: GitHub keeps a busy runner's registration.
        if let Some(run_id) = running.filter(|_| runner.reason.as_deref() == Some(FORK_JOB)) {
            self.cancel(&runner.cell, run_id).await;
        }
        if let Some(forge_id) = &runner.forge_id {
            match self.remove(repo, runner, forge_id).await {
                Ok(true) => {}
                Ok(false) => return Ok(false),
                Err(e) => {
                    tracing::warn!(runner = %runner.name, error = ?e, "fleet: a runner's registration could not be removed; it is tried again");
                    return Ok(false);
                }
            }
        }
        // Still running, with no more time to be given: asked to stop.
        if let Some(run_id) = running {
            self.cancel(&runner.cell, run_id).await;
        }
        // A runner never registered was given no configuration to keep.
        if let Some(base) = self
            .cells
            .get(&runner.cell)
            .filter(|_| runner.forge_id.is_some())
        {
            let name = self.environment_for(&runner.trust);
            match self.environment(&runner.cell, base, name).await {
                Ok(environment) => {
                    let secret = secret_name(&runner.name);
                    if let Err(why) = self.secret(base, &environment, &secret, None).await {
                        tracing::warn!(runner = %runner.name, %why, "fleet: a runner's secret could not be deleted; it is tried again");
                        return Ok(false);
                    }
                }
                Err(why) => {
                    tracing::warn!(runner = %runner.name, %why, "fleet: a runner's secret could not be deleted; it is tried again");
                    return Ok(false);
                }
            }
        }
        self.db.fleet_swept(&runner.name).await?;
        Ok(true)
    }

    /// Remove `runner`'s registration, `forge_id`, at its forge: `false`
    /// where it is running a job, and is kept.
    async fn remove(
        &self,
        repo: &Repo,
        runner: &Runner,
        forge_id: &str,
    ) -> Result<bool, crate::forge::ForgeError> {
        match is_gitlab(repo) {
            true => self.forge.gitlab_remove_runner(repo, forge_id).await,
            false => {
                self.forge
                    .remove_runner(repo, forge_id, runner.org.as_deref())
                    .await
            }
        }
    }

    fn call(&self, method: reqwest::Method, url: &str) -> reqwest::RequestBuilder {
        self.http
            .to(url)
            .request(method, url)
            .bearer_auth(&self.token)
    }

    /// The id of the environment `name` in a cell's dagron-api: found by its
    /// name, and made where the cell has none.
    async fn environment(&self, cell: &str, base: &str, name: &str) -> Result<String, String> {
        let key = (cell.to_string(), name.to_string());
        if let Some(id) = self
            .environment_ids
            .lock()
            .ok()
            .and_then(|ids| ids.get(&key).cloned())
        {
            return Ok(id);
        }
        let (id, _) = self.find_environment(base, name).await?;
        if let Ok(mut ids) = self.environment_ids.lock() {
            ids.insert(key, id.clone());
        }
        Ok(id)
    }

    /// [`Self::environment`], asked of the cell: its id, and whether its
    /// policy admits a run that is not trusted, where it has one: `None` for
    /// one with no policy, as dagron-ci makes it, which admits none.
    async fn find_environment(
        &self,
        base: &str,
        name: &str,
    ) -> Result<(String, Option<bool>), String> {
        let url = format!("{base}/api/environments");
        let said = |what: &str, e: &dyn std::fmt::Display| format!("{what} {url}: {e}");
        let response = self
            .call(reqwest::Method::GET, &url)
            .send()
            .await
            .map_err(|e| said("reading", &e))?;
        if !response.status().is_success() {
            return Err(said(
                "reading",
                &format!("HTTP {}", response.status().as_u16()),
            ));
        }
        let listed: Vec<Environment> = response.json().await.map_err(|e| said("reading", &e))?;
        if let Some(found) = listed.into_iter().find(|e| e.name == name) {
            let admits = found
                .policy
                .map(|p| p.allowed_trust.iter().any(|t| t == UNTRUSTED));
            return Ok((found.id, admits));
        }
        let description = match name == self.untrusted_environment {
            true => "dagron-ci's fleet: the configuration of each runner started for a fork's job, while it is out",
            false => "dagron-ci's fleet: each runner's configuration, while it is out",
        };
        let response = self
            .call(reqwest::Method::POST, &url)
            .json(&serde_json::json!({ "name": name, "description": description }))
            .send()
            .await
            .map_err(|e| said("making an environment at", &e))?;
        if !response.status().is_success() {
            return Err(said(
                "making an environment at",
                &format!("HTTP {}", response.status().as_u16()),
            ));
        }
        let made: Environment = response
            .json()
            .await
            .map_err(|e| said("making an environment at", &e))?;
        tracing::info!(environment = %name, "fleet: the environment its runners' configurations are kept in was made");
        Ok((made.id, None))
    }

    /// Whether a cell may be given a runner for a fork's job, as the cell
    /// says, believed for [`RECHECK`]: `Err` is why not.
    ///
    /// - The cell takes a record of what started a run from dagron-ci's
    ///   token (the `ci:trigger` group). Without one nothing says the run is
    ///   untrusted, and it would run as any other.
    /// - The environment its configuration is kept in admits such a run. One
    ///   with no policy, as dagron-ci makes it, is given one that does,
    ///   for the fleet's runs alone, where dagron-ci's token may write it. One
    ///   an admin gave a policy is left as they wrote it.
    async fn floor(&self, cell: &str, base: &str) -> Result<(), String> {
        if let Some((asked, said)) = self.floors.lock().ok().and_then(|f| f.get(cell).cloned()) {
            if asked.elapsed() < RECHECK {
                return said;
            }
        }
        let said = self.ask_floor(cell, base).await;
        if let Ok(mut floors) = self.floors.lock() {
            floors.insert(cell.to_string(), (Instant::now(), said.clone()));
        }
        said
    }

    async fn ask_floor(&self, cell: &str, base: &str) -> Result<(), String> {
        let me = format!("{base}/api/me");
        let groups = match self.call(reqwest::Method::GET, &me).send().await {
            Ok(response) if response.status().is_success() => response
                .json::<serde_json::Value>()
                .await
                .map_err(|e| format!("reading {me}: {e}"))?["groups"]
                .clone(),
            Ok(response) => {
                return Err(format!("reading {me}: HTTP {}", response.status().as_u16()))
            }
            Err(e) => return Err(format!("reading {me}: {e}")),
        };
        let group = crate::dispatch::TRIGGER_GROUP;
        if !groups
            .as_array()
            .is_some_and(|groups| groups.iter().any(|g| g == group))
        {
            return Err(format!(
                "cell {cell:?} takes no record of what started a run from dagron-ci's token \
                 (the `{group}` group), so nothing would say a fork's job's runner is not \
                 trusted: none is started"
            ));
        }
        let name = &self.untrusted_environment;
        let (id, admits) = self.find_environment(base, name).await?;
        if let Ok(mut ids) = self.environment_ids.lock() {
            ids.insert((cell.to_string(), name.clone()), id.clone());
        }
        let refused = |why: String| {
            Err(format!(
                "the environment {name:?} of cell {cell:?} admits no run that is not trusted \
                 ({why}): an admin gives it a policy that does, \
                 `PUT /api/environments/{id}/policy {{\"allowed_trust\": [\"{UNTRUSTED}\"]}}`"
            ))
        };
        match admits {
            Some(true) => return Ok(()),
            Some(false) => return refused("an admin gave it a policy that does not".into()),
            None => {}
        }
        let url = format!("{base}/api/environments/{id}/policy");
        let response = self
            .call(reqwest::Method::PUT, &url)
            .json(&serde_json::json!({
                "allowed_trust": [UNTRUSTED],
                "allowed_pipelines": [FLEET_PIPELINES],
            }))
            .send()
            .await
            .map_err(|e| format!("{url}: {e}"))?;
        match response.status().as_u16() {
            200..=299 => {
                tracing::info!(
                    cell, environment = %name,
                    "fleet: the environment of a fork's job's runners admits such runs, the \
                     fleet's alone"
                );
                Ok(())
            }
            code => refused(format!(
                "dagron-ci's token may not write its policy: HTTP {code}"
            )),
        }
    }

    /// Set a secret of the fleet's environment to `value`, or delete it.
    async fn secret(
        &self,
        base: &str,
        environment: &str,
        name: &str,
        value: Option<&str>,
    ) -> Result<(), String> {
        let url = format!("{base}/api/environments/{environment}/secrets/{name}");
        let request = match value {
            Some(value) => self
                .call(reqwest::Method::PUT, &url)
                .json(&serde_json::json!({ "value": value })),
            None => self.call(reqwest::Method::DELETE, &url),
        };
        let response = request.send().await.map_err(|e| format!("{url}: {e}"))?;
        match (response.status().as_u16(), value.is_some()) {
            (200..=299, _) | (404, false) => Ok(()),
            // No key to encrypt it with: it is not kept in the clear instead.
            (503, true) => Err(format!(
                "{url}: HTTP 503; the cell's dagron-api keeps no secret without a key \
                 (DAGRON_ENV_SECRET_KEY)"
            )),
            (code, _) => Err(format!("{url}: HTTP {code}")),
        }
    }

    /// Submit the run that hosts a runner, with its trigger record where it
    /// has one. Its name is the submit's key, so a submit made twice is one
    /// run.
    async fn submit(
        &self,
        base: &str,
        runner: &str,
        spec: &str,
        trigger: Option<serde_json::Value>,
    ) -> Result<String, String> {
        let url = format!("{base}/api/runs");
        let mut body = serde_json::json!({ "yaml": spec, "parameters": {} });
        if let Some(trigger) = trigger {
            body["trigger"] = trigger;
        }
        let response = self
            .call(reqwest::Method::POST, &url)
            .header("Idempotency-Key", format!("fleet-{runner}"))
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("{url}: {e}"))?;
        let status = response.status().as_u16();
        if !matches!(status, 200 | 201) {
            let said: String = response
                .text()
                .await
                .unwrap_or_default()
                .trim()
                .chars()
                .take(300)
                .collect();
            return Err(format!("{url}: HTTP {status}: {said}"));
        }
        response
            .json::<Submitted>()
            .await
            .map(|run| run.run_id)
            .map_err(|e| format!("{url}: no run id in the answer: {e}"))
    }

    /// Ask a cell to stop a run. Said, and not waited on: a run that is over
    /// already answers that it is.
    async fn cancel(&self, cell: &str, run_id: &str) {
        let Some(base) = self.cells.get(cell) else {
            return;
        };
        let url = format!("{base}/api/runs/{run_id}/cancel");
        match self.call(reqwest::Method::POST, &url).send().await {
            Ok(response) if response.status().is_success() => {}
            Ok(response) => tracing::debug!(
                run_id,
                status = response.status().as_u16(),
                "fleet: a runner's run was not cancelled; it may be over"
            ),
            Err(e) => {
                tracing::warn!(run_id, error = %e, "fleet: a runner's run could not be cancelled")
            }
        }
    }
}

/// Whether `runner`, started as a trusted job's, was given a fork's job
/// that its class gives no such runner: one told so, or one of the fleet's
/// that has not been told apart in [`UNTOLD_FOR`]. A job of a repository
/// the fleet keeps no job of (an organization's runner may take one) is
/// its runner group's to have let in.
fn has_a_fork_s_job(fleet: &Fleet, runner: &Runner) -> bool {
    runner.trust == TRUSTED
        && (runner.job_trust.as_deref() == Some(UNTRUSTED)
            || runner
                .job_untold_secs
                .is_some_and(|secs| secs >= UNTOLD_FOR))
        && fleet
            .class(&runner.class)
            .is_some_and(Class::tells_forks_apart)
}

/// The merge request a GitLab job's ref is of: `refs/merge-requests/{iid}/…`.
fn merge_request_of(git_ref: &str) -> Option<i64> {
    git_ref
        .strip_prefix("refs/merge-requests/")?
        .split('/')
        .next()?
        .parse()
        .ok()
}

/// How many runners a class wants out for the jobs that wait, as trusted
/// runs and as runs that are not trusted. A job not yet told apart is taken
/// for a fork's, where the class tells them apart.
fn wanted(class: &Class, waiting: Waiting) -> (usize, usize) {
    let count = |n: i64| n.max(0) as usize;
    match class.untrusted {
        Untrusted::Serve => (count(waiting.all()), 0),
        Untrusted::Refuse => (count(waiting.trusted), 0),
        Untrusted::Floor => (
            count(waiting.trusted),
            count(waiting.untrusted + waiting.unknown),
        ),
    }
}

/// Start the fleet's looker. It stops when `stop` turns true, after the
/// look in hand.
pub fn spawn(lookout: Arc<Lookout>, mut stop: watch::Receiver<bool>) -> JoinHandle<()> {
    tokio::spawn(async move {
        let claim = format!("fleet-{}", uuid::Uuid::now_v7());
        while !*stop.borrow() {
            let looked = match lookout.look_one(&claim).await {
                Ok(looked) => looked,
                Err(e) => {
                    tracing::warn!(error = %e, "fleet: the database failed");
                    false
                }
            };
            if !looked {
                tokio::select! {
                    _ = tokio::time::sleep(IDLE) => {}
                    _ = stop.changed() => {}
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use axum::extract::{Path, State};
    use axum::http::{HeaderMap, StatusCode};
    use axum::response::{IntoResponse, Response};
    use axum::routing::{get, post, put};
    use axum::{Json, Router};
    use serde_json::{json, Value};

    use super::*;
    use crate::db::tests::{exec, sqlite};
    use crate::forge::tests::{stub_github, StubGithub};
    use crate::github::{Job, JobAction};
    use crate::metrics::Metrics;

    const SHA: &str = "4b4708e725f29891d22b4c96af9f0a403ff51c7c";
    const FLEET: &str =
        "labels:\n  dagron:\n    image: ghcr.io/actions/actions-runner:2.321.0\n    pool: ci\n";

    /// A cell's dagron-api, as much of it as the fleet asks.
    #[derive(Default)]
    struct Cell {
        /// The environments it has: id and name.
        environments: Vec<(String, String)>,
        /// Each secret set: environment id, name, value.
        secrets: Vec<(String, String, String)>,
        /// The names of the secrets deleted, in order.
        deleted: Vec<String>,
        /// Each run submitted: its id, its key, its spec.
        runs: Vec<(String, String, String)>,
        /// How each run reads.
        status: BTreeMap<String, String>,
        /// The runs asked to stop, in order.
        cancelled: Vec<String>,
        /// A status every submit is answered instead.
        refuse_submit: Option<u16>,
        /// The groups dagron-ci's token is in, as `GET /api/me` says.
        groups: Vec<String>,
        /// Each environment's policy, by its id, where it has one.
        policies: BTreeMap<String, Value>,
        /// A status a policy's write is answered instead.
        refuse_policy: Option<u16>,
        /// The trigger record each run was submitted with, by its id.
        triggers: BTreeMap<String, Value>,
    }

    type StubCell = Arc<Mutex<Cell>>;

    async fn stub_cell() -> (String, StubCell) {
        async fn environments(State(cell): State<StubCell>) -> Response {
            let cell = cell.lock().unwrap();
            let listed: Vec<Value> = cell
                .environments
                .iter()
                .map(
                    |(id, name)| json!({ "id": id, "name": name, "policy": cell.policies.get(id) }),
                )
                .collect();
            Json(listed).into_response()
        }
        async fn make(State(cell): State<StubCell>, Json(body): Json<Value>) -> Response {
            let mut cell = cell.lock().unwrap();
            let name = body["name"].as_str().unwrap().to_string();
            let id = format!("env-{}", cell.environments.len() + 1);
            cell.environments.push((id.clone(), name.clone()));
            (StatusCode::CREATED, Json(json!({ "id": id, "name": name }))).into_response()
        }
        async fn set(
            State(cell): State<StubCell>,
            Path((id, name)): Path<(String, String)>,
            Json(body): Json<Value>,
        ) -> Response {
            let value = body["value"].as_str().unwrap().to_string();
            cell.lock().unwrap().secrets.push((id, name, value));
            StatusCode::NO_CONTENT.into_response()
        }
        async fn unset(
            State(cell): State<StubCell>,
            Path((_, name)): Path<(String, String)>,
        ) -> Response {
            cell.lock().unwrap().deleted.push(name);
            StatusCode::NO_CONTENT.into_response()
        }
        async fn submit(
            State(cell): State<StubCell>,
            headers: HeaderMap,
            Json(body): Json<Value>,
        ) -> Response {
            let mut cell = cell.lock().unwrap();
            if let Some(code) = cell.refuse_submit {
                return (StatusCode::from_u16(code).unwrap(), "no room").into_response();
            }
            let key = headers["idempotency-key"].to_str().unwrap().to_string();
            let id = format!("run-{}", cell.runs.len() + 1);
            let spec = body["yaml"].as_str().unwrap().to_string();
            if let Some(trigger) = body.get("trigger") {
                cell.triggers.insert(id.clone(), trigger.clone());
            }
            cell.runs.push((id.clone(), key, spec));
            cell.status.insert(id.clone(), "running".into());
            (StatusCode::CREATED, Json(json!({ "run_id": id }))).into_response()
        }
        async fn run(State(cell): State<StubCell>, Path(id): Path<String>) -> Response {
            match cell.lock().unwrap().status.get(&id) {
                Some(status) => Json(json!({ "id": id, "status": status })).into_response(),
                None => StatusCode::NOT_FOUND.into_response(),
            }
        }
        async fn cancel(State(cell): State<StubCell>, Path(id): Path<String>) -> Response {
            let mut cell = cell.lock().unwrap();
            cell.cancelled.push(id.clone());
            cell.status.insert(id, "cancelled".into());
            Json(json!({})).into_response()
        }
        async fn me(State(cell): State<StubCell>) -> Response {
            Json(json!({ "user": "dagron-ci", "groups": cell.lock().unwrap().groups }))
                .into_response()
        }
        async fn policy(
            State(cell): State<StubCell>,
            Path(id): Path<String>,
            Json(body): Json<Value>,
        ) -> Response {
            let mut cell = cell.lock().unwrap();
            if let Some(code) = cell.refuse_policy {
                return StatusCode::from_u16(code).unwrap().into_response();
            }
            cell.policies.insert(id, body.clone());
            Json(body).into_response()
        }
        let cell: StubCell = Arc::default();
        let app = Router::new()
            .route("/api/me", get(me))
            .route("/api/environments/{id}/policy", put(policy))
            .route("/api/environments", get(environments).post(make))
            .route(
                "/api/environments/{id}/secrets/{name}",
                put(set).delete(unset),
            )
            .route("/api/runs", post(submit))
            .route("/api/runs/{id}", get(run))
            .route("/api/runs/{id}/cancel", post(cancel))
            .with_state(cell.clone());
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
        (base, cell)
    }

    struct Rig {
        db: Db,
        repo: Repo,
        lookout: Lookout,
        gh: StubGithub,
        cell: StubCell,
        _dir: tempfile::TempDir,
    }

    /// A fleet of one label over one repository, an App that may register
    /// its runners, and a cell. `None`, and the test skipped, where there
    /// is no `openssl` to make the App's key with.
    async fn fleet_rig() -> Option<Rig> {
        fleet_rig_with(FLEET).await
    }

    /// [`fleet_rig`], with the label map `fleet`.
    async fn fleet_rig_with(fleet: &str) -> Option<Rig> {
        let dir = tempfile::tempdir().unwrap();
        let (private, _) = crate::checkout::tests::rsa_key(dir.path())?;
        let app = crate::checkout::GitHubApp::new(4242, &private).unwrap();
        let (origin, gh) = stub_github().await;
        gh.lock().unwrap().installed.insert("acme/widgets".into());
        let (base, cell) = stub_cell().await;
        let (db, db_dir) = sqlite().await;
        drop(dir);
        let repo = db
            .add_repo("github", "github.com", "acme/widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        let cells = BTreeMap::from([("default".to_string(), base)]);
        let forge = Forge::new(None).with_origin(&origin).with_app(Some(app));
        let reruns = Reruns::new(cells.clone(), "dgr_ci".into(), Arc::new(Metrics::default()));
        let lookout = Lookout::new(
            db.clone(),
            Fleet::parse(fleet).unwrap(),
            Fleet::default(),
            Arc::new(forge),
            cells,
            "dgr_ci".into(),
            Arc::new(reruns),
            "dagron-ci-fleet".into(),
            "dagron-ci-fleet-untrusted".into(),
        );
        Some(Rig {
            db,
            repo,
            lookout,
            gh,
            cell,
            _dir: db_dir,
        })
    }

    impl Rig {
        /// GitHub delivers what became of a job.
        async fn job(
            &self,
            id: u64,
            action: JobAction,
            runner: Option<&str>,
            conclusion: Option<&str>,
        ) {
            self.job_of(77, id, action, runner, conclusion).await
        }

        /// [`Rig::job`], of the workflow run `run`.
        async fn job_of(
            &self,
            run: u64,
            id: u64,
            action: JobAction,
            runner: Option<&str>,
            conclusion: Option<&str>,
        ) {
            let job = Job {
                forge_host: "github.com".into(),
                full_name: "acme/widgets".into(),
                action,
                id,
                run_id: run,
                run_attempt: 1,
                name: "build".into(),
                workflow: Some("CI".into()),
                labels: vec![
                    "self-hosted".into(),
                    "linux".into(),
                    "x64".into(),
                    "dagron".into(),
                ],
                head_sha: SHA.into(),
                runner_id: None,
                runner_name: runner.map(str::to_string),
                conclusion: conclusion.map(str::to_string),
            };
            self.db.fleet_job(&self.repo, &job, "dagron").await.unwrap();
        }

        /// Look the fleet over now, whenever it was next due.
        async fn look(&self) -> bool {
            exec(
                &self.db,
                "UPDATE ci_repos SET fleet_due_at = '2000-01-01T00:00:00Z', fleet_claim = NULL \
                 WHERE fleet_due_at IS NOT NULL",
            )
            .await;
            self.lookout.look_one("test").await.unwrap()
        }

        /// The runners not yet swept: name, state, why.
        async fn runners(&self) -> Vec<(String, String, Option<String>)> {
            let sql = "SELECT name, state, reason FROM ci_fleet_runners \
                       WHERE swept_at IS NULL ORDER BY created_at, name";
            match &self.db {
                Db::Postgres(p) => sqlx::query_as(sql).fetch_all(p).await.unwrap(),
                Db::Sqlite(p) => sqlx::query_as(sql).fetch_all(p).await.unwrap(),
            }
        }

        /// Make every runner out as old as one nothing has waited for long.
        async fn age(&self) {
            exec(
                &self.db,
                "UPDATE ci_fleet_runners SET created_at = '2000-01-01T00:00:00Z'",
            )
            .await;
        }
    }

    /// A runner that does not exit after its job is stopped, once it has
    /// been given the time one takes to.
    #[tokio::test]
    async fn a_runner_that_does_not_exit_after_its_job_is_stopped_in_time() {
        let Some(rig) = fleet_rig().await else {
            return;
        };
        rig.job(1, JobAction::Queued, None, None).await;
        assert!(rig.look().await);
        let name = rig.runners().await[0].0.clone();
        rig.job(1, JobAction::InProgress, Some(&name), None).await;
        rig.job(1, JobAction::Completed, Some(&name), Some("success"))
            .await;
        // Its run still runs, and goes on running.
        assert!(rig.look().await);
        assert!(rig.cell.lock().unwrap().cancelled.is_empty());
        exec(
            &rig.db,
            "UPDATE ci_fleet_runners SET ended_at = '2000-01-01T00:00:00Z'",
        )
        .await;
        assert!(rig.look().await);
        assert_eq!(
            rig.cell.lock().unwrap().cancelled,
            ["run-1"],
            "asked to stop, having had its time"
        );
        assert!(rig.runners().await.is_empty(), "and swept");
    }

    /// A job waits: a runner is registered for its labels, its
    /// configuration is kept as a secret and nowhere a reader of the run
    /// sees it, and a run is submitted to host it. Its job over, what it
    /// left is taken back.
    #[tokio::test]
    async fn a_runner_is_started_for_a_job_that_waits_and_cleared_up_after() {
        let Some(rig) = fleet_rig().await else {
            return;
        };
        assert!(
            !rig.lookout.look_one("test").await.unwrap(),
            "nothing to look over"
        );
        rig.job(1, JobAction::Queued, None, None).await;
        assert!(rig.look().await);

        let name = {
            let gh = rig.gh.lock().unwrap();
            let [(id, name, labels, _)] = &gh.runners[..] else {
                panic!("{:?}", gh.runners);
            };
            assert_eq!(*id, 20);
            assert_eq!(labels, &["dagron", "self-hosted", "linux", "x64"]);
            assert!(name.starts_with("dagron-") && name.len() == 19, "{name}");
            name.clone()
        };
        let secret = secret_name(&name);
        {
            let cell = rig.cell.lock().unwrap();
            assert_eq!(
                cell.environments,
                [("env-1".to_string(), "dagron-ci-fleet".to_string())]
            );
            assert_eq!(
                cell.secrets,
                [(
                    "env-1".to_string(),
                    secret.clone(),
                    format!("jit-of-{name}")
                )]
            );
            let [(run, key, spec)] = &cell.runs[..] else {
                panic!("{:?}", cell.runs);
            };
            assert_eq!((run.as_str(), key), ("run-1", &format!("fleet-{name}")));
            let spec: Value = serde_yaml::from_str(spec).unwrap();
            assert_eq!(spec["environment"], "dagron-ci-fleet");
            let task = &spec["tasks"][0];
            assert_eq!(
                task["docker_image"],
                "ghcr.io/actions/actions-runner:2.321.0"
            );
            assert_eq!(task["command"], json!([RUN_SCRIPT]));
            assert_eq!(task["pool"], "ci");
            assert_eq!(task["max_attempts"], 1);
            assert_eq!(
                task["env"],
                json!([{ "name": JIT_VARIABLE, "value_from": { "secret": secret } }])
            );
            assert!(
                !cell.runs[0].2.contains("jit-of-"),
                "the configuration is named in the run, and not written in it"
            );
        }
        assert_eq!(
            rig.runners().await,
            [(name.clone(), "launched".to_string(), None)]
        );

        // Looked over again with nothing moved, and old enough to be spare
        // if it were: the job still waits, so its runner is left.
        rig.age().await;
        assert!(rig.look().await);
        assert_eq!(rig.gh.lock().unwrap().runners.len(), 1, "one runner a job");
        assert!(rig.gh.lock().unwrap().removed.is_empty());

        // GitHub gives it the job, and the job ends; its run with it.
        rig.job(1, JobAction::InProgress, Some(&name), None).await;
        assert_eq!(rig.runners().await[0].1, "busy");
        assert!(rig.look().await);
        assert!(
            rig.cell.lock().unwrap().cancelled.is_empty(),
            "a busy runner is left to its job"
        );
        rig.job(1, JobAction::Completed, Some(&name), Some("success"))
            .await;
        // GitHub says the job has ended while its runner is still
        // finishing. Its run is not asked to stop: the runner exits by
        // itself, and is looked at again until it has.
        assert!(rig.look().await);
        assert!(
            rig.cell.lock().unwrap().cancelled.is_empty(),
            "a runner whose job has just ended is given time to exit"
        );
        assert_eq!(rig.runners().await[0].1, "ended", "and is not swept yet");
        assert!(
            rig.gh.lock().unwrap().removed.is_empty(),
            "nor is its registration asked for: GitHub removes a runner whose job is done"
        );
        assert!(
            rig.cell.lock().unwrap().deleted.is_empty(),
            "its secret is kept while its run still runs"
        );
        rig.cell
            .lock()
            .unwrap()
            .status
            .insert("run-1".into(), "succeeded".into());
        assert!(rig.look().await);
        assert!(rig.runners().await.is_empty(), "swept");
        assert_eq!(
            rig.gh.lock().unwrap().removed,
            [20],
            "its registration, if GitHub still had one"
        );
        assert_eq!(rig.cell.lock().unwrap().deleted, [secret]);
        assert!(rig.cell.lock().unwrap().cancelled.is_empty());
        assert!(
            !rig.look().await,
            "nothing waits and nothing is out: nothing looks"
        );
    }

    /// A job cancelled before any runner took it leaves a runner nothing
    /// waits for. It is taken back, its registration first; one GitHub says
    /// has a job after all is left to it.
    #[tokio::test]
    async fn a_runner_nothing_waits_for_is_taken_back_unless_it_has_a_job() {
        let Some(rig) = fleet_rig().await else {
            return;
        };
        rig.job(1, JobAction::Queued, None, None).await;
        rig.job(2, JobAction::Queued, None, None).await;
        assert!(rig.look().await);
        assert_eq!(rig.gh.lock().unwrap().runners.len(), 2, "a runner for each");

        // One is cancelled as it waits. Its runner is not taken back at
        // once: a job queued a moment later would have had it.
        rig.job(1, JobAction::Completed, None, Some("cancelled"))
            .await;
        assert!(rig.look().await);
        assert!(rig.gh.lock().unwrap().removed.is_empty());
        rig.age().await;
        assert!(rig.look().await);
        // One of the two, by its registration and then its run.
        let removed = rig.gh.lock().unwrap().removed.clone();
        let (gone, run, kept) = match removed[..] {
            [20] => (20, "run-1", 21),
            [21] => (21, "run-2", 20),
            _ => panic!("{removed:?}"),
        };
        assert_eq!(rig.cell.lock().unwrap().cancelled, [run]);
        let mut runners: Vec<(String, Option<String>)> = rig
            .runners()
            .await
            .into_iter()
            .map(|(_, state, why)| (state, why))
            .collect();
        runners.sort();
        assert_eq!(
            runners,
            [
                (
                    "ended".to_string(),
                    Some("no job waited for it".to_string())
                ),
                ("launched".to_string(), None)
            ]
        );
        assert!(rig.look().await);
        assert_eq!(rig.runners().await.len(), 1, "and swept at the next look");

        // The other's job is cancelled too, as far as dagron-ci has heard;
        // GitHub says the runner is running one. It is left to it.
        rig.job(2, JobAction::Completed, None, Some("cancelled"))
            .await;
        rig.gh.lock().unwrap().busy_runner = Some(kept);
        assert!(rig.look().await);
        assert_eq!(rig.runners().await[0].1, "launched");
        // (The one taken back was asked for twice: as it was taken back,
        // and again as it was cleared up after, when GitHub had none.)
        assert_eq!(
            rig.gh.lock().unwrap().removed,
            [gone, gone, kept],
            "asked, and refused"
        );
        assert_eq!(
            rig.cell.lock().unwrap().cancelled,
            [run],
            "its run is not stopped"
        );
    }

    /// A runner that cannot be started leaves its job waiting, says why, and
    /// is tried again later and not at every look.
    #[tokio::test]
    async fn a_runner_that_cannot_be_started_says_why_and_its_job_waits() {
        let Some(rig) = fleet_rig().await else {
            return;
        };
        rig.gh.lock().unwrap().no_runners = true;
        rig.job(1, JobAction::Queued, None, None).await;
        assert!(rig.look().await);
        let runners = rig.runners().await;
        assert_eq!(runners.len(), 1);
        assert_eq!(runners[0].1, "ended");
        assert!(
            runners[0]
                .2
                .as_deref()
                .unwrap_or_default()
                .contains("`administration: write`"),
            "{runners:?}"
        );
        assert!(rig.cell.lock().unwrap().runs.is_empty());
        assert!(
            !rig.lookout.look_one("test").await.unwrap(),
            "not looked over again at once"
        );
        // Looked over again all the same, as a delivery would have it: the
        // runner that failed to start holds its place, and no other is tried.
        assert!(rig.look().await);
        assert_eq!(
            rig.runners().await.len(),
            1,
            "one try a minute, not one a look"
        );

        // Registered, and the cell then has no room for its run.
        let fresh = fleet_rig().await.unwrap();
        fresh.cell.lock().unwrap().refuse_submit = Some(503);
        fresh.job(1, JobAction::Queued, None, None).await;
        assert!(fresh.look().await);
        let runners = fresh.runners().await;
        assert!(
            runners[0]
                .2
                .as_deref()
                .unwrap_or_default()
                .contains("could not be submitted"),
            "{runners:?}"
        );
        // It holds its place for a minute, and nothing of it is touched.
        assert!(fresh.look().await);
        assert!(fresh.gh.lock().unwrap().removed.is_empty());
        assert_eq!(fresh.gh.lock().unwrap().runners.len(), 1);
        // The minute over, what was made of it is taken back, its
        // registration and its secret, and a runner is tried again.
        exec(
            &fresh.db,
            "UPDATE ci_fleet_runners SET ended_at = '2000-01-01T00:00:00Z'",
        )
        .await;
        assert!(fresh.look().await);
        assert_eq!(fresh.gh.lock().unwrap().removed, [20]);
        assert_eq!(fresh.cell.lock().unwrap().deleted.len(), 1);
        assert_eq!(fresh.gh.lock().unwrap().runners.len(), 2, "tried again");
    }

    /// A fork's job is told by its workflow run's head, which GitHub is
    /// asked once a run. Its label gives it no runner unless it says.
    #[tokio::test]
    async fn a_fork_s_job_is_given_no_runner_unless_its_label_says() {
        let Some(rig) = fleet_rig().await else {
            return;
        };
        rig.gh.lock().unwrap().fork_runs.insert(78);
        rig.job_of(78, 1, JobAction::Queued, None, None).await;
        rig.job_of(78, 2, JobAction::Queued, None, None).await;
        assert!(rig.look().await);
        assert!(
            rig.gh.lock().unwrap().runners.is_empty(),
            "none for a fork's"
        );
        let asked = |gh: &StubGithub| {
            gh.lock()
                .unwrap()
                .seen
                .iter()
                .filter(|(path, ..)| path.contains("/actions/runs/"))
                .count()
        };
        assert_eq!(asked(&rig.gh), 1, "one question for the run's two jobs");
        let view = rig.db.fleet_view(&rig.repo.id).await.unwrap();
        assert_eq!(view.queued["dagron"].untrusted, 2);
        // The repository's own job is served, beside them.
        rig.job(3, JobAction::Queued, None, None).await;
        assert!(rig.look().await);
        assert_eq!(rig.gh.lock().unwrap().runners.len(), 1);
        assert_eq!(asked(&rig.gh), 2);

        // Where GitHub cannot be asked, a job is taken for a fork's.
        let Some(rig) = fleet_rig().await else {
            return;
        };
        rig.gh.lock().unwrap().no_actions = true;
        rig.job(1, JobAction::Queued, None, None).await;
        assert!(rig.look().await);
        assert!(rig.gh.lock().unwrap().runners.is_empty());
        assert_eq!(
            rig.db.fleet_view(&rig.repo.id).await.unwrap().queued["dagron"].unknown,
            1
        );

        // A label that serves a fork's job as any other asks nothing.
        let fleet = format!("{FLEET}    untrusted: serve\n");
        let Some(rig) = fleet_rig_with(&fleet).await else {
            return;
        };
        rig.gh.lock().unwrap().fork_runs.insert(78);
        rig.job_of(78, 1, JobAction::Queued, None, None).await;
        assert!(rig.look().await);
        assert_eq!(rig.gh.lock().unwrap().runners.len(), 1);
        assert_eq!(asked(&rig.gh), 0);
    }

    /// With `untrusted: floor`, a fork's job's runner is a run recorded as
    /// untrusted, kept in an environment that admits such runs and given
    /// none of what they are refused; where the cell would not record it so,
    /// none is started.
    #[tokio::test]
    async fn a_fork_s_job_s_runner_is_a_run_that_is_not_trusted() {
        let fleet = format!("{FLEET}    untrusted: floor\n    docker: true\n");
        let Some(rig) = fleet_rig_with(&fleet).await else {
            return;
        };
        rig.gh.lock().unwrap().fork_runs.insert(78);
        rig.job_of(78, 1, JobAction::Queued, None, None).await;
        // The cell takes no record of what started a run.
        assert!(rig.look().await);
        assert!(rig.gh.lock().unwrap().runners.is_empty());
        assert!(rig.cell.lock().unwrap().runs.is_empty());

        let Some(rig) = fleet_rig_with(&fleet).await else {
            return;
        };
        rig.gh.lock().unwrap().fork_runs.insert(78);
        rig.cell.lock().unwrap().groups = vec!["ci:trigger".into()];
        rig.job_of(78, 1, JobAction::Queued, None, None).await;
        rig.job(2, JobAction::Queued, None, None).await;
        assert!(rig.look().await);
        let untrusted = {
            let cell = rig.cell.lock().unwrap();
            let untrusted = cell
                .environments
                .iter()
                .find(|(_, name)| name == "dagron-ci-fleet-untrusted")
                .map(|(id, _)| id.clone())
                .expect("made");
            assert_eq!(
                cell.policies[&untrusted],
                json!({ "allowed_trust": ["untrusted"], "allowed_pipelines": ["fleet-*"] }),
                "an environment with no policy is given one that admits the fleet's runs"
            );
            assert_eq!(cell.runs.len(), 2, "one for each");
            let mut floor = None;
            for (run, _, spec) in &cell.runs {
                let spec: Value = serde_yaml::from_str(spec).unwrap();
                let task = &spec["tasks"][0];
                match cell.triggers.get(run) {
                    Some(trigger) => {
                        assert_eq!(trigger["trust"], "untrusted");
                        assert_eq!(trigger["kind"], "ci");
                        assert_eq!(trigger["repo"], "acme/widgets");
                        assert_eq!(spec["environment"], "dagron-ci-fleet-untrusted");
                        assert!(task.get("pool").is_none() && task.get("docker_daemon").is_none());
                        floor = Some(run.clone());
                    }
                    None => {
                        assert_eq!(spec["environment"], "dagron-ci-fleet");
                        assert_eq!(
                            (&task["pool"], &task["docker_daemon"]),
                            (&json!("ci"), &json!(true))
                        );
                    }
                }
            }
            assert!(floor.is_some());
            let secrets: Vec<&str> = cell.secrets.iter().map(|(env, ..)| env.as_str()).collect();
            assert!(secrets.contains(&untrusted.as_str()) && secrets.len() == 2);
            untrusted
        };
        assert!(untrusted.starts_with("env-"));
        let mut trusts: Vec<String> = {
            let view = rig.db.fleet_view(&rig.repo.id).await.unwrap();
            view.runners.iter().map(|r| r.trust.clone()).collect()
        };
        trusts.sort();
        assert_eq!(trusts, [TRUSTED, UNTRUSTED]);

        // An environment an admin gave a policy that admits no such run is
        // left as it is, and no runner is started in it.
        let Some(rig) = fleet_rig_with(&fleet).await else {
            return;
        };
        {
            let mut cell = rig.cell.lock().unwrap();
            cell.groups = vec!["ci:trigger".into()];
            cell.environments
                .push(("env-9".into(), "dagron-ci-fleet-untrusted".into()));
            cell.policies
                .insert("env-9".into(), json!({ "allowed_trust": ["trusted"] }));
        }
        rig.gh.lock().unwrap().fork_runs.insert(78);
        rig.job_of(78, 1, JobAction::Queued, None, None).await;
        assert!(rig.look().await);
        assert!(rig.cell.lock().unwrap().runs.is_empty());
        assert_eq!(
            rig.cell.lock().unwrap().policies["env-9"],
            json!({ "allowed_trust": ["trusted"] })
        );
    }

    /// GitHub may give a fork's job to a runner started for another's: the
    /// runner's run is stopped as soon as that is heard of.
    #[tokio::test]
    async fn a_trusted_runner_given_a_fork_s_job_is_stopped() {
        let Some(rig) = fleet_rig().await else {
            return;
        };
        rig.gh.lock().unwrap().fork_runs.insert(78);
        rig.job(1, JobAction::Queued, None, None).await;
        assert!(rig.look().await);
        let name = rig.runners().await[0].0.clone();
        // The fork's job is queued, and taken by that runner before the
        // repository's own is.
        rig.job_of(78, 2, JobAction::Queued, None, None).await;
        rig.job_of(78, 2, JobAction::InProgress, Some(&name), None)
            .await;
        assert!(rig.look().await);
        assert_eq!(rig.cell.lock().unwrap().cancelled, ["run-1"]);
        let runners = rig.runners().await;
        let stopped = runners.iter().find(|r| r.0 == name).unwrap();
        assert_eq!(
            (stopped.1.as_str(), stopped.2.as_deref()),
            ("ended", Some(FORK_JOB))
        );
        // The repository's own job still waits, and is given another.
        assert_eq!(rig.gh.lock().unwrap().runners.len(), 2);
    }

    /// A job on a trusted runner that cannot be told apart (GitHub answers
    /// with an error) is taken for a fork's once it has run its while, and
    /// its runner stopped. A job of a repository the fleet keeps no job of,
    /// which an organization's runner may take, is left to it.
    #[tokio::test]
    async fn a_job_not_told_apart_in_time_on_a_trusted_runner_is_stopped() {
        let Some(rig) = fleet_rig().await else {
            return;
        };
        rig.job(1, JobAction::Queued, None, None).await;
        assert!(rig.look().await);
        let name = rig.runners().await[0].0.clone();
        rig.gh.lock().unwrap().broken_runs.insert(80);
        rig.job_of(80, 2, JobAction::Queued, None, None).await;
        rig.job_of(80, 2, JobAction::InProgress, Some(&name), None)
            .await;
        assert!(rig.look().await);
        assert!(
            rig.cell.lock().unwrap().cancelled.is_empty(),
            "a look may not have told it yet"
        );
        exec(
            &rig.db,
            "UPDATE ci_fleet_jobs SET started_at = '2000-01-01T00:00:00Z' WHERE job_id = '2'",
        )
        .await;
        assert!(rig.look().await);
        assert_eq!(rig.cell.lock().unwrap().cancelled, ["run-1"]);
        let runners = rig.runners().await;
        let stopped = runners.iter().find(|r| r.0 == name).unwrap();
        assert_eq!(
            (stopped.1.as_str(), stopped.2.as_deref()),
            ("ended", Some(FORK_JOB))
        );

        // Another repository's job, of which the fleet keeps no row.
        let Some(rig) = fleet_rig().await else {
            return;
        };
        rig.job(1, JobAction::Queued, None, None).await;
        assert!(rig.look().await);
        let name = rig.runners().await[0].0.clone();
        let other = Job {
            forge_host: "github.com".into(),
            full_name: "acme/elsewhere".into(),
            action: JobAction::InProgress,
            id: 9,
            run_id: 90,
            run_attempt: 1,
            name: "build".into(),
            workflow: Some("CI".into()),
            labels: vec!["self-hosted".into(), "dagron".into()],
            head_sha: SHA.into(),
            runner_id: None,
            runner_name: Some(name.clone()),
            conclusion: None,
        };
        rig.db.fleet_runner_heard(&other).await.unwrap();
        exec(
            &rig.db,
            "UPDATE ci_fleet_runners SET created_at = '2000-01-01T00:00:00Z'",
        )
        .await;
        assert!(rig.look().await);
        assert!(rig.cell.lock().unwrap().cancelled.is_empty(), "left to it");
        assert_eq!(rig.runners().await[0].1, "busy");
    }

    /// A label's quota holds its runners out to what it says: the jobs past
    /// it wait for one to end.
    #[tokio::test]
    async fn a_label_s_quota_holds_its_jobs_until_a_runner_ends() {
        let fleet = format!("{FLEET}    max_runners: 2\n");
        let Some(rig) = fleet_rig_with(&fleet).await else {
            return;
        };
        for id in 1..=3 {
            rig.job(id, JobAction::Queued, None, None).await;
        }
        assert!(rig.look().await);
        assert_eq!(rig.gh.lock().unwrap().runners.len(), 2, "two, and no more");
        assert!(rig.look().await);
        assert_eq!(rig.gh.lock().unwrap().runners.len(), 2);
        // One takes a job, which ends: its place goes to the third.
        let name = rig.runners().await[0].0.clone();
        rig.job(1, JobAction::InProgress, Some(&name), None).await;
        rig.job(1, JobAction::Completed, Some(&name), Some("success"))
            .await;
        assert!(rig.look().await);
        assert_eq!(rig.gh.lock().unwrap().runners.len(), 3);
    }

    /// An organization's runner is registered for the owner, in its group,
    /// and taken back there.
    #[tokio::test]
    async fn an_organization_s_runner_is_registered_and_taken_back_at_the_organization() {
        let fleet = format!("{FLEET}    scope: organization\n    runner_group: 7\n");
        let Some(rig) = fleet_rig_with(&fleet).await else {
            return;
        };
        rig.job(1, JobAction::Queued, None, None).await;
        assert!(rig.look().await);
        {
            let gh = rig.gh.lock().unwrap();
            assert!(gh.runners.is_empty());
            let [(id, _, labels, _, group)] = &gh.org_runners[..] else {
                panic!("{:?}", gh.org_runners);
            };
            assert_eq!((*id, *group), (500, 7));
            assert_eq!(labels[0], "dagron");
        }
        let view = rig.db.fleet_view(&rig.repo.id).await.unwrap();
        assert_eq!(view.runners[0].org.as_deref(), Some("acme"));
        // Cancelled as it waits: taken back at the organization.
        rig.job(1, JobAction::Completed, None, Some("cancelled"))
            .await;
        rig.age().await;
        assert!(rig.look().await);
        assert_eq!(rig.gh.lock().unwrap().org_removed, [500]);
        assert!(rig.gh.lock().unwrap().removed.is_empty());
    }

    /// A job whose end was never delivered is asked about, once it has
    /// waited a minute: GitHub says it was cancelled, and its runner is
    /// taken back.
    #[tokio::test]
    async fn a_job_whose_end_was_never_delivered_is_asked_about() {
        let Some(rig) = fleet_rig().await else {
            return;
        };
        rig.job(1, JobAction::Queued, None, None).await;
        assert!(rig.look().await);
        assert_eq!(rig.gh.lock().unwrap().runners.len(), 1);
        rig.gh.lock().unwrap().jobs.insert(
            1,
            json!({
                "id": 1, "run_id": 77, "status": "completed", "conclusion": "cancelled",
                "labels": ["dagron"], "head_sha": SHA,
            }),
        );
        // Not asked about yet: it has waited a moment.
        rig.age().await;
        assert!(rig.look().await);
        assert!(rig.gh.lock().unwrap().removed.is_empty());
        exec(
            &rig.db,
            "UPDATE ci_fleet_jobs SET queued_at = '2000-01-01T00:00:00Z'",
        )
        .await;
        assert!(rig.look().await);
        assert_eq!(rig.gh.lock().unwrap().removed, [20], "taken back");
        assert_eq!(
            rig.runners().await[0].2.as_deref(),
            Some(NO_JOB),
            "nothing waited for it"
        );
    }

    /// A job whose runner died is run again, once; one whose runner's run
    /// succeeded failed of itself, and is not.
    #[tokio::test]
    async fn a_job_whose_runner_died_is_run_again_once() {
        let Some(rig) = fleet_rig().await else {
            return;
        };
        // Its runner's run fails as the job runs, and then GitHub says the
        // job failed.
        let dies = |id: u64| {
            let rig = &rig;
            async move {
                rig.job(id, JobAction::Queued, None, None).await;
                assert!(rig.look().await);
                let name = rig.runners().await.last().unwrap().0.clone();
                rig.job(id, JobAction::InProgress, Some(&name), None).await;
                let run = rig.cell.lock().unwrap().runs.last().unwrap().0.clone();
                rig.cell.lock().unwrap().status.insert(run, "failed".into());
                assert!(rig.look().await);
                rig.job(id, JobAction::Completed, Some(&name), Some("failure"))
                    .await;
                assert!(rig.look().await);
            }
        };
        dies(1).await;
        assert_eq!(rig.gh.lock().unwrap().reruns, [1], "run again");
        let retry = |id: &str| {
            let sql = format!("SELECT retry, retry_note FROM ci_fleet_jobs WHERE job_id = '{id}'");
            let db = rig.db.clone();
            async move {
                match &db {
                    Db::Sqlite(p) => sqlx::query_as::<_, (Option<String>, Option<String>)>(&sql)
                        .fetch_one(p)
                        .await
                        .unwrap(),
                    Db::Postgres(p) => sqlx::query_as(&sql).fetch_one(p).await.unwrap(),
                }
            }
        };
        assert_eq!(retry("1").await.0.as_deref(), Some("asked"));
        // Its second attempt, a job of the same name of the same workflow
        // run, dies too: it is not run again.
        dies(2).await;
        assert_eq!(rig.gh.lock().unwrap().reruns, [1], "once");
        let (state, why) = retry("2").await;
        assert_eq!(state.as_deref(), Some("refused"));
        assert!(why.unwrap().contains("once already"));

        // A job that failed on a runner whose run succeeded failed of
        // itself.
        rig.job(3, JobAction::Queued, None, None).await;
        assert!(rig.look().await);
        let name = rig.runners().await.last().unwrap().0.clone();
        rig.job(3, JobAction::InProgress, Some(&name), None).await;
        rig.job(3, JobAction::Completed, Some(&name), Some("failure"))
            .await;
        let run = rig.cell.lock().unwrap().runs.last().unwrap().0.clone();
        rig.cell
            .lock()
            .unwrap()
            .status
            .insert(run, "succeeded".into());
        assert!(rig.look().await);
        assert!(!rig.look().await, "nothing is out, and nothing is due");
        assert_eq!(rig.gh.lock().unwrap().reruns, [1]);
        assert_eq!(retry("3").await.0, None);
    }

    /// GitHub runs a job again once its workflow run is over: until then the
    /// job is asked for each minute. A label may say not to, and a runner
    /// that had its label's whole time is not taken for one that died.
    #[tokio::test]
    async fn a_job_whose_runner_died_waits_for_github_or_is_let_go() {
        let Some(rig) = fleet_rig().await else {
            return;
        };
        rig.gh.lock().unwrap().rerun_answer = Some((403, "the workflow run is still running"));
        let died = |rig: &Rig, id: u64| {
            let sql = [format!(
                "UPDATE ci_fleet_runners SET state = 'ended', reason = 'its run failed', \
                     job_id = '{id}', ended_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), \
                     launched_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-60 seconds') \
                     WHERE name = 'r-{id}'"
            )];
            let db = rig.db.clone();
            let repo = rig.repo.clone();
            async move {
                db.fleet_want(
                    &repo,
                    "dagron",
                    &format!("r-{id}"),
                    None,
                    TRUSTED,
                    Quota::default(),
                )
                .await
                .unwrap();
                for sql in sql {
                    exec(&db, &sql).await;
                }
            }
        };
        died(&rig, 1).await;
        rig.job(1, JobAction::Completed, Some("r-1"), Some("failure"))
            .await;
        assert!(rig.look().await);
        assert_eq!(rig.gh.lock().unwrap().reruns, [1]);
        // Not asked again before its minute is up, and asked once it is.
        assert!(rig.look().await, "a job due to be run again is looked over");
        assert_eq!(rig.gh.lock().unwrap().reruns, [1]);
        rig.gh.lock().unwrap().rerun_answer = None;
        exec(
            &rig.db,
            "UPDATE ci_fleet_jobs SET retry_at = '2000-01-01T00:00:00Z'",
        )
        .await;
        assert!(rig.look().await);
        assert_eq!(rig.gh.lock().unwrap().reruns, [1, 1]);

        // A runner that ran to its label's time.
        died(&rig, 4).await;
        exec(
            &rig.db,
            "UPDATE ci_fleet_runners SET launched_at = '2000-01-01T00:00:00Z' WHERE name = 'r-4'",
        )
        .await;
        rig.job_of(80, 4, JobAction::Completed, Some("r-4"), Some("failure"))
            .await;
        assert!(rig.look().await);
        assert_eq!(rig.gh.lock().unwrap().reruns, [1, 1]);

        // A label that says not to.
        let fleet = format!("{FLEET}    retry: false\n");
        let Some(rig) = fleet_rig_with(&fleet).await else {
            return;
        };
        died(&rig, 5).await;
        rig.job(5, JobAction::Completed, Some("r-5"), Some("failure"))
            .await;
        assert!(rig.look().await);
        assert!(rig.gh.lock().unwrap().reruns.is_empty());
        assert!(
            !rig.look().await,
            "nothing is due: the job was let go, and its runner swept"
        );
    }

    /// A runner that dies (its pod evicted) fails its job, and GitHub may
    /// say so before a look sees the runner's run fail: the job is run
    /// again all the same.
    #[tokio::test]
    async fn a_job_whose_runner_died_is_run_again_where_github_said_so_first() {
        let Some(rig) = fleet_rig().await else {
            return;
        };
        rig.job(1, JobAction::Queued, None, None).await;
        assert!(rig.look().await);
        let name = rig.runners().await[0].0.clone();
        rig.job(1, JobAction::InProgress, Some(&name), None).await;
        rig.job(1, JobAction::Completed, Some(&name), Some("failure"))
            .await;
        let run = rig.cell.lock().unwrap().runs[0].0.clone();
        rig.cell.lock().unwrap().status.insert(run, "failed".into());
        assert!(rig.look().await);
        assert!(rig.runners().await.is_empty(), "swept");
        assert_eq!(rig.gh.lock().unwrap().reruns, [1], "run again");
        assert!(!rig.look().await, "nothing is out, and nothing is due");
    }

    /// An ask GitHub took whose answer was lost leaves its job due. Once a
    /// later attempt of it is heard of, it is not asked for again, and that
    /// attempt is not run again when its own runner dies.
    #[tokio::test]
    async fn a_job_run_again_whose_answer_was_lost_is_run_again_once() {
        let Some(rig) = fleet_rig().await else {
            return;
        };
        let retry = |id: &str| {
            let sql = format!("SELECT retry, retry_note FROM ci_fleet_jobs WHERE job_id = '{id}'");
            let db = rig.db.clone();
            async move {
                match &db {
                    Db::Sqlite(p) => sqlx::query_as::<_, (Option<String>, Option<String>)>(&sql)
                        .fetch_one(p)
                        .await
                        .unwrap(),
                    Db::Postgres(p) => sqlx::query_as(&sql).fetch_one(p).await.unwrap(),
                }
            }
        };
        // Taken, and answered with what reads as an error.
        rig.gh.lock().unwrap().rerun_answer = Some((502, "bad gateway"));
        rig.job(1, JobAction::Queued, None, None).await;
        assert!(rig.look().await);
        let name = rig.runners().await[0].0.clone();
        rig.job(1, JobAction::InProgress, Some(&name), None).await;
        rig.job(1, JobAction::Completed, Some(&name), Some("failure"))
            .await;
        let run = rig.cell.lock().unwrap().runs[0].0.clone();
        rig.cell.lock().unwrap().status.insert(run, "failed".into());
        assert!(rig.look().await);
        assert_eq!(rig.gh.lock().unwrap().reruns, [1]);
        assert_eq!(retry("1").await.0.as_deref(), Some("due"));
        // Its second attempt is queued.
        rig.gh.lock().unwrap().rerun_answer = None;
        rig.job(2, JobAction::Queued, None, None).await;
        exec(
            &rig.db,
            "UPDATE ci_fleet_jobs SET run_attempt = 2 WHERE job_id = '2'",
        )
        .await;
        exec(
            &rig.db,
            "UPDATE ci_fleet_jobs SET retry_at = '2000-01-01T00:00:00Z' WHERE job_id = '1'",
        )
        .await;
        assert!(rig.look().await);
        assert_eq!(rig.gh.lock().unwrap().reruns, [1], "not asked again");
        let (state, note) = retry("1").await;
        assert_eq!(state.as_deref(), Some("asked"));
        assert!(note.unwrap().contains("later attempt"));
        // Its runner dies too.
        let name = rig.runners().await.last().unwrap().0.clone();
        rig.job(2, JobAction::InProgress, Some(&name), None).await;
        let run = rig.cell.lock().unwrap().runs.last().unwrap().0.clone();
        rig.cell.lock().unwrap().status.insert(run, "failed".into());
        assert!(rig.look().await);
        rig.job(2, JobAction::Completed, Some(&name), Some("failure"))
            .await;
        assert!(rig.look().await);
        assert_eq!(rig.gh.lock().unwrap().reruns, [1], "once");
        let (state, why) = retry("2").await;
        assert_eq!(state.as_deref(), Some("refused"));
        assert!(why.unwrap().contains("once already"));
    }

    /// The reasons a runner that died is told by are the ones it is ended
    /// with.
    #[test]
    fn a_runner_that_died_is_told_by_the_reasons_it_ends_with() {
        assert!(super::super::store::DIED.contains(&run_ended("failed").as_str()));
        for well in ["succeeded", "cancelled"] {
            assert!(!super::super::store::DIED.contains(&run_ended(well).as_str()));
        }
    }

    /// A label's warm runners are kept beyond the jobs that wait, for a
    /// repository that had a job of it lately; one that waited too long is
    /// started anew, and none is kept once the repository's last job is
    /// older than the label's while.
    #[tokio::test]
    async fn a_label_keeps_runners_warm_after_a_job_and_starts_them_anew() {
        let fleet = format!("{FLEET}    warm: 2\n    untrusted: serve\n");
        let Some(rig) = fleet_rig_with(&fleet).await else {
            return;
        };
        assert!(!rig.look().await, "no job yet: nothing kept warm");
        rig.job(1, JobAction::Queued, None, None).await;
        assert!(rig.look().await);
        assert_eq!(
            rig.gh.lock().unwrap().runners.len(),
            3,
            "the job's and two warm"
        );
        // The job is taken and ends: two are left ready, and none started.
        let name = rig.runners().await[0].0.clone();
        rig.job(1, JobAction::InProgress, Some(&name), None).await;
        rig.job(1, JobAction::Completed, Some(&name), Some("success"))
            .await;
        exec(
            &rig.db,
            "UPDATE ci_fleet_runners SET created_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-60 seconds')",
        )
        .await;
        for _ in 0..2 {
            assert!(rig.look().await);
        }
        assert_eq!(rig.gh.lock().unwrap().runners.len(), 3);
        assert!(
            rig.gh.lock().unwrap().removed.is_empty(),
            "the warm ones are kept"
        );
        // Waited too long: one is taken back, and another started in its place.
        rig.age().await;
        assert!(rig.look().await);
        assert_eq!(rig.gh.lock().unwrap().removed.len(), 1, "one a look");
        assert!(rig.look().await);
        assert_eq!(rig.gh.lock().unwrap().runners.len(), 4, "started anew");
        // The repository's last job is older than the label's while.
        exec(
            &rig.db,
            "UPDATE ci_fleet_jobs SET queued_at = '2000-01-01T00:00:00Z'",
        )
        .await;
        rig.age().await;
        assert!(rig.look().await);
        let out: Vec<String> = rig
            .runners()
            .await
            .into_iter()
            .filter(|r| r.1 != "ended")
            .map(|r| r.0)
            .collect();
        assert!(out.is_empty(), "none kept: {out:?}");
    }

    /// A GitLab project, its GitLab, a cell, and a looker that serves the
    /// project's jobs by `fleet`, a GitLab label map.
    struct GitlabRig {
        db: Db,
        repo: Repo,
        lookout: Lookout,
        gl: crate::forge::gitlab::tests::StubGitlab,
        cell: StubCell,
        _dir: tempfile::TempDir,
    }

    const GITLAB_FLEET: &str = "labels:\n  linux:\n    image: gitlab/gitlab-runner:v17.5.0\n";
    const PENDING: &str = "/projects/group%2Fapp/jobs?scope%5B%5D=pending&per_page=100&page=1";

    async fn gitlab_rig(fleet: &str) -> GitlabRig {
        let (origin, gl) = crate::forge::gitlab::tests::stub_gitlab().await;
        let (base, cell) = stub_cell().await;
        let (db, dir) = sqlite().await;
        let repo = db
            .add_repo("gitlab", "gitlab.example", "group/app", None, "default")
            .await
            .unwrap()
            .unwrap();
        let cells = BTreeMap::from([("default".to_string(), base)]);
        let tokens = BTreeMap::from([("gitlab.example".to_string(), "glpat-x".to_string())]);
        let forge = Forge::new(None)
            .with_origin(&origin)
            .with_gitlab_tokens(&tokens);
        let reruns = Reruns::new(cells.clone(), "dgr_ci".into(), Arc::new(Metrics::default()));
        let lookout = Lookout::new(
            db.clone(),
            Fleet::default(),
            Fleet::parse_gitlab(fleet).unwrap(),
            Arc::new(forge),
            cells,
            "dgr_ci".into(),
            Arc::new(reruns),
            "dagron-ci-fleet".into(),
            "dagron-ci-fleet-untrusted".into(),
        );
        let rig = GitlabRig {
            db,
            repo,
            lookout,
            gl,
            cell,
            _dir: dir,
        };
        rig.answer(
            "/projects/group%2Fapp",
            json!({ "id": 42, "namespace": { "id": 7, "kind": "group" } }),
        );
        rig.pending(&[]);
        rig
    }

    impl GitlabRig {
        fn answer(&self, at: &str, body: Value) {
            self.gl
                .lock()
                .unwrap()
                .answers
                .insert(at.to_string(), (200, Vec::new(), body.to_string()));
        }

        /// The jobs GitLab lists as waiting: id, ref and tags.
        fn pending(&self, jobs: &[(u64, &str, &[&str])]) {
            let listed: Vec<Value> = jobs
                .iter()
                .map(|(id, git_ref, tags)| {
                    json!({
                        "id": id, "status": "pending", "name": "test", "stage": "test",
                        "ref": git_ref, "tag_list": tags,
                        "pipeline": { "id": 77, "sha": SHA }, "commit": { "id": SHA },
                    })
                })
                .collect();
            self.answer(PENDING, json!(listed));
        }

        /// A Job Hook's delivery, as the server writes it.
        async fn hook(
            &self,
            id: u64,
            action: JobAction,
            runner: Option<&str>,
            conclusion: Option<&str>,
        ) {
            let job = Job {
                forge_host: "gitlab.example".into(),
                full_name: "group/app".into(),
                action,
                id,
                run_id: 77,
                run_attempt: 1,
                name: "test".into(),
                workflow: Some("test".into()),
                labels: Vec::new(),
                head_sha: SHA.into(),
                runner_id: None,
                runner_name: runner.map(str::to_string),
                conclusion: conclusion.map(str::to_string),
            };
            let class = self
                .db
                .fleet_job_class("gitlab.example", &id.to_string())
                .await
                .unwrap();
            match class {
                Some(class) if action != JobAction::Queued => {
                    self.db.fleet_job(&self.repo, &job, &class).await.unwrap()
                }
                _ => self.db.fleet_due(&self.repo.id).await.unwrap(),
            }
        }

        async fn look(&self) -> bool {
            exec(
                &self.db,
                "UPDATE ci_repos SET fleet_due_at = '2000-01-01T00:00:00Z', fleet_claim = NULL \
                 WHERE fleet_due_at IS NOT NULL",
            )
            .await;
            self.lookout.look_one("test").await.unwrap()
        }

        async fn runners(&self) -> Vec<(String, String, Option<String>)> {
            let sql = "SELECT name, state, reason FROM ci_fleet_runners \
                       WHERE swept_at IS NULL ORDER BY created_at, name";
            match &self.db {
                Db::Postgres(p) => sqlx::query_as(sql).fetch_all(p).await.unwrap(),
                Db::Sqlite(p) => sqlx::query_as(sql).fetch_all(p).await.unwrap(),
            }
        }
    }

    /// A GitLab job that waits is heard of by its Job Hook, which names no
    /// tag; the look asks GitLab for the project's jobs that wait, and makes
    /// GitLab's own runner for the one the fleet serves, for the project,
    /// with its tags, its token kept as a secret. Its job over, the runner is
    /// removed.
    #[tokio::test]
    async fn a_gitlab_job_that_waits_is_given_gitlab_s_own_runner_and_cleared_up_after() {
        let rig = gitlab_rig(GITLAB_FLEET).await;
        rig.pending(&[(501, "main", &["linux"]), (502, "main", &["windows"])]);
        rig.hook(501, JobAction::Queued, None, None).await;
        assert!(rig.look().await);
        let name = {
            let gl = rig.gl.lock().unwrap();
            let [(id, made)] = &gl.runners[..] else {
                panic!("{:?}", gl.runners);
            };
            assert_eq!(*id, 900);
            assert_eq!(made["runner_type"], "project_type");
            assert_eq!(made["project_id"], 42);
            assert_eq!(made["tag_list"], json!(["linux"]));
            assert_eq!(
                (&made["run_untagged"], &made["locked"]),
                (&json!(false), &json!(true))
            );
            assert_eq!(made["maximum_timeout"], 6 * 3600);
            assert!(gl.seen.iter().any(|s| s.2 == "Bearer glpat-x"));
            made["description"].as_str().unwrap().to_string()
        };
        assert!(is_runner_name(&name));
        {
            let cell = rig.cell.lock().unwrap();
            assert_eq!(cell.secrets[0].2, "glrt-900");
            let spec: Value = serde_yaml::from_str(&cell.runs[0].2).unwrap();
            let task = &spec["tasks"][0];
            assert_eq!(task["docker_image"], "gitlab/gitlab-runner:v17.5.0");
            assert_eq!(task["command"], json!(GITLAB_RUN));
            assert_eq!(
                task["env"],
                json!([
                    { "name": "CI_SERVER_TOKEN", "value_from": { "secret": secret_name(&name) } },
                    { "name": "CI_SERVER_URL", "value": "https://gitlab.example" },
                    { "name": "RUNNER_EXECUTOR", "value": "shell" },
                    { "name": "RUNNER_NAME", "value": name },
                ])
            );
            assert!(
                !cell.runs[0].2.contains("glrt-"),
                "the token is named, not written"
            );
        }
        // The runner takes the job, which ends; GitLab lists none waiting.
        rig.hook(501, JobAction::InProgress, Some(&name), None)
            .await;
        assert_eq!(rig.runners().await[0].1, "busy");
        rig.pending(&[]);
        rig.hook(501, JobAction::Completed, Some(&name), Some("success"))
            .await;
        rig.cell
            .lock()
            .unwrap()
            .status
            .insert("run-1".into(), "succeeded".into());
        assert!(rig.look().await);
        assert!(rig.runners().await.is_empty(), "swept");
        assert_eq!(rig.gl.lock().unwrap().removed, [900]);
        assert_eq!(rig.cell.lock().unwrap().deleted, [secret_name(&name)]);
        assert!(!rig.look().await, "nothing waits, nothing is out");
    }

    /// A job of a merge request from a fork is told by the merge request,
    /// and its label gives it no runner unless it says; a branch's job is
    /// the project's own, and GitLab is not asked of it.
    #[tokio::test]
    async fn a_gitlab_merge_request_s_job_is_told_by_its_merge_request() {
        let rig = gitlab_rig(GITLAB_FLEET).await;
        rig.answer(
            "/projects/group%2Fapp/merge_requests/5",
            json!({ "iid": 5, "source_project_id": 99, "target_project_id": 42 }),
        );
        rig.pending(&[(501, "refs/merge-requests/5/head", &["linux"])]);
        rig.hook(501, JobAction::Queued, None, None).await;
        assert!(rig.look().await);
        assert!(
            rig.gl.lock().unwrap().runners.is_empty(),
            "none for a fork's"
        );
        let view = rig.db.fleet_view(&rig.repo.id).await.unwrap();
        assert_eq!(view.queued["linux"].untrusted, 1);

        let rig = gitlab_rig(GITLAB_FLEET).await;
        rig.answer(
            "/projects/group%2Fapp/merge_requests/6",
            json!({ "iid": 6, "source_project_id": 42, "target_project_id": 42 }),
        );
        rig.pending(&[(601, "refs/merge-requests/6/head", &["linux"])]);
        rig.hook(601, JobAction::Queued, None, None).await;
        assert!(rig.look().await);
        assert_eq!(rig.gl.lock().unwrap().runners.len(), 1, "the project's own");
    }

    /// A job GitLab lists with no ref is told by its pipeline's, and is
    /// taken for a fork's where GitLab gives none: never for the project's
    /// own.
    #[tokio::test]
    async fn a_gitlab_job_with_no_ref_is_told_by_its_pipeline_or_taken_for_a_fork_s() {
        let no_ref = json!([{
            "id": 501, "status": "pending", "name": "test", "stage": "test",
            "tag_list": ["linux"],
            "pipeline": { "id": 77, "sha": SHA }, "commit": { "id": SHA },
        }]);
        let rig = gitlab_rig(GITLAB_FLEET).await;
        rig.answer(PENDING, no_ref.clone());
        rig.hook(501, JobAction::Queued, None, None).await;
        assert!(rig.look().await);
        assert!(
            rig.gl.lock().unwrap().runners.is_empty(),
            "no ref of the job's or its pipeline's"
        );
        assert_eq!(
            rig.db.fleet_view(&rig.repo.id).await.unwrap().queued["linux"].untrusted,
            1
        );

        let rig = gitlab_rig(GITLAB_FLEET).await;
        rig.answer(PENDING, no_ref);
        rig.answer(
            "/projects/group%2Fapp/pipelines/77",
            json!({ "id": 77, "ref": "main" }),
        );
        rig.hook(501, JobAction::Queued, None, None).await;
        assert!(rig.look().await);
        assert_eq!(
            rig.gl.lock().unwrap().runners.len(),
            1,
            "its pipeline's ref"
        );
    }

    /// A fork's merge request's job that a runner of the fleet's took
    /// before any look listed it as waiting is told by its pipeline's ref,
    /// and the runner, started as a trusted job's, is stopped.
    #[tokio::test]
    async fn a_gitlab_fork_s_job_a_trusted_runner_took_is_told_and_its_runner_stopped() {
        let rig = gitlab_rig(GITLAB_FLEET).await;
        rig.pending(&[(501, "main", &["linux"])]);
        rig.hook(501, JobAction::Queued, None, None).await;
        assert!(rig.look().await);
        let name = rig.runners().await[0].0.clone();
        // GitLab gives the runner a job of another pipeline, a fork's merge
        // request's, which no look listed.
        rig.pending(&[]);
        rig.answer(
            "/projects/group%2Fapp/pipelines/78",
            json!({ "id": 78, "ref": "refs/merge-requests/5/head" }),
        );
        rig.answer(
            "/projects/group%2Fapp/merge_requests/5",
            json!({ "iid": 5, "source_project_id": 99, "target_project_id": 42 }),
        );
        let taken = Job {
            forge_host: "gitlab.example".into(),
            full_name: "group/app".into(),
            action: JobAction::InProgress,
            id: 601,
            run_id: 78,
            run_attempt: 1,
            name: "test".into(),
            workflow: Some("test".into()),
            labels: Vec::new(),
            head_sha: SHA.into(),
            runner_id: None,
            runner_name: Some(name.clone()),
            conclusion: None,
        };
        rig.db.fleet_job(&rig.repo, &taken, "linux").await.unwrap();
        assert!(rig.look().await);
        assert_eq!(rig.cell.lock().unwrap().cancelled, ["run-1"]);
        let runners = rig.runners().await;
        let stopped = runners.iter().find(|r| r.0 == name).unwrap();
        assert_eq!(
            (stopped.1.as_str(), stopped.2.as_deref()),
            ("ended", Some(FORK_JOB))
        );
    }

    /// A group's runner is made for the project's group; one that is
    /// running a job is not removed, and is left to it.
    #[tokio::test]
    async fn a_gitlab_group_s_runner_is_made_for_the_group_and_left_to_its_job() {
        let rig = gitlab_rig(&format!("{GITLAB_FLEET}    scope: organization\n")).await;
        rig.pending(&[(501, "main", &["linux"])]);
        rig.hook(501, JobAction::Queued, None, None).await;
        assert!(rig.look().await);
        {
            let gl = rig.gl.lock().unwrap();
            assert_eq!(gl.runners[0].1["runner_type"], "group_type");
            assert_eq!(gl.runners[0].1["group_id"], 7);
            assert!(gl.runners[0].1.get("locked").is_none());
        }
        // Cancelled as it waits, as far as the fleet has heard; GitLab says
        // the runner is running a job.
        rig.pending(&[]);
        rig.hook(501, JobAction::Completed, None, Some("cancelled"))
            .await;
        rig.gl.lock().unwrap().busy.insert(900);
        exec(
            &rig.db,
            "UPDATE ci_fleet_runners SET created_at = '2000-01-01T00:00:00Z'",
        )
        .await;
        assert!(rig.look().await);
        assert!(rig.gl.lock().unwrap().removed.is_empty(), "left to its job");
        assert_eq!(rig.runners().await[0].1, "launched");
        rig.gl.lock().unwrap().busy.clear();
        assert!(rig.look().await);
        assert_eq!(rig.gl.lock().unwrap().removed, [900]);
    }

    /// A GitLab job whose runner died is run again, at once.
    #[tokio::test]
    async fn a_gitlab_job_whose_runner_died_is_run_again() {
        let rig = gitlab_rig(GITLAB_FLEET).await;
        rig.gl.lock().unwrap().answers.insert(
            "/projects/group%2Fapp/jobs/501/retry".into(),
            (201, Vec::new(), "{}".into()),
        );
        rig.pending(&[(501, "main", &["linux"])]);
        rig.hook(501, JobAction::Queued, None, None).await;
        assert!(rig.look().await);
        let name = rig.runners().await[0].0.clone();
        rig.hook(501, JobAction::InProgress, Some(&name), None)
            .await;
        rig.cell
            .lock()
            .unwrap()
            .status
            .insert("run-1".into(), "failed".into());
        rig.pending(&[]);
        assert!(rig.look().await);
        rig.hook(501, JobAction::Completed, Some(&name), Some("failure"))
            .await;
        assert!(rig.look().await);
        let asked = rig
            .gl
            .lock()
            .unwrap()
            .seen
            .iter()
            .filter(|s| s.0 == "POST" && s.1.ends_with("/jobs/501/retry"))
            .count();
        assert_eq!(asked, 1);
    }

    /// What a runner's run asks for, by what it is submitted as.
    #[test]
    fn a_runner_s_run_asks_for_what_its_trust_may_have() {
        let fleet = Fleet::parse(
            "labels:\n  d:\n    image: runner:1\n    runner_class: large\n    pool: ci\n    \
             priority: 5\n    docker: true\n    untrusted: floor\n    \
             untrusted_runner_class: sandbox\n",
        )
        .unwrap();
        let class = fleet.class("d").unwrap();
        let task = |trust: &str| {
            let spec: Value =
                serde_yaml::from_str(&run_spec("d", class, "env", "dagron-aaa", trust, None))
                    .unwrap();
            spec["tasks"][0].clone()
        };
        let trusted = task(TRUSTED);
        assert_eq!(
            (
                &trusted["runner_class"],
                &trusted["pool"],
                &trusted["priority"],
                &trusted["docker_daemon"]
            ),
            (&json!("large"), &json!("ci"), &json!(5), &json!(true))
        );
        let untrusted = task(UNTRUSTED);
        assert_eq!(untrusted["runner_class"], "sandbox");
        for key in ["pool", "priority", "docker_daemon"] {
            assert!(untrusted.get(key).is_none(), "{key}");
        }
    }

    #[test]
    fn a_runner_s_name_is_told_from_another_runner_s() {
        assert!(is_runner_name("dagron-0a1b2c3d4e5f"));
        for theirs in [
            "dagron-0a1b2c3d4e5",
            "dagron-0a1b2c3d4e5g",
            "GitHub Actions 3",
            "dagron",
        ] {
            assert!(!is_runner_name(theirs), "{theirs}");
        }
    }

    #[test]
    fn a_runner_s_secret_is_named_as_an_environment_names_one() {
        assert_eq!(
            secret_name("dagron-0a1b2c3d4e5f"),
            "FLEET_DAGRON_0A1B2C3D4E5F"
        );
        assert_eq!(secret_name("a.b/c"), "FLEET_A_B_C");
    }
}
