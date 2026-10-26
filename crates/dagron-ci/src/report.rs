//! Commit statuses: on each commit dagron-ci ran, how each of its jobs went.
//!
//! A run dagron-ci submits is recorded in `ci_runs`, due for a report at once.
//! A reporter claims a due run as a sweeper claims a repository: the claim
//! fences its write, and a reporter that stops leaves the run due again once
//! its lease lapses. It reads the run's jobs from its cell's dagron-api
//! (`GET /api/runs/{id}/graph`, which carries no output), works out each job's
//! status, posts those that changed since the last look, and makes the run
//! due again in a few seconds while it runs. Once every job has finished it
//! reads the run's own status (`GET /api/runs/{id}/wait`), settles the last
//! statuses, and leaves the run: they are final.
//!
//! **One status per job**, under `dagron-ci/<pipeline>/<job> (<event>)`. The
//! pipeline is its file's name in `.dagron/ci`, so every run of it on that
//! kind of event updates the same checks rather than adding new ones, and a
//! push and a pull request on one commit do not overwrite each other's. A
//! merge group reports under its pull requests' names: a merge queue waits
//! for the checks branch protection requires of them, on the group's commit.
//!
//! | The job | Its status |
//! |---|---|
//! | waiting (pending, ready) | `pending`: queued |
//! | awaiting approval | `pending`: awaiting approval |
//! | running | `pending`: running |
//! | succeeded | `success` |
//! | failed | `failure` |
//! | cancelled | `error`: cancelled (GitHub's statuses have no cancelled) |
//! | skipped | `pending` until the run ends; then `success` if the run succeeded, else `error` |
//! | not finished when the run ended | `error`: did not finish |
//!
//! A skipped job waits for the run because dagron skips a job whose upstream
//! failed as it skips one whose `when:` is false, and the first must not pass
//! a required check.
//!
//! **Where.** GitHub's Statuses API, for a repository on github.com, with
//! CI_GITHUB_TOKEN_FILE's token, which needs `statuses: write`. A run of a
//! repository whose commits cannot take statuses, or whose statuses GitHub
//! refused, records why and posts nothing more.
//!
//! **As check runs, where the App can** (CI_GITHUB_CHECKS=on, and the App
//! installed on the repository with `checks: write`). A job is then one
//! check run and no status, under the name its status would have as its
//! context, so what branch protection requires by name goes on being met:
//!
//! | The job | Its check run |
//! |---|---|
//! | waiting, or awaiting approval | `queued` |
//! | running | `in_progress` |
//! | succeeded | `completed`: `success` |
//! | failed | `completed`: `failure` |
//! | cancelled | `completed`: `cancelled` |
//! | skipped | `queued` until the run ends; then `skipped` if the run succeeded, else `cancelled` |
//! | not finished when the run ended | `completed`: `failure` |
//!
//! A check run is brought up to date while its job is on its way, and is
//! not opened again once it is over: a job that runs again, after a rerun,
//! is a new check run under the same name, which is what GitHub shows for
//! that name from then on. A check run carries its engine run's id, so
//! that a "Re-run" of it names the run (`server.rs`). A run is one or the
//! other from its first post to its last: turning check runs on or off
//! changes the runs that start after.
//!
//! **A run owns each of its contexts** from its first post under it
//! (`ci_contexts`): no event says a job skipped under one a run owns. Where
//! an event is saying one skipped as the run comes to it (the job left out
//! of that event, or its pipeline by `paths:`), under a lease the event
//! holds for its post, the run leaves that context to its next look, a few
//! seconds on: its own post is the later, and stands.
//!
//! **Every run is followed to its end** all the same, every 30 s when nothing
//! is posted for it: its end is recorded in `ci_runs.status`, which the
//! checkout-token endpoint reads, and a run left `submitted` would stay one a
//! checkout credential is minted for. A run its cell's dagron-api no longer
//! has is recorded `gone`, as nothing will finish it.
//!
//! **And again, when it runs again.** A run rerun, retried or cleared is seen
//! running (`rerun.rs`), and its record re-opens: due at once, and followed
//! as before. Its jobs read `pending` again and then how they end, under the
//! contexts they had, since a context never names an attempt. A job that did
//! not succeed is posted again after each re-opening, though it read the same
//! before: it is a new result, and a rerun that is over by the next look still
//! leaves it on the commit. Beside its looks, the reporter's task looks over
//! each cell's running runs every 30 s for one it has on record as over.
//!
//! **And in one comment on a pull request**, where that is asked for
//! (CI_GITHUB_PR_COMMENT=on): [`comment`]. A look at a pull request's run
//! keeps what its jobs came to, and the comment is written from that.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use serde::Deserialize;
use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::db::{Db, DueReport, ReportWrite};
use crate::forge::{CheckRun, CommitStatus, Forge, StatusError};
use crate::meter::RunMeter;
use crate::metrics::{Metrics, StatusOutcome};
use crate::rerun::Reruns;
use crate::said::{self, Said};

/// How often a running run is looked at.
pub(crate) const EVERY: Duration = Duration::from_secs(5);

/// How long a reporter holds the run it looks at. A look that takes longer
/// loses the run to another reporter, whose look is as good.
const LEASE: Duration = Duration::from_secs(60);

/// How long a reporter with nothing due waits before it asks again.
const IDLE: Duration = Duration::from_secs(2);

/// How often a reporter whose looks have not ended asks for a run that fell
/// due meanwhile, while it has a look free.
const REFILL: Duration = Duration::from_millis(500);

/// How long after a look that failed the run is looked at again.
const BACKOFF: Duration = Duration::from_secs(30);

/// How long one read from dagron-api may take. The wait for a run's status
/// asks for one second of it.
const READ_TIMEOUT: Duration = Duration::from_secs(10);

/// GitHub's longest description.
const DESCRIPTION_MAX: usize = 140;

/// Under this key in `reported`, which no context has (theirs begin
/// `dagron-ci/`): why the forge refused the run's statuses. None is posted
/// for the run again.
const REFUSED: &str = "!refused";

/// What a context's entry in `reported` begins with once the forge has
/// refused a status under it for itself ([`StatusError::ThisStatus`]), and
/// why after it. No state a status is posted as begins so. Nothing more is
/// posted under that context for the run; its other contexts go on.
const CONTEXT_REFUSED: &str = "!refused:";

/// What a context's entry in `reported` begins with where the job is a
/// check run: then the check run's id, a colon, and how it was last posted
/// ([`check_key`]). No state a status is posted as begins so.
const CHECK: &str = "check:";

/// The status of a run its cell's dagron-api no longer has: over.
pub(crate) const GONE: &str = "gone";

/// Posts each run's commit statuses. One per replica; several replicas share
/// the runs through their claims.
pub struct Reporter {
    db: Db,
    /// Each cell's dagron-api, without a trailing slash.
    cells: BTreeMap<String, String>,
    /// dagron-ci's dagron-api token.
    token: String,
    http: crate::config::Clients,
    forge: Arc<Forge>,
    /// CI_RUN_URL: the page a status links to.
    run_url: Option<String>,
    metrics: Arc<Metrics>,
    /// Told of each run's end, when a build counts runs.
    meter: Option<Arc<dyn RunMeter>>,
    /// Looks over each cell's running runs for one that is running again.
    reruns: Option<Arc<Reruns>>,
    /// When each cell's outbox last answered, and the client it is
    /// claimed with.
    feeds: Arc<feed::Feeds>,
    feed_http: crate::config::Clients,
    /// CI_FLAKE_ISSUES and CI_FLAKE_DAYS: how many flakes of a job, within
    /// how many days, file an issue of it. `None`: flakes are not kept.
    flakes: Option<flake::Counted>,
}

/// How many due runs a reporter looks at at once: the bound of what waits to
/// be written.
pub const LOOKS_AT_ONCE: usize = 8;

/// A reporter's looks in flight, each its run, what it found, and when it
/// finished.
pub(crate) type Looking = tokio::task::JoinSet<(String, ReportWrite, std::time::Instant)>;

/// How long a meter has to take one run's end before it is told again.
const METER_LEASE: Duration = Duration::from_secs(60);

/// How long a meter may take to answer for one run's end.
const METER_TIMEOUT: Duration = Duration::from_secs(10);

/// The most ends told in one pass, so a meter that has fallen behind does not
/// hold the commit statuses back for long.
const METER_BATCH: usize = 32;

/// How long raising one failed run's `failed` event may hold it.
const FAILED_LEASE: Duration = Duration::from_secs(60);

/// The most `failed` events raised in one pass.
const FAILED_BATCH: usize = 32;

/// The longest a failed job's name, fault class or disposition is carried
/// into a `failed` event's inputs.
const FAILED_FIELD_MAX: usize = 200;

/// What a run's read says of its failure, as far as a `failed` event
/// carries it (`GET /api/runs/{id}`).
#[derive(Deserialize)]
struct RunFailure {
    #[serde(default)]
    failure: Option<FailureSummary>,
}

#[derive(Deserialize)]
struct FailureSummary {
    #[serde(default)]
    task_name: Option<String>,
    #[serde(default)]
    fault_class: Option<String>,
    #[serde(default)]
    disposition: Option<String>,
}

/// What one pass of [`Reporter::meter_ends`] did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct MeterPass {
    /// How many ends the meter took.
    pub(crate) taken: usize,
    /// Whether the pass stopped at an end the meter refused, or did not take
    /// in time. That end is told again once its lease is out.
    pub(crate) refused: bool,
}

/// A job, as the run's graph lists it.
#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub(crate) struct Job {
    /// The engine's id for the task: what its log is asked for by.
    #[serde(default)]
    pub id: String,
    pub name: String,
    pub status: String,
    /// How many times a scheduler claimed it: above 0 once it started.
    #[serde(default)]
    pub attempt: i64,
}

#[derive(Deserialize)]
struct Graph {
    nodes: Vec<Job>,
}

#[derive(Deserialize)]
struct Wait {
    status: String,
    finished: bool,
    /// How many times the run was rerun whole; `None` from a dagron-api
    /// that does not say.
    #[serde(default)]
    whole_reruns: Option<i64>,
}

/// A run's log as dagron-api answers it, as far as its lines.
#[derive(Deserialize)]
struct Logs {
    #[serde(default)]
    lines: Vec<LogLine>,
}

#[derive(Deserialize)]
struct LogLine {
    #[serde(default)]
    text: String,
    /// False for a line that is there as another's context.
    #[serde(default = "matched")]
    matched: bool,
}

fn matched() -> bool {
    true
}

/// How many of a job's own lines are asked for, from the end of its log:
/// those it says of itself are its last, and are fewer.
const SAID_LINES: usize = 400;

/// How long one job's log is waited for, and how long a look spends on its
/// jobs' logs in all. The look holds its run meanwhile ([`LEASE`]), and a
/// check run is not kept open for what its job said.
const SAID_TIMEOUT: Duration = Duration::from_secs(5);
const SAID_BUDGET: Duration = Duration::from_secs(20);

/// What dagron-api says of a run, as far as its statuses need.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Seen {
    pub jobs: Vec<Job>,
    /// The run's status once it finished; `None` while it runs.
    pub finished: Option<String>,
    /// How many times the run was rerun whole, as its finish was read;
    /// `None` while it runs, or where dagron-api does not say.
    pub whole_reruns: Option<i64>,
}

impl Reporter {
    pub fn new(
        db: Db,
        cells: BTreeMap<String, String>,
        token: String,
        forge: Arc<Forge>,
        run_url: Option<String>,
        metrics: Arc<Metrics>,
    ) -> Reporter {
        Reporter {
            http: crate::config::Clients::new(|| {
                reqwest::Client::builder()
                    .timeout(READ_TIMEOUT)
                    .user_agent(concat!("dagron-ci/", env!("CARGO_PKG_VERSION")))
            }),
            db,
            cells,
            token,
            forge,
            run_url,
            metrics,
            meter: None,
            reruns: None,
            feeds: Arc::new(feed::Feeds::default()),
            feed_http: feed::client(),
            flakes: None,
        }
    }

    /// Look over each cell's running runs for one on record as over
    /// ([`Reruns::sweep`]).
    pub fn with_reruns(mut self, reruns: Arc<Reruns>) -> Reporter {
        self.reruns = Some(reruns);
        self
    }

    /// Keep the jobs that flake, and file an issue of one that flakes
    /// `file_after` times within `days` (CI_FLAKE_ISSUES, CI_FLAKE_DAYS).
    pub fn with_flakes(mut self, file_after: i64, days: i64) -> Reporter {
        self.flakes = Some(flake::Counted { file_after, days });
        self
    }

    /// Tell `meter` of each run's end.
    pub fn with_meter(mut self, meter: Arc<dyn RunMeter>) -> Reporter {
        self.meter = Some(meter);
        self
    }

    /// Tell the meter each run end that is due, up to [`METER_BATCH`] of
    /// them. The pass stops at the first end the meter refuses or does not
    /// take in time: a meter that refuses one refuses the next, and waiting on
    /// each would hold the commit statuses back. Nothing when no meter is set.
    pub(crate) async fn meter_ends(&self) -> sqlx::Result<MeterPass> {
        let mut pass = MeterPass::default();
        let Some(meter) = &self.meter else {
            return Ok(pass);
        };
        while pass.taken < METER_BATCH {
            let Some(ended) = self.db.claim_meter_end(METER_LEASE).await? else {
                break;
            };
            match tokio::time::timeout(METER_TIMEOUT, meter.ended(&ended)).await {
                Ok(Ok(())) => {
                    self.db.meter_told(&ended.run.run_id).await?;
                    pass.taken += 1;
                }
                Ok(Err(why)) => {
                    tracing::warn!(
                        run_id = %ended.run.run_id, error = %why,
                        "the meter refused a run's end; it is told again in a minute"
                    );
                    pass.refused = true;
                    break;
                }
                Err(_) => {
                    tracing::warn!(
                        run_id = %ended.run.run_id,
                        "the meter did not take a run's end in time; it is told again in a minute"
                    );
                    pass.refused = true;
                    break;
                }
            }
        }
        Ok(pass)
    }

    /// Queue the `failed` event of each failed run that is due, up to
    /// [`FAILED_BATCH`] of them, with what its cell says of the failure: the
    /// job that failed first, its fault class and its disposition. A failure
    /// that cannot be read leaves those out, and its event is queued all the
    /// same: what answers it can read the run. How many were queued.
    pub(crate) async fn raise_failures(&self) -> sqlx::Result<usize> {
        let mut raised = 0;
        // The cells that did not answer this pass: asked no more in it, so
        // that one that hangs costs the loop one read, not one a failure. What
        // it would have said is optional, and its events are raised without it.
        let mut unanswered = std::collections::BTreeSet::new();
        while raised < FAILED_BATCH {
            let Some(due) = self.db.claim_failure(FAILED_LEASE).await? else {
                break;
            };
            let mut inputs = BTreeMap::from([
                ("ci_failed_run".to_string(), due.run_id.clone()),
                (
                    crate::pipeline::compile::FAILED_PIPELINE.to_string(),
                    pipeline_name(&due.pipeline).to_string(),
                ),
            ]);
            if let Some(base) = self
                .cells
                .get(&due.cell)
                .filter(|_| !unanswered.contains(&due.cell))
            {
                let url = format!("{base}/api/runs/{}", due.run_id);
                match self.get::<RunFailure>(&url).await {
                    Ok(Some(RunFailure {
                        failure: Some(failure),
                    })) => {
                        let fields = [
                            ("ci_failed_job", failure.task_name),
                            ("ci_failed_fault_class", failure.fault_class),
                            ("ci_failed_disposition", failure.disposition),
                        ];
                        for (name, value) in fields {
                            if let Some(value) = value.filter(|v| {
                                v.len() <= FAILED_FIELD_MAX && !v.chars().any(char::is_control)
                            }) {
                                inputs.insert(name.to_string(), value);
                            }
                        }
                    }
                    Ok(_) => {}
                    Err(why) => {
                        unanswered.insert(due.cell.clone());
                        tracing::debug!(
                            run_id = %due.run_id, %why,
                            "a failed run's failure was not read: its event says the run alone"
                        );
                    }
                }
            }
            if let Some(event) = self.db.raise_failure(&due, &inputs).await? {
                tracing::info!(run_id = %due.run_id, %event, pipeline = %due.pipeline, "a failed run raised its `failed` event");
                raised += 1;
            }
        }
        Ok(raised)
    }

    /// Look at the most overdue run, if one is due, as `claim`. `false` when
    /// none was. The service looks at several at once ([`Self::report_round`]).
    #[cfg(test)]
    pub(crate) async fn report_one(&self, claim: &str) -> sqlx::Result<bool> {
        let Some((run_id, due)) = self.db.claim_report(claim, LEASE).await? else {
            return Ok(false);
        };
        let write = self.write_for(&run_id, due.as_ref()).await;
        if !self.db.finish_report(&run_id, claim, &write).await? {
            tracing::warn!(run_id = %run_id, "commit statuses: the claim lapsed mid-look; another reporter has the run");
        }
        Ok(true)
    }

    /// One round of the reporter's looks, as `claim`: due runs claimed
    /// until [`LOOKS_AT_ONCE`] are being looked at, then the first look to
    /// finish and every other finished by then written together
    /// ([`Db::finish_reports`]): one statement for all their runs. A look
    /// that finishes waits only for the write it is in, and a run that falls
    /// due is looked at as soon as a look is free, never after the slowest
    /// of a batch. What waits to be written is at most those looks; a
    /// database slow to take them holds this reporter back, and nothing
    /// else. How many runs were written.
    pub(crate) async fn report_round(
        self: &Arc<Self>,
        claim: &str,
        looks: &mut Looking,
    ) -> sqlx::Result<usize> {
        // A look that is slow holds no run back: while none has ended, the
        // free ones are filled again every [`REFILL`].
        let first = loop {
            while looks.len() < LOOKS_AT_ONCE {
                let Some((run_id, due)) = self.db.claim_report(claim, LEASE).await? else {
                    break;
                };
                let me = self.clone();
                looks.spawn(async move {
                    let write = me.write_for(&run_id, due.as_ref()).await;
                    (run_id, write, std::time::Instant::now())
                });
            }
            if looks.is_empty() {
                return Ok(0);
            }
            let refill = tokio::time::sleep(REFILL);
            tokio::select! {
                ended = looks.join_next() => match ended {
                    Some(ended) => break ended,
                    None => return Ok(0),
                },
                _ = refill, if looks.len() < LOOKS_AT_ONCE => {}
            }
        };
        let mut done = Vec::new();
        for looked in std::iter::once(first).chain(std::iter::from_fn(|| looks.try_join_next())) {
            match looked {
                Ok(looked) => done.push(looked),
                // Its run stays claimed until its lease lapses, and is
                // looked at again then.
                Err(e) => tracing::error!(error = %e, "commit statuses: a look did not finish"),
            }
        }
        self.metrics.report_writes_waiting(done.len());
        let batch: Vec<(&str, &str, &ReportWrite)> = done
            .iter()
            .map(|(run_id, write, _)| (run_id.as_str(), claim, write))
            .collect();
        let written = self.db.finish_reports(&batch).await;
        let oldest = done.iter().map(|(.., at)| *at).min();
        self.metrics.report_writes_waiting(0);
        let written = written?;
        self.metrics
            .report_batch(done.len(), oldest.map_or(Duration::ZERO, |at| at.elapsed()));
        for (run_id, ..) in &done {
            if !written.contains(run_id) {
                tracing::warn!(run_id = %run_id, "commit statuses: the claim lapsed mid-look; another reporter has the run");
            }
        }
        Ok(done.len())
    }

    /// What a look at the claimed run `run_id` leaves on its record.
    async fn write_for(&self, run_id: &str, due: Option<&DueReport>) -> ReportWrite {
        let write = match &due {
            Some(due) => self.look(due).await,
            None => ReportWrite {
                error: Some("its event or repository can no longer be read".into()),
                meter: self.meter.is_some(),
                ..ReportWrite::default()
            },
        };
        // A repository that takes no statuses, or a run whose statuses were
        // refused at an earlier look, is said quietly: it is how that
        // repository is set up, not something going wrong now.
        let expected = due.as_ref().is_some_and(|d| {
            !self.forge.posts_statuses(&d.repo) || d.reported.contains_key(REFUSED)
        });
        match &write.error {
            Some(error) if expected => {
                tracing::debug!(run_id = %run_id, error = %error, "commit statuses")
            }
            Some(error) => {
                tracing::warn!(run_id = %run_id, error = %error, again = write.next.is_some(), "commit statuses")
            }
            None => {}
        }
        write
    }

    /// One look at `due`: read it, post what changed, and say when to look
    /// again. The run is read whether or not anything can be posted for it:
    /// its end is recorded either way.
    async fn look(&self, due: &DueReport) -> ReportWrite {
        let mut write = ReportWrite {
            reported: due.reported.clone(),
            meter: self.meter.is_some(),
            ..ReportWrite::default()
        };
        let Some(base) = self.cells.get(&due.cell) else {
            write.error = Some(format!("its cell {:?} is not in CI_CELLS", due.cell));
            write.next = Some(BACKOFF);
            return write;
        };
        let seen = match self.read(base, &due.run_id).await {
            Ok(Some(seen)) => seen,
            Ok(None) => {
                write.error = Some(format!("{} has no run {}", base, due.run_id));
                write.status = Some(GONE.into());
                // Its pull request's comment says so, where it has one.
                write.comment = self.comment_of(due);
                return write;
            }
            Err(why) => {
                write.error = Some(why);
                write.next = Some(BACKOFF);
                return write;
            }
        };
        write.status = seen.finished.clone();
        write.flakes = self.flakes_of(base, due, &seen).await;
        // Told through the cell's outbox of each change, a running run is
        // looked at again once a minute; or every few seconds, where it is not.
        write.next = seen.finished.is_none().then(|| self.feeds.every(&due.cell));
        write.started = seen.jobs.iter().any(|job| job.attempt > 0);
        write.whole_reruns = seen.whole_reruns;
        // What each job said of itself, where this look read it for its
        // check run: its pull request's comment says of it too.
        let mut told: BTreeMap<String, comment::Told> = BTreeMap::new();
        // Whether what a job said was left unread at this look.
        let mut cut_short = false;
        // Nothing is posted where the forge takes no statuses, or refused them
        // at an earlier look; the run is still followed to its end, less often.
        let cannot = if due.bundle_id.is_some() {
            Some(
                "a run on a patch bundle: the forge is told nothing of it, its commit being one \
                 the change is not"
                    .to_string(),
            )
        } else if self.forge.posts_statuses(&due.repo) {
            write.reported.get(REFUSED).cloned()
        } else {
            Some(format!(
                "{}/{} gets no commit statuses: they go to repositories on github.com, \
                 with CI_GITHUB_TOKEN_FILE's token, and to GitLab projects on a host \
                 CI_GITLAB_TOKENS_DIR keeps a token for",
                due.repo.forge_host, due.repo.full_name
            ))
        };
        if let Some(why) = cannot {
            let reading = (std::time::Instant::now(), &mut false);
            self.note_jobs(due, base, &seen, &told, reading, &mut write)
                .await;
            write.error = Some(why);
            write.next = seen.finished.is_none().then_some(BACKOFF);
            return write;
        }
        let pipeline = pipeline_name(&due.pipeline);
        let target_url = self.run_url.as_ref().map(|t| {
            t.replace("{run_id}", &due.run_id)
                .replace("{cell}", &due.cell)
        });
        let running = seen.finished.is_none();
        // Whether a context was left for the next look, while a skipped
        // status was posted under it ([`Self::may_post`]).
        let mut waiting = false;
        // Check runs or statuses, and the one a run began with to its end:
        // a job that had both would be two lines on the commit.
        let as_checks = match posted_how(&write.reported) {
            Some(as_checks) => as_checks,
            None => match self.forge.checks(&due.repo).await {
                Ok(as_checks) => as_checks,
                Err(why) => {
                    write.error = Some(format!(
                        "whether {}/{} takes check runs is not known yet: {why}",
                        due.repo.forge_host, due.repo.full_name
                    ));
                    write.next = Some(BACKOFF);
                    return write;
                }
            },
        };
        if as_checks {
            let jobs: BTreeMap<String, &Job> = seen
                .jobs
                .iter()
                .map(|job| (check_name(pipeline, &due.kind, &job.name), job))
                .collect();
            // The jobs' logs are read while the run is held: one that is
            // not answered is the last asked for at this look.
            let reading = std::time::Instant::now();
            let mut unread = false;
            for (name, (status, conclusion, title)) in checks(pipeline, &due.kind, &seen) {
                let key = check_key(status, conclusion, &title, due.reopens);
                let was = write.reported.get(&name);
                if was.is_some_and(|was| was.starts_with(CONTEXT_REFUSED)) {
                    continue;
                }
                let had = was.and_then(|was| check_posted(was));
                if had.is_some_and(|(_, was)| was == key) {
                    continue;
                }
                match self.may_post(due, &name, was.is_some()).await {
                    Ok(true) => {}
                    Ok(false) => {
                        waiting = true;
                        continue;
                    }
                    Err(why) => {
                        write.error = Some(why);
                        write.next = Some(BACKOFF);
                        break;
                    }
                }
                // One that is over stays as it ended: what its job does
                // after, in a rerun, is a new check run of that name.
                let open = had
                    .filter(|(_, was)| !was.starts_with("completed/"))
                    .map(|(id, _)| id);
                // What a job said of itself goes with the writing that ends
                // its check run, which is the last there is of it. A job
                // that was never started said nothing.
                let said = match jobs.get(&name) {
                    Some(job) if status == "completed" && job.attempt > 0 => {
                        let asked = !unread && reading.elapsed() <= SAID_BUDGET;
                        let read = match asked {
                            true => self.said(base, &due.run_id, job).await,
                            false => None,
                        };
                        if asked {
                            let said = read.as_ref().map(comment::Told::of);
                            told.insert(job.name.clone(), said.unwrap_or_default());
                        }
                        read.unwrap_or_else(|| {
                            unread = true;
                            cut_short = true;
                            Said::unread()
                        })
                    }
                    _ => Said::default(),
                };
                let mut summary = check_summary(pipeline, &name, due);
                if !said.summary.trim().is_empty() {
                    summary.push_str("\n\n---\n\n");
                    summary.push_str(said.summary.trim());
                }
                let check = CheckRun {
                    summary,
                    text: said.text(),
                    annotations: said.annotations,
                    name: name.clone(),
                    status,
                    conclusion,
                    title,
                    details_url: target_url.clone(),
                    external_id: due.run_id.clone(),
                };
                match self
                    .forge
                    .post_check(&due.repo, &due.sha, &check, open)
                    .await
                {
                    Ok(id) => {
                        self.metrics.commit_status(StatusOutcome::Posted);
                        write.reported.insert(name, format!("{CHECK}{id}:{key}"));
                    }
                    Err(e) => {
                        if !self.not_posted(&mut write, name, e, running) {
                            break;
                        }
                    }
                }
            }
            self.note_jobs(due, base, &seen, &told, (reading, &mut unread), &mut write)
                .await;
            if waiting {
                soon(&mut write);
            }
            if write.error.is_none() {
                write.error = context_refused(&write.reported);
            }
            return write;
        }
        for (context, (state, description)) in
            statuses(pipeline, status_kind(&due.repo, &due.kind), &seen)
        {
            let key = posted_as(state, &description, due.reopens);
            let was = write.reported.get(&context);
            if was == Some(&key) || was.is_some_and(|was| was.starts_with(CONTEXT_REFUSED)) {
                continue;
            }
            match self.may_post(due, &context, was.is_some()).await {
                Ok(true) => {}
                Ok(false) => {
                    waiting = true;
                    continue;
                }
                Err(why) => {
                    write.error = Some(why);
                    write.next = Some(BACKOFF);
                    break;
                }
            }
            let status = CommitStatus {
                state,
                context: context.clone(),
                description,
                target_url: target_url.clone(),
                pipeline_ref: crate::forge::gitlab::status_ref(
                    &due.git_ref,
                    due.head_ref.as_deref(),
                    &due.trust,
                ),
            };
            let posted = self.forge.post_status(&due.repo, &due.sha, &status).await;
            write.logged.push(logged(
                &due.event_id,
                Some(&due.run_id),
                &due.repo.id,
                &due.sha,
                &status,
                &posted,
            ));
            match posted {
                Ok(()) => {
                    self.metrics.commit_status(StatusOutcome::Posted);
                    write.reported.insert(context, key);
                }
                Err(e) => {
                    if !self.not_posted(&mut write, context, e, running) {
                        break;
                    }
                }
            }
        }
        let mut notes_unread = false;
        let reading = (std::time::Instant::now(), &mut notes_unread);
        self.note_jobs(due, base, &seen, &told, reading, &mut write)
            .await;
        // What some job said was not read at this look, for the time a look
        // has for it: the next is soon, however its cell's feed is.
        if cut_short || notes_unread {
            write.next = write.next.map(|next| next.min(EVERY));
        }
        if waiting {
            soon(&mut write);
        }
        if write.error.is_none() {
            write.error = context_refused(&write.reported);
        }
        write
    }

    /// Whether the run may post under `context` now. Before its first post
    /// there it owns it ([`Db::own_context`]), so that no skipped status is
    /// posted over what it says from then on; `false` while one is posted
    /// under it, and the run posts after, at its next look. One it posted
    /// under before (`posted`) is its own already.
    async fn may_post(&self, due: &DueReport, context: &str, posted: bool) -> Result<bool, String> {
        if posted {
            return Ok(true);
        }
        self.db
            .own_context(&due.repo.id, &due.sha, context, &due.run_id)
            .await
            .map_err(|e| format!("owning {context:?} before posting under it: {e}"))
    }

    /// What a status or a check run the forge did not take leaves on the
    /// run's record, and whether the run's others go on at this look.
    fn not_posted(
        &self,
        write: &mut ReportWrite,
        context: String,
        refusal: StatusError,
        running: bool,
    ) -> bool {
        match refusal {
            StatusError::RateLimited { wait, why } => {
                self.metrics.commit_status(StatusOutcome::RateLimited);
                write.error = Some(why);
                write.next = Some(wait.max(EVERY));
                false
            }
            StatusError::Failed(why) => {
                self.metrics.commit_status(StatusOutcome::Failed);
                write.error = Some(why);
                write.next = Some(BACKOFF);
                false
            }
            // Asking again would be refused again: nothing more is posted
            // for the run, which is still followed to its end.
            StatusError::Refused(why) => {
                self.metrics.commit_status(StatusOutcome::Refused);
                write.reported.insert(REFUSED.into(), why.clone());
                write.error = Some(why);
                write.next = running.then_some(BACKOFF);
                false
            }
            // Refused for itself: this context is left alone from here, and
            // the run's other statuses go on. It stopped them all.
            StatusError::ThisStatus(why) => {
                self.metrics.commit_status(StatusOutcome::Refused);
                write
                    .reported
                    .insert(context, format!("{CONTEXT_REFUSED}{why}"));
                true
            }
        }
    }

    /// The run's jobs, and its status once they have all finished. `None`
    /// when dagron-api has no such run.
    async fn read(&self, base: &str, run_id: &str) -> Result<Option<Seen>, String> {
        let graph: Graph = match self.get(&format!("{base}/api/runs/{run_id}/graph")).await? {
            Some(graph) => graph,
            None => return Ok(None),
        };
        // A run finishes only once every job has (cancelling it, or its
        // timeout, cancels the rest in the same write), so its status is read
        // only then. The wait answers at once for a finished run.
        let (finished, whole_reruns) = if graph.nodes.iter().all(|j| is_terminal(&j.status)) {
            let wait: Wait = match self
                .get(&format!("{base}/api/runs/{run_id}/wait?timeout_secs=1"))
                .await?
            {
                Some(wait) => wait,
                None => return Ok(None),
            };
            (wait.finished.then_some(wait.status), wait.whole_reruns)
        } else {
            (None, None)
        };
        Ok(Some(Seen {
            jobs: graph.nodes,
            finished,
            whole_reruns,
        }))
    }

    /// What `job` said of itself as it ended ([`crate::said`]), read from
    /// the end of its log. `None` for a log that was not read, with why in
    /// dagron-ci's own log: it holds up nothing, and the check run is ended
    /// without, and says that it is. A pull request's comment says nothing
    /// of that job's tests.
    async fn said(&self, base: &str, run_id: &str, job: &Job) -> Option<Said> {
        // A log is asked for by its task's id, which names one task of the
        // run. A graph that gives none is asked nothing.
        if job.id.is_empty() {
            return Some(Said::default());
        }
        let task = &job.id;
        let read = async {
            let mut url = reqwest::Url::parse(&format!("{base}/api/runs/{run_id}/logs"))
                .map_err(|e| format!("{base}: {e}"))?;
            url.query_pairs_mut()
                .append_pair("task", task)
                .append_pair("q", said::PREFIX)
                .append_pair("case", "1")
                .append_pair("tail", "1")
                .append_pair("limit", &SAID_LINES.to_string());
            self.get::<Logs>(url.as_str()).await
        };
        let read = match tokio::time::timeout(SAID_TIMEOUT, read).await {
            Ok(read) => read,
            Err(_) => Err(format!("no answer in {} s", SAID_TIMEOUT.as_secs())),
        };
        match read {
            Ok(Some(logs)) => Some(said::read(
                logs.lines
                    .iter()
                    .filter(|line| line.matched)
                    .map(|line| line.text.as_str()),
            )),
            // No log to ask of: a cell from before there was one.
            Ok(None) => Some(Said::default()),
            Err(why) => {
                tracing::warn!(
                    run = %run_id,
                    job = %job.name,
                    "what the job said of itself was not read, and is said without: {why}"
                );
                None
            }
        }
    }

    /// GET `url` from dagron-api as JSON. `None` on 404.
    async fn get<T: serde::de::DeserializeOwned>(&self, url: &str) -> Result<Option<T>, String> {
        let response = self
            .http
            .to(url)
            .get(url)
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| format!("reading {url}: {e}"))?;
        match response.status().as_u16() {
            404 => Ok(None),
            200 => response
                .json()
                .await
                .map(Some)
                .map_err(|e| format!("reading {url}: {e}")),
            code => Err(format!("reading {url}: HTTP {code}")),
        }
    }
}

/// Start the reporter. It stops when `stop` turns true, after the look in
/// hand.
///
/// The look over each cell's running runs goes beside the statuses, not among
/// them: a dagron-api that is slow to list its runs holds no status back.
/// So do the pull requests' comments: a forge that is slow to take one
/// holds no status back either.
pub fn spawn(reporter: Arc<Reporter>, stop: watch::Receiver<bool>) -> JoinHandle<()> {
    tokio::spawn(async move {
        tokio::join!(
            report(reporter.clone(), stop.clone()),
            comment(reporter.clone(), stop.clone()),
            flake_issues(reporter.clone(), stop.clone()),
            follow_reruns(reporter.clone(), stop.clone()),
            feed::follow_feeds(reporter, stop),
        );
    })
}

/// Post statuses, and tell the meter each end, until `stop`.
async fn report(reporter: Arc<Reporter>, mut stop: watch::Receiver<bool>) {
    let claim = format!("report-{}", uuid::Uuid::now_v7());
    // A meter that refused an end, or did not take it in time, is let be
    // for as long as that end's lease, and the commit statuses go on.
    let mut meter_resumes: Option<tokio::time::Instant> = None;
    // Dropped at stop, which ends the looks in flight: their runs' claims
    // lapse, and they are looked at again.
    let mut looks = Looking::new();
    while !*stop.borrow() {
        let looked = match reporter.report_round(&claim, &mut looks).await {
            Ok(looked) => looked > 0 || !looks.is_empty(),
            Err(e) => {
                tracing::warn!(error = %e, "commit statuses: the database failed");
                false
            }
        };
        let resting = meter_resumes.is_some_and(|at| tokio::time::Instant::now() < at);
        let told = if resting {
            false
        } else {
            match reporter.meter_ends().await {
                Ok(pass) => {
                    meter_resumes = pass
                        .refused
                        .then(|| tokio::time::Instant::now() + METER_LEASE);
                    pass.taken > 0
                }
                Err(e) => {
                    tracing::warn!(error = %e, "run ends: the database failed");
                    false
                }
            }
        };
        let raised = match reporter.raise_failures().await {
            Ok(raised) => raised > 0,
            Err(e) => {
                tracing::warn!(error = %e, "failed runs: the database failed");
                false
            }
        };
        let idle = !looked && !told && !raised;
        if idle {
            tokio::select! {
                _ = tokio::time::sleep(IDLE) => {}
                _ = stop.changed() => {}
            }
        }
    }
}

/// Write each pull request's comment that is due ([`comment`]), until
/// `stop`. Nothing where no comment is kept (CI_GITHUB_PR_COMMENT). A write
/// in hand at `stop` is one request to the forge; one that is cut off
/// leaves the comment to its lease, as a look leaves a run.
async fn comment(reporter: Arc<Reporter>, mut stop: watch::Receiver<bool>) {
    if !reporter.forge.pr_comments() {
        return;
    }
    let claim = format!("comment-{}", uuid::Uuid::now_v7());
    while !*stop.borrow() {
        let wrote = match reporter.comment_one(&claim).await {
            Ok(wrote) => wrote,
            Err(e) => {
                tracing::warn!(error = %e, "pull request comments: the database failed");
                false
            }
        };
        if !wrote {
            tokio::select! {
                _ = tokio::time::sleep(IDLE) => {}
                _ = stop.changed() => {}
            }
        }
    }
}

/// Write each issue of a job that flakes that is due ([`flake`]), until
/// `stop`, as [`comment`] writes the comments. Nothing where flakes are not
/// kept (CI_FLAKE_ISSUES).
async fn flake_issues(reporter: Arc<Reporter>, mut stop: watch::Receiver<bool>) {
    if reporter.flakes.is_none() {
        return;
    }
    let claim = format!("flake-{}", uuid::Uuid::now_v7());
    while !*stop.borrow() {
        let wrote = match reporter.flake_issue_one(&claim).await {
            Ok(wrote) => wrote,
            Err(e) => {
                tracing::warn!(error = %e, "flaky jobs' issues: the database failed");
                false
            }
        };
        if !wrote {
            tokio::select! {
                _ = tokio::time::sleep(IDLE) => {}
                _ = stop.changed() => {}
            }
        }
    }
}

/// Look over each cell's running runs for one that is running again
/// ([`Reruns::sweep`]), at once and then every [`crate::rerun::SWEEP_EVERY`],
/// until `stop`. A look in hand is dropped at `stop`: each record it re-opens
/// is one write, and the next start looks again.
async fn follow_reruns(reporter: Arc<Reporter>, mut stop: watch::Receiver<bool>) {
    let Some(reruns) = reporter.reruns.clone() else {
        return;
    };
    while !*stop.borrow() {
        tokio::select! {
            _ = reruns.sweep(&reporter.db) => {}
            _ = stop.changed() => return,
        }
        tokio::select! {
            _ = tokio::time::sleep(crate::rerun::SWEEP_EVERY) => {}
            _ = stop.changed() => return,
        }
    }
}

fn is_terminal(status: &str) -> bool {
    matches!(status, "succeeded" | "failed" | "skipped" | "cancelled")
}

/// What a check calls the pipeline: its file's name, without `.dagron/ci/` or
/// `.yaml`; a bound workflow's id for a repository without pipelines.
pub(crate) fn pipeline_name(recorded: &str) -> &str {
    if let Some(id) = recorded.strip_prefix("workflow:") {
        return id;
    }
    let file = recorded.rsplit('/').next().unwrap_or(recorded);
    let stem = file
        .strip_suffix(".yaml")
        .or_else(|| file.strip_suffix(".yml"))
        .unwrap_or(file);
    // `.gitlab-ci.yml` is `gitlab-ci`: a check's name begins with no dot.
    // A pipeline's of `.dagron/ci`, and a workflow file's, keep theirs: a
    // check that has been named so stays so.
    if recorded.starts_with(".dagron/") || recorded.starts_with(".github/") {
        return stem;
    }
    match stem.strip_prefix('.') {
        Some(rest) if !rest.is_empty() => rest,
        _ => stem,
    }
}

/// Each job's context, and its status: the state GitHub takes and the line
/// it shows. Contexts never name a run, so a later run updates them.
pub(crate) fn statuses(
    pipeline: &str,
    kind: &str,
    seen: &Seen,
) -> BTreeMap<String, (&'static str, String)> {
    let kind = if kind == "merge_group" {
        "pull_request"
    } else {
        kind
    };
    let run_succeeded = seen.finished.as_deref() == Some("succeeded");
    seen.jobs
        .iter()
        .map(|job| {
            let (state, description) = match (job.status.as_str(), &seen.finished) {
                ("succeeded", _) => ("success", "succeeded"),
                ("failed", _) => ("failure", "failed"),
                ("cancelled", _) => ("error", "cancelled"),
                ("skipped", None) => ("pending", "skipped; settled when the run ends"),
                ("skipped", Some(_)) if run_succeeded => ("success", "skipped"),
                ("skipped", Some(_)) => ("error", "skipped: an earlier job did not succeed"),
                (_, Some(_)) => ("error", "did not finish"),
                ("running", None) => ("pending", "running"),
                ("awaiting_approval", None) => ("pending", "awaiting approval"),
                (_, None) => ("pending", "queued"),
            };
            let context = format!("dagron-ci/{pipeline}/{} ({kind})", job.name);
            (context, (state, clamp(description)))
        })
        .collect()
}

/// The next look of a run that left a context while a skipped status went
/// up under it ([`Reporter::may_post`]): soon, though the run is over, but
/// not before a wait this look met asked for (a rate limit, a forge or a
/// database that failed), which the left context waits out with the rest.
fn soon(write: &mut ReportWrite) {
    if write.error.is_some() && write.next.is_some() {
        return;
    }
    write.next = Some(write.next.map_or(EVERY, |next| next.min(EVERY)));
}

/// A context that takes no more, and why: said on the run's record at every
/// look, as a refusal of them all is, unless the look has more to say.
fn context_refused(reported: &BTreeMap<String, String>) -> Option<String> {
    reported
        .values()
        .find_map(|was| was.strip_prefix(CONTEXT_REFUSED))
        .map(str::to_string)
}

/// How a run's jobs have been posted so far: as check runs (`true`), as
/// statuses, or not at all yet (`None`).
fn posted_how(reported: &BTreeMap<String, String>) -> Option<bool> {
    let mut posted = reported
        .iter()
        .filter(|(context, was)| !context.starts_with('!') && !was.starts_with(CONTEXT_REFUSED))
        .map(|(_, was)| was.starts_with(CHECK));
    posted.next()
}

/// The id of the check run a context's entry in `reported` names, and how
/// it was last posted.
fn check_posted(was: &str) -> Option<(u64, &str)> {
    let (id, key) = was.strip_prefix(CHECK)?.split_once(':')?;
    Some((id.parse().ok()?, key))
}

/// What `reported` keeps of a check run that was posted, after its id: as
/// [`posted_as`] keeps a status. One that ended without succeeding is kept
/// with how often its run's record had re-opened, so that the same end after
/// a rerun is posted, as a new check run.
fn check_key(status: &str, conclusion: Option<&str>, title: &str, reopens: i64) -> String {
    let conclusion = conclusion.unwrap_or("-");
    if reopens > 0 && matches!(conclusion, "failure" | "cancelled") {
        format!("{status}/{conclusion}:{title}#{reopens}")
    } else {
        format!("{status}/{conclusion}:{title}")
    }
}

/// Each job's check run: its name, which is the context its status would
/// have ([`statuses`]), then its status, how it ended, and the line it
/// shows.
pub(crate) fn checks(
    pipeline: &str,
    kind: &str,
    seen: &Seen,
) -> BTreeMap<String, (&'static str, Option<&'static str>, String)> {
    let run_succeeded = seen.finished.as_deref() == Some("succeeded");
    let over = |conclusion| ("completed", Some(conclusion));
    seen.jobs
        .iter()
        .map(|job| {
            let ((status, conclusion), title) = match (job.status.as_str(), &seen.finished) {
                ("succeeded", _) => (over("success"), "succeeded"),
                ("failed", _) => (over("failure"), "failed"),
                ("cancelled", _) => (over("cancelled"), "cancelled"),
                ("skipped", None) => (("queued", None), "skipped; settled when the run ends"),
                ("skipped", Some(_)) if run_succeeded => (over("skipped"), "skipped"),
                // Not GitHub's `skipped`, which meets a required check.
                ("skipped", Some(_)) => {
                    (over("cancelled"), "skipped: an earlier job did not succeed")
                }
                (_, Some(_)) => (over("failure"), "did not finish"),
                ("running", None) => (("in_progress", None), "running"),
                ("awaiting_approval", None) => (("queued", None), "awaiting approval"),
                (_, None) => (("queued", None), "queued"),
            };
            let name = check_name(pipeline, kind, &job.name);
            (name, (status, conclusion, clamp(title)))
        })
        .collect()
}

/// The report log's row for posting `status` on `sha`, of the event
/// `event_id` (and of the run `run_id`, where it was a run's job), and what
/// came of it.
pub(crate) fn logged(
    event_id: &str,
    run_id: Option<&str>,
    repo_id: &str,
    sha: &str,
    status: &CommitStatus,
    posted: &Result<(), StatusError>,
) -> crate::db::ReportLog {
    let (outcome, detail) = match posted {
        Ok(()) => ("posted", None),
        Err(StatusError::RateLimited { why, .. }) => ("rate_limited", Some(why.clone())),
        Err(StatusError::Refused(why) | StatusError::ThisStatus(why)) => {
            ("refused", Some(why.clone()))
        }
        Err(StatusError::Failed(why)) => ("failed", Some(why.clone())),
    };
    crate::db::ReportLog {
        event_id: event_id.to_string(),
        run_id: run_id.map(str::to_string),
        repo_id: repo_id.to_string(),
        sha: sha.to_string(),
        context: status.context.clone(),
        state: status.state.to_string(),
        description: Some(status.description.clone()).filter(|d| !d.is_empty()),
        target_url: status.target_url.clone(),
        outcome,
        detail,
    }
}

/// The word a status's name ends with, for an event of `kind` on `repo`'s
/// forge: a merge group's is its pull request's, and on GitLab a pull
/// request is a merge request, as GitLab calls it. Only the name: an event's
/// kind is still `pull_request` wherever it is stored or matched.
pub(crate) fn status_kind<'a>(repo: &crate::db::Repo, kind: &'a str) -> &'a str {
    match kind {
        "merge_group" | "pull_request" if crate::forge::gitlab::is_gitlab(repo) => "merge_request",
        "merge_group" => "pull_request",
        // A run that answers another's failure: not one that failed.
        "failed" => "after_failure",
        kind => kind,
    }
}

#[cfg(test)]
#[test]
fn a_status_s_kind_is_said_in_its_forge_s_words() {
    let github = crate::forge::tests::repo("github.com", "acme/widgets");
    let gitlab = crate::forge::gitlab::tests::project("gitlab.com");
    assert_eq!(status_kind(&github, "pull_request"), "pull_request");
    assert_eq!(status_kind(&github, "merge_group"), "pull_request");
    assert_eq!(status_kind(&gitlab, "pull_request"), "merge_request");
    assert_eq!(
        status_kind(&gitlab, "merge_group"),
        "merge_request",
        "a merge train's are its merge request's"
    );
    assert_eq!(status_kind(&github, "failed"), "after_failure");
    assert_eq!(
        check_name("fix", "failed", "agent"),
        "dagron-ci/fix/agent (after_failure)",
        "a check run is named as the status is"
    );
    for kind in ["push", "tag", "dispatch"] {
        assert_eq!(status_kind(&github, kind), kind);
        assert_eq!(status_kind(&gitlab, kind), kind);
    }
}

/// The name of `job`'s check run, for an event of `kind`: a merge group's
/// is its pull request's, and a failure's answer is said as its status is.
fn check_name(pipeline: &str, kind: &str, job: &str) -> String {
    let kind = match kind {
        "merge_group" => "pull_request",
        "failed" => "after_failure",
        kind => kind,
    };
    format!("dagron-ci/{pipeline}/{job} ({kind})")
}

/// What a check run's own page says: the run it is of, and where that runs.
/// Names go in as code, so that nothing in one is read as Markdown.
fn check_summary(pipeline: &str, name: &str, due: &DueReport) -> String {
    let code = |text: &str| text.replace(['`', '\n', '\r'], " ");
    let mut summary = format!(
        "`{}`, of the pipeline `{}`.\n\nEngine run `{}`, on the cell `{}`.",
        code(name),
        code(pipeline),
        code(&due.run_id),
        code(&due.cell),
    );
    if due.reopens > 0 {
        summary.push_str(&format!(
            "\n\nThe run has been run again {} time{} since it first ended.",
            due.reopens,
            if due.reopens == 1 { "" } else { "s" }
        ));
    }
    summary
}

/// What `reported` keeps of a status that was posted, by which the next look
/// knows whether there is anything new to post. A job that did not succeed is
/// kept with how often its run's record had re-opened: the same failure after
/// a rerun is another result, and is posted again. One that succeeded, or is
/// on its way, reads the same whichever attempt it is of.
fn posted_as(state: &str, description: &str, reopens: i64) -> String {
    if reopens > 0 && matches!(state, "failure" | "error") {
        format!("{state}:{description}#{reopens}")
    } else {
        format!("{state}:{description}")
    }
}

pub(crate) fn clamp(text: &str) -> String {
    if text.chars().count() <= DESCRIPTION_MAX {
        return text.to_string();
    }
    let mut out: String = text.chars().take(DESCRIPTION_MAX - 1).collect();
    out.push('…');
    out
}

pub(crate) mod comment;
pub(crate) mod feed;
pub(crate) mod flake;

#[cfg(test)]
pub(crate) mod tests;
