//! A run that runs again: how dagron-ci's record of a run follows it.
//!
//! The engine can move a run after it has ended: a rerun from its failed jobs,
//! a retry of one, a clear. dagron-ci's record used to move once, to how the
//! run first ended, and everything that reads it took that for good. A rerun's
//! checkout was refused its credential ("the run is over"), the commit kept
//! the old statuses, and the run held no slot under the build's limit.
//!
//! **A record re-opens on a sight of its run running.** dagron-ci asks the
//! run's cell itself (`GET /api/runs/{id}`), and only `running` re-opens it:
//! not a run dagron-api no longer has, not one it could not be asked about,
//! and never the word of whoever asked for a credential. A re-opened record
//! is `submitted` again, so it is once more a run a checkout credential is
//! minted for, a run that holds a slot, and one whose statuses the reporter
//! follows: `pending`, then how it ends, under the contexts it had.
//!
//! dagron-ci looks in three places:
//! - **its own rerun door** (`POST /ci/v1/runs/{id}/rerun`, in `api.rs`),
//!   which asks dagron-api for the rerun, from the failed jobs or of every
//!   job, and re-opens the record with it;
//! - **a checkout credential asked for a run on record as over**
//!   (`checkout.rs`): the engine is starting a job of it, so dagron-ci looks
//!   before it answers;
//! - **a sweep** ([`Reruns::sweep`]) of each cell's running runs, for a run
//!   moved through dagron-api whose jobs fetch nothing.
//!
//! **The count moves for a run of every job alone.** A run is one run however
//! often its failed jobs are run again: its end is told to the meter again,
//! which takes a repeat as a no-op. A rerun of every job is told with how
//! many there have been (`whole_reruns`, which the reporter reads as the run
//! ends), and a meter counts each such round as a run.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use serde::Deserialize;

use crate::db::Db;
use crate::metrics::{Metrics, Reopened};

/// How long one call to dagron-api may take here. A sight is on the path of a
/// checkout credential, which has its own, shorter bound around it.
const TIMEOUT: Duration = Duration::from_secs(5);

/// How many running runs one page of dagron-api's list holds: its own most.
const PAGE: usize = 500;

/// The most pages one sweep reads of one cell: ten thousand running runs. A
/// cell with more is swept for its newest, and says so.
const MAX_PAGES: usize = 20;

/// How often each cell's running runs are looked over ([`Reruns::sweep`]).
pub(crate) const SWEEP_EVERY: Duration = Duration::from_secs(30);

/// What a cell's dagron-api says of a run, when asked.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Sight {
    /// It is running.
    Running,
    /// It is there, and not running: how it reads.
    Over(String),
    /// dagron-api has no such run.
    Gone,
}

/// Which of a run's jobs a rerun runs again.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum RerunFrom {
    /// Its failed jobs, and what they cancelled; what succeeded is kept.
    /// Asked of dagron-api with no body, as it always was.
    Failed,
    /// Every job: the run whole, one that succeeded too (`{"from": "all"}`).
    All,
}

/// What dagron-api answered to a rerun.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Asked {
    /// It is running again; this many of its jobs were reset.
    Rerun(i64),
    /// Not a run that can be rerun now (409): dagron-api's own words.
    Refused(String),
    /// A rerun of every job, answered 400: a dagron-api from before it ran
    /// one. Its own words.
    Unsupported(String),
    /// Its concurrency key is full (429): it waits for the key, as a new run
    /// of it would. dagron-api's own words.
    Waiting(String),
    /// dagron-api has no such run.
    Gone,
    /// The caller's credential was not accepted for it (401 or 403).
    Denied(u16),
}

/// Asks each cell's dagron-api about a run, and for a rerun.
pub struct Reruns {
    cells: BTreeMap<String, String>,
    token: String,
    http: crate::config::Clients,
    metrics: Arc<Metrics>,
}

#[derive(Deserialize)]
struct RunStatus {
    status: String,
}

#[derive(Deserialize)]
struct RunId {
    id: String,
}

#[derive(Deserialize)]
struct RerunAnswer {
    #[serde(default)]
    rerun: i64,
}

/// What dagron-api said with an answer, for the caller: the first 300
/// characters of it.
async fn said(response: reqwest::Response) -> String {
    response
        .text()
        .await
        .unwrap_or_default()
        .trim()
        .chars()
        .take(300)
        .collect()
}

/// A run id as dagron-api makes them, and nothing that could change the path
/// it is put in.
pub(crate) fn is_run_id(id: &str) -> bool {
    !id.is_empty()
        && id.len() <= 128
        && id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

impl Reruns {
    /// With each cell's dagron-api (`CI_CELLS`) and dagron-ci's own token for
    /// them (`CI_API_TOKEN_FILE`).
    pub fn new(cells: BTreeMap<String, String>, token: String, metrics: Arc<Metrics>) -> Reruns {
        Reruns {
            http: crate::config::Clients::new(|| {
                reqwest::Client::builder()
                    .timeout(TIMEOUT)
                    .user_agent(concat!("dagron-ci/", env!("CARGO_PKG_VERSION")))
            }),
            cells,
            token,
            metrics,
        }
    }

    fn base(&self, cell: &str) -> Result<&str, String> {
        self.cells
            .get(cell)
            .map(String::as_str)
            .ok_or_else(|| format!("cell {cell:?} is not in CI_CELLS"))
    }

    /// What `cell`'s dagron-api says of `run_id` now. `Err` when it could not
    /// be asked, or answered something else: that is no sight of anything.
    pub(crate) async fn sight(&self, cell: &str, run_id: &str) -> Result<Sight, String> {
        if !is_run_id(run_id) {
            return Ok(Sight::Gone);
        }
        let url = format!("{}/api/runs/{run_id}", self.base(cell)?);
        let response = self
            .http
            .to(&url)
            .get(&url)
            .bearer_auth(&self.token)
            .send()
            .await
            .map_err(|e| format!("reading {url}: {e}"))?;
        match response.status().as_u16() {
            200 => {
                let run: RunStatus = response
                    .json()
                    .await
                    .map_err(|e| format!("reading {url}: not a run: {e}"))?;
                Ok(match run.status.as_str() {
                    "running" => Sight::Running,
                    _ => Sight::Over(run.status),
                })
            }
            404 => Ok(Sight::Gone),
            code => Err(format!("reading {url}: HTTP {code}")),
        }
    }

    /// Re-open `run_id`'s record if `cell`'s dagron-api says it is running
    /// now. `true` when it says so: the record is then followed, whether this
    /// call re-opened it or another had. `by` is what occasioned the look.
    pub(crate) async fn follow(
        &self,
        db: &Db,
        cell: &str,
        run_id: &str,
        by: Reopened,
    ) -> Result<bool, String> {
        let asked = Instant::now();
        if self.sight(cell, run_id).await? != Sight::Running {
            return Ok(false);
        }
        self.reopen(db, run_id, asked.elapsed(), None, by).await?;
        Ok(true)
    }

    /// Re-open `run_id`'s record on a sight `seen_ago` old, and say so.
    pub(crate) async fn reopen(
        &self,
        db: &Db,
        run_id: &str,
        seen_ago: Duration,
        slot: Option<&str>,
        by: Reopened,
    ) -> Result<bool, String> {
        let reopened = db
            .reopen_run(run_id, seen_ago, slot)
            .await
            .map_err(|e| format!("the database could not be written: {e}"))?;
        if reopened {
            self.metrics.run_reopened(by);
            tracing::info!(
                run_id,
                by = by.as_str(),
                "the run is running again: its record is re-opened"
            );
        }
        Ok(reopened)
    }

    /// dagron-ci's own token for its cells' dagron-api: what a rerun nobody
    /// with a dagron-api credential asked for is asked with (a check's
    /// "Re-run", which GitHub lets a person with write access ask).
    pub(crate) fn own_token(&self) -> &str {
        &self.token
    }

    /// Ask `cell`'s dagron-api to rerun `run_id`, from its failed jobs or
    /// whole (`from`), as `credential`: the caller's own, so that the rule on
    /// who may is dagron-api's, and its record of who did names the caller.
    pub(crate) async fn rerun(
        &self,
        cell: &str,
        run_id: &str,
        credential: &str,
        from: RerunFrom,
    ) -> Result<Asked, String> {
        if !is_run_id(run_id) {
            return Ok(Asked::Gone);
        }
        let url = format!("{}/api/runs/{run_id}/rerun", self.base(cell)?);
        let mut request = self.http.to(&url).post(&url).bearer_auth(credential);
        if from == RerunFrom::All {
            request = request.json(&serde_json::json!({ "from": "all" }));
        }
        let response = request
            .send()
            .await
            .map_err(|e| format!("dagron-api at {url}: {e}"))?;
        match response.status().as_u16() {
            200 => {
                let answer: RerunAnswer = response
                    .json()
                    .await
                    .map_err(|e| format!("dagron-api at {url} answered something else: {e}"))?;
                Ok(Asked::Rerun(answer.rerun))
            }
            code @ (409 | 429) => {
                let said = said(response).await;
                Ok(if code == 429 {
                    Asked::Waiting(said)
                } else {
                    Asked::Refused(said)
                })
            }
            400 if from == RerunFrom::All => Ok(Asked::Unsupported(said(response).await)),
            404 => Ok(Asked::Gone),
            code @ (401 | 403) => Ok(Asked::Denied(code)),
            code => Err(format!("dagron-api at {url}: HTTP {code}")),
        }
    }

    /// The runs `cell`'s dagron-api has running now, newest first: all of
    /// them, not the ones marked as CI's (`trigger=ci`). A run dagron-ci
    /// submitted while its token's user was not in `ci:trigger` reads
    /// `manual`, and would be left out; the caller keeps the ones on
    /// dagron-ci's own record, which is the narrowing that counts.
    async fn running(&self, cell: &str) -> Result<Vec<String>, String> {
        let base = self.base(cell)?;
        let mut ids = Vec::new();
        for page in 0..MAX_PAGES {
            let url = format!(
                "{base}/api/runs?status=running&limit={PAGE}&offset={}",
                page * PAGE
            );
            let response = self
                .http
                .to(&url)
                .get(&url)
                .bearer_auth(&self.token)
                .send()
                .await
                .map_err(|e| format!("reading {url}: {e}"))?;
            if !response.status().is_success() {
                return Err(format!(
                    "reading {url}: HTTP {}",
                    response.status().as_u16()
                ));
            }
            let runs: Vec<RunId> = response
                .json()
                .await
                .map_err(|e| format!("reading {url}: not a list of runs: {e}"))?;
            let last = runs.len() < PAGE;
            ids.extend(runs.into_iter().map(|run| run.id));
            if last {
                return Ok(ids);
            }
        }
        tracing::warn!(
            cell,
            runs = ids.len(),
            "more runs are running than one sweep reads: the oldest are not looked over for a rerun"
        );
        Ok(ids)
    }

    /// Look over each cell's running runs, and re-open the record of any
    /// that is on record as over: it was run again through dagron-api, by a
    /// rerun, a retry or a clear, and nothing told dagron-ci. Returns how
    /// many records it re-opened.
    ///
    /// A record re-opens only if it had ended before the cell was asked: the
    /// reporter may end a run while the list is read, and that list says
    /// nothing of after.
    pub(crate) async fn sweep(&self, db: &Db) -> usize {
        let mut reopened = 0;
        for cell in self.cells.keys() {
            let asked = Instant::now();
            let running = match self.running(cell).await {
                Ok(running) => running,
                Err(why) => {
                    tracing::warn!(cell, reason = %why, "reruns: a cell's running runs could not be read");
                    continue;
                }
            };
            let over = match db.ended_among(&running).await {
                Ok(over) => over,
                Err(e) => {
                    tracing::warn!(error = %e, "reruns: the database failed");
                    return reopened;
                }
            };
            for run_id in over {
                match self
                    .reopen(db, &run_id, asked.elapsed(), None, Reopened::Sweep)
                    .await
                {
                    Ok(true) => reopened += 1,
                    Ok(false) => {}
                    Err(why) => {
                        tracing::warn!(run_id, reason = %why, "reruns: a record was not re-opened")
                    }
                }
            }
        }
        reopened
    }
}

#[cfg(test)]
mod tests;
