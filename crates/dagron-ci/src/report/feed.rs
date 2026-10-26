//! The reporter told.
//!
//! A run's record is looked at when something of it changed: each cell's
//! dagron-api hands dagron-ci the engine's outbox, as it has it for this
//! service alone (`POST /api/outbox/ci/claim`): a task of a run beginning or
//! ending, the run ending, the run running again. Each is the run's record
//! made due ([`Db::tell_reports`]), or, for a run running again, the record
//! re-opened, and then acknowledged. A run told of again and again between
//! two looks is looked at once.
//!
//! While a cell's feed answers, a running run with nothing new is looked at
//! once a minute ([`NET`]), the net for a change lost, where it was every
//! five seconds; a cell whose dagron-api has no outbox to hand (one older
//! than this), or does not answer, has its runs looked at every five
//! seconds again, until it does.

use std::collections::{BTreeSet, HashMap};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use serde::Deserialize;
use tokio::sync::watch;
use tokio::time::Instant;

use super::{Reporter, EVERY};
use crate::db::Db;

/// How often a running run with nothing new is looked at while its cell's
/// feed answers: the net for a change the feed did not tell.
pub(crate) const NET: Duration = Duration::from_secs(60);
/// How long one claim waits for a change.
const WAIT_SECS: u64 = 25;
/// How long a claim may take, its wait within it.
const CLAIM_TIMEOUT: Duration = Duration::from_secs(WAIT_SECS + 15);
/// How long a feed is taken to answer after it last did.
const FRESH: Duration = Duration::from_secs(3 * WAIT_SECS + 15);
/// After a claim that failed, how long before the next.
const AGAIN: Duration = Duration::from_secs(10);
/// After a dagron-api said it has no outbox to hand, how long before asking
/// again (an upgrade).
const ABSENT_AGAIN: Duration = Duration::from_secs(300);
/// Events one claim hands at most.
const LIMIT: i64 = 500;

/// When each cell's feed last answered.
#[derive(Default)]
pub(crate) struct Feeds {
    answered: Mutex<HashMap<String, Instant>>,
}

impl Feeds {
    fn answered(&self, cell: &str) {
        self.answered
            .lock()
            .unwrap()
            .insert(cell.to_string(), Instant::now());
    }

    fn silent(&self, cell: &str) {
        self.answered.lock().unwrap().remove(cell);
    }

    /// How soon a running run of `cell` with nothing new is looked at again.
    pub(crate) fn every(&self, cell: &str) -> Duration {
        let fresh = self
            .answered
            .lock()
            .unwrap()
            .get(cell)
            .is_some_and(|at| at.elapsed() < FRESH);
        if fresh {
            NET
        } else {
            EVERY
        }
    }
}

#[derive(Debug, Deserialize)]
struct Claimed {
    events: Vec<Notice>,
}

#[derive(Debug, Deserialize)]
pub(crate) struct Notice {
    pub id: String,
    pub run_id: String,
    #[serde(rename = "type")]
    pub kind: String,
}

/// What one claim of a cell's feed came to.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Fed {
    /// This many changes were told and acknowledged.
    Told(usize),
    /// The cell's dagron-api has no outbox to hand.
    Absent,
}

/// What `notices` ask of the records: the runs to look at, and the runs
/// running again, each once.
pub(crate) fn sort(notices: &[Notice]) -> (Vec<String>, Vec<String>) {
    let mut look = BTreeSet::new();
    let mut again = BTreeSet::new();
    for n in notices {
        match n.kind.as_str() {
            "run.rerun" | "run.auto_rerun" => {
                again.insert(n.run_id.clone());
            }
            _ => {
                look.insert(n.run_id.clone());
            }
        }
    }
    (look.into_iter().collect(), again.into_iter().collect())
}

impl Reporter {
    /// Claim `cell`'s feed once, and do what it tells.
    pub(crate) async fn feed_once(&self, cell: &str, base: &str) -> Result<Fed, String> {
        let url = format!("{base}/api/outbox/ci/claim");
        let response = self
            .feed_http
            .to(&url)
            .post(&url)
            .bearer_auth(&self.token)
            .json(&serde_json::json!({ "limit": LIMIT, "lease_secs": 60, "wait_secs": WAIT_SECS }))
            .send()
            .await
            .map_err(|e| format!("claiming {url}: {e}"))?;
        match response.status().as_u16() {
            200 => {}
            404 | 405 => return Ok(Fed::Absent),
            code => return Err(format!("claiming {url}: HTTP {code}")),
        }
        let claimed: Claimed = response
            .json()
            .await
            .map_err(|e| format!("claiming {url}: {e}"))?;
        if claimed.events.is_empty() {
            return Ok(Fed::Told(0));
        }
        let (look, again) = sort(&claimed.events);
        tell(&self.db, &look).await?;
        if let Some(reruns) = &self.reruns {
            for run_id in &again {
                if let Err(e) = reruns
                    .follow(&self.db, cell, run_id, crate::metrics::Reopened::Outbox)
                    .await
                {
                    // The sweep of the cell's running runs is the net.
                    tracing::warn!(cell, run_id, error = %e, "a run said to be running again could not be followed");
                }
            }
        }
        let url = format!("{base}/api/outbox/ci/ack");
        let ids: Vec<&str> = claimed.events.iter().map(|n| n.id.as_str()).collect();
        let answer = self
            .feed_http
            .to(&url)
            .post(&url)
            .bearer_auth(&self.token)
            .json(&serde_json::json!({ "ids": ids }))
            .send()
            .await
            .map_err(|e| format!("acknowledging {url}: {e}"))?;
        if !answer.status().is_success() {
            // Handed again once their lease is up: told twice, looked at once.
            return Err(format!("acknowledging {url}: HTTP {}", answer.status()));
        }
        self.metrics.report_notices(claimed.events.len());
        Ok(Fed::Told(claimed.events.len()))
    }

    /// Follow `cell`'s feed until `stop`.
    async fn follow_feed(&self, cell: &str, base: &str, mut stop: watch::Receiver<bool>) {
        while !*stop.borrow() {
            let rest = match self.feed_once(cell, base).await {
                Ok(Fed::Told(_)) => {
                    self.feeds.answered(cell);
                    None
                }
                Ok(Fed::Absent) => {
                    self.feeds.silent(cell);
                    Some(ABSENT_AGAIN)
                }
                Err(e) => {
                    self.feeds.silent(cell);
                    self.metrics.report_feed_failed();
                    tracing::warn!(cell, error = %e, "the cell's outbox could not be claimed: its runs are looked at every few seconds");
                    Some(AGAIN)
                }
            };
            if let Some(rest) = rest {
                tokio::select! {
                    _ = tokio::time::sleep(rest) => {}
                    _ = stop.changed() => {}
                }
            }
        }
    }
}

/// Make `runs`' records due.
async fn tell(db: &Db, runs: &[String]) -> Result<(), String> {
    db.tell_reports(runs)
        .await
        .map(drop)
        .map_err(|e| format!("the database could not be written: {e}"))
}

/// Follow each cell's feed until `stop`.
pub(crate) async fn follow_feeds(reporter: Arc<Reporter>, stop: watch::Receiver<bool>) {
    let cells: Vec<(String, String)> = reporter
        .cells
        .iter()
        .map(|(c, b)| (c.clone(), b.clone()))
        .collect();
    let mut feeds = tokio::task::JoinSet::new();
    for (cell, base) in cells {
        let (reporter, stop) = (reporter.clone(), stop.clone());
        feeds.spawn(async move { reporter.follow_feed(&cell, &base, stop).await });
    }
    while feeds.join_next().await.is_some() {}
}

/// The client a feed is claimed with: its claim waits, so it may take longer
/// than a look's reads.
pub(crate) fn client() -> crate::config::Clients {
    crate::config::Clients::new(|| {
        reqwest::Client::builder()
            .timeout(CLAIM_TIMEOUT)
            .user_agent(concat!("dagron-ci/", env!("CARGO_PKG_VERSION")))
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn notice(run: &str, kind: &str) -> Notice {
        Notice {
            id: format!("{run}-{kind}"),
            run_id: run.into(),
            kind: kind.into(),
        }
    }

    #[test]
    fn notices_ask_a_look_of_each_run_once_and_a_follow_of_each_run_running_again() {
        let (look, again) = sort(&[
            notice("r1", "task.started"),
            notice("r1", "task.finished"),
            notice("r2", "run.completed"),
            notice("r3", "run.rerun"),
            notice("r3", "task.started"),
            notice("r4", "run.auto_rerun"),
        ]);
        assert_eq!(look, ["r1", "r2", "r3"]);
        assert_eq!(again, ["r3", "r4"]);
    }

    #[test]
    fn a_running_run_is_looked_at_once_a_minute_while_its_cell_s_feed_answers() {
        let feeds = Feeds::default();
        assert_eq!(feeds.every("a"), EVERY, "no feed yet");
        feeds.answered("a");
        assert_eq!(feeds.every("a"), NET);
        assert_eq!(feeds.every("b"), EVERY, "another cell's is its own");
        feeds.silent("a");
        assert_eq!(feeds.every("a"), EVERY);
    }
}
