//! A meter on the runs dagron-ci submits, for a build that counts them.
//!
//! The open build sets none. A build that counts runs passes a [`RunMeter`]
//! in its [`crate::Seams`], and dagron-ci tells it two things of each run:
//! - **submitted**, once dagron-api has answered with the run and the run is
//!   recorded: made now, or given back for a retry of the same submit;
//! - **ended**, once the reporter has recorded how the run ended, with what a
//!   count needs: that status, and whether any of the run's jobs started.
//!
//! **An end is told until the meter takes it.** Recording a run's end marks
//! it due (`ci_runs.meter_due_at`, in the same write), and the reporter tells
//! each due end, again a minute later while the meter refuses it, and lets
//! the mark go once it takes it. So the meter can be told an end more than
//! once, and must take a repeat as a no-op: the run id is its key. A submit is
//! told once, at best: a meter that missed it still hears the run's end. A
//! meter that refuses an end, or does not take it within 10 seconds, is let be
//! for a minute, so that one which is down holds no commit status back.
//!
//! **Started** means a scheduler claimed one of the run's jobs at least once
//! (its `attempt` is above 0), as any look at the run saw. A run cancelled
//! before any job started has none, nor has a run dagron-api no longer has
//! (status `gone`).

use std::future::Future;
use std::pin::Pin;

/// What a meter's answer is awaited as.
pub type MeterFuture<'a> = Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;

/// A run dagron-ci submitted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MeteredRun {
    pub run_id: String,
    /// The cell it was submitted to.
    pub cell: String,
    /// Its repository: `forge host/owner/name`.
    pub repo: String,
    /// What it runs: a pipeline's path, or `workflow:ID`.
    pub pipeline: String,
}

/// A run's end, as the reporter recorded it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EndedRun {
    pub run: MeteredRun,
    /// The kind of event it ran for: `push`, `pull_request`, `dispatch`, …
    pub kind: String,
    /// How it ended: dagron-api's status (`succeeded`, `failed`,
    /// `cancelled`), or `gone` when dagron-api no longer had the run.
    pub status: String,
    /// Whether any of its jobs started: of this round, where it was rerun
    /// whole.
    pub started: bool,
    /// How many times it was rerun whole (`from: all`) before this end: 0
    /// for its first round and the reruns of its failed jobs. Each rerun of
    /// every job is the same run's again, and a meter that counts runs may
    /// count each round as one.
    pub whole_reruns: i64,
}

/// What dagron-ci tells a build that counts its runs.
pub trait RunMeter: Send + Sync + 'static {
    /// `run` was submitted and recorded. An `Err` is logged, and the run
    /// goes on.
    fn submitted<'a>(&'a self, run: &'a MeteredRun) -> MeterFuture<'a>;

    /// `run` ended. An `Err` leaves its end due, and it is told again.
    fn ended<'a>(&'a self, run: &'a EndedRun) -> MeterFuture<'a>;
}
