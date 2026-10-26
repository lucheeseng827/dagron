//! dagron-ci — CI on dagron.
//!
//! An event arrives from a forge, a pipeline runs, and a status goes back. The
//! pipeline runs as an ordinary dagron run: dagron-ci is a façade over
//! dagron-api's submit path, and the engine never learns what a pull request,
//! a check run or a CI job is. The day CI needs its own engine semantics, the
//! design is wrong.
//!
//! What this crate does today:
//! - `POST /hooks/github` verifies a delivery's signature before reading it,
//!   dedupes it on its delivery id, decides its trust class, and queues one
//!   event in `ci_events` ([`github`], [`db`]);
//! - the poll sweep reads each repository's branches, tags and open pull
//!   requests every `CI_POLL_SECS`, and queues what changed since its last
//!   sweep, under the commit keys a webhook would use ([`poll`], [`git`]);
//! - `POST /ci/v1/runs` queues a dispatch for a caller the repository's
//!   dagron-api identifies, and `GET /ci/v1/runs/{id}` reads any event back
//!   (the run API, `api.rs`);
//! - event workers claim the queue under a lease and run each trusted event's
//!   workflow through dagron-api, once per commit ([`worker`], [`dispatch`]),
//!   within a limit on runs in flight when a build sets one ([`admission`]);
//! - a pull request from a fork is refused, or, where the operator says so,
//!   run as an untrusted run, behind an approval where one is asked
//!   ([`forks`]);
//! - a reporter posts each run's commit statuses, one per job, pending and
//!   then its outcome ([`report`]), and tells a build that counts runs how
//!   each one ended ([`meter`]);
//! - `POST /ci/v1/checkout-tokens` mints the credential an engine fetches a
//!   job's checkout with: a GitHub App's installation token for that attempt,
//!   that repository and that commit's run alone, revoked when told
//!   ([`checkout`]);
//! - `/healthz`, `/readyz` and `/metrics` ([`server`]);
//! - `dagron-ci repo add|set|list|rm` connects the repositories it serves,
//!   binds each to the workflow its events run, and sets how often it is
//!   polled; `dagron-ci events` shows what became of the events;
//! - pipelines, `.dagron/ci/*.yaml`: `dagron-ci validate` checks them,
//!   `dagron-ci compile` shows the workflow one compiles to for an event
//!   ([`pipeline`]), and `dagron-ci run` runs a checkout's on a local engine.
//!
//! Next: pipelines read from forges beyond GitHub, and their checkouts'
//! credentials. See the README.
//!
//! An alternate build composes this crate rather than patching it: its binary
//! is [`cli::run`] with its own [`Seams`].

use std::sync::Arc;

pub mod admission;
mod api;
pub mod audit;
pub mod bundles;
pub mod checkout;
pub mod cli;
pub mod config;
pub mod cron;
pub mod db;
pub mod dispatch;
pub mod event;
pub mod fleet;
pub mod forge;
pub mod forks;
pub mod git;
pub mod github;
pub mod github_door;
pub mod gitlab;
pub mod governance;
mod import;
pub mod meter;
pub mod metrics;
pub mod nested;
pub mod payloads;
pub mod pipeline;
pub mod poll;
mod problem;
pub mod records;
mod remote;
mod sse;
pub mod report;
pub mod rerun;
pub mod retention;
mod run_local;
mod said;
pub mod schedule;
pub mod server;
pub mod worker;

/// What the fuzz targets reach that nothing else outside the crate does
/// (`fuzz/`): two readers of bytes that come from outside, each of which is
/// otherwise the crate's own, and a pack's checksum.
#[doc(hidden)]
pub mod fuzzing {
    /// The lines of a job's log that it says of itself (`said.rs`).
    pub fn said(lines: &[&str]) {
        let _ = crate::said::read(lines.iter().copied());
    }

    /// A forge's list of a repository's open pull requests (`github.rs`).
    pub fn open_pulls(body: &[u8]) {
        let _ = crate::github::parse_open_pulls(body, "acme/widgets");
    }

    /// `body` as a pack, with the checksum git ends one with: so that what
    /// reads a pack is reached past its checksum (`git/pack.rs`).
    pub fn sealed_pack(body: &[u8], format: crate::git::pack::ObjectFormat) -> Vec<u8> {
        crate::git::pack::sealed(body, format)
    }
}

pub use admission::Admission;
pub use audit::Audit;
pub use config::Config;
pub use governance::Governance;
pub use meter::RunMeter;
pub use metrics::MetricsSource;
pub use server::{router, serve, AppState};

/// Extension points for an alternate build. The open build passes
/// `Seams::default()`, which adds nothing.
#[derive(Clone, Default)]
pub struct Seams {
    /// More series for `/metrics`, rendered after dagron-ci's own.
    pub metrics: Vec<Arc<dyn MetricsSource>>,
    /// A limit on runs in flight, asked before each run is submitted. `None`
    /// submits every run as soon as its event is handled.
    pub admission: Option<Arc<dyn Admission>>,
    /// Told of each run submitted and of each run's end. `None` tells no one.
    pub run_meter: Option<Arc<dyn RunMeter>>,
    /// The pipelines a build requires of the repositories it serves, its word
    /// on each run before it is submitted, and who an event's actor is.
    /// `None` runs what each repository writes, as it writes it.
    pub governance: Option<Arc<dyn Governance>>,
    /// Told of each act dagron-ci records, to keep or send on. `None` tells
    /// no one.
    pub audit: Option<Arc<dyn Audit>>,
}
