//! `/metrics`, in the Prometheus text format, written by hand as the engine's
//! is: a handful of series do not need a metrics crate.

use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use crate::db::{Backlog, Finish};
use crate::poll::Swept;

pub const CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

/// The states a fleet runner is counted in while it is out.
const RUNNER_STATES: [&str; 3] = ["wanted", "launched", "busy"];

/// Fleet mode's series, from its rows as they stand: the jobs that wait and
/// the runners out, of every repository together, by label and state. Each
/// label of the fleet's is exported, at 0 where nothing is of it, so that a
/// rate over one never starts from a missing series; a label the fleet no
/// longer has is exported while something is still of it.
pub fn fleet(labels: &[&str], gauges: &crate::fleet::store::Gauges) -> String {
    let mut out = String::new();
    let mut every: Vec<&str> = labels.to_vec();
    every.extend(gauges.waiting.keys().map(String::as_str));
    every.extend(gauges.runners.keys().map(|(label, _)| label.as_str()));
    every.sort_unstable();
    every.dedup();
    let _ = writeln!(
        out,
        "# HELP dagron_ci_fleet_jobs_waiting Fleet mode's jobs that wait for a runner, by label."
    );
    let _ = writeln!(out, "# TYPE dagron_ci_fleet_jobs_waiting gauge");
    for label in &every {
        let _ = writeln!(
            out,
            "dagron_ci_fleet_jobs_waiting{{label=\"{label}\"}} {}",
            gauges.waiting.get(*label).copied().unwrap_or(0)
        );
    }
    let _ = writeln!(
        out,
        "# HELP dagron_ci_fleet_runners Fleet mode's runners out, by label and state: wanted, launched (waiting for a job, warm ones among them) and busy."
    );
    let _ = writeln!(out, "# TYPE dagron_ci_fleet_runners gauge");
    for label in &every {
        for state in RUNNER_STATES {
            let _ = writeln!(
                out,
                "dagron_ci_fleet_runners{{label=\"{label}\",state=\"{state}\"}} {}",
                gauges
                    .runners
                    .get(&(label.to_string(), state.to_string()))
                    .copied()
                    .unwrap_or(0)
            );
        }
    }
    out
}

/// More series for `/metrics`, from an alternate build ([`crate::Seams`]).
/// `render` appends complete exposition text: `# HELP`, `# TYPE` and samples.
pub trait MetricsSource: Send + Sync {
    fn render(&self, out: &mut String);
}

/// How a webhook delivery ended. Every outcome is exported from the first
/// scrape, at zero, so a rate over it never starts from a missing series.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Outcome {
    /// Verified and queued as a new event.
    Accepted,
    /// Verified, and its delivery id was seen before: nothing written.
    Duplicate,
    /// Verified, and nothing in it can lead to a run.
    Ignored,
    /// The signature was missing, malformed or wrong: nothing written.
    Unauthorized,
    /// Verified but unreadable: a missing header or a payload that does not parse.
    BadRequest,
    /// Verified, for a repository this dagron-ci does not serve.
    UnknownRepo,
    /// No secret is configured for this receiver.
    NotConfigured,
    /// The database failed; the forge will redeliver.
    Error,
}

impl Outcome {
    const ALL: [Outcome; 8] = [
        Outcome::Accepted,
        Outcome::Duplicate,
        Outcome::Ignored,
        Outcome::Unauthorized,
        Outcome::BadRequest,
        Outcome::UnknownRepo,
        Outcome::NotConfigured,
        Outcome::Error,
    ];

    fn as_str(self) -> &'static str {
        match self {
            Outcome::Accepted => "accepted",
            Outcome::Duplicate => "duplicate",
            Outcome::Ignored => "ignored",
            Outcome::Unauthorized => "unauthorized",
            Outcome::BadRequest => "bad_request",
            Outcome::UnknownRepo => "unknown_repo",
            Outcome::NotConfigured => "not_configured",
            Outcome::Error => "error",
        }
    }
}

/// How a `POST /ci/v1/runs` ended: `dagron_ci_api_requests_total{outcome}`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ApiOutcome {
    /// A dispatch was queued.
    Accepted,
    /// A repeat of an earlier request's `Idempotency-Key`: its event returned.
    Replayed,
    /// No credential, or one dagron-api did not accept.
    Unauthorized,
    /// A viewer asked to run something.
    Forbidden,
    /// An unreadable or invalid request, or a ref the forge does not have.
    BadRequest,
    /// The repository is not connected.
    NotFound,
    /// An `Idempotency-Key` sent before with another request, or a run of a
    /// repository in fleet mode, whose forge runs its pipelines.
    Conflict,
    /// dagron-api or the forge could not be asked; worth retrying.
    Unavailable,
    /// No cell is configured to identify callers with.
    NotConfigured,
    /// The database failed.
    Error,
}

impl ApiOutcome {
    const ALL: [ApiOutcome; 10] = [
        ApiOutcome::Accepted,
        ApiOutcome::Replayed,
        ApiOutcome::Unauthorized,
        ApiOutcome::Forbidden,
        ApiOutcome::BadRequest,
        ApiOutcome::NotFound,
        ApiOutcome::Conflict,
        ApiOutcome::Unavailable,
        ApiOutcome::NotConfigured,
        ApiOutcome::Error,
    ];

    fn as_str(self) -> &'static str {
        match self {
            ApiOutcome::Accepted => "accepted",
            ApiOutcome::Replayed => "replayed",
            ApiOutcome::Unauthorized => "unauthorized",
            ApiOutcome::Forbidden => "forbidden",
            ApiOutcome::BadRequest => "bad_request",
            ApiOutcome::NotFound => "not_found",
            ApiOutcome::Conflict => "conflict",
            ApiOutcome::Unavailable => "unavailable",
            ApiOutcome::NotConfigured => "not_configured",
            ApiOutcome::Error => "error",
        }
    }
}

/// How workers left events: `dagron_ci_events_finished_total{outcome}`.
const FINISHES: [&str; 5] = ["done", "skipped", "failed", "retried", "waited"];

/// How poll sweeps ended: `dagron_ci_poll_sweeps_total{outcome}`.
const SWEEPS: [&str; 5] = ["baseline", "queued", "unchanged", "failed", "rate_limited"];

/// What became of each commit status this replica meant to post, a run's
/// from its reporter and a refused pull request's from the worker that
/// refused it: `dagron_ci_commit_statuses_total{outcome}`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum StatusOutcome {
    Posted = 0,
    /// The forge refused it, and would again: it is not retried.
    Refused = 1,
    /// Retried after a pause.
    Failed = 2,
    RateLimited = 3,
}

const STATUSES: [&str; 4] = ["posted", "refused", "failed", "rate_limited"];

/// What became of each request for a checkout's credential:
/// `dagron_ci_checkout_tokens_total{outcome}`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CheckoutOutcome {
    /// An installation token, minted for the attempt.
    Minted = 0,
    /// None was needed or could be had: the repository is fetched without one.
    Anonymous = 1,
    /// Refused: not the engine, not a run dagron-ci made, not its commit.
    Refused = 2,
    /// The forge could not mint it.
    Failed = 3,
    /// A used one, revoked.
    Revoked = 4,
}

const CHECKOUTS: [&str; 5] = ["minted", "anonymous", "refused", "failed", "revoked"];

/// What occasioned the look that re-opened a run's record:
/// `dagron_ci_runs_reopened_total{by}`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Reopened {
    /// dagron-ci's own rerun door asked dagron-api for the rerun.
    Door = 0,
    /// A checkout credential was asked for a run on record as over.
    Credential = 1,
    /// The sweep of a cell's running CI runs found it among them.
    Sweep = 2,
    /// The cell's outbox said it is running again.
    Outbox = 3,
}

impl Reopened {
    pub fn as_str(self) -> &'static str {
        REOPENED[self as usize]
    }
}

const REOPENED: [&str; 4] = ["rerun", "credential", "sweep", "outbox"];

#[derive(Default)]
pub struct Metrics {
    github: [AtomicU64; 8],
    gitlab: [AtomicU64; 8],
    finished: [AtomicU64; 5],
    workers: AtomicU64,
    sweeps: [AtomicU64; 5],
    polled_events: AtomicU64,
    api: [AtomicU64; 10],
    statuses: [AtomicU64; 4],
    checkouts: [AtomicU64; 5],
    reopened: [AtomicU64; 4],
    /// Changes the reporter was told of through its cells' outboxes, and
    /// claims of them that failed.
    report_notices: AtomicU64,
    report_feed_failures: AtomicU64,
    /// The age of the oldest record in the database, in milliseconds.
    oldest_record_ms: AtomicU64,
    segments_written: AtomicU64,
    records_dropped: AtomicU64,
    /// The reporter's looks waiting to be written, and the batches written:
    /// how many, how many looks in all, and how long the last one's oldest
    /// waited, in milliseconds.
    report_waiting: AtomicU64,
    report_batches: AtomicU64,
    report_writes: AtomicU64,
    report_oldest_wait_ms: AtomicU64,
}

impl Metrics {
    pub fn github_delivery(&self, outcome: Outcome) {
        self.github[outcome as usize].fetch_add(1, Ordering::Relaxed);
    }

    pub fn gitlab_delivery(&self, outcome: Outcome) {
        self.gitlab[outcome as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// A worker left an event as `finish` says.
    pub fn event_finished(&self, finish: &Finish) {
        let i = match finish {
            Finish::Done(_) => 0,
            Finish::Skipped(_) => 1,
            Finish::Failed(_) => 2,
            Finish::Retry { .. } => 3,
            Finish::Wait { .. } => 4,
        };
        self.finished[i].fetch_add(1, Ordering::Relaxed);
    }

    /// A `POST /ci/v1/runs` ended as `outcome` says.
    pub fn api_request(&self, outcome: ApiOutcome) {
        self.api[outcome as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// A poll sweep ended as `swept` says. A sweep that lost its claim wrote
    /// nothing, and is not counted.
    pub fn poll_swept(&self, swept: &Swept) {
        let i = match swept {
            Swept::Baseline(_) => 0,
            Swept::Queued(ids) => {
                self.polled_events
                    .fetch_add(ids.len() as u64, Ordering::Relaxed);
                1
            }
            Swept::Unchanged => 2,
            Swept::Failed(_) => 3,
            Swept::RateLimited(_) => 4,
            Swept::Lost => return,
        };
        self.sweeps[i].fetch_add(1, Ordering::Relaxed);
    }

    /// A commit status ended as `outcome` says.
    pub fn commit_status(&self, outcome: StatusOutcome) {
        self.statuses[outcome as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// A request for a checkout's credential ended as `outcome` says.
    pub fn checkout_token(&self, outcome: CheckoutOutcome) {
        self.checkouts[outcome as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// A run's record was re-opened: the run was seen running again.
    pub fn run_reopened(&self, by: Reopened) {
        self.reopened[by as usize].fetch_add(1, Ordering::Relaxed);
    }

    /// `n` changes the reporter was told of through a cell's outbox.
    pub fn report_notices(&self, n: usize) {
        self.report_notices.fetch_add(n as u64, Ordering::Relaxed);
    }

    /// A claim of a cell's outbox that failed.
    pub fn report_feed_failed(&self) {
        self.report_feed_failures.fetch_add(1, Ordering::Relaxed);
    }

    /// How old the oldest record in the database is.
    pub fn set_oldest_record_age(&self, secs: f64) {
        self.oldest_record_ms
            .store((secs.max(0.0) * 1000.0) as u64, Ordering::Relaxed);
    }

    /// Segments written to the object store and read back.
    pub fn segments_written(&self, n: u64) {
        self.segments_written.fetch_add(n, Ordering::Relaxed);
    }

    /// How many of the reporter's looks wait to be written now.
    pub fn report_writes_waiting(&self, n: usize) {
        self.report_waiting.store(n as u64, Ordering::Relaxed);
    }

    /// A batch of `looks` written, its oldest having waited `oldest`.
    pub fn report_batch(&self, looks: usize, oldest: Duration) {
        self.report_batches.fetch_add(1, Ordering::Relaxed);
        self.report_writes
            .fetch_add(looks as u64, Ordering::Relaxed);
        self.report_oldest_wait_ms
            .store(oldest.as_millis() as u64, Ordering::Relaxed);
    }

    /// Events dropped with their day.
    pub fn records_dropped(&self, events: u64) {
        self.records_dropped.fetch_add(events, Ordering::Relaxed);
    }

    /// How many event workers this replica runs.
    pub fn set_workers(&self, n: usize) {
        self.workers.store(n as u64, Ordering::Relaxed);
    }

    /// The exposition text. `backlog` is `None` when the database did not
    /// answer in time, and its series are left out rather than reported as 0.
    pub fn render(&self, backlog: Option<Backlog>, extra: &[Arc<dyn MetricsSource>]) -> String {
        let mut out = String::new();
        let _ = writeln!(out, "# HELP dagron_ci_build_info dagron-ci's version.");
        let _ = writeln!(out, "# TYPE dagron_ci_build_info gauge");
        let _ = writeln!(
            out,
            "dagron_ci_build_info{{version=\"{}\"}} 1",
            dagron_logging::VERSION
        );

        let _ = writeln!(
            out,
            "# HELP dagron_ci_webhook_deliveries_total Webhook deliveries received, by forge and outcome."
        );
        let _ = writeln!(out, "# TYPE dagron_ci_webhook_deliveries_total counter");
        for (forge, counts) in [("github", &self.github), ("gitlab", &self.gitlab)] {
            for outcome in Outcome::ALL {
                let _ = writeln!(
                    out,
                    "dagron_ci_webhook_deliveries_total{{forge=\"{forge}\",outcome=\"{}\"}} {}",
                    outcome.as_str(),
                    counts[outcome as usize].load(Ordering::Relaxed)
                );
            }
        }

        let _ = writeln!(
            out,
            "# HELP dagron_ci_api_requests_total Runs asked for through POST /ci/v1/runs, by outcome."
        );
        let _ = writeln!(out, "# TYPE dagron_ci_api_requests_total counter");
        for outcome in ApiOutcome::ALL {
            let _ = writeln!(
                out,
                "dagron_ci_api_requests_total{{outcome=\"{}\"}} {}",
                outcome.as_str(),
                self.api[outcome as usize].load(Ordering::Relaxed)
            );
        }

        let _ = writeln!(
            out,
            "# HELP dagron_ci_event_workers Event workers this replica runs; 0 when no cell is configured."
        );
        let _ = writeln!(out, "# TYPE dagron_ci_event_workers gauge");
        let _ = writeln!(
            out,
            "dagron_ci_event_workers {}",
            self.workers.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "# HELP dagron_ci_events_finished_total Events workers finished, by how: done, skipped, failed, retried later, or waited (its run's concurrency key full, or the forge's rate limit spent; no attempt spent)."
        );
        let _ = writeln!(out, "# TYPE dagron_ci_events_finished_total counter");
        for (i, outcome) in FINISHES.iter().enumerate() {
            let _ = writeln!(
                out,
                "dagron_ci_events_finished_total{{outcome=\"{outcome}\"}} {}",
                self.finished[i].load(Ordering::Relaxed)
            );
        }

        let _ = writeln!(
            out,
            "# HELP dagron_ci_poll_sweeps_total Poll sweeps this replica finished, by outcome: baseline (a first sweep, which queues nothing), queued, unchanged, failed, rate_limited."
        );
        let _ = writeln!(out, "# TYPE dagron_ci_poll_sweeps_total counter");
        for (i, outcome) in SWEEPS.iter().enumerate() {
            let _ = writeln!(
                out,
                "dagron_ci_poll_sweeps_total{{outcome=\"{outcome}\"}} {}",
                self.sweeps[i].load(Ordering::Relaxed)
            );
        }
        let _ = writeln!(
            out,
            "# HELP dagron_ci_poll_events_total Events this replica's poll sweeps queued."
        );
        let _ = writeln!(out, "# TYPE dagron_ci_poll_events_total counter");
        let _ = writeln!(
            out,
            "dagron_ci_poll_events_total {}",
            self.polled_events.load(Ordering::Relaxed)
        );

        let _ = writeln!(
            out,
            "# HELP dagron_ci_commit_statuses_total Commit statuses this replica meant to post, by outcome: posted, refused (not retried), failed (retried), rate_limited (retried once the forge allows)."
        );
        let _ = writeln!(out, "# TYPE dagron_ci_commit_statuses_total counter");
        for (i, outcome) in STATUSES.iter().enumerate() {
            let _ = writeln!(
                out,
                "dagron_ci_commit_statuses_total{{outcome=\"{outcome}\"}} {}",
                self.statuses[i].load(Ordering::Relaxed)
            );
        }

        let _ = writeln!(
            out,
            "# HELP dagron_ci_checkout_tokens_total Requests for a checkout's credential, by outcome: minted, anonymous (none needed or to be had), refused, failed (the forge could not mint one), revoked."
        );
        let _ = writeln!(out, "# TYPE dagron_ci_checkout_tokens_total counter");
        for (i, outcome) in CHECKOUTS.iter().enumerate() {
            let _ = writeln!(
                out,
                "dagron_ci_checkout_tokens_total{{outcome=\"{outcome}\"}} {}",
                self.checkouts[i].load(Ordering::Relaxed)
            );
        }

        let _ = writeln!(
            out,
            "# HELP dagron_ci_runs_reopened_total Records re-opened because their run was seen running again, by what occasioned the look: rerun (dagron-ci's own door), credential (a checkout credential asked for a run on record as over), sweep (found among a cell's running runs), outbox (its cell's outbox said so)."
        );
        let _ = writeln!(out, "# TYPE dagron_ci_runs_reopened_total counter");
        for (i, by) in REOPENED.iter().enumerate() {
            let _ = writeln!(
                out,
                "dagron_ci_runs_reopened_total{{by=\"{by}\"}} {}",
                self.reopened[i].load(Ordering::Relaxed)
            );
        }

        if let Some(backlog) = backlog {
            let _ = writeln!(
                out,
                "# HELP dagron_ci_events_pending Events queued and not yet handled."
            );
            let _ = writeln!(out, "# TYPE dagron_ci_events_pending gauge");
            let _ = writeln!(out, "dagron_ci_events_pending {}", backlog.pending);
            let _ = writeln!(
                out,
                "# HELP dagron_ci_events_oldest_pending_age_seconds Age of the oldest pending event; 0 when none is pending."
            );
            let _ = writeln!(
                out,
                "# TYPE dagron_ci_events_oldest_pending_age_seconds gauge"
            );
            let _ = writeln!(
                out,
                "dagron_ci_events_oldest_pending_age_seconds {}",
                backlog.oldest_age_secs.unwrap_or(0.0).max(0.0)
            );
        }

        let _ = writeln!(
            out,
            "# HELP dagron_ci_records_oldest_age_seconds Age of the oldest event or delivery in the database: past CI_RECORD_DAYS, the days not yet written out and dropped."
        );
        let _ = writeln!(out, "# TYPE dagron_ci_records_oldest_age_seconds gauge");
        let _ = writeln!(
            out,
            "dagron_ci_records_oldest_age_seconds {}",
            self.oldest_record_ms.load(Ordering::Relaxed) as f64 / 1000.0
        );
        let _ = writeln!(
            out,
            "# HELP dagron_ci_segments_written_total Segments of records written to CI_OBJECT_STORE_URL and read back."
        );
        let _ = writeln!(out, "# TYPE dagron_ci_segments_written_total counter");
        let _ = writeln!(
            out,
            "dagron_ci_segments_written_total {}",
            self.segments_written.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "# HELP dagron_ci_report_writes_waiting The reporter's looks waiting to be written to the database."
        );
        let _ = writeln!(out, "# TYPE dagron_ci_report_writes_waiting gauge");
        let _ = writeln!(
            out,
            "dagron_ci_report_writes_waiting {}",
            self.report_waiting.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "# HELP dagron_ci_report_write_oldest_wait_seconds How long the oldest look of the last batch written waited for it."
        );
        let _ = writeln!(
            out,
            "# TYPE dagron_ci_report_write_oldest_wait_seconds gauge"
        );
        let _ = writeln!(
            out,
            "dagron_ci_report_write_oldest_wait_seconds {}",
            self.report_oldest_wait_ms.load(Ordering::Relaxed) as f64 / 1000.0
        );
        let _ = writeln!(
            out,
            "# HELP dagron_ci_report_notices_total Changes of runs the reporter was told of through its cells' outboxes."
        );
        let _ = writeln!(out, "# TYPE dagron_ci_report_notices_total counter");
        let _ = writeln!(
            out,
            "dagron_ci_report_notices_total {}",
            self.report_notices.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "# HELP dagron_ci_report_feed_failures_total Claims of a cell's outbox that failed, after which its runs are looked at every few seconds until one succeeds."
        );
        let _ = writeln!(out, "# TYPE dagron_ci_report_feed_failures_total counter");
        let _ = writeln!(
            out,
            "dagron_ci_report_feed_failures_total {}",
            self.report_feed_failures.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "# HELP dagron_ci_report_batches_total Batches of the reporter's looks written, each one statement for its runs."
        );
        let _ = writeln!(out, "# TYPE dagron_ci_report_batches_total counter");
        let _ = writeln!(
            out,
            "dagron_ci_report_batches_total {}",
            self.report_batches.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "# HELP dagron_ci_report_looks_written_total The reporter's looks written, in those batches."
        );
        let _ = writeln!(out, "# TYPE dagron_ci_report_looks_written_total counter");
        let _ = writeln!(
            out,
            "dagron_ci_report_looks_written_total {}",
            self.report_writes.load(Ordering::Relaxed)
        );
        let _ = writeln!(
            out,
            "# HELP dagron_ci_events_dropped_total Events dropped with their day, past CI_RECORD_DAYS."
        );
        let _ = writeln!(out, "# TYPE dagron_ci_events_dropped_total counter");
        let _ = writeln!(
            out,
            "dagron_ci_events_dropped_total {}",
            self.records_dropped.load(Ordering::Relaxed)
        );

        for source in extra {
            source.render(&mut out);
        }
        out
    }
}

#[cfg(test)]
mod fleet_tests {
    use super::*;

    #[test]
    fn the_fleet_is_counted_by_label_from_the_first_scrape() {
        let mut gauges = crate::fleet::store::Gauges::default();
        gauges.waiting.insert("dagron".into(), 3);
        gauges.runners.insert(("gone".into(), "busy".into()), 1);
        let text = fleet(&["big", "dagron"], &gauges);
        for line in [
            "dagron_ci_fleet_jobs_waiting{label=\"big\"} 0",
            "dagron_ci_fleet_jobs_waiting{label=\"dagron\"} 3",
            "dagron_ci_fleet_runners{label=\"big\",state=\"launched\"} 0",
            "dagron_ci_fleet_runners{label=\"gone\",state=\"busy\"} 1",
        ] {
            assert!(text.lines().any(|l| l == line), "{line}\n{text}");
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct Extra;
    impl MetricsSource for Extra {
        fn render(&self, out: &mut String) {
            out.push_str("# TYPE extra gauge\nextra 1\n");
        }
    }

    #[test]
    fn every_outcome_is_exported_from_the_start() {
        let text = Metrics::default().render(None, &[]);
        for outcome in Outcome::ALL {
            assert!(
                text.contains(&format!("outcome=\"{}\"}} 0", outcome.as_str())),
                "{text}"
            );
        }
    }

    #[test]
    fn deliveries_are_counted_by_outcome() {
        let m = Metrics::default();
        m.github_delivery(Outcome::Accepted);
        m.github_delivery(Outcome::Accepted);
        m.github_delivery(Outcome::Unauthorized);
        let text = m.render(None, &[]);
        assert!(text.contains("outcome=\"accepted\"} 2"));
        assert!(text.contains("outcome=\"unauthorized\"} 1"));
        assert!(text.contains("outcome=\"duplicate\"} 0"));
    }

    #[test]
    fn the_backlog_is_left_out_when_unknown_not_zeroed() {
        let m = Metrics::default();
        assert!(!m.render(None, &[]).contains("dagron_ci_events_pending"));
        let text = m.render(
            Some(Backlog {
                pending: 3,
                oldest_age_secs: Some(12.5),
            }),
            &[],
        );
        assert!(text.contains("dagron_ci_events_pending 3\n"));
        assert!(text.contains("dagron_ci_events_oldest_pending_age_seconds 12.5\n"));
        let idle = m.render(
            Some(Backlog {
                pending: 0,
                oldest_age_secs: None,
            }),
            &[],
        );
        assert!(idle.contains("dagron_ci_events_oldest_pending_age_seconds 0\n"));
    }

    #[test]
    fn worker_outcomes_are_counted_from_zero() {
        let m = Metrics::default();
        let text = m.render(None, &[]);
        for outcome in FINISHES {
            assert!(
                text.contains(&format!(
                    "dagron_ci_events_finished_total{{outcome=\"{outcome}\"}} 0"
                )),
                "{text}"
            );
        }
        assert!(text.contains("dagron_ci_event_workers 0\n"));
        m.set_workers(8);
        m.event_finished(&Finish::Done("x".into()));
        m.event_finished(&Finish::Retry {
            reason: "x".into(),
            after: std::time::Duration::ZERO,
        });
        let text = m.render(None, &[]);
        assert!(text.contains("dagron_ci_event_workers 8\n"));
        assert!(text.contains("outcome=\"done\"} 1"));
        assert!(text.contains("outcome=\"retried\"} 1"));
    }

    #[test]
    fn poll_sweeps_are_counted_from_zero() {
        let m = Metrics::default();
        let text = m.render(None, &[]);
        for outcome in SWEEPS {
            assert!(
                text.contains(&format!(
                    "dagron_ci_poll_sweeps_total{{outcome=\"{outcome}\"}} 0"
                )),
                "{text}"
            );
        }
        m.poll_swept(&Swept::Queued(vec!["a".into(), "b".into()]));
        m.poll_swept(&Swept::Baseline(12));
        m.poll_swept(&Swept::Lost);
        let text = m.render(None, &[]);
        assert!(text.contains("dagron_ci_poll_sweeps_total{outcome=\"queued\"} 1"));
        assert!(text.contains("dagron_ci_poll_sweeps_total{outcome=\"baseline\"} 1"));
        assert!(text.contains("dagron_ci_poll_events_total 2\n"));
    }

    #[test]
    fn run_api_outcomes_are_counted_from_zero() {
        let m = Metrics::default();
        let text = m.render(None, &[]);
        for outcome in ApiOutcome::ALL {
            assert!(
                text.contains(&format!(
                    "dagron_ci_api_requests_total{{outcome=\"{}\"}} 0",
                    outcome.as_str()
                )),
                "{text}"
            );
        }
        m.api_request(ApiOutcome::Accepted);
        m.api_request(ApiOutcome::Replayed);
        m.api_request(ApiOutcome::Replayed);
        let text = m.render(None, &[]);
        assert!(text.contains("dagron_ci_api_requests_total{outcome=\"accepted\"} 1"));
        assert!(text.contains("dagron_ci_api_requests_total{outcome=\"replayed\"} 2"));
    }

    #[test]
    fn extra_sources_are_appended() {
        let text = Metrics::default().render(None, &[Arc::new(Extra)]);
        assert!(text.ends_with("extra 1\n"));
    }
}
