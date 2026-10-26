//! dagron-ci's own database: Postgres or SQLite, chosen at start by
//! `CI_DATABASE_URL`. Both drivers are in every build.
//!
//! The statements are written once, Postgres-style (`$1`), and rewritten for
//! SQLite (`?1`, its numbered form) by [`lite`]. Where the dialects differ in
//! more than that, both texts are here, side by side.
//!
//! On Postgres every connection's `search_path` is `CI_DATABASE_SCHEMA`, and
//! the schema is created before migrating. sqlx keeps its migration history in
//! an unqualified `_sqlx_migrations` table, so this is what keeps dagron-ci's
//! history apart from dagron-api's when the two share a database.

use std::collections::BTreeMap;
use std::str::FromStr;
use std::time::Duration;

use anyhow::Context;
use serde::Serialize;
use sqlx::postgres::{PgConnectOptions, PgPool, PgPoolOptions};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePool, SqlitePoolOptions};
use sqlx::Row;

use crate::config::{is_postgres, Config};
use crate::event::{commit_key, NewEvent};

/// dagron-ci's Postgres migrations with sqlx's own lock, as the tests run them
/// sqlx's way; dagron-ci itself runs them by [`migrate_pg`].
#[cfg(test)]
static MIGRATIONS_PG: sqlx::migrate::Migrator = sqlx::migrate!("./migrations_pg");
static MIGRATIONS_SQLITE: sqlx::migrate::Migrator = sqlx::migrate!("./migrations");

/// How long a pooled query may wait for a connection before it fails.
const ACQUIRE_TIMEOUT: Duration = Duration::from_secs(5);

/// The largest raw payload kept with its event. A larger one is kept in
/// CI_OBJECT_STORE_URL where there is one ([`crate::payloads`]), and
/// otherwise not at all; the event's columns still say everything the queue
/// needs.
pub const PAYLOAD_INLINE_MAX: usize = 64 * 1024;

const SELECT_EVENT_PAYLOAD: &str = "SELECT payload, payload_ref FROM ci_events WHERE id = $1";

/// Where an event's payload is.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EventPayload {
    /// In its row.
    Inline(String),
    /// In CI_OBJECT_STORE_URL, under this name.
    Stored(String),
}

const SELECT_RUN_NUMBERED: &str = "SELECT r.run_id, r.cell FROM ci_runs r \
     JOIN ci_run_numbered n ON n.idempotency_key = r.idempotency_key \
     WHERE r.repo_id = $1 AND n.run_id = $2 ORDER BY r.created_at LIMIT 1";

const SELECT_RUN_PLACE: &str = "SELECT repo_id, cell FROM ci_runs WHERE run_id = $1";

const SELECT_LIVE_CREDENTIAL_PG: &str = "SELECT run_id, repository, expires_at \
     FROM ci_checkout_credentials WHERE token_sha256 = $1 AND revoked_at IS NULL \
     AND (expires_at IS NULL OR expires_at::timestamptz > now())";

const SELECT_LIVE_CREDENTIAL_SQLITE: &str = "SELECT run_id, repository, expires_at \
     FROM ci_checkout_credentials WHERE token_sha256 = ?1 AND revoked_at IS NULL \
     AND (expires_at IS NULL OR julianday(expires_at) > julianday('now'))";

#[derive(Clone, Debug)]
pub enum Db {
    Postgres(PgPool),
    Sqlite(SqlitePool),
}

/// A repository dagron-ci serves.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Repo {
    pub id: String,
    pub forge: String,
    pub forge_host: String,
    pub full_name: String,
    /// The dagron-api workflow (its id) this repository's events run, until
    /// pipelines are compiled from the repository. `None` runs nothing.
    pub workflow: Option<String>,
    /// The cell, a dagron-api named in CI_CELLS, its runs go to.
    pub cell: String,
    /// How it is served: what dagron-ci runs of it.
    pub mode: Mode,
}

/// How a repository is served (its `mode`): which of its
/// pipelines dagron-ci runs, if any.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, serde::Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Mode {
    /// Its `.dagron/ci` pipelines (and the workflow bound to it).
    #[default]
    Native,
    /// Those, and the forge's own files its `.dagron/ci.yaml` names
    /// (`.github/workflows`, `.gitlab-ci.yml`), where the build reads them
    /// (CI_GITHUB_WORKFLOWS, CI_GITLAB_PIPELINES): the files the team keeps,
    /// run by dagron-ci in their forge's place.
    Compat,
    /// None: its forge runs its pipelines, and dagron-ci the runners of the
    /// jobs that ask for one of its labels ([`crate::fleet`]). Its pushes and
    /// merge requests queue nothing here, it is not swept, and its run
    /// requests are refused.
    Fleet,
}

impl Mode {
    /// Each mode as the table and the API say it.
    pub const NAMES: [&'static str; 3] = ["native", "compat", "fleet"];

    pub fn as_str(self) -> &'static str {
        match self {
            Mode::Native => "native",
            Mode::Compat => "compat",
            Mode::Fleet => "fleet",
        }
    }

    pub fn parse(name: &str) -> Option<Mode> {
        match name {
            "native" => Some(Mode::Native),
            "compat" => Some(Mode::Compat),
            "fleet" => Some(Mode::Fleet),
            _ => None,
        }
    }

    /// Whether dagron-ci runs any pipeline of a repository so served.
    pub fn runs_pipelines(self) -> bool {
        self != Mode::Fleet
    }
}

/// Why an event of a repository in fleet mode was not run.
pub const FLEET_MODE: &str =
    "its repository is in fleet mode: its forge runs its pipelines, and dagron-ci the runners \
     of the jobs that ask for one of its labels";

/// Who a change to a repository is recorded as made by, where it was made
/// with the database's own credentials and nobody was named.
pub const LOCAL: &str = "local";

/// Why an event of a disconnected repository was not run.
pub const DISCONNECTED: &str = "its repository was disconnected";

/// What connecting a repository came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Connected {
    /// It is connected for the first time.
    New(Repo),
    /// It was disconnected, and is connected again as the repository it
    /// was: its events and runs are under it still.
    Again(Repo),
    /// It is connected already, and nothing changed.
    Already,
}

/// One change to a repository, as it was recorded.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepoChange {
    pub at: String,
    /// The caller, as dagron-api named them; `cli:` and a name for one
    /// made at a terminal; [`LOCAL`] where nobody was named.
    pub actor: String,
    /// `connected`, `reconnected`, `changed` or `disconnected`.
    pub action: String,
    /// What it set, as JSON.
    pub detail: String,
}

/// An event a worker holds under a lease, with its repository.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Claimed {
    pub id: String,
    pub repo: Repo,
    pub kind: String,
    pub git_ref: String,
    /// The commit the event is for. An annotated tag's event is keyed by the
    /// tag's own id (`commit_key`), and this is the commit the tag names,
    /// where the door that queued it could say.
    pub sha: String,
    pub base_sha: Option<String>,
    pub base_ref: Option<String>,
    pub pr: Option<i64>,
    pub actor: Option<String>,
    pub trust: String,
    /// What the forge said a pull request's author is to the repository
    /// ([`crate::event::NewEvent::association`]).
    pub association: Option<String>,
    /// A pull request's head branch ([`crate::event::NewEvent::head_ref`]).
    pub head_ref: Option<String>,
    pub commit_key: String,
    /// Claims so far, this one included. More than one means an earlier
    /// attempt failed and was retried, or its worker stopped mid-event.
    pub attempts: i64,
    /// A dispatch's inputs; empty for every other event.
    pub inputs: BTreeMap<String, String>,
    /// The one pipeline a dispatch runs, as its caller named it (`deploy`,
    /// `deploy.yaml`). `None` runs every pipeline whose `on:` takes the event.
    pub pipeline: Option<String>,
    /// The bundle a run API request named: its runs are of the
    /// base, `sha`, with the bundle laid over it.
    pub bundle_id: Option<String>,
    /// What the bundle changes, read once the event is dispatched, for the
    /// reads of its pipelines ([`crate::bundles::Patch`]). Never stored.
    pub patch: Option<std::sync::Arc<crate::bundles::Patch>>,
}

/// How a worker leaves an event it holds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Finish {
    /// What the event asked for is done; the note says what that was.
    Done(String),
    /// Nothing to do, and the reason why: never retried.
    Skipped(String),
    /// It cannot be done, and the reason why: never retried.
    Failed(String),
    /// This attempt failed, for the reason given, in a way worth trying again
    /// once `after` has passed.
    Retry { reason: String, after: Duration },
    /// Not yet, for the reason given: try again once `after` has passed,
    /// without spending an attempt. A run waiting its turn behind its
    /// concurrency key, or a forge's spent rate limit.
    Wait { reason: String, after: Duration },
}

/// A run dagron-ci submitted, as `ci_runs` records it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SubmittedRun {
    pub run_id: String,
    pub event_id: String,
    pub repo_id: String,
    pub cell: String,
    pub pipeline: String,
    pub idempotency_key: String,
    /// Whether its failure raises a `failed` event: a pipeline
    /// of the commit's takes `on.failed` naming it, and the run is a trusted
    /// one's, of no patch bundle, and no failure's own.
    pub on_failed: bool,
}

/// A failed run whose `failed` event is due ([`Db::claim_failure`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DueFailure {
    pub run_id: String,
    pub cell: String,
    /// Its pipeline's file, as recorded (`.dagron/ci/ci.yaml`).
    pub pipeline: String,
    /// Until when the claim holds it: what its raise is fenced on.
    pub until: String,
}

/// What a run dagron-ci submitted is for: its repository, the commit its
/// event names, the cell it was submitted to, and whether it is over
/// (`status` other than `submitted`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RunCommit {
    pub repo: Repo,
    pub sha: String,
    pub cell: String,
    pub status: String,
    /// Whether the repository is connected still: a run outlives a
    /// disconnect, and is given nothing of the repository's after it.
    pub connected: bool,
    /// Its event's trust: what a forge token it holds may do.
    pub trust: String,
    /// The bundle laid over its commit, for a run on one.
    pub bundle_id: Option<String>,
}

/// A checkout credential, as its row names it: what it was minted for, and
/// never the credential itself (`ci_checkout_credentials`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckoutCredential {
    /// The credential's SHA-256, in hex: how a revoke finds its row.
    pub token_sha256: String,
    pub run_id: String,
    /// The task and the attempt, as the engine named them, if it did.
    pub task_id: Option<String>,
    pub attempt: Option<i64>,
    /// The URL the checkout fetched.
    pub repository: String,
    pub commit: String,
    /// The cell the run was submitted to, whose engine asked.
    pub cell: String,
    /// When the forge says it lapses on its own, in the forge's words.
    pub expires_at: Option<String>,
    /// Whether dagron-ci revoked it. Not written with the row: a credential
    /// is recorded before it is handed over.
    pub revoked: bool,
    /// For a forge token a job holds, the permissions it was given, as
    /// JSON; `None` for a checkout's credential.
    pub permissions: Option<String>,
    /// The forge's own id of a credential it revokes by id: a GitLab
    /// project access token's. `None` for a GitHub installation token.
    pub forge_token_id: Option<String>,
}

/// A run whose commit statuses are due, as a reporter holds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DueReport {
    /// The bundle laid over the run's commit: a run of a change
    /// nobody pushed, of which the forge is told nothing.
    pub bundle_id: Option<String>,
    pub run_id: String,
    /// The cell the run was submitted to, which serves it.
    pub cell: String,
    /// What `ci_runs` says the run ran: a pipeline's path, or `workflow:ID`.
    pub pipeline: String,
    pub repo: Repo,
    /// The event's kind (`push`, `pull_request`, …) and commit.
    pub kind: String,
    pub sha: String,
    /// The event's ref, and a pull request's head branch: what GitLab is
    /// told a status is of ([`crate::forge::gitlab::status_ref`]).
    pub git_ref: String,
    pub head_ref: Option<String>,
    /// The event's trust: a GitLab merge request's head branch is its
    /// project's own where it is `trusted`.
    pub trust: String,
    /// The event the run is of, which the report log keeps its posts by.
    pub event_id: String,
    /// What was last posted under each context: `state:description`, and
    /// `#N` after it for a result posted since the record's Nth re-opening.
    pub reported: BTreeMap<String, String>,
    /// How often the record re-opened: the run was seen running again.
    pub reopens: i64,
    /// The pull request the event is of, where it is one's.
    pub pr: Option<i64>,
    /// What the run's jobs came to at the last look that kept it, as JSON
    /// (`crate::report::comment`). `None` where none did.
    pub jobs: Option<String>,
}

/// How a reporter leaves a run it holds.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ReportWrite {
    /// What has been posted under each context, this look's posts included.
    pub reported: BTreeMap<String, String>,
    /// The run's status, when this look read it.
    pub status: Option<String>,
    /// Why this look could not post everything, when it could not.
    pub error: Option<String>,
    /// When to look again; `None` once the run is over and its statuses are
    /// as final as they will be.
    pub next: Option<Duration>,
    /// Whether this look saw any of the run's jobs started.
    pub started: bool,
    /// How many times the run was rerun whole, where this look read it (at
    /// its end). A count above the record's starts a round: `started` is
    /// then this look's alone.
    pub whole_reruns: Option<i64>,
    /// Whether a meter is to be told of the run's end, should this write
    /// record it (`crate::meter`).
    pub meter: bool,
    /// What the run's jobs came to, as JSON, where that is kept and has
    /// changed since the last look. `None` leaves what is kept.
    pub jobs: Option<String>,
    /// The pull request, by its repository's id and its number, whose
    /// comment is due once this is written: with it, or not at all.
    pub comment: Option<(String, i64)>,
    /// Each status this look posted, and what came of it: the report log's
    /// rows, written with the run's record.
    pub logged: Vec<ReportLog>,
    /// The jobs that flaked, where this look saw the run end and flakes are
    /// counted (CI_FLAKE_ISSUES): kept with the run's record.
    pub flakes: Option<Flakes>,
}

/// The jobs of a run that flaked, as a look at its end saw them, and how a
/// job's flakes are counted (`crate::report::flake`).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Flakes {
    /// Each job that passed after an attempt of it failed, and how many did.
    pub jobs: Vec<(String, i64)>,
    /// How many of a job's flakes within `days` make its issue due.
    pub file_after: i64,
    pub days: i64,
}

/// The issue of a job that flakes, as its writer holds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DueFlakeIssue {
    pub repo: Repo,
    /// What `ci_runs` says the runs ran: a pipeline's path, or `workflow:ID`.
    pub pipeline: String,
    pub job: String,
    /// The forge's number for the issue, while one is open.
    pub issue: Option<String>,
    /// A digest of what it last said.
    pub said: Option<String>,
}

/// One flake of a job, as its issue lists it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FlakeRow {
    pub run_id: String,
    /// The cell that ran it, which its page is of.
    pub cell: String,
    pub sha: String,
    /// How many attempts failed before the one that passed.
    pub failed: i64,
    pub seen_at: String,
}

/// A pull request's comment that is due, as its writer holds it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DueComment {
    pub repo: Repo,
    pub pr: i64,
    /// The forge's id for the comment, once there is one.
    pub comment_id: Option<String>,
    /// A digest of what it last said.
    pub said: Option<String>,
}

/// A run of a pull request's latest commit, as its comment says it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PrRun {
    pub run_id: String,
    pub cell: String,
    /// What `ci_runs` says the run ran: a pipeline's path, or `workflow:ID`.
    pub pipeline: String,
    /// The run's status as dagron-ci last saw it.
    pub status: String,
    /// What its jobs came to, as JSON; `None` before its first look.
    pub jobs: Option<String>,
    /// The commit it ran.
    pub sha: String,
}

/// How a writer leaves a comment it holds.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CommentWrite {
    /// A digest of what the comment now says, where it was written.
    pub said: Option<String>,
    /// Why it was not written, when it was not.
    pub error: Option<String>,
    /// When to try again; `None` leaves it until a run of the pull request
    /// changes.
    pub next: Option<Duration>,
}

/// One row of `dagron-ci events`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventRow {
    pub id: String,
    pub repo: String,
    pub kind: String,
    pub git_ref: String,
    pub sha: String,
    pub state: String,
    pub attempts: i64,
    pub reason: Option<String>,
}

/// What recording a delivery did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Recorded {
    /// A new event, with its id.
    Event(String),
    /// This delivery id was seen before; nothing was written.
    Duplicate,
}

/// One day, UTC, as days since 1970-01-01.
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub struct Day(pub i64);

impl Day {
    /// The day it is now.
    pub fn today() -> Day {
        let secs = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(0, |d| d.as_secs());
        Day((secs / 86_400) as i64)
    }

    pub fn next(self) -> Day {
        Day(self.0 + 1)
    }

    pub fn minus(self, days: u32) -> Day {
        Day(self.0 - i64::from(days))
    }

    fn ymd(self) -> (i64, u32, u32) {
        crate::cron::civil(self.0)
    }

    /// Its first instant, as both backends compare `received_at` with it.
    pub fn start(self) -> String {
        let (y, m, d) = self.ymd();
        format!("{y:04}-{m:02}-{d:02}T00:00:00.000Z")
    }

    /// The name of its partition of `ci_events`.
    pub fn partition(self) -> String {
        let (y, m, d) = self.ymd();
        format!("ci_events_{y:04}{m:02}{d:02}")
    }

    /// `YYYY-MM-DD`.
    pub fn date(self) -> String {
        let (y, m, d) = self.ymd();
        format!("{y:04}-{m:02}-{d:02}")
    }

    /// The day `YYYY-MM-DD` names, where it names one.
    pub fn parse(date: &str) -> Option<Day> {
        let mut parts = date.splitn(3, '-');
        let y: i64 = parts.next()?.parse().ok()?;
        let m: i64 = parts.next()?.parse().ok()?;
        let d: i64 = parts.next()?.parse().ok()?;
        if date.len() != 10 || !(1..=12).contains(&m) || !(1..=31).contains(&d) {
            return None;
        }
        // Howard Hinnant's days_from_civil, the inverse of cron::civil.
        let y = if m <= 2 { y - 1 } else { y };
        let era = y.div_euclid(400);
        let yoe = y - era * 400;
        let doy = (153 * ((m + 9) % 12) + 2) / 5 + d - 1;
        let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
        let day = Day(era * 146_097 + doe - 719_468);
        (day.date() == date).then_some(day)
    }

    /// The day of the instant a UUIDv7 was made at: an event's id says
    /// which day it was received, near enough.
    pub fn of_uuid_v7(id: &str) -> Option<Day> {
        let uuid = uuid::Uuid::parse_str(id).ok()?;
        let (secs, _) = uuid.get_timestamp()?.to_unix();
        Some(Day((secs / 86_400) as i64))
    }
}

/// dagron-ci's records, as they leave the database a day at a time.
pub const RECORD_TABLES: [&str; 5] = [
    "ci_deliveries",
    "ci_events",
    "ci_runs",
    "ci_api_requests",
    "ci_report_log",
];

/// A status dagron-ci posted, and what came of it: a row of
/// `ci_report_log`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize)]
pub struct ReportLog {
    pub event_id: String,
    pub run_id: Option<String>,
    pub repo_id: String,
    pub sha: String,
    pub context: String,
    pub state: String,
    pub description: Option<String>,
    pub target_url: Option<String>,
    /// `posted`, `refused`, `rate_limited` or `failed`.
    pub outcome: &'static str,
    /// Why, where it was not posted.
    pub detail: Option<String>,
}

/// A day's rows of one table, written to the store: a row of
/// `ci_segments`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Segment {
    pub table: String,
    /// `YYYY-MM-DD`.
    pub day: String,
    pub scope: String,
    pub part: i32,
    pub object_key: String,
    pub row_count: i64,
    pub size_bytes: i64,
    /// Of what is stored, compressed.
    pub sha256: String,
    /// Of the table's rows that day as they were read ([`rows_digest`]).
    pub rows_sha256: String,
    pub verified: bool,
}

/// The order a table's rows of a day are read in, so that the same rows read
/// alike.
fn record_order(table: &str) -> &'static str {
    match table {
        "ci_deliveries" => "forge_host, delivery_id",
        "ci_runs" => "run_id",
        "ci_api_requests" => "principal, idempotency_key",
        _ => "id",
    }
}

/// The statement that reads `table`'s rows of a day, each a JSON object of
/// its columns, in [`record_order`]: `a` and `b` the day's bounds as the
/// backend takes them, and on SQLite the table's `columns`.
fn records_sql(table: &str, a: &str, b: &str, columns: Option<&[String]>) -> String {
    let when = format!("received_at >= {a} AND received_at < {b}");
    let when = match table {
        "ci_events" | "ci_deliveries" => when,
        _ => format!("event_id IN (SELECT id FROM ci_events WHERE {when})"),
    };
    let order = record_order(table);
    match columns {
        None => format!("SELECT row_to_json(t)::TEXT FROM {table} t WHERE {when} ORDER BY {order}"),
        Some(columns) => {
            let object = columns
                .iter()
                .map(|c| format!("'{c}', {c}"))
                .collect::<Vec<_>>()
                .join(", ");
            format!("SELECT json_object({object}) FROM {table} WHERE {when} ORDER BY {order}")
        }
    }
}

/// The SHA-256 of rows as [`Db::records_of_day`] reads them, one a line;
/// empty for none.
pub fn rows_digest(rows: &[String]) -> String {
    if rows.is_empty() {
        return String::new();
    }
    let mut hash = <sha2::Sha256 as sha2::Digest>::new();
    for row in rows {
        sha2::Digest::update(&mut hash, row.as_bytes());
        sha2::Digest::update(&mut hash, b"\n");
    }
    hex::encode(sha2::Digest::finalize(hash))
}

macro_rules! segment_of {
    ($name:ident, $row:ty) => {
        fn $name(row: &$row) -> sqlx::Result<Segment> {
            Ok(Segment {
                table: row.try_get("table_name")?,
                day: row.try_get("day")?,
                scope: row.try_get("scope")?,
                part: row.try_get("part")?,
                object_key: row.try_get("object_key")?,
                row_count: row.try_get("row_count")?,
                size_bytes: row.try_get("size_bytes")?,
                sha256: row.try_get("sha256")?,
                rows_sha256: row.try_get("rows_sha256")?,
                verified: row.try_get("verified")?,
            })
        }
    };
}
segment_of!(segment_pg, sqlx::postgres::PgRow);
segment_of!(segment_sqlite, sqlx::sqlite::SqliteRow);

/// What dropping old events took.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct DroppedEvents {
    /// The partitions dropped whole, on Postgres.
    pub partitions: Vec<String>,
    pub events: u64,
    pub runs: u64,
    /// The stored payloads the dropped events named.
    pub payloads: Vec<String>,
}

/// Held by whoever makes or drops `ci_events`' partitions, so that two
/// replicas never do it at once.
const PARTITIONS_LOCK: &str =
    "SELECT pg_advisory_xact_lock(hashtext('dagron-ci: ci_events partitions'))";

/// How long dropping a day's partition waits for the queue's lock on
/// `ci_events`.
pub const DROP_LOCK_WAIT: Duration = Duration::from_secs(5);

/// What recording a dispatch did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Dispatched {
    /// A new event, with its id.
    Event(String),
    /// The caller sent this `Idempotency-Key` before: the event that request
    /// queued, and whether this one is the same request. Nothing was written.
    Repeat { event_id: String, same: bool },
}

/// A run API caller's `Idempotency-Key`, and the request it came with.
#[derive(Clone, Copy, Debug)]
pub struct RequestKey<'a> {
    /// The caller's subject in dagron-api: keys are scoped to it.
    pub principal: &'a str,
    pub key: &'a str,
    pub fingerprint: &'a str,
}

/// An event as `GET /ci/v1/runs/{id}` shows it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EventView {
    pub id: String,
    /// `host/owner/name`.
    pub repo: String,
    /// The repository's cell, whose dagron-api identifies a reader.
    pub cell: String,
    pub kind: String,
    pub git_ref: String,
    pub sha: String,
    pub source: String,
    pub trust: String,
    pub actor: Option<String>,
    pub state: String,
    pub reason: Option<String>,
    pub attempts: i64,
    /// The one pipeline a dispatch asked for, if it named one.
    pub pipeline: Option<String>,
    /// The patch bundle the run API laid over the commit, if one was, and
    /// its SHA-256 as dagron-ci checked it (none once the bundle has gone).
    pub bundle: Option<String>,
    pub bundle_sha256: Option<String>,
    /// The engine runs this event submitted, oldest first.
    pub runs: Vec<EngineRun>,
}

/// An engine run an event submitted.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct EngineRun {
    pub id: String,
    pub cell: String,
    /// The run's status as dagron-ci last saw it: `submitted` while it is
    /// followed, then how it ended (`succeeded`, `failed`, `cancelled`), or
    /// `gone`. `submitted` again once it is seen running again.
    pub status: String,
    /// The key the run was submitted under, and holds its admission slot by.
    pub key: String,
    /// The pipeline's path, `.dagron/ci/engine.yaml`, or `workflow:ID` for
    /// the bound workflow.
    pub pipeline: String,
}

/// The event queue's depth, for `/metrics`.
#[derive(Clone, Copy, Debug, PartialEq)]
pub struct Backlog {
    pub pending: i64,
    /// Seconds since the oldest pending event arrived, or `None` when nothing
    /// is pending.
    pub oldest_age_secs: Option<f64>,
}

/// One row of `dagron-ci repo list`: a repository and how its polling goes.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepoListing {
    pub repo: Repo,
    /// Its own poll interval in seconds, or `None` for CI_POLL_SECS's.
    pub poll_secs: Option<i64>,
    /// When a sweep last finished, as the database prints it.
    pub polled_at: Option<String>,
    /// Why the last sweep failed, until one succeeds.
    pub poll_error: Option<String>,
}

/// A repository a sweeper holds, with what its sweep needs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DueRepo {
    pub repo: Repo,
    /// Its poll interval: its own, or CI_POLL_SECS's.
    pub poll_secs: u64,
    /// No sweep of it has finished: this one records what it finds and
    /// queues nothing.
    pub baseline: bool,
    /// The open pull requests' ETag from the last sweep.
    pub etag: Option<String>,
}

/// A repository whose schedules are due to be looked at.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DueSchedule {
    pub repo: Repo,
    /// The commit its schedules were last read at, if ever.
    pub sha: Option<String>,
}

/// One cron of one workflow file: when it next fires and last fired, in
/// seconds since the epoch.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ScheduleRow {
    /// The file's name under `.github/workflows`: what its event names as
    /// its pipeline.
    pub workflow: String,
    pub cron: String,
    /// `None`: it never fires (`0 0 31 2 *`).
    pub next_at: Option<i64>,
    pub last_at: Option<i64>,
}

/// A cron that fired at `at`: the `schedule` event it queues.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Fired {
    pub workflow: String,
    pub cron: String,
    pub at: i64,
    /// The default branch, `refs/heads/main`, and its head.
    pub git_ref: String,
    pub sha: String,
}

impl Fired {
    /// Its event's commit key: its own, whatever else fires at that commit,
    /// so that each fire is a run of its own.
    pub fn key(&self, repo_id: &str) -> String {
        use sha2::{Digest, Sha256};
        let digest = Sha256::digest(
            format!("{repo_id}\0{}\0{}\0{}", self.workflow, self.cron, self.at).as_bytes(),
        );
        format!("schedule:{}", hex::encode(digest))
    }
}

/// What a finished sweep writes, in one transaction.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SweepWrite {
    /// Events to queue, as source `poll`.
    pub events: Vec<NewEvent>,
    /// Refs seen new or moved, and the commit each points at now.
    pub set: Vec<(String, String)>,
    /// Refs no longer there.
    pub drop: Vec<String>,
    /// The open pull requests' ETag to keep for the next sweep.
    pub etag: Option<String>,
}

/// A run's end, as [`METER_END_VIEW`] reads it on either database.
fn ended_run<R: Row>(row: &R) -> sqlx::Result<crate::meter::EndedRun>
where
    for<'r> &'r str: sqlx::ColumnIndex<R>,
    for<'r> String: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    for<'r> i32: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
    for<'r> i64: sqlx::Decode<'r, R::Database> + sqlx::Type<R::Database>,
{
    Ok(crate::meter::EndedRun {
        run: crate::meter::MeteredRun {
            run_id: row.try_get("run_id")?,
            cell: row.try_get("cell")?,
            repo: row.try_get("repo")?,
            pipeline: row.try_get("pipeline")?,
        },
        kind: row.try_get("kind")?,
        status: row.try_get("status")?,
        started: row.try_get::<i32, _>("started")? != 0,
        whole_reruns: row.try_get("whole_reruns")?,
    })
}

/// SQLite's numbered parameters are `?1`, where Postgres writes `$1`. No
/// statement here contains a `$` for any other reason.
pub(crate) fn lite(sql: &str) -> String {
    sql.replace('$', "?")
}

const INSERT_DELIVERY: &str =
    "INSERT INTO ci_deliveries (forge_host, delivery_id) VALUES ($1, $2) \
     ON CONFLICT (forge_host, delivery_id) DO NOTHING";

const FORGET_DELIVERY: &str =
    "DELETE FROM ci_deliveries WHERE forge_host = $1 AND delivery_id = $2";

const INSERT_EVENT: &str = "INSERT INTO ci_events \
     (id, repo_id, source, delivery_id, kind, ref, sha, base_sha, pr, actor, trust, commit_key, payload, \
      base_ref, commit_sha, association, head_ref) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17)";

/// A webhook's event, which may name a payload kept in the store.
const INSERT_DELIVERED_EVENT: &str = "INSERT INTO ci_events \
     (id, repo_id, source, delivery_id, kind, ref, sha, base_sha, pr, actor, trust, commit_key, payload, \
      base_ref, commit_sha, association, head_ref, payload_ref) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15, $16, $17, $18)";

// A repository is connected while its row has no `disconnected_at`: one
// that was disconnected keeps its row, for its events and runs to be read
// by, and is no repository of ours to a webhook, a run request or a sweep.
const FIND_REPO: &str = "SELECT id, forge, forge_host, full_name, workflow, cell, mode \
     FROM ci_repos WHERE forge_host = $1 AND full_name = $2 AND disconnected_at IS NULL";

// A repository whether or not it is still connected, and since when it is
// not.
const REPO_ANY: &str = "SELECT id, forge, forge_host, full_name, workflow, cell, mode, \
         CAST(disconnected_at AS TEXT) AS disconnected_at \
     FROM ci_repos WHERE forge_host = $1 AND full_name = $2";

// The same, by its id.
const REPO_ANY_BY_ID: &str = "SELECT id, forge, forge_host, full_name, workflow, cell, mode, \
         CAST(disconnected_at AS TEXT) AS disconnected_at \
     FROM ci_repos WHERE id = $1";

const INSERT_REPO_CHANGE: &str =
    "INSERT INTO ci_repo_changes (id, repo_id, actor, action, detail) VALUES ($1, $2, $3, $4, $5)";

const REPO_CHANGES: &str = "SELECT CAST(at AS TEXT) AS at, actor, action, detail \
     FROM ci_repo_changes WHERE repo_id = $1 ORDER BY at, id";

// The events of a repository that were waiting when it was disconnected:
// they are not run, and say why.
const SKIP_EVENTS_OF: &str =
    "UPDATE ci_events SET state = 'skipped', reason = $2 WHERE repo_id = $1 AND state = 'new'";

// The fleet's jobs of a repository that waited when it was disconnected.
// Its deliveries are refused from then on, so nothing would ever say they
// were taken or had ended: left waiting, they would keep the repository's
// fleet looked over for good, and be given runners when it is connected
// again, for jobs GitHub has long been done with. They are closed, with no
// conclusion: how they ended is not known here.
const CLOSE_FLEET_JOBS_OF: &str =
    "UPDATE ci_fleet_jobs SET state = 'done' WHERE repo_id = $1 AND state = 'queued'";

// Whether a repository is connected, by the name a checkout's URL gives it.
// Without regard to case: a forge takes `Acme/Widgets` for `acme/widgets`,
// and so must the answer to "is this one of ours".
const SERVES_REPO: &str = "SELECT 1 FROM ci_repos \
     WHERE lower(forge_host) = lower($1) AND lower(full_name) = lower($2) \
       AND disconnected_at IS NULL LIMIT 1";

const REPO_BY_ID: &str = "SELECT id, forge, forge_host, full_name, workflow, cell, mode, \
         CAST(disconnected_at IS NULL AS INTEGER) AS connected \
     FROM ci_repos WHERE id = $1";

const LIST_REPOS: &str = "SELECT id, forge, forge_host, full_name, workflow, cell, mode, \
         CAST(poll_secs AS BIGINT) AS poll_secs, CAST(polled_at AS TEXT) AS polled_at, poll_error \
     FROM ci_repos WHERE disconnected_at IS NULL ORDER BY forge_host, full_name";

// A repository that was disconnected is connected again as the row it was,
// so that its history goes on under it: its id comes back, and none does
// for one that is connected already. What it is set to is what this connect
// names, as a first time, its interval among them, and its next sweep is
// a baseline (`polled_at` is null), so that what moved while it was away is
// recorded and not run.
const INSERT_REPO: &str =
    "INSERT INTO ci_repos (id, forge, forge_host, full_name, workflow, cell, poll_secs, mode) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8) \
     ON CONFLICT (forge_host, full_name) DO UPDATE \
         SET disconnected_at = NULL, forge = excluded.forge, workflow = excluded.workflow, \
             cell = excluded.cell, poll_secs = excluded.poll_secs, mode = excluded.mode, \
             poll_due_at = NULL, \
             polled_at = NULL, \
             poll_etag = NULL, poll_error = NULL \
         WHERE ci_repos.disconnected_at IS NOT NULL \
     RETURNING id";

/// `$3` and `$4` say whether to set the workflow and the cell, so one
/// statement serves every combination `dagron-ci repo set` takes.
const BIND_REPO: &str = "UPDATE ci_repos \
     SET workflow = CASE WHEN $3 THEN $5 ELSE workflow END, \
         cell = CASE WHEN $4 THEN $6 ELSE cell END \
     WHERE forge_host = $1 AND full_name = $2 AND disconnected_at IS NULL";

// A repository that leaves fleet mode is swept next from a baseline: no
// sweep looked at it while it was in it, and what moved then is recorded,
// and not run.
const SET_MODE: &str = "UPDATE ci_repos \
     SET mode = $3, \
         polled_at = CASE WHEN mode = 'fleet' THEN NULL ELSE polled_at END, \
         poll_etag = CASE WHEN mode = 'fleet' THEN NULL ELSE poll_etag END, \
         poll_due_at = CASE WHEN mode = 'fleet' THEN NULL ELSE poll_due_at END \
     WHERE forge_host = $1 AND full_name = $2 AND disconnected_at IS NULL";

// The claim takes the oldest event that is new and due, or whose claim has
// lapsed (its worker stopped mid-event). Each claim counts as an attempt.
// What it hands the worker as `sha` is the commit: for an annotated tag the
// one the tag names, where the event has it (`commit_sha`).
const CLAIM_PG: &str = "UPDATE ci_events \
     SET state = 'claimed', claimed_by = $1, \
         claim_expires_at = now() + make_interval(secs => $2), attempts = attempts + 1 \
     WHERE id = ( \
         SELECT id FROM ci_events \
         WHERE (state = 'new' AND (next_attempt_at IS NULL OR next_attempt_at <= now())) \
            OR (state = 'claimed' AND claim_expires_at < now()) \
         ORDER BY received_at \
         LIMIT 1 \
         FOR UPDATE SKIP LOCKED) \
     RETURNING id, repo_id, kind, ref, COALESCE(commit_sha, sha) AS sha, base_sha, base_ref, pr, \
         actor, trust, association, head_ref, commit_key, CAST(attempts AS BIGINT) AS attempts, \
         inputs, pipeline, bundle_id";

const CLAIM_SQLITE: &str = "UPDATE ci_events \
     SET state = 'claimed', claimed_by = ?1, \
         claim_expires_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?2 || ' seconds'), \
         attempts = attempts + 1 \
     WHERE id = ( \
         SELECT id FROM ci_events \
         WHERE (state = 'new' AND (next_attempt_at IS NULL \
                OR next_attempt_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now'))) \
            OR (state = 'claimed' AND claim_expires_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) \
         ORDER BY received_at \
         LIMIT 1) \
     RETURNING id, repo_id, kind, ref, COALESCE(commit_sha, sha) AS sha, base_sha, base_ref, pr, \
         actor, trust, association, head_ref, commit_key, CAST(attempts AS BIGINT) AS attempts, \
         inputs, pipeline, bundle_id";

// Fenced on the claim: a worker whose lease lapsed, and whose event another
// worker took over, changes nothing.
const FINISH: &str = "UPDATE ci_events \
     SET state = $3, reason = $4, claimed_by = NULL, claim_expires_at = NULL \
     WHERE id = $1 AND claimed_by = $2 AND state = 'claimed'";

// `$5` is 1 for a wait, which gives back the attempt its claim counted.
const RETRY_PG: &str = "UPDATE ci_events \
     SET state = 'new', reason = $3, claimed_by = NULL, claim_expires_at = NULL, \
         next_attempt_at = now() + make_interval(secs => $4), attempts = attempts - $5 \
     WHERE id = $1 AND claimed_by = $2 AND state = 'claimed'";

const RETRY_SQLITE: &str = "UPDATE ci_events \
     SET state = 'new', reason = ?3, claimed_by = NULL, claim_expires_at = NULL, \
         next_attempt_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?4 || ' seconds'), \
         attempts = attempts - ?5 \
     WHERE id = ?1 AND claimed_by = ?2 AND state = 'claimed'";

const FIND_RUN: &str = "SELECT run_id FROM ci_runs WHERE idempotency_key = $1";

// The next of a count of runs ([`Db::run_numbers`]): one statement, so that
// two workers never take one number.
const NEXT_RUN_NUMBER: &str = "INSERT INTO ci_run_numbers (repo_id, workflow, last) \
     VALUES ($1, $2, 1) \
     ON CONFLICT (repo_id, workflow) DO UPDATE SET last = ci_run_numbers.last + 1 \
     RETURNING CAST(last AS BIGINT)";

const FIND_RUN_NUMBERS: &str = "SELECT CAST(run_id AS BIGINT), CAST(run_number AS BIGINT) \
     FROM ci_run_numbered WHERE idempotency_key = $1";

// The first numbers a key is given are the ones it keeps.
const KEEP_RUN_NUMBERS: &str =
    "INSERT INTO ci_run_numbered (idempotency_key, run_id, run_number) VALUES ($1, $2, $3) \
     ON CONFLICT (idempotency_key) DO NOTHING";

const INSERT_DISPATCH: &str = "INSERT INTO ci_events \
     (id, repo_id, source, kind, ref, sha, actor, trust, commit_key, inputs, pipeline, bundle_id) \
     VALUES ($1, $2, 'api', $3, $4, $5, $6, $7, $8, $9, $10, $11)";

const INSERT_API_REQUEST: &str = "INSERT INTO ci_api_requests \
     (principal, idempotency_key, fingerprint, event_id) VALUES ($1, $2, $3, $4) \
     ON CONFLICT (principal, idempotency_key) DO NOTHING";

const FIND_API_REQUEST: &str = "SELECT event_id, fingerprint FROM ci_api_requests \
     WHERE principal = $1 AND idempotency_key = $2";

// An event that waits to be dispatched is withdrawn. One a worker holds is
// that worker's to finish, and one that is settled stays as it ended.
const WITHDRAW_EVENT: &str = "UPDATE ci_events \
     SET state = 'skipped', reason = $2 \
     WHERE id = $1 AND state = 'new'";

const INSERT_CREDENTIAL: &str = "INSERT INTO ci_checkout_credentials \
     (id, token_sha256, run_id, task_id, attempt, repository, commit_sha, cell, expires_at, \
      permissions, forge_token_id) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)";

const FORGE_TOKEN_OF: &str = "SELECT repository, forge_token_id, \
         CAST(CASE WHEN revoked_at IS NULL THEN 0 ELSE 1 END AS BIGINT) AS revoked \
     FROM ci_checkout_credentials WHERE token_sha256 = $1 AND forge_token_id IS NOT NULL";

const REVOKE_CREDENTIAL_PG: &str = "UPDATE ci_checkout_credentials SET revoked_at = now() \
     WHERE token_sha256 = $1 AND revoked_at IS NULL";

const REVOKE_CREDENTIAL_SQLITE: &str = "UPDATE ci_checkout_credentials \
     SET revoked_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
     WHERE token_sha256 = ?1 AND revoked_at IS NULL";

const CREDENTIALS_OF_RUN: &str = "SELECT token_sha256, run_id, task_id, \
         CAST(attempt AS BIGINT) AS attempt, repository, commit_sha, cell, expires_at, \
         CAST(CASE WHEN revoked_at IS NULL THEN 0 ELSE 1 END AS BIGINT) AS revoked, \
         permissions, forge_token_id \
     FROM ci_checkout_credentials WHERE run_id = $1 ORDER BY minted_at, id";

// Have a pipeline's runs at a commit post their statuses afresh: each due
// for a report now, what it posted forgotten so that every context is posted
// again, and a reporter that holds one let go of it, whose write then
// changes nothing (as after a re-opening, REOPEN_RUN). The runs are those
// under an event of either of two kinds: the kinds whose jobs report under
// one name. Where an event is named ($6), its own runs are not among them.
const REPORT_AGAIN_PG: &str = "UPDATE ci_runs \
     SET report_due_at = now(), report_claim = NULL, reported = '{}' \
     WHERE run_id IN ( \
         SELECT r.run_id FROM ci_runs r \
         JOIN ci_events e ON e.id = r.event_id \
         WHERE r.repo_id = $1 AND r.pipeline = $2 \
           AND COALESCE(e.commit_sha, e.sha) = $3 AND e.kind IN ($4, $5) \
           AND (CAST($6 AS TEXT) IS NULL OR r.event_id <> $6))";

const REPORT_AGAIN_SQLITE: &str = "UPDATE ci_runs \
     SET report_due_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), report_claim = NULL, \
         reported = '{}' \
     WHERE run_id IN ( \
         SELECT r.run_id FROM ci_runs r \
         JOIN ci_events e ON e.id = r.event_id \
         WHERE r.repo_id = ?1 AND r.pipeline = ?2 \
           AND COALESCE(e.commit_sha, e.sha) = ?3 AND e.kind IN (?4, ?5) \
           AND (CAST(?6 AS TEXT) IS NULL OR r.event_id <> ?6))";

// What each run REPORT_AGAIN names has posted, but for one event's own runs.
const POSTED_BY_OTHERS: &str = "SELECT r.reported FROM ci_runs r \
     JOIN ci_events e ON e.id = r.event_id \
     WHERE r.repo_id = $1 AND r.pipeline = $2 \
       AND COALESCE(e.commit_sha, e.sha) = $3 AND e.kind IN ($4, $5) \
       AND r.event_id <> $6";

// A run owns a context (ci_contexts) unless a lease is in force on it: then
// no row comes back, and the run posts after the lease is given back.
const OWN_CONTEXT_PG: &str = "INSERT INTO ci_contexts (repo_id, sha, context, run_id) \
     VALUES ($1, $2, $3, $4) \
     ON CONFLICT (repo_id, sha, context) DO UPDATE \
     SET run_id = EXCLUDED.run_id, lease_holder = NULL, lease_until = NULL, written_at = now() \
     WHERE ci_contexts.lease_until IS NULL OR ci_contexts.lease_until <= now() \
     RETURNING run_id";

const OWN_CONTEXT_SQLITE: &str = "INSERT INTO ci_contexts (repo_id, sha, context, run_id) \
     VALUES (?1, ?2, ?3, ?4) \
     ON CONFLICT (repo_id, sha, context) DO UPDATE \
     SET run_id = excluded.run_id, lease_holder = NULL, lease_until = NULL, \
         written_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
     WHERE ci_contexts.lease_until IS NULL \
        OR ci_contexts.lease_until <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
     RETURNING run_id";

// A lease on a context no run owns, and that no one else's lease is in
// force on: its holder's own again, where it took one before.
const LEASE_CONTEXT_PG: &str = "INSERT INTO ci_contexts \
         (repo_id, sha, context, lease_holder, lease_until) \
     VALUES ($1, $2, $3, $4, now() + make_interval(secs => $5)) \
     ON CONFLICT (repo_id, sha, context) DO UPDATE \
     SET lease_holder = EXCLUDED.lease_holder, lease_until = EXCLUDED.lease_until, \
         written_at = now() \
     WHERE ci_contexts.run_id IS NULL \
       AND (ci_contexts.lease_until IS NULL OR ci_contexts.lease_until <= now() \
            OR ci_contexts.lease_holder = EXCLUDED.lease_holder) \
     RETURNING lease_holder";

const LEASE_CONTEXT_SQLITE: &str = "INSERT INTO ci_contexts \
         (repo_id, sha, context, lease_holder, lease_until) \
     VALUES (?1, ?2, ?3, ?4, strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?5 || ' seconds')) \
     ON CONFLICT (repo_id, sha, context) DO UPDATE \
     SET lease_holder = excluded.lease_holder, lease_until = excluded.lease_until, \
         written_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
     WHERE ci_contexts.run_id IS NULL \
       AND (ci_contexts.lease_until IS NULL \
            OR ci_contexts.lease_until <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
            OR ci_contexts.lease_holder = excluded.lease_holder) \
     RETURNING lease_holder";

const RELEASE_CONTEXT: &str = "UPDATE ci_contexts SET lease_holder = NULL, lease_until = NULL \
     WHERE repo_id = $1 AND sha = $2 AND context = $3 AND lease_holder = $4";

// A repository's runs at a commit that were reported as check runs: what a
// re-run of all of a commit's checks is of. A run's record keeps what it
// posted, and a check run's entry begins `check:` (report.rs).
const CHECKED_RUNS_AT: &str = "SELECT r.run_id FROM ci_runs r \
     JOIN ci_events e ON e.id = r.event_id \
     WHERE r.repo_id = $1 AND COALESCE(e.commit_sha, e.sha) = $2 \
       AND r.reported LIKE '%\"check:%' \
     ORDER BY r.created_at, r.run_id";

const EVENT_OF_RUN: &str = "SELECT event_id FROM ci_runs WHERE run_id = $1";

// A merge-queue group is its ref and its head: a GitLab merge train's ref is
// its merge request's, and names each pipeline the train makes of it.
const GROUP_EVENTS: &str = "SELECT id, state FROM ci_events \
     WHERE repo_id = $1 AND kind = 'merge_group' AND ref = $2 AND sha = $3 \
     ORDER BY received_at, id";

// Whether a group asked for its checks since it was last let go of, if it
// was: one let go of and asked for again is asked anew.
const GROUP_ASKED: &str = "SELECT 1 FROM ci_events a \
     WHERE a.repo_id = $1 AND a.kind = 'merge_group' AND a.ref = $2 AND a.sha = $3 \
       AND NOT EXISTS (SELECT 1 FROM ci_events d \
           WHERE d.repo_id = a.repo_id AND d.kind = 'merge_group_destroyed' \
             AND d.ref = a.ref AND d.sha = a.sha AND d.received_at >= a.received_at) \
     LIMIT 1";

const GROUP_RUNS: &str = "SELECT r.run_id, r.cell FROM ci_runs r \
     JOIN ci_events e ON e.id = r.event_id \
     WHERE r.repo_id = $1 AND e.kind = 'merge_group' AND e.ref = $2 AND e.sha = $3 \
       AND r.status NOT IN ('succeeded', 'failed', 'cancelled') \
     ORDER BY r.created_at, r.run_id";

const EVENT_VIEW: &str = "SELECT e.id, r.forge_host || '/' || r.full_name AS repo, r.cell, \
         e.kind, e.ref, e.sha, e.source, e.trust, e.actor, e.state, e.reason, \
         CAST(e.attempts AS BIGINT) AS attempts, e.pipeline, e.bundle_id, b.sha256 AS bundle_sha256 \
     FROM ci_events e \
     JOIN ci_repos r ON r.id = e.repo_id \
     LEFT JOIN ci_bundles b ON b.id = e.bundle_id \
     WHERE e.id = $1";

const RUNS_OF_EVENT: &str = "SELECT run_id, cell, pipeline, status, idempotency_key FROM ci_runs \
     WHERE event_id = $1 ORDER BY created_at, run_id";

// A run's admission slot goes when the run is recorded, in the same
// transaction, so the run is counted once throughout (see admission.rs).
const DELETE_ADMISSION: &str = "DELETE FROM ci_admissions WHERE idempotency_key = $1";

const LOCK_ADMISSIONS_PG: &str = "LOCK TABLE ci_admissions IN SHARE ROW EXCLUSIVE MODE";

const LAPSE_ADMISSIONS_PG: &str = "DELETE FROM ci_admissions WHERE expires_at <= now()";

// The first write of an admission on SQLite: it takes the database's one
// write lock, which the count and the slot are then taken under.
const LAPSE_ADMISSIONS_SQLITE: &str = "DELETE FROM ci_admissions \
     WHERE expires_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now')";

// The runs a limit counts: those dagron-ci submitted that the reporter still
// follows, and the slots of runs being submitted, but for the run asking.
const COUNT_ACTIVE: &str = "SELECT \
         (SELECT COUNT(*) FROM ci_runs WHERE status = 'submitted' AND report_due_at IS NOT NULL) \
       + (SELECT COUNT(*) FROM ci_admissions WHERE idempotency_key <> $1)";

// The repository of each run COUNT_ACTIVE counts, one row a run.
const ACTIVE_REPOS: &str = "SELECT r.forge_host || '/' || r.full_name \
     FROM ci_runs c JOIN ci_repos r ON r.id = c.repo_id \
     WHERE c.status = 'submitted' AND c.report_due_at IS NOT NULL \
     UNION ALL \
     SELECT COALESCE(repo, '') FROM ci_admissions WHERE idempotency_key <> $1";

// The caller of each run COUNT_ACTIVE counts, one row a run: a run's is its
// event's actor, a slot's its own.
const ACTIVE_CALLERS: &str = "SELECT COALESCE(e.actor, '') \
     FROM ci_runs c LEFT JOIN ci_events e ON e.id = c.event_id \
     WHERE c.status = 'submitted' AND c.report_due_at IS NOT NULL \
     UNION ALL \
     SELECT COALESCE(actor, '') FROM ci_admissions WHERE idempotency_key <> $1";

const TAKE_ADMISSION_PG: &str =
    "INSERT INTO ci_admissions (idempotency_key, expires_at, repo, actor) \
     VALUES ($1, now() + make_interval(secs => $2), $3, $4) \
     ON CONFLICT (idempotency_key) DO UPDATE SET expires_at = excluded.expires_at";

const TAKE_ADMISSION_SQLITE: &str =
    "INSERT INTO ci_admissions (idempotency_key, expires_at, repo, actor) \
     VALUES (?1, strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?2 || ' seconds'), ?3, ?4) \
     ON CONFLICT (idempotency_key) DO UPDATE SET expires_at = excluded.expires_at";

// A run is due for its commit statuses as soon as it is recorded: its first
// look posts `pending` for each of its jobs.
const INSERT_RUN_PG: &str = "INSERT INTO ci_runs \
     (run_id, event_id, repo_id, cell, pipeline, idempotency_key, frontend, report_due_at, on_failed) \
     VALUES ($1, $2, $3, $4, $5, $6, 'native', now(), $7) ON CONFLICT DO NOTHING";

const INSERT_RUN_SQLITE: &str = "INSERT INTO ci_runs \
     (run_id, event_id, repo_id, cell, pipeline, idempotency_key, frontend, report_due_at, on_failed) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, 'native', strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), ?7) \
     ON CONFLICT DO NOTHING";

// The claim takes the most overdue run and holds it for the lease, as a
// sweep's claim holds a repository: `report_due_at` moves to the lease's end,
// so a reporter that stops mid-look leaves the run due again then. A run
// no look of which was written yet (`report_looked`) comes first: its jobs are to read pending
// before they end, and a run told of a change is due as often as it
// changes, which is not to hold a new run's first look back.
const CLAIM_REPORT_PG: &str = "UPDATE ci_runs \
     SET report_claim = $1, report_due_at = now() + make_interval(secs => $2) \
     WHERE run_id = ( \
         SELECT run_id FROM ci_runs \
         WHERE report_due_at IS NOT NULL AND report_due_at <= now() \
         ORDER BY report_looked, report_due_at, run_id \
         LIMIT 1 \
         FOR UPDATE SKIP LOCKED) \
     RETURNING run_id";

const CLAIM_REPORT_SQLITE: &str = "UPDATE ci_runs \
     SET report_claim = ?1, \
         report_due_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?2 || ' seconds') \
     WHERE run_id = ( \
         SELECT run_id FROM ci_runs \
         WHERE report_due_at IS NOT NULL \
           AND report_due_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
         ORDER BY report_looked, report_due_at, run_id \
         LIMIT 1) \
     RETURNING run_id";

// A run's statuses, and the credential its checkout is given, are for the
// commit: the one an annotated tag names, where the event has it.
const REPORT_VIEW: &str = "SELECT r.run_id, r.cell AS run_cell, r.pipeline, r.reported, \
         r.reopens, r.jobs, e.kind, e.pr, COALESCE(e.commit_sha, e.sha) AS sha, \
         e.ref AS git_ref, e.head_ref, e.trust, e.id AS event_id, e.bundle_id, p.id, p.forge, \
         p.forge_host, p.full_name, p.workflow, p.cell, p.mode \
     FROM ci_runs r \
     JOIN ci_events e ON e.id = r.event_id \
     JOIN ci_repos p ON p.id = r.repo_id \
     WHERE r.run_id = $1";

const RUN_COMMIT: &str = "SELECT r.status, r.cell AS run_cell, e.trust, e.bundle_id, \
         COALESCE(e.commit_sha, e.sha) AS sha, p.id, p.forge, \
         p.forge_host, p.full_name, \
         p.workflow, p.cell, p.mode, \
         CAST(p.disconnected_at IS NULL AS INTEGER) AS connected \
     FROM ci_runs r \
     JOIN ci_events e ON e.id = r.event_id \
     JOIN ci_repos p ON p.id = r.repo_id \
     WHERE r.run_id = $1";

// Fenced on the claim, like a sweep's finish. A NULL interval leaves the run
// due never: `now() + NULL` is NULL, as is SQLite's date with a NULL modifier.
// A run's status moves once from `submitted` to how it ended: a later look
// (say, one that finds the run deleted while its last statuses were still
// being retried) does not rewrite it. It is `submitted` again only when the
// record re-opens (REOPEN_RUN), on a sight of the run running again.
// A run's end is due to its meter in the write that records it, and that
// write says when it was (`ended_at`): every SET reads the row as it was, so
// `status = 'submitted'` here means this write is the one that ends it.
// A batch of the reporter's looks ($1: a JSON array, a run each), each
// fenced on its claim, in one statement: the runs written come back.
const FINISH_REPORTS_PG: &str = "UPDATE ci_runs r \
     SET report_claim = NULL, \
         report_due_at = CASE WHEN r.report_told AND v.next IS NOT NULL \
             THEN now() + make_interval(secs => LEAST(v.hold, v.next)) \
             ELSE now() + make_interval(secs => v.next) END, \
         report_hold_until = now() + make_interval(secs => v.hold), \
         report_told = false, report_looked = true, \
         reported = v.reported, \
         jobs = COALESCE(v.jobs, r.jobs), \
         status = CASE WHEN r.status = 'submitted' THEN COALESCE(v.status, r.status) ELSE r.status END, \
         report_error = v.error, \
         started = CASE WHEN v.whole_reruns > r.whole_reruns THEN v.started \
             ELSE r.started OR v.started END, \
         whole_reruns = GREATEST(r.whole_reruns, COALESCE(v.whole_reruns, 0)), \
         meter_due_at = CASE WHEN v.meter AND r.status = 'submitted' AND v.status IS NOT NULL \
             THEN now() ELSE r.meter_due_at END, \
         ended_at = CASE WHEN r.status = 'submitted' AND v.status IS NOT NULL \
             THEN now() ELSE r.ended_at END, \
         failed_due_at = CASE WHEN r.on_failed AND r.status = 'submitted' AND v.status = 'failed' \
             THEN now() ELSE r.failed_due_at END \
     FROM jsonb_to_recordset($1::jsonb) AS v(run_id TEXT, claim TEXT, next FLOAT8, hold FLOAT8, reported TEXT, \
         status TEXT, error TEXT, started BOOLEAN, meter BOOLEAN, jobs TEXT, whole_reruns BIGINT) \
     WHERE r.run_id = v.run_id AND r.report_claim = v.claim \
     RETURNING r.run_id";

const FINISH_REPORTS_SQLITE: &str = "UPDATE ci_runs \
     SET report_claim = NULL, \
         report_due_at = CASE WHEN ci_runs.report_told AND v.next IS NOT NULL \
             THEN strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || min(v.hold, v.next) || ' seconds') \
             ELSE strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || v.next || ' seconds') END, \
         report_hold_until = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || v.hold || ' seconds'), \
         report_told = 0, report_looked = 1, \
         reported = v.reported, \
         jobs = COALESCE(v.jobs, ci_runs.jobs), \
         status = CASE WHEN ci_runs.status = 'submitted' THEN COALESCE(v.status, ci_runs.status) \
             ELSE ci_runs.status END, \
         report_error = v.error, \
         started = CASE WHEN v.whole_reruns > ci_runs.whole_reruns THEN v.started \
             ELSE MAX(ci_runs.started, v.started) END, \
         whole_reruns = MAX(ci_runs.whole_reruns, COALESCE(v.whole_reruns, 0)), \
         meter_due_at = CASE WHEN v.meter AND ci_runs.status = 'submitted' AND v.status IS NOT NULL \
             THEN strftime('%Y-%m-%dT%H:%M:%fZ', 'now') ELSE ci_runs.meter_due_at END, \
         ended_at = CASE WHEN ci_runs.status = 'submitted' AND v.status IS NOT NULL \
             THEN strftime('%Y-%m-%dT%H:%M:%fZ', 'now') ELSE ci_runs.ended_at END, \
         failed_due_at = CASE WHEN ci_runs.on_failed AND ci_runs.status = 'submitted' \
             AND v.status = 'failed' \
             THEN strftime('%Y-%m-%dT%H:%M:%fZ', 'now') ELSE ci_runs.failed_due_at END \
     FROM (SELECT json_extract(value, '$.run_id') AS run_id, json_extract(value, '$.claim') AS claim, \
         json_extract(value, '$.next') AS next, json_extract(value, '$.hold') AS hold, \
         json_extract(value, '$.reported') AS reported, \
         json_extract(value, '$.status') AS status, json_extract(value, '$.error') AS error, \
         json_extract(value, '$.started') AS started, json_extract(value, '$.meter') AS meter, \
         json_extract(value, '$.jobs') AS jobs, \
         json_extract(value, '$.whole_reruns') AS whole_reruns FROM json_each(?1)) AS v \
     WHERE ci_runs.run_id = v.run_id AND ci_runs.report_claim = v.claim \
     RETURNING ci_runs.run_id";

/// The least time between two looks of a run that a notice brings:
/// a run whose tasks change many times a second is looked at once for them.
pub const REPORT_HOLD: std::time::Duration = std::time::Duration::from_secs(2);

/// Make the records of the runs given (a JSON array of run ids) due now,
/// as their cell's outbox told of a change in each: one not in a
/// look's hands is due at once, or at its hold (`report_hold_until`); one
/// that is is told (`report_told`), and is due again as its look is
/// written, at its hold. A record that follows nothing more
/// (`report_due_at` NULL) is left as it is: a run running again is
/// re-opened instead.
const TELL_REPORTS_PG: &str = "UPDATE ci_runs \
     SET report_due_at = CASE WHEN report_claim IS NULL \
             THEN LEAST(report_due_at, GREATEST(now(), COALESCE(report_hold_until, now()))) \
             ELSE report_due_at END, \
         report_told = report_told OR report_claim IS NOT NULL \
     WHERE run_id IN (SELECT jsonb_array_elements_text($1::jsonb)) AND report_due_at IS NOT NULL";

const TELL_REPORTS_SQLITE: &str = "UPDATE ci_runs \
     SET report_due_at = CASE WHEN report_claim IS NULL \
             THEN min(report_due_at, max(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), \
                 COALESCE(report_hold_until, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')))) \
             ELSE report_due_at END, \
         report_told = report_told OR report_claim IS NOT NULL \
     WHERE run_id IN (SELECT value FROM json_each(?1)) AND report_due_at IS NOT NULL";

/// A patch bundle's row ([`crate::bundles`]).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Bundle {
    pub id: String,
    pub repo_id: String,
    pub base_sha: String,
    /// Who uploaded it: the caller's subject, the only one who may run it.
    pub principal: String,
    /// Its name in CI_OBJECT_STORE_URL.
    pub object_name: String,
    pub size_bytes: Option<i64>,
    pub sha256: Option<String>,
    /// Its bytes were checked: one uploaded through a link is not, until a
    /// run first names it.
    pub ready: bool,
    /// RFC 3339; empty before it is inserted.
    pub expires_at: String,
}

const INSERT_BUNDLE_PG: &str = "INSERT INTO ci_bundles \
     (id, repo_id, base_sha, principal, object_name, size_bytes, sha256, ready, expires_at) \
     VALUES ($1, $2, $3, $4, $5, $6, $7, $8, now() + make_interval(days => $9)) \
     RETURNING to_char(expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"')";

const INSERT_BUNDLE_SQLITE: &str = "INSERT INTO ci_bundles \
     (id, repo_id, base_sha, principal, object_name, size_bytes, sha256, ready, expires_at) \
     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, strftime('%Y-%m-%dT%H:%M:%SZ', 'now', '+' || ?9 || ' days')) \
     RETURNING expires_at";

const BUNDLE_PG: &str =
    "SELECT id, repo_id, base_sha, principal, object_name, size_bytes, sha256, \
     ready, to_char(expires_at AT TIME ZONE 'UTC', 'YYYY-MM-DD\"T\"HH24:MI:SS\"Z\"') AS expires_at \
     FROM ci_bundles WHERE id = $1 AND expires_at > now()";

const BUNDLE_SQLITE: &str =
    "SELECT id, repo_id, base_sha, principal, object_name, size_bytes, sha256, \
     ready, expires_at FROM ci_bundles \
     WHERE id = ?1 AND expires_at > strftime('%Y-%m-%dT%H:%M:%SZ', 'now')";

const EXPIRED_BUNDLES_PG: &str =
    "SELECT id, object_name FROM ci_bundles WHERE expires_at <= now() ORDER BY expires_at LIMIT $1";

const EXPIRED_BUNDLES_SQLITE: &str = "SELECT id, object_name FROM ci_bundles \
     WHERE expires_at <= strftime('%Y-%m-%dT%H:%M:%SZ', 'now') ORDER BY expires_at LIMIT ?1";

/// The report log's rows, given as a JSON array of [`ReportLog`]: one
/// statement for any number.
const LOG_REPORTS_PG: &str = "INSERT INTO ci_report_log \
     (event_id, run_id, repo_id, sha, context, state, description, target_url, outcome, detail) \
     SELECT event_id, run_id, repo_id, sha, context, state, description, target_url, outcome, detail \
     FROM jsonb_to_recordset($1::jsonb) AS r(event_id TEXT, run_id TEXT, repo_id TEXT, \
     sha TEXT, context TEXT, state TEXT, description TEXT, target_url TEXT, outcome TEXT, detail TEXT)";

const LOG_REPORTS_SQLITE: &str = "INSERT INTO ci_report_log \
     (event_id, run_id, repo_id, sha, context, state, description, target_url, outcome, detail) \
     SELECT json_extract(value, '$.event_id'), json_extract(value, '$.run_id'), \
     json_extract(value, '$.repo_id'), json_extract(value, '$.sha'), \
     json_extract(value, '$.context'), json_extract(value, '$.state'), \
     json_extract(value, '$.description'), json_extract(value, '$.target_url'), \
     json_extract(value, '$.outcome'), json_extract(value, '$.detail') \
     FROM json_each(?1)";

/// How long after a pull request's comment was written it is not written
/// again. A run's jobs change one after another, each a few seconds after
/// the last, and a forge counts every edit against what an App may write:
/// the comment says where the runs stand a few times a minute, and how they
/// ended within this long of their end.
pub(crate) const COMMENT_EVERY: Duration = Duration::from_secs(20);

// A pull request's comment is due once a run of it has changed: at once, or
// [`COMMENT_EVERY`] ($3, in seconds) after it was last written. One that is
// being written stays its writer's, who leaves it due (`again`); one that is
// waiting out a rate limit, or to be tried again, keeps its wait.
const COMMENT_DUE_PG: &str = "INSERT INTO ci_pr_comments (repo_id, pr, due_at) \
     VALUES ($1, $2, now()) \
     ON CONFLICT (repo_id, pr) DO UPDATE SET \
         due_at = COALESCE(ci_pr_comments.due_at, GREATEST(now(), \
             ci_pr_comments.written_at + make_interval(secs => $3))), \
         again = ci_pr_comments.claim IS NOT NULL";

// SQLite's MAX of two is NULL where either is: a comment never written is
// due now.
const COMMENT_DUE_SQLITE: &str = "INSERT INTO ci_pr_comments (repo_id, pr, due_at) \
     VALUES (?1, ?2, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) \
     ON CONFLICT (repo_id, pr) DO UPDATE SET \
         due_at = COALESCE(ci_pr_comments.due_at, \
             MAX(strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), \
                 strftime('%Y-%m-%dT%H:%M:%fZ', ci_pr_comments.written_at, \
                          '+' || ?3 || ' seconds')), \
             strftime('%Y-%m-%dT%H:%M:%fZ', 'now')), \
         again = ci_pr_comments.claim IS NOT NULL";

// As a run is claimed for its statuses: the most overdue, held to the end
// of a lease, which is when another writer may take it.
const CLAIM_COMMENT_PG: &str = "UPDATE ci_pr_comments c \
     SET claim = $1, again = FALSE, due_at = now() + make_interval(secs => $2) \
     FROM (SELECT repo_id, pr FROM ci_pr_comments \
           WHERE due_at IS NOT NULL AND due_at <= now() \
           ORDER BY due_at, repo_id, pr \
           LIMIT 1 \
           FOR UPDATE SKIP LOCKED) due \
     WHERE c.repo_id = due.repo_id AND c.pr = due.pr \
     RETURNING c.repo_id, c.pr, c.comment_id, c.said";

const CLAIM_COMMENT_SQLITE: &str = "UPDATE ci_pr_comments \
     SET claim = ?1, again = 0, \
         due_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?2 || ' seconds') \
     WHERE rowid = ( \
         SELECT rowid FROM ci_pr_comments \
         WHERE due_at IS NOT NULL \
           AND due_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
         ORDER BY due_at, repo_id, pr \
         LIMIT 1) \
     RETURNING repo_id, pr, comment_id, said";

// Fenced on the claim. $4 is the wait the writer asks for, and $7 the wait
// of a comment a run changed while it was written: [`COMMENT_EVERY`] where
// this write was made, and none where it was not. With neither, a NULL
// leaves the comment due never, as it leaves a run.
const FINISH_COMMENT_PG: &str = "UPDATE ci_pr_comments \
     SET claim = NULL, \
         due_at = CASE WHEN again AND $4::DOUBLE PRECISION IS NULL \
                       THEN now() + make_interval(secs => $7) \
                       ELSE now() + make_interval(secs => $4) END, \
         again = FALSE, said = COALESCE($5, said), error = $6, \
         written_at = CASE WHEN $5 IS NULL THEN written_at ELSE now() END \
     WHERE repo_id = $1 AND pr = $2 AND claim = $3";

const FINISH_COMMENT_SQLITE: &str = "UPDATE ci_pr_comments \
     SET claim = NULL, \
         due_at = CASE WHEN again AND ?4 IS NULL \
                       THEN strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?7 || ' seconds') \
                       ELSE strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?4 || ' seconds') END, \
         again = 0, said = COALESCE(?5, said), error = ?6, \
         written_at = CASE WHEN ?5 IS NULL THEN written_at \
                           ELSE strftime('%Y-%m-%dT%H:%M:%fZ', 'now') END \
     WHERE repo_id = ?1 AND pr = ?2 AND claim = ?3";

// Not fenced: a comment that was made is on the pull request whoever holds
// the row by now, and one made beside it would be a second.
const COMMENT_MADE: &str =
    "UPDATE ci_pr_comments SET comment_id = $3 WHERE repo_id = $1 AND pr = $2";

// A job of a run that flaked, once a run: of a trusted run alone, and of no
// patch bundle's, so that what a stranger wrote files nothing. Its
// repository and pipeline, where it was new.
const INSERT_FLAKE: &str =
    "INSERT INTO ci_flakes (run_id, job, repo_id, pipeline, cell, sha, failed) \
     SELECT r.run_id, $2, r.repo_id, r.pipeline, r.cell, COALESCE(e.commit_sha, e.sha), $3 \
     FROM ci_runs r \
     JOIN ci_events e ON e.id = r.event_id \
     WHERE r.run_id = $1 AND e.trust = 'trusted' AND e.bundle_id IS NULL \
     ON CONFLICT (run_id, job) DO NOTHING \
     RETURNING repo_id, pipeline";

// The flakes past the days they are counted for, of every job.
const PRUNE_FLAKES_PG: &str =
    "DELETE FROM ci_flakes WHERE seen_at < now() - make_interval(days => CAST($1 AS INT))";
const PRUNE_FLAKES_SQLITE: &str = "DELETE FROM ci_flakes \
     WHERE seen_at < strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-' || ?1 || ' days')";

// A job's flakes since its last issue was closed, if one was.
const FLAKES_SINCE: &str = "FROM ci_flakes f \
     LEFT JOIN ci_flake_issues i \
         ON i.repo_id = f.repo_id AND i.pipeline = f.pipeline AND i.job = f.job \
     WHERE f.repo_id = $1 AND f.pipeline = $2 AND f.job = $3 \
       AND (i.since IS NULL OR f.seen_at > i.since)";

// A job's issue made due. One a writer holds is left to it, told to be
// due again once it is done; one waiting already keeps its time.
const FLAKE_DUE_PG: &str = "INSERT INTO ci_flake_issues (repo_id, pipeline, job, due_at) \
     VALUES ($1, $2, $3, now()) \
     ON CONFLICT (repo_id, pipeline, job) DO UPDATE SET \
         due_at = COALESCE(ci_flake_issues.due_at, now()), \
         again = ci_flake_issues.claim IS NOT NULL";
const FLAKE_DUE_SQLITE: &str = "INSERT INTO ci_flake_issues (repo_id, pipeline, job, due_at) \
     VALUES (?1, ?2, ?3, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) \
     ON CONFLICT (repo_id, pipeline, job) DO UPDATE SET \
         due_at = COALESCE(ci_flake_issues.due_at, strftime('%Y-%m-%dT%H:%M:%fZ', 'now')), \
         again = ci_flake_issues.claim IS NOT NULL";

// As a pull request's comment is claimed: the most overdue, held to the end
// of a lease.
const CLAIM_FLAKE_ISSUE_PG: &str = "UPDATE ci_flake_issues c \
     SET claim = $1, again = FALSE, due_at = now() + make_interval(secs => $2) \
     FROM (SELECT repo_id, pipeline, job FROM ci_flake_issues \
           WHERE due_at IS NOT NULL AND due_at <= now() \
           ORDER BY due_at, repo_id, pipeline, job \
           LIMIT 1 \
           FOR UPDATE SKIP LOCKED) due \
     WHERE c.repo_id = due.repo_id AND c.pipeline = due.pipeline AND c.job = due.job \
     RETURNING c.repo_id, c.pipeline, c.job, c.issue, c.said";
const CLAIM_FLAKE_ISSUE_SQLITE: &str = "UPDATE ci_flake_issues \
     SET claim = ?1, again = 0, \
         due_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?2 || ' seconds') \
     WHERE rowid = ( \
         SELECT rowid FROM ci_flake_issues \
         WHERE due_at IS NOT NULL \
           AND due_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
         ORDER BY due_at, repo_id, pipeline, job \
         LIMIT 1) \
     RETURNING repo_id, pipeline, job, issue, said";

// Fenced on the claim. A flake while it was written leaves it due at once;
// else $5 is the wait the writer asks for, and NULL leaves it due never.
const FINISH_FLAKE_ISSUE_PG: &str = "UPDATE ci_flake_issues \
     SET claim = NULL, \
         due_at = CASE WHEN again THEN now() \
                       ELSE now() + make_interval(secs => $5) END, \
         again = FALSE, said = COALESCE($6, said), error = $7 \
     WHERE repo_id = $1 AND pipeline = $2 AND job = $3 AND claim = $4";
const FINISH_FLAKE_ISSUE_SQLITE: &str = "UPDATE ci_flake_issues \
     SET claim = NULL, \
         due_at = CASE WHEN again THEN strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                       ELSE strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?5 || ' seconds') END, \
         again = 0, said = COALESCE(?6, said), error = ?7 \
     WHERE repo_id = ?1 AND pipeline = ?2 AND job = ?3 AND claim = ?4";

// Not fenced, as a comment's id is not: an issue that was opened is on the
// forge whoever holds the row by now.
const FLAKE_ISSUE_MADE: &str = "UPDATE ci_flake_issues SET issue = $4 \
     WHERE repo_id = $1 AND pipeline = $2 AND job = $3";

// An issue a person closed: none is open, and the job's flakes are counted
// from when it was closed.
const FLAKE_ISSUE_CLOSED_PG: &str = "UPDATE ci_flake_issues \
     SET issue = NULL, said = NULL, since = COALESCE(CAST($4 AS TIMESTAMPTZ), now()) \
     WHERE repo_id = $1 AND pipeline = $2 AND job = $3";
const FLAKE_ISSUE_CLOSED_SQLITE: &str = "UPDATE ci_flake_issues \
     SET issue = NULL, said = NULL, \
         since = COALESCE(strftime('%Y-%m-%dT%H:%M:%fZ', ?4), strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) \
     WHERE repo_id = ?1 AND pipeline = ?2 AND job = ?3";

// A pull request's runs, its latest event's first. An event names the
// commit, so the runs of the latest commit are those of the first row's.
const PR_RUNS: &str = "SELECT r.run_id, r.cell AS run_cell, r.pipeline, r.status, r.jobs, \
         COALESCE(e.commit_sha, e.sha) AS sha \
     FROM ci_runs r \
     JOIN ci_events e ON e.id = r.event_id \
     WHERE r.repo_id = $1 AND e.repo_id = $1 AND e.pr = $2 AND e.kind = 'pull_request' \
     ORDER BY e.received_at DESC, e.id DESC, r.pipeline, r.run_id \
     LIMIT 200";

// A record re-opens when its run is seen running again: `submitted` once
// more, due for a look at once, and so again a run a checkout credential is
// minted for, a run that holds a slot, and one whose statuses are followed.
// What it posted stays (`reported`), so only what changes is posted again;
// and `reopens` counts this, by which the reporter knows a job's failure
// after it from the same failure before it.
//
// Only an ended record, and only on a sight later than its end: $2 is how
// long ago the run was seen running, and a sight from before the end says
// nothing of after it. A record that ended before `ended_at` was kept has
// none, and re-opens on any sight.
const REOPEN_RUN_PG: &str = "UPDATE ci_runs \
     SET status = 'submitted', report_due_at = now(), report_claim = NULL, \
         report_error = NULL, ended_at = NULL, reopens = reopens + 1 \
     WHERE run_id = $1 AND status <> 'submitted' \
       AND (ended_at IS NULL OR ended_at <= now() - make_interval(secs => $2))";

const REOPEN_RUN_SQLITE: &str = "UPDATE ci_runs \
     SET status = 'submitted', report_due_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), \
         report_claim = NULL, report_error = NULL, ended_at = NULL, \
         reopens = reopens + 1 \
     WHERE run_id = ?1 AND status <> 'submitted' \
       AND (ended_at IS NULL \
            OR ended_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-' || ?2 || ' seconds'))";

// Of these runs, the ones on record as over.
const ENDED_AMONG_PG: &str =
    "SELECT run_id FROM ci_runs WHERE status <> 'submitted' AND run_id = ANY($1)";

// The most overdue end due to the meter, held for a lease: told again once
// the lease is out unless the meter took it.
const CLAIM_METER_END_PG: &str = "UPDATE ci_runs \
     SET meter_due_at = now() + make_interval(secs => $1) \
     WHERE run_id = ( \
         SELECT run_id FROM ci_runs \
         WHERE meter_due_at IS NOT NULL AND meter_due_at <= now() \
         ORDER BY meter_due_at, run_id \
         LIMIT 1 \
         FOR UPDATE SKIP LOCKED) \
     RETURNING run_id";

const CLAIM_METER_END_SQLITE: &str = "UPDATE ci_runs \
     SET meter_due_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?1 || ' seconds') \
     WHERE run_id = ( \
         SELECT run_id FROM ci_runs \
         WHERE meter_due_at IS NOT NULL \
           AND meter_due_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
         ORDER BY meter_due_at, run_id \
         LIMIT 1) \
     RETURNING run_id";

const METER_END_VIEW: &str = "SELECT r.run_id, r.cell, r.pipeline, r.status, \
         CAST(r.started AS INTEGER) AS started, COALESCE(e.kind, '') AS kind, \
         CAST(r.whole_reruns AS BIGINT) AS whole_reruns, \
         p.forge_host || '/' || p.full_name AS repo \
     FROM ci_runs r \
     JOIN ci_repos p ON p.id = r.repo_id \
     LEFT JOIN ci_events e ON e.id = r.event_id \
     WHERE r.run_id = $1";

const METER_TOLD: &str = "UPDATE ci_runs SET meter_due_at = NULL WHERE run_id = $1";

// A failed run whose `failed` event is due, held for a lease as a
// meter's end is; the lease's end is what raising it is fenced on.
const CLAIM_FAILURE_PG: &str = "UPDATE ci_runs \
     SET failed_due_at = now() + make_interval(secs => $1) \
     WHERE run_id = ( \
         SELECT run_id FROM ci_runs \
         WHERE failed_due_at IS NOT NULL AND failed_due_at <= now() \
         ORDER BY failed_due_at, run_id \
         LIMIT 1 \
         FOR UPDATE SKIP LOCKED) \
     RETURNING run_id, cell, pipeline, CAST(failed_due_at AS TEXT) AS until";

const CLAIM_FAILURE_SQLITE: &str = "UPDATE ci_runs \
     SET failed_due_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?1 || ' seconds') \
     WHERE run_id = ( \
         SELECT run_id FROM ci_runs \
         WHERE failed_due_at IS NOT NULL \
           AND failed_due_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
         ORDER BY failed_due_at, run_id \
         LIMIT 1) \
     RETURNING run_id, cell, pipeline, failed_due_at AS until";

const FAILURE_RAISED_PG: &str = "UPDATE ci_runs SET failed_due_at = NULL \
     WHERE run_id = $1 AND CAST(failed_due_at AS TEXT) = $2";

const FAILURE_RAISED_SQLITE: &str = "UPDATE ci_runs SET failed_due_at = NULL \
     WHERE run_id = ?1 AND failed_due_at = ?2";

// The `failed` event of a run: of the failed run's event's commit, ref, pull
// request, actor and trust, by the poll's door, keyed by the run and how many
// times it has run again, so that each of its failures raises one.
const INSERT_FAILED: &str = "INSERT INTO ci_events \
     (id, repo_id, source, kind, ref, sha, base_sha, base_ref, pr, actor, trust, commit_key, \
      inputs, head_ref, commit_sha) \
     SELECT $2, e.repo_id, 'poll', 'failed', e.ref, e.sha, e.base_sha, e.base_ref, e.pr, \
         e.actor, e.trust, 'failed:' || r.run_id || ':' || CAST(r.reopens AS TEXT), $3, \
         e.head_ref, e.commit_sha \
     FROM ci_runs r JOIN ci_events e ON e.id = r.event_id \
     WHERE r.run_id = $1";

const LIST_EVENTS: &str =
    "SELECT e.id, r.forge_host || '/' || r.full_name AS repo, e.kind, e.ref, \
         e.sha, e.state, e.attempts, e.reason \
     FROM ci_events e JOIN ci_repos r ON r.id = e.repo_id \
     WHERE $1 = '' OR e.state = $1 \
     ORDER BY e.received_at DESC, e.id DESC LIMIT $2";

// A disconnect keeps the row, and with it the repository's events and runs.
const DISCONNECT_REPO_PG: &str = "UPDATE ci_repos SET disconnected_at = now(), poll_claim = NULL \
     WHERE forge_host = $1 AND full_name = $2 AND disconnected_at IS NULL RETURNING id";

const DISCONNECT_REPO_SQLITE: &str = "UPDATE ci_repos \
     SET disconnected_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), poll_claim = NULL \
     WHERE forge_host = ?1 AND full_name = ?2 AND disconnected_at IS NULL RETURNING id";

/// A changed interval applies at once: the repository is due now.
const SET_POLL_SECS: &str = "UPDATE ci_repos SET poll_secs = $3, poll_due_at = NULL \
     WHERE forge_host = $1 AND full_name = $2 AND disconnected_at IS NULL";

// The claim takes the most overdue repository that polls (its own interval,
// or CI_POLL_SECS's, is above 0, and it is not in fleet mode, whose forge
// runs its pipelines) and holds it for the lease: `poll_due_at` moves to the
// lease's end, so a sweeper that stops mid-sweep leaves the repository due
// again then.
const CLAIM_SWEEP_PG: &str = "UPDATE ci_repos \
     SET poll_claim = $1, poll_due_at = now() + make_interval(secs => $2) \
     WHERE id = ( \
         SELECT id FROM ci_repos \
         WHERE disconnected_at IS NULL AND mode <> 'fleet' \
           AND COALESCE(poll_secs, $3) > 0 AND (poll_due_at IS NULL OR poll_due_at <= now()) \
         ORDER BY poll_due_at NULLS FIRST, id \
         LIMIT 1 \
         FOR UPDATE SKIP LOCKED) \
     RETURNING id, forge, forge_host, full_name, workflow, cell, mode, \
         COALESCE(poll_secs, $3)::BIGINT AS poll_secs, polled_at IS NULL AS baseline, poll_etag";

const CLAIM_SWEEP_SQLITE: &str = "UPDATE ci_repos \
     SET poll_claim = ?1, \
         poll_due_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?2 || ' seconds') \
     WHERE id = ( \
         SELECT id FROM ci_repos \
         WHERE disconnected_at IS NULL AND mode <> 'fleet' AND COALESCE(poll_secs, ?3) > 0 \
           AND (poll_due_at IS NULL OR poll_due_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) \
         ORDER BY poll_due_at NULLS FIRST, id \
         LIMIT 1) \
     RETURNING id, forge, forge_host, full_name, workflow, cell, mode, \
         COALESCE(poll_secs, ?3) AS poll_secs, polled_at IS NULL AS baseline, poll_etag";

const POLL_REFS: &str = "SELECT ref, sha FROM ci_poll_refs WHERE repo_id = $1";

const SET_POLL_REF: &str = "INSERT INTO ci_poll_refs (repo_id, ref, sha) VALUES ($1, $2, $3) \
     ON CONFLICT (repo_id, ref) DO UPDATE SET sha = excluded.sha";

const DROP_POLL_REF: &str = "DELETE FROM ci_poll_refs WHERE repo_id = $1 AND ref = $2";

// Both ends of a sweep are fenced on its claim: a sweeper whose repository
// another took over changes nothing.
const FINISH_SWEEP_PG: &str = "UPDATE ci_repos \
     SET poll_claim = NULL, poll_due_at = now() + make_interval(secs => $3), \
         polled_at = now(), poll_etag = $4, poll_error = NULL \
     WHERE id = $1 AND poll_claim = $2";

const FINISH_SWEEP_SQLITE: &str = "UPDATE ci_repos \
     SET poll_claim = NULL, \
         poll_due_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?3 || ' seconds'), \
         polled_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now'), poll_etag = ?4, poll_error = NULL \
     WHERE id = ?1 AND poll_claim = ?2";

// The scheduler's claim, as the sweep's ([`CLAIM_SWEEP_PG`]): of a GitHub
// repository, the forge whose workflow files dagron-ci reads, that is in
// compat mode, the one whose workflow files are read.
const CLAIM_SCHEDULE_PG: &str = "UPDATE ci_repos \
     SET schedule_claim = $1, schedule_due_at = now() + make_interval(secs => $2) \
     WHERE id = ( \
         SELECT id FROM ci_repos \
         WHERE disconnected_at IS NULL AND forge = 'github' AND mode = 'compat' \
           AND (schedule_due_at IS NULL OR schedule_due_at <= now()) \
         ORDER BY schedule_due_at NULLS FIRST, id \
         LIMIT 1 \
         FOR UPDATE SKIP LOCKED) \
     RETURNING id, forge, forge_host, full_name, workflow, cell, mode, schedule_sha";

const CLAIM_SCHEDULE_SQLITE: &str = "UPDATE ci_repos \
     SET schedule_claim = ?1, \
         schedule_due_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?2 || ' seconds') \
     WHERE id = ( \
         SELECT id FROM ci_repos \
         WHERE disconnected_at IS NULL AND forge = 'github' AND mode = 'compat' \
           AND (schedule_due_at IS NULL \
                OR schedule_due_at <= strftime('%Y-%m-%dT%H:%M:%fZ', 'now')) \
         ORDER BY schedule_due_at NULLS FIRST, id \
         LIMIT 1) \
     RETURNING id, forge, forge_host, full_name, workflow, cell, mode, schedule_sha";

const SCHEDULES: &str = "SELECT workflow, cron, CAST(next_at AS BIGINT), CAST(last_at AS BIGINT) \
     FROM ci_schedules WHERE repo_id = $1 ORDER BY workflow, cron";

const DROP_SCHEDULES: &str = "DELETE FROM ci_schedules WHERE repo_id = $1";

const SET_SCHEDULE: &str = "INSERT INTO ci_schedules (repo_id, workflow, cron, next_at, last_at) \
     VALUES ($1, $2, $3, $4, $5)";

// A schedule's event: trusted, at the default branch's head, naming its
// workflow file, its cron its one input. It comes in by the poll's door.
const INSERT_SCHEDULED: &str = "INSERT INTO ci_events \
     (id, repo_id, source, kind, ref, sha, trust, commit_key, inputs, pipeline) \
     VALUES ($1, $2, 'poll', 'schedule', $3, $4, 'trusted', $5, $6, $7)";

// Both ends of a look are fenced on its claim, as a sweep's are.
const FINISH_SCHEDULE_PG: &str = "UPDATE ci_repos \
     SET schedule_claim = NULL, schedule_due_at = now() + make_interval(secs => $3), \
         schedule_sha = $4, schedule_error = NULL \
     WHERE id = $1 AND schedule_claim = $2";

const FINISH_SCHEDULE_SQLITE: &str = "UPDATE ci_repos \
     SET schedule_claim = NULL, \
         schedule_due_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?3 || ' seconds'), \
         schedule_sha = ?4, schedule_error = NULL \
     WHERE id = ?1 AND schedule_claim = ?2";

const FAIL_SCHEDULE_PG: &str = "UPDATE ci_repos \
     SET schedule_claim = NULL, schedule_due_at = now() + make_interval(secs => $3), \
         schedule_error = $4 \
     WHERE id = $1 AND schedule_claim = $2";

const FAIL_SCHEDULE_SQLITE: &str = "UPDATE ci_repos \
     SET schedule_claim = NULL, \
         schedule_due_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?3 || ' seconds'), \
         schedule_error = ?4 \
     WHERE id = ?1 AND schedule_claim = ?2";

const FAIL_SWEEP_PG: &str = "UPDATE ci_repos \
     SET poll_claim = NULL, poll_due_at = now() + make_interval(secs => $3), poll_error = $4 \
     WHERE id = $1 AND poll_claim = $2";

const FAIL_SWEEP_SQLITE: &str = "UPDATE ci_repos \
     SET poll_claim = NULL, \
         poll_due_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?3 || ' seconds'), \
         poll_error = ?4 \
     WHERE id = ?1 AND poll_claim = ?2";

const BACKLOG_PG: &str = "SELECT count(*)::BIGINT, \
     EXTRACT(EPOCH FROM (now() - min(received_at)))::DOUBLE PRECISION \
     FROM ci_events WHERE state = 'new'";

const BACKLOG_SQLITE: &str = "SELECT count(*), \
     (julianday('now') - julianday(min(received_at))) * 86400.0 \
     FROM ci_events WHERE state = 'new'";

impl Db {
    /// Connect to `CI_DATABASE_URL` and bring its schema up to date.
    pub async fn connect(config: &Config) -> anyhow::Result<Db> {
        let url = &config.database_url;
        if is_postgres(url) {
            let schema = &config.database_schema;
            let options = PgConnectOptions::from_str(url)
                .context("CI_DATABASE_URL is not a Postgres URL sqlx can read")?
                .options([("search_path", schema.as_str())]);
            let pool = PgPoolOptions::new()
                .max_connections(config.database_max_connections)
                .acquire_timeout(ACQUIRE_TIMEOUT)
                .connect_with(options)
                .await
                .context("cannot connect to CI_DATABASE_URL")?;
            create_schema(&pool, schema).await?;
            migrate_pg(&pool)
                .await
                .context("migrating dagron-ci's Postgres schema")?;
            Ok(Db::Postgres(pool))
        } else {
            let options = SqliteConnectOptions::from_str(url)
                .context("CI_DATABASE_URL is not a SQLite URL sqlx can read")?
                .create_if_missing(true)
                .journal_mode(SqliteJournalMode::Wal)
                .busy_timeout(ACQUIRE_TIMEOUT)
                .foreign_keys(true);
            let pool = SqlitePoolOptions::new()
                .max_connections(config.database_max_connections)
                .acquire_timeout(ACQUIRE_TIMEOUT)
                .connect_with(options)
                .await
                .context("cannot open CI_DATABASE_URL")?;
            MIGRATIONS_SQLITE
                .run(&pool)
                .await
                .context("migrating dagron-ci's SQLite database")?;
            Ok(Db::Sqlite(pool))
        }
    }

    /// One round trip, for `/readyz`.
    pub async fn ping(&self) -> sqlx::Result<()> {
        match self {
            Db::Postgres(p) => sqlx::query("SELECT 1").execute(p).await.map(drop),
            Db::Sqlite(p) => sqlx::query("SELECT 1").execute(p).await.map(drop),
        }
    }

    /// The connected repository `full_name` on `forge_host`, whatever the
    /// case it is asked for in: a forge takes `Acme/Widgets` for
    /// `acme/widgets`, and a repository is kept in lower case.
    pub async fn find_repo(&self, forge_host: &str, full_name: &str) -> sqlx::Result<Option<Repo>> {
        let (forge_host, full_name) = (
            forge_host.to_ascii_lowercase(),
            full_name.to_ascii_lowercase(),
        );
        let row = match self {
            Db::Postgres(p) => sqlx::query(FIND_REPO)
                .bind(&forge_host)
                .bind(&full_name)
                .fetch_optional(p)
                .await?
                .map(|r| repo_pg(&r)),
            Db::Sqlite(p) => sqlx::query(&lite(FIND_REPO))
                .bind(&forge_host)
                .bind(&full_name)
                .fetch_optional(p)
                .await?
                .map(|r| repo_sqlite(&r)),
        };
        row.transpose()
    }

    /// Whether dagron-ci serves the repository `full_name` on `forge_host`:
    /// whether it is connected (`dagron-ci repo add`), whatever the case it
    /// is written in.
    /// The forge's payload of the event `event_id`, as its webhook
    /// delivered it, where it was kept: in its row, or in the store for one
    /// over [`PAYLOAD_INLINE_MAX`]; none for one over it that no store kept,
    /// and none for an event dagron-ci made itself, which keeps `{}`.
    pub async fn event_payload(&self, event_id: &str) -> sqlx::Result<Option<EventPayload>> {
        let found: Option<(Option<String>, Option<String>)> = match self {
            Db::Postgres(p) => {
                sqlx::query_as(SELECT_EVENT_PAYLOAD)
                    .bind(event_id)
                    .fetch_optional(p)
                    .await?
            }
            Db::Sqlite(p) => {
                sqlx::query_as(&lite(SELECT_EVENT_PAYLOAD))
                    .bind(event_id)
                    .fetch_optional(p)
                    .await?
            }
        };
        Ok(match found {
            Some((_, Some(stored))) => Some(EventPayload::Stored(stored)),
            Some((Some(inline), None)) if inline.trim() != "{}" => {
                Some(EventPayload::Inline(inline))
            }
            _ => None,
        })
    }

    /// The run of `repo_id` that its workflow's `github.run_id` names, and
    /// its cell: what dagron-ci gave it as it made it ([`Db::run_numbers`]).
    pub async fn run_numbered(
        &self,
        repo_id: &str,
        github_run_id: i64,
    ) -> sqlx::Result<Option<(String, String)>> {
        match self {
            Db::Postgres(p) => {
                sqlx::query_as(SELECT_RUN_NUMBERED)
                    .bind(repo_id)
                    .bind(github_run_id)
                    .fetch_optional(p)
                    .await
            }
            Db::Sqlite(p) => {
                sqlx::query_as(&lite(SELECT_RUN_NUMBERED))
                    .bind(repo_id)
                    .bind(github_run_id)
                    .fetch_optional(p)
                    .await
            }
        }
    }

    /// The repository (its id) and the cell of the run `run_id`, where it is
    /// one dagron-ci submitted.
    pub async fn run_place(&self, run_id: &str) -> sqlx::Result<Option<(String, String)>> {
        match self {
            Db::Postgres(p) => {
                sqlx::query_as(SELECT_RUN_PLACE)
                    .bind(run_id)
                    .fetch_optional(p)
                    .await
            }
            Db::Sqlite(p) => {
                sqlx::query_as(&lite(SELECT_RUN_PLACE))
                    .bind(run_id)
                    .fetch_optional(p)
                    .await
            }
        }
    }

    /// The run and the repository URL of a credential dagron-ci minted,
    /// has not revoked, and that has not lapsed as the forge said it would,
    /// by its SHA-256, with when it lapses: a job's `GITHUB_TOKEN` among them.
    pub async fn live_credential(
        &self,
        token_sha256: &str,
    ) -> sqlx::Result<Option<(String, String, Option<String>)>> {
        match self {
            Db::Postgres(p) => {
                sqlx::query_as(SELECT_LIVE_CREDENTIAL_PG)
                    .bind(token_sha256)
                    .fetch_optional(p)
                    .await
            }
            Db::Sqlite(p) => {
                sqlx::query_as(SELECT_LIVE_CREDENTIAL_SQLITE)
                    .bind(token_sha256)
                    .fetch_optional(p)
                    .await
            }
        }
    }

    pub async fn serves_repo(&self, forge_host: &str, full_name: &str) -> sqlx::Result<bool> {
        Ok(match self {
            Db::Postgres(p) => sqlx::query(SERVES_REPO)
                .bind(forge_host)
                .bind(full_name)
                .fetch_optional(p)
                .await?
                .is_some(),
            Db::Sqlite(p) => sqlx::query(&lite(SERVES_REPO))
                .bind(forge_host)
                .bind(full_name)
                .fetch_optional(p)
                .await?
                .is_some(),
        })
    }

    pub async fn list_repos(&self) -> sqlx::Result<Vec<RepoListing>> {
        macro_rules! listing {
            ($row:expr, $repo:expr) => {
                Ok(RepoListing {
                    repo: $repo,
                    poll_secs: $row.try_get("poll_secs")?,
                    polled_at: $row.try_get("polled_at")?,
                    poll_error: $row.try_get("poll_error")?,
                })
            };
        }
        match self {
            Db::Postgres(p) => sqlx::query(LIST_REPOS)
                .fetch_all(p)
                .await?
                .iter()
                .map(|r| listing!(r, repo_pg(r)?))
                .collect(),
            Db::Sqlite(p) => sqlx::query(LIST_REPOS)
                .fetch_all(p)
                .await?
                .iter()
                .map(|r| listing!(r, repo_sqlite(r)?))
                .collect(),
        }
    }

    /// Connect a repository, running `workflow` on `cell` for its events,
    /// in compat mode, as a repository connected before there were modes is
    /// served: the forge's own files are read wherever the build reads them.
    /// `Ok(None)` when it is connected already. Recorded as nobody's
    /// ([`LOCAL`]): [`Self::connect_repo`] names who, and the mode.
    pub async fn add_repo(
        &self,
        forge: &str,
        forge_host: &str,
        full_name: &str,
        workflow: Option<&str>,
        cell: &str,
    ) -> sqlx::Result<Option<Repo>> {
        Ok(
            match self
                .connect_repo(
                    forge,
                    forge_host,
                    full_name,
                    workflow,
                    cell,
                    None,
                    Mode::Compat,
                    LOCAL,
                )
                .await?
            {
                Connected::New(repo) | Connected::Again(repo) => Some(repo),
                Connected::Already => None,
            },
        )
    }

    /// Connect a repository, running `workflow` on `cell` for its events,
    /// polled every `poll` seconds (never for 0, CI_POLL_SECS's interval for
    /// `None`) and served as `mode` says, and record that `actor` did, in one
    /// transaction. One that was disconnected is connected again as the row
    /// it was.
    #[allow(clippy::too_many_arguments)]
    pub async fn connect_repo(
        &self,
        forge: &str,
        forge_host: &str,
        full_name: &str,
        workflow: Option<&str>,
        cell: &str,
        poll: Option<u64>,
        mode: Mode,
        actor: &str,
    ) -> sqlx::Result<Connected> {
        let secs = poll.map(|s| s.min(i32::MAX as u64) as i32);
        let mut repo = Repo {
            id: uuid::Uuid::now_v7().to_string(),
            forge: forge.to_string(),
            forge_host: forge_host.to_ascii_lowercase(),
            full_name: full_name.to_ascii_lowercase(),
            workflow: workflow.map(str::to_string),
            cell: cell.to_string(),
            mode,
        };
        let detail = serde_json::json!({
            "forge": forge, "workflow": workflow, "cell": cell, "poll_secs": secs,
            "mode": mode.as_str(),
        })
        .to_string();
        let change = uuid::Uuid::now_v7().to_string();
        macro_rules! connect {
            ($pool:expr, $insert:expr, $record:expr) => {{
                let mut tx = $pool.begin().await?;
                let id: Option<String> = sqlx::query_scalar($insert)
                    .bind(&repo.id)
                    .bind(&repo.forge)
                    .bind(&repo.forge_host)
                    .bind(&repo.full_name)
                    .bind(repo.workflow.as_deref())
                    .bind(&repo.cell)
                    .bind(secs)
                    .bind(repo.mode.as_str())
                    .fetch_optional(&mut *tx)
                    .await?;
                if let Some(id) = &id {
                    let action = if id == &repo.id {
                        "connected"
                    } else {
                        "reconnected"
                    };
                    sqlx::query($record)
                        .bind(&change)
                        .bind(id)
                        .bind(actor)
                        .bind(action)
                        .bind(&detail)
                        .execute(&mut *tx)
                        .await?;
                }
                tx.commit().await?;
                id
            }};
        }
        let id = match self {
            Db::Postgres(p) => connect!(p, INSERT_REPO, INSERT_REPO_CHANGE),
            Db::Sqlite(p) => connect!(p, &lite(INSERT_REPO), &lite(INSERT_REPO_CHANGE)),
        };
        Ok(match id {
            None => Connected::Already,
            Some(id) if id == repo.id => Connected::New(repo),
            Some(id) => {
                repo.id = id;
                Connected::Again(repo)
            }
        })
    }

    /// A repository by its name, connected or not, and since when it is not.
    pub async fn repo_any(
        &self,
        forge_host: &str,
        full_name: &str,
    ) -> sqlx::Result<Option<(Repo, Option<String>)>> {
        let (host, name) = (
            forge_host.to_ascii_lowercase(),
            full_name.to_ascii_lowercase(),
        );
        Ok(match self {
            Db::Postgres(p) => match sqlx::query(REPO_ANY)
                .bind(&host)
                .bind(&name)
                .fetch_optional(p)
                .await?
            {
                Some(row) => Some((repo_pg(&row)?, row.try_get("disconnected_at")?)),
                None => None,
            },
            Db::Sqlite(p) => match sqlx::query(&lite(REPO_ANY))
                .bind(&host)
                .bind(&name)
                .fetch_optional(p)
                .await?
            {
                Some(row) => Some((repo_sqlite(&row)?, row.try_get("disconnected_at")?)),
                None => None,
            },
        })
    }

    /// A repository by its id, connected or not, and since when it is not.
    pub async fn repo_any_by_id(&self, id: &str) -> sqlx::Result<Option<(Repo, Option<String>)>> {
        Ok(match self {
            Db::Postgres(p) => match sqlx::query(REPO_ANY_BY_ID)
                .bind(id)
                .fetch_optional(p)
                .await?
            {
                Some(row) => Some((repo_pg(&row)?, row.try_get("disconnected_at")?)),
                None => None,
            },
            Db::Sqlite(p) => match sqlx::query(&lite(REPO_ANY_BY_ID))
                .bind(id)
                .fetch_optional(p)
                .await?
            {
                Some(row) => Some((repo_sqlite(&row)?, row.try_get("disconnected_at")?)),
                None => None,
            },
        })
    }

    /// The changes recorded for a repository, oldest first.
    pub async fn repo_changes(&self, repo_id: &str) -> sqlx::Result<Vec<RepoChange>> {
        macro_rules! changes {
            ($pool:expr, $sql:expr) => {{
                let mut found = Vec::new();
                for row in sqlx::query($sql).bind(repo_id).fetch_all($pool).await? {
                    found.push(RepoChange {
                        at: row.try_get("at")?,
                        actor: row.try_get("actor")?,
                        action: row.try_get("action")?,
                        detail: row.try_get("detail")?,
                    });
                }
                found
            }};
        }
        Ok(match self {
            Db::Postgres(p) => changes!(p, REPO_CHANGES),
            Db::Sqlite(p) => changes!(p, &lite(REPO_CHANGES)),
        })
    }

    /// Change what a connected repository's events run. `workflow` is
    /// `Some(None)` to unbind it, `None` to leave it; `cell` is `None` to leave
    /// it. `false` when the repository is not connected. Recorded as
    /// nobody's ([`LOCAL`]): [`Self::change_repo`] names who.
    pub async fn bind_repo(
        &self,
        forge_host: &str,
        full_name: &str,
        workflow: Option<Option<&str>>,
        cell: Option<&str>,
    ) -> sqlx::Result<bool> {
        let changed = self
            .change_repo(forge_host, full_name, workflow, cell, None, None, LOCAL)
            .await?;
        Ok(changed.is_some())
    }

    /// Set how often a repository is polled: every `secs` seconds, never for
    /// `Some(0)`, or CI_POLL_SECS's interval for `None`. It is due at once.
    /// `false` when the repository is not connected. Recorded as nobody's.
    pub async fn set_poll_secs(
        &self,
        forge_host: &str,
        full_name: &str,
        secs: Option<u64>,
    ) -> sqlx::Result<bool> {
        let changed = self
            .change_repo(forge_host, full_name, None, None, Some(secs), None, LOCAL)
            .await?;
        Ok(changed.is_some())
    }

    /// Change a connected repository, and record that `actor` did, in one
    /// transaction: what its events run (`workflow`, `Some(None)` to unbind
    /// it), the `cell` they run on, how often it is polled (`poll`: every so
    /// many seconds, never for 0, CI_POLL_SECS's interval for `Some(None)`),
    /// and how it is served (`mode`). Each is left as it is where it is
    /// `None`.
    ///
    /// `None` when the repository is not connected. Else whether anything
    /// changed: what is asked for and is so already is not set again and not
    /// recorded, so that a caller who says the same thing every minute writes
    /// one row, and does not make the repository due for a sweep each time.
    #[allow(clippy::too_many_arguments)]
    pub async fn change_repo(
        &self,
        forge_host: &str,
        full_name: &str,
        workflow: Option<Option<&str>>,
        cell: Option<&str>,
        poll: Option<Option<u64>>,
        mode: Option<Mode>,
        actor: &str,
    ) -> sqlx::Result<Option<bool>> {
        let (host, name) = (
            forge_host.to_ascii_lowercase(),
            full_name.to_ascii_lowercase(),
        );
        let secs = poll.map(|secs| secs.map(|s| s.min(i32::MAX as u64) as i32));
        let change = uuid::Uuid::now_v7().to_string();
        macro_rules! change {
            ($pool:expr, $find:expr, $bind:expr, $poll:expr, $mode:expr, $record:expr) => {{
                let mut tx = $pool.begin().await?;
                let now: Option<(String, Option<String>, String, Option<i64>, String)> =
                    sqlx::query_as($find)
                        .bind(&host)
                        .bind(&name)
                        .fetch_optional(&mut *tx)
                        .await?;
                match now {
                    None => None,
                    Some((id, has_workflow, has_cell, has_secs, has_mode)) => {
                        let workflow = workflow.filter(|w| *w != has_workflow.as_deref());
                        let cell = cell.filter(|c| *c != has_cell.as_str());
                        let secs = secs.filter(|s| s.map(i64::from) != has_secs);
                        let mode = mode.filter(|m| m.as_str() != has_mode);
                        let mut set = serde_json::Map::new();
                        if let Some(workflow) = workflow {
                            set.insert("workflow".into(), serde_json::json!(workflow));
                        }
                        if let Some(cell) = cell {
                            set.insert("cell".into(), serde_json::json!(cell));
                        }
                        if let Some(secs) = secs {
                            set.insert("poll_secs".into(), serde_json::json!(secs));
                        }
                        if let Some(mode) = mode {
                            set.insert("mode".into(), serde_json::json!(mode.as_str()));
                        }
                        let changes = !set.is_empty();
                        if workflow.is_some() || cell.is_some() {
                            sqlx::query($bind)
                                .bind(&host)
                                .bind(&name)
                                .bind(workflow.is_some())
                                .bind(cell.is_some())
                                .bind(workflow.flatten())
                                .bind(cell)
                                .execute(&mut *tx)
                                .await?;
                        }
                        if let Some(secs) = secs {
                            sqlx::query($poll)
                                .bind(&host)
                                .bind(&name)
                                .bind(secs)
                                .execute(&mut *tx)
                                .await?;
                        }
                        if let Some(mode) = mode {
                            sqlx::query($mode)
                                .bind(&host)
                                .bind(&name)
                                .bind(mode.as_str())
                                .execute(&mut *tx)
                                .await?;
                        }
                        if changes {
                            sqlx::query($record)
                                .bind(&change)
                                .bind(&id)
                                .bind(actor)
                                .bind("changed")
                                .bind(serde_json::Value::Object(set).to_string())
                                .execute(&mut *tx)
                                .await?;
                        }
                        tx.commit().await?;
                        Some(changes)
                    }
                }
            }};
        }
        // The row as it stands, held from here to the commit: an update that
        // sets nothing, so that a disconnect waits for this change or this
        // change finds the repository disconnected, on either database.
        const CONNECTED_ID: &str = "UPDATE ci_repos SET forge_host = forge_host \
             WHERE forge_host = $1 AND full_name = $2 AND disconnected_at IS NULL \
             RETURNING id, workflow, cell, CAST(poll_secs AS BIGINT) AS poll_secs, mode";
        Ok(match self {
            Db::Postgres(p) => {
                change!(
                    p,
                    CONNECTED_ID,
                    BIND_REPO,
                    SET_POLL_SECS,
                    SET_MODE,
                    INSERT_REPO_CHANGE
                )
            }
            Db::Sqlite(p) => change!(
                p,
                &lite(CONNECTED_ID),
                &lite(BIND_REPO),
                &lite(SET_POLL_SECS),
                &lite(SET_MODE),
                &lite(INSERT_REPO_CHANGE)
            ),
        })
    }

    /// Disconnect a repository. `false` when it was not connected. Recorded
    /// as nobody's ([`LOCAL`]): [`Self::disconnect_repo`] names who.
    pub async fn remove_repo(&self, forge_host: &str, full_name: &str) -> sqlx::Result<bool> {
        Ok(self
            .disconnect_repo(forge_host, full_name, LOCAL)
            .await?
            .is_some())
    }

    /// Disconnect a repository, and record that `actor` did, in one
    /// transaction: its id, or `None` when it was not connected. Its row
    /// stays, and its events and runs with it, to be read. It takes no more
    /// webhooks, run requests or sweeps, and the events of it that were
    /// waiting are not run: they are skipped, and say why. The fleet's jobs
    /// of it that waited are closed, as nothing would say they ended.
    pub async fn disconnect_repo(
        &self,
        forge_host: &str,
        full_name: &str,
        actor: &str,
    ) -> sqlx::Result<Option<String>> {
        let (host, name) = (
            forge_host.to_ascii_lowercase(),
            full_name.to_ascii_lowercase(),
        );
        let change = uuid::Uuid::now_v7().to_string();
        macro_rules! disconnect {
            ($pool:expr, $sql:expr, $skip:expr, $record:expr, $jobs:expr) => {{
                let mut tx = $pool.begin().await?;
                let id: Option<String> = sqlx::query_scalar($sql)
                    .bind(&host)
                    .bind(&name)
                    .fetch_optional(&mut *tx)
                    .await?;
                if let Some(id) = &id {
                    sqlx::query($skip)
                        .bind(id)
                        .bind(DISCONNECTED)
                        .execute(&mut *tx)
                        .await?;
                    sqlx::query($jobs).bind(id).execute(&mut *tx).await?;
                    sqlx::query($record)
                        .bind(&change)
                        .bind(id)
                        .bind(actor)
                        .bind("disconnected")
                        .bind("{}")
                        .execute(&mut *tx)
                        .await?;
                }
                tx.commit().await?;
                id
            }};
        }
        Ok(match self {
            Db::Postgres(p) => {
                disconnect!(
                    p,
                    DISCONNECT_REPO_PG,
                    SKIP_EVENTS_OF,
                    INSERT_REPO_CHANGE,
                    CLOSE_FLEET_JOBS_OF
                )
            }
            Db::Sqlite(p) => disconnect!(
                p,
                DISCONNECT_REPO_SQLITE,
                &lite(SKIP_EVENTS_OF),
                &lite(INSERT_REPO_CHANGE),
                &lite(CLOSE_FLEET_JOBS_OF)
            ),
        })
    }

    /// Take a delivery that makes no event (a check's "Re-run"): its id is
    /// recorded, and `false` says it was seen before and is not to be acted
    /// on again. A redelivery, or a replayed capture, then reruns nothing.
    pub async fn take_delivery(&self, forge_host: &str, delivery_id: &str) -> sqlx::Result<bool> {
        let rows = match self {
            Db::Postgres(p) => sqlx::query(INSERT_DELIVERY)
                .bind(forge_host)
                .bind(delivery_id)
                .execute(p)
                .await?
                .rows_affected(),
            Db::Sqlite(p) => sqlx::query(&lite(INSERT_DELIVERY))
                .bind(forge_host)
                .bind(delivery_id)
                .execute(p)
                .await?
                .rows_affected(),
        };
        Ok(rows == 1)
    }

    /// Forget a delivery [`Self::take_delivery`] took and nothing could be
    /// done for, so that the same delivery sent again is taken.
    pub async fn forget_delivery(&self, forge_host: &str, delivery_id: &str) -> sqlx::Result<()> {
        match self {
            Db::Postgres(p) => sqlx::query(FORGET_DELIVERY)
                .bind(forge_host)
                .bind(delivery_id)
                .execute(p)
                .await
                .map(drop),
            Db::Sqlite(p) => sqlx::query(&lite(FORGET_DELIVERY))
                .bind(forge_host)
                .bind(delivery_id)
                .execute(p)
                .await
                .map(drop),
        }
    }

    /// Record one webhook delivery: its id, and the event it carried, in one
    /// transaction. A delivery id seen before writes nothing, so a redelivery
    /// (or a replayed capture) can never queue a second event. And because the
    /// two rows commit together, an event that failed to write leaves no
    /// delivery row behind to make the retry look like a duplicate.
    pub async fn record_delivery(
        &self,
        repo: &Repo,
        delivery_id: &str,
        event: &NewEvent,
        payload: &[u8],
    ) -> sqlx::Result<Recorded> {
        let kept = (payload.len() <= PAYLOAD_INLINE_MAX)
            .then(|| String::from_utf8_lossy(payload).into_owned());
        if kept.is_none() {
            tracing::info!(
                delivery_id,
                bytes = payload.len(),
                "payload over 64 KiB, and no CI_OBJECT_STORE_URL to keep it in; the event is \
                 queued without it"
            );
        }
        self.record_delivered(repo, delivery_id, event, kept.as_deref(), None)
            .await
    }

    /// [`Db::record_delivery`] for a payload kept in the store, under
    /// `payload_ref`.
    pub async fn record_delivery_stored(
        &self,
        repo: &Repo,
        delivery_id: &str,
        event: &NewEvent,
        payload_ref: &str,
    ) -> sqlx::Result<Recorded> {
        self.record_delivered(repo, delivery_id, event, None, Some(payload_ref))
            .await
    }

    async fn record_delivered(
        &self,
        repo: &Repo,
        delivery_id: &str,
        event: &NewEvent,
        kept: Option<&str>,
        payload_ref: Option<&str>,
    ) -> sqlx::Result<Recorded> {
        let id = uuid::Uuid::now_v7().to_string();
        let key = commit_key(&repo.id, event);
        macro_rules! record {
            ($pool:expr, $delivery:expr, $insert:expr) => {{
                let mut tx = $pool.begin().await?;
                let fresh = sqlx::query($delivery)
                    .bind(&repo.forge_host)
                    .bind(delivery_id)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
                    == 1;
                if !fresh {
                    tx.rollback().await?;
                    return Ok(Recorded::Duplicate);
                }
                sqlx::query($insert)
                    .bind(&id)
                    .bind(&repo.id)
                    .bind("webhook")
                    .bind(delivery_id)
                    .bind(event.kind.as_str())
                    .bind(&event.git_ref)
                    .bind(&event.sha)
                    .bind(event.base_sha.as_deref())
                    .bind(event.pr)
                    .bind(event.actor.as_deref())
                    .bind(event.trust.as_str())
                    .bind(&key)
                    .bind(kept)
                    .bind(event.base_ref.as_deref())
                    .bind(event.commit.as_deref())
                    .bind(event.association.as_deref())
                    .bind(event.head_ref.as_deref())
                    .bind(payload_ref)
                    .execute(&mut *tx)
                    .await?;
                tx.commit().await?;
            }};
        }
        match self {
            Db::Postgres(p) => record!(p, INSERT_DELIVERY, INSERT_DELIVERED_EVENT),
            Db::Sqlite(p) => record!(p, &lite(INSERT_DELIVERY), &lite(INSERT_DELIVERED_EVENT)),
        }
        Ok(Recorded::Event(id))
    }

    /// On Postgres, make `ci_events`' partition for each of the `days` that
    /// has none: one a day, UTC, named `ci_events_YYYYMMDD`. A day whose
    /// events are in the default partition already (its own was not made in
    /// time) is left there, and said: its partition cannot be made over
    /// them. Nothing on SQLite, which has no partitions. The names made.
    pub async fn ensure_event_partitions(&self, days: &[Day]) -> sqlx::Result<Vec<String>> {
        let Db::Postgres(p) = self else {
            return Ok(Vec::new());
        };
        let mut tx = p.begin().await?;
        sqlx::query(PARTITIONS_LOCK).execute(&mut *tx).await?;
        let mut made = Vec::new();
        for day in days {
            let name = day.partition();
            let there: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
                .bind(&name)
                .fetch_one(&mut *tx)
                .await?;
            if there {
                continue;
            }
            let held: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM ci_events_default \
                 WHERE received_at >= $1::timestamptz AND received_at < $2::timestamptz)",
            )
            .bind(day.start())
            .bind(day.next().start())
            .fetch_one(&mut *tx)
            .await?;
            if held {
                tracing::warn!(
                    partition = %name,
                    "events of this day are in ci_events_default: its partition was not made in \
                     time, and is not made over them"
                );
                continue;
            }
            // The name and the bounds are dagron-ci's own, made of digits.
            sqlx::query(&format!(
                "CREATE TABLE {name} PARTITION OF ci_events FOR VALUES FROM ('{}') TO ('{}')",
                day.start(),
                day.next().start()
            ))
            .execute(&mut *tx)
            .await?;
            made.push(name);
        }
        tx.commit().await?;
        Ok(made)
    }

    /// Drop the events received before `horizon` (midnight, UTC, of that
    /// day), a day at a time ([`Db::drop_day`]), with what goes with them.
    /// What was dropped. A day the queue's lock kept is left for the next
    /// pass.
    pub async fn drop_events_before(&self, horizon: Day) -> sqlx::Result<DroppedEvents> {
        let mut all = DroppedEvents::default();
        for day in self.days_before(horizon).await? {
            if let Some(dropped) = self.drop_day(day, None).await? {
                all.partitions.extend(dropped.partitions);
                all.events += dropped.events;
                all.runs += dropped.runs;
                all.payloads.extend(dropped.payloads);
            }
        }
        Ok(all)
    }

    /// The days before `horizon` that dagron-ci has records of: an event or
    /// a delivery received then, or, on Postgres, a partition of
    /// `ci_events`, however empty. Oldest first.
    pub async fn days_before(&self, horizon: Day) -> sqlx::Result<Vec<Day>> {
        let before = horizon.start();
        let dates: Vec<String> = match self {
            Db::Postgres(p) => {
                let mut dates: Vec<String> = sqlx::query_scalar(
                    "SELECT to_char(received_at AT TIME ZONE 'UTC', 'YYYY-MM-DD') FROM ci_events \
                     WHERE received_at < $1::timestamptz \
                     UNION SELECT to_char(received_at AT TIME ZONE 'UTC', 'YYYY-MM-DD') FROM ci_deliveries \
                     WHERE received_at < $1::timestamptz",
                )
                .bind(&before)
                .fetch_all(p)
                .await?;
                let parts: Vec<String> = sqlx::query_scalar(
                    "SELECT c.relname::TEXT FROM pg_inherits i JOIN pg_class c ON c.oid = i.inhrelid \
                     WHERE i.inhparent = 'ci_events'::regclass AND c.relname ~ '^ci_events_[0-9]{8}$'",
                )
                .fetch_all(p)
                .await?;
                dates.extend(parts.iter().map(|name| {
                    let d = &name["ci_events_".len()..];
                    format!("{}-{}-{}", &d[..4], &d[4..6], &d[6..])
                }));
                dates
            }
            Db::Sqlite(p) => {
                sqlx::query_scalar(
                    "SELECT substr(received_at, 1, 10) FROM ci_events WHERE received_at < ?1 \
                     UNION SELECT substr(received_at, 1, 10) FROM ci_deliveries WHERE received_at < ?1",
                )
                .bind(&before)
                .fetch_all(p)
                .await?
            }
        };
        let mut days: Vec<Day> = dates
            .iter()
            .filter_map(|d| Day::parse(d))
            .filter(|d| *d < horizon)
            .collect();
        days.sort();
        days.dedup();
        Ok(days)
    }

    /// Drop the records of `day`: its events, their runs' rows, their run
    /// API requests and their report log, and its deliveries, in one
    /// transaction. On Postgres the day's partition of `ci_events` is
    /// dropped whole, and any of its events in the default partition
    /// deleted; on SQLite the rows are deleted. What was dropped, with the
    /// stored payloads its events named, for the caller to delete once the
    /// rows are gone.
    ///
    /// `None` where nothing was dropped, and the day is for a later pass: a
    /// run of its events is still followed (a rerun reopens an old run's
    /// record, and a run may last days), its rows are not those `written`
    /// says its segments hold (a post logged since they were written), or
    /// the queue held `ci_events` longer than [`DROP_LOCK_WAIT`].
    ///
    /// `written`: where the day was written out first, the digest of each
    /// table's rows its verified segments hold ([`rows_digest`]): the rows
    /// must still be those, not only as many, for an event's or a run's row
    /// changes in place.
    pub async fn drop_day(
        &self,
        day: Day,
        written: Option<&BTreeMap<String, String>>,
    ) -> sqlx::Result<Option<DroppedEvents>> {
        let (from, to) = (day.start(), day.next().start());
        let sqlite = matches!(self, Db::Sqlite(_));
        let mut dropped = DroppedEvents::default();
        macro_rules! drop_rows {
            ($tx:expr, $at:expr) => {{
                let (a, b, d) = $at;
                let events_of = format!("SELECT id FROM ci_events WHERE received_at >= {a} AND received_at < {b}");
                let followed: i64 = sqlx::query_scalar(&format!(
                    "SELECT count(*) FROM ci_runs WHERE report_due_at IS NOT NULL \
                     AND event_id IN ({events_of})"
                ))
                .bind(&from)
                .bind(&to)
                .fetch_one(&mut *$tx)
                .await?;
                if followed > 0 {
                    tracing::info!(
                        day = %day.date(),
                        runs = followed,
                        "a day with runs still followed is kept for a later pass"
                    );
                    return Ok(None);
                }
                if let Some(written) = written {
                    for table in RECORD_TABLES {
                        let columns: Option<Vec<String>> = if sqlite {
                            Some(
                                sqlx::query_scalar("SELECT name FROM pragma_table_info(?1) ORDER BY cid")
                                    .bind(table)
                                    .fetch_all(&mut *$tx)
                                    .await?,
                            )
                        } else {
                            None
                        };
                        let rows: Vec<String> =
                            sqlx::query_scalar(&records_sql(table, a, b, columns.as_deref()))
                                .bind(&from)
                                .bind(&to)
                                .fetch_all(&mut *$tx)
                                .await?;
                        if rows_digest(&rows) != written.get(table).cloned().unwrap_or_default() {
                            tracing::info!(
                                day = %day.date(),
                                table,
                                rows = rows.len(),
                                "a day's rows are not those its segments hold: it is written again first"
                            );
                            return Ok(None);
                        }
                    }
                }
                dropped.payloads = sqlx::query_scalar(&format!(
                    "SELECT payload_ref FROM ci_events WHERE received_at >= {a} AND received_at < {b} \
                     AND payload_ref IS NOT NULL"
                ))
                .bind(&from)
                .bind(&to)
                .fetch_all(&mut *$tx)
                .await?;
                let events: i64 = sqlx::query_scalar(&format!(
                    "SELECT count(*) FROM ci_events WHERE received_at >= {a} AND received_at < {b}"
                ))
                .bind(&from)
                .bind(&to)
                .fetch_one(&mut *$tx)
                .await?;
                dropped.events = events as u64;
                // Where the day was written out, each of its events and runs
                // is indexed, to be found in the segments, and its reader
                // identified, once its rows are gone.
                if written.is_some() {
                    sqlx::query(&format!(
                        "INSERT INTO ci_archived (id, event_id, day, cell) \
                         SELECT id, event_id, day, cell FROM ( \
                           SELECT e.id AS id, e.id AS event_id, {d} AS day, p.cell AS cell \
                           FROM ci_events e JOIN ci_repos p ON p.id = e.repo_id \
                           WHERE e.received_at >= {a} AND e.received_at < {b} \
                           UNION ALL \
                           SELECT r.run_id, r.event_id, {d}, p.cell \
                           FROM ci_runs r JOIN ci_events e ON e.id = r.event_id \
                           JOIN ci_repos p ON p.id = e.repo_id \
                           WHERE e.received_at >= {a} AND e.received_at < {b} \
                         ) AS ids WHERE true \
                         ON CONFLICT (id) DO NOTHING"
                    ))
                    .bind(&from)
                    .bind(&to)
                    .bind(day.date())
                    .execute(&mut *$tx)
                    .await?;
                }
                for table in ["ci_report_log", "ci_api_requests"] {
                    sqlx::query(&format!("DELETE FROM {table} WHERE event_id IN ({events_of})"))
                        .bind(&from)
                        .bind(&to)
                        .execute(&mut *$tx)
                        .await?;
                }
                dropped.runs = sqlx::query(&format!("DELETE FROM ci_runs WHERE event_id IN ({events_of})"))
                    .bind(&from)
                    .bind(&to)
                    .execute(&mut *$tx)
                    .await?
                    .rows_affected();
                // Who writes under a commit's contexts, last written by the
                // day's end: the runs that owned them then are over and gone,
                // and one a later run owns was written again as it did.
                // Bound once, to the day's end.
                sqlx::query(&format!("DELETE FROM ci_contexts WHERE written_at < {a}"))
                    .bind(&to)
                    .execute(&mut *$tx)
                    .await?;
                sqlx::query(&format!(
                    "DELETE FROM ci_deliveries WHERE received_at >= {a} AND received_at < {b}"
                ))
                .bind(&from)
                .bind(&to)
                .execute(&mut *$tx)
                .await?;
            }};
        }
        match self {
            Db::Postgres(p) => {
                let mut tx = p.begin().await?;
                sqlx::query(PARTITIONS_LOCK).execute(&mut *tx).await?;
                sqlx::query(&format!(
                    "SET LOCAL lock_timeout = '{}s'",
                    DROP_LOCK_WAIT.as_secs()
                ))
                .execute(&mut *tx)
                .await?;
                drop_rows!(tx, ("$1::timestamptz", "$2::timestamptz", "$3"));
                let name = day.partition();
                let there: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
                    .bind(&name)
                    .fetch_one(&mut *tx)
                    .await?;
                let gone = if there {
                    sqlx::query(&format!("DROP TABLE {name}"))
                        .execute(&mut *tx)
                        .await
                } else {
                    sqlx::query(
                        "DELETE FROM ci_events_default WHERE received_at >= $1::timestamptz \
                         AND received_at < $2::timestamptz",
                    )
                    .bind(&from)
                    .bind(&to)
                    .execute(&mut *tx)
                    .await
                };
                match gone {
                    Ok(_) => {}
                    // The queue held its lock longer than the drop waits.
                    Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("55P03") => {
                        tracing::info!(partition = %name, "the queue held ci_events: this day is dropped at the next pass");
                        return Ok(None);
                    }
                    Err(e) => return Err(e),
                }
                if there {
                    // Its events of the default partition, where the day's
                    // own was made late.
                    sqlx::query(
                        "DELETE FROM ci_events_default WHERE received_at >= $1::timestamptz \
                         AND received_at < $2::timestamptz",
                    )
                    .bind(&from)
                    .bind(&to)
                    .execute(&mut *tx)
                    .await?;
                    dropped.partitions.push(name);
                }
                tx.commit().await?;
            }
            Db::Sqlite(p) => {
                let mut tx = p.begin().await?;
                drop_rows!(tx, ("?1", "?2", "?3"));
                sqlx::query("DELETE FROM ci_events WHERE received_at >= ?1 AND received_at < ?2")
                    .bind(&from)
                    .bind(&to)
                    .execute(&mut *tx)
                    .await?;
                tx.commit().await?;
            }
        }
        Ok(Some(dropped))
    }

    /// The rows of `table`, one of [`RECORD_TABLES`], that are `day`'s
    /// (an event's and a delivery's by when it was received, and the rest by
    /// their event's), each as a JSON object of its columns.
    pub async fn records_of_day(&self, table: &str, day: Day) -> sqlx::Result<Vec<String>> {
        assert!(
            RECORD_TABLES.contains(&table),
            "{table} is no table of dagron-ci's records"
        );
        let (from, to) = (day.start(), day.next().start());
        match self {
            Db::Postgres(p) => {
                sqlx::query_scalar(&records_sql(
                    table,
                    "$1::timestamptz",
                    "$2::timestamptz",
                    None,
                ))
                .bind(&from)
                .bind(&to)
                .fetch_all(p)
                .await
            }
            Db::Sqlite(p) => {
                let columns: Vec<String> =
                    sqlx::query_scalar("SELECT name FROM pragma_table_info(?1) ORDER BY cid")
                        .bind(table)
                        .fetch_all(p)
                        .await?;
                sqlx::query_scalar(&records_sql(table, "?1", "?2", Some(&columns)))
                    .bind(&from)
                    .bind(&to)
                    .fetch_all(p)
                    .await
            }
        }
    }

    /// Log the statuses dagron-ci posted, and what came of each, in one
    /// statement.
    pub async fn log_reports(&self, rows: &[ReportLog]) -> sqlx::Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let json = serde_json::to_string(rows).unwrap_or_else(|_| "[]".into());
        match self {
            Db::Postgres(p) => sqlx::query(LOG_REPORTS_PG)
                .bind(&json)
                .execute(p)
                .await
                .map(drop)?,
            Db::Sqlite(p) => sqlx::query(LOG_REPORTS_SQLITE)
                .bind(&json)
                .execute(p)
                .await
                .map(drop)?,
        };
        Ok(())
    }

    /// What dagron-ci posted for the event `event_id`, oldest first, each
    /// a JSON object of the log's columns: none once the event's day has left
    /// the database ([`crate::records`] reads it back from there).
    pub async fn report_log_of(&self, event_id: &str) -> sqlx::Result<Vec<String>> {
        match self {
            Db::Postgres(p) => {
                sqlx::query_scalar(
                    "SELECT row_to_json(l)::TEXT FROM ci_report_log l WHERE event_id = $1 ORDER BY id",
                )
                .bind(event_id)
                .fetch_all(p)
                .await
            }
            Db::Sqlite(p) => {
                sqlx::query_scalar(
                    "SELECT json_object('id', id, 'event_id', event_id, 'run_id', run_id, 'repo_id', repo_id, \
                     'sha', sha, 'context', context, 'state', state, 'description', description, \
                     'target_url', target_url, 'posted_at', posted_at, 'outcome', outcome, 'detail', detail) \
                     FROM ci_report_log WHERE event_id = ?1 ORDER BY id",
                )
                .bind(event_id)
                .fetch_all(p)
                .await
            }
        }
    }

    /// Record a segment written to the store, not yet read back.
    pub async fn put_segment(&self, s: &Segment) -> sqlx::Result<()> {
        const UPSERT: &str = "INSERT INTO ci_segments \
             (table_name, day, scope, part, object_key, row_count, size_bytes, sha256, rows_sha256, verified_at) \
             VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NULL) \
             ON CONFLICT (table_name, day, scope, part) DO UPDATE SET object_key = excluded.object_key, \
             row_count = excluded.row_count, size_bytes = excluded.size_bytes, sha256 = excluded.sha256, \
             rows_sha256 = excluded.rows_sha256, \
             verified_at = NULL";
        macro_rules! put {
            ($pool:expr, $sql:expr) => {
                sqlx::query($sql)
                    .bind(&s.table)
                    .bind(&s.day)
                    .bind(&s.scope)
                    .bind(s.part)
                    .bind(&s.object_key)
                    .bind(s.row_count)
                    .bind(s.size_bytes)
                    .bind(&s.sha256)
                    .bind(&s.rows_sha256)
                    .execute($pool)
                    .await
                    .map(drop)?
            };
        }
        match self {
            Db::Postgres(p) => put!(p, UPSERT),
            Db::Sqlite(p) => put!(p, &lite(UPSERT)),
        };
        Ok(())
    }

    /// A segment read back whole, its checksum matched.
    pub async fn verify_segment(&self, s: &Segment) -> sqlx::Result<()> {
        macro_rules! verify {
            ($pool:expr, $sql:expr) => {
                sqlx::query($sql)
                    .bind(&s.table)
                    .bind(&s.day)
                    .bind(&s.scope)
                    .bind(s.part)
                    .execute($pool)
                    .await
                    .map(drop)?
            };
        }
        match self {
            Db::Postgres(p) => verify!(
                p,
                "UPDATE ci_segments SET verified_at = now() \
                 WHERE table_name = $1 AND day = $2 AND scope = $3 AND part = $4"
            ),
            Db::Sqlite(p) => verify!(
                p,
                "UPDATE ci_segments SET verified_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now') \
                 WHERE table_name = ?1 AND day = ?2 AND scope = ?3 AND part = ?4"
            ),
        };
        Ok(())
    }

    /// The segments of `day`, of every table.
    pub async fn segments_of_day(&self, day: Day) -> sqlx::Result<Vec<Segment>> {
        const OF: &str =
            "SELECT table_name, day, scope, part, object_key, row_count, size_bytes, sha256, \
             rows_sha256, verified_at IS NOT NULL AS verified FROM ci_segments WHERE day = $1 \
             ORDER BY table_name, scope, part";
        let date = day.date();
        let rows = match self {
            Db::Postgres(p) => sqlx::query(OF)
                .bind(&date)
                .fetch_all(p)
                .await?
                .iter()
                .map(segment_pg)
                .collect(),
            Db::Sqlite(p) => sqlx::query(&lite(OF))
                .bind(&date)
                .fetch_all(p)
                .await?
                .iter()
                .map(segment_sqlite)
                .collect(),
        };
        rows
    }

    /// Set every segment's time to be deleted from `keep_days`
    /// (CI_SEGMENT_DAYS): that many days after its day, or never.
    pub async fn set_segment_expiry(&self, keep_days: Option<u32>) -> sqlx::Result<()> {
        match (self, keep_days) {
            (Db::Postgres(p), Some(n)) => sqlx::query(
                "UPDATE ci_segments SET expires_at = \
                     ((day::date + 1 + $1::int)::timestamp AT TIME ZONE 'UTC')",
            )
            .bind(n as i32)
            .execute(p)
            .await
            .map(drop)?,
            (Db::Postgres(p), None) => sqlx::query("UPDATE ci_segments SET expires_at = NULL")
                .execute(p)
                .await
                .map(drop)?,
            (Db::Sqlite(p), Some(n)) => sqlx::query(
                "UPDATE ci_segments SET expires_at = \
                     strftime('%Y-%m-%dT%H:%M:%fZ', day, '+' || (1 + ?1) || ' days')",
            )
            .bind(n as i64)
            .execute(p)
            .await
            .map(drop)?,
            (Db::Sqlite(p), None) => sqlx::query("UPDATE ci_segments SET expires_at = NULL")
                .execute(p)
                .await
                .map(drop)?,
        };
        Ok(())
    }

    /// The segments whose time to be deleted has come.
    pub async fn expired_segments(&self) -> sqlx::Result<Vec<Segment>> {
        const DUE: &str = "SELECT table_name, day, scope, part, object_key, row_count, size_bytes, sha256, \
             rows_sha256, verified_at IS NOT NULL AS verified FROM ci_segments WHERE expires_at IS NOT NULL AND expires_at < ";
        let rows = match self {
            Db::Postgres(p) => sqlx::query(&format!("{DUE}now()"))
                .fetch_all(p)
                .await?
                .iter()
                .map(segment_pg)
                .collect(),
            Db::Sqlite(p) => sqlx::query(&format!("{DUE}strftime('%Y-%m-%dT%H:%M:%fZ', 'now')"))
                .fetch_all(p)
                .await?
                .iter()
                .map(segment_sqlite)
                .collect(),
        };
        rows
    }

    /// The event an archived id is of (its own, or an engine run's), its
    /// day, and its repository's cell: where its statuses are read from, and
    /// whose dagron-api says who may read them. `None` for an id no day
    /// written out held.
    pub async fn archived(&self, id: &str) -> sqlx::Result<Option<(String, Day, String)>> {
        const OF: &str = "SELECT event_id, day, cell FROM ci_archived WHERE id = $1";
        let row: Option<(String, String, String)> = match self {
            Db::Postgres(p) => sqlx::query_as(OF).bind(id).fetch_optional(p).await?,
            Db::Sqlite(p) => sqlx::query_as(&lite(OF)).bind(id).fetch_optional(p).await?,
        };
        Ok(row.and_then(|(event_id, day, cell)| Some((event_id, Day::parse(&day)?, cell))))
    }

    /// Forget the archived ids of every day that has no segment left.
    pub async fn forget_unarchived(&self) -> sqlx::Result<u64> {
        const FORGET: &str =
            "DELETE FROM ci_archived WHERE day NOT IN (SELECT DISTINCT day FROM ci_segments)";
        Ok(match self {
            Db::Postgres(p) => sqlx::query(FORGET).execute(p).await?.rows_affected(),
            Db::Sqlite(p) => sqlx::query(FORGET).execute(p).await?.rows_affected(),
        })
    }

    /// A segment deleted from the store: its row goes too.
    pub async fn forget_segment(&self, s: &Segment) -> sqlx::Result<()> {
        const FORGET: &str = "DELETE FROM ci_segments WHERE table_name = $1 AND day = $2 AND scope = $3 AND part = $4";
        match self {
            Db::Postgres(p) => sqlx::query(FORGET)
                .bind(&s.table)
                .bind(&s.day)
                .bind(&s.scope)
                .bind(s.part)
                .execute(p)
                .await
                .map(drop)?,
            Db::Sqlite(p) => sqlx::query(&lite(FORGET))
                .bind(&s.table)
                .bind(&s.day)
                .bind(&s.scope)
                .bind(s.part)
                .execute(p)
                .await
                .map(drop)?,
        };
        Ok(())
    }

    /// When the oldest record in the database was received: its event's
    /// time, or its delivery's. `None` when there is none.
    pub async fn oldest_record_age_secs(&self) -> sqlx::Result<Option<f64>> {
        match self {
            Db::Postgres(p) => sqlx::query_scalar(
                "SELECT EXTRACT(EPOCH FROM now() - LEAST((SELECT min(received_at) FROM ci_events), \
                 (SELECT min(received_at) FROM ci_deliveries)))::FLOAT8",
            )
            .fetch_one(p)
            .await,
            Db::Sqlite(p) => sqlx::query_scalar(
                "SELECT (julianday('now') - julianday(min(m))) * 86400.0 FROM \
                 (SELECT min(received_at) AS m FROM ci_events UNION ALL SELECT min(received_at) FROM ci_deliveries)",
            )
            .fetch_one(p)
            .await,
        }
    }

    pub async fn backlog(&self) -> sqlx::Result<Backlog> {
        let (pending, oldest_age_secs): (i64, Option<f64>) = match self {
            Db::Postgres(p) => sqlx::query_as(BACKLOG_PG).fetch_one(p).await?,
            Db::Sqlite(p) => sqlx::query_as(BACKLOG_SQLITE).fetch_one(p).await?,
        };
        Ok(Backlog {
            pending,
            oldest_age_secs,
        })
    }

    /// Claim the oldest event that is due, for `lease`, as `worker`. An event
    /// whose last claim lapsed is due: its worker stopped mid-event, and this
    /// claim takes it over. `None` when nothing is due.
    ///
    /// An event whose repository has been disconnected is not handed over:
    /// it is skipped, saying why, and the next one is looked for. A
    /// disconnect skips the events that wait, and this is for the ones it
    /// could not: one a worker held and gave back to be tried again, one
    /// whose worker stopped, one recorded as the disconnect went through.
    pub async fn claim_event(
        &self,
        worker: &str,
        lease: Duration,
    ) -> sqlx::Result<Option<Claimed>> {
        macro_rules! claimed {
            ($row:expr) => {{
                let row = $row;
                (
                    row.try_get::<String, _>("repo_id")?,
                    row.try_get::<Option<String>, _>("inputs")?,
                    Claimed {
                        id: row.try_get("id")?,
                        repo: Repo::default(),
                        kind: row.try_get("kind")?,
                        git_ref: row.try_get("ref")?,
                        sha: row.try_get("sha")?,
                        base_sha: row.try_get("base_sha")?,
                        base_ref: row.try_get("base_ref")?,
                        pr: row.try_get("pr")?,
                        actor: row.try_get("actor")?,
                        trust: row.try_get("trust")?,
                        association: row.try_get("association")?,
                        head_ref: row.try_get("head_ref")?,
                        commit_key: row.try_get("commit_key")?,
                        attempts: row.try_get("attempts")?,
                        inputs: BTreeMap::new(),
                        pipeline: row.try_get("pipeline")?,
                        bundle_id: row.try_get("bundle_id")?,
                        patch: None,
                    },
                )
            }};
        }
        loop {
            let claimed = match self {
                Db::Postgres(p) => sqlx::query(CLAIM_PG)
                    .bind(worker)
                    .bind(lease.as_secs_f64())
                    .fetch_optional(p)
                    .await?
                    .map(|row| -> sqlx::Result<_> { Ok(claimed!(row)) })
                    .transpose()?,
                Db::Sqlite(p) => sqlx::query(CLAIM_SQLITE)
                    .bind(worker)
                    .bind(lease.as_secs().max(1) as i64)
                    .fetch_optional(p)
                    .await?
                    .map(|row| -> sqlx::Result<_> { Ok(claimed!(row)) })
                    .transpose()?,
            };
            let Some((repo_id, inputs, mut claimed)) = claimed else {
                return Ok(None);
            };
            // Written by the run API from a map of strings; anything else is not
            // this queue's, and runs with no inputs rather than not at all.
            claimed.inputs = inputs
                .and_then(|json| serde_json::from_str(&json).ok())
                .unwrap_or_default();
            let (repo, connected) = match self {
                Db::Postgres(p) => {
                    let row = sqlx::query(REPO_BY_ID).bind(&repo_id).fetch_one(p).await?;
                    (repo_pg(&row)?, row.try_get::<i32, _>("connected")?)
                }
                Db::Sqlite(p) => {
                    let row = sqlx::query(&lite(REPO_BY_ID))
                        .bind(&repo_id)
                        .fetch_one(p)
                        .await?;
                    (repo_sqlite(&row)?, row.try_get::<i32, _>("connected")?)
                }
            };
            if connected == 0 {
                let skipped = Finish::Skipped(DISCONNECTED.into());
                self.finish_event(&claimed.id, worker, &skipped).await?;
                continue;
            }
            claimed.repo = repo;
            return Ok(Some(claimed));
        }
    }

    /// Leave an event `worker` holds as `finish` says. `false` when the worker
    /// no longer holds it (its lease lapsed and another worker took it): then
    /// nothing changes, and the event is the other worker's to finish.
    pub async fn finish_event(
        &self,
        id: &str,
        worker: &str,
        finish: &Finish,
    ) -> sqlx::Result<bool> {
        let (state, reason) = match finish {
            Finish::Done(note) => ("done", note.as_str()),
            Finish::Skipped(why) => ("skipped", why.as_str()),
            Finish::Failed(why) => ("failed", why.as_str()),
            Finish::Retry { reason, after } | Finish::Wait { reason, after } => {
                let given_back = i32::from(matches!(finish, Finish::Wait { .. }));
                let rows = match self {
                    Db::Postgres(p) => sqlx::query(RETRY_PG)
                        .bind(id)
                        .bind(worker)
                        .bind(reason)
                        .bind(after.as_secs_f64())
                        .bind(given_back)
                        .execute(p)
                        .await?
                        .rows_affected(),
                    Db::Sqlite(p) => sqlx::query(RETRY_SQLITE)
                        .bind(id)
                        .bind(worker)
                        .bind(reason)
                        .bind(after.as_secs().max(1) as i64)
                        .bind(i64::from(given_back))
                        .execute(p)
                        .await?
                        .rows_affected(),
                };
                return Ok(rows == 1);
            }
        };
        let rows = match self {
            Db::Postgres(p) => sqlx::query(FINISH)
                .bind(id)
                .bind(worker)
                .bind(state)
                .bind(reason)
                .execute(p)
                .await?
                .rows_affected(),
            Db::Sqlite(p) => sqlx::query(&lite(FINISH))
                .bind(id)
                .bind(worker)
                .bind(state)
                .bind(reason)
                .execute(p)
                .await?
                .rows_affected(),
        };
        Ok(rows == 1)
    }

    /// Write the row of a checkout credential that is about to be handed
    /// over. An error is a credential with no row, which is not handed over.
    pub async fn record_credential(&self, minted: &CheckoutCredential) -> sqlx::Result<()> {
        let id = uuid::Uuid::now_v7().to_string();
        macro_rules! insert {
            ($pool:expr, $sql:expr) => {
                sqlx::query($sql)
                    .bind(&id)
                    .bind(&minted.token_sha256)
                    .bind(&minted.run_id)
                    .bind(&minted.task_id)
                    .bind(minted.attempt)
                    .bind(&minted.repository)
                    .bind(&minted.commit)
                    .bind(&minted.cell)
                    .bind(&minted.expires_at)
                    .bind(&minted.permissions)
                    .bind(&minted.forge_token_id)
                    .execute($pool)
                    .await?
            };
        }
        match self {
            Db::Postgres(p) => {
                insert!(p, INSERT_CREDENTIAL);
            }
            Db::Sqlite(p) => {
                insert!(p, &lite(INSERT_CREDENTIAL));
            }
        }
        Ok(())
    }

    /// Mark the row of the credential whose SHA-256 is `token_sha256` as
    /// revoked now. `false` when no row waited for it: none was written (a
    /// credential from before the table, or another dagron-ci's), or it was
    /// marked before.
    pub async fn credential_revoked(&self, token_sha256: &str) -> sqlx::Result<bool> {
        let rows = match self {
            Db::Postgres(p) => sqlx::query(REVOKE_CREDENTIAL_PG)
                .bind(token_sha256)
                .execute(p)
                .await?
                .rows_affected(),
            Db::Sqlite(p) => sqlx::query(REVOKE_CREDENTIAL_SQLITE)
                .bind(token_sha256)
                .execute(p)
                .await?
                .rows_affected(),
        };
        Ok(rows > 0)
    }

    /// The repository and the forge's own id of the credential whose SHA-256
    /// is `token_sha256`, and whether dagron-ci revoked it already, where it
    /// is one the forge revokes by id (a GitLab project access token).
    pub async fn forge_token_of(
        &self,
        token_sha256: &str,
    ) -> sqlx::Result<Option<(String, String, bool)>> {
        let row: Option<(String, String, i64)> = match self {
            Db::Postgres(p) => {
                sqlx::query_as(FORGE_TOKEN_OF)
                    .bind(token_sha256)
                    .fetch_optional(p)
                    .await?
            }
            Db::Sqlite(p) => {
                sqlx::query_as(&lite(FORGE_TOKEN_OF))
                    .bind(token_sha256)
                    .fetch_optional(p)
                    .await?
            }
        };
        Ok(row.map(|(repository, id, revoked)| (repository, id, revoked != 0)))
    }

    /// The rows of the checkout credentials minted for `run_id`, oldest
    /// first.
    pub async fn credentials_of_run(&self, run_id: &str) -> sqlx::Result<Vec<CheckoutCredential>> {
        macro_rules! rows {
            ($pool:expr, $sql:expr) => {{
                let mut found = Vec::new();
                for row in sqlx::query($sql).bind(run_id).fetch_all($pool).await? {
                    found.push(CheckoutCredential {
                        token_sha256: row.try_get("token_sha256")?,
                        run_id: row.try_get("run_id")?,
                        task_id: row.try_get("task_id")?,
                        attempt: row.try_get("attempt")?,
                        repository: row.try_get("repository")?,
                        commit: row.try_get("commit_sha")?,
                        cell: row.try_get("cell")?,
                        expires_at: row.try_get("expires_at")?,
                        revoked: row.try_get::<i64, _>("revoked")? != 0,
                        permissions: row.try_get("permissions")?,
                        forge_token_id: row.try_get("forge_token_id")?,
                    });
                }
                found
            }};
        }
        Ok(match self {
            Db::Postgres(p) => rows!(p, CREDENTIALS_OF_RUN),
            Db::Sqlite(p) => rows!(p, &lite(CREDENTIALS_OF_RUN)),
        })
    }

    /// Have the runs of the pipeline at `pipeline`, at the commit `sha` of the
    /// repository `repo_id`, post their statuses afresh, and say how many
    /// there are: 0 when the pipeline has no run of its own there. The runs
    /// are those under an event of kind `kind` or `also`: the kinds whose
    /// jobs report under one name. With `but`, the runs of that event are
    /// left as they are, and not counted: the runs other events made.
    ///
    /// For whoever writes a status under a job's context that is not that
    /// job's own: the run's statuses, posted after, are then the last word.
    pub async fn report_pipeline_again(
        &self,
        repo_id: &str,
        pipeline: &str,
        sha: &str,
        kind: &str,
        also: &str,
        but: Option<&str>,
    ) -> sqlx::Result<u64> {
        macro_rules! again {
            ($pool:expr, $sql:expr) => {
                sqlx::query($sql)
                    .bind(repo_id)
                    .bind(pipeline)
                    .bind(sha)
                    .bind(kind)
                    .bind(also)
                    .bind(but)
                    .execute($pool)
                    .await?
                    .rows_affected()
            };
        }
        Ok(match self {
            Db::Postgres(p) => again!(p, REPORT_AGAIN_PG),
            Db::Sqlite(p) => again!(p, REPORT_AGAIN_SQLITE),
        })
    }

    /// The contexts under which the runs of the pipeline at `pipeline`, at
    /// the commit `sha` of the repository `repo_id`, have posted a status:
    /// the runs [`Self::report_pipeline_again`] names, but for those of the
    /// event `but`. A run posts under each of its jobs as soon as it is
    /// looked at, so these are the jobs other events' runs have.
    ///
    /// A run that was just recorded, or just told to post again, has none
    /// on record until it is next looked at: what is not here may yet be
    /// posted.
    pub async fn posted_by_others(
        &self,
        repo_id: &str,
        pipeline: &str,
        sha: &str,
        kind: &str,
        also: &str,
        but: &str,
    ) -> sqlx::Result<std::collections::BTreeSet<String>> {
        macro_rules! posted {
            ($pool:expr, $sql:expr) => {
                sqlx::query_scalar::<_, String>($sql)
                    .bind(repo_id)
                    .bind(pipeline)
                    .bind(sha)
                    .bind(kind)
                    .bind(also)
                    .bind(but)
                    .fetch_all($pool)
                    .await?
            };
        }
        let rows = match self {
            Db::Postgres(p) => posted!(p, POSTED_BY_OTHERS),
            Db::Sqlite(p) => posted!(p, &lite(POSTED_BY_OTHERS)),
        };
        Ok(rows
            .iter()
            .flat_map(|reported| {
                serde_json::from_str::<BTreeMap<String, String>>(reported).unwrap_or_default()
            })
            // What is on record under a context the forge refused is why,
            // and no status; neither is the run's own entry (`!refused`).
            .filter(|(context, posted)| !context.starts_with('!') && !posted.starts_with('!'))
            .map(|(context, _)| context)
            .collect())
    }

    /// Have the run `run_id` own the status context `context` at the commit
    /// `sha` of the repository `repo_id`, before it first posts under it.
    /// `false` while someone posts a skipped status under it
    /// ([`Self::lease_context`]): the run is to post once that is over, so
    /// that what it posts is the later.
    pub async fn own_context(
        &self,
        repo_id: &str,
        sha: &str,
        context: &str,
        run_id: &str,
    ) -> sqlx::Result<bool> {
        macro_rules! own {
            ($pool:expr, $sql:expr) => {
                sqlx::query_scalar::<_, String>($sql)
                    .bind(repo_id)
                    .bind(sha)
                    .bind(context)
                    .bind(run_id)
                    .fetch_optional($pool)
                    .await?
            };
        }
        let owned = match self {
            Db::Postgres(p) => own!(p, OWN_CONTEXT_PG),
            Db::Sqlite(p) => own!(p, OWN_CONTEXT_SQLITE),
        };
        Ok(owned.is_some())
    }

    /// A lease, held by `holder` for `lease`, under which to post a skipped
    /// status under the context `context` at the commit `sha` of the
    /// repository `repo_id`. `false` where a run owns the context
    /// ([`Self::own_context`]), whose statuses are its own word, or another
    /// holder's lease is in force: nothing is to be posted then. Given back
    /// with [`Self::release_context`] once the post is answered; `lease` is
    /// longer than any post, and one not given back lapses.
    pub async fn lease_context(
        &self,
        repo_id: &str,
        sha: &str,
        context: &str,
        holder: &str,
        lease: Duration,
    ) -> sqlx::Result<bool> {
        let leased = match self {
            Db::Postgres(p) => {
                sqlx::query_scalar::<_, String>(LEASE_CONTEXT_PG)
                    .bind(repo_id)
                    .bind(sha)
                    .bind(context)
                    .bind(holder)
                    .bind(lease.as_secs_f64())
                    .fetch_optional(p)
                    .await?
            }
            Db::Sqlite(p) => {
                sqlx::query_scalar::<_, String>(LEASE_CONTEXT_SQLITE)
                    .bind(repo_id)
                    .bind(sha)
                    .bind(context)
                    .bind(holder)
                    .bind(lease.as_secs().max(1) as i64)
                    .fetch_optional(p)
                    .await?
            }
        };
        Ok(leased.is_some())
    }

    /// Give back `holder`'s lease on the context `context` at the commit
    /// `sha` of the repository `repo_id`: a run waiting to own it may.
    pub async fn release_context(
        &self,
        repo_id: &str,
        sha: &str,
        context: &str,
        holder: &str,
    ) -> sqlx::Result<()> {
        macro_rules! release {
            ($pool:expr, $sql:expr) => {
                sqlx::query($sql)
                    .bind(repo_id)
                    .bind(sha)
                    .bind(context)
                    .bind(holder)
                    .execute($pool)
                    .await?
                    .rows_affected()
            };
        }
        match self {
            Db::Postgres(p) => release!(p, RELEASE_CONTEXT),
            Db::Sqlite(p) => release!(p, &lite(RELEASE_CONTEXT)),
        };
        Ok(())
    }

    /// The event that submitted the run `run_id`, if dagron-ci submitted it.
    pub async fn event_of_run(&self, run_id: &str) -> sqlx::Result<Option<String>> {
        match self {
            Db::Postgres(p) => {
                sqlx::query_scalar(EVENT_OF_RUN)
                    .bind(run_id)
                    .fetch_optional(p)
                    .await
            }
            Db::Sqlite(p) => {
                sqlx::query_scalar(&lite(EVENT_OF_RUN))
                    .bind(run_id)
                    .fetch_optional(p)
                    .await
            }
        }
    }

    /// Withdraw an event that waits to be dispatched: it ends `skipped`,
    /// with `reason`, and no worker takes it. `false` when it is not
    /// waiting (a worker holds it, or it is settled): then nothing changes.
    pub async fn withdraw_event(&self, id: &str, reason: &str) -> sqlx::Result<bool> {
        let rows = match self {
            Db::Postgres(p) => sqlx::query(WITHDRAW_EVENT)
                .bind(id)
                .bind(reason)
                .execute(p)
                .await?
                .rows_affected(),
            Db::Sqlite(p) => sqlx::query(&lite(WITHDRAW_EVENT))
                .bind(id)
                .bind(reason)
                .execute(p)
                .await?
                .rows_affected(),
        };
        Ok(rows == 1)
    }

    /// The run already submitted under `idempotency_key`, if there is one.
    pub async fn find_run(&self, idempotency_key: &str) -> sqlx::Result<Option<String>> {
        match self {
            Db::Postgres(p) => {
                sqlx::query_scalar(FIND_RUN)
                    .bind(idempotency_key)
                    .fetch_optional(p)
                    .await
            }
            Db::Sqlite(p) => {
                sqlx::query_scalar(&lite(FIND_RUN))
                    .bind(idempotency_key)
                    .fetch_optional(p)
                    .await
            }
        }
    }

    /// The numbers of the run whose idempotency key is `key`, a run of
    /// `workflow` (a workflow file's path) on the repository `repo_id`:
    /// `github.run_id`, of a count of every run of the repository's, and
    /// `github.run_number`, of the workflow's own. Given once: the same key
    /// is given the same numbers however often its event is handled, so the
    /// run submitted again is the one submitted before. Two workers that
    /// give one key its numbers at once take a number each that neither run
    /// has, a gap; no number is given two runs.
    pub async fn run_numbers(
        &self,
        repo_id: &str,
        workflow: &str,
        key: &str,
    ) -> sqlx::Result<(i64, i64)> {
        macro_rules! numbered {
            ($pool:expr, $find:expr, $next:expr, $keep:expr) => {{
                let given: Option<(i64, i64)> = sqlx::query_as($find)
                    .bind(key)
                    .fetch_optional($pool)
                    .await?;
                if let Some(given) = given {
                    return Ok(given);
                }
                let mut tx = $pool.begin().await?;
                let id: i64 = sqlx::query_scalar($next)
                    .bind(repo_id)
                    .bind("")
                    .fetch_one(&mut *tx)
                    .await?;
                let number: i64 = sqlx::query_scalar($next)
                    .bind(repo_id)
                    .bind(workflow)
                    .fetch_one(&mut *tx)
                    .await?;
                sqlx::query($keep)
                    .bind(key)
                    .bind(id)
                    .bind(number)
                    .execute(&mut *tx)
                    .await?;
                tx.commit().await?;
                let given: (i64, i64) = sqlx::query_as($find).bind(key).fetch_one($pool).await?;
                Ok(given)
            }};
        }
        match self {
            Db::Postgres(p) => numbered!(p, FIND_RUN_NUMBERS, NEXT_RUN_NUMBER, KEEP_RUN_NUMBERS),
            Db::Sqlite(p) => numbered!(
                p,
                &lite(FIND_RUN_NUMBERS),
                &lite(NEXT_RUN_NUMBER),
                &lite(KEEP_RUN_NUMBERS)
            ),
        }
    }

    /// Record a submitted run, and let go of its admission slot if it holds
    /// one. `false` when its key (or run id) is recorded already: a retry that
    /// got the same run back from dagron-api.
    pub async fn record_run(&self, run: &SubmittedRun) -> sqlx::Result<bool> {
        macro_rules! insert {
            ($pool:expr, $sql:expr, $delete:expr) => {{
                let mut tx = $pool.begin().await?;
                let rows = sqlx::query($sql)
                    .bind(&run.run_id)
                    .bind(&run.event_id)
                    .bind(&run.repo_id)
                    .bind(&run.cell)
                    .bind(&run.pipeline)
                    .bind(&run.idempotency_key)
                    .bind(run.on_failed)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected();
                sqlx::query($delete)
                    .bind(&run.idempotency_key)
                    .execute(&mut *tx)
                    .await?;
                tx.commit().await?;
                rows
            }};
        }
        let rows = match self {
            Db::Postgres(p) => insert!(p, INSERT_RUN_PG, DELETE_ADMISSION),
            Db::Sqlite(p) => insert!(p, INSERT_RUN_SQLITE, &lite(DELETE_ADMISSION)),
        };
        Ok(rows == 1)
    }

    /// Take an admission slot for the run under `key`, held for `hold`, if
    /// `admit` takes one more run beside those active now: the runs
    /// dagron-ci submitted that have not ended, and the other slots. What
    /// `admit` refuses with comes back as `Ok(Err(why))`, and nothing is
    /// taken. A slot the run holds already is renewed, and never counts
    /// against itself.
    pub async fn admit(
        &self,
        key: &str,
        hold: Duration,
        admit: impl FnOnce(u64) -> Result<(), String>,
    ) -> sqlx::Result<Result<(), String>> {
        self.admit_load(key, None, hold, false, |load| admit(load.active))
            .await
    }

    /// [`Db::admit`], for a run of `repo` (`forge host/owner/name`), which
    /// its slot names; with the active runs counted by repository as well
    /// where `by_repo` asks (`Admission::by_repository`).
    pub async fn admit_load(
        &self,
        key: &str,
        repo: Option<&str>,
        hold: Duration,
        by_repo: bool,
        admit: impl FnOnce(&crate::admission::Load) -> Result<(), String>,
    ) -> sqlx::Result<Result<(), String>> {
        self.admit_for(key, repo, None, hold, by_repo, false, admit)
            .await
    }

    /// [`Db::admit_load`], for a run that `caller` (its event's actor) asked
    /// for too, which its slot keeps; with the active runs counted by caller
    /// as well where `by_caller` asks (`Admission::by_caller`).
    #[allow(clippy::too_many_arguments)]
    pub async fn admit_for(
        &self,
        key: &str,
        repo: Option<&str>,
        caller: Option<&str>,
        hold: Duration,
        by_repo: bool,
        by_caller: bool,
        admit: impl FnOnce(&crate::admission::Load) -> Result<(), String>,
    ) -> sqlx::Result<Result<(), String>> {
        let hold_secs = hold.as_secs().max(1) as i64;
        let load = |active: i64, repos: Vec<String>, callers: Vec<String>| {
            let mut load = crate::admission::Load {
                active: active.max(0) as u64,
                by_repo: Default::default(),
                by_caller: Default::default(),
            };
            for repo in repos {
                *load.by_repo.entry(repo).or_default() += 1;
            }
            for caller in callers {
                *load.by_caller.entry(caller).or_default() += 1;
            }
            load
        };
        match self {
            Db::Postgres(p) => {
                let mut tx = p.begin().await?;
                sqlx::query(LOCK_ADMISSIONS_PG).execute(&mut *tx).await?;
                sqlx::query(LAPSE_ADMISSIONS_PG).execute(&mut *tx).await?;
                let active: i64 = sqlx::query_scalar(COUNT_ACTIVE)
                    .bind(key)
                    .fetch_one(&mut *tx)
                    .await?;
                let repos: Vec<String> = if by_repo {
                    sqlx::query_scalar(ACTIVE_REPOS)
                        .bind(key)
                        .fetch_all(&mut *tx)
                        .await?
                } else {
                    Vec::new()
                };
                let callers: Vec<String> = if by_caller {
                    sqlx::query_scalar(ACTIVE_CALLERS)
                        .bind(key)
                        .fetch_all(&mut *tx)
                        .await?
                } else {
                    Vec::new()
                };
                if let Err(why) = admit(&load(active, repos, callers)) {
                    tx.rollback().await?;
                    return Ok(Err(why));
                }
                sqlx::query(TAKE_ADMISSION_PG)
                    .bind(key)
                    .bind(hold_secs as f64)
                    .bind(repo)
                    .bind(caller)
                    .execute(&mut *tx)
                    .await?;
                tx.commit().await?;
            }
            Db::Sqlite(p) => {
                let mut tx = p.begin().await?;
                sqlx::query(LAPSE_ADMISSIONS_SQLITE)
                    .execute(&mut *tx)
                    .await?;
                let active: i64 = sqlx::query_scalar(&lite(COUNT_ACTIVE))
                    .bind(key)
                    .fetch_one(&mut *tx)
                    .await?;
                let repos: Vec<String> = if by_repo {
                    sqlx::query_scalar(&lite(ACTIVE_REPOS))
                        .bind(key)
                        .fetch_all(&mut *tx)
                        .await?
                } else {
                    Vec::new()
                };
                let callers: Vec<String> = if by_caller {
                    sqlx::query_scalar(&lite(ACTIVE_CALLERS))
                        .bind(key)
                        .fetch_all(&mut *tx)
                        .await?
                } else {
                    Vec::new()
                };
                if let Err(why) = admit(&load(active, repos, callers)) {
                    tx.rollback().await?;
                    return Ok(Err(why));
                }
                sqlx::query(TAKE_ADMISSION_SQLITE)
                    .bind(key)
                    .bind(hold_secs)
                    .bind(repo)
                    .bind(caller)
                    .execute(&mut *tx)
                    .await?;
                tx.commit().await?;
            }
        }
        Ok(Ok(()))
    }

    /// Let go of the admission slot taken for `key`: its run was not created.
    pub async fn release_admission(&self, key: &str) -> sqlx::Result<()> {
        match self {
            Db::Postgres(p) => sqlx::query(DELETE_ADMISSION)
                .bind(key)
                .execute(p)
                .await
                .map(drop),
            Db::Sqlite(p) => sqlx::query(&lite(DELETE_ADMISSION))
                .bind(key)
                .execute(p)
                .await
                .map(drop),
        }
    }

    /// Re-open the record of `run_id`, which was seen running `seen_ago` ago:
    /// `submitted` again and due for a look at once. `false`, and nothing
    /// written, when the record is not an ended one, or ended after that
    /// sight. `slot` is an admission slot the run was given to run again
    /// under: it goes in the same transaction, as when a run is recorded, so
    /// the run is counted once throughout.
    pub async fn reopen_run(
        &self,
        run_id: &str,
        seen_ago: Duration,
        slot: Option<&str>,
    ) -> sqlx::Result<bool> {
        macro_rules! reopen {
            ($pool:expr, $sql:expr, $ago:expr, $delete:expr) => {{
                let mut tx = $pool.begin().await?;
                let rows = sqlx::query($sql)
                    .bind(run_id)
                    .bind($ago)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected();
                if let Some(key) = slot {
                    sqlx::query($delete).bind(key).execute(&mut *tx).await?;
                }
                tx.commit().await?;
                rows == 1
            }};
        }
        Ok(match self {
            Db::Postgres(p) => reopen!(p, REOPEN_RUN_PG, seen_ago.as_secs_f64(), DELETE_ADMISSION),
            // To the millisecond its times are kept to, rounded up: an older
            // sight asks more of the end. As text, which the date modifier is
            // made of.
            Db::Sqlite(p) => reopen!(
                p,
                REOPEN_RUN_SQLITE,
                format!("{:.3}", (seen_ago.as_secs_f64() * 1000.0).ceil() / 1000.0),
                &lite(DELETE_ADMISSION)
            ),
        })
    }

    /// Of `run_ids`, the ones on record as over: the runs a sight of them
    /// running would re-open.
    pub async fn ended_among(&self, run_ids: &[String]) -> sqlx::Result<Vec<String>> {
        if run_ids.is_empty() {
            return Ok(Vec::new());
        }
        match self {
            Db::Postgres(p) => {
                sqlx::query_scalar(ENDED_AMONG_PG)
                    .bind(run_ids)
                    .fetch_all(p)
                    .await
            }
            Db::Sqlite(p) => {
                // Built rather than bound as one array: SQLite has none. The
                // values are still bound, and in chunks under its limit.
                let mut ended = Vec::new();
                for chunk in run_ids.chunks(500) {
                    let marks = std::iter::repeat_n("?", chunk.len())
                        .collect::<Vec<_>>()
                        .join(",");
                    let sql = format!(
                        "SELECT run_id FROM ci_runs \
                          WHERE status <> 'submitted' AND run_id IN ({marks})"
                    );
                    let mut q = sqlx::query_scalar::<_, String>(&sql);
                    for id in chunk {
                        q = q.bind(id);
                    }
                    ended.extend(q.fetch_all(p).await?);
                }
                Ok(ended)
            }
        }
    }

    /// The runs of `repo_id` at the commit `sha` that dagron-ci reported as
    /// check runs, oldest first. A run reported as commit statuses is not
    /// among them: it is of no check suite.
    pub async fn checked_runs_at(&self, repo_id: &str, sha: &str) -> sqlx::Result<Vec<String>> {
        match self {
            Db::Postgres(p) => {
                sqlx::query_scalar(CHECKED_RUNS_AT)
                    .bind(repo_id)
                    .bind(sha)
                    .fetch_all(p)
                    .await
            }
            Db::Sqlite(p) => {
                sqlx::query_scalar(&lite(CHECKED_RUNS_AT))
                    .bind(repo_id)
                    .bind(sha)
                    .fetch_all(p)
                    .await
            }
        }
    }

    /// The events of `repo_id` in which a merge-queue group, `git_ref` at
    /// `sha`, asked for its checks, each with its state, oldest first.
    pub async fn group_events(
        &self,
        repo_id: &str,
        git_ref: &str,
        sha: &str,
    ) -> sqlx::Result<Vec<(String, String)>> {
        match self {
            Db::Postgres(p) => {
                sqlx::query_as(GROUP_EVENTS)
                    .bind(repo_id)
                    .bind(git_ref)
                    .bind(sha)
                    .fetch_all(p)
                    .await
            }
            Db::Sqlite(p) => {
                sqlx::query_as(&lite(GROUP_EVENTS))
                    .bind(repo_id)
                    .bind(git_ref)
                    .bind(sha)
                    .fetch_all(p)
                    .await
            }
        }
    }

    /// Whether the group `git_ref` at `sha` asked for its checks since it was
    /// last let go of, where it was.
    pub async fn group_asked(&self, repo_id: &str, git_ref: &str, sha: &str) -> sqlx::Result<bool> {
        Ok(match self {
            Db::Postgres(p) => sqlx::query(GROUP_ASKED)
                .bind(repo_id)
                .bind(git_ref)
                .bind(sha)
                .fetch_optional(p)
                .await?
                .is_some(),
            Db::Sqlite(p) => sqlx::query(&lite(GROUP_ASKED))
                .bind(repo_id)
                .bind(git_ref)
                .bind(sha)
                .fetch_optional(p)
                .await?
                .is_some(),
        })
    }

    /// The runs of those events that are not seen to have ended, each with
    /// its cell, oldest first.
    pub async fn group_runs(
        &self,
        repo_id: &str,
        git_ref: &str,
        sha: &str,
    ) -> sqlx::Result<Vec<(String, String)>> {
        match self {
            Db::Postgres(p) => {
                sqlx::query_as(GROUP_RUNS)
                    .bind(repo_id)
                    .bind(git_ref)
                    .bind(sha)
                    .fetch_all(p)
                    .await
            }
            Db::Sqlite(p) => {
                sqlx::query_as(&lite(GROUP_RUNS))
                    .bind(repo_id)
                    .bind(git_ref)
                    .bind(sha)
                    .fetch_all(p)
                    .await
            }
        }
    }

    /// What the run `run_id` is for, or `None` when dagron-ci did not submit
    /// it: what a checkout credential is minted for, and no more.
    pub async fn run_commit(&self, run_id: &str) -> sqlx::Result<Option<RunCommit>> {
        macro_rules! read {
            ($pool:expr, $sql:expr, $repo:expr) => {{
                match sqlx::query($sql).bind(run_id).fetch_optional($pool).await? {
                    None => None,
                    Some(row) => Some(RunCommit {
                        repo: $repo(&row)?,
                        sha: row.try_get("sha")?,
                        cell: row.try_get("run_cell")?,
                        status: row.try_get("status")?,
                        connected: row.try_get::<i32, _>("connected")? != 0,
                        trust: row.try_get("trust")?,
                        bundle_id: row.try_get("bundle_id")?,
                    }),
                }
            }};
        }
        Ok(match self {
            Db::Postgres(p) => read!(p, RUN_COMMIT, repo_pg),
            Db::Sqlite(p) => read!(p, &lite(RUN_COMMIT), repo_sqlite),
        })
    }

    /// Claim the most overdue run whose commit statuses are due, for
    /// `lease`, as `claim`. `None` when none is due. A run whose event or
    /// repository cannot be read back comes with no report: the caller
    /// finishes it.
    pub async fn claim_report(
        &self,
        claim: &str,
        lease: Duration,
    ) -> sqlx::Result<Option<(String, Option<DueReport>)>> {
        let run_id: Option<String> = match self {
            Db::Postgres(p) => {
                sqlx::query_scalar(CLAIM_REPORT_PG)
                    .bind(claim)
                    .bind(lease.as_secs_f64())
                    .fetch_optional(p)
                    .await?
            }
            Db::Sqlite(p) => {
                sqlx::query_scalar(CLAIM_REPORT_SQLITE)
                    .bind(claim)
                    .bind(lease.as_secs().max(1) as i64)
                    .fetch_optional(p)
                    .await?
            }
        };
        let Some(run_id) = run_id else {
            return Ok(None);
        };
        macro_rules! view {
            ($pool:expr, $sql:expr, $repo:expr) => {{
                let row = sqlx::query($sql)
                    .bind(&run_id)
                    .fetch_optional($pool)
                    .await?;
                match row {
                    None => None,
                    Some(row) => {
                        let reported: String = row.try_get("reported")?;
                        Some(DueReport {
                            bundle_id: row.try_get("bundle_id")?,
                            run_id: row.try_get("run_id")?,
                            cell: row.try_get("run_cell")?,
                            pipeline: row.try_get("pipeline")?,
                            repo: $repo(&row)?,
                            kind: row.try_get("kind")?,
                            sha: row.try_get("sha")?,
                            git_ref: row.try_get("git_ref")?,
                            head_ref: row.try_get("head_ref")?,
                            trust: row.try_get("trust")?,
                            event_id: row.try_get("event_id")?,
                            reported: serde_json::from_str(&reported).unwrap_or_default(),
                            reopens: row.try_get::<i64, _>("reopens")?,
                            pr: row.try_get("pr")?,
                            jobs: row.try_get("jobs")?,
                        })
                    }
                }
            }};
        }
        let due = match self {
            Db::Postgres(p) => view!(p, REPORT_VIEW, repo_pg),
            Db::Sqlite(p) => view!(p, &lite(REPORT_VIEW), repo_sqlite),
        };
        Ok(Some((run_id, due)))
    }

    /// Keep `bundle`'s row, kept for `days`. When it expires, RFC 3339.
    pub async fn insert_bundle(&self, b: &Bundle, days: i64) -> sqlx::Result<String> {
        macro_rules! insert {
            ($pool:expr, $sql:expr, $ready:expr) => {
                sqlx::query_scalar::<_, String>($sql)
                    .bind(&b.id)
                    .bind(&b.repo_id)
                    .bind(&b.base_sha)
                    .bind(&b.principal)
                    .bind(&b.object_name)
                    .bind(b.size_bytes)
                    .bind(b.sha256.as_deref())
                    .bind($ready)
                    .bind(days as i32)
                    .fetch_one($pool)
                    .await
            };
        }
        match self {
            Db::Postgres(p) => insert!(p, INSERT_BUNDLE_PG, b.ready),
            Db::Sqlite(p) => insert!(p, INSERT_BUNDLE_SQLITE, b.ready as i32),
        }
    }

    /// Bundle `id`, while it is kept.
    pub async fn bundle(&self, id: &str) -> sqlx::Result<Option<Bundle>> {
        macro_rules! read {
            ($pool:expr, $sql:expr, $ready:ty) => {{
                match sqlx::query($sql).bind(id).fetch_optional($pool).await? {
                    None => None,
                    Some(row) => Some(Bundle {
                        id: row.try_get("id")?,
                        repo_id: row.try_get("repo_id")?,
                        base_sha: row.try_get("base_sha")?,
                        principal: row.try_get("principal")?,
                        object_name: row.try_get("object_name")?,
                        size_bytes: row.try_get("size_bytes")?,
                        sha256: row.try_get("sha256")?,
                        ready: row.try_get::<$ready, _>("ready")? as i64 != 0,
                        expires_at: row.try_get("expires_at")?,
                    }),
                }
            }};
        }
        Ok(match self {
            Db::Postgres(p) => read!(p, BUNDLE_PG, bool),
            Db::Sqlite(p) => read!(p, BUNDLE_SQLITE, i64),
        })
    }

    /// Bundle `id`'s bytes were checked: it is ready, of this size and digest.
    pub async fn mark_bundle_ready(&self, id: &str, size: i64, sha256: &str) -> sqlx::Result<()> {
        let sql = "UPDATE ci_bundles SET ready = $1, size_bytes = $2, sha256 = $3 WHERE id = $4";
        match self {
            Db::Postgres(p) => sqlx::query(sql)
                .bind(true)
                .bind(size)
                .bind(sha256)
                .bind(id)
                .execute(p)
                .await
                .map(drop),
            Db::Sqlite(p) => sqlx::query(&lite(sql))
                .bind(1)
                .bind(size)
                .bind(sha256)
                .bind(id)
                .execute(p)
                .await
                .map(drop),
        }
    }

    /// Up to `limit` bundles past their time: `(id, object_name)`.
    pub async fn expired_bundles(&self, limit: i64) -> sqlx::Result<Vec<(String, String)>> {
        match self {
            Db::Postgres(p) => {
                sqlx::query_as(EXPIRED_BUNDLES_PG)
                    .bind(limit)
                    .fetch_all(p)
                    .await
            }
            Db::Sqlite(p) => {
                sqlx::query_as(EXPIRED_BUNDLES_SQLITE)
                    .bind(limit)
                    .fetch_all(p)
                    .await
            }
        }
    }

    /// Bundle `id`'s row is gone.
    pub async fn delete_bundle(&self, id: &str) -> sqlx::Result<()> {
        let sql = "DELETE FROM ci_bundles WHERE id = $1";
        match self {
            Db::Postgres(p) => sqlx::query(sql).bind(id).execute(p).await.map(drop),
            Db::Sqlite(p) => sqlx::query(&lite(sql)).bind(id).execute(p).await.map(drop),
        }
    }

    /// Make the records of `runs` due, as their cell's outbox told of a
    /// change in each ([`TELL_REPORTS_PG`]). How many records were told.
    pub async fn tell_reports(&self, runs: &[String]) -> sqlx::Result<u64> {
        if runs.is_empty() {
            return Ok(0);
        }
        let runs = serde_json::to_string(runs).expect("a list of strings serialises");
        Ok(match self {
            Db::Postgres(p) => sqlx::query(TELL_REPORTS_PG)
                .bind(&runs)
                .execute(p)
                .await?
                .rows_affected(),
            Db::Sqlite(p) => sqlx::query(TELL_REPORTS_SQLITE)
                .bind(&runs)
                .execute(p)
                .await?
                .rows_affected(),
        })
    }

    /// Leave a run `claim` holds as `write` says. `false` when the claim was
    /// lost (its lease lapsed and another reporter took the run): then
    /// nothing is written.
    pub async fn finish_report(
        &self,
        run_id: &str,
        claim: &str,
        write: &ReportWrite,
    ) -> sqlx::Result<bool> {
        let finished = self.finish_reports(&[(run_id, claim, write)]).await?;
        Ok(!finished.is_empty())
    }

    /// Leave the runs `claim`s hold as their writes say, in one transaction:
    /// one statement for every run's record, one for every status the looks
    /// posted, and one for each pull request a comment was made due of. The
    /// runs whose claim still held, which are the ones written; a run whose
    /// claim lapsed is another reporter's, and nothing of it is written but
    /// its posts' log, since what was posted was posted.
    pub async fn finish_reports(
        &self,
        writes: &[(&str, &str, &ReportWrite)],
    ) -> sqlx::Result<Vec<String>> {
        if writes.is_empty() {
            return Ok(Vec::new());
        }
        let sqlite = matches!(self, Db::Sqlite(_));
        let rows: Vec<serde_json::Value> = writes
            .iter()
            .map(|(run_id, claim, write)| {
                // The soonest a notice brings the next look: the wait this
                // look was given where it failed or was rate limited, else
                // REPORT_HOLD.
                let hold = match (&write.error, write.next) {
                    (Some(_), Some(next)) => next.max(REPORT_HOLD),
                    _ => REPORT_HOLD,
                };
                let hold = if sqlite {
                    serde_json::json!(hold.as_secs().max(1))
                } else {
                    serde_json::json!(hold.as_secs_f64())
                };
                serde_json::json!({
                    "run_id": run_id,
                    "claim": claim,
                    // Postgres takes seconds as a float; SQLite as a whole
                    // number, at least one. None: never due again.
                    "next": write.next.map(|d| if sqlite {
                        serde_json::json!(d.as_secs().max(1))
                    } else {
                        serde_json::json!(d.as_secs_f64())
                    }),
                    "hold": hold,
                    "reported": serde_json::to_string(&write.reported).unwrap_or_else(|_| "{}".into()),
                    "status": write.status,
                    "error": write.error,
                    "started": write.started,
                    "meter": write.meter,
                    "jobs": write.jobs,
                    "whole_reruns": write.whole_reruns,
                })
            })
            .collect();
        let rows = serde_json::Value::Array(rows).to_string();
        let logged: Vec<&ReportLog> = writes
            .iter()
            .flat_map(|(_, _, w)| w.logged.iter())
            .collect();
        let logged = serde_json::to_string(&logged).unwrap_or_else(|_| "[]".into());
        macro_rules! finish {
            ($pool:expr, $sql:expr, $due:expr, $every:expr, $log:expr, $flakes:expr) => {{
                let mut tx = $pool.begin().await?;
                let finished: Vec<String> = sqlx::query_scalar($sql)
                    .bind(&rows)
                    .fetch_all(&mut *tx)
                    .await?;
                // What was posted was posted, whoever holds the run now.
                if logged != "[]" {
                    sqlx::query($log).bind(&logged).execute(&mut *tx).await?;
                }
                // The run's record and its pull request's comment being due
                // are one write: a comment made due and not written from this
                // would be written from what was there before, and stay so.
                for (run_id, _, write) in writes {
                    if let (true, Some((repo_id, pr))) =
                        (finished.iter().any(|f| f == run_id), &write.comment)
                    {
                        sqlx::query($due)
                            .bind(repo_id)
                            .bind(pr)
                            .bind($every)
                            .execute(&mut *tx)
                            .await?;
                    }
                }
                // A run's flakes are kept with its record, and make their
                // jobs' issues due, in the same write.
                let (insert, prune, count, flake_due): (&str, &str, &str, &str) = $flakes;
                for (run_id, _, write) in writes {
                    let Some(flakes) = write
                        .flakes
                        .as_ref()
                        .filter(|f| !f.jobs.is_empty() && finished.iter().any(|r| r == run_id))
                    else {
                        continue;
                    };
                    sqlx::query(prune)
                        .bind(flakes.days)
                        .execute(&mut *tx)
                        .await?;
                    for (job, failed) in &flakes.jobs {
                        let new: Option<(String, String)> = sqlx::query_as(insert)
                            .bind(*run_id)
                            .bind(job)
                            .bind(failed)
                            .fetch_optional(&mut *tx)
                            .await?;
                        let Some((repo_id, pipeline)) = new else {
                            continue;
                        };
                        let n: i64 = sqlx::query_scalar(count)
                            .bind(&repo_id)
                            .bind(&pipeline)
                            .bind(job)
                            .fetch_one(&mut *tx)
                            .await?;
                        if flakes.file_after > 0 && n >= flakes.file_after {
                            sqlx::query(flake_due)
                                .bind(&repo_id)
                                .bind(&pipeline)
                                .bind(job)
                                .execute(&mut *tx)
                                .await?;
                        }
                    }
                }
                tx.commit().await?;
                finished
            }};
        }
        let count = format!("SELECT CAST(count(*) AS BIGINT) {FLAKES_SINCE}");
        let (insert_lite, count_lite) = (lite(INSERT_FLAKE), lite(&count));
        Ok(match self {
            Db::Postgres(p) => finish!(
                p,
                FINISH_REPORTS_PG,
                COMMENT_DUE_PG,
                COMMENT_EVERY.as_secs_f64(),
                LOG_REPORTS_PG,
                (INSERT_FLAKE, PRUNE_FLAKES_PG, count.as_str(), FLAKE_DUE_PG)
            ),
            Db::Sqlite(p) => finish!(
                p,
                FINISH_REPORTS_SQLITE,
                COMMENT_DUE_SQLITE,
                COMMENT_EVERY.as_secs() as i64,
                LOG_REPORTS_SQLITE,
                (
                    insert_lite.as_str(),
                    PRUNE_FLAKES_SQLITE,
                    count_lite.as_str(),
                    FLAKE_DUE_SQLITE
                )
            ),
        })
    }

    /// Claim the most overdue comment of a pull request, for `lease`. `None`
    /// when none is due.
    pub async fn claim_comment(
        &self,
        claim: &str,
        lease: Duration,
    ) -> sqlx::Result<Option<DueComment>> {
        macro_rules! take {
            ($pool:expr, $sql:expr, $lease:expr, $by_id:expr, $repo:expr) => {{
                let row = sqlx::query($sql)
                    .bind(claim)
                    .bind($lease)
                    .fetch_optional($pool)
                    .await?;
                match row {
                    None => None,
                    Some(row) => {
                        let repo_id: String = row.try_get("repo_id")?;
                        let of = sqlx::query($by_id).bind(&repo_id).fetch_one($pool).await?;
                        Some(DueComment {
                            repo: $repo(&of)?,
                            pr: row.try_get("pr")?,
                            comment_id: row.try_get("comment_id")?,
                            said: row.try_get("said")?,
                        })
                    }
                }
            }};
        }
        Ok(match self {
            Db::Postgres(p) => take!(
                p,
                CLAIM_COMMENT_PG,
                lease.as_secs_f64(),
                REPO_BY_ID,
                repo_pg
            ),
            Db::Sqlite(p) => take!(
                p,
                CLAIM_COMMENT_SQLITE,
                lease.as_secs().max(1) as i64,
                &lite(REPO_BY_ID),
                repo_sqlite
            ),
        })
    }

    /// Leave a comment `claim` holds as `write` says. `false` when the claim
    /// was lost: then nothing is written.
    pub async fn finish_comment(
        &self,
        due: &DueComment,
        claim: &str,
        write: &CommentWrite,
    ) -> sqlx::Result<bool> {
        // Where this write was made, the next is not at once.
        let settle = match write.said {
            Some(_) => COMMENT_EVERY,
            None => Duration::ZERO,
        };
        macro_rules! finish {
            ($pool:expr, $sql:expr, $next:expr, $settle:expr) => {
                sqlx::query($sql)
                    .bind(&due.repo.id)
                    .bind(due.pr)
                    .bind(claim)
                    .bind($next)
                    .bind(write.said.as_deref())
                    .bind(write.error.as_deref())
                    .bind($settle)
                    .execute($pool)
                    .await?
                    .rows_affected()
            };
        }
        let rows = match self {
            Db::Postgres(p) => finish!(
                p,
                FINISH_COMMENT_PG,
                write.next.map(|d| d.as_secs_f64()),
                settle.as_secs_f64()
            ),
            Db::Sqlite(p) => finish!(
                p,
                FINISH_COMMENT_SQLITE,
                write.next.map(|d| d.as_secs().max(1) as i64),
                settle.as_secs() as i64
            ),
        };
        Ok(rows == 1)
    }

    /// Claim the most overdue issue of a job that flakes, for `lease`.
    /// `None` when none is due.
    pub async fn claim_flake_issue(
        &self,
        claim: &str,
        lease: Duration,
    ) -> sqlx::Result<Option<DueFlakeIssue>> {
        macro_rules! take {
            ($pool:expr, $sql:expr, $lease:expr, $by_id:expr, $repo:expr) => {{
                let row = sqlx::query($sql)
                    .bind(claim)
                    .bind($lease)
                    .fetch_optional($pool)
                    .await?;
                match row {
                    None => None,
                    Some(row) => {
                        let repo_id: String = row.try_get("repo_id")?;
                        let of = sqlx::query($by_id).bind(&repo_id).fetch_one($pool).await?;
                        Some(DueFlakeIssue {
                            repo: $repo(&of)?,
                            pipeline: row.try_get("pipeline")?,
                            job: row.try_get("job")?,
                            issue: row.try_get("issue")?,
                            said: row.try_get("said")?,
                        })
                    }
                }
            }};
        }
        Ok(match self {
            Db::Postgres(p) => take!(
                p,
                CLAIM_FLAKE_ISSUE_PG,
                lease.as_secs_f64(),
                REPO_BY_ID,
                repo_pg
            ),
            Db::Sqlite(p) => take!(
                p,
                CLAIM_FLAKE_ISSUE_SQLITE,
                lease.as_secs().max(1) as i64,
                &lite(REPO_BY_ID),
                repo_sqlite
            ),
        })
    }

    /// A job's flakes since its last issue was closed, newest first, at
    /// most `limit` of them; and how many there are in all.
    pub async fn flakes_of(
        &self,
        due: &DueFlakeIssue,
        limit: i64,
    ) -> sqlx::Result<(Vec<FlakeRow>, i64)> {
        let list = format!(
            "SELECT f.run_id, f.cell, f.sha, CAST(f.failed AS BIGINT) AS failed, \
                    CAST(f.seen_at AS TEXT) AS seen_at {FLAKES_SINCE} \
             ORDER BY f.seen_at DESC, f.run_id LIMIT $4"
        );
        let count = format!("SELECT CAST(count(*) AS BIGINT) {FLAKES_SINCE}");
        type Row = (String, String, String, i64, String);
        macro_rules! read {
            ($pool:expr, $list:expr, $count:expr) => {{
                let rows: Vec<Row> = sqlx::query_as($list)
                    .bind(&due.repo.id)
                    .bind(&due.pipeline)
                    .bind(&due.job)
                    .bind(limit)
                    .fetch_all($pool)
                    .await?;
                let n: i64 = sqlx::query_scalar($count)
                    .bind(&due.repo.id)
                    .bind(&due.pipeline)
                    .bind(&due.job)
                    .fetch_one($pool)
                    .await?;
                (rows, n)
            }};
        }
        let (rows, n) = match self {
            Db::Postgres(p) => read!(p, &list, &count),
            Db::Sqlite(p) => read!(p, &lite(&list), &lite(&count)),
        };
        let rows = rows
            .into_iter()
            .map(|(run_id, cell, sha, failed, seen_at)| FlakeRow {
                run_id,
                cell,
                sha,
                failed,
                seen_at,
            })
            .collect();
        Ok((rows, n))
    }

    /// Leave an issue `claim` holds as `write` says. `false` when the claim
    /// was lost: then nothing is written.
    pub async fn finish_flake_issue(
        &self,
        due: &DueFlakeIssue,
        claim: &str,
        write: &CommentWrite,
    ) -> sqlx::Result<bool> {
        macro_rules! finish {
            ($pool:expr, $sql:expr, $next:expr) => {
                sqlx::query($sql)
                    .bind(&due.repo.id)
                    .bind(&due.pipeline)
                    .bind(&due.job)
                    .bind(claim)
                    .bind($next)
                    .bind(write.said.as_deref())
                    .bind(write.error.as_deref())
                    .execute($pool)
                    .await?
                    .rows_affected()
            };
        }
        let rows = match self {
            Db::Postgres(p) => finish!(
                p,
                FINISH_FLAKE_ISSUE_PG,
                write.next.map(|d| d.as_secs_f64())
            ),
            Db::Sqlite(p) => finish!(
                p,
                FINISH_FLAKE_ISSUE_SQLITE,
                write.next.map(|d| d.as_secs().max(1) as i64)
            ),
        };
        Ok(rows == 1)
    }

    /// Keep the forge's number for a job's issue, once one is opened; or,
    /// with `closed`, that a person closed it (when, where the forge said).
    pub async fn flake_issue_is(
        &self,
        due: &DueFlakeIssue,
        opened: Option<&str>,
        closed: Option<&str>,
    ) -> sqlx::Result<()> {
        macro_rules! set {
            ($pool:expr, $sql:expr, $value:expr) => {
                sqlx::query($sql)
                    .bind(&due.repo.id)
                    .bind(&due.pipeline)
                    .bind(&due.job)
                    .bind($value)
                    .execute($pool)
                    .await?
                    .rows_affected()
            };
        }
        match (self, opened) {
            (Db::Postgres(p), Some(n)) => set!(p, FLAKE_ISSUE_MADE, n),
            (Db::Sqlite(p), Some(n)) => set!(p, &lite(FLAKE_ISSUE_MADE), n),
            (Db::Postgres(p), None) => set!(p, FLAKE_ISSUE_CLOSED_PG, closed),
            (Db::Sqlite(p), None) => set!(p, FLAKE_ISSUE_CLOSED_SQLITE, closed),
        };
        Ok(())
    }

    /// Keep the forge's id for a pull request's comment, once one is made.
    pub async fn comment_made(&self, repo_id: &str, pr: i64, comment_id: &str) -> sqlx::Result<()> {
        macro_rules! made {
            ($pool:expr, $sql:expr) => {
                sqlx::query($sql)
                    .bind(repo_id)
                    .bind(pr)
                    .bind(comment_id)
                    .execute($pool)
                    .await?
                    .rows_affected()
            };
        }
        match self {
            Db::Postgres(p) => made!(p, COMMENT_MADE),
            Db::Sqlite(p) => made!(p, &lite(COMMENT_MADE)),
        };
        Ok(())
    }

    /// The runs of a pull request's latest commit, one a pipeline: the
    /// latest of each, by its pipeline. None where the pull request has had
    /// no run.
    pub async fn pr_runs(&self, repo_id: &str, pr: i64) -> sqlx::Result<Vec<PrRun>> {
        macro_rules! runs {
            ($pool:expr, $sql:expr) => {{
                let mut runs: Vec<PrRun> = Vec::new();
                for row in sqlx::query($sql)
                    .bind(repo_id)
                    .bind(pr)
                    .fetch_all($pool)
                    .await?
                {
                    let run = PrRun {
                        run_id: row.try_get("run_id")?,
                        cell: row.try_get("run_cell")?,
                        pipeline: row.try_get("pipeline")?,
                        status: row.try_get("status")?,
                        jobs: row.try_get("jobs")?,
                        sha: row.try_get("sha")?,
                    };
                    // The rows come latest event first: the first names the
                    // commit, and a pipeline's first is its latest run.
                    let latest = runs.first().map_or(true, |first| first.sha == run.sha);
                    if latest && runs.iter().all(|kept| kept.pipeline != run.pipeline) {
                        runs.push(run);
                    }
                }
                runs
            }};
        }
        let mut runs = match self {
            Db::Postgres(p) => runs!(p, PR_RUNS),
            Db::Sqlite(p) => runs!(p, &lite(PR_RUNS)),
        };
        runs.sort_by(|a, b| a.pipeline.cmp(&b.pipeline));
        Ok(runs)
    }

    /// Claim the most overdue run end due to the meter, for `lease`, with what
    /// the meter is told of it. `None` when none is due.
    pub async fn claim_meter_end(
        &self,
        lease: Duration,
    ) -> sqlx::Result<Option<crate::meter::EndedRun>> {
        let run_id: Option<String> = match self {
            Db::Postgres(p) => {
                sqlx::query_scalar(CLAIM_METER_END_PG)
                    .bind(lease.as_secs_f64())
                    .fetch_optional(p)
                    .await?
            }
            Db::Sqlite(p) => {
                sqlx::query_scalar(CLAIM_METER_END_SQLITE)
                    .bind(lease.as_secs().max(1) as i64)
                    .fetch_optional(p)
                    .await?
            }
        };
        let Some(run_id) = run_id else {
            return Ok(None);
        };
        macro_rules! view {
            ($pool:expr, $sql:expr) => {
                sqlx::query($sql)
                    .bind(&run_id)
                    .fetch_optional($pool)
                    .await?
            };
        }
        let row = match self {
            Db::Postgres(p) => view!(p, METER_END_VIEW).map(|r| ended_run(&r)),
            Db::Sqlite(p) => view!(p, &lite(METER_END_VIEW)).map(|r| ended_run(&r)),
        };
        row.transpose()
    }

    /// Claim the most overdue failed run whose `failed` event is due,
    /// for `lease`. `None` when none is due.
    pub async fn claim_failure(&self, lease: Duration) -> sqlx::Result<Option<DueFailure>> {
        let row = match self {
            Db::Postgres(p) => sqlx::query(CLAIM_FAILURE_PG)
                .bind(lease.as_secs_f64())
                .fetch_optional(p)
                .await?
                .map(|r| -> sqlx::Result<DueFailure> {
                    Ok(DueFailure {
                        run_id: r.try_get("run_id")?,
                        cell: r.try_get("cell")?,
                        pipeline: r.try_get("pipeline")?,
                        until: r.try_get("until")?,
                    })
                }),
            Db::Sqlite(p) => sqlx::query(CLAIM_FAILURE_SQLITE)
                .bind(lease.as_secs().max(1) as i64)
                .fetch_optional(p)
                .await?
                .map(|r| -> sqlx::Result<DueFailure> {
                    Ok(DueFailure {
                        run_id: r.try_get("run_id")?,
                        cell: r.try_get("cell")?,
                        pipeline: r.try_get("pipeline")?,
                        until: r.try_get("until")?,
                    })
                }),
        };
        row.transpose()
    }

    /// Queue the `failed` event of the run `due` holds, with `inputs`, and
    /// take the run off what is due, in one transaction: its event id, or
    /// `None` where the claim was lost (another replica raised it).
    pub async fn raise_failure(
        &self,
        due: &DueFailure,
        inputs: &BTreeMap<String, String>,
    ) -> sqlx::Result<Option<String>> {
        let id = uuid::Uuid::now_v7().to_string();
        let inputs = serde_json::to_string(inputs).expect("a map of strings serializes");
        macro_rules! raise {
            ($pool:expr, $raised:expr, $insert:expr) => {{
                let mut tx = $pool.begin().await?;
                let held = sqlx::query($raised)
                    .bind(&due.run_id)
                    .bind(&due.until)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
                    == 1;
                if !held {
                    tx.rollback().await?;
                    return Ok(None);
                }
                sqlx::query($insert)
                    .bind(&due.run_id)
                    .bind(&id)
                    .bind(&inputs)
                    .execute(&mut *tx)
                    .await?;
                tx.commit().await?;
            }};
        }
        match self {
            Db::Postgres(p) => raise!(p, FAILURE_RAISED_PG, INSERT_FAILED),
            Db::Sqlite(p) => raise!(p, FAILURE_RAISED_SQLITE, &lite(INSERT_FAILED)),
        }
        Ok(Some(id))
    }

    /// The meter took `run_id`'s end: it is due no more.
    pub async fn meter_told(&self, run_id: &str) -> sqlx::Result<()> {
        match self {
            Db::Postgres(p) => sqlx::query(METER_TOLD)
                .bind(run_id)
                .execute(p)
                .await
                .map(drop),
            Db::Sqlite(p) => sqlx::query(&lite(METER_TOLD))
                .bind(run_id)
                .execute(p)
                .await
                .map(drop),
        }
    }

    /// The event a caller's earlier request under `key` queued, and that
    /// request's fingerprint.
    pub async fn find_api_request(
        &self,
        principal: &str,
        key: &str,
    ) -> sqlx::Result<Option<(String, String)>> {
        match self {
            Db::Postgres(p) => {
                sqlx::query_as(FIND_API_REQUEST)
                    .bind(principal)
                    .bind(key)
                    .fetch_optional(p)
                    .await
            }
            Db::Sqlite(p) => {
                sqlx::query_as(&lite(FIND_API_REQUEST))
                    .bind(principal)
                    .bind(key)
                    .fetch_optional(p)
                    .await
            }
        }
    }

    /// Queue a dispatch the run API accepted, with its inputs and the
    /// pipeline it names, if any, and with the
    /// caller's `Idempotency-Key` when it sent one, in one transaction. A key
    /// the caller used before writes nothing and says which event it made.
    ///
    /// A dispatch's commit key ends in its event id: two dispatches of one
    /// commit are two runs, and the key's dedupe happens here instead.
    pub async fn record_dispatch(
        &self,
        repo: &Repo,
        event: &NewEvent,
        inputs: &BTreeMap<String, String>,
        pipeline: Option<&str>,
        bundle: Option<&str>,
        request: Option<&RequestKey<'_>>,
    ) -> sqlx::Result<Dispatched> {
        let id = uuid::Uuid::now_v7().to_string();
        let key = format!("{}:{id}", commit_key(&repo.id, event));
        let inputs = (!inputs.is_empty())
            .then(|| serde_json::to_string(inputs).expect("a map of strings serializes"));
        macro_rules! record {
            ($pool:expr, $insert:expr, $request:expr) => {{
                let mut tx = $pool.begin().await?;
                sqlx::query($insert)
                    .bind(&id)
                    .bind(&repo.id)
                    .bind(event.kind.as_str())
                    .bind(&event.git_ref)
                    .bind(&event.sha)
                    .bind(event.actor.as_deref())
                    .bind(event.trust.as_str())
                    .bind(&key)
                    .bind(inputs.as_deref())
                    .bind(pipeline)
                    .bind(bundle)
                    .execute(&mut *tx)
                    .await?;
                if let Some(r) = request {
                    let fresh = sqlx::query($request)
                        .bind(r.principal)
                        .bind(r.key)
                        .bind(r.fingerprint)
                        .bind(&id)
                        .execute(&mut *tx)
                        .await?
                        .rows_affected()
                        == 1;
                    if !fresh {
                        // The event goes with the transaction.
                        tx.rollback().await?;
                        let (event_id, fingerprint) = self
                            .find_api_request(r.principal, r.key)
                            .await?
                            .ok_or(sqlx::Error::RowNotFound)?;
                        return Ok(Dispatched::Repeat {
                            event_id,
                            same: fingerprint == r.fingerprint,
                        });
                    }
                }
                tx.commit().await?;
            }};
        }
        match self {
            Db::Postgres(p) => record!(p, INSERT_DISPATCH, INSERT_API_REQUEST),
            Db::Sqlite(p) => record!(p, &lite(INSERT_DISPATCH), &lite(INSERT_API_REQUEST)),
        }
        Ok(Dispatched::Event(id))
    }

    /// One event as the run API shows it, with the engine runs it submitted.
    pub async fn event_view(&self, id: &str) -> sqlx::Result<Option<EventView>> {
        macro_rules! view {
            ($row:expr) => {{
                let row = $row;
                EventView {
                    id: row.try_get("id")?,
                    repo: row.try_get("repo")?,
                    cell: row.try_get("cell")?,
                    kind: row.try_get("kind")?,
                    git_ref: row.try_get("ref")?,
                    sha: row.try_get("sha")?,
                    source: row.try_get("source")?,
                    trust: row.try_get("trust")?,
                    actor: row.try_get("actor")?,
                    state: row.try_get("state")?,
                    reason: row.try_get("reason")?,
                    attempts: row.try_get("attempts")?,
                    pipeline: row.try_get("pipeline")?,
                    bundle: row.try_get("bundle_id")?,
                    bundle_sha256: row.try_get("bundle_sha256")?,
                    runs: Vec::new(),
                }
            }};
        }
        type Run = (String, String, String, String, String);
        let (view, runs): (Option<EventView>, Vec<Run>) = match self {
            Db::Postgres(p) => (
                match sqlx::query(EVENT_VIEW).bind(id).fetch_optional(p).await? {
                    Some(row) => Some(view!(row)),
                    None => None,
                },
                sqlx::query_as(RUNS_OF_EVENT).bind(id).fetch_all(p).await?,
            ),
            Db::Sqlite(p) => (
                match sqlx::query(&lite(EVENT_VIEW))
                    .bind(id)
                    .fetch_optional(p)
                    .await?
                {
                    Some(row) => Some(view!(row)),
                    None => None,
                },
                sqlx::query_as(&lite(RUNS_OF_EVENT))
                    .bind(id)
                    .fetch_all(p)
                    .await?,
            ),
        };
        Ok(view.map(|view| EventView {
            runs: runs
                .into_iter()
                .map(|(id, cell, pipeline, status, key)| EngineRun {
                    id,
                    cell,
                    pipeline,
                    status,
                    key,
                })
                .collect(),
            ..view
        }))
    }

    /// The newest events, every state or one (`state` empty for all).
    pub async fn list_events(&self, state: &str, limit: i64) -> sqlx::Result<Vec<EventRow>> {
        type Row = (
            String,
            String,
            String,
            String,
            String,
            String,
            i64,
            Option<String>,
        );
        let rows: Vec<Row> = match self {
            Db::Postgres(p) => sqlx::query_as::<
                _,
                (
                    String,
                    String,
                    String,
                    String,
                    String,
                    String,
                    i32,
                    Option<String>,
                ),
            >(LIST_EVENTS)
            .bind(state)
            .bind(limit)
            .fetch_all(p)
            .await?
            .into_iter()
            .map(|r| (r.0, r.1, r.2, r.3, r.4, r.5, i64::from(r.6), r.7))
            .collect(),
            Db::Sqlite(p) => {
                sqlx::query_as(&lite(LIST_EVENTS))
                    .bind(state)
                    .bind(limit)
                    .fetch_all(p)
                    .await?
            }
        };
        Ok(rows
            .into_iter()
            .map(
                |(id, repo, kind, git_ref, sha, state, attempts, reason)| EventRow {
                    id,
                    repo,
                    kind,
                    git_ref,
                    sha,
                    state,
                    attempts,
                    reason,
                },
            )
            .collect())
    }

    /// Claim the most overdue repository that polls, for `lease`, as
    /// `claim`. `default_secs` is CI_POLL_SECS: the interval of a repository
    /// that sets none. `None` when no repository is due.
    pub async fn claim_sweep(
        &self,
        claim: &str,
        lease: Duration,
        default_secs: u64,
    ) -> sqlx::Result<Option<DueRepo>> {
        let default_secs = default_secs.min(i64::MAX as u64) as i64;
        match self {
            Db::Postgres(p) => {
                let Some(row) = sqlx::query(CLAIM_SWEEP_PG)
                    .bind(claim)
                    .bind(lease.as_secs_f64())
                    .bind(default_secs)
                    .fetch_optional(p)
                    .await?
                else {
                    return Ok(None);
                };
                Ok(Some(DueRepo {
                    repo: repo_pg(&row)?,
                    poll_secs: row.try_get::<i64, _>("poll_secs")?.max(0) as u64,
                    baseline: row.try_get("baseline")?,
                    etag: row.try_get("poll_etag")?,
                }))
            }
            Db::Sqlite(p) => {
                let Some(row) = sqlx::query(CLAIM_SWEEP_SQLITE)
                    .bind(claim)
                    .bind(lease.as_secs().max(1) as i64)
                    .bind(default_secs)
                    .fetch_optional(p)
                    .await?
                else {
                    return Ok(None);
                };
                Ok(Some(DueRepo {
                    repo: repo_sqlite(&row)?,
                    poll_secs: row.try_get::<i64, _>("poll_secs")?.max(0) as u64,
                    baseline: row.try_get::<i64, _>("baseline")? != 0,
                    etag: row.try_get("poll_etag")?,
                }))
            }
        }
    }

    /// What the last sweep of a repository saw: each ref and its commit.
    pub async fn poll_refs(&self, repo_id: &str) -> sqlx::Result<BTreeMap<String, String>> {
        let rows: Vec<(String, String)> = match self {
            Db::Postgres(p) => sqlx::query_as(POLL_REFS).bind(repo_id).fetch_all(p).await?,
            Db::Sqlite(p) => {
                sqlx::query_as(&lite(POLL_REFS))
                    .bind(repo_id)
                    .fetch_all(p)
                    .await?
            }
        };
        Ok(rows.into_iter().collect())
    }

    /// Finish a sweep `claim` holds: queue its events, record what it saw, and
    /// make the repository due again after `next`, in one transaction. The ids
    /// of the events queued, or `None` when the claim was lost (its lease
    /// lapsed and another sweeper took the repository): then nothing is written.
    pub async fn finish_sweep(
        &self,
        repo_id: &str,
        claim: &str,
        write: &SweepWrite,
        next: Duration,
    ) -> sqlx::Result<Option<Vec<String>>> {
        macro_rules! finish {
            ($pool:expr, $finish:expr, $set:expr, $drop:expr, $insert:expr, $secs:expr) => {{
                let mut tx = $pool.begin().await?;
                // The fence first: on Postgres its row lock also keeps another
                // sweeper from claiming the repository until this commits.
                let held = sqlx::query($finish)
                    .bind(repo_id)
                    .bind(claim)
                    .bind($secs)
                    .bind(write.etag.as_deref())
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
                    == 1;
                if !held {
                    tx.rollback().await?;
                    return Ok(None);
                }
                for (name, sha) in &write.set {
                    sqlx::query($set)
                        .bind(repo_id)
                        .bind(name)
                        .bind(sha)
                        .execute(&mut *tx)
                        .await?;
                }
                for name in &write.drop {
                    sqlx::query($drop)
                        .bind(repo_id)
                        .bind(name)
                        .execute(&mut *tx)
                        .await?;
                }
                let mut ids = Vec::with_capacity(write.events.len());
                for event in &write.events {
                    let id = uuid::Uuid::now_v7().to_string();
                    sqlx::query($insert)
                        .bind(&id)
                        .bind(repo_id)
                        .bind("poll")
                        .bind(None::<&str>)
                        .bind(event.kind.as_str())
                        .bind(&event.git_ref)
                        .bind(&event.sha)
                        .bind(event.base_sha.as_deref())
                        .bind(event.pr)
                        .bind(event.actor.as_deref())
                        .bind(event.trust.as_str())
                        .bind(commit_key(repo_id, event))
                        .bind(None::<&str>)
                        .bind(event.base_ref.as_deref())
                        .bind(event.commit.as_deref())
                        .bind(event.association.as_deref())
                        .bind(event.head_ref.as_deref())
                        .execute(&mut *tx)
                        .await?;
                    ids.push(id);
                }
                tx.commit().await?;
                Ok(Some(ids))
            }};
        }
        match self {
            Db::Postgres(p) => finish!(
                p,
                FINISH_SWEEP_PG,
                SET_POLL_REF,
                DROP_POLL_REF,
                INSERT_EVENT,
                next.as_secs_f64()
            ),
            Db::Sqlite(p) => {
                let (set, drop, insert) =
                    (lite(SET_POLL_REF), lite(DROP_POLL_REF), lite(INSERT_EVENT));
                finish!(
                    p,
                    FINISH_SWEEP_SQLITE,
                    &set,
                    &drop,
                    &insert,
                    next.as_secs().max(1) as i64
                )
            }
        }
    }

    /// End a sweep `claim` holds that failed: record why, and make the
    /// repository due again after `next`. `false` when the claim was lost.
    pub async fn fail_sweep(
        &self,
        repo_id: &str,
        claim: &str,
        reason: &str,
        next: Duration,
    ) -> sqlx::Result<bool> {
        let rows = match self {
            Db::Postgres(p) => sqlx::query(FAIL_SWEEP_PG)
                .bind(repo_id)
                .bind(claim)
                .bind(next.as_secs_f64())
                .bind(reason)
                .execute(p)
                .await?
                .rows_affected(),
            Db::Sqlite(p) => sqlx::query(FAIL_SWEEP_SQLITE)
                .bind(repo_id)
                .bind(claim)
                .bind(next.as_secs().max(1) as i64)
                .bind(reason)
                .execute(p)
                .await?
                .rows_affected(),
        };
        Ok(rows == 1)
    }

    /// Claim the GitHub repository whose schedules are most overdue to be
    /// looked at, for `lease`, as `claim` ([`crate::schedule`]). `None` when
    /// none is due.
    pub async fn claim_schedule(
        &self,
        claim: &str,
        lease: Duration,
    ) -> sqlx::Result<Option<DueSchedule>> {
        match self {
            Db::Postgres(p) => {
                let Some(row) = sqlx::query(CLAIM_SCHEDULE_PG)
                    .bind(claim)
                    .bind(lease.as_secs_f64())
                    .fetch_optional(p)
                    .await?
                else {
                    return Ok(None);
                };
                Ok(Some(DueSchedule {
                    repo: repo_pg(&row)?,
                    sha: row.try_get("schedule_sha")?,
                }))
            }
            Db::Sqlite(p) => {
                let Some(row) = sqlx::query(CLAIM_SCHEDULE_SQLITE)
                    .bind(claim)
                    .bind(lease.as_secs().max(1) as i64)
                    .fetch_optional(p)
                    .await?
                else {
                    return Ok(None);
                };
                Ok(Some(DueSchedule {
                    repo: repo_sqlite(&row)?,
                    sha: row.try_get("schedule_sha")?,
                }))
            }
        }
    }

    /// A repository's schedules, as last written.
    pub async fn schedules(&self, repo_id: &str) -> sqlx::Result<Vec<ScheduleRow>> {
        let rows: Vec<(String, String, Option<i64>, Option<i64>)> = match self {
            Db::Postgres(p) => sqlx::query_as(SCHEDULES).bind(repo_id).fetch_all(p).await?,
            Db::Sqlite(p) => {
                sqlx::query_as(&lite(SCHEDULES))
                    .bind(repo_id)
                    .fetch_all(p)
                    .await?
            }
        };
        Ok(rows
            .into_iter()
            .map(|(workflow, cron, next_at, last_at)| ScheduleRow {
                workflow,
                cron,
                next_at,
                last_at,
            })
            .collect())
    }

    /// Finish a look `claim` holds at a repository's schedules: write them
    /// as `rows` (read at `sha`), queue a `schedule` event for each of
    /// `fired`, and make the repository due again after `next`, in one
    /// transaction. The ids of the events queued, or `None` when the claim
    /// was lost: then nothing is written, and no cron fires twice.
    pub async fn finish_schedule(
        &self,
        repo_id: &str,
        claim: &str,
        sha: Option<&str>,
        rows: &[ScheduleRow],
        fired: &[Fired],
        next: Duration,
    ) -> sqlx::Result<Option<Vec<String>>> {
        macro_rules! finish {
            ($pool:expr, $finish:expr, $secs:expr, $drop:expr, $set:expr, $queue:expr) => {{
                let mut tx = $pool.begin().await?;
                let held = sqlx::query($finish)
                    .bind(repo_id)
                    .bind(claim)
                    .bind($secs)
                    .bind(sha)
                    .execute(&mut *tx)
                    .await?
                    .rows_affected()
                    == 1;
                if !held {
                    tx.rollback().await?;
                    return Ok(None);
                }
                sqlx::query($drop).bind(repo_id).execute(&mut *tx).await?;
                for row in rows {
                    sqlx::query($set)
                        .bind(repo_id)
                        .bind(&row.workflow)
                        .bind(&row.cron)
                        .bind(row.next_at)
                        .bind(row.last_at)
                        .execute(&mut *tx)
                        .await?;
                }
                let mut ids = Vec::with_capacity(fired.len());
                for f in fired {
                    let id = uuid::Uuid::now_v7().to_string();
                    let inputs = serde_json::to_string(&BTreeMap::from([(
                        crate::pipeline::compile::SCHEDULE_INPUT,
                        f.cron.as_str(),
                    )]))
                    .expect("a map of strings serializes");
                    sqlx::query($queue)
                        .bind(&id)
                        .bind(repo_id)
                        .bind(&f.git_ref)
                        .bind(&f.sha)
                        .bind(f.key(repo_id))
                        .bind(inputs)
                        // By its path: no native pipeline of the same file
                        // name is taken for it.
                        .bind(format!(
                            "{}/{}",
                            crate::pipeline::github::WORKFLOW_DIR,
                            f.workflow
                        ))
                        .execute(&mut *tx)
                        .await?;
                    ids.push(id);
                }
                tx.commit().await?;
                Ok(Some(ids))
            }};
        }
        match self {
            Db::Postgres(p) => finish!(
                p,
                FINISH_SCHEDULE_PG,
                next.as_secs_f64(),
                DROP_SCHEDULES,
                SET_SCHEDULE,
                INSERT_SCHEDULED
            ),
            Db::Sqlite(p) => finish!(
                p,
                FINISH_SCHEDULE_SQLITE,
                next.as_secs().max(1) as i64,
                &lite(DROP_SCHEDULES),
                &lite(SET_SCHEDULE),
                &lite(INSERT_SCHEDULED)
            ),
        }
    }

    /// End a look `claim` holds that failed: record why, and make the
    /// repository due again after `next`. `false` when the claim was lost.
    pub async fn fail_schedule(
        &self,
        repo_id: &str,
        claim: &str,
        reason: &str,
        next: Duration,
    ) -> sqlx::Result<bool> {
        let rows = match self {
            Db::Postgres(p) => sqlx::query(FAIL_SCHEDULE_PG)
                .bind(repo_id)
                .bind(claim)
                .bind(next.as_secs_f64())
                .bind(reason)
                .execute(p)
                .await?
                .rows_affected(),
            Db::Sqlite(p) => sqlx::query(FAIL_SCHEDULE_SQLITE)
                .bind(repo_id)
                .bind(claim)
                .bind(next.as_secs().max(1) as i64)
                .bind(reason)
                .execute(p)
                .await?
                .rows_affected(),
        };
        Ok(rows == 1)
    }

    /// Close the pool; called once the server has drained.
    pub async fn close(&self) {
        match self {
            Db::Postgres(p) => p.close().await,
            Db::Sqlite(p) => p.close().await,
        }
    }
}

fn repo_pg(row: &sqlx::postgres::PgRow) -> sqlx::Result<Repo> {
    Ok(Repo {
        id: row.try_get("id")?,
        forge: row.try_get("forge")?,
        forge_host: row.try_get("forge_host")?,
        full_name: row.try_get("full_name")?,
        workflow: row.try_get("workflow")?,
        cell: row.try_get("cell")?,
        mode: mode_of(&row.try_get::<String, _>("mode")?)?,
    })
}

fn repo_sqlite(row: &sqlx::sqlite::SqliteRow) -> sqlx::Result<Repo> {
    Ok(Repo {
        id: row.try_get("id")?,
        forge: row.try_get("forge")?,
        forge_host: row.try_get("forge_host")?,
        full_name: row.try_get("full_name")?,
        workflow: row.try_get("workflow")?,
        cell: row.try_get("cell")?,
        mode: mode_of(&row.try_get::<String, _>("mode")?)?,
    })
}

/// A repository's mode as the table holds it, which its CHECK keeps to the
/// three.
pub(crate) fn mode_of(name: &str) -> sqlx::Result<Mode> {
    Mode::parse(name).ok_or_else(|| {
        sqlx::Error::Decode(format!("ci_repos.mode {name:?} is none of {:?}", Mode::NAMES).into())
    })
}

/// `CREATE SCHEMA IF NOT EXISTS` is not atomic against another session doing
/// the same: two replicas starting together can both pass the existence check,
/// and the loser fails on the catalog's unique index. Losing that race means
/// the schema exists, which is all this wanted.
async fn create_schema(pool: &PgPool, schema: &str) -> anyhow::Result<()> {
    // `schema` passed `is_identifier` in the config: no quoting can be escaped.
    match sqlx::query(&format!("CREATE SCHEMA IF NOT EXISTS {schema}"))
        .execute(pool)
        .await
    {
        Ok(_) => Ok(()),
        Err(e)
            if e.as_database_error().is_some_and(|db| {
                matches!(db.code().as_deref(), Some("42P06") | Some("23505"))
            }) =>
        {
            Ok(())
        }
        Err(e) => Err(anyhow::Error::new(e).context(format!(
            "creating schema {schema} (create it beforehand if this role may not)"
        ))),
    }
}

/// How long a migrator waiting for another's migration lock sleeps between
/// tries.
const MIGRATION_LOCK_POLL: Duration = Duration::from_millis(200);

/// How often a migrator still waiting for the lock says so again, louder.
const MIGRATION_LOCK_WARN_EVERY: Duration = Duration::from_secs(60);

/// Bring dagron-ci's Postgres schema up to date, under the migration lock.
///
/// sqlx's migrator takes a session advisory lock keyed by the database's name
/// alone, so where dagron-ci shares the engine's database, as it does by
/// default (the chart gives it the release's, and the compose stack
/// `workflow`), its lock and the engine's are one, whatever the schema. sqlx
/// waits for that lock inside one `SELECT pg_advisory_lock`, and a session
/// waiting in a statement holds a snapshot. An engine's `-- no-transaction`
/// migration, a `CREATE INDEX CONCURRENTLY`, waits out every older snapshot
/// in the database, so an engine that held the lock for its index build
/// waited for a dagron-ci that waited for the lock: Postgres failed the build
/// as a deadlock and left its index INVALID. So sqlx's own locking is off
/// here, and the lock is polled with `pg_try_advisory_lock`, each try a
/// statement of its own, as the engine's `migrate` in dagron-core polls it.
/// It is sqlx's lock, at sqlx's key, so a dagron-ci or an engine of an
/// earlier release, which waits for it sqlx's way, still takes turns with
/// this one.
async fn migrate_pg(pool: &PgPool) -> anyhow::Result<()> {
    let mut migrator = sqlx::migrate!("./migrations_pg");
    migrator.set_locking(false);
    let mut conn = pool.acquire().await?;
    let key = match lock_migrations(&mut conn).await {
        Ok(key) => key,
        Err(e) => {
            let _ = conn.close().await;
            return Err(e);
        }
    };
    if let Err(e) = migrator.run_direct(&mut *conn).await {
        // The lock is the session's: closing the connection frees it, where
        // returning it to the pool would keep it.
        let _ = conn.close().await;
        return Err(e.into());
    }
    release_migration_lock(conn, key).await;
    Ok(())
}

/// Take the migration lock on `conn`, waiting for another session that holds
/// it, and return its key. Between tries a waiter holds no snapshot.
async fn lock_migrations(conn: &mut sqlx::PgConnection) -> anyhow::Result<i64> {
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&mut *conn)
        .await?;
    let key = migration_lock_key(&database);
    let started = std::time::Instant::now();
    let mut warn_at = MIGRATION_LOCK_WARN_EVERY;
    let mut waiting = false;
    while !try_migration_lock(conn, key).await? {
        if !waiting {
            waiting = true;
            tracing::info!(
                database = %database,
                "waiting for an engine's or another dagron-ci's migrations to finish"
            );
        } else if started.elapsed() >= warn_at {
            warn_at += MIGRATION_LOCK_WARN_EVERY;
            // pg_locks shows a bigint key in two halves.
            tracing::warn!(
                database = %database,
                key,
                classid = key >> 32,
                objid = key & 0xffff_ffff,
                waited_secs = started.elapsed().as_secs(),
                "still waiting for the migration lock; its holder is the session \
                 pg_locks shows with locktype advisory, this classid and objid, and \
                 objsubid 1"
            );
        }
        tokio::time::sleep(MIGRATION_LOCK_POLL).await;
    }
    Ok(key)
}

async fn try_migration_lock(conn: &mut sqlx::PgConnection, key: i64) -> sqlx::Result<bool> {
    sqlx::query_scalar("SELECT pg_try_advisory_lock($1)")
        .bind(key)
        .fetch_one(conn)
        .await
}

/// Let the migration lock go. A connection whose unlock fails is closed rather
/// than returned to the pool, for closing the session frees the lock all the
/// same; one whose unlock finds the lock not its own is closed too, so that it
/// is not used again (the lock is on another server session, which closing
/// this connection does not end).
async fn release_migration_lock(mut conn: sqlx::pool::PoolConnection<sqlx::Postgres>, key: i64) {
    let released = sqlx::query_scalar::<_, bool>("SELECT pg_advisory_unlock($1)")
        .bind(key)
        .fetch_one(&mut *conn)
        .await;
    match released {
        Ok(true) => {}
        Ok(false) => {
            // The try and the unlock reached two server sessions, as through
            // a pooler in transaction mode: the lock is left on the other.
            tracing::warn!(
                key,
                "the migration lock was not this session's to release; through a \
                 pooler in transaction mode the lock keeps no migrators apart"
            );
            let _ = conn.close().await;
        }
        Err(e) => {
            tracing::warn!(
                error = %e,
                "could not release the migration lock; closing its connection"
            );
            let _ = conn.close().await;
        }
    }
}

/// The advisory-lock key of a database's migrations, as sqlx-postgres 0.8
/// makes it for its own migrator's lock (`generate_lock_id`): the CRC-32
/// (ISO-HDLC) of the database's name times `0x3d32ad9e`. The product fits an
/// `i64`. dagron-core's `migration_lock_key` is the same: dagron-ci does not
/// link dagron-core (see Cargo.toml).
fn migration_lock_key(database: &str) -> i64 {
    0x3d32_ad9e * i64::from(crc32_iso_hdlc(database.as_bytes()))
}

/// CRC-32 ISO-HDLC (the zlib and Ethernet CRC), bit by bit: it runs over one
/// database name a start, so a table would buy nothing.
fn crc32_iso_hdlc(bytes: &[u8]) -> u32 {
    let mut crc = u32::MAX;
    for &byte in bytes {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            crc = (crc >> 1) ^ (0xEDB8_8320 & (crc & 1).wrapping_neg());
        }
    }
    !crc
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::event::{Kind, Trust};

    pub(crate) async fn sqlite() -> (Db, tempfile::TempDir) {
        let dir = tempfile::tempdir().unwrap();
        let url = format!("sqlite://{}", dir.path().join("ci.db").display());
        let config =
            Config::from_lookup(|name| (name == "CI_DATABASE_URL").then(|| url.clone())).unwrap();
        (Db::connect(&config).await.unwrap(), dir)
    }

    /// A Postgres database with dagron-ci migrated into a schema of its own,
    /// dropped by the caller, or `None` when TEST_DATABASE_URL is unset.
    pub(crate) async fn postgres() -> Option<(Db, String, PgPool)> {
        let url = match std::env::var("TEST_DATABASE_URL") {
            Ok(u) if !u.trim().is_empty() => u,
            _ => {
                eprintln!("TEST_DATABASE_URL unset - skipping");
                return None;
            }
        };
        let schema = format!("dagron_ci_test_{}", uuid::Uuid::now_v7().simple());
        let config = Config::from_lookup(|name| match name {
            "CI_DATABASE_URL" => Some(url.clone()),
            "CI_DATABASE_SCHEMA" => Some(schema.clone()),
            _ => None,
        })
        .unwrap();
        let db = Db::connect(&config).await.unwrap();
        let admin = PgPool::connect(&url).await.unwrap();
        Some((db, schema, admin))
    }

    pub(crate) async fn drop_schema(admin: &PgPool, schema: &str) {
        sqlx::query(&format!("DROP SCHEMA IF EXISTS {schema} CASCADE"))
            .execute(admin)
            .await
            .unwrap();
    }

    pub(crate) fn event() -> NewEvent {
        NewEvent {
            kind: Kind::Push,
            git_ref: "refs/heads/main".into(),
            sha: "a".repeat(40),
            commit: None,
            base_sha: None,
            base_ref: None,
            pr: None,
            actor: Some("octocat".into()),
            trust: Trust::Trusted,
            association: None,
            head_ref: None,
        }
    }

    async fn the_queue_contract(db: &Db) {
        assert_eq!(
            db.backlog().await.unwrap(),
            Backlog {
                pending: 0,
                oldest_age_secs: None
            }
        );

        let repo = db
            .add_repo("github", "GitHub.com", "Acme/Widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (repo.forge_host.as_str(), repo.full_name.as_str()),
            ("github.com", "acme/widgets")
        );
        assert!(
            db.add_repo("github", "github.com", "acme/widgets", None, "default")
                .await
                .unwrap()
                .is_none(),
            "already connected"
        );
        assert_eq!(
            db.find_repo("github.com", "acme/widgets").await.unwrap(),
            Some(repo.clone())
        );
        let listed = db.list_repos().await.unwrap();
        assert_eq!(
            listed,
            vec![RepoListing {
                repo: repo.clone(),
                poll_secs: None,
                polled_at: None,
                poll_error: None,
            }]
        );

        let first = db
            .record_delivery(&repo, "d-1", &event(), b"{}")
            .await
            .unwrap();
        assert!(matches!(first, Recorded::Event(_)));
        assert_eq!(
            db.record_delivery(&repo, "d-1", &event(), b"{}")
                .await
                .unwrap(),
            Recorded::Duplicate,
            "a replayed delivery id writes nothing"
        );
        let backlog = db.backlog().await.unwrap();
        assert_eq!(backlog.pending, 1);
        assert!(backlog.oldest_age_secs.unwrap() >= 0.0);

        // A payload over the inline limit is not kept; the event still is.
        let big = vec![b' '; PAYLOAD_INLINE_MAX + 1];
        assert!(matches!(
            db.record_delivery(&repo, "d-2", &event(), &big)
                .await
                .unwrap(),
            Recorded::Event(_)
        ));
        assert_eq!(db.backlog().await.unwrap().pending, 2);

        // A disconnect keeps the repository's row, and its events with it:
        // the ones that were waiting are not run, and say why. One of the
        // two is a worker's as it happens.
        let held = db
            .claim_event("w-gone", LEASE)
            .await
            .unwrap()
            .expect("one is due");
        let disconnected = db
            .disconnect_repo("github.com", "ACME/widgets", "alice")
            .await
            .unwrap();
        assert_eq!(disconnected.as_deref(), Some(repo.id.as_str()));
        assert!(!db.remove_repo("github.com", "acme/widgets").await.unwrap());
        assert_eq!(
            db.backlog().await.unwrap().pending,
            0,
            "none of its events waits"
        );
        let kept = db.list_events("skipped", 10).await.unwrap();
        assert_eq!(kept.len(), 1, "the one a worker holds is that worker's");
        // Given back to be tried again, that one is not run either: the
        // next claim skips it, and has nothing to hand over.
        let later = Finish::Retry {
            reason: "the forge did not answer".into(),
            after: Duration::from_secs(1),
        };
        assert!(db.finish_event(&held.id, "w-gone", &later).await.unwrap());
        exec(&db, "UPDATE ci_events SET next_attempt_at = NULL").await;
        assert_eq!(db.backlog().await.unwrap().pending, 1);
        assert_eq!(db.claim_event("w-1", LEASE).await.unwrap(), None);
        assert_eq!(db.backlog().await.unwrap().pending, 0);
        let kept = db.list_events("skipped", 10).await.unwrap();
        assert_eq!(kept.len(), 2, "its events are there to read");
        assert!(kept
            .iter()
            .all(|e| e.reason.as_deref() == Some(DISCONNECTED)));

        // It is no repository of ours until it is connected again: to a
        // webhook, a listing, a change.
        assert_eq!(
            db.find_repo("github.com", "acme/widgets").await.unwrap(),
            None
        );
        assert!(!db.serves_repo("github.com", "Acme/Widgets").await.unwrap());
        assert!(db.list_repos().await.unwrap().is_empty());
        assert!(!db
            .bind_repo("github.com", "acme/widgets", None, Some("eu"))
            .await
            .unwrap());
        let (was, since) = db
            .repo_any("github.com", "acme/widgets")
            .await
            .unwrap()
            .expect("its row stays");
        assert_eq!(was.id, repo.id);
        assert!(since.is_some(), "and says since when");

        // Connected again, it is the row it was: its history goes on.
        let again = db
            .connect_repo(
                "github",
                "github.com",
                "Acme/Widgets",
                Some("wf-2"),
                "default",
                Some(600),
                Mode::Native,
                "bob",
            )
            .await
            .unwrap();
        match &again {
            Connected::Again(r) => {
                assert_eq!(
                    (r.id.as_str(), r.workflow.as_deref()),
                    (repo.id.as_str(), Some("wf-2"))
                )
            }
            other => panic!("{other:?}"),
        }
        assert_eq!(
            db.list_repos().await.unwrap()[0].poll_secs,
            Some(600),
            "with the interval its connect names"
        );
        assert_eq!(
            db.connect_repo(
                "github",
                "github.com",
                "acme/widgets",
                None,
                "default",
                None,
                Mode::Native,
                "bob"
            )
            .await
            .unwrap(),
            Connected::Already
        );
        assert_eq!(
            db.repo_any("github.com", "acme/widgets")
                .await
                .unwrap()
                .unwrap()
                .1,
            None
        );
        for changed in [Some(true), Some(false)] {
            assert_eq!(
                db.change_repo(
                    "github.com",
                    "acme/widgets",
                    Some(None),
                    None,
                    Some(Some(120)),
                    None,
                    "carol"
                )
                .await
                .unwrap(),
                changed,
                "what is so already is not changed, and not recorded"
            );
        }
        assert_eq!(db.list_repos().await.unwrap()[0].poll_secs, Some(120));

        // Each change is on record, with who made it.
        let changes: Vec<(String, String, String)> = db
            .repo_changes(&repo.id)
            .await
            .unwrap()
            .into_iter()
            .map(|c| (c.actor, c.action, c.detail))
            .collect();
        let said: Vec<(&str, &str)> = changes
            .iter()
            .map(|(actor, action, _)| (actor.as_str(), action.as_str()))
            .collect();
        assert_eq!(
            said,
            [
                (LOCAL, "connected"),
                ("alice", "disconnected"),
                ("bob", "reconnected"),
                ("carol", "changed"),
            ]
        );
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&changes[3].2).unwrap(),
            serde_json::json!({ "workflow": null, "poll_secs": 120 })
        );
        // A change to what is not connected is not one, and is not recorded.
        assert_eq!(
            db.change_repo(
                "github.com",
                "acme/nope",
                None,
                Some("eu"),
                None,
                None,
                "carol"
            )
            .await
            .unwrap(),
            None
        );
        assert!(db
            .disconnect_repo("github.com", "acme/nope", "carol")
            .await
            .unwrap()
            .is_none());
    }

    const LEASE: Duration = Duration::from_secs(60);

    /// Run `sql` (Postgres-style, `$1` = `id`) on either driver.
    async fn force(db: &Db, sql: &str, id: &str) {
        match db {
            Db::Postgres(p) => sqlx::query(sql).bind(id).execute(p).await.map(drop),
            Db::Sqlite(p) => sqlx::query(&lite(sql)).bind(id).execute(p).await.map(drop),
        }
        .unwrap();
    }

    async fn the_worker_contract(db: &Db) {
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
        assert_eq!(
            (repo.workflow.as_deref(), repo.cell.as_str()),
            (Some("wf-1"), "default")
        );
        assert!(
            db.claim_event("w1", LEASE).await.unwrap().is_none(),
            "nothing queued"
        );
        let measured = NewEvent {
            base_sha: Some("b".repeat(40)),
            base_ref: Some("main".into()),
            association: Some("FIRST_TIME_CONTRIBUTOR".into()),
            head_ref: None,
            ..event()
        };
        for id in ["d-1", "d-2"] {
            db.record_delivery(&repo, id, &measured, b"{}")
                .await
                .unwrap();
        }

        // Oldest first, with its repository, and the claim counts.
        let a = db.claim_event("w1", LEASE).await.unwrap().unwrap();
        assert_eq!(
            (a.repo.clone(), a.attempts, a.kind.as_str()),
            (repo.clone(), 1, "push")
        );
        assert_eq!(a.sha, "a".repeat(40));
        assert_eq!(
            (a.base_sha.clone(), a.base_ref.as_deref()),
            (Some("b".repeat(40)), Some("main"))
        );
        assert_eq!(
            a.association.as_deref(),
            Some("FIRST_TIME_CONTRIBUTOR"),
            "what the forge said of the author is kept"
        );
        let b = db.claim_event("w2", LEASE).await.unwrap().unwrap();
        assert_ne!(a.id, b.id);
        assert!(
            db.claim_event("w3", LEASE).await.unwrap().is_none(),
            "both are held"
        );

        // Only the holder finishes, and only once.
        assert!(!db
            .finish_event(&a.id, "w2", &Finish::Done("x".into()))
            .await
            .unwrap());
        assert!(db
            .finish_event(&a.id, "w1", &Finish::Done("run r-1".into()))
            .await
            .unwrap());
        assert!(!db
            .finish_event(&a.id, "w1", &Finish::Done("again".into()))
            .await
            .unwrap());

        // A retry is not due before its time.
        let retry = Finish::Retry {
            reason: "dagron-api answered 503".into(),
            after: Duration::from_secs(3600),
        };
        assert!(db.finish_event(&b.id, "w2", &retry).await.unwrap());
        assert!(
            db.claim_event("w3", LEASE).await.unwrap().is_none(),
            "not due yet"
        );
        force(
            db,
            "UPDATE ci_events SET next_attempt_at = '2000-01-01T00:00:00Z' WHERE id = $1",
            &b.id,
        )
        .await;
        let again = db.claim_event("w3", LEASE).await.unwrap().unwrap();
        assert_eq!((again.id.as_str(), again.attempts), (b.id.as_str(), 2));

        // A wait is not due before its time either, and gives back the
        // attempt its claim counted.
        let wait = Finish::Wait {
            reason: "waiting, max_active_runs reached".into(),
            after: Duration::from_secs(3600),
        };
        assert!(db.finish_event(&b.id, "w3", &wait).await.unwrap());
        assert!(
            db.claim_event("w3", LEASE).await.unwrap().is_none(),
            "not due yet"
        );
        force(
            db,
            "UPDATE ci_events SET next_attempt_at = '2000-01-01T00:00:00Z' WHERE id = $1",
            &b.id,
        )
        .await;
        let again = db.claim_event("w3", LEASE).await.unwrap().unwrap();
        assert_eq!(
            (again.id.as_str(), again.attempts),
            (b.id.as_str(), 2),
            "the wait spent nothing"
        );

        // A worker that stops mid-event loses nothing: once its lease lapses,
        // the next claim takes the event over, and the stopped worker's late
        // finish changes nothing.
        force(
            db,
            "UPDATE ci_events SET claim_expires_at = '2000-01-01T00:00:00Z' WHERE id = $1",
            &b.id,
        )
        .await;
        let taken = db.claim_event("w4", LEASE).await.unwrap().unwrap();
        assert_eq!((taken.id.as_str(), taken.attempts), (b.id.as_str(), 3));
        assert!(!db
            .finish_event(&b.id, "w3", &Finish::Failed("late".into()))
            .await
            .unwrap());
        assert!(db
            .finish_event(&b.id, "w4", &Finish::Skipped("no workflow".into()))
            .await
            .unwrap());

        let events = db.list_events("", 10).await.unwrap();
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].id, b.id, "newest first");
        assert_eq!(
            (events[0].state.as_str(), events[0].reason.as_deref()),
            ("skipped", Some("no workflow"))
        );
        assert_eq!(
            (events[1].state.as_str(), events[1].reason.as_deref()),
            ("done", Some("run r-1"))
        );
        assert_eq!(events[1].repo, "github.com/acme/widgets");
        assert_eq!(db.list_events("skipped", 10).await.unwrap().len(), 1);
        assert_eq!(db.backlog().await.unwrap().pending, 0);

        // A submitted run is found by its key, and recorded once.
        let run = SubmittedRun {
            run_id: "r-1".into(),
            event_id: a.id.clone(),
            repo_id: repo.id.clone(),
            cell: "default".into(),
            pipeline: "workflow:wf-1".into(),
            idempotency_key: "ci-k".into(),
            on_failed: false,
        };
        assert_eq!(db.find_run("ci-k").await.unwrap(), None);
        assert!(db.record_run(&run).await.unwrap());
        assert!(
            !db.record_run(&run).await.unwrap(),
            "a retry's run is recorded once"
        );
        assert_eq!(db.find_run("ci-k").await.unwrap().as_deref(), Some("r-1"));

        // What a checkout's credential may be minted for: the run's
        // repository and its event's commit, on the cell it went to, until
        // the run is over.
        assert_eq!(
            db.run_commit("r-1").await.unwrap(),
            Some(RunCommit {
                repo: repo.clone(),
                sha: "a".repeat(40),
                cell: run.cell.clone(),
                status: "submitted".into(),
                connected: true,
                trust: "trusted".into(),
                bundle_id: None,
            })
        );
        assert_eq!(db.run_commit("r-0").await.unwrap(), None);
        let (claimed, _) = db.claim_report("rep", LEASE).await.unwrap().unwrap();
        assert_eq!(claimed, "r-1");
        let over = ReportWrite {
            status: Some("cancelled".into()),
            ..ReportWrite::default()
        };
        assert!(db.finish_report("r-1", "rep", &over).await.unwrap());
        assert_eq!(
            db.run_commit("r-1").await.unwrap().map(|r| r.status),
            Some("cancelled".into())
        );

        // What a run has posted is another event's to ask for, and to have
        // posted again: its own event's question leaves it out. Told to post
        // again it is due, with nothing on record as posted.
        let sha = "a".repeat(40);
        let again = |but: Option<&'static str>| {
            let (db, repo, sha) = (db.clone(), repo.id.clone(), sha.clone());
            async move {
                db.report_pipeline_again(&repo, "workflow:wf-1", &sha, "push", "push", but)
                    .await
                    .unwrap()
            }
        };
        let (own, other): (&'static str, &'static str) = (
            Box::leak(a.id.clone().into_boxed_str()),
            Box::leak(b.id.clone().into_boxed_str()),
        );
        assert_eq!(again(Some(own)).await, 0, "its own event's runs are left");
        assert!(db.claim_report("rep", LEASE).await.unwrap().is_none());
        assert_eq!(again(None).await, 1);
        let (claimed, _) = db.claim_report("rep", LEASE).await.unwrap().unwrap();
        assert_eq!(claimed, "r-1");
        let posted = ReportWrite {
            reported: BTreeMap::from([
                (
                    "dagron-ci/wf/build (push)".to_string(),
                    "failure:failed".to_string(),
                ),
                // Why the forge took none under this one, and none of the run's.
                (
                    "dagron-ci/wf/docs (push)".to_string(),
                    "!refused:too long".to_string(),
                ),
                ("!refused".to_string(), "no such commit".to_string()),
            ]),
            status: Some("cancelled".into()),
            ..ReportWrite::default()
        };
        assert!(db.finish_report("r-1", "rep", &posted).await.unwrap());
        let others = |pipeline: &'static str, kind: &'static str, but: &'static str| {
            let (db, repo, sha) = (db.clone(), repo.id.clone(), sha.clone());
            async move {
                let posted = db
                    .posted_by_others(&repo, pipeline, &sha, kind, kind, but)
                    .await
                    .unwrap();
                posted.into_iter().collect::<Vec<_>>()
            }
        };
        assert_eq!(
            others("workflow:wf-1", "push", other).await,
            ["dagron-ci/wf/build (push)"]
        );
        assert!(others("workflow:wf-1", "push", own).await.is_empty());
        assert!(others("workflow:wf-2", "push", other).await.is_empty());
        assert!(others("workflow:wf-1", "pull_request", other)
            .await
            .is_empty());
        assert_eq!(again(Some(other)).await, 1);
        let (claimed, due) = db.claim_report("rep", LEASE).await.unwrap().unwrap();
        assert_eq!(claimed, "r-1");
        assert!(due.expect("its event is there").reported.is_empty());
        assert!(others("workflow:wf-1", "push", other).await.is_empty());
        assert!(db.finish_report("r-1", "rep", &over).await.unwrap());

        // A commit's runs that were reported as check runs: what a re-run of
        // all its checks is of. So far this one posted statuses, or nothing.
        assert!(db.checked_runs_at(&repo.id, &sha).await.unwrap().is_empty());
        assert_eq!(again(None).await, 1);
        let (claimed, _) = db.claim_report("rep", LEASE).await.unwrap().unwrap();
        assert_eq!(claimed, "r-1");
        let checked = ReportWrite {
            reported: BTreeMap::from([(
                "dagron-ci/wf/build (push)".to_string(),
                "check:12:completed/failure:failed".to_string(),
            )]),
            status: Some("cancelled".into()),
            ..ReportWrite::default()
        };
        assert!(db.finish_report("r-1", "rep", &checked).await.unwrap());
        assert_eq!(db.checked_runs_at(&repo.id, &sha).await.unwrap(), ["r-1"]);
        // Of this repository, at this commit, and no other.
        assert!(db
            .checked_runs_at(&repo.id, &"0".repeat(40))
            .await
            .unwrap()
            .is_empty());
        assert!(db
            .checked_runs_at("another-repo", &sha)
            .await
            .unwrap()
            .is_empty());

        // The binding changes in place; the cell stays unless named.
        assert!(db
            .bind_repo("GitHub.com", "acme/widgets", Some(Some("wf-2")), None)
            .await
            .unwrap());
        let r = db
            .find_repo("github.com", "acme/widgets")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (r.workflow.as_deref(), r.cell.as_str()),
            (Some("wf-2"), "default")
        );
        assert!(db
            .bind_repo("github.com", "acme/widgets", Some(None), Some("eu"))
            .await
            .unwrap());
        let r = db
            .find_repo("github.com", "acme/widgets")
            .await
            .unwrap()
            .unwrap();
        assert_eq!((r.workflow, r.cell.as_str()), (None, "eu"));
        assert!(!db
            .bind_repo("github.com", "acme/nope", None, Some("eu"))
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn sqlite_keeps_the_worker_contract() {
        let (db, _dir) = sqlite().await;
        the_worker_contract(&db).await;
    }

    #[tokio::test]
    async fn postgres_keeps_the_worker_contract() {
        let Some((db, schema, admin)) = postgres().await else {
            return;
        };
        the_worker_contract(&db).await;
        db.close().await;
        drop_schema(&admin, &schema).await;
    }

    /// How a repository is served: said as it is connected, changed
    /// and recorded as its other settings are, read by its id too, and kept
    /// from the sweeps it has no use for.
    async fn the_mode_contract(db: &Db) {
        let (host, name) = ("github.com", "acme/widgets");
        let made = db
            .connect_repo(
                "github",
                host,
                name,
                None,
                "default",
                None,
                Mode::Native,
                "alice",
            )
            .await
            .unwrap();
        let Connected::New(repo) = made else {
            panic!("{made:?}")
        };
        assert_eq!(repo.mode, Mode::Native);
        assert_eq!(db.find_repo(host, name).await.unwrap().unwrap(), repo);
        let due = db
            .claim_sweep("s-1", LEASE, 60)
            .await
            .unwrap()
            .expect("a native repository is swept");
        assert_eq!((due.repo.mode, due.baseline), (Mode::Native, true));
        let write = SweepWrite {
            events: Vec::new(),
            set: Vec::new(),
            drop: Vec::new(),
            etag: None,
        };
        db.finish_sweep(&repo.id, "s-1", &write, Duration::ZERO)
            .await
            .unwrap()
            .expect("its sweep is its claim's");
        assert!(
            db.claim_schedule("c-1", LEASE).await.unwrap().is_none(),
            "the scheduler reads workflow files, and a native repository's are not read"
        );

        // In fleet mode, its forge runs its pipelines: it is not swept.
        for changed in [Some(true), Some(false)] {
            assert_eq!(
                db.change_repo(host, name, None, None, None, Some(Mode::Fleet), "bob")
                    .await
                    .unwrap(),
                changed
            );
        }
        assert_eq!(
            db.find_repo(host, name).await.unwrap().unwrap().mode,
            Mode::Fleet
        );
        force(
            db,
            "UPDATE ci_repos SET poll_due_at = NULL WHERE id = $1",
            &repo.id,
        )
        .await;
        assert!(db.claim_sweep("s-2", LEASE, 60).await.unwrap().is_none());

        // Out of it, it is swept from a baseline: nothing looked at it while
        // it was in it. In compat mode, its schedules are read.
        db.change_repo(host, name, None, None, None, Some(Mode::Compat), "carol")
            .await
            .unwrap();
        let due = db
            .claim_sweep("s-3", LEASE, 60)
            .await
            .unwrap()
            .expect("swept again");
        assert_eq!((due.repo.mode, due.baseline), (Mode::Compat, true));
        let scheduled = db
            .claim_schedule("c-2", LEASE)
            .await
            .unwrap()
            .expect("a compat repository's schedules are read");
        assert_eq!(scheduled.repo.mode, Mode::Compat);

        let set: Vec<serde_json::Value> = db
            .repo_changes(&repo.id)
            .await
            .unwrap()
            .iter()
            .map(|c| serde_json::from_str(&c.detail).unwrap())
            .collect();
        assert_eq!(set[0]["mode"], "native", "said as it was connected");
        assert_eq!(
            set[1..],
            [
                serde_json::json!({ "mode": "fleet" }),
                serde_json::json!({ "mode": "compat" })
            ],
            "and each change, once"
        );

        // By its id, connected or not.
        let (by_id, since) = db.repo_any_by_id(&repo.id).await.unwrap().unwrap();
        assert_eq!((by_id.full_name.as_str(), since), (name, None));
        db.disconnect_repo(host, name, "carol").await.unwrap();
        let (_, since) = db.repo_any_by_id(&repo.id).await.unwrap().unwrap();
        assert!(since.is_some());
        assert!(db.repo_any_by_id("no-such-id").await.unwrap().is_none());
    }

    /// A merge-queue group is its ref and its head: asked for its
    /// checks until it is let go of, and asked anew after.
    async fn the_group_contract(db: &Db) {
        let repo = db
            .add_repo("gitlab", "gitlab.example", "group/app", None, "default")
            .await
            .unwrap()
            .unwrap();
        let train = "refs/merge-requests/4/train";
        let at = |kind: Kind, sha: &str| NewEvent {
            kind,
            git_ref: train.into(),
            sha: sha.repeat(40),
            base_ref: Some("main".into()),
            ..event()
        };
        let asked = |sha: &'static str| {
            let (repo_id, sha) = (repo.id.clone(), sha.repeat(40));
            async move { db.group_asked(&repo_id, train, &sha).await }
        };
        assert!(!asked("d").await.unwrap());
        db.record_delivery(&repo, "p-1", &at(Kind::MergeGroup, "d"), b"{}")
            .await
            .unwrap();
        assert!(asked("d").await.unwrap());
        assert!(!asked("e").await.unwrap(), "another of its pipelines");
        assert_eq!(
            db.group_events(&repo.id, train, &"d".repeat(40))
                .await
                .unwrap()
                .len(),
            1
        );
        assert!(db
            .group_events(&repo.id, train, &"e".repeat(40))
            .await
            .unwrap()
            .is_empty());
        db.record_delivery(&repo, "p-2", &at(Kind::MergeGroupDestroyed, "d"), b"{}")
            .await
            .unwrap();
        assert!(!asked("d").await.unwrap(), "let go of");
        // Past the millisecond the database keeps: one received in the same
        // one is taken for one before it, and asked again.
        tokio::time::sleep(Duration::from_millis(5)).await;
        db.record_delivery(&repo, "p-3", &at(Kind::MergeGroup, "d"), b"{}")
            .await
            .unwrap();
        assert!(asked("d").await.unwrap(), "and asked anew");
    }

    #[tokio::test]
    async fn sqlite_keeps_the_group_contract() {
        let (db, _dir) = sqlite().await;
        the_group_contract(&db).await;
    }

    #[tokio::test]
    async fn postgres_keeps_the_group_contract() {
        let Some((db, schema, admin)) = postgres().await else {
            return;
        };
        the_group_contract(&db).await;
        db.close().await;
        drop_schema(&admin, &schema).await;
    }

    #[tokio::test]
    async fn sqlite_keeps_the_mode_contract() {
        let (db, _dir) = sqlite().await;
        the_mode_contract(&db).await;
    }

    #[tokio::test]
    async fn postgres_keeps_the_mode_contract() {
        let Some((db, schema, admin)) = postgres().await else {
            return;
        };
        the_mode_contract(&db).await;
        db.close().await;
        drop_schema(&admin, &schema).await;
    }

    /// An annotated tag's event is keyed by the tag's own id, and shown under
    /// it. What a worker, a checkout's credential and a reporter are handed
    /// is the commit the tag names.
    async fn the_annotated_tag_contract(db: &Db) {
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
        let (tag, commit) = ("d".repeat(40), "c".repeat(40));
        let annotated = NewEvent {
            kind: Kind::Tag,
            git_ref: "refs/tags/v1".into(),
            sha: tag.clone(),
            commit: Some(commit.clone()),
            ..event()
        };
        db.record_delivery(&repo, "d-tag", &annotated, b"{}")
            .await
            .unwrap();
        // A lightweight tag is its commit's own id: nothing beside it.
        let light = NewEvent {
            git_ref: "refs/tags/light".into(),
            commit: None,
            ..annotated.clone()
        };
        db.record_delivery(&repo, "d-light", &light, b"{}")
            .await
            .unwrap();

        let ev = db.claim_event("w", LEASE).await.unwrap().unwrap();
        assert_eq!(ev.sha, commit, "the worker is handed the commit");
        assert_eq!(
            ev.commit_key,
            format!("ci:{}:tag:refs/tags/v1:{tag}", repo.id),
            "and the key is the tag's own id's, as a sweep derives it"
        );
        let other = db.claim_event("w", LEASE).await.unwrap().unwrap();
        assert_eq!(
            (other.git_ref.as_str(), other.sha.as_str()),
            ("refs/tags/light", tag.as_str())
        );

        let run = SubmittedRun {
            run_id: "r-tag".into(),
            event_id: ev.id.clone(),
            repo_id: repo.id.clone(),
            cell: "default".into(),
            pipeline: ".dagron/ci/tags.yaml".into(),
            idempotency_key: "ci-tag".into(),
            on_failed: false,
        };
        assert!(db.record_run(&run).await.unwrap());
        assert_eq!(
            db.run_commit("r-tag").await.unwrap().map(|r| r.sha),
            Some(commit.clone()),
            "a checkout's credential is for the commit"
        );
        let (_, due) = db.claim_report("rep", LEASE).await.unwrap().unwrap();
        assert_eq!(
            due.map(|d| (d.kind, d.sha)),
            Some(("tag".to_string(), commit.clone())),
            "the statuses go to the commit"
        );
        // The event is still shown under the id it came with.
        let view = db.event_view(&ev.id).await.unwrap().unwrap();
        assert_eq!(view.sha, tag);
    }

    #[tokio::test]
    async fn sqlite_keeps_the_annotated_tag_contract() {
        let (db, _dir) = sqlite().await;
        the_annotated_tag_contract(&db).await;
    }

    #[tokio::test]
    async fn postgres_keeps_the_annotated_tag_contract() {
        let Some((db, schema, admin)) = postgres().await else {
            return;
        };
        the_annotated_tag_contract(&db).await;
        db.close().await;
        drop_schema(&admin, &schema).await;
    }

    /// A run as a worker records it: `run-N`, under `key-N`.
    fn run_for(repo: &Repo, n: usize) -> SubmittedRun {
        SubmittedRun {
            run_id: format!("run-{n}"),
            event_id: format!("event-{n}"),
            repo_id: repo.id.clone(),
            cell: "default".into(),
            pipeline: ".dagron/ci/ci.yaml".into(),
            idempotency_key: format!("key-{n}"),
            on_failed: false,
        }
    }

    /// Run `sql` as it stands on either database.
    pub(crate) async fn exec(db: &Db, sql: &str) {
        match db {
            Db::Postgres(p) => sqlx::query(sql).execute(p).await.map(drop),
            Db::Sqlite(p) => sqlx::query(sql).execute(p).await.map(drop),
        }
        .unwrap();
    }

    /// What the reporter writes once it sees `run_id` end.
    pub(crate) async fn end_run(db: &Db, run_id: &str) {
        exec(
            db,
            &format!(
                "UPDATE ci_runs SET status = 'succeeded', report_due_at = NULL \
                 WHERE run_id = '{run_id}'"
            ),
        )
        .await;
    }

    async fn the_admission_contract(db: &Db) {
        let repo = db
            .add_repo("github", "github.com", "acme/widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        let hold = Duration::from_secs(60);
        let two = |active: u64| {
            if active < 2 {
                Ok(())
            } else {
                Err(format!("{active} active"))
            }
        };
        let full = Err("2 active".to_string());
        // Two slots fit; the third run waits, and is told why.
        assert_eq!(db.admit("key-1", hold, two).await.unwrap(), Ok(()));
        assert_eq!(db.admit("key-2", hold, two).await.unwrap(), Ok(()));
        assert_eq!(db.admit("key-3", hold, two).await.unwrap(), full);
        // A run asking again renews its slot, which never counts against it.
        assert_eq!(db.admit("key-1", hold, two).await.unwrap(), Ok(()));
        // Recorded, the run counts in its slot's place: still two.
        assert!(db.record_run(&run_for(&repo, 1)).await.unwrap());
        assert_eq!(db.admit("key-3", hold, two).await.unwrap(), full);
        // A slot let go of is a place again.
        db.release_admission("key-2").await.unwrap();
        assert_eq!(db.admit("key-3", hold, two).await.unwrap(), Ok(()));
        assert_eq!(db.admit("key-4", hold, two).await.unwrap(), full);
        // A run the reporter saw end no longer counts.
        end_run(db, "run-1").await;
        assert_eq!(db.admit("key-4", hold, two).await.unwrap(), Ok(()));
        // A slot whose worker stopped before recording its run lapses.
        exec(
            db,
            "UPDATE ci_admissions SET expires_at = '2000-01-01T00:00:00Z'",
        )
        .await;
        let counted = |want: u64| {
            move |active: u64| {
                assert_eq!(active, want);
                Ok(())
            }
        };
        assert_eq!(db.admit("key-5", hold, counted(0)).await.unwrap(), Ok(()));
        // A run nothing follows to its end (recorded before the reporter
        // existed) is not counted; a followed one is.
        assert!(db.record_run(&run_for(&repo, 6)).await.unwrap());
        assert!(db.record_run(&run_for(&repo, 7)).await.unwrap());
        exec(
            db,
            "UPDATE ci_runs SET report_due_at = NULL WHERE run_id = 'run-6'",
        )
        .await;
        assert_eq!(db.admit("key-8", hold, counted(2)).await.unwrap(), Ok(()));
    }

    #[tokio::test]
    async fn sqlite_keeps_the_admission_contract() {
        let (db, _dir) = sqlite().await;
        the_admission_contract(&db).await;
    }

    #[tokio::test]
    async fn postgres_keeps_the_admission_contract() {
        let Some((db, schema, admin)) = postgres().await else {
            return;
        };
        the_admission_contract(&db).await;
        db.close().await;
        drop_schema(&admin, &schema).await;
    }

    /// The count by repository: a build that reads the active runs by repository
    /// is given each run's, the recorded ones and the slots alike.
    async fn the_admission_by_repository_contract(db: &Db) {
        let widgets = db
            .add_repo("github", "github.com", "acme/widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        let hold = Duration::from_secs(60);
        let seen = std::sync::Mutex::new(Vec::new());
        let keep = |load: &crate::admission::Load| {
            seen.lock().unwrap().push(load.clone());
            Ok(())
        };
        let w = Some("github.com/acme/widgets");
        let g = Some("github.com/acme/gadgets");
        assert_eq!(
            db.admit_load("key-1", w, hold, true, keep).await.unwrap(),
            Ok(())
        );
        assert_eq!(
            db.admit_load("key-2", g, hold, true, keep).await.unwrap(),
            Ok(())
        );
        // key-1's slot becomes its run.
        assert!(db.record_run(&run_for(&widgets, 1)).await.unwrap());
        assert_eq!(
            db.admit_load("key-3", w, hold, true, keep).await.unwrap(),
            Ok(())
        );
        // Not asked by repository: only the total.
        assert_eq!(
            db.admit_load("key-4", w, hold, false, keep).await.unwrap(),
            Ok(())
        );
        let seen = seen.into_inner().unwrap();
        let by = |n: usize| -> Vec<(String, u64)> {
            seen[n]
                .by_repo
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect()
        };
        assert_eq!((seen[0].active, by(0)), (0, vec![]));
        assert_eq!(
            (seen[1].active, by(1)),
            (1, vec![("github.com/acme/widgets".into(), 1)])
        );
        assert_eq!(
            (seen[2].active, by(2)),
            (
                2,
                vec![
                    ("github.com/acme/gadgets".into(), 1),
                    ("github.com/acme/widgets".into(), 1)
                ]
            )
        );
        assert_eq!((seen[3].active, by(3)), (3, vec![]));
    }

    /// A slot keeps its caller, and a run counts for its event's actor: the
    /// runs active counted by caller where the admission asks, and not
    /// otherwise.
    async fn the_admission_by_caller_contract(db: &Db) {
        let widgets = db
            .add_repo("github", "github.com", "acme/widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        let hold = Duration::from_secs(60);
        let seen = std::sync::Mutex::new(Vec::new());
        let keep = |load: &crate::admission::Load| {
            seen.lock().unwrap().push(load.clone());
            Ok(())
        };
        let w = Some("github.com/acme/widgets");
        let fixer = Some("agent:fixer");
        assert_eq!(
            db.admit_for("key-1", w, fixer, hold, false, true, keep)
                .await
                .unwrap(),
            Ok(())
        );
        assert_eq!(
            db.admit_for("key-2", w, None, hold, false, true, keep)
                .await
                .unwrap(),
            Ok(())
        );
        assert_eq!(
            db.admit_for("key-3", w, fixer, hold, false, true, keep)
                .await
                .unwrap(),
            Ok(())
        );
        // Not asked by caller: only the total.
        assert_eq!(
            db.admit_for("key-4", w, fixer, hold, false, false, keep)
                .await
                .unwrap(),
            Ok(())
        );
        let _ = widgets;
        let seen = seen.into_inner().unwrap();
        let by = |n: usize| -> Vec<(String, u64)> {
            seen[n]
                .by_caller
                .iter()
                .map(|(k, v)| (k.clone(), *v))
                .collect()
        };
        assert_eq!((seen[0].active, by(0)), (0, vec![]));
        assert_eq!(
            (seen[1].active, by(1)),
            (1, vec![("agent:fixer".into(), 1)])
        );
        assert_eq!(
            (seen[2].active, by(2)),
            (2, vec![("".into(), 1), ("agent:fixer".into(), 1)])
        );
        assert_eq!((seen[3].active, by(3)), (3, vec![]));
        assert!(seen.iter().all(|l| l.by_repo.is_empty()));
    }

    #[tokio::test]
    async fn sqlite_counts_admissions_by_caller() {
        let (db, _dir) = sqlite().await;
        the_admission_by_caller_contract(&db).await;
    }

    #[tokio::test]
    async fn postgres_counts_admissions_by_caller() {
        let Some((db, schema, admin)) = postgres().await else {
            return;
        };
        the_admission_by_caller_contract(&db).await;
        db.close().await;
        drop_schema(&admin, &schema).await;
    }

    #[tokio::test]
    async fn sqlite_counts_admissions_by_repository() {
        let (db, _dir) = sqlite().await;
        the_admission_by_repository_contract(&db).await;
    }

    #[tokio::test]
    async fn postgres_counts_admissions_by_repository() {
        let Some((db, schema, admin)) = postgres().await else {
            return;
        };
        the_admission_by_repository_contract(&db).await;
        db.close().await;
        drop_schema(&admin, &schema).await;
    }

    /// Runs asking at once never take more slots than the limit allows.
    async fn admissions_race_to_the_limit_and_no_further(db: &Db) {
        let asking: Vec<_> = (0..40)
            .map(|n| {
                let db = db.clone();
                tokio::spawn(async move {
                    db.admit(&format!("key-{n}"), Duration::from_secs(60), |active| {
                        if active < 5 {
                            Ok(())
                        } else {
                            Err("full".into())
                        }
                    })
                    .await
                    .unwrap()
                })
            })
            .collect();
        let mut admitted = 0;
        for task in asking {
            admitted += usize::from(task.await.unwrap().is_ok());
        }
        assert_eq!(admitted, 5);
    }

    #[tokio::test]
    async fn sqlite_admits_no_more_than_the_limit_at_once() {
        let (db, _dir) = sqlite().await;
        admissions_race_to_the_limit_and_no_further(&db).await;
    }

    #[tokio::test]
    async fn postgres_admits_no_more_than_the_limit_at_once() {
        let Some((db, schema, admin)) = postgres().await else {
            return;
        };
        admissions_race_to_the_limit_and_no_further(&db).await;
        db.close().await;
        drop_schema(&admin, &schema).await;
    }

    #[tokio::test]
    async fn sqlite_keeps_the_queue_contract() {
        let (db, _dir) = sqlite().await;
        the_queue_contract(&db).await;
    }

    #[tokio::test]
    async fn postgres_keeps_the_queue_contract() {
        let Some((db, schema, admin)) = postgres().await else {
            return;
        };
        the_queue_contract(&db).await;
        db.close().await;
        drop_schema(&admin, &schema).await;
    }

    /// The tables and the migration history land in CI_DATABASE_SCHEMA, so
    /// dagron-ci and dagron-api can share a database without sharing
    /// `_sqlx_migrations`.
    #[tokio::test]
    async fn postgres_keeps_its_migration_history_in_its_own_schema() {
        let Some((db, schema, admin)) = postgres().await else {
            return;
        };
        // A partition is a table of its own, of its parent's: ci_events' days.
        let partitions: Vec<String> = sqlx::query_scalar(
            "SELECT c.relname::TEXT FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace              WHERE n.nspname = $1 AND c.relispartition AND c.relkind = 'r' ORDER BY 1",
        )
        .bind(&schema)
        .fetch_all(&admin)
        .await
        .unwrap();
        assert!(
            partitions.len() >= 9
                && partitions.iter().all(|p| p == "ci_events_default"
                    || (p.starts_with("ci_events_")
                        && p[10..].bytes().all(|b| b.is_ascii_digit()))),
            "{partitions:?}"
        );
        let tables: Vec<String> = sqlx::query_scalar(
            "SELECT table_name::TEXT FROM information_schema.tables WHERE table_schema = $1 ORDER BY 1",
        )
        .bind(&schema)
        .fetch_all(&admin)
        .await
        .unwrap()
        .into_iter()
        .filter(|t| !partitions.contains(t))
        .collect();
        assert_eq!(
            tables,
            [
                "_sqlx_migrations",
                "ci_admissions",
                "ci_api_requests",
                "ci_archived",
                "ci_bundles",
                "ci_checkout_credentials",
                "ci_contexts",
                "ci_deliveries",
                "ci_events",
                "ci_flake_issues",
                "ci_flakes",
                "ci_fleet_jobs",
                "ci_fleet_runners",
                "ci_poll_refs",
                "ci_pr_comments",
                "ci_repo_changes",
                "ci_report_log",
                "ci_repos",
                "ci_run_numbered",
                "ci_run_numbers",
                "ci_runs",
                "ci_schedules",
                "ci_segments"
            ]
        );
        db.close().await;
        drop_schema(&admin, &schema).await;
    }

    /// Two replicas starting at once both create the schema and migrate it.
    #[tokio::test]
    async fn postgres_replicas_can_start_together() {
        let url = match std::env::var("TEST_DATABASE_URL") {
            Ok(u) if !u.trim().is_empty() => u,
            _ => return eprintln!("TEST_DATABASE_URL unset - skipping"),
        };
        let schema = format!("dagron_ci_test_{}", uuid::Uuid::now_v7().simple());
        let config = Config::from_lookup(|name| match name {
            "CI_DATABASE_URL" => Some(url.clone()),
            "CI_DATABASE_SCHEMA" => Some(schema.clone()),
            _ => None,
        })
        .unwrap();
        let starts: Vec<_> = (0..8)
            .map(|_| {
                let config = config.clone();
                tokio::spawn(async move { Db::connect(&config).await })
            })
            .collect();
        let mut results = Vec::new();
        for start in starts {
            results.push(start.await.unwrap());
        }
        // Clean up before asserting, so a failing run leaves no schema behind.
        let admin = PgPool::connect(&url).await.unwrap();
        drop_schema(&admin, &schema).await;
        for result in results {
            result.expect("every replica comes up");
        }
    }

    /// Move event `id` to `at`, as if it had been received then. On
    /// Postgres the row moves to that day's partition, or to the default one.
    async fn received_at(db: &Db, id: &str, at: &str) {
        let rows = match db {
            Db::Postgres(p) => {
                sqlx::query("UPDATE ci_events SET received_at = $1::timestamptz WHERE id = $2")
                    .bind(at)
                    .bind(id)
                    .execute(p)
                    .await
                    .unwrap()
                    .rows_affected()
            }
            Db::Sqlite(p) => sqlx::query("UPDATE ci_events SET received_at = ?1 WHERE id = ?2")
                .bind(at)
                .bind(id)
                .execute(p)
                .await
                .unwrap()
                .rows_affected(),
        };
        assert_eq!(rows, 1);
    }

    /// Set when the row of the context `context` was last written.
    async fn context_made_at(db: &Db, context: &str, at: &str) {
        let rows = match db {
            Db::Postgres(p) => sqlx::query(
                "UPDATE ci_contexts SET written_at = $1::timestamptz WHERE context = $2",
            )
            .bind(at)
            .bind(context)
            .execute(p)
            .await
            .unwrap()
            .rows_affected(),
            Db::Sqlite(p) => {
                sqlx::query("UPDATE ci_contexts SET written_at = ?1 WHERE context = ?2")
                    .bind(at)
                    .bind(context)
                    .execute(p)
                    .await
                    .unwrap()
                    .rows_affected()
            }
        };
        assert_eq!(rows, 1);
    }

    /// The contexts on record, in order.
    async fn contexts(db: &Db) -> Vec<String> {
        let sql = "SELECT context FROM ci_contexts ORDER BY context";
        match db {
            Db::Postgres(p) => sqlx::query_scalar(sql).fetch_all(p).await.unwrap(),
            Db::Sqlite(p) => sqlx::query_scalar(sql).fetch_all(p).await.unwrap(),
        }
    }

    /// Who writes under a commit's context: a run once it owns it, and one
    /// who says a job skipped under a lease no run is in and no one else's
    /// is. A run waits a lease out, and one not given back lapses.
    async fn the_contexts_contract(db: &Db) {
        let (repo, sha, other) = ("repo-1", "a".repeat(40), "b".repeat(40));
        let test = "dagron-ci/ci/test (push)";
        let minute = Duration::from_secs(60);

        // A lease, where nobody has the context; its holder's again, and
        // nobody else's while it is in force.
        assert!(db
            .lease_context(repo, &sha, test, "ev-1", minute)
            .await
            .unwrap());
        assert!(db
            .lease_context(repo, &sha, test, "ev-1", minute)
            .await
            .unwrap());
        assert!(!db
            .lease_context(repo, &sha, test, "ev-2", minute)
            .await
            .unwrap());
        // A run waits it out: it is not given back by who does not hold it.
        assert!(!db.own_context(repo, &sha, test, "run-1").await.unwrap());
        db.release_context(repo, &sha, test, "ev-2").await.unwrap();
        assert!(!db.own_context(repo, &sha, test, "run-1").await.unwrap());
        // Given back: the run owns it, and no lease is had of it again,
        // though another run may own it too.
        db.release_context(repo, &sha, test, "ev-1").await.unwrap();
        assert!(db.own_context(repo, &sha, test, "run-1").await.unwrap());
        assert!(db.own_context(repo, &sha, test, "run-1").await.unwrap());
        assert!(!db
            .lease_context(repo, &sha, test, "ev-1", minute)
            .await
            .unwrap());
        assert!(db.own_context(repo, &sha, test, "run-2").await.unwrap());
        assert!(!db
            .lease_context(repo, &sha, test, "ev-3", minute)
            .await
            .unwrap());
        // Another commit's context is another.
        assert!(db
            .lease_context(repo, &other, test, "ev-1", minute)
            .await
            .unwrap());
        assert!(db
            .lease_context("repo-2", &sha, test, "ev-1", minute)
            .await
            .unwrap());

        // A lease not given back lapses: then a run owns the context, and
        // another may lease one nobody owns.
        let docs = "dagron-ci/ci/docs (push)";
        let second = Duration::from_secs(1);
        assert!(db
            .lease_context(repo, &sha, docs, "ev-1", second)
            .await
            .unwrap());
        assert!(!db.own_context(repo, &sha, docs, "run-1").await.unwrap());
        assert!(!db
            .lease_context(repo, &other, test, "ev-2", minute)
            .await
            .unwrap());
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert!(db.own_context(repo, &sha, docs, "run-1").await.unwrap());
        let lint = "dagron-ci/ci/lint (push)";
        assert!(db
            .lease_context(repo, &sha, lint, "ev-1", second)
            .await
            .unwrap());
        tokio::time::sleep(Duration::from_millis(1200)).await;
        assert!(db
            .lease_context(repo, &sha, lint, "ev-2", minute)
            .await
            .unwrap());
    }

    #[tokio::test]
    async fn a_context_is_a_run_s_once_it_owns_it_and_a_lease_s_while_one_is_in_force() {
        let (db, _dir) = sqlite().await;
        the_contexts_contract(&db).await;
    }

    #[tokio::test]
    async fn a_context_is_a_run_s_once_it_owns_it_and_a_lease_s_while_one_is_in_force_on_postgres()
    {
        let Some((db, schema, admin)) = postgres().await else {
            return;
        };
        the_contexts_contract(&db).await;
        db.close().await;
        drop_schema(&admin, &schema).await;
    }

    async fn event_ids(db: &Db) -> Vec<String> {
        match db {
            Db::Postgres(p) => sqlx::query_scalar("SELECT id FROM ci_events ORDER BY id")
                .fetch_all(p)
                .await
                .unwrap(),
            Db::Sqlite(p) => sqlx::query_scalar("SELECT id FROM ci_events ORDER BY id")
                .fetch_all(p)
                .await
                .unwrap(),
        }
    }

    async fn run_ids(db: &Db) -> Vec<String> {
        match db {
            Db::Postgres(p) => sqlx::query_scalar("SELECT run_id FROM ci_runs ORDER BY run_id")
                .fetch_all(p)
                .await
                .unwrap(),
            Db::Sqlite(p) => sqlx::query_scalar("SELECT run_id FROM ci_runs ORDER BY run_id")
                .fetch_all(p)
                .await
                .unwrap(),
        }
    }

    /// Events of a day past the horizon go, with their runs' rows and the
    /// names of the payloads they kept in the store; a later day's stay. On
    /// Postgres a day with its own partition goes as that partition, and one
    /// whose events landed in the default partition goes as rows of it.
    async fn the_days_contract(db: &Db) {
        let repo = db
            .add_repo("github", "github.com", "acme/widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        let id_of = |r: Recorded| match r {
            Recorded::Event(id) => id,
            Recorded::Duplicate => panic!("a new delivery"),
        };
        let old = id_of(
            db.record_delivery_stored(
                &repo,
                "d-old",
                &event(),
                "payloads/2026-01-01/0190a1b2.json",
            )
            .await
            .unwrap(),
        );
        let older = id_of(
            db.record_delivery(&repo, "d-older", &event(), b"{\"a\":1}")
                .await
                .unwrap(),
        );
        let new = id_of(
            db.record_delivery(&repo, "d-new", &event(), b"{\"a\":2}")
                .await
                .unwrap(),
        );
        assert_eq!(
            db.event_payload(&old).await.unwrap(),
            Some(EventPayload::Stored(
                "payloads/2026-01-01/0190a1b2.json".into()
            ))
        );
        assert_eq!(
            db.event_payload(&new).await.unwrap(),
            Some(EventPayload::Inline("{\"a\":2}".into()))
        );
        for (run, ev) in [("run-old", &old), ("run-new", &new)] {
            let submitted = SubmittedRun {
                run_id: run.into(),
                event_id: ev.clone(),
                repo_id: repo.id.clone(),
                cell: "default".into(),
                pipeline: ".dagron/ci/ci.yaml".into(),
                idempotency_key: format!("key-{run}"),
                on_failed: false,
            };
            assert!(db.record_run(&submitted).await.unwrap());
        }

        let today = Day::today();
        let (twenty, thirty) = (today.minus(20), today.minus(30));
        // Today's and the week's partitions are there from the migration.
        let ahead: Vec<Day> = (0..=7).map(|n| Day(today.0 + n)).collect();
        assert_eq!(
            db.ensure_event_partitions(&ahead).await.unwrap(),
            Vec::<String>::new()
        );
        let made = db.ensure_event_partitions(&[twenty]).await.unwrap();
        match db {
            Db::Postgres(_) => assert_eq!(made, [twenty.partition()]),
            Db::Sqlite(_) => assert!(made.is_empty(), "SQLite has no partitions"),
        }
        received_at(db, &old, &twenty.start()).await;
        received_at(db, &older, &thirty.start()).await;
        // Who writes under a context, of a commit of the old day's and of
        // today's; and one of the old day's that a later event's run owns
        // today, which is dated by that.
        let sha = "c".repeat(40);
        for (context, run) in [
            ("dagron-ci/ci/again (push)", "run-old"),
            ("dagron-ci/ci/old (push)", "run-old"),
            ("dagron-ci/ci/new (push)", "run-new"),
        ] {
            assert!(db.own_context(&repo.id, &sha, context, run).await.unwrap());
        }
        context_made_at(db, "dagron-ci/ci/old (push)", &twenty.start()).await;
        context_made_at(db, "dagron-ci/ci/again (push)", &twenty.start()).await;
        assert!(db
            .own_context(&repo.id, &sha, "dagron-ci/ci/again (push)", "run-new")
            .await
            .unwrap());
        // A day whose events are in the default partition has its own made
        // over none of them.
        assert!(db
            .ensure_event_partitions(&[thirty])
            .await
            .unwrap()
            .is_empty());

        // The older day goes; the one whose run is still followed (a rerun
        // reopened it, say) is kept.
        let held = db.drop_events_before(today.minus(14)).await.unwrap();
        assert_eq!((held.events, held.runs), (1, 0), "{held:?}");
        assert!(held.partitions.is_empty(), "{held:?}");
        assert_eq!(run_ids(db).await, ["run-new", "run-old"]);
        assert_eq!(
            contexts(db).await,
            [
                "dagron-ci/ci/again (push)",
                "dagron-ci/ci/new (push)",
                "dagron-ci/ci/old (push)"
            ],
            "made after the day dropped"
        );
        // Its run ends: its day goes.
        let ended = match db {
            Db::Postgres(p) => {
                sqlx::query("UPDATE ci_runs SET report_due_at = NULL WHERE run_id = 'run-old'")
                    .execute(p)
                    .await
                    .unwrap()
                    .rows_affected()
            }
            Db::Sqlite(p) => {
                sqlx::query("UPDATE ci_runs SET report_due_at = NULL WHERE run_id = 'run-old'")
                    .execute(p)
                    .await
                    .unwrap()
                    .rows_affected()
            }
        };
        assert_eq!(ended, 1);
        let dropped = db.drop_events_before(today.minus(14)).await.unwrap();
        match db {
            Db::Postgres(_) => assert_eq!(dropped.partitions, [twenty.partition()]),
            Db::Sqlite(_) => assert!(dropped.partitions.is_empty()),
        }
        assert_eq!((dropped.events, dropped.runs), (1, 1));
        assert_eq!(dropped.payloads, ["payloads/2026-01-01/0190a1b2.json"]);
        assert_eq!(event_ids(db).await, std::slice::from_ref(&new));
        assert_eq!(run_ids(db).await, ["run-new"]);
        assert_eq!(
            contexts(db).await,
            ["dagron-ci/ci/again (push)", "dagron-ci/ci/new (push)"],
            "one a run of a later day owns is kept with that day"
        );
        // A day of the horizon is kept, and nothing is left to drop.
        let again = db.drop_events_before(today.minus(14)).await.unwrap();
        assert_eq!(again, DroppedEvents::default());
        // New events still go in.
        assert!(matches!(
            db.record_delivery(&repo, "d-after", &event(), b"{}")
                .await
                .unwrap(),
            Recorded::Event(_)
        ));
    }

    #[tokio::test]
    async fn old_days_are_dropped_with_what_goes_with_them() {
        let (db, _dir) = sqlite().await;
        the_days_contract(&db).await;
    }

    #[tokio::test]
    async fn old_days_are_dropped_with_what_goes_with_them_on_postgres() {
        let Some((db, schema, admin)) = postgres().await else {
            return;
        };
        the_days_contract(&db).await;
        db.close().await;
        drop_schema(&admin, &schema).await;
    }

    /// The migration to partitions keeps every event, in the partition of
    /// its day, and the run API's requests that name them.
    #[tokio::test]
    async fn the_partitions_migration_keeps_every_event() {
        let Ok(url) = std::env::var("TEST_DATABASE_URL") else {
            eprintln!("TEST_DATABASE_URL unset - skipping");
            return;
        };
        let schema = format!("dagron_ci_test_{}", uuid::Uuid::now_v7().simple());
        let admin = PgPool::connect(&url).await.unwrap();
        sqlx::query(&format!("CREATE SCHEMA {schema}"))
            .execute(&admin)
            .await
            .unwrap();
        let options = PgConnectOptions::from_str(&url)
            .unwrap()
            .options([("search_path", schema.as_str())]);
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect_with(options)
            .await
            .unwrap();
        // Migrated as it was before partitions.
        let before = sqlx::migrate::Migrator {
            migrations: std::borrow::Cow::Owned(
                MIGRATIONS_PG
                    .migrations
                    .iter()
                    .filter(|m| m.version < 21)
                    .cloned()
                    .collect(),
            ),
            ignore_missing: false,
            locking: true,
            no_tx: false,
        };
        before.run(&pool).await.unwrap();
        sqlx::query("INSERT INTO ci_repos (id, forge, forge_host, full_name, cell) VALUES ('r', 'github', 'github.com', 'acme/widgets', 'default')")
            .execute(&pool)
            .await
            .unwrap();
        for (id, at) in [
            ("e-old", "2026-01-15T10:00:00Z"),
            ("e-new", "2026-10-18T23:59:59Z"),
        ] {
            sqlx::query(
                "INSERT INTO ci_events (id, received_at, repo_id, source, kind, ref, sha, trust, commit_key) \
                 VALUES ($1, $2::timestamptz, 'r', 'webhook', 'push', 'refs/heads/main', $3, 'trusted', $1)",
            )
            .bind(id)
            .bind(at)
            .bind("a".repeat(40))
            .execute(&pool)
            .await
            .unwrap();
        }
        sqlx::query("INSERT INTO ci_api_requests (principal, idempotency_key, fingerprint, event_id) VALUES ('u', 'k', 'f', 'e-old')")
            .execute(&pool)
            .await
            .unwrap();
        MIGRATIONS_PG.run(&pool).await.unwrap();
        let placed: Vec<(String, String)> = sqlx::query_as(
            "SELECT e.id, c.relname::TEXT FROM ci_events e JOIN pg_class c ON c.oid = e.tableoid ORDER BY e.id",
        )
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(
            placed,
            [
                ("e-new".to_string(), "ci_events_20261018".to_string()),
                ("e-old".to_string(), "ci_events_20260115".to_string()),
            ]
        );
        let kept: i64 =
            sqlx::query_scalar("SELECT count(*) FROM ci_api_requests WHERE event_id = 'e-old'")
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(kept, 1);
        let today = Day::today();
        for n in 0..=7 {
            let there: bool = sqlx::query_scalar("SELECT to_regclass($1) IS NOT NULL")
                .bind(Day(today.0 + n).partition())
                .fetch_one(&pool)
                .await
                .unwrap();
            assert!(there, "{}", Day(today.0 + n).partition());
        }
        pool.close().await;
        drop_schema(&admin, &schema).await;
    }

}

#[cfg(test)]
mod migration_lock_tests {
    //! The migration lock, live, each test on a database of its own: the
    //! lock's key is the database's, and a concurrent index build waits only
    //! on the sessions of its own database, so neither reaches the other live
    //! tests, which share TEST_DATABASE_URL's database.

    use super::*;
    use sqlx::migrate::Migrate;
    use sqlx::{Connection, PgConnection};

    /// A database made for one test, and dropped, with whatever is still
    /// connected to it, when the test ends, by a panic too.
    struct OwnDatabase {
        admin_url: String,
        name: String,
        url: String,
    }

    impl OwnDatabase {
        async fn new() -> Option<Self> {
            let admin_url = match std::env::var("TEST_DATABASE_URL") {
                Ok(u) if !u.trim().is_empty() => u,
                _ => {
                    eprintln!("TEST_DATABASE_URL unset - skipping");
                    return None;
                }
            };
            let name = format!("ci_mig_lock_{}", uuid::Uuid::now_v7().simple());
            let mut admin = PgConnection::connect(&admin_url).await.unwrap();
            let created = sqlx::query(&format!("CREATE DATABASE {name}"))
                .execute(&mut admin)
                .await;
            let _ = admin.close().await;
            if let Err(e) = created {
                // insufficient_privilege: a role without CREATEDB.
                if e.as_database_error().and_then(|db| db.code()).as_deref() == Some("42501") {
                    eprintln!("TEST_DATABASE_URL's role may not create a database - skipping");
                    return None;
                }
                panic!("CREATE DATABASE {name}: {e}");
            }
            let (head, query) = admin_url
                .split_once('?')
                .unwrap_or((admin_url.as_str(), ""));
            let (server, _) = head.rsplit_once('/').unwrap();
            let url = if query.is_empty() {
                format!("{server}/{name}")
            } else {
                format!("{server}/{name}?{query}")
            };
            Some(Self {
                admin_url,
                name,
                url,
            })
        }

        async fn connect(&self) -> PgConnection {
            PgConnection::connect(&self.url).await.unwrap()
        }
    }

    impl Drop for OwnDatabase {
        fn drop(&mut self) {
            let (admin_url, name) = (self.admin_url.clone(), self.name.clone());
            // A thread and a runtime of its own: a drop cannot await, and the
            // test's runtime may be the one unwinding.
            let _ = std::thread::spawn(move || {
                let runtime = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                runtime.block_on(async {
                    if let Ok(mut admin) = PgConnection::connect(&admin_url).await {
                        let _ =
                            sqlx::query(&format!("DROP DATABASE IF EXISTS {name} WITH (FORCE)"))
                                .execute(&mut admin)
                                .await;
                        let _ = admin.close().await;
                    }
                });
            })
            .join();
        }
    }

    #[test]
    fn the_key_s_crc_is_crc_32_iso_hdlc() {
        // The CRC catalogue's check value for CRC-32/ISO-HDLC.
        assert_eq!(crc32_iso_hdlc(b"123456789"), 0xCBF4_3926);
        assert_eq!(crc32_iso_hdlc(b""), 0);
    }

    /// sqlx's migration lock, which engines and dagron-ci of earlier releases
    /// wait for, and dagron-ci's are one lock: while either is held the
    /// other is not to be had.
    #[tokio::test]
    async fn sqlx_s_migration_lock_and_dagron_ci_s_exclude_each_other() {
        let Some(db) = OwnDatabase::new().await else {
            return;
        };
        let (mut sqlx_s, mut ours) = (db.connect().await, db.connect().await);
        let key = migration_lock_key(&db.name);

        sqlx_s.lock().await.unwrap();
        assert!(
            !try_migration_lock(&mut ours, key).await.unwrap(),
            "sqlx's lock holds dagron-ci's off"
        );
        sqlx_s.unlock().await.unwrap();
        assert!(
            try_migration_lock(&mut ours, key).await.unwrap(),
            "free once sqlx's is let go"
        );

        // sqlx's waits in its statement while dagron-ci's is held.
        let sqlx_waits = tokio::spawn(async move {
            sqlx_s.lock().await.unwrap();
            sqlx_s
        });
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert!(
            !sqlx_waits.is_finished(),
            "dagron-ci's lock holds sqlx's off"
        );
        let released: bool = sqlx::query_scalar("SELECT pg_advisory_unlock($1)")
            .bind(key)
            .fetch_one(&mut ours)
            .await
            .unwrap();
        assert!(released, "the lock was this session's");
        let mut sqlx_s = tokio::time::timeout(Duration::from_secs(10), sqlx_waits)
            .await
            .expect("sqlx's lock is taken once dagron-ci's is let go")
            .unwrap();
        sqlx_s.unlock().await.unwrap();
        let _ = sqlx_s.close().await;
        let _ = ours.close().await;
    }

    /// What became of an engine's concurrent index build, run while it held
    /// the migration lock and dagron-ci waited for the lock.
    struct BuiltWhileCiWaited {
        build: std::result::Result<(), sqlx::Error>,
        index_valid: Option<bool>,
        ci: anyhow::Result<()>,
    }

    /// An engine takes the migration lock (sqlx's way, at sqlx's key); a
    /// dagron-ci starts on the same database, in a schema of its own, and
    /// comes to wait for the lock (by [`Db::connect`], or, with `sqlx_s_way`,
    /// by sqlx's `run`, as dagron-ci did before); the engine then builds an
    /// index concurrently, as a `-- no-transaction` migration does, and lets
    /// the lock go.
    async fn build_an_index_while_dagron_ci_waits(
        db: &OwnDatabase,
        sqlx_s_way: bool,
    ) -> BuiltWhileCiWaited {
        let mut engine = db.connect().await;
        sqlx::query("CREATE TABLE things (id int)")
            .execute(&mut engine)
            .await
            .unwrap();
        sqlx::query("INSERT INTO things SELECT generate_series(1, 1000)")
            .execute(&mut engine)
            .await
            .unwrap();
        engine.lock().await.unwrap();
        let (url, key) = (db.url.clone(), migration_lock_key(&db.name));
        let ci = tokio::spawn(async move {
            if sqlx_s_way {
                let options =
                    PgConnectOptions::from_str(&url)?.options([("search_path", "dagron_ci")]);
                let pool = PgPoolOptions::new()
                    .max_connections(2)
                    .connect_with(options)
                    .await?;
                create_schema(&pool, "dagron_ci").await?;
                MIGRATIONS_PG.run(&pool).await?;
                pool.close().await;
            } else {
                let config = Config::from_lookup(|name| match name {
                    "CI_DATABASE_URL" => Some(url.clone()),
                    "CI_DATABASE_SCHEMA" => Some("dagron_ci".to_string()),
                    _ => None,
                })?;
                if let Db::Postgres(pool) = Db::connect(&config).await? {
                    // The pool is still open, as it is while dagron-ci serves,
                    // and the lock is to be had: the connection that migrated
                    // did not keep it.
                    let mut other = PgConnection::connect(&url).await?;
                    let free = try_migration_lock(&mut other, key).await?;
                    let _ = other.close().await;
                    pool.close().await;
                    anyhow::ensure!(free, "dagron-ci kept the migration lock");
                }
            }
            Ok::<_, anyhow::Error>(())
        });
        // Longer than deadlock_timeout (1 s by default): dagron-ci's own
        // deadlock check, if it waits in a statement, has run and found
        // nothing before the build begins, as when it starts beside an engine
        // that is migrating. A cycle the build then closes is the build's to
        // break.
        tokio::time::sleep(Duration::from_millis(1500)).await;
        assert!(!ci.is_finished(), "dagron-ci waits for the lock");
        // And it waits because it asks for the lock, not because it is slow:
        // a session other than the engine's last asked for an advisory lock.
        let asks_by = tokio::time::Instant::now() + Duration::from_secs(10);
        loop {
            let asks: bool = sqlx::query_scalar(
                "SELECT EXISTS (SELECT 1 FROM pg_stat_activity
                  WHERE datname = current_database() AND pid <> pg_backend_pid()
                    AND query LIKE '%advisory_lock(%')",
            )
            .fetch_one(&mut engine)
            .await
            .unwrap();
            if asks {
                break;
            }
            assert!(
                tokio::time::Instant::now() < asks_by,
                "dagron-ci asks for the migration lock"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let build = tokio::time::timeout(
            Duration::from_secs(20),
            sqlx::query("CREATE INDEX CONCURRENTLY things_id ON things (id)").execute(&mut engine),
        )
        .await
        .expect("the index build ends within 20 s")
        .map(|_| ());
        engine.unlock().await.unwrap();
        let ci = tokio::time::timeout(Duration::from_secs(30), ci)
            .await
            .expect("dagron-ci migrates within 30 s of the lock's release")
            .unwrap();
        let index_valid = sqlx::query_scalar(
            "SELECT i.indisvalid FROM pg_index i JOIN pg_class c ON c.oid = i.indexrelid
              WHERE c.relname = 'things_id'",
        )
        .fetch_optional(&mut engine)
        .await
        .unwrap();
        let _ = engine.close().await;
        BuiltWhileCiWaited {
            build,
            index_valid,
            ci,
        }
    }

    /// The deadlock dagron-ci polls for the lock to avoid: with dagron-ci
    /// waiting for the migration lock, the engine's concurrent index build
    /// completes, and the index is valid. dagron-ci then migrates.
    #[tokio::test]
    async fn an_engine_s_index_build_completes_while_dagron_ci_waits_for_the_lock() {
        let Some(db) = OwnDatabase::new().await else {
            return;
        };
        let outcome = build_an_index_while_dagron_ci_waits(&db, false).await;
        outcome.build.expect("the index build completes");
        assert_eq!(outcome.index_valid, Some(true), "the index is valid");
        outcome
            .ci
            .expect("dagron-ci migrates once the lock is let go");
    }

    /// The same, with dagron-ci waiting as sqlx's migrator does, in one
    /// `SELECT pg_advisory_lock`: Postgres finds the build and dagron-ci
    /// waiting on each other and fails the build, which leaves the index
    /// INVALID. Ignored, for it shows the defect rather than guards against
    /// one: it is what makes the test above a test.
    #[tokio::test]
    #[ignore = "shows the deadlock sqlx's own lock meets; run by hand"]
    async fn an_engine_s_index_build_while_dagron_ci_waits_sqlx_s_way_deadlocks() {
        let Some(db) = OwnDatabase::new().await else {
            return;
        };
        let outcome = build_an_index_while_dagron_ci_waits(&db, true).await;
        let err = outcome
            .build
            .expect_err("the index build is the deadlock's victim");
        eprintln!("the index build failed: {err}");
        let code = err
            .as_database_error()
            .and_then(|e| e.code().map(|c| c.into_owned()));
        assert_eq!(code.as_deref(), Some("40P01"), "deadlock_detected");
        assert_eq!(
            outcome.index_valid,
            Some(false),
            "the build left the index INVALID"
        );
        outcome
            .ci
            .expect("dagron-ci migrates once the lock is let go");
    }
}
