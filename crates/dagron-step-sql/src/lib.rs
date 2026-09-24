//! dagron SQL step — run one statement against an analytical store.
//!
//! One binary, one statement, per-protocol transports. Named after what people
//! search for (ClickHouse, StarRocks, Postgres) and implemented against what
//! actually varies (HTTP, MySQL wire, Postgres wire), because Airflow ran the
//! per-store-operator experiment and reversed it: `SnowflakeOperator`,
//! `BigQueryExecuteQueryOperator`, `PostgresOperator` and `TrinoOperator` were
//! all deprecated in favour of one `SQLExecuteQueryOperator` plus a connection.
//! Per-store operators rot; generic SQL plus a per-store connection ages.
//! Maintenance here is bounded by protocol count, not vendor count.
//!
//! ## What this actually buys over `command: ["sh","-c","clickhouse-client …"]`
//!
//! That already works, and the dagron docs say so. Two things it cannot do:
//!
//! 1. **A bounded result contract** instead of `SELECT *` into an uncapped
//!    column — see [`Mode`], and the section below on what it does and does
//!    not bound.
//! 2. One image instead of N vendor CLIs baked into N task images.
//!
//! Two more are what having a *binary* here leaves open rather than what it
//! does. **Neither is built** — do not plan around either:
//!
//! * **A cancel hook** the engine could reach (`KILL QUERY <query_id>`).
//!   Nothing here captures a query id, so there is nothing to kill: a cancelled
//!   run abandons the statement exactly as an opaque `command:` would.
//! * **A defer handle.** This step never prints `dagron::handle=`, so `defer:`
//!   on a `dagron-step-sql` task would park on nothing. A 40-minute query holds
//!   its worker for 40 minutes. `dagron-step-spark` is the deferred one.
//!
//! ## The output contract is the point
//!
//! Live-log streaming is unconditional in the engine, and each stdout line is
//! appended to the task's `output` column with `output = COALESCE(output,'') ||
//! ?` — **no cap anywhere on that path**. So a `SELECT *` that writes rows to
//! stdout is an unbounded write into the datastore, and a check applied *after*
//! the rows are printed is a check applied after the damage.
//!
//! Therefore: rows never go to stdout, and [`Budget`] is checked **before each
//! row is written**, so an over-budget statement writes nothing rather than
//! being cleaned up afterwards.
//!
//! ### The budget bounds the read as well as the write
//!
//! Both transports stream. The HTTP path reads `bytes_stream()` and parses
//! NDJSON incrementally; the wire path uses `fetch` rather than `fetch_all`. A
//! row is admitted as it arrives and the stream is dropped at the first refusal,
//! so an over-budget result stops arriving rather than being read in full and
//! rejected afterwards. That buys two guarantees rather than one:
//!
//! * The datastore is protected. An over-budget result never reaches the
//!   uncapped `output` column or the artifact, which is the failure this
//!   contract exists to prevent. `rows` mode writes to a temporary file and
//!   renames it into place only once the whole result has been admitted, so a
//!   refusal leaves *nothing* at the artifact path rather than a partial file
//!   to clean up — streaming the write does not weaken the write-side promise.
//! * **The step's own memory is bounded too.** A `SELECT *` over a table larger
//!   than the container's limit now fails with the message naming
//!   `SQL_MAX_ROWS` or `SQL_MAX_BYTES`, rather than being OOM-killed before the
//!   refusal can print and leaving the operator an exit code with no
//!   explanation. Resident memory is one row plus one chunk of the stream,
//!   whatever `SQL_MAX_BYTES` is set to.
//!
//! The two transports bound that at different granularities, and the difference
//! is worth knowing. The HTTP path sees raw bytes, so [`Budget::check_pending`]
//! can refuse part-way through a row. The wire path does not: sqlx hands over a
//! row already decoded, so one row is the smallest thing that can be refused and
//! a single enormous cell is in memory before the budget is consulted. One row
//! rather than the whole result is the guarantee there.
//!
//! One case does not follow from "admit each row as it arrives": a store that
//! answers with a single enormous line. Nothing completes, so nothing is
//! admitted, and the line buffer grows unbounded. [`Budget::check_pending`]
//! closes it by refusing a row that is still arriving once the bytes already
//! read exceed what the budget could admit.
//!
//! A `LIMIT` in the statement is still the better way to ask for less data — it
//! is the store that stops working rather than this step that stops reading —
//! but it is no longer what stands between a careless `SELECT *` and an exit
//! code nobody can explain.

use anyhow::{bail, Context, Result};

/// Which wire protocol a store speaks. Vendors are named in the docs and in
/// [`Engine`]; only these three are implemented, and that is the whole
/// maintenance bound.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    /// ClickHouse `:8123`, and anything else that speaks the same shape: one
    /// POST carrying the statement, one response carrying the whole answer.
    ///
    /// Deliberately NOT Trino. Trino's `/v1/statement` is a different protocol
    /// wearing the same verb — the client must follow `nextUri` until it is
    /// absent, echo response headers back, and read `QueryResults.error`
    /// rather than trusting the HTTP status. Routing it here would append
    /// ClickHouse's `FORMAT JSONEachRow`, send `X-ClickHouse-User`, and report
    /// success on a queued query that has returned no rows yet. It needs its
    /// own transport, not this one.
    Http,
    /// StarRocks `:9030`, Doris, MySQL.
    MySql,
    /// Postgres, Redshift, Materialize, CockroachDB, Greenplum.
    Postgres,
}

/// A store, as a person names it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Engine {
    ClickHouse,
    StarRocks,
    Doris,
    Postgres,
    Redshift,
    MySql,
}

impl Engine {
    pub fn parse(raw: &str) -> Result<Self> {
        Ok(match raw.trim().to_ascii_lowercase().as_str() {
            "clickhouse" => Engine::ClickHouse,
            "starrocks" => Engine::StarRocks,
            "doris" => Engine::Doris,
            "postgres" | "postgresql" => Engine::Postgres,
            "redshift" => Engine::Redshift,
            "mysql" => Engine::MySql,
            "" => bail!("SQL_ENGINE is required (clickhouse, starrocks, doris, postgres, redshift, mysql)"),
            other => bail!(
                "unknown SQL_ENGINE '{other}'. Known: clickhouse, starrocks, doris, \
                 postgres, redshift, mysql. A store not listed here very likely speaks one of \
                 these three wires already — Redshift is Postgres, Doris is MySQL — so try the \
                 one it is compatible with rather than waiting for a name. Trino is NOT one of \
                 them: its /v1/statement protocol needs a nextUri loop this step does not \
                 implement, so it is refused rather than routed through the ClickHouse path."
            ),
        })
    }

    /// The wire this store speaks.
    ///
    /// StarRocks and Doris default to the MySQL wire because that is their
    /// primary interface, even though both also answer over HTTP.
    pub fn transport(self) -> Transport {
        match self {
            Engine::ClickHouse => Transport::Http,
            Engine::StarRocks | Engine::Doris | Engine::MySql => Transport::MySql,
            Engine::Postgres | Engine::Redshift => Transport::Postgres,
        }
    }
}

/// What the statement is expected to produce, and therefore where it may go.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    /// A statement that returns nothing worth keeping (INSERT, MERGE, DDL).
    ///
    /// Writes **nothing to stdout** — the outcome goes to the log, and what the
    /// log carries differs by transport (the wire path has `rows_affected`, the
    /// HTTP path does not), so it is not a contract to gate on. A downstream
    /// `when:` that needs a value wants `scalar`.
    Exec,
    /// Exactly one row, one column, to stdout — so a downstream `when:` can
    /// gate on it. Bounded to [`MAX_SCALAR_BYTES`], because this is the one
    /// mode that writes to the uncapped `output` column on purpose.
    ///
    /// "Exactly one row" is **enforced**, not assumed: a statement returning
    /// more than one is refused rather than having its first row taken. Without
    /// an `ORDER BY` there is no first row, only whichever the store sent
    /// first, so silently picking it makes a gate depend on an order nothing
    /// fixed. Both transports read one row past the answer to check.
    Scalar,
    /// Many rows, as NDJSON, into `$DAGRON_ARTIFACTS`. Never stdout.
    Rows,
    /// Several statements, run in order on one connection as one script — a write
    /// that is a sequence, like `BEGIN; DROP TABLE t; CREATE TABLE t AS …; COMMIT;`,
    /// which is how a planner recreates a Postgres table (Postgres has no
    /// `CREATE OR REPLACE TABLE`). Nothing to stdout, like `exec`.
    ///
    /// **Wire transports only.** They send the script as one simple-query message,
    /// which the store runs statement by statement on the same session — and which
    /// Postgres runs as a single implicit transaction unless the script manages its
    /// own. `exec` cannot: it prepares its statement, and a prepared statement holds
    /// exactly one. ClickHouse's HTTP interface takes one statement per request, so
    /// a script there is refused rather than cut down to its first statement.
    Script,
}

impl Mode {
    pub fn parse(raw: Option<&str>) -> Result<Self> {
        match raw.map(str::trim).filter(|s| !s.is_empty()) {
            None | Some("exec") => Ok(Mode::Exec),
            Some("scalar") => Ok(Mode::Scalar),
            Some("rows") => Ok(Mode::Rows),
            Some("script") => Ok(Mode::Script),
            Some(other) => bail!(
                "unknown SQL_MODE '{other}'. Expected 'exec' (no rows kept), 'scalar' (one \
                 value to stdout, for a `when:` gate), 'rows' (NDJSON into \
                 $DAGRON_ARTIFACTS), or 'script' (several statements in order, wire \
                 transports only)."
            ),
        }
    }

    /// Whether this mode produces rows that must land in an artifact file.
    pub fn needs_artifacts(self) -> bool {
        matches!(self, Mode::Rows)
    }
}

/// The most a `scalar` result may write to stdout.
///
/// Small deliberately. stdout is appended to the task's `output` column with no
/// cap, and the only reason `scalar` writes there at all is so a downstream
/// `when:` can read one value. A value that does not fit in a kilobyte is not
/// the kind of value a gate compares.
pub const MAX_SCALAR_BYTES: usize = 1024;

/// Default ceiling on rows fetched in `rows` mode.
pub const DEFAULT_MAX_ROWS: u64 = 10_000;
/// Default ceiling on bytes written in `rows` mode (8 MiB).
pub const DEFAULT_MAX_BYTES: u64 = 8 * 1024 * 1024;

/// A budget on both sides of the transfer: checked before each row is read into
/// the process, and therefore before each row is written.
///
/// Both transports stream, so a row reaches [`admit`](Budget::admit) as it
/// arrives off the wire and a refusal stops the read. See the crate docs.
///
/// Refusing *before* rather than *after* is the whole design. A post-hoc "was
/// that too big?" fires once the rows are already in the artifact — or worse,
/// already appended to the task's `output` — and failing the task afterwards
/// does not remove them.
#[derive(Debug, Clone, Copy)]
pub struct Budget {
    pub max_rows: u64,
    pub max_bytes: u64,
    rows: u64,
    bytes: u64,
}

impl Budget {
    pub fn new(max_rows: u64, max_bytes: u64) -> Self {
        Budget { max_rows, max_bytes, rows: 0, bytes: 0 }
    }

    /// Account for one row that has just arrived and is about to be written.
    /// `Err` means **do not write it, and stop reading**.
    ///
    /// Called per row as the transport yields it, so the result set behind the
    /// refused row is never read. What it prevents is rows reaching the task's
    /// uncapped output column or its artifact; what streaming adds is that they
    /// never reach this process's memory either.
    pub fn admit(&mut self, row_bytes: usize) -> Result<()> {
        self.rows += 1;
        self.bytes += row_bytes as u64;
        if self.rows > self.max_rows {
            bail!(
                "the statement returned more than SQL_MAX_ROWS ({}) rows. Raise the limit if \
                 you meant it, or add a LIMIT — this is refused before anything is written, \
                 because a task's output has no cap and failing afterwards does not unwrite \
                 anything.",
                self.max_rows
            );
        }
        if self.bytes > self.max_bytes {
            bail!(
                "the statement returned more than SQL_MAX_BYTES ({}) bytes. Same reasoning as \
                 the row cap: refused before the bytes land anywhere durable.",
                self.max_bytes
            );
        }
        Ok(())
    }

    /// Refuse a row that is *still arriving* once it has outgrown the budget.
    ///
    /// [`admit`](Budget::admit) can only be called on a complete row, which
    /// leaves one gap: a store that answers with a single enormous line commits
    /// this process to buffering all of it before the budget is ever consulted.
    /// The reader calls this with the length of the partial row it is holding,
    /// so the refusal fires while the rest is still on the wire.
    ///
    /// Takes `&self`: a partial row is not a row, so it is not counted. It is
    /// counted by `admit` if it ever completes.
    pub fn check_pending(&self, pending_bytes: usize) -> Result<()> {
        let projected = self.bytes.saturating_add(pending_bytes as u64);
        if projected > self.max_bytes {
            bail!(
                "the statement's result passed SQL_MAX_BYTES ({}) part-way through a row — \
                 {projected} bytes read with the row still incomplete. Refused while it is \
                 still arriving, so it lands neither in the artifact nor in this step's \
                 memory. A store answering with one enormous row rather than many rows hits \
                 this rather than the row cap.",
                self.max_bytes
            );
        }
        Ok(())
    }

    pub fn rows_written(&self) -> u64 {
        self.rows
    }
}

/// Where the credential comes from.
///
/// **Not the DSN.** `Redactor::DEFAULT_PATTERNS` masks a task env var by *name*
/// — SECRET, TOKEN, PASSWORD and friends — so `DAGRON_SQL_DSN=clickhouse://user:pw@host`
/// is masked precisely nowhere: not in a log line, not in an error, not in the
/// stderr warning a failure prints. Taking the password in its own var named so
/// the default patterns catch it is the difference between a credential that is
/// redacted everywhere and one that is redacted nowhere.
///
/// The mirrored precedent is `examples/marketing/05_paid_media_cost_sync.yaml`,
/// where `SNOWFLAKE_PWD` is fed by `value_from: { secret: SNOWFLAKE_PASSWORD }`.
///
/// Note the redactor's `MIN_LEN = 4`: a password shorter than four characters is
/// never masked by anything, here or elsewhere.
pub const PASSWORD_VAR: &str = "SQL_PASSWORD";

/// Refuse a DSN that carries an inline password, naming the fix.
///
/// A hard error rather than a warning, because the cost of the mistake is a
/// credential in a log the operator did not know was there, and a warning is
/// read after the fact if at all.
pub fn reject_inline_credential(dsn: &str) -> Result<()> {
    // `scheme://user:pass@host` — the colon inside the userinfo is the tell.
    let Some(after) = dsn.split_once("://").map(|(_, r)| r) else { return Ok(()) };
    let Some((userinfo, _)) = after.split_once('@') else { return Ok(()) };
    if userinfo.contains(':') {
        bail!(
            "SQL_DSN carries an inline password. Nothing masks it: the redactor matches task \
             env vars by NAME (SECRET/TOKEN/PASSWORD/…), and 'SQL_DSN' matches none of them, so \
             the credential would appear unmasked in logs and in any error this step prints. \
             Put the user in the DSN and the password in {PASSWORD_VAR}, fed by \
             `value_from: {{ secret: … }}` — that name the redactor does match."
        );
    }
    Ok(())
}

/// Resolve the artifact directory for `rows` mode.
///
/// A hard refusal when unset, rather than a fallback to stdout or the working
/// directory. Falling back to stdout is the unbounded write this whole contract
/// exists to prevent; falling back to the working directory writes rows into a
/// container that is about to disappear, which looks like success and loses the
/// data.
pub fn artifacts_dir(get: impl Fn(&str) -> Option<String>) -> Result<String> {
    get("DAGRON_ARTIFACTS")
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())
        .context(
            "SQL_MODE=rows writes NDJSON into $DAGRON_ARTIFACTS, which is not set. The engine \
             injects it per task only when the operator has set DAGRON_ARTIFACT_DIR. Without an \
             artifact store there is nowhere durable to put rows — stdout is appended to the \
             task's uncapped output column, and the container's filesystem disappears with the \
             task — so this refuses rather than pretending to have written them.",
        )
}

/// One result row as an NDJSON line (trailing newline included).
pub fn ndjson_line(row: &serde_json::Map<String, serde_json::Value>) -> String {
    let mut s = serde_json::to_string(row).unwrap_or_else(|_| "{}".into());
    s.push('\n');
    s
}

/// Bound a scalar before it reaches stdout.
pub fn scalar_for_stdout(v: &str) -> Result<String> {
    let v = v.trim();
    if v.len() > MAX_SCALAR_BYTES {
        bail!(
            "the scalar result is {} bytes, over the {MAX_SCALAR_BYTES}-byte limit for stdout. \
             `scalar` exists so a downstream `when:` can read one value; anything larger belongs \
             in SQL_MODE=rows, which writes to an artifact instead of the task's output column.",
            v.len()
        );
    }
    Ok(v.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn engines_map_to_the_three_wires_that_actually_exist() {
        assert_eq!(Engine::parse("clickhouse").unwrap().transport(), Transport::Http);
        // Trino is deliberately absent: its client protocol is not this one.
        // Accepting the name would route it through the ClickHouse transport.
        assert!(Engine::parse("trino").is_err(), "trino is not implemented, so it is not accepted");
        assert_eq!(Engine::parse(" StarRocks ").unwrap().transport(), Transport::MySql);
        assert_eq!(Engine::parse("doris").unwrap().transport(), Transport::MySql);
        assert_eq!(Engine::parse("redshift").unwrap().transport(), Transport::Postgres,
            "Redshift speaks the Postgres wire — which is also why gating it by vendor name \
             would be unenforceable");
        let e = Engine::parse("snowflake").unwrap_err().to_string();
        assert!(e.contains("Known:"), "{e}");
        assert!(e.contains("Redshift is Postgres"), "teaches the mapping: {e}");
    }

    #[test]
    fn modes_parse_and_only_rows_needs_an_artifact_dir() {
        assert_eq!(Mode::parse(None).unwrap(), Mode::Exec);
        assert_eq!(Mode::parse(Some("scalar")).unwrap(), Mode::Scalar);
        assert!(Mode::parse(Some("rows")).unwrap().needs_artifacts());
        assert!(!Mode::parse(Some("exec")).unwrap().needs_artifacts());
        assert!(!Mode::parse(Some("scalar")).unwrap().needs_artifacts());
        assert_eq!(Mode::parse(Some("script")).unwrap(), Mode::Script);
        assert!(!Mode::Script.needs_artifacts(), "a script writes nothing anywhere but the store");
        assert!(Mode::parse(Some("csv")).unwrap_err().to_string().contains("unknown SQL_MODE"));
    }

    /// The budget refuses the row it is called with, so the caller never writes
    /// it. A budget that reported "too big" after the write would be a report,
    /// not a budget.
    #[test]
    fn the_budget_refuses_before_the_write_not_after() {
        let mut b = Budget::new(3, 1_000_000);
        for _ in 0..3 {
            b.admit(10).unwrap();
        }
        let e = b.admit(10).unwrap_err().to_string();
        assert!(e.contains("SQL_MAX_ROWS (3)"), "{e}");
        assert!(e.contains("does not unwrite"), "says why it refuses before writing: {e}");
        assert_eq!(b.rows_written(), 4, "the refused row is counted, and not written");

        let mut b = Budget::new(1_000_000, 100);
        b.admit(60).unwrap();
        assert!(b.admit(60).unwrap_err().to_string().contains("SQL_MAX_BYTES"));
    }

    /// The gap `admit` alone leaves: a store that answers with one enormous row
    /// completes no line, so nothing is ever admitted and the reader's buffer
    /// grows without a check. `check_pending` is what makes "stream the read"
    /// an actual bound rather than a bound on well-shaped results only.
    #[test]
    fn a_row_still_arriving_is_refused_once_it_outgrows_the_budget() {
        let mut b = Budget::new(1_000_000, 100);
        b.check_pending(100).unwrap();
        let e = b.check_pending(101).unwrap_err().to_string();
        assert!(e.contains("SQL_MAX_BYTES (100)"), "{e}");
        assert!(e.contains("still arriving"), "says it refused mid-row: {e}");

        // A partial row is not a row: nothing was counted, so a later complete
        // row is still admitted against the full budget.
        assert_eq!(b.rows_written(), 0);
        b.admit(40).unwrap();
        // …and what has already been admitted narrows what the next partial row
        // may grow to, which is the point of projecting rather than comparing.
        assert!(b.check_pending(61).is_err(), "40 already admitted leaves 60");
        b.check_pending(60).unwrap();
    }

    /// The credential rule, which exists because nothing masks a DSN.
    #[test]
    fn an_inline_dsn_password_is_refused_with_the_fix() {
        let e = reject_inline_credential("clickhouse://user:hunter2@host:8123")
            .unwrap_err()
            .to_string();
        assert!(e.contains("SQL_PASSWORD"), "names the var that IS masked: {e}");
        assert!(e.contains("by NAME"), "explains why the DSN is not: {e}");

        // A user with no password is fine, and so is a DSN with no userinfo —
        // the port colon must not be mistaken for a credential.
        reject_inline_credential("clickhouse://user@host:8123").unwrap();
        reject_inline_credential("clickhouse://host:8123/db").unwrap();
        reject_inline_credential("not a url").unwrap();
    }

    #[test]
    fn rows_mode_refuses_rather_than_falling_back_when_there_is_no_artifact_store() {
        let e = artifacts_dir(|_| None).unwrap_err().to_string();
        assert!(e.contains("DAGRON_ARTIFACT_DIR"), "names the operator knob: {e}");
        assert!(e.contains("uncapped output column"), "names what the fallback would cost: {e}");
        assert_eq!(artifacts_dir(|_| Some("/artifacts".into())).unwrap(), "/artifacts");
        assert!(artifacts_dir(|_| Some("   ".into())).is_err(), "blank is unset");
    }

    #[test]
    fn a_scalar_is_bounded_before_it_reaches_the_output_column() {
        assert_eq!(scalar_for_stdout("  ok  ").unwrap(), "ok");
        let big = "x".repeat(MAX_SCALAR_BYTES + 1);
        let e = scalar_for_stdout(&big).unwrap_err().to_string();
        assert!(e.contains("SQL_MODE=rows"), "points at the mode that can hold it: {e}");
    }

    #[test]
    fn ndjson_is_one_object_per_line() {
        let mut row = serde_json::Map::new();
        row.insert("n".into(), json!(1));
        let line = ndjson_line(&row);
        assert!(line.ends_with('\n'));
        assert_eq!(line.trim(), r#"{"n":1}"#);
    }
}
