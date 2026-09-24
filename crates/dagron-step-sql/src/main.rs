//! dagron-step-sql — run one SQL statement as a workflow task.
//!
//! Invoked as a task `command`. Reads the statement from `SQL_STATEMENT` (or
//! `SQL_STATEMENT_FILE`), runs it against `SQL_ENGINE`, and honours the output
//! contract in [`dagron_step_sql`]: nothing to stdout in `exec`, one bounded
//! value in `scalar`, NDJSON into `$DAGRON_ARTIFACTS` in `rows`.

use anyhow::{bail, Context, Result};
use dagron_step_sql::{
    artifacts_dir, reject_inline_credential, scalar_for_stdout, Budget, Engine, Mode, Transport,
    DEFAULT_MAX_BYTES, DEFAULT_MAX_ROWS, PASSWORD_VAR,
};
// Only the wire transports re-encode rows; the HTTP path copies the store's own
// JSONEachRow lines through, so this is unused in a default build.
#[cfg(any(feature = "mysql", feature = "postgres"))]
use dagron_step_sql::ndjson_line;

#[tokio::main]
async fn main() -> Result<()> {
    dagron_logging::init("dagron-step-sql");

    let engine = Engine::parse(&std::env::var("SQL_ENGINE").unwrap_or_default())?;
    let mode = Mode::parse(std::env::var("SQL_MODE").ok().as_deref())?;
    let dsn = std::env::var("SQL_DSN")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .context("SQL_DSN is required (e.g. http://clickhouse:8123/?database=analytics)")?;
    reject_inline_credential(&dsn)?;

    let statement = match std::env::var("SQL_STATEMENT_FILE").ok().filter(|s| !s.trim().is_empty())
    {
        Some(f) => std::fs::read_to_string(&f).with_context(|| format!("reading {f}"))?,
        None => std::env::var("SQL_STATEMENT")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .context("SQL_STATEMENT (or SQL_STATEMENT_FILE) is required")?,
    };

    // Resolve the artifact path BEFORE running anything. A statement that runs
    // and then discovers it has nowhere to put its rows has already done its
    // work — and in `rows` mode that work is usually the expensive half.
    let out_path = if mode.needs_artifacts() {
        let dir = artifacts_dir(|k| std::env::var(k).ok())?;
        let name = std::env::var("SQL_OUTPUT_NAME").unwrap_or_else(|_| "result.ndjson".into());
        Some(std::path::Path::new(&dir).join(name))
    } else {
        None
    };

    let budget = Budget::new(
        env_u64("SQL_MAX_ROWS", DEFAULT_MAX_ROWS)?,
        env_u64("SQL_MAX_BYTES", DEFAULT_MAX_BYTES)?,
    );

    match engine.transport() {
        Transport::Http if mode == Mode::Script => bail!(
            "SQL_MODE=script needs a wire transport: {engine:?} is reached over HTTP, which runs \
             one statement per request. Run each statement as its own task instead — sending \
             the script anyway would run its first statement and drop the rest."
        ),
        Transport::Http => run_http(&dsn, &statement, mode, out_path, budget).await,
        Transport::MySql => run_wire(engine, &dsn, &statement, mode, out_path, budget).await,
        Transport::Postgres => run_wire(engine, &dsn, &statement, mode, out_path, budget).await,
    }
}

fn env_u64(key: &str, default: u64) -> Result<u64> {
    match std::env::var(key).ok().filter(|s| !s.trim().is_empty()) {
        None => Ok(default),
        Some(raw) => raw
            .trim()
            .parse()
            .with_context(|| format!("{key} must be a non-negative integer, got '{raw}'")),
    }
}

/// SQL over HTTP — ClickHouse `:8123` and anything else that takes a
/// statement in a request body.
///
/// The statement goes in the body rather than the query string: a URL-encoded
/// statement shows up in every access log between here and the store, and a
/// long one runs into URL length limits at the worst possible time.
///
/// The response body is **streamed**, never buffered. Which mode is running
/// decides how much of it is read at all: `exec` reads none of it, `scalar`
/// stops after the first line, and `rows` holds one line at a time.
async fn run_http(
    dsn: &str,
    statement: &str,
    mode: Mode,
    out_path: Option<std::path::PathBuf>,
    mut budget: Budget,
) -> Result<()> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(env_u64("SQL_TIMEOUT_SECS", 300)? ))
        .build()
        .context("building the HTTP client")?;

    // ClickHouse answers JSONEachRow natively, which is NDJSON — so `rows` mode
    // is a copy rather than a re-encode, and the budget is applied line by line
    // as each line arrives off `bytes_stream()`. Nothing here holds the whole
    // body, so the budget bounds the read and not only the write.
    let want_rows = !matches!(mode, Mode::Exec);
    let body = if want_rows {
        format!("{} FORMAT JSONEachRow", statement.trim().trim_end_matches(';'))
    } else {
        statement.to_string()
    };

    let mut req = client.post(dsn).body(body);
    if let Ok(user) = std::env::var("SQL_USER") {
        req = req.header("X-ClickHouse-User", user);
    }
    if let Ok(pw) = std::env::var(PASSWORD_VAR) {
        req = req.header("X-ClickHouse-Key", pw);
    }
    let resp = req.send().await.context("sending the statement")?;
    let status = resp.status();
    if !status.is_success() {
        // A failing store answers with a message rather than a result set — but
        // nothing forces that, and the whole point of streaming the success path
        // is not to take the store's word for how much it is about to send. So
        // the snippet is read off the stream and cut off as it arrives.
        bail!("the store returned {status}: {}", error_snippet(resp).await);
    }

    match mode {
        Mode::Script => unreachable!("refused before any request is sent: see `main`"),
        Mode::Exec => {
            // Not read at all. `exec` keeps nothing, and a successful INSERT or
            // DDL body is not part of this step's contract — so dropping the
            // response here is the difference between a bounded read and one
            // that depends on what the store felt like returning.
            drop(resp);
            tracing::info!("statement executed");
            Ok(())
        }
        Mode::Scalar => {
            let first = scalar_row(resp.bytes_stream(), &mut budget).await?;
            let v: serde_json::Value = serde_json::from_str(&first)
                .context("the store did not answer with JSONEachRow")?;
            let obj = v.as_object().context("expected one JSON object per row")?;
            if obj.len() != 1 {
                bail!(
                    "SQL_MODE=scalar needs exactly one column, got {}. A gate compares one \
                     value; use SQL_MODE=rows for anything wider.",
                    obj.len()
                );
            }
            let raw = obj.values().next().map(render).unwrap_or_default();
            println!("{}", scalar_for_stdout(&raw)?);
            Ok(())
        }
        Mode::Rows => {
            let path = out_path.expect("rows mode resolved an artifact path");
            // Each line is admitted and written as it arrives, so memory holds
            // one row rather than the result set — and the write still lands in
            // a temp file that is renamed into place only if every row was
            // admitted, so an over-budget result leaves nothing at `path`.
            let mut sink = RowSink::create(path)?;
            for_each_line(resp.bytes_stream(), &mut budget, |line| {
                sink.write_row(line)?;
                Ok(Flow::Continue)
            })
            .await?;
            sink.commit()?;
            tracing::info!(rows = budget.rows_written(), path = %sink.path.display(), "wrote NDJSON");
            Ok(())
        }
    }
}

/// The most of a failed response's body this step will read before giving up on
/// it. The status is the news; the body is context, and context is worth a
/// bounded read, not an unbounded one.
const MAX_ERROR_SNIPPET_BYTES: usize = 2048;

/// A bounded snippet of a failed response's body.
///
/// `resp.text()` here would buffer whatever the store sent on its way to
/// reporting a failure, which is the same unbounded read the success path no
/// longer does.
async fn error_snippet(resp: reqwest::Response) -> String {
    use futures_util::stream::StreamExt;

    let mut buf: Vec<u8> = Vec::new();
    let mut stream = resp.bytes_stream();
    while buf.len() < MAX_ERROR_SNIPPET_BYTES {
        match stream.next().await {
            Some(Ok(chunk)) => {
                // Truncate the chunk rather than the accumulated buffer: chunk
                // sizes are the transport's business, and appending a whole one
                // before cutting it back would make the bound depend on them.
                let room = MAX_ERROR_SNIPPET_BYTES - buf.len();
                let bytes = chunk.as_ref();
                buf.extend_from_slice(&bytes[..room.min(bytes.len())]);
            }
            // A body that will not read does not change what the status says,
            // and failing here would replace the store's error with ours.
            Some(Err(_)) | None => break,
        }
    }
    // A cut mid-character is why this is lossy rather than a `from_utf8`.
    String::from_utf8_lossy(&buf).into_owned()
}

/// Refusal shared by both transports when `scalar` is handed more than one row.
///
/// Printing the first of several rows is worse than failing: the value looks
/// like an answer, a downstream `when:` gates on it, and which row won depends
/// on an ordering the statement never declared.
const SCALAR_MULTIROW: &str = "SQL_MODE=scalar needs exactly one row, and the statement returned \
     more than one. A gate that reads the first of several rows decides on whichever row the \
     store happened to send first — add a LIMIT 1 or an aggregate, or use SQL_MODE=rows.";


/// Read the one row `scalar` is allowed, and prove it was the only one.
///
/// Reads at most **two** lines: the answer, and one more to establish there was
/// no second row. Stopping at the first is what bounds memory, and it was also
/// what let a two-row result print row one as if it were the answer. One extra
/// line settles the cardinality, and `Flow::Stop` drops the stream there — so
/// the read stays bounded at two rows rather than the result set.
async fn scalar_row<S, B, E>(stream: S, budget: &mut Budget) -> Result<String>
where
    S: futures_util::stream::Stream<Item = std::result::Result<B, E>>,
    B: AsRef<[u8]>,
    E: std::error::Error + Send + Sync + 'static,
{
    let mut first: Option<String> = None;
    let mut extra = false;
    for_each_line(stream, budget, |line| {
        if first.is_none() {
            first = Some(line.to_string());
            Ok(Flow::Continue)
        } else {
            extra = true;
            Ok(Flow::Stop)
        }
    })
    .await?;
    if extra {
        bail!("{SCALAR_MULTIROW}");
    }
    first.context("the statement returned no rows")
}

/// Whether the line consumer wants the rest of the stream.
enum Flow {
    Continue,
    /// Stop reading and drop the stream. `scalar` wants the first line and
    /// nothing after it.
    Stop,
}

/// Pull NDJSON lines off a byte stream, admitting each against the budget as it
/// completes.
///
/// This is the read-side half of the output contract. `on_line` is called with
/// one complete line at a time and never sees a line the budget refused, and the
/// first refusal returns — dropping the stream — so an over-budget result stops
/// arriving rather than being read in full and rejected afterwards. Resident
/// memory is one line plus one chunk.
async fn for_each_line<S, B, E, F>(stream: S, budget: &mut Budget, mut on_line: F) -> Result<()>
where
    S: futures_util::stream::Stream<Item = std::result::Result<B, E>>,
    B: AsRef<[u8]>,
    E: std::error::Error + Send + Sync + 'static,
    F: FnMut(&str) -> Result<Flow>,
{
    use futures_util::stream::StreamExt;

    let mut stream = std::pin::pin!(stream);
    let mut pending: Vec<u8> = Vec::new();
    // Bytes of `pending` already handed to `on_line`, and bytes already searched
    // for a newline. Tracking the second is what keeps a line split across N
    // chunks from being rescanned from the start N times.
    let mut start = 0usize;
    let mut scanned = 0usize;

    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("reading the result stream")?;
        pending.extend_from_slice(chunk.as_ref());

        while let Some(offset) = pending[scanned..].iter().position(|&b| b == b'\n') {
            let end = scanned + offset;
            let stop = {
                let line = std::str::from_utf8(&pending[start..end])
                    .context("the store's result is not valid UTF-8")?;
                emit(line, budget, &mut on_line)?
            };
            start = end + 1;
            scanned = start;
            if stop {
                return Ok(());
            }
        }
        // One compaction per chunk rather than one per line: dropping the
        // consumed prefix shifts at most what is left of this chunk.
        pending.drain(..start);
        start = 0;
        scanned = pending.len();
        // What is left is a row still arriving. Without this, a store answering
        // with one enormous line would buffer all of it — no line completes, so
        // `admit` never runs.
        budget.check_pending(pending.len())?;
    }

    // A body whose last line carries no trailing newline still carries a row.
    // `str::lines()` yielded it, and dropping it here would silently lose one.
    if !pending.is_empty() {
        let line = std::str::from_utf8(&pending).context("the store's result is not valid UTF-8")?;
        emit(line, budget, &mut on_line)?;
    }
    Ok(())
}

/// Admit one complete line and hand it on. Blank lines are skipped rather than
/// charged: a trailing newline at the end of a result set is not a row.
fn emit(
    line: &str,
    budget: &mut Budget,
    on_line: &mut impl FnMut(&str) -> Result<Flow>,
) -> Result<bool> {
    if line.trim().is_empty() {
        return Ok(false);
    }
    // `+ 1` for the newline the line will carry wherever it lands.
    budget.admit(line.len() + 1)?;
    Ok(matches!(on_line(line)?, Flow::Stop))
}

/// An artifact written a row at a time, and moved into place only once the whole
/// result has been admitted.
///
/// Rows go to a sibling temp file first. That holds two things true at once that
/// otherwise pull against each other: memory carries one row rather than the
/// result set, *and* an over-budget statement still leaves **nothing** at the
/// artifact path — a refusal drops the temp file rather than leaving a partial
/// artifact to be cleaned up, which is exactly what the budget exists to avoid.
struct RowSink {
    path: std::path::PathBuf,
    tmp: std::path::PathBuf,
    file: std::io::BufWriter<std::fs::File>,
    committed: bool,
}

impl RowSink {
    fn create(path: std::path::PathBuf) -> Result<Self> {
        // A dotfile sibling: the same directory, so the rename is within one
        // filesystem and therefore atomic, and hidden so a collector walking the
        // artifact directory does not pick up a result still being written.
        let name = path
            .file_name()
            .map(|n| n.to_string_lossy().into_owned())
            .unwrap_or_else(|| "result.ndjson".into());
        let tmp = path.with_file_name(format!(".{name}.partial"));
        let file =
            std::fs::File::create(&tmp).with_context(|| format!("creating {}", tmp.display()))?;
        Ok(RowSink { path, tmp, file: std::io::BufWriter::new(file), committed: false })
    }

    /// Write one NDJSON line, adding the newline if the caller's line does not
    /// carry one — the HTTP path copies lines without their terminator, the wire
    /// path re-encodes them with it.
    fn write_row(&mut self, line: &str) -> Result<()> {
        use std::io::Write;
        let write = |f: &mut std::io::BufWriter<std::fs::File>| -> std::io::Result<()> {
            f.write_all(line.as_bytes())?;
            if !line.ends_with('\n') {
                f.write_all(b"\n")?;
            }
            Ok(())
        };
        write(&mut self.file).with_context(|| format!("writing {}", self.tmp.display()))
    }

    fn commit(&mut self) -> Result<()> {
        use std::io::Write;
        self.file.flush().with_context(|| format!("flushing {}", self.tmp.display()))?;
        std::fs::rename(&self.tmp, &self.path)
            .with_context(|| format!("renaming {} into place", self.tmp.display()))?;
        self.committed = true;
        Ok(())
    }
}

impl Drop for RowSink {
    fn drop(&mut self) {
        if !self.committed {
            // Every error path lands here, a budget refusal included. Leaving the
            // partial file behind would put an over-budget result in the artifact
            // directory under another name.
            let _ = std::fs::remove_file(&self.tmp);
        }
    }
}

/// A JSON value as the text a gate compares or an NDJSON field holds.
fn render(v: &serde_json::Value) -> String {
    match v {
        serde_json::Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}

/// SQL over a wire protocol — the MySQL and Postgres families, via sqlx.
///
/// Rows are **streamed**: `fetch` yields them as the driver decodes them, so the
/// budget sees each row as it arrives. `fetch_all` had the entire result set in
/// memory before the first `admit` ran, which made the budget a bound on the
/// write only.
#[cfg(any(feature = "mysql", feature = "postgres"))]
async fn run_wire(
    engine: Engine,
    dsn: &str,
    statement: &str,
    mode: Mode,
    out_path: Option<std::path::PathBuf>,
    mut budget: Budget,
) -> Result<()> {
    use futures_util::stream::TryStreamExt;
    use sqlx::Row;

    // The password is applied to the DSN here rather than being carried in it,
    // which is the whole point of PASSWORD_VAR: the value is masked by name
    // everywhere it might be printed, and the DSN never holds it.
    let dsn = match std::env::var(PASSWORD_VAR).ok().filter(|s| !s.is_empty()) {
        Some(pw) => splice_password(dsn, &pw)?,
        None => dsn.to_string(),
    };

    // One connection, one statement, no pool: a step process runs exactly one
    // query and then exits. The connection outlives the row stream borrowing it,
    // which is why it is bound here rather than inside a block.
    sqlx::any::install_default_drivers();
    let mut conn = <sqlx::AnyConnection as sqlx::Connection>::connect(&dsn)
        .await
        .with_context(|| format!("connecting to {engine:?}"))?;

    if mode == Mode::Exec {
        let done =
            sqlx::query(statement).execute(&mut conn).await.context("executing the statement")?;
        tracing::info!(rows_affected = done.rows_affected(), "statement executed");
        return Ok(());
    }
    if mode == Mode::Script {
        // `raw_sql`, not `query`: no arguments and no preparation, so the driver
        // uses the simple-query protocol, which carries several statements. The
        // first failure stops the script and — inside the script's own
        // BEGIN/COMMIT, or Postgres's implicit transaction — rolls it back.
        let done = sqlx::raw_sql(statement).execute(&mut conn).await.context("executing the script")?;
        tracing::info!(rows_affected = done.rows_affected(), "script executed");
        return Ok(());
    }

    // `fetch`, not `fetch_all`: rows arrive one at a time, so an over-budget
    // result is refused with the rest of it still on the wire.
    let mut rows = sqlx::query(statement).fetch(&mut conn);

    match mode {
        Mode::Exec | Mode::Script => unreachable!("handled above"),
        Mode::Scalar => {
            // Two rows at most, then the stream is dropped — the driver never
            // decodes the rest of a result the gate was never going to look at,
            // and one extra row is what proves there was exactly one.
            let row = rows
                .try_next()
                .await
                .context("running the statement")?
                .context("the statement returned no rows")?;
            if row.columns().len() != 1 {
                bail!("SQL_MODE=scalar needs exactly one column, got {}", row.columns().len());
            }
            let obj = to_obj(&row);
            // Charged to the budget like any other row. The HTTP path admits its
            // scalar line through `for_each_line`; without this the two
            // transports would disagree about whether a scalar costs anything.
            budget.admit(ndjson_line(&obj).len())?;
            let raw = obj.values().next().map(render).unwrap_or_default();
            // Checked BEFORE printing: a value already on stdout is already in
            // the task's `output` column and already gating something.
            if rows.try_next().await.context("running the statement")?.is_some() {
                bail!("{SCALAR_MULTIROW}");
            }
            println!("{}", scalar_for_stdout(&raw)?);
            Ok(())
        }
        Mode::Rows => {
            let path = out_path.expect("rows mode resolved an artifact path");
            // Same shape as the HTTP path: admit, write, and only rename into
            // place once the whole result has been admitted.
            let mut sink = RowSink::create(path)?;
            while let Some(row) = rows.try_next().await.context("running the statement")? {
                let line = ndjson_line(&to_obj(&row));
                budget.admit(line.len())?;
                sink.write_row(&line)?;
            }
            sink.commit()?;
            tracing::info!(rows = budget.rows_written(), path = %sink.path.display(), "wrote NDJSON");
            Ok(())
        }
    }
}

/// One result row as a JSON object.
#[cfg(any(feature = "mysql", feature = "postgres"))]
fn to_obj(row: &sqlx::any::AnyRow) -> serde_json::Map<String, serde_json::Value> {
    use sqlx::{Column, Row};

    let mut o = serde_json::Map::new();
    for (i, col) in row.columns().iter().enumerate() {
        // Text first, then the numeric widths: a store's type names vary,
        // and a value that will not decode is better recorded as null than
        // used to fail a whole result set.
        let v = row
            .try_get::<Option<String>, _>(i)
            .map(|s| s.map(serde_json::Value::String))
            .or_else(|_| row.try_get::<Option<i64>, _>(i).map(|n| n.map(Into::into)))
            .or_else(|_| {
                row.try_get::<Option<f64>, _>(i).map(|n| {
                    n.and_then(serde_json::Number::from_f64).map(serde_json::Value::Number)
                })
            })
            .or_else(|_| row.try_get::<Option<bool>, _>(i).map(|b| b.map(Into::into)))
            .unwrap_or(None)
            .unwrap_or(serde_json::Value::Null);
        o.insert(col.name().to_string(), v);
    }
    o
}

/// Put the password into the DSN's userinfo just before connecting.
#[cfg(any(feature = "mysql", feature = "postgres"))]
fn splice_password(dsn: &str, pw: &str) -> Result<String> {
    let (scheme, rest) = dsn
        .split_once("://")
        .context("SQL_DSN must look like scheme://[user@]host[:port]/db")?;
    let (userinfo, host) = match rest.split_once('@') {
        Some((u, h)) => (u.to_string(), h.to_string()),
        None => bail!(
            "SQL_DSN names no user, so there is nowhere to apply {PASSWORD_VAR}. Write \
             scheme://user@host/db and keep the password in {PASSWORD_VAR}."
        ),
    };
    // Percent-encode the delimiters that would otherwise re-parse the DSN.
    //
    // `%` is first and that ordering is the whole point: it is not a delimiter,
    // it is the *escape*. Leaving it alone splices a password like `pa%73s` in
    // verbatim, the DSN parser decodes `%73` back to `s`, and sqlx authenticates
    // as `pass` — an auth failure with nothing in the message to explain it.
    // Encoding it first means every later substitution emits a `%` that is
    // already known to be ours.
    let enc: String = pw
        .chars()
        .map(|c| match c {
            '%' => "%25".into(),
            '@' => "%40".into(),
            ':' => "%3A".into(),
            '/' => "%2F".into(),
            '?' => "%3F".into(),
            '#' => "%23".into(),
            other => other.to_string(),
        })
        .collect();
    Ok(format!("{scheme}://{userinfo}:{enc}@{host}"))
}

#[cfg(not(any(feature = "mysql", feature = "postgres")))]
async fn run_wire(
    engine: Engine,
    _dsn: &str,
    _statement: &str,
    _mode: Mode,
    _out_path: Option<std::path::PathBuf>,
    _budget: Budget,
) -> Result<()> {
    bail!(
        "SQL_ENGINE={engine:?} speaks a wire protocol this binary was not built with. Rebuild \
         with `--features mysql` (StarRocks, Doris, MySQL) or `--features postgres` (Postgres, \
         Redshift, Materialize, CockroachDB), or use the image built with them. The default \
         build carries only the HTTP transport, because a wire driver is a large dependency for \
         a task image that may never speak one — ClickHouse needs none."
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A byte stream that hands out the chunks given, in order — standing in for
    /// `bytes_stream()`, whose chunk boundaries have nothing to do with where the
    /// rows are.
    fn chunks(
        parts: &[&str],
    ) -> impl futures_util::stream::Stream<Item = std::result::Result<Vec<u8>, std::io::Error>>
    {
        let owned: Vec<std::result::Result<Vec<u8>, std::io::Error>> =
            parts.iter().map(|p| Ok(p.as_bytes().to_vec())).collect();
        futures_util::stream::iter(owned)
    }

    /// Every line the reader admitted, for the tests that only care about that.
    async fn collect(parts: &[&str], budget: &mut Budget) -> Result<Vec<String>> {
        let mut got = Vec::new();
        for_each_line(chunks(parts), budget, |line| {
            got.push(line.to_string());
            Ok(Flow::Continue)
        })
        .await?;
        Ok(got)
    }

    /// The property the whole read-side rewrite rests on: a row is a row wherever
    /// the transport happened to cut the chunk. A naive "parse each chunk" split
    /// `{"n":1}` into two rows here and lost both.
    #[tokio::test]
    async fn a_row_split_across_chunks_is_one_row() {
        let mut b = Budget::new(DEFAULT_MAX_ROWS, DEFAULT_MAX_BYTES);
        let got = collect(&[r#"{"n":1}"#, "\n{\"n\":", "2}\n"], &mut b).await.unwrap();
        assert_eq!(got, vec![r#"{"n":1}"#.to_string(), r#"{"n":2}"#.to_string()]);
        assert_eq!(b.rows_written(), 2);
    }

    /// `str::lines()` yielded a final unterminated line, and the streaming reader
    /// has to as well — a store that omits the trailing newline must not lose its
    /// last row.
    #[tokio::test]
    async fn a_final_line_without_a_newline_is_still_a_row() {
        let mut b = Budget::new(DEFAULT_MAX_ROWS, DEFAULT_MAX_BYTES);
        let got = collect(&["{\"n\":1}\n{\"n\":2}"], &mut b).await.unwrap();
        assert_eq!(got.len(), 2, "{got:?}");
        assert_eq!(b.rows_written(), 2);
    }

    /// Blank lines are not rows, so they are not charged to the budget either —
    /// the previous code filtered them before `admit` and that has to survive.
    #[tokio::test]
    async fn blank_lines_are_skipped_rather_than_charged() {
        let mut b = Budget::new(2, DEFAULT_MAX_BYTES);
        let got = collect(&["\n\n{\"n\":1}\n\n\n{\"n\":2}\n\n"], &mut b).await.unwrap();
        assert_eq!(got.len(), 2, "{got:?}");
        assert_eq!(b.rows_written(), 2, "the blank lines did not spend the 2-row budget");
    }

    /// The reason for streaming at all: the refusal fires on the row that breaks
    /// the budget, not after the result set has been read.
    #[tokio::test]
    async fn the_budget_stops_the_read_at_the_row_that_breaks_it() {
        let mut b = Budget::new(2, DEFAULT_MAX_BYTES);
        let stream = chunks(&["{\"n\":1}\n{\"n\":2}\n{\"n\":3}\n{\"n\":4}\n"]);
        let mut got = Vec::new();
        let e = for_each_line(stream, &mut b, |line| {
            got.push(line.to_string());
            Ok(Flow::Continue)
        })
        .await
        .unwrap_err()
        .to_string();
        assert!(e.contains("SQL_MAX_ROWS (2)"), "{e}");
        assert_eq!(got.len(), 2, "the refused row never reached the writer: {got:?}");
    }

    /// The gap `admit` alone leaves. A store answering with one enormous row
    /// completes no line, so without `check_pending` this buffers the whole thing
    /// and the budget never runs — which is the OOM the streaming rewrite exists
    /// to remove.
    #[tokio::test]
    async fn one_enormous_row_is_refused_while_it_is_still_arriving() {
        let mut b = Budget::new(DEFAULT_MAX_ROWS, 64);
        let huge = "x".repeat(50);
        let stream = chunks(&[&huge, &huge, &huge]);
        let e = for_each_line(stream, &mut b, |_| Ok(Flow::Continue))
            .await
            .unwrap_err()
            .to_string();
        assert!(e.contains("SQL_MAX_BYTES (64)"), "{e}");
        assert!(e.contains("still arriving"), "names the mid-row refusal: {e}");
        assert_eq!(b.rows_written(), 0, "nothing completed, so nothing was admitted");
    }

    /// `scalar` reads one line and stops. The rest of the body is bytes nobody
    /// will look at, and pulling it would undo the bound on a `SELECT *` that
    /// someone ran in the wrong mode.
    #[tokio::test]
    async fn stop_ends_the_read_after_the_line_that_asked_for_it() {
        let mut b = Budget::new(DEFAULT_MAX_ROWS, DEFAULT_MAX_BYTES);
        let stream = chunks(&["{\"n\":1}\n", "{\"n\":2}\n{\"n\":3}\n"]);
        let mut got = Vec::new();
        for_each_line(stream, &mut b, |line| {
            got.push(line.to_string());
            Ok(Flow::Stop)
        })
        .await
        .unwrap();
        assert_eq!(got, vec![r#"{"n":1}"#.to_string()]);
        assert_eq!(b.rows_written(), 1, "the rest of the stream was never admitted");
    }

    /// The gate contract: one row means one row. Reading the first of several
    /// prints a value that looks like an answer and gates a downstream `when:`
    /// on whichever row the store happened to send first.
    #[tokio::test]
    async fn scalar_refuses_a_result_with_more_than_one_row() {
        let mut b = Budget::new(DEFAULT_MAX_ROWS, DEFAULT_MAX_BYTES);
        let err = scalar_row(chunks(&["{\"n\":1}\n{\"n\":2}\n"]), &mut b).await.unwrap_err();
        assert!(err.to_string().contains("exactly one row"), "{err}");
    }

    #[tokio::test]
    async fn scalar_accepts_exactly_one_row() {
        let mut b = Budget::new(DEFAULT_MAX_ROWS, DEFAULT_MAX_BYTES);
        let got = scalar_row(chunks(&["{\"n\":1}\n"]), &mut b).await.unwrap();
        assert_eq!(got, r#"{"n":1}"#);
    }

    /// A trailing newline is not a second row — `emit` skips blanks, so the
    /// cardinality check must not be tripped by formatting.
    #[tokio::test]
    async fn a_trailing_blank_line_is_not_a_second_row() {
        let mut b = Budget::new(DEFAULT_MAX_ROWS, DEFAULT_MAX_BYTES);
        let got = scalar_row(chunks(&["{\"n\":1}\n\n\n"]), &mut b).await.unwrap();
        assert_eq!(got, r#"{"n":1}"#);
    }

    #[tokio::test]
    async fn scalar_says_so_when_there_were_no_rows_at_all() {
        let mut b = Budget::new(DEFAULT_MAX_ROWS, DEFAULT_MAX_BYTES);
        let err = scalar_row(chunks(&["\n"]), &mut b).await.unwrap_err();
        assert!(err.to_string().contains("no rows"), "{err}");
    }

    /// The bound survives the cardinality check: a huge result is still read
    /// two rows deep, not in full.
    #[tokio::test]
    async fn the_cardinality_check_still_reads_only_two_rows() {
        let mut b = Budget::new(DEFAULT_MAX_ROWS, DEFAULT_MAX_BYTES);
        let body = "{\"n\":1}\n".repeat(10_000);
        let _ = scalar_row(chunks(&[&body]), &mut b).await.unwrap_err();
        assert_eq!(b.rows_written(), 2, "two admitted, not ten thousand");
    }

    /// The write-side guarantee has to survive the write becoming incremental:
    /// rows land in a temp file, and a refusal leaves the artifact path empty
    /// rather than holding a truncated result.
    #[test]
    fn an_abandoned_sink_leaves_nothing_at_the_artifact_path() {
        let dir = std::env::temp_dir().join(format!("dagron-step-sql-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("result.ndjson");

        {
            let mut sink = RowSink::create(path.clone()).unwrap();
            sink.write_row("{\"n\":1}").unwrap();
            // Dropped without `commit` — the budget refusal path.
        }
        assert!(!path.exists(), "no artifact");
        assert!(!dir.join(".result.ndjson.partial").exists(), "and no partial left behind");

        {
            let mut sink = RowSink::create(path.clone()).unwrap();
            sink.write_row("{\"n\":1}").unwrap();
            sink.write_row("{\"n\":2}\n").unwrap();
            sink.commit().unwrap();
        }
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "{\"n\":1}\n{\"n\":2}\n",
            "the newline is added when the caller's line does not carry one, and not doubled \
             when it does"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
