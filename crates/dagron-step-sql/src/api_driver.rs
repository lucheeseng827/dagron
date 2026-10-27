//! The statement-API driver: submit, poll until the statement settles, page the
//! result. A deadline covers all of it, and the statement is cancelled when the
//! deadline passes.
//!
//! [`dagron_step_sql::statement_api`] decides what each request is and what each
//! answer means. This half sends them. It holds the token, retries a read that
//! failed in transit, bounds every body it reads, and owns the deadline.

use std::collections::VecDeque;
use std::time::{Duration, Instant};

use anyhow::{anyhow, bail, Context, Result};
use dagron_step_sql::statement_api::{
    snowflake_token_type, Answer, Api, Ask, Call, Cursor, Field, Handle, Method, Page, Reply,
    Target,
};
use dagron_step_sql::{ndjson_line, scalar_for_stdout, Budget, Mode, Settings};

use crate::{render, RowSink, SCALAR_MULTIROW};

/// The longest one HTTP call may take. This is not the statement's deadline,
/// which is `SQL_TIMEOUT_SECS` over every call; it is the bound on any one of
/// them, so the deadline is looked at least this often. Each call is asked to
/// answer well inside it: Snowflake at once (`async=true`), BigQuery and
/// Databricks within 5 s.
const CALL_TIMEOUT: Duration = Duration::from_secs(30);
/// A cancel is sent once the deadline has already passed, so it gets less time.
const CANCEL_TIMEOUT: Duration = Duration::from_secs(10);
/// The wait before the first poll, doubled after each one up to [`POLL_MAX`].
const POLL_FIRST: Duration = Duration::from_millis(250);
const POLL_MAX: Duration = Duration::from_secs(5);
/// Attempts at a read (a poll, a page) before its failure is the step's.
const READ_ATTEMPTS: u32 = 3;
/// Room for a page's envelope (metadata, schema, handles) on top of its rows.
const PAGE_ENVELOPE_BYTES: u64 = 1024 * 1024;

pub async fn run(
    api: Api,
    settings: &Settings,
    statement: &str,
    mode: Mode,
    sink: Option<&mut RowSink>,
    mut budget: Budget,
) -> Result<()> {
    let target = Target::parse(api, &settings.dsn).with_context(|| format!("reading {}", settings.var("DSN")))?;
    let timeout_secs = settings.timeout_secs;
    let client = reqwest::Client::builder()
        .timeout(CALL_TIMEOUT)
        .user_agent(concat!("dagron-step-sql/", env!("CARGO_PKG_VERSION")))
        // Every request is built from the DSN's host, and a redirect is a URL the
        // store handed back. Followed, it would take the request (and, to the same
        // host, the token) somewhere the DSN did not name, and would turn a POST
        // into a GET on a 302. Unfollowed, it is an HTTP 3xx error naming itself.
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("building the HTTP client")?;
    let ask = Ask::new(mode, budget.max_rows, timeout_secs);
    let mut session = Session {
        auth: Auth::from_settings(api, settings)?,
        client,
        target,
        ceiling: page_ceiling(&budget),
        deadline: Instant::now() + Duration::from_secs(timeout_secs),
        timeout_secs,
    };

    let answer = session.settle(statement, &ask).await?;
    let id = answer.handle.as_ref().map(|h| h.id.clone()).unwrap_or_default();
    match mode {
        Mode::Exec | Mode::Script => {
            tracing::info!(statement = %id, "statement executed");
            Ok(())
        }
        Mode::Scalar => {
            if answer.fields.len() > 1 {
                bail!(
                    "SQL_MODE=scalar needs exactly one column, got {}. A gate compares one value; \
                     use SQL_MODE=rows for anything wider.",
                    answer.fields.len()
                );
            }
            let mut rows = Rows::new(&mut session, &ask, answer);
            let first = rows.next().await?.context("the statement returned no rows")?;
            // Charged like any other row, as on the other transports.
            budget.admit(ndjson_line(&first).len())?;
            let raw = first.values().next().map(render).unwrap_or_default();
            // Checked BEFORE printing: a value on stdout is already in the task's
            // `output` column and already gating something.
            if rows.next().await?.is_some() {
                bail!("{SCALAR_MULTIROW}");
            }
            println!("{}", scalar_for_stdout(&raw)?);
            Ok(())
        }
        Mode::Rows => {
            // Admit, then write — the same sink, and so the same promise, as the
            // other transports: the caller renames it into place only once the
            // whole result has been admitted, so a refusal leaves nothing.
            let sink = sink.expect("rows mode passes a sink");
            let mut rows = Rows::new(&mut session, &ask, answer);
            while let Some(row) = rows.next().await? {
                let line = ndjson_line(&row);
                budget.admit(line.len())?;
                sink.write_row(&line)?;
            }
            tracing::info!(statement = %id, rows = budget.rows_written(), "read the result");
            Ok(())
        }
    }
}

/// The most of one answer read into memory.
///
/// A page is a JSON document, not a stream of lines, so it is read whole, and
/// resident memory is one page where the other transports hold one row. This is
/// the bound on it, and it is derived from the budget so that it never refuses a
/// page the budget would have admitted. Snowflake's and Databricks' arrays of
/// cells are smaller than the same rows as NDJSON, and BigQuery's `f`/`v` cells
/// of a flat row are under twice their size. So twice `SQL_MAX_BYTES`, plus room
/// for the envelope, holds any page the budget could admit. Deeply nested BigQuery
/// records can exceed that ratio, and then the refusal fires early, which is the
/// safe direction to be wrong in.
fn page_ceiling(budget: &Budget) -> u64 {
    budget.max_bytes.saturating_mul(2).saturating_add(PAGE_ENVELOPE_BYTES)
}

struct Session {
    client: reqwest::Client,
    target: Target,
    auth: Auth,
    ceiling: u64,
    deadline: Instant,
    timeout_secs: u64,
}

impl Session {
    /// Submit the statement and poll it until it settles.
    ///
    /// The deadline is checked between calls. A statement still running when it
    /// passes is **cancelled**, and so is one whose polls stop getting answers. The
    /// handle is the only thing that can stop it, and once this process exits
    /// nothing holds that handle.
    async fn settle(&mut self, statement: &str, ask: &Ask) -> Result<Answer> {
        let api = self.target.api;
        let submit = self.target.submit(statement, ask);
        // Not retried. A submit whose answer was lost may still have started the
        // statement, and sending it again could run it twice.
        let resp = self.send(&submit, CALL_TIMEOUT).await.with_context(|| {
            format!(
                "sending the statement to {api:?}. It was not resent: if the request got there \
                 before the connection failed, the statement may be running, and a second \
                 submit could run it twice. Check the warehouse's query history."
            )
        })?;
        let mut reply = self.reply(resp, ask.mode).await?;
        let mut wait = POLL_FIRST;
        let mut last: Option<Handle> = None;
        loop {
            let handle = match reply {
                Reply::Done(mut answer) => {
                    // Snowflake settles `exec` by status alone, without the body that
                    // names the statement; the handle it was polled by still does.
                    if answer.handle.is_none() {
                        answer.handle = last;
                    }
                    return Ok(answer);
                }
                Reply::Running(handle) => handle,
            };
            if last.is_none() {
                tracing::info!(statement = %handle.id, "{api:?} is running the statement");
            }
            let now = Instant::now();
            if now >= self.deadline {
                self.cancel(&handle).await;
                bail!(
                    "the statement was still running at SQL_TIMEOUT_SECS ({}s), so it was \
                     cancelled: {api:?} statement {}. Raise SQL_TIMEOUT_SECS if it is meant to \
                     run this long.",
                    self.timeout_secs,
                    handle.id
                );
            }
            // The last wait is cut short at the deadline, so the statement gets one
            // more look before it is cancelled rather than being cancelled unseen.
            tokio::time::sleep(wait.min(self.deadline - now)).await;
            wait = (wait * 2).min(POLL_MAX);
            let poll = self.target.poll(&handle, ask);
            reply = match self.poll(&poll, ask.mode).await {
                // A failure the answer reports, the statement's own included, has
                // settled it: there is nothing left to cancel.
                Polled::Answered(reply) => reply?,
                Polled::Unanswered(e) => {
                    self.cancel(&handle).await;
                    return Err(e.context(format!(
                        "polling {api:?} statement {}, which was then cancelled",
                        handle.id
                    )));
                }
            };
            last = Some(handle);
        }
    }

    /// Read an answer to a submit or a poll.
    async fn reply(&self, resp: reqwest::Response, mode: Mode) -> Result<Reply> {
        match self.body(resp, mode).await? {
            Some((status, body)) => self.target.reply(status, &body),
            None => Ok(settled_by_status()),
        }
    }

    /// An answer's status and its body, read whole; `None` when the status alone
    /// settles it, and then the body is not read at all: see `settled_by_status`.
    async fn body(&self, resp: reqwest::Response, mode: Mode) -> Result<Option<(u16, Vec<u8>)>> {
        let status = resp.status().as_u16();
        if self.target.settled_by_status(status, mode) {
            drop(resp);
            return Ok(None);
        }
        Ok(Some((status, read_capped(resp, self.ceiling).await?)))
    }

    /// Ask after a running statement. Only an answer read whole, with a status
    /// that does not mean "try again", says anything about it. A poll still
    /// transient after its retries, or whose body was lost on the way, does not,
    /// and the statement may still be running.
    async fn poll(&mut self, call: &Call, mode: Mode) -> Polled {
        let resp = match self.read(call).await {
            Ok(resp) => resp,
            Err(e) => return Polled::Unanswered(e),
        };
        let (status, body) = match self.body(resp, mode).await {
            Ok(Some(answer)) => answer,
            Ok(None) => return Polled::Answered(Ok(settled_by_status())),
            Err(e) => return Polled::Unanswered(e),
        };
        let reply = self.target.reply(status, &body);
        if !transient(status) {
            return Polled::Answered(reply);
        }
        // The vendor's own words, where it gave any, say why.
        Polled::Unanswered(match reply {
            Err(e) => e,
            Ok(_) => anyhow!("{:?} answered the poll with HTTP {status}", self.target.api),
        })
    }

    /// Send a read, retrying what failed in transit: a connection that did not
    /// complete, a `429`, or a `5xx` that means "try again". Only reads come here.
    /// A poll asked twice is a poll, where a submit sent twice is two statements.
    ///
    /// An answer that is still transient after the last attempt is returned, not
    /// turned into an error here, so the vendor's own message reaches the operator.
    async fn read(&mut self, call: &Call) -> Result<reqwest::Response> {
        debug_assert!(call.idempotent(), "only reads are retried");
        let mut backoff = Duration::from_secs(1);
        let mut attempt = 1;
        loop {
            let outcome = self.send(call, CALL_TIMEOUT).await;
            let again = match &outcome {
                Ok(resp) => transient(resp.status().as_u16()),
                Err(_) => true,
            };
            if !again || attempt >= READ_ATTEMPTS || Instant::now() + backoff >= self.deadline {
                return outcome;
            }
            tracing::warn!(attempt, path = call.url.path(), "a read failed in transit; retrying");
            tokio::time::sleep(backoff).await;
            backoff *= 2;
            attempt += 1;
        }
    }

    async fn send(&mut self, call: &Call, timeout: Duration) -> Result<reqwest::Response> {
        let token = self.auth.bearer(&self.client).await?;
        let mut req = match call.method {
            Method::Get => self.client.get(call.url.clone()),
            Method::Post => self.client.post(call.url.clone()),
        };
        req = req
            .timeout(timeout)
            .bearer_auth(token)
            .header(reqwest::header::ACCEPT, "application/json");
        if let Some(kind) = self.auth.snowflake_type {
            req = req.header("X-Snowflake-Authorization-Token-Type", kind);
        }
        if let Some(body) = &call.body {
            req = req.json(body);
        }
        // The path, not the URL: a BigQuery page token is not worth a log line.
        req.send().await.with_context(|| format!("{:?} {}", call.method, call.url.path()))
    }

    /// Cancel a statement, once and best-effort. It is called on the way to an
    /// error that is already the news, so its own failure is logged, not raised.
    async fn cancel(&mut self, handle: &Handle) {
        let call = self.target.cancel(handle);
        match self.send(&call, CANCEL_TIMEOUT).await {
            Ok(resp) if resp.status().is_success() => {
                tracing::warn!(statement = %handle.id, "cancelled the statement");
            }
            Ok(resp) => tracing::warn!(
                statement = %handle.id,
                status = resp.status().as_u16(),
                "the cancel was refused, so the statement may still be running"
            ),
            Err(e) => tracing::warn!(
                statement = %handle.id,
                error = %format!("{e:#}"),
                "the cancel did not get through, so the statement may still be running"
            ),
        }
    }
}

/// What a poll came back with.
enum Polled {
    /// An answer read whole, with a status that is not "try again". What it says
    /// about the statement is settled, a failure included.
    Answered(Result<Reply>),
    /// No such answer: the statement may still be running.
    Unanswered(anyhow::Error),
}

/// A status that means "try again", not "this is how the statement went".
fn transient(status: u16) -> bool {
    matches!(status, 429 | 500 | 502 | 503 | 504)
}

/// An answer settled by its status alone, which carries nothing to read.
fn settled_by_status() -> Reply {
    Reply::Done(Answer { handle: None, fields: Vec::new(), first: Page { rows: Vec::new(), next: None } })
}

/// Read a body, refusing it once it passes `cap`, while it is still arriving.
async fn read_capped(resp: reqwest::Response, cap: u64) -> Result<Vec<u8>> {
    use futures_util::stream::StreamExt;

    let mut buf: Vec<u8> = Vec::new();
    let mut stream = resp.bytes_stream();
    while let Some(chunk) = stream.next().await {
        let chunk = chunk.context("reading the answer")?;
        if (buf.len() + chunk.len()) as u64 > cap {
            bail!(
                "one page of the answer passed {cap} bytes while it was still arriving. A page is \
                 read whole, so it is bounded at twice SQL_MAX_BYTES plus 1 MiB for its envelope, \
                 and this one cannot be a page of rows the budget would admit. Raise \
                 SQL_MAX_BYTES if the result is meant to be this large, or ask for less of it."
            );
        }
        buf.extend_from_slice(&chunk);
    }
    Ok(buf)
}

/// A settled result's rows, one at a time, a page in memory at a time.
struct Rows<'a> {
    session: &'a mut Session,
    ask: &'a Ask,
    handle: Option<Handle>,
    fields: Vec<Field>,
    page: VecDeque<serde_json::Map<String, serde_json::Value>>,
    next: Option<Cursor>,
}

impl<'a> Rows<'a> {
    fn new(session: &'a mut Session, ask: &'a Ask, answer: Answer) -> Rows<'a> {
        Rows {
            session,
            ask,
            handle: answer.handle,
            fields: answer.fields,
            page: answer.first.rows.into(),
            next: answer.first.next,
        }
    }

    /// The next row, fetching the next page when this one is used up. A page is
    /// fetched only when a row is asked for, so a refusal part-way through a
    /// result leaves the rest of it unfetched.
    async fn next(&mut self) -> Result<Option<serde_json::Map<String, serde_json::Value>>> {
        loop {
            if let Some(row) = self.page.pop_front() {
                return Ok(Some(row));
            }
            let Some(cursor) = self.next.take() else { return Ok(None) };
            let handle = self
                .handle
                .as_ref()
                .context("the result has more pages, but the API named no statement to fetch them from")?;
            if Instant::now() >= self.session.deadline {
                bail!(
                    "SQL_TIMEOUT_SECS ({}s) passed while the result was still being read. The \
                     statement had finished; reading what it returned took the rest.",
                    self.session.timeout_secs
                );
            }
            let call = self.session.target.page(handle, &cursor, self.ask);
            let resp = self.session.read(&call).await.context("fetching the next page of the result")?;
            let status = resp.status().as_u16();
            let body = read_capped(resp, self.session.ceiling).await?;
            let page = self.session.target.read_page(&self.fields, &cursor, status, &body)?;
            self.page = page.rows.into();
            self.next = page.next;
        }
    }
}

/// Where the bearer token comes from.
struct Auth {
    source: TokenSource,
    /// Snowflake only: what kind of token it is.
    snowflake_type: Option<&'static str>,
}

enum TokenSource {
    /// `SQL_PASSWORD` (or a parity side's `…_PASSWORD`), masked by name like every
    /// other credential this step takes.
    Fixed(String),
    /// BigQuery with no `SQL_PASSWORD`: the GCE metadata server, which hands a
    /// workload its service account's token (GKE Workload Identity, Compute
    /// Engine, Cloud Run). It is the one way to reach BigQuery with no stored
    /// secret at all, and the token it hands out lives an hour, so it is fetched
    /// again before it runs out.
    Metadata { url: reqwest::Url, cached: Option<(String, Instant)>, password_var: String },
}

impl Auth {
    fn from_settings(api: Api, settings: &Settings) -> Result<Auth> {
        let snowflake_type = match api {
            Api::Snowflake => Some(
                snowflake_token_type(settings.token_type.as_deref())
                    .with_context(|| format!("reading {}", settings.var("TOKEN_TYPE")))?,
            ),
            Api::BigQuery | Api::Databricks => None,
        };
        // Trimmed: a token has no whitespace in it, and a secret written from a
        // file usually ends in a newline.
        let token = settings.password.as_ref().map(|t| t.trim().to_string()).filter(|t| !t.is_empty());
        let password_var = settings.var("PASSWORD");
        let source = match (token, api) {
            (Some(token), _) => TokenSource::Fixed(token),
            (None, Api::BigQuery) => TokenSource::Metadata { url: metadata_url()?, cached: None, password_var },
            (None, Api::Snowflake) => bail!(
                "Snowflake authenticates by token, and {password_var} is not set. Put a \
                 programmatic access token in it via `value_from: {{ secret: … }}` (or an OAuth \
                 token or key-pair JWT, naming which in {}).",
                settings.var("TOKEN_TYPE")
            ),
            (None, Api::Databricks) => bail!(
                "Databricks authenticates by token, and {password_var} is not set. Put a personal \
                 access token, or an OAuth token for a service principal, in it via \
                 `value_from: {{ secret: … }}`."
            ),
        };
        Ok(Auth { source, snowflake_type })
    }

    async fn bearer(&mut self, client: &reqwest::Client) -> Result<String> {
        match &mut self.source {
            TokenSource::Fixed(token) => Ok(token.clone()),
            TokenSource::Metadata { url, cached, password_var } => {
                if let Some((token, fresh_until)) = cached {
                    if Instant::now() < *fresh_until {
                        return Ok(token.clone());
                    }
                }
                let (token, fresh_until) = metadata_token(client, url, password_var).await?;
                *cached = Some((token.clone(), fresh_until));
                Ok(token)
            }
        }
    }
}

/// The metadata server's token endpoint. `GCE_METADATA_HOST` moves it, as it does
/// for Google's own client libraries.
fn metadata_url() -> Result<reqwest::Url> {
    let host = std::env::var("GCE_METADATA_HOST")
        .ok()
        .map(|h| h.trim().to_string())
        .filter(|h| !h.is_empty())
        .unwrap_or_else(|| "metadata.google.internal".into());
    reqwest::Url::parse(&format!(
        "http://{host}/computeMetadata/v1/instance/service-accounts/default/token"
    ))
    .with_context(|| format!("GCE_METADATA_HOST '{host}' does not make a URL"))
}

async fn metadata_token(
    client: &reqwest::Client,
    url: &reqwest::Url,
    password_var: &str,
) -> Result<(String, Instant)> {
    let refusal = || {
        format!(
            "BigQuery needs a token, {password_var} is not set, and the GCE metadata server at \
             {} did not hand one out. Either set {password_var} to an OAuth access token, or run \
             the task where a service account is attached (GKE Workload Identity, Compute \
             Engine, Cloud Run).",
            url.host_str().unwrap_or("?")
        )
    };
    let resp = client
        .get(url.clone())
        .header("Metadata-Flavor", "Google")
        .timeout(Duration::from_secs(5))
        .send()
        .await
        .with_context(refusal)?;
    let status = resp.status();
    if !status.is_success() {
        bail!("{} It answered HTTP {status}.", refusal());
    }
    let v: serde_json::Value = resp.json().await.with_context(refusal)?;
    let token = v
        .get("access_token")
        .and_then(serde_json::Value::as_str)
        .filter(|t| !t.is_empty())
        .with_context(refusal)?
        .to_string();
    // A minute early, so a token is never sent in its last seconds.
    let lifetime = v.get("expires_in").and_then(serde_json::Value::as_u64).unwrap_or(0);
    Ok((token, Instant::now() + Duration::from_secs(lifetime.saturating_sub(60))))
}
