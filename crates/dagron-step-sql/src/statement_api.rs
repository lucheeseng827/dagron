//! The statement APIs — Snowflake's SQL API, BigQuery's `jobs.query`, and Databricks'
//! Statement Execution API.
//!
//! ## Three protocols, one shape
//!
//! None of the three speaks a wire this crate already had. There is no Postgres or
//! MySQL wire in front of them, and their HTTP is not ClickHouse's one POST, one
//! answer. Each is its own vendor protocol and counts against the maintenance bound
//! in the crate docs as one: three protocols, not three names for one.
//!
//! One transport carries all three because they share a shape:
//!
//! 1. **Submit.** One POST carries the statement and its session context
//!    (warehouse, database, project, catalog…).
//! 2. **Poll.** The answer is the result or a *handle*, and a handle is polled
//!    until the statement settles: succeeded, failed or cancelled.
//! 3. **Page.** A settled result arrives a page at a time: Snowflake partitions,
//!    BigQuery page tokens, Databricks chunks.
//!
//! This module is the pure half of that shape. It builds the requests each step
//! sends ([`Call`]) and says what each answer means ([`Reply`], [`Page`]), so all of
//! it is testable without a network. It sends nothing and holds no credential. The
//! binary drives it, applies the token, and owns the deadline.
//!
//! ## What the handle buys
//!
//! A wire transport gets no handle back. A statement there is a connection, and a
//! run cancelled mid-statement abandons it. Each API here names the statement it is
//! running. The step uses that name to **cancel on its own deadline**
//! (`SQL_TIMEOUT_SECS`), so a statement that outlives the step does not go on
//! spending the warehouse's money. Snowflake is also sent the deadline as its own
//! `timeout`, so a step killed outright leaves a statement Snowflake stops by
//! itself.
//!
//! ## Scripts
//!
//! `script` is one request on each. Snowflake is told `MULTI_STATEMENT_COUNT=0`;
//! its default of 1 is what holds `exec` to one statement. BigQuery runs a script
//! wherever it is sent, so `exec` cannot hold it to one. Databricks takes one
//! statement per request, so its script is sent as one `BEGIN … END` compound
//! statement ([`databricks_script`]). Only the script's own transaction makes any
//! of it atomic, and on Databricks nothing does.
//!
//! ## The budget reaches the store where the API lets it
//!
//! `scalar` asks for two rows and `rows` for one past `SQL_MAX_ROWS`: BigQuery's
//! `maxResults` and Databricks' `row_limit`. One past, so that a result over the
//! budget arrives over it and is refused, rather than being cut to fit without a
//! word. `exec` and `script` ask BigQuery for none. Snowflake's API has no such
//! knob, so there the budget bounds the read alone, as on the wire transports.
//!
//! ## Values
//!
//! Each row becomes one JSON object keyed by column name, like the other
//! transports. Values are what the API sent: **strings and nulls**, because all
//! three render every value as text (Snowflake's `jsonv2`, BigQuery's `f`/`v`
//! cells, Databricks' `JSON_ARRAY`). BigQuery's records become objects and its
//! repeated fields arrays. Nothing is re-typed, because a `NUMBER(38,0)` does not
//! fit an `i64`, and silently losing digits is worse than handing over the text.

use std::collections::BTreeMap;

use anyhow::{bail, Context, Result};
use reqwest::Url;
use serde_json::{json, Map, Value};

use crate::Mode;

/// Which statement API a store is reached through.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Api {
    /// Snowflake's SQL API (`/api/v2/statements`).
    Snowflake,
    /// BigQuery's `jobs.query` and `jobs.getQueryResults`.
    BigQuery,
    /// Databricks' Statement Execution API (`/api/2.0/sql/statements`).
    Databricks,
}

impl Api {
    /// The session parameters this API's DSN may carry, and whether each is
    /// required. Anything else in the query string is refused, not ignored: a
    /// misspelt `warehouse_Id` that is dropped runs the statement somewhere the
    /// author did not ask for.
    fn params(self) -> &'static [(&'static str, bool)] {
        match self {
            Api::Snowflake => {
                &[("warehouse", false), ("database", false), ("schema", false), ("role", false)]
            }
            Api::BigQuery => &[("project", true), ("dataset", false), ("location", false)],
            Api::Databricks => &[("warehouse_id", true), ("catalog", false), ("schema", false)],
        }
    }

    fn example_dsn(self) -> &'static str {
        match self {
            Api::Snowflake => {
                "https://<account>.snowflakecomputing.com/?warehouse=TRANSFORM&database=ANALYTICS&schema=PUBLIC&role=TRANSFORMER"
            }
            Api::BigQuery => {
                "https://bigquery.googleapis.com/?project=my-project&dataset=analytics&location=EU"
            }
            Api::Databricks => {
                "https://<workspace>.cloud.databricks.com/?warehouse_id=abc123&catalog=main&schema=analytics"
            }
        }
    }

    fn name(self) -> &'static str {
        match self {
            Api::Snowflake => "Snowflake",
            Api::BigQuery => "BigQuery",
            Api::Databricks => "Databricks",
        }
    }
}

/// The token types Snowflake's SQL API accepts in
/// `X-Snowflake-Authorization-Token-Type`, as `SQL_TOKEN_TYPE` spells them.
///
/// The default is the programmatic access token, because it is the one a task can
/// hold as a stored secret: an OAuth access token lives about ten minutes and a
/// key-pair JWT at most an hour, so either has to be minted just before the run.
/// This step mints neither.
pub fn snowflake_token_type(raw: Option<&str>) -> Result<&'static str> {
    match raw.map(|s| s.trim().to_ascii_lowercase()).filter(|s| !s.is_empty()).as_deref() {
        None | Some("programmatic_access_token") | Some("pat") => Ok("PROGRAMMATIC_ACCESS_TOKEN"),
        Some("oauth") => Ok("OAUTH"),
        Some("keypair_jwt") | Some("jwt") => Ok("KEYPAIR_JWT"),
        Some(other) => bail!(
            "unknown SQL_TOKEN_TYPE '{other}'. Snowflake takes 'programmatic_access_token' \
             (the default), 'oauth' or 'keypair_jwt'. The token itself goes in SQL_PASSWORD."
        ),
    }
}

/// Where a statement API is, and the session it runs in, parsed from `SQL_DSN`.
///
/// The DSN is the API's origin with the session context as query parameters (each
/// API's own set; any other is refused). It names a host and nothing else, so it carries no path and
/// no user. The token alone says who is asking, and every request is built from
/// this origin. A URL the store hands back is never followed, so the token never
/// leaves for a host the DSN did not name.
#[derive(Debug, Clone)]
pub struct Target {
    pub api: Api,
    origin: Url,
    params: BTreeMap<String, String>,
}

impl Target {
    pub fn parse(api: Api, dsn: &str) -> Result<Target> {
        let example = api.example_dsn();
        let url = Url::parse(dsn.trim()).with_context(|| {
            format!("SQL_DSN for {} must be a URL, like {example}", api.name())
        })?;
        match url.scheme() {
            "https" => {}
            // Loopback only, which is a proxy on the same host or a test. Anywhere
            // else, plain HTTP sends a long-lived bearer token in the clear to a
            // service that only answers HTTPS anyway.
            "http" if is_loopback(&url) => {}
            "http" => bail!(
                "SQL_DSN for {} is plain http:// to a host that is not loopback. Every request \
                 carries the token, so it would cross the network in the clear. Use https://.",
                api.name()
            ),
            other => bail!(
                "SQL_DSN for {} must be an https:// URL, got scheme '{other}'. For example: \
                 {example}",
                api.name()
            ),
        }
        if !url.username().is_empty() {
            bail!(
                "SQL_DSN for {} names a user ('{}'). These APIs authenticate by token alone, so a \
                 user here would be ignored. Drop it and put the token in SQL_PASSWORD.",
                api.name(),
                url.username()
            );
        }
        if !matches!(url.path(), "" | "/") {
            bail!(
                "SQL_DSN for {} carries a path ('{}'). It names the API's host, and the step \
                 builds every path itself. Put the session context in the query string instead, \
                 like {example}",
                api.name(),
                url.path()
            );
        }

        let known = api.params();
        let mut params = BTreeMap::new();
        for (key, value) in url.query_pairs() {
            let Some(_) = known.iter().find(|(k, _)| *k == key) else {
                let names: Vec<&str> = known.iter().map(|(k, _)| *k).collect();
                bail!(
                    "SQL_DSN for {} carries an unknown parameter '{key}'. Known: {}. It is refused \
                     rather than dropped: a misspelt parameter that is ignored runs the statement \
                     somewhere you did not ask for.",
                    api.name(),
                    names.join(", ")
                );
            };
            if value.trim().is_empty() {
                bail!("SQL_DSN for {} sets '{key}' to nothing", api.name());
            }
            if params.insert(key.to_string(), value.trim().to_string()).is_some() {
                bail!("SQL_DSN for {} sets '{key}' twice", api.name());
            }
        }
        for (key, required) in known {
            if *required && !params.contains_key(*key) {
                bail!(
                    "SQL_DSN for {} needs '{key}', for example: {example}",
                    api.name()
                );
            }
        }

        let mut origin = url;
        origin.set_query(None);
        origin.set_fragment(None);
        Ok(Target { api, origin, params })
    }

    fn param(&self, key: &str) -> Option<&str> {
        self.params.get(key).map(String::as_str)
    }

    /// A URL on this target's origin with exactly these path segments, each one
    /// percent-encoded. A handle is a string the store chose, and it lands in a
    /// path here, so it is encoded rather than trusted to be path-safe.
    fn url(&self, segments: &[&str]) -> Url {
        let mut url = self.origin.clone();
        url.path_segments_mut()
            .expect("an http(s) URL always has a path")
            .clear()
            .extend(segments);
        url
    }

    /// The one request that submits `statement`.
    pub fn submit(&self, statement: &str, ask: &Ask) -> Call {
        match self.api {
            Api::Snowflake => {
                let mut url = self.url(&["api", "v2", "statements"]);
                // `async=true`: the answer is a handle at once, rather than after up to
                // 45 seconds of Snowflake waiting on our behalf. A call that blocks
                // that long can outlast the step's deadline before the step holds
                // anything it could cancel.
                url.query_pairs_mut().append_pair("async", "true");
                let mut body = json!({
                    "statement": statement,
                    // Snowflake's own bound, which holds even when this process is killed
                    // and cannot cancel anything. 0 means "Snowflake's maximum" there, and
                    // its maximum is 604800 s.
                    "timeout": ask.timeout_secs.clamp(1, 604_800),
                });
                for key in ["warehouse", "database", "schema", "role"] {
                    if let Some(v) = self.param(key) {
                        body[key] = json!(v);
                    }
                }
                if ask.mode == Mode::Script {
                    // 0 = any number of statements. The default is 1, which is what keeps
                    // `exec` to the one statement it promises.
                    body["parameters"] = json!({ "MULTI_STATEMENT_COUNT": "0" });
                }
                Call::post(url, body)
            }
            Api::BigQuery => {
                let project = self.param("project").expect("required at parse");
                let url = self.url(&["bigquery", "v2", "projects", project, "queries"]);
                let mut body = json!({
                    "query": statement,
                    "useLegacySql": false,
                    "timeoutMs": BIGQUERY_WAIT_MS,
                    "maxResults": ask.bigquery_max_results(),
                    // Timestamps as integer microseconds, not floating-point seconds,
                    // which cannot hold a microsecond exactly.
                    "formatOptions": { "useInt64Timestamp": true },
                });
                if let Some(dataset) = self.param("dataset") {
                    body["defaultDataset"] = json!({ "projectId": project, "datasetId": dataset });
                }
                if let Some(location) = self.param("location") {
                    body["location"] = json!(location);
                }
                // A script needs nothing more: BigQuery runs one wherever it is sent.
                // Which is also why `exec` cannot hold BigQuery to one statement the way
                // a prepared statement does on the wire.
                Call::post(url, body)
            }
            Api::Databricks => {
                // Documented with the trailing slash.
                let url = self.url(&["api", "2.0", "sql", "statements", ""]);
                let statement = match ask.mode {
                    Mode::Script => databricks_script(statement),
                    _ => statement.to_string(),
                };
                let mut body = json!({
                    "statement": statement,
                    "warehouse_id": self.param("warehouse_id").expect("required at parse"),
                    // Databricks' shortest wait other than none. A quick statement answers
                    // in this one call, and a slow one hands back its id within 5 s.
                    "wait_timeout": "5s",
                    "on_wait_timeout": "CONTINUE",
                    "disposition": "INLINE",
                    "format": "JSON_ARRAY",
                });
                for key in ["catalog", "schema"] {
                    if let Some(v) = self.param(key) {
                        body[key] = json!(v);
                    }
                }
                if let Some(limit) = ask.row_limit {
                    body["row_limit"] = json!(limit);
                }
                Call::post(url, body)
            }
        }
    }

    /// The request that asks after a running statement.
    pub fn poll(&self, handle: &Handle, ask: &Ask) -> Call {
        match self.api {
            Api::Snowflake => Call::get(self.url(&["api", "v2", "statements", &handle.id])),
            Api::BigQuery => self.bigquery_results(handle, ask, None),
            Api::Databricks => Call::get(self.url(&["api", "2.0", "sql", "statements", &handle.id])),
        }
    }

    /// The request for the page `cursor` names.
    pub fn page(&self, handle: &Handle, cursor: &Cursor, ask: &Ask) -> Call {
        match cursor {
            Cursor::Partition { next, .. } => {
                let mut url = self.url(&["api", "v2", "statements", &handle.id]);
                url.query_pairs_mut().append_pair("partition", &next.to_string());
                Call::get(url)
            }
            Cursor::PageToken(token) => self.bigquery_results(handle, ask, Some(token)),
            Cursor::Chunk(index) => Call::get(self.url(&[
                "api",
                "2.0",
                "sql",
                "statements",
                &handle.id,
                "result",
                "chunks",
                &index.to_string(),
            ])),
        }
    }

    /// The request that cancels a running statement.
    pub fn cancel(&self, handle: &Handle) -> Call {
        match self.api {
            Api::Snowflake => {
                Call::post(self.url(&["api", "v2", "statements", &handle.id, "cancel"]), json!({}))
            }
            Api::BigQuery => {
                let project = self.bigquery_project(handle);
                let mut url = self.url(&["bigquery", "v2", "projects", project, "jobs", &handle.id, "cancel"]);
                if let Some(location) = self.bigquery_location(handle) {
                    url.query_pairs_mut().append_pair("location", location);
                }
                Call::post(url, json!({}))
            }
            Api::Databricks => Call::post(
                self.url(&["api", "2.0", "sql", "statements", &handle.id, "cancel"]),
                json!({}),
            ),
        }
    }

    /// `jobs.getQueryResults`: BigQuery's poll and its pager are the same call, and
    /// the first answer with `jobComplete` is also the result's first page.
    fn bigquery_results(&self, handle: &Handle, ask: &Ask, page_token: Option<&str>) -> Call {
        let project = self.bigquery_project(handle);
        let mut url = self.url(&["bigquery", "v2", "projects", project, "queries", &handle.id]);
        {
            let mut q = url.query_pairs_mut();
            if let Some(location) = self.bigquery_location(handle) {
                q.append_pair("location", location);
            }
            q.append_pair("timeoutMs", &BIGQUERY_WAIT_MS.to_string());
            q.append_pair("maxResults", &ask.bigquery_max_results().to_string());
            q.append_pair("formatOptions.useInt64Timestamp", "true");
            if let Some(token) = page_token {
                q.append_pair("pageToken", token);
            }
        }
        Call::get(url)
    }

    fn bigquery_project<'a>(&'a self, handle: &'a Handle) -> &'a str {
        handle
            .project
            .as_deref()
            .unwrap_or_else(|| self.param("project").expect("required at parse"))
    }

    /// The job's own location wins over the DSN's: BigQuery decides where a job
    /// runs when the DSN names nowhere, and a poll sent to the wrong location
    /// answers "not found".
    fn bigquery_location<'a>(&'a self, handle: &'a Handle) -> Option<&'a str> {
        handle.location.as_deref().or_else(|| self.param("location"))
    }

    /// Whether `status` alone settles the statement, for a mode that keeps no rows.
    ///
    /// Snowflake answers a finished statement with `200` and the first partition of
    /// its result in the same body. `exec` and `script` keep nothing, so that body
    /// is not theirs to read: an `exec` that happened to be a large `SELECT` would
    /// otherwise fail to read an answer it never wanted. BigQuery and Databricks
    /// put the statement's state in the body, so there the body is always read.
    pub fn settled_by_status(&self, status: u16, mode: Mode) -> bool {
        self.api == Api::Snowflake && status == 200 && matches!(mode, Mode::Exec | Mode::Script)
    }

    /// What an answer to [`submit`](Target::submit) or [`poll`](Target::poll) says.
    ///
    /// `Err` is the statement's own failure, or an answer this module cannot read.
    /// Either way there is nothing left running to cancel.
    pub fn reply(&self, status: u16, body: &[u8]) -> Result<Reply> {
        match self.api {
            Api::Snowflake => snowflake_reply(status, body),
            Api::BigQuery => bigquery_reply(status, body),
            Api::Databricks => databricks_reply(status, body),
        }
    }

    /// What an answer to [`page`](Target::page) holds.
    pub fn read_page(&self, fields: &[Field], cursor: &Cursor, status: u16, body: &[u8]) -> Result<Page> {
        if !(200..300).contains(&status) {
            return Err(vendor_error(self.api, status, body));
        }
        let v = json_body(self.api, body)?;
        match cursor {
            Cursor::Partition { next, total } => Ok(Page {
                rows: array_rows(fields, v.get("data"))?,
                next: (next + 1 < *total).then_some(Cursor::Partition { next: next + 1, total: *total }),
            }),
            Cursor::PageToken(_) => Ok(Page {
                rows: bigquery_rows(fields, v.get("rows"))?,
                next: page_token(&v),
            }),
            Cursor::Chunk(_) => Ok(Page {
                rows: array_rows(fields, v.get("data_array"))?,
                next: v.get("next_chunk_index").and_then(Value::as_u64).map(Cursor::Chunk),
            }),
        }
    }
}

/// How long BigQuery is asked to hold a `jobs.query` or `getQueryResults` call open
/// while the job runs. Short enough that one call cannot outlast a deadline by
/// much, and long enough that a quick query answers in one round trip.
const BIGQUERY_WAIT_MS: u64 = 5_000;

fn is_loopback(url: &Url) -> bool {
    let Some(host) = url.host_str() else { return false };
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    // `host_str` keeps an IPv6 literal's brackets.
    host.trim_start_matches('[')
        .trim_end_matches(']')
        .parse::<std::net::IpAddr>()
        .is_ok_and(|ip| ip.is_loopback())
}

/// Wrap a script as one Databricks SQL-scripting compound statement.
///
/// The Statement Execution API takes one statement per request, and a script is
/// several. `BEGIN … END` makes them one, which Databricks runs in order and stops at
/// the first failure: the same promise `script` makes on the wire transports. It is
/// **not** a transaction. Delta has no multi-statement transactions, and a failure
/// part-way leaves the statements before it applied, which is why Freshet marks a
/// Databricks `delete_insert` `atomic: false`. It needs a warehouse on a release
/// with SQL scripting.
///
/// Every statement inside the block needs its `;`. dagron-state's `{{ sql }}`
/// gives each one but passes a lone statement through without one, so a final `;`
/// is added on a line of its own when the script does not end with one. On a line
/// of its own, a trailing `-- comment` cannot swallow it.
pub fn databricks_script(script: &str) -> String {
    let body = script.trim();
    let end = if body.ends_with(';') { "" } else { "\n;" };
    format!("BEGIN\n{body}{end}\nEND")
}

/// One HTTP request, described rather than sent.
#[derive(Debug, Clone, PartialEq)]
pub struct Call {
    pub method: Method,
    pub url: Url,
    pub body: Option<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Get,
    Post,
}

impl Call {
    fn get(url: Url) -> Call {
        Call { method: Method::Get, url, body: None }
    }

    fn post(url: Url, body: Value) -> Call {
        Call { method: Method::Post, url, body: Some(body) }
    }

    /// Whether sending this twice is the same as sending it once, and so whether a
    /// transient failure may be retried. A submit is not: retrying it after a lost
    /// answer could run the statement twice. A cancel is best-effort and sent once.
    pub fn idempotent(&self) -> bool {
        self.method == Method::Get
    }
}

/// What the step asks of the store beyond the statement itself.
#[derive(Debug, Clone, Copy)]
pub struct Ask {
    pub mode: Mode,
    /// The most rows the store is asked to send, for the modes that keep rows.
    /// `None` for `exec` and `script`, which keep none.
    pub row_limit: Option<u64>,
    /// `SQL_TIMEOUT_SECS`, for a store that can bound the statement itself.
    pub timeout_secs: u64,
}

impl Ask {
    pub fn new(mode: Mode, max_rows: u64, timeout_secs: u64) -> Ask {
        let row_limit = match mode {
            // The answer, and the one row that would prove it was not the only one.
            Mode::Scalar => Some(2),
            // One past the budget, so a result over it is refused, not trimmed to fit.
            Mode::Rows => Some(max_rows.saturating_add(1)),
            Mode::Exec | Mode::Script => None,
        };
        Ask { mode, row_limit, timeout_secs }
    }

    /// BigQuery's `maxResults` is a 32-bit count, and 0 asks for no rows at all,
    /// which is what a mode that keeps none wants.
    fn bigquery_max_results(&self) -> u64 {
        self.row_limit.unwrap_or(0).min(u64::from(u32::MAX))
    }
}

/// A running statement, as its API names it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Handle {
    /// Snowflake's `statementHandle`, BigQuery's `jobId`, Databricks' `statement_id`.
    pub id: String,
    /// BigQuery only: the project the job runs in, which is the DSN's.
    pub project: Option<String>,
    /// BigQuery only: where the job runs. Every later call has to name it.
    pub location: Option<String>,
}

/// Where the next page of a settled result is.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Cursor {
    /// Snowflake: the next partition, of `total`.
    Partition { next: u64, total: u64 },
    /// BigQuery: the `pageToken` of the next page.
    PageToken(String),
    /// Databricks: the next chunk's index.
    Chunk(u64),
}

/// A column of the result. Flat for Snowflake and Databricks; BigQuery's records
/// nest, and its repeated fields are arrays.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Field {
    pub name: String,
    pub repeated: bool,
    pub fields: Vec<Field>,
}

impl Field {
    fn flat(name: &str) -> Field {
        Field { name: name.to_string(), repeated: false, fields: Vec::new() }
    }
}

/// What an answer to a submit or a poll says.
#[derive(Debug, Clone, PartialEq)]
pub enum Reply {
    /// Still running: poll this handle.
    Running(Handle),
    /// Settled, and succeeded.
    Done(Answer),
}

/// A statement that succeeded: its columns and the first page of its result.
#[derive(Debug, Clone, PartialEq)]
pub struct Answer {
    /// The statement, when the API named it. Needed for the pages after the first.
    pub handle: Option<Handle>,
    /// Empty for a statement with no result set (DDL, most DML).
    pub fields: Vec<Field>,
    pub first: Page,
}

impl Answer {
    fn without_result(handle: Option<Handle>) -> Answer {
        Answer { handle, fields: Vec::new(), first: Page { rows: Vec::new(), next: None } }
    }
}

/// One page of a result.
#[derive(Debug, Clone, PartialEq)]
pub struct Page {
    pub rows: Vec<Map<String, Value>>,
    pub next: Option<Cursor>,
}

// ---------------------------------------------------------------------------
// Snowflake
// ---------------------------------------------------------------------------

/// `200` is a finished statement and carries partition 0; `202` is a running one;
/// anything else (`422` a failed statement, `408` its own timeout) is a failure.
fn snowflake_reply(status: u16, body: &[u8]) -> Result<Reply> {
    match status {
        200 => {
            let v = json_body(Api::Snowflake, body)?;
            let handle = v.get("statementHandle").and_then(Value::as_str).map(|id| Handle {
                id: id.to_string(),
                project: None,
                location: None,
            });
            let Some(meta) = v.get("resultSetMetaData") else {
                return Ok(Reply::Done(Answer::without_result(handle)));
            };
            let fields: Vec<Field> = meta
                .get("rowType")
                .and_then(Value::as_array)
                .context("Snowflake's resultSetMetaData carries no rowType")?
                .iter()
                .map(|col| {
                    col.get("name")
                        .and_then(Value::as_str)
                        .map(Field::flat)
                        .context("a Snowflake rowType entry carries no name")
                })
                .collect::<Result<_>>()?;
            let total = meta
                .get("partitionInfo")
                .and_then(Value::as_array)
                .map(|p| p.len() as u64)
                .unwrap_or(1);
            let rows = array_rows(&fields, v.get("data"))?;
            let next = (total > 1).then_some(Cursor::Partition { next: 1, total });
            Ok(Reply::Done(Answer { handle, fields, first: Page { rows, next } }))
        }
        202 => {
            let v = json_body(Api::Snowflake, body)?;
            let id = v
                .get("statementHandle")
                .and_then(Value::as_str)
                .context("Snowflake answered 202 (still running) without a statementHandle")?;
            Ok(Reply::Running(Handle { id: id.to_string(), project: None, location: None }))
        }
        _ => Err(vendor_error(Api::Snowflake, status, body)),
    }
}

// ---------------------------------------------------------------------------
// BigQuery
// ---------------------------------------------------------------------------

/// `jobComplete: false` is a running job; `true` is a finished one, whose answer is
/// also its first page. A failed job answers with an HTTP error, not a body.
fn bigquery_reply(status: u16, body: &[u8]) -> Result<Reply> {
    if !(200..300).contains(&status) {
        return Err(vendor_error(Api::BigQuery, status, body));
    }
    let v = json_body(Api::BigQuery, body)?;
    let handle = v.get("jobReference").and_then(|r| {
        Some(Handle {
            id: r.get("jobId")?.as_str()?.to_string(),
            project: r.get("projectId").and_then(Value::as_str).map(str::to_string),
            location: r.get("location").and_then(Value::as_str).map(str::to_string),
        })
    });
    if !v.get("jobComplete").and_then(Value::as_bool).unwrap_or(false) {
        let handle =
            handle.context("BigQuery answered jobComplete: false without a jobReference to poll")?;
        return Ok(Reply::Running(handle));
    }
    let fields = match v.get("schema").and_then(|s| s.get("fields")) {
        Some(fields) => bigquery_fields(fields)?,
        None => return Ok(Reply::Done(Answer::without_result(handle))),
    };
    let rows = bigquery_rows(&fields, v.get("rows"))?;
    let next = page_token(&v);
    Ok(Reply::Done(Answer { handle, fields, first: Page { rows, next } }))
}

fn page_token(v: &Value) -> Option<Cursor> {
    v.get("pageToken")
        .and_then(Value::as_str)
        .filter(|t| !t.is_empty())
        .map(|t| Cursor::PageToken(t.to_string()))
}

fn bigquery_fields(fields: &Value) -> Result<Vec<Field>> {
    fields
        .as_array()
        .context("BigQuery's schema.fields is not an array")?
        .iter()
        .map(|f| {
            Ok(Field {
                name: f
                    .get("name")
                    .and_then(Value::as_str)
                    .context("a BigQuery schema field carries no name")?
                    .to_string(),
                repeated: f.get("mode").and_then(Value::as_str) == Some("REPEATED"),
                fields: match f.get("fields") {
                    Some(sub) => bigquery_fields(sub)?,
                    None => Vec::new(),
                },
            })
        })
        .collect()
}

fn bigquery_rows(fields: &[Field], rows: Option<&Value>) -> Result<Vec<Map<String, Value>>> {
    let Some(rows) = rows else { return Ok(Vec::new()) };
    rows.as_array()
        .context("BigQuery's rows is not an array")?
        .iter()
        .map(|row| bigquery_record(fields, row))
        .collect()
}

/// `{"f": [{"v": …}, …]}` → `{name: value, …}`, by the schema's field order.
fn bigquery_record(fields: &[Field], record: &Value) -> Result<Map<String, Value>> {
    let cells = record
        .get("f")
        .and_then(Value::as_array)
        .context("a BigQuery row carries no `f` array")?;
    if cells.len() != fields.len() {
        bail!("a BigQuery row has {} cells for {} fields", cells.len(), fields.len());
    }
    fields
        .iter()
        .zip(cells)
        .map(|(field, cell)| {
            Ok((field.name.clone(), bigquery_value(field, cell.get("v").unwrap_or(&Value::Null))?))
        })
        .collect()
}

/// A repeated field is `[{"v": …}, …]`, and a record is `{"f": […]}` — and a
/// repeated record is both.
fn bigquery_value(field: &Field, v: &Value) -> Result<Value> {
    if !field.repeated {
        return bigquery_cell(field, v);
    }
    match v {
        Value::Null => Ok(Value::Array(Vec::new())),
        Value::Array(items) => items
            .iter()
            .map(|item| bigquery_cell(field, item.get("v").unwrap_or(&Value::Null)))
            .collect::<Result<Vec<_>>>()
            .map(Value::Array),
        other => bail!("BigQuery sent {other} for the repeated field `{}`", field.name),
    }
}

fn bigquery_cell(field: &Field, v: &Value) -> Result<Value> {
    if field.fields.is_empty() || v.is_null() {
        return Ok(v.clone());
    }
    bigquery_record(&field.fields, v).map(Value::Object)
}

// ---------------------------------------------------------------------------
// Databricks
// ---------------------------------------------------------------------------

/// The state is in the body: `PENDING` and `RUNNING` are polled, `SUCCEEDED` carries
/// the manifest and the first chunk, and `FAILED`, `CANCELED` and `CLOSED` are
/// failures.
fn databricks_reply(status: u16, body: &[u8]) -> Result<Reply> {
    if !(200..300).contains(&status) {
        return Err(vendor_error(Api::Databricks, status, body));
    }
    let v = json_body(Api::Databricks, body)?;
    let id = v
        .get("statement_id")
        .and_then(Value::as_str)
        .context("Databricks answered without a statement_id")?;
    let handle = Handle { id: id.to_string(), project: None, location: None };
    let state = v
        .get("status")
        .and_then(|s| s.get("state"))
        .and_then(Value::as_str)
        .context("Databricks answered without status.state")?;
    match state {
        "PENDING" | "RUNNING" => Ok(Reply::Running(handle)),
        "SUCCEEDED" => {
            let Some(columns) = v
                .get("manifest")
                .and_then(|m| m.get("schema"))
                .and_then(|s| s.get("columns"))
            else {
                return Ok(Reply::Done(Answer::without_result(Some(handle))));
            };
            let mut columns: Vec<(u64, Field)> = columns
                .as_array()
                .context("Databricks' manifest.schema.columns is not an array")?
                .iter()
                .enumerate()
                .map(|(i, c)| {
                    let name = c
                        .get("name")
                        .and_then(Value::as_str)
                        .context("a Databricks column carries no name")?;
                    let position = c.get("position").and_then(Value::as_u64).unwrap_or(i as u64);
                    Ok((position, Field::flat(name)))
                })
                .collect::<Result<_>>()?;
            columns.sort_by_key(|(position, _)| *position);
            let fields: Vec<Field> = columns.into_iter().map(|(_, f)| f).collect();
            let result = v.get("result");
            let rows = array_rows(&fields, result.and_then(|r| r.get("data_array")))?;
            let next = result
                .and_then(|r| r.get("next_chunk_index"))
                .and_then(Value::as_u64)
                .map(Cursor::Chunk);
            Ok(Reply::Done(Answer { handle: Some(handle), fields, first: Page { rows, next } }))
        }
        other => {
            let error = v.get("status").and_then(|s| s.get("error"));
            let code = error.and_then(|e| e.get("error_code")).and_then(Value::as_str);
            let message = error
                .and_then(|e| e.get("message"))
                .and_then(Value::as_str)
                .unwrap_or("no message");
            bail!(
                "Databricks reports the statement {other}{}: {message} [statement {id}]",
                code.map(|c| format!(" ({c})")).unwrap_or_default()
            )
        }
    }
}

// ---------------------------------------------------------------------------
// Shared
// ---------------------------------------------------------------------------

/// Rows that arrive as arrays of cells, in column order: Snowflake's `data` and
/// Databricks' `data_array`.
fn array_rows(fields: &[Field], data: Option<&Value>) -> Result<Vec<Map<String, Value>>> {
    let Some(data) = data.filter(|d| !d.is_null()) else { return Ok(Vec::new()) };
    data.as_array()
        .context("the result's rows are not an array")?
        .iter()
        .map(|row| {
            let cells = row.as_array().context("a result row is not an array")?;
            if cells.len() != fields.len() {
                bail!("a result row has {} values for {} columns", cells.len(), fields.len());
            }
            Ok(fields.iter().zip(cells).map(|(f, v)| (f.name.clone(), v.clone())).collect())
        })
        .collect()
}

fn json_body(api: Api, body: &[u8]) -> Result<Value> {
    serde_json::from_slice(body).with_context(|| {
        format!("{} answered with a body that is not JSON: {}", api.name(), snippet(body))
    })
}

/// The most of an error body quoted back. The status is the news; the body is
/// context, and a store's error page is not worth more than this of a log line.
const MAX_SNIPPET_BYTES: usize = 2048;

fn snippet(body: &[u8]) -> String {
    let cut = &body[..body.len().min(MAX_SNIPPET_BYTES)];
    // A cut mid-character is why this is lossy rather than a `from_utf8`.
    String::from_utf8_lossy(cut).trim().to_string()
}

/// A failure answer, read for the message the vendor put in it.
pub fn vendor_error(api: Api, status: u16, body: &[u8]) -> anyhow::Error {
    let v: Option<Value> = serde_json::from_slice(body).ok();
    let detail = v.as_ref().and_then(|v| match api {
        // {"code": "002003", "sqlState": "42S02", "message": "…", "statementHandle": "…"}
        Api::Snowflake => {
            let message = v.get("message").and_then(Value::as_str)?;
            let mut tags = Vec::new();
            if let Some(s) = v.get("sqlState").and_then(Value::as_str) {
                tags.push(format!("SQLSTATE {s}"));
            }
            if let Some(c) = v.get("code").and_then(Value::as_str) {
                tags.push(format!("code {c}"));
            }
            if let Some(h) = v.get("statementHandle").and_then(Value::as_str) {
                tags.push(format!("statement {h}"));
            }
            Some(if tags.is_empty() { message.to_string() } else { format!("{message} [{}]", tags.join(", ")) })
        }
        // {"error": {"code": 400, "status": "INVALID_ARGUMENT", "message": "…"}}
        Api::BigQuery => {
            let e = v.get("error")?;
            let message = e.get("message").and_then(Value::as_str)?;
            Some(match e.get("status").and_then(Value::as_str) {
                Some(s) => format!("{message} [{s}]"),
                None => message.to_string(),
            })
        }
        // {"error_code": "INVALID_PARAMETER_VALUE", "message": "…"}
        Api::Databricks => {
            let message = v.get("message").and_then(Value::as_str)?;
            Some(match v.get("error_code").and_then(Value::as_str) {
                Some(c) => format!("{message} [{c}]"),
                None => message.to_string(),
            })
        }
    });
    let detail = detail.unwrap_or_else(|| snippet(body));
    let hint = match status {
        401 | 403 => match api {
            Api::Snowflake => {
                " The token in SQL_PASSWORD was refused. Check it, and that SQL_TOKEN_TYPE names \
                 its kind."
            }
            _ => " The token was refused. Check SQL_PASSWORD, or the identity it was minted for.",
        },
        _ => "",
    };
    anyhow::anyhow!("{} answered HTTP {status}: {detail}.{hint}", api.name())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn target(api: Api, dsn: &str) -> Target {
        Target::parse(api, dsn).unwrap()
    }

    fn snowflake() -> Target {
        target(Api::Snowflake, "https://acme.snowflakecomputing.com/?warehouse=WH&database=DB&schema=S&role=R")
    }

    fn bigquery() -> Target {
        target(Api::BigQuery, "https://bigquery.googleapis.com/?project=p1&dataset=ds&location=EU")
    }

    fn databricks() -> Target {
        target(Api::Databricks, "https://dbc.cloud.databricks.com/?warehouse_id=w1&catalog=main&schema=s")
    }

    fn ask(mode: Mode) -> Ask {
        Ask::new(mode, 100, 300)
    }

    fn reply(t: &Target, status: u16, body: Value) -> Result<Reply> {
        t.reply(status, body.to_string().as_bytes())
    }

    #[test]
    fn a_dsn_names_the_host_and_the_session_and_nothing_else() {
        let e = Target::parse(Api::Databricks, "https://dbc.cloud.databricks.com/?warehouse_Id=w1")
            .unwrap_err()
            .to_string();
        assert!(e.contains("unknown parameter 'warehouse_Id'"), "{e}");
        assert!(e.contains("warehouse_id"), "names the one it meant: {e}");

        let e = Target::parse(Api::BigQuery, "https://bigquery.googleapis.com/?dataset=d")
            .unwrap_err()
            .to_string();
        assert!(e.contains("needs 'project'"), "{e}");

        let e = Target::parse(Api::Snowflake, "https://acme.snowflakecomputing.com/api/v2")
            .unwrap_err()
            .to_string();
        assert!(e.contains("carries a path"), "{e}");

        let e = Target::parse(Api::Snowflake, "https://etl@acme.snowflakecomputing.com/")
            .unwrap_err()
            .to_string();
        assert!(e.contains("by token alone"), "{e}");

        let e = Target::parse(Api::Snowflake, "https://acme.snowflakecomputing.com/?role=")
            .unwrap_err()
            .to_string();
        assert!(e.contains("'role' to nothing"), "{e}");

        let e = Target::parse(Api::Snowflake, "snowflake://acme/db").unwrap_err().to_string();
        assert!(e.contains("https://"), "{e}");

        // No parameters at all is a Snowflake session on the user's defaults.
        Target::parse(Api::Snowflake, "https://acme.snowflakecomputing.com").unwrap();
    }

    /// A bearer token rides on every request, so plain HTTP is for this host only.
    #[test]
    fn plain_http_is_refused_except_to_loopback() {
        let e = Target::parse(Api::Databricks, "http://dbc.cloud.databricks.com/?warehouse_id=w")
            .unwrap_err()
            .to_string();
        assert!(e.contains("in the clear"), "{e}");
        for host in ["127.0.0.1:8080", "localhost:9", "[::1]:9"] {
            Target::parse(Api::Databricks, &format!("http://{host}/?warehouse_id=w")).unwrap();
        }
    }

    #[test]
    fn snowflake_submits_async_with_its_session_and_its_own_timeout() {
        let call = snowflake().submit("select 1", &ask(Mode::Scalar));
        assert_eq!(call.method, Method::Post);
        assert_eq!(call.url.as_str(), "https://acme.snowflakecomputing.com/api/v2/statements?async=true");
        let body = call.body.unwrap();
        assert_eq!(body["statement"], "select 1");
        assert_eq!(body["timeout"], 300);
        assert_eq!(body["warehouse"], "WH");
        assert_eq!(body["database"], "DB");
        assert_eq!(body["schema"], "S");
        assert_eq!(body["role"], "R");
        assert!(body.get("parameters").is_none(), "exec and scalar keep Snowflake's one-statement default");
    }

    /// `script` is the only mode that lifts Snowflake's one-statement default, which
    /// is what keeps `exec` to the one statement it promises.
    #[test]
    fn a_snowflake_script_is_one_request_of_any_number_of_statements() {
        let body = snowflake().submit("begin; delete …; insert …; commit;", &ask(Mode::Script)).body.unwrap();
        assert_eq!(body["parameters"]["MULTI_STATEMENT_COUNT"], "0");
    }

    #[test]
    fn snowflake_answers_read_as_running_done_or_failed() {
        let t = snowflake();
        let running = reply(&t, 202, json!({"code": "333334", "statementHandle": "h-1"})).unwrap();
        assert_eq!(running, Reply::Running(Handle { id: "h-1".into(), project: None, location: None }));

        let done = reply(
            &t,
            200,
            json!({
                "statementHandle": "h-1",
                "resultSetMetaData": {
                    "rowType": [{"name": "N"}, {"name": "S"}],
                    "partitionInfo": [{"rowCount": 2}, {"rowCount": 1}, {"rowCount": 1}],
                },
                "data": [["1", "a"], ["2", null]],
            }),
        )
        .unwrap();
        let Reply::Done(a) = done else { panic!("{done:?}") };
        assert_eq!(a.fields.iter().map(|f| f.name.as_str()).collect::<Vec<_>>(), ["N", "S"]);
        assert_eq!(a.first.rows.len(), 2);
        assert_eq!(a.first.rows[0]["N"], "1", "values stay the strings Snowflake sent");
        assert_eq!(a.first.rows[1]["S"], Value::Null);
        assert_eq!(a.first.next, Some(Cursor::Partition { next: 1, total: 3 }));

        let e = reply(
            &t,
            422,
            json!({"code": "002003", "sqlState": "42S02", "message": "Object 'X' does not exist.", "statementHandle": "h-2"}),
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("HTTP 422"), "{e}");
        assert!(e.contains("Object 'X' does not exist."), "{e}");
        assert!(e.contains("SQLSTATE 42S02") && e.contains("statement h-2"), "{e}");
    }

    #[test]
    fn snowflake_pages_through_its_partitions_and_stops_after_the_last() {
        let t = snowflake();
        let h = Handle { id: "h/1".into(), project: None, location: None };
        let cursor = Cursor::Partition { next: 1, total: 3 };
        let call = t.page(&h, &cursor, &ask(Mode::Rows));
        assert_eq!(
            call.url.as_str(),
            "https://acme.snowflakecomputing.com/api/v2/statements/h%2F1?partition=1",
            "the handle is encoded, never trusted to be path-safe"
        );
        let fields = [Field::flat("N")];
        let p = t.read_page(&fields, &cursor, 200, br#"{"data": [["3"]]}"#).unwrap();
        assert_eq!(p.next, Some(Cursor::Partition { next: 2, total: 3 }));
        let last = Cursor::Partition { next: 2, total: 3 };
        assert_eq!(t.read_page(&fields, &last, 200, br#"{"data": [["4"]]}"#).unwrap().next, None);
    }

    /// The body of a finished `exec` is the first partition of whatever it ran.
    /// It is not `exec`'s to read, and on Snowflake the status already settles it.
    #[test]
    fn only_snowflake_settles_by_status_and_only_for_the_modes_that_keep_nothing() {
        assert!(snowflake().settled_by_status(200, Mode::Exec));
        assert!(snowflake().settled_by_status(200, Mode::Script));
        assert!(!snowflake().settled_by_status(200, Mode::Rows));
        assert!(!snowflake().settled_by_status(202, Mode::Exec));
        assert!(!bigquery().settled_by_status(200, Mode::Exec));
        assert!(!databricks().settled_by_status(200, Mode::Exec));
    }

    #[test]
    fn bigquery_asks_for_no_more_rows_than_the_mode_can_keep() {
        let t = bigquery();
        let call = t.submit("select 1", &ask(Mode::Rows));
        assert_eq!(call.url.as_str(), "https://bigquery.googleapis.com/bigquery/v2/projects/p1/queries");
        let body = call.body.unwrap();
        assert_eq!(body["useLegacySql"], false);
        assert_eq!(body["maxResults"], 101, "one past SQL_MAX_ROWS");
        assert_eq!(body["defaultDataset"], json!({"projectId": "p1", "datasetId": "ds"}));
        assert_eq!(body["location"], "EU");
        assert_eq!(t.submit("x", &ask(Mode::Scalar)).body.unwrap()["maxResults"], 2);
        assert_eq!(t.submit("x", &ask(Mode::Exec)).body.unwrap()["maxResults"], 0);
        assert_eq!(t.submit("x", &ask(Mode::Script)).body.unwrap()["maxResults"], 0);
    }

    #[test]
    fn a_bigquery_job_is_polled_where_it_runs_and_paged_by_token() {
        let t = target(Api::BigQuery, "https://bigquery.googleapis.com/?project=p1");
        let running = reply(
            &t,
            200,
            json!({"jobComplete": false, "jobReference": {"projectId": "p1", "jobId": "job_1", "location": "asia-southeast1"}}),
        )
        .unwrap();
        let Reply::Running(h) = running else { panic!("{running:?}") };
        let poll = t.poll(&h, &ask(Mode::Rows));
        assert_eq!(poll.method, Method::Get);
        assert_eq!(poll.url.path(), "/bigquery/v2/projects/p1/queries/job_1");
        let q: BTreeMap<String, String> = poll.url.query_pairs().into_owned().collect();
        assert_eq!(q["location"], "asia-southeast1", "the job's location, though the DSN named none");
        assert_eq!(q["maxResults"], "101");

        let page = t.page(&h, &Cursor::PageToken("tok 2".into()), &ask(Mode::Rows));
        let q: BTreeMap<String, String> = page.url.query_pairs().into_owned().collect();
        assert_eq!(q["pageToken"], "tok 2");

        let cancel = t.cancel(&h);
        assert_eq!(cancel.method, Method::Post);
        assert_eq!(
            cancel.url.as_str(),
            "https://bigquery.googleapis.com/bigquery/v2/projects/p1/jobs/job_1/cancel?location=asia-southeast1"
        );
    }

    #[test]
    fn bigquery_cells_become_objects_arrays_and_strings() {
        let t = bigquery();
        let done = reply(
            &t,
            200,
            json!({
                "jobComplete": true,
                "jobReference": {"projectId": "p1", "jobId": "job_1", "location": "EU"},
                "schema": {"fields": [
                    {"name": "id", "type": "INTEGER"},
                    {"name": "tags", "type": "STRING", "mode": "REPEATED"},
                    {"name": "who", "type": "RECORD", "fields": [
                        {"name": "name", "type": "STRING"},
                        {"name": "emails", "type": "STRING", "mode": "REPEATED"},
                    ]},
                    {"name": "events", "type": "RECORD", "mode": "REPEATED", "fields": [
                        {"name": "at", "type": "TIMESTAMP"},
                    ]},
                ]},
                "rows": [
                    {"f": [
                        {"v": "1"},
                        {"v": [{"v": "a"}, {"v": "b"}]},
                        {"v": {"f": [{"v": "ann"}, {"v": [{"v": "a@x"}]}]}},
                        {"v": [{"v": {"f": [{"v": "1700000000000000"}]}}]},
                    ]},
                    {"f": [{"v": "2"}, {"v": []}, {"v": null}, {"v": []}]},
                ],
                "pageToken": "next",
            }),
        )
        .unwrap();
        let Reply::Done(a) = done else { panic!("{done:?}") };
        assert_eq!(
            Value::Object(a.first.rows[0].clone()),
            json!({
                "id": "1",
                "tags": ["a", "b"],
                "who": {"name": "ann", "emails": ["a@x"]},
                "events": [{"at": "1700000000000000"}],
            })
        );
        assert_eq!(Value::Object(a.first.rows[1].clone()), json!({"id": "2", "tags": [], "who": null, "events": []}));
        assert_eq!(a.first.next, Some(Cursor::PageToken("next".into())));
        assert_eq!(a.handle.unwrap().location.as_deref(), Some("EU"));
    }

    #[test]
    fn a_bigquery_statement_without_a_result_set_is_done_with_no_rows() {
        let done = reply(&bigquery(), 200, json!({"jobComplete": true, "numDmlAffectedRows": "5"})).unwrap();
        let Reply::Done(a) = done else { panic!("{done:?}") };
        assert!(a.fields.is_empty() && a.first.rows.is_empty() && a.first.next.is_none());
    }

    #[test]
    fn a_bigquery_failure_reads_its_error_message() {
        let e = reply(
            &bigquery(),
            400,
            json!({"error": {"code": 400, "status": "INVALID_ARGUMENT", "message": "Syntax error: Unexpected end of script at [1:7]"}}),
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("Syntax error: Unexpected end of script"), "{e}");
        assert!(e.contains("INVALID_ARGUMENT"), "{e}");
    }

    #[test]
    fn databricks_submits_inline_json_and_limits_the_rows_it_will_be_sent() {
        let call = databricks().submit("select 1", &ask(Mode::Rows));
        assert_eq!(call.url.as_str(), "https://dbc.cloud.databricks.com/api/2.0/sql/statements/");
        let body = call.body.unwrap();
        assert_eq!(body["warehouse_id"], "w1");
        assert_eq!(body["catalog"], "main");
        assert_eq!(body["schema"], "s");
        assert_eq!(body["disposition"], "INLINE");
        assert_eq!(body["format"], "JSON_ARRAY");
        assert_eq!(body["on_wait_timeout"], "CONTINUE", "never CANCEL: the deadline is the step's");
        assert_eq!(body["row_limit"], 101);
        assert!(databricks().submit("x", &ask(Mode::Exec)).body.unwrap().get("row_limit").is_none());
    }

    #[test]
    fn a_databricks_script_is_one_compound_statement_with_every_statement_terminated() {
        assert_eq!(databricks_script("DELETE FROM t WHERE d = 1;\nINSERT INTO t SELECT 1;\n"),
            "BEGIN\nDELETE FROM t WHERE d = 1;\nINSERT INTO t SELECT 1;\nEND");
        // dagron-state passes a lone statement through without its `;`.
        assert_eq!(databricks_script("CREATE OR REPLACE TABLE t AS SELECT 1"),
            "BEGIN\nCREATE OR REPLACE TABLE t AS SELECT 1\n;\nEND");
        // A trailing line comment cannot swallow the added terminator.
        assert_eq!(databricks_script("SELECT 1 -- one"), "BEGIN\nSELECT 1 -- one\n;\nEND");
        let body = databricks().submit("SELECT 1;", &ask(Mode::Script)).body.unwrap();
        assert_eq!(body["statement"], "BEGIN\nSELECT 1;\nEND");
        assert_eq!(databricks().submit("SELECT 1", &ask(Mode::Exec)).body.unwrap()["statement"], "SELECT 1");
    }

    #[test]
    fn databricks_answers_read_by_state() {
        let t = databricks();
        let running = reply(&t, 200, json!({"statement_id": "s-1", "status": {"state": "PENDING"}})).unwrap();
        assert!(matches!(running, Reply::Running(ref h) if h.id == "s-1"));

        let done = reply(
            &t,
            200,
            json!({
                "statement_id": "s-1",
                "status": {"state": "SUCCEEDED"},
                "manifest": {"schema": {"columns": [
                    {"name": "b", "position": 1},
                    {"name": "a", "position": 0},
                ]}},
                "result": {"chunk_index": 0, "data_array": [["1", "x"]], "next_chunk_index": 1},
            }),
        )
        .unwrap();
        let Reply::Done(a) = done else { panic!("{done:?}") };
        assert_eq!(Value::Object(a.first.rows[0].clone()), json!({"a": "1", "b": "x"}), "by position, not by listing order");
        assert_eq!(a.first.next, Some(Cursor::Chunk(1)));

        let e = reply(
            &t,
            200,
            json!({"statement_id": "s-2", "status": {"state": "FAILED", "error": {"error_code": "BAD_REQUEST", "message": "[TABLE_OR_VIEW_NOT_FOUND] x"}}}),
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("FAILED (BAD_REQUEST): [TABLE_OR_VIEW_NOT_FOUND] x [statement s-2]"), "{e}");

        let e = reply(&t, 403, json!({"error_code": "PERMISSION_DENIED", "message": "no"})).unwrap_err().to_string();
        assert!(e.contains("HTTP 403") && e.contains("[PERMISSION_DENIED]") && e.contains("SQL_PASSWORD"), "{e}");
    }

    #[test]
    fn databricks_pages_by_chunk_index_on_the_dsn_host() {
        let t = databricks();
        let h = Handle { id: "s-1".into(), project: None, location: None };
        let call = t.page(&h, &Cursor::Chunk(2), &ask(Mode::Rows));
        assert_eq!(
            call.url.as_str(),
            "https://dbc.cloud.databricks.com/api/2.0/sql/statements/s-1/result/chunks/2"
        );
        let fields = [Field::flat("a")];
        let p = t.read_page(&fields, &Cursor::Chunk(2), 200, br#"{"data_array": [["9"]]}"#).unwrap();
        assert_eq!(p.rows.len(), 1);
        assert_eq!(p.next, None, "no next_chunk_index is the last chunk");
    }

    #[test]
    fn a_row_that_does_not_match_its_columns_is_refused_not_zipped_short() {
        let e = reply(
            &snowflake(),
            200,
            json!({"resultSetMetaData": {"rowType": [{"name": "A"}, {"name": "B"}]}, "data": [["1"]]}),
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains("1 values for 2 columns"), "{e}");
    }

    #[test]
    fn the_snowflake_token_type_defaults_to_the_one_a_task_can_store() {
        assert_eq!(snowflake_token_type(None).unwrap(), "PROGRAMMATIC_ACCESS_TOKEN");
        assert_eq!(snowflake_token_type(Some(" OAuth ")).unwrap(), "OAUTH");
        assert_eq!(snowflake_token_type(Some("keypair_jwt")).unwrap(), "KEYPAIR_JWT");
        assert!(snowflake_token_type(Some("password")).unwrap_err().to_string().contains("SQL_PASSWORD"));
    }

    #[test]
    fn only_reads_are_retried() {
        let t = databricks();
        let h = Handle { id: "s".into(), project: None, location: None };
        assert!(!t.submit("x", &ask(Mode::Exec)).idempotent(), "a retried submit can run twice");
        assert!(t.poll(&h, &ask(Mode::Exec)).idempotent());
        assert!(!t.cancel(&h).idempotent());
    }
}
