//! dagron MCP server core — Model Context Protocol over stdio.
//!
//! Exposes the dagron management API as MCP **tools** so an AI agent (an MCP
//! client) can submit, list, inspect, and cancel workflow runs and read task logs.
//! The catalogue also surfaces cluster-internal signals — `dagron_get_metrics`
//! (run/task counts + dead-letter total), `dagron_list_dead_letters` (the poison
//! queue), and `dagron_get_run_events` (a bounded read of the per-run SSE event
//! channel) — so the engine itself is the communication seam between the AI
//! agent and the live state of the Dagron cluster, not just a CRUD façade.
//! [`handle`] dispatches one JSON-RPC message; [`DagronClient`] is the thin
//! dagron-api HTTP adapter the tools call.

use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::time::Duration;

/// MCP protocol revision this server implements.
pub const PROTOCOL_VERSION: &str = "2024-11-05";
pub const SERVER_NAME: &str = "dagron-mcp";

/// Thin client for the dagron management API (`dagron-api`).
pub struct DagronClient {
    http: reqwest::Client,
    base: String,
    token: Option<String>,
}

impl DagronClient {
    /// `DAGRON_API_URL` (default `http://localhost:8080`) + optional
    /// `DAGRON_MCP_TOKEN` (sent as `Authorization: Bearer …`).
    ///
    /// Fails when the token would cross the network in the clear — see
    /// [`plaintext_token_verdict`] for the reasoning and the opt-out.
    pub fn from_env() -> Result<Self> {
        let base = std::env::var("DAGRON_API_URL")
            .unwrap_or_else(|_| "http://localhost:8080".to_string());
        let token = std::env::var("DAGRON_MCP_TOKEN").ok().filter(|t| !t.is_empty());
        let opted_in = std::env::var("DAGRON_MCP_ALLOW_PLAINTEXT_TOKEN")
            .map(|v| matches!(v.as_str(), "1" | "true" | "yes"))
            .unwrap_or(false);
        plaintext_token_verdict(&base, token.is_some(), opted_in)?;
        Ok(Self { http: reqwest::Client::new(), base, token })
    }

    fn auth(&self, rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.token {
            Some(t) => rb.bearer_auth(t),
            None => rb,
        }
    }

    /// GET `path` against dagron-api. Public so a composing server can add tools
    /// without re-implementing base-URL and auth handling.
    pub async fn get(&self, path: &str) -> Result<String> {
        let r = self
            .auth(self.http.get(format!("{}{path}", self.base)))
            .send()
            .await
            .context("dagron-api request failed")?;
        body_or_status(r).await
    }

    /// POST `path` with an optional `(body, content-type)`. Public for the same
    /// reason as [`DagronClient::get`].
    pub async fn post(&self, path: &str, body: Option<(String, &'static str)>) -> Result<String> {
        let mut rb = self.auth(self.http.post(format!("{}{path}", self.base)));
        if let Some((b, ct)) = body {
            rb = rb.header("content-type", ct).body(b);
        }
        let r = rb.send().await.context("dagron-api request failed")?;
        body_or_status(r).await
    }

    /// Bounded read of an SSE endpoint: open the connection, then pull chunks
    /// until `budget` elapses (or the server closes the stream early).
    /// Returns the raw text accumulated. The caller is responsible for parsing
    /// SSE framing — keeping the I/O primitive small means the same helper
    /// works for any future stream endpoint we expose.
    async fn read_sse(&self, path: &str, budget: Duration) -> Result<String> {
        let mut r = self
            .auth(self.http.get(format!("{}{path}", self.base)))
            .header("accept", "text/event-stream")
            .send()
            .await
            .context("dagron-api request failed")?;
        let status = r.status();
        if !status.is_success() {
            let text = r.text().await.unwrap_or_default();
            anyhow::bail!("dagron-api returned {status}: {text}");
        }
        // Cap accumulated bytes so a chatty stream can't blow up the MCP reply
        // — 256 KiB is well above a few SSE events but small enough to keep the
        // JSON-RPC frame manageable.
        const MAX_BYTES: usize = 256 * 1024;
        // Buffer raw bytes, not decoded chars. `reqwest::Response::chunk()` is
        // aligned to network frames, not to UTF-8 codepoint boundaries, so a
        // multi-byte char that straddles two chunks would be turned into U+FFFD
        // if we decoded each chunk independently. Decode once at the end.
        let mut buf: Vec<u8> = Vec::new();
        let deadline = tokio::time::Instant::now() + budget;
        loop {
            let remaining = match deadline.checked_duration_since(tokio::time::Instant::now()) {
                Some(d) if !d.is_zero() => d,
                _ => break,
            };
            match tokio::time::timeout(remaining, r.chunk()).await {
                // Budget elapsed — return whatever we have.
                Err(_) => break,
                // Server closed the stream cleanly — also return.
                Ok(Ok(None)) => break,
                Ok(Ok(Some(bytes))) => {
                    if buf.len() + bytes.len() > MAX_BYTES {
                        let take = MAX_BYTES.saturating_sub(buf.len());
                        buf.extend_from_slice(&bytes[..take]);
                        break;
                    }
                    buf.extend_from_slice(&bytes);
                }
                Ok(Err(e)) => anyhow::bail!("sse read failed: {e}"),
            }
        }
        Ok(String::from_utf8_lossy(&buf).into_owned())
    }
}

/// Refuse a configuration that would put the bearer token on the wire in the
/// clear, unless the operator has explicitly opted in.
///
/// A warning was the first attempt here and it was the wrong call: a process
/// cannot verify that a service mesh is protecting the connection, and a log line
/// nobody reads does not stop a misconfigured deployment from shipping the token
/// in cleartext. Refusing does stop it — and `DAGRON_MCP_ALLOW_PLAINTEXT_TOKEN=1`
/// keeps the legitimate case (plaintext to an in-cluster Service, transport
/// secured below us) fully supported while making that exception explicit and
/// auditable rather than silent. `DAGRON_MCP_HTTP_ALLOW_UNAUTHENTICATED` is the
/// same shape of risk answered the same way.
fn plaintext_token_verdict(base: &str, has_token: bool, opted_in: bool) -> Result<()> {
    if !has_token || opted_in {
        return Ok(());
    }
    if let Some(host) = plaintext_remote_host(base) {
        anyhow::bail!(
            "refusing to send DAGRON_MCP_TOKEN to '{host}' over plaintext http://: anything on \
             the path could read it. Use https://, point DAGRON_API_URL at loopback, or set \
             DAGRON_MCP_ALLOW_PLAINTEXT_TOKEN=1 if the transport is already secured (a service \
             mesh, an encrypted link)"
        );
    }
    Ok(())
}

/// The host a bearer token would cross the network *in the clear* to reach, or
/// `None` when the configuration is safe.
///
/// `http://` to a loopback address never leaves the box, so it doesn't count.
/// Anything else on plaintext does.
///
/// Loopback is decided by parsing the host as an [`std::net::IpAddr`], not by a
/// textual prefix: `127.0.0.1.example.com` is a perfectly ordinary remote
/// hostname, and a `starts_with("127.")` test would silently exempt it while the
/// token still crossed the wire.
///
/// The host is returned with any userinfo stripped, so a caller reporting it
/// cannot accidentally print the credentials in `http://user:pass@host`.
fn plaintext_remote_host(base: &str) -> Option<String> {
    let rest = base.strip_prefix("http://")?;
    // Authority is everything before the path/query/fragment; userinfo (which may
    // carry credentials) is everything before the last '@' and is dropped here so
    // it can neither be compared nor logged.
    let authority = rest.split(['/', '?', '#']).next().unwrap_or("");
    let host_port = authority.rsplit_once('@').map_or(authority, |(_, h)| h);
    // Strip the port, keeping a bracketed IPv6 literal intact.
    let host = match host_port.strip_prefix('[') {
        Some(v6) => v6.split(']').next().unwrap_or(""),
        None => host_port.rsplit_once(':').map_or(host_port, |(h, _)| h),
    };
    if host.is_empty() {
        return None;
    }
    let is_loopback = match host.parse::<std::net::IpAddr>() {
        Ok(ip) => ip.is_loopback(),
        Err(_) => host.eq_ignore_ascii_case("localhost"),
    };
    (!is_loopback).then(|| host.to_string())
}

async fn body_or_status(r: reqwest::Response) -> Result<String> {
    let status = r.status();
    let text = r.text().await.unwrap_or_default();
    if status.is_success() {
        Ok(text)
    } else {
        anyhow::bail!("dagron-api returned {status}: {text}")
    }
}

/// Query params of the shared log filter grammar
/// (`dagron_logging::logfilter`), as MCP tool arguments. Listed once so the
/// per-task and whole-run log tools can't drift into offering different filters
/// over the same data — and so an agent that learns one has learned both.
const LOG_FILTER_ARGS: &[(&str, &str)] = &[
    ("q", "keep only lines containing this text"),
    ("exclude", "drop lines containing this text"),
    ("regex", "keep only lines matching this regular expression"),
    ("level", "keep only these inferred levels, comma-separated (error,warn,info,debug,trace,plain)"),
    ("case", "match case-sensitively ('1' to enable; default is insensitive)"),
    ("context", "also keep this many lines either side of each match"),
    ("limit", "maximum lines to return"),
    ("tail", "when capped, keep the last lines instead of the first ('1' to enable)"),
];

/// Build a log tool's `properties` map: `run_id`, the filter args, plus whatever
/// is specific to that tool.
fn log_tool_props(extra: &[(&str, Value)]) -> Value {
    let mut props = serde_json::Map::new();
    props.insert("run_id".into(), json!({ "type": "string" }));
    for (name, value) in extra {
        props.insert((*name).into(), value.clone());
    }
    for (name, description) in LOG_FILTER_ARGS {
        props.insert((*name).into(), json!({ "type": "string", "description": description }));
    }
    Value::Object(props)
}

/// Collect the log filter args present in a tool call into a query string.
///
/// Values are passed through as strings and validated by the engine, which owns
/// the grammar — re-implementing the parse here would give an agent two places
/// to be told a regex is invalid, with two different messages.
fn log_filter_query(args: &Value) -> Result<String> {
    let mut parts: Vec<String> = Vec::new();
    let mut push = |key: &str| -> Result<()> {
        match args.get(key) {
            None | Some(Value::Null) => Ok(()),
            Some(Value::String(s)) if s.is_empty() => Ok(()),
            Some(Value::String(s)) => {
                parts.push(format!("{key}={}", urlencode(s)));
                Ok(())
            }
            Some(other) => anyhow::bail!("`{key}` must be a string, got {other}"),
        }
    };
    for (name, _) in LOG_FILTER_ARGS {
        push(name)?;
    }
    push("task")?;
    Ok(if parts.is_empty() { String::new() } else { format!("?{}", parts.join("&")) })
}

/// Percent-encode a query value. Only unreserved characters pass through, so a
/// regex containing `&`, `+` or `[` reaches the engine as one intact value.
fn urlencode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// The MCP tool catalogue (name, description, JSON-Schema input).
pub fn tool_defs() -> Vec<Value> {
    vec![
        json!({
            "name": "dagron_list_runs",
            "description": "List recent workflow runs (id, status, timing).",
            "inputSchema": { "type": "object", "properties": {}, "additionalProperties": false }
        }),
        json!({
            "name": "dagron_get_run",
            "description": "Get one workflow run's detail by id.",
            "inputSchema": {
                "type": "object",
                "properties": { "run_id": { "type": "string" } },
                "required": ["run_id"], "additionalProperties": false
            }
        }),
        json!({
            "name": "dagron_submit_run",
            "description": "Submit a new workflow run from a DAG YAML spec.",
            "inputSchema": {
                "type": "object",
                "properties": { "yaml": { "type": "string", "description": "the DAG spec (YAML)" } },
                "required": ["yaml"], "additionalProperties": false
            }
        }),
        json!({
            "name": "dagron_cancel_run",
            "description": "Cancel a running workflow by id.",
            "inputSchema": {
                "type": "object",
                "properties": { "run_id": { "type": "string" } },
                "required": ["run_id"], "additionalProperties": false
            }
        }),
        json!({
            "name": "dagron_get_task_logs",
            "description": "Read a task's captured logs/output within a run. Accepts the same log filter as dagron_get_run_logs.",
            "inputSchema": {
                "type": "object",
                "properties": log_tool_props(&[("task_id", json!({ "type": "string" }))]),
                "required": ["run_id", "task_id"], "additionalProperties": false
            }
        }),
        json!({
            // The tool an agent should reach for first when a run failed: one
            // call returns every task's output, attributed and filtered, instead
            // of N calls to guess which task printed the error.
            "name": "dagron_get_run_logs",
            "description": "Read the whole run's logs as one attributed stream, filtered server-side. \
Use `level`/`q`/`regex`/`exclude` to narrow, `task` to restrict to specific tasks, and `context` \
to keep surrounding lines. Prefer this over per-task reads when diagnosing a failure.",
            "inputSchema": {
                "type": "object",
                "properties": log_tool_props(&[(
                    "task",
                    json!({ "type": "string", "description": "restrict to these task names or ids (comma-separated)" }),
                )]),
                "required": ["run_id"], "additionalProperties": false
            }
        }),
        json!({
            "name": "dagron_get_metrics",
            "description": "Cluster-internal snapshot: run/task counts by status and dead-letter total.",
            "inputSchema": { "type": "object", "properties": {}, "additionalProperties": false }
        }),
        json!({
            "name": "dagron_list_dead_letters",
            "description": "Inspect the poison queue (parked submissions). `limit` defaults to 100, capped server-side at 500.",
            "inputSchema": {
                "type": "object",
                "properties": { "limit": { "type": "integer", "minimum": 1, "maximum": 500 } },
                "additionalProperties": false
            }
        }),
        json!({
            "name": "dagron_get_run_events",
            "description": "Bounded read of the run's live event channel (SSE). Connects, collects events emitted within `wait_ms`, then returns them. `wait_ms` defaults to 2000 and is capped at 10000 so the call always returns promptly.",
            "inputSchema": {
                "type": "object",
                "properties": {
                    "run_id": { "type": "string" },
                    "wait_ms": { "type": "integer", "minimum": 100, "maximum": 10000 }
                },
                "required": ["run_id"], "additionalProperties": false
            }
        }),
    ]
}

/// Execute a tool against dagron-api, returning the response text.
pub async fn call_tool(client: &DagronClient, name: &str, args: &Value) -> Result<String> {
    let s = |k: &str| -> Result<String> {
        args.get(k)
            .and_then(|v| v.as_str())
            .map(|v| v.to_string())
            .with_context(|| format!("missing required string argument `{k}`"))
    };
    // Path-segment ids are interpolated into request URLs, so restrict them to a
    // safe alphabet — a crafted id with slashes or query fragments must not be
    // able to reshape which dagron-api endpoint we call with our auth token.
    let safe_id = |k: &str| -> Result<String> {
        let v = s(k)?;
        if v.is_empty() || !v.chars().all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_') {
            anyhow::bail!("invalid `{k}`: only non-empty [A-Za-z0-9_-] allowed");
        }
        Ok(v)
    };
    match name {
        "dagron_list_runs" => client.get("/api/runs").await,
        "dagron_get_run" => client.get(&format!("/api/runs/{}", safe_id("run_id")?)).await,
        "dagron_submit_run" => {
            // `POST /api/runs` takes a JSON envelope (`{"yaml": "…"}`), not a raw
            // YAML body: its handler binds `Json<SubmitBody>`, which rejects any
            // request that isn't `application/json` with a 415 before the handler
            // runs. Posting the spec as `application/yaml` meant the one tool an
            // agent needs most — submit a workflow — never reached the engine.
            let body = json!({ "yaml": s("yaml")? }).to_string();
            client.post("/api/runs", Some((body, "application/json"))).await
        }
        "dagron_cancel_run" => {
            client
                .post(&format!("/api/runs/{}/cancel", safe_id("run_id")?), None)
                .await
        }
        "dagron_get_task_logs" => {
            client
                .get(&format!(
                    "/api/runs/{}/tasks/{}/logs{}",
                    safe_id("run_id")?,
                    safe_id("task_id")?,
                    log_filter_query(args)?
                ))
                .await
        }
        "dagron_get_run_logs" => {
            client
                .get(&format!(
                    "/api/runs/{}/logs{}",
                    safe_id("run_id")?,
                    log_filter_query(args)?
                ))
                .await
        }
        "dagron_get_metrics" => client.get("/api/metrics").await,
        "dagron_list_dead_letters" => {
            // `limit` is optional — when present the server clamps to 1..=500,
            // but we still parse it here so a bogus type fails fast as a tool
            // error rather than confusing the AI agent with an HTTP 400.
            let path = match args.get("limit") {
                Some(v) if !v.is_null() => {
                    let n = v.as_i64().with_context(|| "`limit` must be an integer")?;
                    if !(1..=500).contains(&n) {
                        anyhow::bail!("`limit` must be between 1 and 500");
                    }
                    format!("/api/dead-letters?limit={n}")
                }
                _ => "/api/dead-letters".to_string(),
            };
            client.get(&path).await
        }
        "dagron_get_run_events" => {
            // Bounded poll over the per-run SSE channel. The budget is hard-capped
            // so an MCP tool call always returns promptly — even a never-emitting
            // run won't block the AI agent past `wait_ms`.
            let run_id = safe_id("run_id")?;
            let wait_ms = match args.get("wait_ms") {
                Some(v) if !v.is_null() => v
                    .as_i64()
                    .with_context(|| "`wait_ms` must be an integer")?,
                _ => 2000,
            };
            if !(100..=10000).contains(&wait_ms) {
                anyhow::bail!("`wait_ms` must be between 100 and 10000");
            }
            let raw = client
                .read_sse(
                    &format!("/api/runs/{run_id}/stream"),
                    Duration::from_millis(wait_ms as u64),
                )
                .await?;
            let events = parse_sse(&raw);
            Ok(json!({
                "run_id": run_id,
                "wait_ms": wait_ms,
                "event_count": events.len(),
                "events": events,
            })
            .to_string())
        }
        other => anyhow::bail!("unknown tool `{other}`"),
    }
}

/// Minimal SSE parser: split on blank lines and gather `event:` / `data:` lines
/// per event. Multi-line `data:` is joined with `\n` per the spec. Comment
/// lines (leading `:`) and unknown fields are ignored.
fn parse_sse(raw: &str) -> Vec<Value> {
    let mut out = Vec::new();
    let mut name: Option<String> = None;
    let mut data: Vec<String> = Vec::new();
    let flush = |name: &mut Option<String>, data: &mut Vec<String>, out: &mut Vec<Value>| {
        if name.is_none() && data.is_empty() {
            return;
        }
        let joined = data.join("\n");
        let parsed = serde_json::from_str::<Value>(&joined).unwrap_or(Value::String(joined));
        out.push(json!({
            "event": name.clone().unwrap_or_else(|| "message".to_string()),
            "data": parsed,
        }));
        *name = None;
        data.clear();
    };
    for line in raw.split('\n') {
        let line = line.strip_suffix('\r').unwrap_or(line);
        if line.is_empty() {
            flush(&mut name, &mut data, &mut out);
            continue;
        }
        if line.starts_with(':') {
            continue;
        }
        let (field, value) = match line.split_once(':') {
            Some((f, v)) => (f, v.strip_prefix(' ').unwrap_or(v)),
            None => (line, ""),
        };
        match field {
            "event" => name = Some(value.to_string()),
            "data" => data.push(value.to_string()),
            _ => {}
        }
    }
    // The final event may not be terminated by a blank line if the read window
    // closed mid-frame — flush so the agent still sees it.
    flush(&mut name, &mut data, &mut out);
    out
}

/// Handle one JSON-RPC message. Returns `Some(response)` for a request, `None` for
/// a notification (no `id`) that needs no reply.
pub async fn handle(client: &DagronClient, msg: &Value) -> Option<Value> {
    // A JSON-RPC message without an `id` is a notification: it must never get a
    // reply, regardless of method. Bail out before producing any response.
    let id = match msg.get("id").cloned() {
        Some(id) => id,
        None => return None,
    };
    let method = msg.get("method").and_then(|m| m.as_str()).unwrap_or("");

    match method {
        "initialize" => Some(ok(
            id,
            json!({
                "protocolVersion": PROTOCOL_VERSION,
                "capabilities": { "tools": {} },
                "serverInfo": { "name": SERVER_NAME, "version": env!("CARGO_PKG_VERSION") }
            }),
        )),
        "ping" => Some(ok(id, json!({}))),
        "tools/list" => Some(ok(id, json!({ "tools": tool_defs() }))),
        "tools/call" => {
            let params = msg.get("params").cloned().unwrap_or(Value::Null);
            let name = params.get("name").and_then(|n| n.as_str()).unwrap_or("");
            let args = params.get("arguments").cloned().unwrap_or(json!({}));
            let (text, is_error) = match call_tool(client, name, &args).await {
                Ok(t) => (t, false),
                Err(e) => (e.to_string(), true),
            };
            Some(ok(
                id,
                json!({ "content": [{ "type": "text", "text": text }], "isError": is_error }),
            ))
        }
        _ => Some(err(id, -32601, "method not found")),
    }
}

fn ok(id: Value, result: Value) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "result": result })
}

fn err(id: Value, code: i64, message: &str) -> Value {
    json!({ "jsonrpc": "2.0", "id": id, "error": { "code": code, "message": message } })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client() -> DagronClient {
        DagronClient {
            http: reqwest::Client::new(),
            base: "http://unused.test".into(),
            token: None,
        }
    }

    #[tokio::test]
    async fn initialize_returns_server_info() {
        let resp = handle(&client(), &json!({"jsonrpc":"2.0","id":1,"method":"initialize"}))
            .await
            .unwrap();
        assert_eq!(resp["result"]["serverInfo"]["name"], SERVER_NAME);
        assert_eq!(resp["result"]["protocolVersion"], PROTOCOL_VERSION);
        assert_eq!(resp["id"], 1);
    }

    #[tokio::test]
    async fn tools_list_advertises_the_catalogue() {
        let resp = handle(&client(), &json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}))
            .await
            .unwrap();
        let tools = resp["result"]["tools"].as_array().unwrap();
        let names: Vec<&str> = tools.iter().map(|t| t["name"].as_str().unwrap()).collect();
        // CRUD surface plus the cluster-internal triple — metrics, dead letters,
        // and the bounded SSE event read — that lets the AI agent observe the
        // engine, not just drive it.
        for expected in [
            "dagron_list_runs",
            "dagron_get_run",
            "dagron_submit_run",
            "dagron_cancel_run",
            "dagron_get_task_logs",
            "dagron_get_run_logs",
            "dagron_get_metrics",
            "dagron_list_dead_letters",
            "dagron_get_run_events",
        ] {
            assert!(names.contains(&expected), "missing tool {expected}");
        }
        for t in tools {
            assert!(t["name"].is_string());
            assert!(t["description"].is_string());
            assert_eq!(t["inputSchema"]["type"], "object");
        }
    }

    #[test]
    fn log_filter_query_encodes_only_what_was_passed() {
        assert_eq!(log_filter_query(&json!({ "run_id": "r1" })).unwrap(), "");
        assert_eq!(
            log_filter_query(&json!({ "run_id": "r1", "level": "error", "context": "2" })).unwrap(),
            "?level=error&context=2",
        );
        // An empty string is "not set", not "match the empty string" — an agent
        // filling in a blank argument must not accidentally filter.
        assert_eq!(log_filter_query(&json!({ "q": "" })).unwrap(), "");
        // A regex with query-significant characters must survive as one value.
        assert_eq!(
            log_filter_query(&json!({ "regex": "a&b[0-9]+" })).unwrap(),
            "?regex=a%26b%5B0-9%5D%2B",
        );
        // Wrong types fail locally with a clear message rather than as an
        // opaque HTTP 400 from dagron-api.
        assert!(log_filter_query(&json!({ "limit": 100 })).is_err());
    }

    #[test]
    fn both_log_tools_offer_the_same_filter() {
        let defs = tool_defs();
        let props = |name: &str| -> Vec<String> {
            let t = defs.iter().find(|t| t["name"] == name).expect("tool present");
            let mut k: Vec<String> =
                t["inputSchema"]["properties"].as_object().unwrap().keys().cloned().collect();
            k.sort();
            k
        };
        let task = props("dagron_get_task_logs");
        let run = props("dagron_get_run_logs");
        for (key, _) in LOG_FILTER_ARGS {
            assert!(task.contains(&key.to_string()), "task tool missing {key}");
            assert!(run.contains(&key.to_string()), "run tool missing {key}");
        }
        assert!(task.contains(&"task_id".to_string()));
        assert!(run.contains(&"task".to_string()));
    }

    #[tokio::test]
    async fn dead_letters_limit_is_validated_before_request() {
        // Out-of-range limits must fail locally so the AI agent gets a clear
        // tool error rather than an opaque HTTP 400 from dagron-api.
        for bad in [json!(0), json!(501), json!("ten")] {
            let resp = handle(
                &client(),
                &json!({"jsonrpc":"2.0","id":4,"method":"tools/call",
                        "params":{"name":"dagron_list_dead_letters","arguments":{"limit":bad}}}),
            )
            .await
            .unwrap();
            assert_eq!(resp["result"]["isError"], true, "expected error for {bad}");
        }
    }

    #[tokio::test]
    async fn run_events_rejects_out_of_range_wait() {
        let resp = handle(
            &client(),
            &json!({"jsonrpc":"2.0","id":5,"method":"tools/call",
                    "params":{"name":"dagron_get_run_events",
                              "arguments":{"run_id":"abc","wait_ms":50}}}),
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], true);
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("wait_ms"), "want wait_ms in error, got {text:?}");
    }

    #[test]
    fn sse_parser_handles_multi_line_and_named_events() {
        let raw = "event: task\ndata: {\"run_id\":\"r1\"}\n\n\
                   : keepalive\n\n\
                   data: line1\ndata: line2\n\n";
        let events = parse_sse(raw);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0]["event"], "task");
        assert_eq!(events[0]["data"]["run_id"], "r1");
        assert_eq!(events[1]["event"], "message");
        assert_eq!(events[1]["data"], "line1\nline2");
    }

    #[test]
    fn sse_parser_flushes_unterminated_tail() {
        // Bounded read may close the connection mid-frame; the last event
        // must not be dropped silently.
        let raw = "event: resync\ndata: lagged";
        let events = parse_sse(raw);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0]["event"], "resync");
        assert_eq!(events[0]["data"], "lagged");
    }

    #[tokio::test]
    async fn notification_gets_no_response() {
        let resp = handle(
            &client(),
            &json!({"jsonrpc":"2.0","method":"notifications/initialized"}),
        )
        .await;
        assert!(resp.is_none());
    }

    #[tokio::test]
    async fn request_without_id_is_treated_as_notification() {
        // A method message lacking `id` is a notification and must get no reply.
        let resp = handle(&client(), &json!({"jsonrpc":"2.0","method":"initialize"})).await;
        assert!(resp.is_none());
    }

    #[tokio::test]
    async fn unsafe_ids_are_rejected_before_any_request() {
        // Crafted ids with path/query characters must not reach the HTTP client.
        for bad in ["../secrets", "1/cancel", "a?b", "x y", ""] {
            let resp = handle(
                &client(),
                &json!({"jsonrpc":"2.0","id":7,"method":"tools/call",
                        "params":{"name":"dagron_get_run","arguments":{"run_id":bad}}}),
            )
            .await
            .unwrap();
            assert_eq!(resp["result"]["isError"], true, "expected error for {bad:?}");
            // Assert the *local* validation message so a regression that lets the
            // id reach the HTTP client (which would also set isError) is caught.
            let text = resp["result"]["content"][0]["text"].as_str().unwrap();
            assert!(
                text.contains("invalid `run_id`"),
                "expected local run_id validation before any request for {bad:?}, got {text:?}"
            );
        }
    }

    /// Serve exactly one HTTP request on an ephemeral port and hand back the
    /// request head and body. Small enough to keep in-crate, and the only way to
    /// assert the *wire shape* of a tool call — which is where a submit that
    /// dagron-api answers with 415 hides from every other kind of test.
    async fn capture_one_request() -> (String, tokio::task::JoinHandle<(String, String)>) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            let (mut sock, _) = listener.accept().await.unwrap();
            let mut raw = Vec::new();
            let mut buf = [0u8; 1024];
            // Read the head, then exactly Content-Length bytes of body.
            loop {
                let n = sock.read(&mut buf).await.unwrap();
                if n == 0 {
                    break;
                }
                raw.extend_from_slice(&buf[..n]);
                let text = String::from_utf8_lossy(&raw).into_owned();
                let Some((head, body)) = text.split_once("\r\n\r\n") else { continue };
                let len: usize = head
                    .lines()
                    .find_map(|l| {
                        let (k, v) = l.split_once(':')?;
                        k.eq_ignore_ascii_case("content-length").then(|| v.trim().parse().ok())?
                    })
                    .unwrap_or(0);
                if body.len() >= len {
                    break;
                }
            }
            let text = String::from_utf8_lossy(&raw).into_owned();
            let (head, body) = text.split_once("\r\n\r\n").unwrap();
            sock.write_all(
                b"HTTP/1.1 201 Created\r\ncontent-type: application/json\r\n\
                  content-length: 21\r\n\r\n{\"run_id\":\"run-0001\"}",
            )
            .await
            .unwrap();
            sock.flush().await.unwrap();
            (head.to_string(), body.to_string())
        });
        (base, handle)
    }

    #[tokio::test]
    async fn submit_posts_the_spec_as_json() {
        let (base, server) = capture_one_request().await;
        let client = DagronClient { http: reqwest::Client::new(), base, token: None };
        let out =
            call_tool(&client, "dagron_submit_run", &json!({ "yaml": "name: w\ntasks: []\n" }))
                .await
                .unwrap();
        assert!(out.contains("run-0001"), "got {out}");

        let (head, body) = server.await.unwrap();
        assert!(head.starts_with("POST /api/runs "), "got {head}");
        assert!(
            head.to_lowercase().contains("content-type: application/json"),
            "submit must be application/json or dagron-api answers 415; head: {head}"
        );
        let sent: Value = serde_json::from_str(&body).unwrap();
        assert_eq!(sent["yaml"], "name: w\ntasks: []\n");
    }

    #[test]
    fn a_token_over_plaintext_to_a_remote_host_is_flagged() {
        for (base, host) in [
            ("http://dagron-api:8080", "dagron-api"),
            ("http://10.0.0.5:8080", "10.0.0.5"),
            ("http://dagron.example.com/api", "dagron.example.com"),
            ("http://[2001:db8::1]:8080", "2001:db8::1"),
            // A hostname that merely *starts* with 127. is a remote host like any
            // other — a textual prefix test would have exempted it.
            ("http://127.0.0.1.example.com", "127.0.0.1.example.com"),
            ("http://127.example.com:8080", "127.example.com"),
        ] {
            assert_eq!(
                plaintext_remote_host(base).as_deref(),
                Some(host),
                "{base} should be flagged"
            );
        }
    }

    #[test]
    fn loopback_and_tls_are_not_flagged() {
        for base in [
            // Never leaves the box — the whole 127/8 block, not just 127.0.0.1.
            "http://localhost:8080",
            "http://LOCALHOST:8080",
            "http://127.0.0.1:8080",
            "http://127.2.3.4:8080",
            "http://[::1]:8080",
            // Encrypted.
            "https://dagron.example.com",
            "https://10.0.0.5:8080",
        ] {
            assert_eq!(plaintext_remote_host(base), None, "{base} should not be flagged");
        }
    }

    #[test]
    fn userinfo_never_reaches_the_message() {
        // The client accepts credentials in the URL. Reporting a leaked token by
        // printing a password would be its own disclosure.
        let host = plaintext_remote_host("http://user:hunter2@dagron.example.com:8080/x");
        assert_eq!(host.as_deref(), Some("dagron.example.com"));
        let err = plaintext_token_verdict(
            "http://user:hunter2@dagron.example.com:8080/x",
            true,
            false,
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("dagron.example.com"), "got {err}");
        assert!(!err.contains("hunter2"), "credentials leaked into the error: {err}");
    }

    #[test]
    fn a_plaintext_token_to_a_remote_host_refuses_to_start() {
        let err = plaintext_token_verdict("http://dagron-api:8080", true, false)
            .unwrap_err()
            .to_string();
        // The message has to name all three ways out, or it just blocks people.
        assert!(err.contains("https://"), "got {err}");
        assert!(err.contains("loopback"), "got {err}");
        assert!(err.contains("DAGRON_MCP_ALLOW_PLAINTEXT_TOKEN"), "got {err}");
    }

    #[test]
    fn the_opt_out_keeps_the_mesh_deployment_working() {
        // Plaintext to an in-cluster Service with the transport secured below us
        // is legitimate; the operator just has to say so once.
        assert!(plaintext_token_verdict("http://dagron-api:8080", true, true).is_ok());
    }

    #[test]
    fn safe_configurations_need_no_opt_out() {
        for (base, has_token) in [
            ("http://dagron-api:8080", false), // no token, nothing to leak
            ("https://dagron-api:8080", true), // encrypted
            ("http://127.0.0.1:8080", true),   // never leaves the box
            ("http://localhost:8080", true),
        ] {
            assert!(
                plaintext_token_verdict(base, has_token, false).is_ok(),
                "{base} (token={has_token}) should be allowed"
            );
        }
    }

    #[tokio::test]
    async fn unknown_method_errors() {
        let resp = handle(&client(), &json!({"jsonrpc":"2.0","id":9,"method":"bogus"}))
            .await
            .unwrap();
        assert_eq!(resp["error"]["code"], -32601);
    }

    #[tokio::test]
    async fn unknown_tool_is_reported_as_tool_error() {
        let resp = handle(
            &client(),
            &json!({"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"nope","arguments":{}}}),
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], true);
    }
}
