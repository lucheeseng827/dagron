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
    pub fn from_env() -> Self {
        Self {
            http: reqwest::Client::new(),
            base: std::env::var("DAGRON_API_URL")
                .unwrap_or_else(|_| "http://localhost:8080".to_string()),
            token: std::env::var("DAGRON_MCP_TOKEN").ok().filter(|t| !t.is_empty()),
        }
    }

    fn auth(&self, rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.token {
            Some(t) => rb.bearer_auth(t),
            None => rb,
        }
    }

    async fn get(&self, path: &str) -> Result<String> {
        let r = self
            .auth(self.http.get(format!("{}{path}", self.base)))
            .send()
            .await
            .context("dagron-api request failed")?;
        body_or_status(r).await
    }

    async fn post(&self, path: &str, body: Option<(String, &'static str)>) -> Result<String> {
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

async fn body_or_status(r: reqwest::Response) -> Result<String> {
    let status = r.status();
    let text = r.text().await.unwrap_or_default();
    if status.is_success() {
        Ok(text)
    } else {
        anyhow::bail!("dagron-api returned {status}: {text}")
    }
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
            "description": "Read a task's captured logs/output within a run.",
            "inputSchema": {
                "type": "object",
                "properties": { "run_id": { "type": "string" }, "task_id": { "type": "string" } },
                "required": ["run_id", "task_id"], "additionalProperties": false
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
            client
                .post("/api/runs", Some((s("yaml")?, "application/yaml")))
                .await
        }
        "dagron_cancel_run" => {
            client
                .post(&format!("/api/runs/{}/cancel", safe_id("run_id")?), None)
                .await
        }
        "dagron_get_task_logs" => {
            client
                .get(&format!(
                    "/api/runs/{}/tasks/{}/logs",
                    safe_id("run_id")?,
                    safe_id("task_id")?
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
