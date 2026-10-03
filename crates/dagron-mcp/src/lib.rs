//! dagron MCP server core — Model Context Protocol over stdio.
//!
//! Exposes the dagron management API as MCP **tools** so an AI agent (an MCP
//! client) can author, run, recover and inspect workflows without a human
//! dropping to `curl`. The catalogue spans the whole loop: register and run a
//! named workflow, submit ad-hoc YAML (idempotently), wait on a run, read its
//! logs and artifacts, resolve an approval gate, rerun or redrive a failure, and
//! write down what it concluded — plus the cluster-internal signals
//! (`dagron_get_metrics`, `dagron_get_health`, `dagron_list_dead_letters`,
//! `dagron_get_run_events`) that let it reason about what the engine is doing,
//! not just send commands.
//!
//! [`handle`] dispatches one JSON-RPC message; [`DagronClient`] is the thin
//! dagron-api HTTP adapter the tools call; [`tools`] owns the catalogue and its
//! dispatch.
//!
//! **Read-only mode.** P0/P1 turned this server from mostly-read into read-write
//! against a live scheduler. `DAGRON_MCP_READONLY=1` hides every mutating tool
//! from `tools/list` and refuses it on call, so the earlier safe-by-default
//! posture stays one env var away.

use anyhow::{Context, Result};
use serde_json::{json, Value};
use std::time::Duration;

mod tools;

pub use tools::{
    call_tool, call_tool_with_progress, tool_access, tool_defs, tool_defs_for,
    tool_defs_for_policy, Access, Effect,
};

/// How often a blocked `dagron_wait_run` re-reads the run to report progress.
/// The API re-checks a waited-on run at the same interval, so a progress poll
/// costs a waiter about what the server already pays for it.
const PROGRESS_POLL: Duration = Duration::from_secs(5);

/// Where `notifications/progress` go while a tool call blocks.
///
/// A trait rather than a channel so the stdio binary can write to its output
/// queue and a test can collect what was sent.
pub trait ProgressSink: Send + Sync {
    /// One notification: `progress` of `total`, for the request that carried
    /// `token`.
    fn send(&self, token: &Value, progress: u64, total: u64);
}

/// A sink that drops everything: what [`handle`] and [`call_tool`] use.
pub struct NoProgress;

impl ProgressSink for NoProgress {
    fn send(&self, _token: &Value, _progress: u64, _total: u64) {}
}

/// A request's progress token and where its notifications go. Only built when
/// the request carried a token: the token is the client's opt-in.
#[derive(Clone, Copy)]
pub struct Progress<'a> {
    pub token: &'a Value,
    pub sink: &'a dyn ProgressSink,
}

/// The `notifications/progress` message for one report.
pub fn progress_notification(token: &Value, progress: u64, total: u64) -> Value {
    json!({
        "jsonrpc": "2.0",
        "method": "notifications/progress",
        "params": { "progressToken": token, "progress": progress, "total": total }
    })
}

/// MCP protocol revision this server implements, and answers every `initialize`
/// with.
///
/// Still `2024-11-05`, although the tool `annotations` in `tools/list` arrived
/// in `2025-03-26`. That is deliberate. A `2024-11-05` tool definition does not
/// forbid extra fields, and a client that acts on annotations reads them from
/// the tool whatever revision was negotiated. One that honours them only under
/// a newer revision falls back to asking before every call, which is the
/// behaviour this server had before it sent any, and the safe direction.
///
/// Advertising a newer revision is not a one-line change. This constant is the
/// answer whatever revision the client asked for, so raising it would hand a
/// client that speaks only older ones a revision it cannot use. Doing it
/// properly means negotiating (echo the client's revision when it is one this
/// server speaks), and each revision claimed carries rules this server does
/// not meet yet: `2025-03-26` requires accepting a JSON-RPC batch, which the
/// stdio loop leaves unanswered today, and `2025-06-18` requires an HTTP
/// transport to refuse an unsupported `MCP-Protocol-Version` header with `400`.
/// That is its own change, with its own tests.
pub const PROTOCOL_VERSION: &str = "2024-11-05";
pub const SERVER_NAME: &str = "dagron-mcp";

/// Inline cap for [`DagronClient::max_artifact_bytes`], overridable with
/// `DAGRON_MCP_MAX_ARTIFACT_BYTES`. Matches the SSE window's cap: comfortably
/// past a log file or a JSON result, far short of a context window.
const DEFAULT_MAX_ARTIFACT_BYTES: usize = 256 * 1024;

/// HTTP method, so one [`DagronClient::request`] serves every tool.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Method {
    Get,
    Post,
    Put,
    Delete,
}

/// One dagron-api response, status intact.
///
/// The status is carried rather than collapsed into `Result` because a `409`
/// ("that workflow is paused") is an answer the agent should reason about, not a
/// malfunction — see `tools::render`.
#[derive(Debug, Clone)]
pub struct ApiResponse {
    pub status: u16,
    pub body: String,
}

impl ApiResponse {
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }

    /// The body on 2xx, an error otherwise. The shape the pre-P0 tools wanted,
    /// and what [`DagronClient::get`] and [`DagronClient::post`] still give a
    /// composing server.
    pub fn into_success(self) -> Result<String> {
        if self.is_success() {
            Ok(self.body)
        } else {
            anyhow::bail!("dagron-api returned {}: {}", self.status, self.body)
        }
    }
}

/// A response whose body is bytes, not text — artifacts are arbitrary files.
#[derive(Debug, Clone)]
pub struct BytesResponse {
    pub status: u16,
    pub content_type: Option<String>,
    pub bytes: Vec<u8>,
}

impl BytesResponse {
    pub fn is_success(&self) -> bool {
        (200..300).contains(&self.status)
    }
}

/// Thin client for the dagron management API (`dagron-api`).
pub struct DagronClient {
    http: reqwest::Client,
    base: String,
    token: Option<String>,
    readonly: bool,
    allow_approve: bool,
    max_artifact_bytes: usize,
    progress_poll: Duration,
}

impl DagronClient {
    /// `DAGRON_API_URL` (default `http://localhost:8080`) + optional
    /// `DAGRON_MCP_TOKEN` (sent as `Authorization: Bearer …`),
    /// `DAGRON_MCP_READONLY` and `DAGRON_MCP_MAX_ARTIFACT_BYTES`.
    ///
    /// Fails when the token would cross the network in the clear — see
    /// [`plaintext_token_verdict`] for the reasoning and the opt-out.
    pub fn from_env() -> Result<Self> {
        let base = std::env::var("DAGRON_API_URL")
            .unwrap_or_else(|_| "http://localhost:8080".to_string());
        let token = std::env::var("DAGRON_MCP_TOKEN").ok().filter(|t| !t.is_empty());
        let opted_in = env_flag("DAGRON_MCP_ALLOW_PLAINTEXT_TOKEN");
        plaintext_token_verdict(&base, token.is_some(), opted_in)?;
        let max_artifact_bytes = std::env::var("DAGRON_MCP_MAX_ARTIFACT_BYTES")
            .ok()
            .and_then(|v| v.parse::<usize>().ok())
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_MAX_ARTIFACT_BYTES);
        Ok(Self {
            http: reqwest::Client::new(),
            base,
            token,
            readonly: env_flag("DAGRON_MCP_READONLY"),
            allow_approve: env_flag("DAGRON_MCP_ALLOW_APPROVE"),
            max_artifact_bytes,
            progress_poll: PROGRESS_POLL,
        })
    }

    /// A client against an explicit base URL. For tests and for a composing
    /// server that resolves its configuration some other way.
    pub fn new(base: impl Into<String>, token: Option<String>) -> Self {
        Self {
            http: reqwest::Client::new(),
            base: base.into(),
            token,
            readonly: false,
            allow_approve: false,
            max_artifact_bytes: DEFAULT_MAX_ARTIFACT_BYTES,
            progress_poll: PROGRESS_POLL,
        }
    }

    /// Re-read a waited-on run this often, so a test need not wait 5 s a tick.
    #[cfg(test)]
    pub(crate) fn with_progress_poll(mut self, every: Duration) -> Self {
        self.progress_poll = every;
        self
    }

    pub(crate) fn progress_poll(&self) -> Duration {
        self.progress_poll
    }

    /// Let this server approve gates (`DAGRON_MCP_ALLOW_APPROVE`). Off by
    /// default: approving is the one decision that should be a person's, and an
    /// agent that reads untrusted text (a plan, a log line) can be talked into it.
    pub fn with_allow_approve(mut self, allow: bool) -> Self {
        self.allow_approve = allow;
        self
    }

    /// Whether `dagron_approve_task` is advertised and honoured.
    pub fn allow_approve(&self) -> bool {
        self.allow_approve
    }

    /// Hide and refuse every mutating tool.
    pub fn with_readonly(mut self, readonly: bool) -> Self {
        self.readonly = readonly;
        self
    }

    /// Whether the write tools are hidden and refused.
    pub fn readonly(&self) -> bool {
        self.readonly
    }

    /// Largest artifact returned inline to the agent, in bytes.
    pub fn max_artifact_bytes(&self) -> usize {
        self.max_artifact_bytes
    }

    fn auth(&self, rb: reqwest::RequestBuilder) -> reqwest::RequestBuilder {
        match &self.token {
            Some(t) => rb.bearer_auth(t),
            None => rb,
        }
    }

    fn build(
        &self,
        method: Method,
        path: &str,
        payload: Option<(String, &'static str)>,
        headers: &[(&str, String)],
    ) -> reqwest::RequestBuilder {
        let url = format!("{}{path}", self.base);
        let mut rb = match method {
            Method::Get => self.http.get(url),
            Method::Post => self.http.post(url),
            Method::Put => self.http.put(url),
            Method::Delete => self.http.delete(url),
        };
        rb = self.auth(rb);
        for (name, value) in headers {
            rb = rb.header(*name, value);
        }
        if let Some((b, ct)) = payload {
            rb = rb.header("content-type", ct).body(b);
        }
        rb
    }

    /// One request against dagron-api, status preserved.
    ///
    /// Public so a composing server can add tools without re-implementing
    /// base-URL, auth and status handling.
    pub async fn request(
        &self,
        method: Method,
        path: &str,
        payload: Option<(String, &'static str)>,
        headers: &[(&str, String)],
    ) -> Result<ApiResponse> {
        let r = self
            .build(method, path, payload, headers)
            .send()
            .await
            .context("dagron-api request failed")?;
        let status = r.status().as_u16();
        // Propagate a failed body read rather than defaulting to "". A
        // connection that breaks after the status line would otherwise arrive
        // here as a 2xx with an empty body, which `tools::render` reports as
        // `{"status":200,"ok":true}` — telling the agent the call succeeded and
        // simply returned nothing. A transport failure has to stay a failure.
        let body = r.text().await.context("dagron-api response body read failed")?;
        Ok(ApiResponse { status, body })
    }

    /// [`DagronClient::request`] for a body that is not text — an artifact.
    pub async fn request_bytes(&self, path: &str) -> Result<BytesResponse> {
        let r = self
            .build(Method::Get, path, None, &[])
            .send()
            .await
            .context("dagron-api request failed")?;
        let status = r.status().as_u16();
        let content_type = r
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string);
        // Same reasoning as `request`, and it bites harder here: a truncated
        // download defaulting to empty would be described to the agent as a
        // complete zero-byte artifact.
        let bytes = r
            .bytes()
            .await
            .context("dagron-api artifact body read failed")?
            .to_vec();
        Ok(BytesResponse { status, content_type, bytes })
    }

    /// GET `path`, body on 2xx and an error otherwise.
    pub async fn get(&self, path: &str) -> Result<String> {
        self.request(Method::Get, path, None, &[]).await?.into_success()
    }

    /// POST `path` with an optional `(body, content-type)`, body on 2xx and an
    /// error otherwise.
    pub async fn post(&self, path: &str, body: Option<(String, &'static str)>) -> Result<String> {
        self.request(Method::Post, path, body, &[]).await?.into_success()
    }

    /// Bounded read of an SSE endpoint: open the connection, then pull chunks
    /// until `budget` elapses (or the server closes the stream early).
    /// Returns the raw text accumulated. The caller is responsible for parsing
    /// SSE framing — keeping the I/O primitive small means the same helper
    /// works for any future stream endpoint we expose.
    pub(crate) async fn read_sse(&self, path: &str, budget: Duration) -> Result<String> {
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

/// A boolean env var, in the one spelling set this crate accepts.
fn env_flag(name: &str) -> bool {
    std::env::var(name)
        .map(|v| matches!(v.as_str(), "1" | "true" | "yes"))
        .unwrap_or(false)
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

/// Handle one JSON-RPC message. Returns `Some(response)` for a request, `None` for
/// a notification (no `id`) that needs no reply.
pub async fn handle(client: &DagronClient, msg: &Value) -> Option<Value> {
    handle_with_progress(client, msg, &NoProgress).await
}

/// [`handle`], sending `notifications/progress` to `sink` while a tool call
/// blocks.
///
/// Only a `tools/call` whose `params._meta.progressToken` is a string or a
/// number gets any: the token is the client's opt-in, and it is echoed as
/// given. Today only `dagron_wait_run` reports progress; every other tool
/// ignores the token, which the protocol allows. Nothing is sent after the
/// response: the caller writes the returned response after every
/// notification this call made.
pub async fn handle_with_progress(
    client: &DagronClient,
    msg: &Value,
    sink: &dyn ProgressSink,
) -> Option<Value> {
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
        "tools/list" => Some(ok(id, json!({ "tools": tool_defs_for_policy(client.readonly(), client.allow_approve()) }))),
        "tools/call" => {
            let params = msg.get("params").cloned().unwrap_or(Value::Null);
            let name = params.get("name").and_then(|n| n.as_str()).unwrap_or("");
            let args = params.get("arguments").cloned().unwrap_or(json!({}));
            let token = params
                .pointer("/_meta/progressToken")
                .filter(|t| t.is_string() || t.is_number());
            let progress = token.map(|token| Progress { token, sink });
            let (text, is_error) =
                match call_tool_with_progress(client, name, &args, progress).await {
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
        DagronClient::new("http://unused.test", None)
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
        // The pre-P0 surface: CRUD plus the cluster-internal triple — metrics,
        // dead letters, and the bounded SSE event read — that lets the AI agent
        // observe the engine, not just drive it.
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
            // What a client decides approval from, on the wire where it reads it.
            assert!(t["annotations"]["readOnlyHint"].is_boolean(), "{} has no readOnlyHint", t["name"]);
        }
        // The one a client may run unasked, and the one it must not.
        let hint = |name: &str| {
            tools.iter().find(|t| t["name"] == name).expect("tool present")["annotations"].clone()
        };
        assert_eq!(hint("dagron_list_runs")["readOnlyHint"], true);
        assert_eq!(hint("dagron_cancel_run")["readOnlyHint"], false);
        assert_eq!(hint("dagron_cancel_run")["destructiveHint"], true);
    }

    #[tokio::test]
    async fn readonly_hides_every_write_tool() {
        let ro = client().with_readonly(true);
        let resp = handle(&ro, &json!({"jsonrpc":"2.0","id":2,"method":"tools/list"}))
            .await
            .unwrap();
        let names: Vec<String> = resp["result"]["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|t| t["name"].as_str().unwrap().to_string())
            .collect();
        for hidden in [
            "dagron_submit_run",
            "dagron_cancel_run",
            "dagron_create_workflow",
            "dagron_delete_workflow",
            "dagron_approve_task",
            "dagron_put_artifact",
        ] {
            assert!(!names.contains(&hidden.to_string()), "{hidden} must be hidden read-only");
        }
        // Reads still work, or the mode would be useless.
        for shown in ["dagron_list_runs", "dagron_get_run_logs", "dagron_get_health"] {
            assert!(names.contains(&shown.to_string()), "{shown} must stay available");
        }
        // And everything left says so: a read-only server is one whose whole
        // catalogue a client may run without asking anyone.
        for t in resp["result"]["tools"].as_array().unwrap() {
            assert_eq!(t["annotations"]["readOnlyHint"], true, "{} is not read-only", t["name"]);
        }
    }

    #[tokio::test]
    async fn readonly_refuses_a_write_tool_even_if_it_was_advertised() {
        // Fail closed: a composing server that advertises the full catalogue
        // must not be able to smuggle a write past the switch.
        let ro = client().with_readonly(true);
        let resp = handle(
            &ro,
            &json!({"jsonrpc":"2.0","id":8,"method":"tools/call",
                    "params":{"name":"dagron_cancel_run","arguments":{"run_id":"abc"}}}),
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], true);
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("DAGRON_MCP_READONLY"), "got {text:?}");
    }

    #[tokio::test]
    async fn approving_is_off_until_asked_for_but_rejecting_is_not() {
        async fn listed(c: &DagronClient) -> Vec<String> {
            let resp = handle(c, &json!({"jsonrpc":"2.0","id":1,"method":"tools/list"})).await.unwrap();
            resp["result"]["tools"]
                .as_array()
                .unwrap()
                .iter()
                .map(|t| t["name"].as_str().unwrap().to_string())
                .collect()
        }
        let default = client();
        let names = listed(&default).await;
        assert!(!names.contains(&"dagron_approve_task".to_string()));
        assert!(names.contains(&"dagron_reject_task".to_string()), "refusing stays available");
        assert!(names.contains(&"dagron_list_approvals".to_string()));

        // Advertised or not, a call is refused: the switch fails closed.
        let resp = handle(
            &default,
            &json!({"jsonrpc":"2.0","id":2,"method":"tools/call",
                    "params":{"name":"dagron_approve_task","arguments":{"run_id":"r","task_id":"t"}}}),
        )
        .await
        .unwrap();
        assert_eq!(resp["result"]["isError"], true);
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(text.contains("DAGRON_MCP_ALLOW_APPROVE"), "got {text:?}");

        let allowed = client().with_allow_approve(true);
        assert!(listed(&allowed).await.contains(&"dagron_approve_task".to_string()));
        // Read-only still wins over the opt-in.
        let ro = client().with_allow_approve(true).with_readonly(true);
        assert!(!listed(&ro).await.contains(&"dagron_approve_task".to_string()));
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

    // ── Progress ─────────────────────────────────────────────────────────────

    /// Records every notification it is handed.
    #[derive(Default)]
    struct Collect(std::sync::Mutex<Vec<(Value, u64, u64)>>);

    impl ProgressSink for Collect {
        fn send(&self, token: &Value, progress: u64, total: u64) {
            self.0
                .lock()
                .unwrap()
                .push((token.clone(), progress, total));
        }
    }

    impl Collect {
        fn sent(&self) -> Vec<(Value, u64, u64)> {
            self.0.lock().unwrap().clone()
        }
    }

    const WAIT_BODY: &str = r#"{"run_id":"r1","status":"succeeded","finished":true}"#;

    /// What the stub answers one `GET /api/runs/r1` with.
    #[derive(Clone, Copy)]
    enum Detail {
        /// `(finished, total)` task counts.
        Counts(usize, usize),
        /// A 500.
        Fails,
        /// Nothing, ever: the connection stays open.
        Hangs,
    }
    use Detail::{Counts, Fails, Hangs};

    /// A stub dagron-api. `/wait` answers after `wait_for`. Each
    /// `GET /api/runs/r1` answers the next entry of `details`, the last one
    /// repeating.
    async fn stub_api(wait_for: Duration, details: Vec<Detail>) -> String {
        use std::sync::atomic::{AtomicUsize, Ordering};
        use std::sync::Arc;
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let details = Arc::new(details);
        let next = Arc::new(AtomicUsize::new(0));
        tokio::spawn(async move {
            loop {
                let Ok((mut sock, _)) = listener.accept().await else {
                    return;
                };
                let (details, next) = (details.clone(), next.clone());
                tokio::spawn(async move {
                    let mut raw = Vec::new();
                    let mut buf = [0u8; 1024];
                    while !raw.windows(4).any(|w| w == b"\r\n\r\n") {
                        match sock.read(&mut buf).await {
                            Ok(0) | Err(_) => return,
                            Ok(n) => raw.extend_from_slice(&buf[..n]),
                        }
                    }
                    let head = String::from_utf8_lossy(&raw).into_owned();
                    let path = head.split_whitespace().nth(1).unwrap_or("").to_string();
                    let (status, body) = if path.contains("/wait") {
                        tokio::time::sleep(wait_for).await;
                        ("200 OK", WAIT_BODY.to_string())
                    } else {
                        let i = next.fetch_add(1, Ordering::SeqCst).min(details.len() - 1);
                        match details[i] {
                            Counts(finished, total) => {
                                let tasks: Vec<Value> = (0..total)
                                    .map(|n| json!({ "status": if n < finished { "succeeded" } else { "running" } }))
                                    .collect();
                                ("200 OK", json!({ "id": "r1", "tasks": tasks }).to_string())
                            }
                            Fails => ("500 Internal Server Error", "boom".to_string()),
                            Hangs => {
                                tokio::time::sleep(Duration::from_secs(3600)).await;
                                return;
                            }
                        }
                    };
                    let resp = format!(
                        "HTTP/1.1 {status}\r\ncontent-type: application/json\r\n\
                         content-length: {}\r\nconnection: close\r\n\r\n{body}",
                        body.len()
                    );
                    let _ = sock.write_all(resp.as_bytes()).await;
                });
            }
        });
        base
    }

    fn wait_call(token: Option<Value>) -> Value {
        let mut msg = json!({"jsonrpc":"2.0","id":5,"method":"tools/call",
            "params":{"name":"dagron_wait_run","arguments":{"run_id":"r1","timeout_secs":5}}});
        if let Some(t) = token {
            msg["params"]["_meta"] = json!({ "progressToken": t });
        }
        msg
    }

    fn fast(base: String) -> DagronClient {
        DagronClient::new(base, None).with_progress_poll(Duration::from_millis(50))
    }

    #[tokio::test]
    async fn a_blocked_wait_reports_rising_progress_under_the_callers_token() {
        let base = stub_api(
            Duration::from_millis(600),
            vec![Counts(1, 4), Counts(1, 4), Counts(2, 4), Counts(3, 5)],
        )
        .await;
        let sink = Collect::default();
        let resp = handle_with_progress(&fast(base.clone()), &wait_call(Some(json!("tok"))), &sink)
            .await
            .unwrap();
        let sent = sink.sent();
        // An unchanged count sends nothing, and `total` grows with the run.
        let counts: Vec<(u64, u64)> = sent.iter().map(|(_, p, t)| (*p, *t)).collect();
        assert_eq!(counts, vec![(1, 4), (2, 4), (3, 5)]);
        assert!(
            sent.iter().all(|(token, _, _)| token == "tok"),
            "token echoed as given: {sent:?}"
        );

        // The answer itself is the one a call without a token gets.
        let plain = handle(&fast(base), &wait_call(None)).await.unwrap();
        assert_eq!(resp, plain);
        assert_eq!(resp["result"]["isError"], false);
    }

    #[tokio::test]
    async fn without_a_token_nothing_is_sent() {
        let base = stub_api(Duration::from_millis(300), vec![Counts(1, 2)]).await;
        let sink = Collect::default();
        let resp = handle_with_progress(&fast(base), &wait_call(None), &sink)
            .await
            .unwrap();
        assert!(
            sink.sent().is_empty(),
            "no token, no notifications: {:?}",
            sink.sent()
        );
        assert_eq!(resp["result"]["isError"], false);
    }

    #[tokio::test]
    async fn a_token_that_is_neither_string_nor_number_is_not_an_opt_in() {
        let base = stub_api(Duration::from_millis(300), vec![Counts(1, 2)]).await;
        let sink = Collect::default();
        handle_with_progress(&fast(base), &wait_call(Some(json!({ "x": 1 }))), &sink)
            .await
            .unwrap();
        assert!(sink.sent().is_empty(), "{:?}", sink.sent());
    }

    #[tokio::test]
    async fn a_numeric_token_round_trips_as_a_number() {
        let base = stub_api(Duration::from_millis(300), vec![Counts(1, 2)]).await;
        let sink = Collect::default();
        handle_with_progress(&fast(base), &wait_call(Some(json!(7))), &sink)
            .await
            .unwrap();
        let sent = sink.sent();
        assert!(!sent.is_empty());
        assert!(
            sent.iter().all(|(token, _, _)| *token == json!(7)),
            "{sent:?}"
        );
    }

    #[tokio::test]
    async fn a_failing_progress_read_does_not_fail_the_wait() {
        let base = stub_api(Duration::from_millis(300), vec![Fails]).await;
        let sink = Collect::default();
        let resp = handle_with_progress(&fast(base), &wait_call(Some(json!("tok"))), &sink)
            .await
            .unwrap();
        assert!(sink.sent().is_empty());
        assert_eq!(resp["result"]["isError"], false, "{resp}");
        let text = resp["result"]["content"][0]["text"].as_str().unwrap();
        assert!(
            text.contains("succeeded"),
            "the wait's own answer comes back: {text}"
        );
    }

    #[tokio::test]
    async fn a_progress_read_that_never_answers_does_not_hold_back_the_wait() {
        // Every read hangs. The wait answers at 300 ms, and that answer must
        // come back then: a read in flight is not something the wait waits on.
        let base = stub_api(Duration::from_millis(300), vec![Hangs]).await;
        let sink = Collect::default();
        let started = std::time::Instant::now();
        let resp = tokio::time::timeout(
            Duration::from_secs(5),
            handle_with_progress(&fast(base), &wait_call(Some(json!("tok"))), &sink),
        )
        .await
        .expect("a hung progress read held back the wait's answer")
        .unwrap();
        assert!(
            started.elapsed() < Duration::from_millis(1500),
            "answered after {:?}",
            started.elapsed()
        );
        assert_eq!(resp["result"]["isError"], false, "{resp}");
        assert!(sink.sent().is_empty());
    }

    /// A composing server runs `handle` inside its own handlers, and axum
    /// takes only `Send` futures. This fails to compile if one stops being.
    #[test]
    fn the_handler_futures_are_send() {
        fn assert_send<T: Send>(_: T) {}
        let c = client();
        let msg = wait_call(Some(json!("tok")));
        let args = json!({ "run_id": "r1" });
        assert_send(handle(&c, &msg));
        assert_send(handle_with_progress(&c, &msg, &NoProgress));
        assert_send(call_tool(&c, "dagron_wait_run", &args));
        assert_send(call_tool_with_progress(
            &c,
            "dagron_wait_run",
            &args,
            Some(Progress { token: &msg, sink: &NoProgress }),
        ));
    }

    #[test]
    fn the_notification_has_the_progress_shape() {
        let n = progress_notification(&json!("abc"), 7, 12);
        assert_eq!(
            n,
            json!({"jsonrpc":"2.0","method":"notifications/progress",
                   "params":{"progressToken":"abc","progress":7,"total":12}})
        );
        assert!(n.get("id").is_none(), "a notification carries no id");
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
}
