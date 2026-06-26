//! dagron MCP server core — Model Context Protocol over stdio.
//!
//! Exposes the dagron management API as MCP **tools** so an AI agent (an MCP
//! client) can submit, list, inspect, and cancel workflow runs and read task logs.
//! [`handle`] dispatches one JSON-RPC message; [`DagronClient`] is the thin
//! dagron-api HTTP adapter the tools call.

use anyhow::{Context, Result};
use serde_json::{json, Value};

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
        other => anyhow::bail!("unknown tool `{other}`"),
    }
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
        assert_eq!(tools.len(), 5);
        for t in tools {
            assert!(t["name"].is_string());
            assert!(t["description"].is_string());
            assert_eq!(t["inputSchema"]["type"], "object");
        }
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
