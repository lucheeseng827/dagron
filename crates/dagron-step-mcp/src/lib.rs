//! dagron MCP-tool step — a workflow task that calls a tool on an MCP server.
//!
//! The mirror image of the LLM step, and of the MCP *server* in
//! [`dagron_mcp`]. That server lets an agent drive dagron; this lets a dagron
//! DAG drive an agent's tools. Once a tool call is a task, it inherits
//! everything a task already has: retries with backoff, a timeout, captured
//! output, artifacts between steps, an approval gate in front of it, and a place
//! in the run's history — none of which an in-process tool call has.
//!
//! **Transport is stdio**, which is the one transport every MCP server
//! supports: the server is spawned as a child process and spoken to in
//! newline-delimited JSON-RPC over its stdin/stdout, exactly as
//! `crates/dagron-mcp/src/main.rs` reads it. A task's `command` already spawns a
//! process, so a child is native here in a way an HTTP connection is not.
//!
//! Configured entirely by environment, like every other dagron step:
//!
//! | Variable | Meaning |
//! |---|---|
//! | `DAGRON_MCP_STEP_SERVER` | server program to spawn (required) |
//! | `DAGRON_MCP_STEP_SERVER_ARGS` | JSON array of its arguments |
//! | `DAGRON_MCP_STEP_TOOL` | tool name to call (required) |
//! | `DAGRON_MCP_STEP_ARGS` | JSON object of tool arguments (default `{}`) |
//! | `DAGRON_MCP_STEP_ARGS_FILE` | read them from a file instead (`-` = stdin) |
//! | `DAGRON_MCP_STEP_OUTPUT` | write the result here instead of stdout |
//! | `DAGRON_MCP_STEP_TIMEOUT_SECS` | whole-exchange deadline (default 300) |

use std::process::Stdio;

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

/// MCP protocol revision this client announces.
///
/// A separate constant from [`dagron_mcp::PROTOCOL_VERSION`] on purpose: a step
/// binary that runs inside every task image must not link the server crate — and
/// its HTTP stack — to read one string. Keep the two in step; a mismatch is a
/// handshake failure, which is loud rather than subtle.
pub const PROTOCOL_VERSION: &str = "2024-11-05";
pub const CLIENT_NAME: &str = "dagron-step-mcp";

/// Default whole-exchange deadline. Generous, because an MCP tool may itself be
/// doing real work; bounded, because a task that hangs forever is worse than one
/// that fails — the engine can retry a failure.
pub const DEFAULT_TIMEOUT_SECS: u64 = 300;

/// One resolved MCP-tool step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepConfig {
    /// The MCP server program to spawn.
    pub program: String,
    /// Its arguments, already split — see [`StepConfig::from_vars`].
    pub args: Vec<String>,
    /// The tool to call on it.
    pub tool: String,
    /// The tool's arguments. Always a JSON object; MCP defines it that way.
    pub arguments: Value,
    /// Where the result goes. `None` = stdout.
    pub output: Option<String>,
    pub timeout_secs: u64,
}

impl StepConfig {
    /// Read the step's configuration from the process environment.
    pub fn from_env(arguments: &str) -> Result<Self> {
        Self::from_vars(|k| std::env::var(k).ok(), arguments)
    }

    /// [`Self::from_env`] with the variable lookup injected, so the parsing
    /// rules below are testable without mutating a process-wide environment
    /// that every other test in the binary shares.
    pub fn from_vars(get: impl Fn(&str) -> Option<String>, arguments: &str) -> Result<Self> {
        let req = |k: &str| -> Result<String> {
            get(k)
                .map(|v| v.trim().to_string())
                .filter(|v| !v.is_empty())
                .with_context(|| format!("{k} must be set"))
        };

        let program = req("DAGRON_MCP_STEP_SERVER")?;
        let tool = req("DAGRON_MCP_STEP_TOOL")?;

        // A JSON array rather than a whitespace-split string, deliberately. A
        // split breaks silently on any argument containing a space — a path, a
        // JSON literal, a prompt — and handing the whole line to a shell would
        // make a workflow parameter injectable into a command line. The list is
        // a list, so it is written as one.
        let args = match get("DAGRON_MCP_STEP_SERVER_ARGS").filter(|v| !v.trim().is_empty()) {
            None => Vec::new(),
            Some(raw) => {
                let v: Value = serde_json::from_str(&raw)
                    .context("DAGRON_MCP_STEP_SERVER_ARGS must be a JSON array of strings")?;
                let arr = v.as_array().context(
                    "DAGRON_MCP_STEP_SERVER_ARGS must be a JSON array of strings, e.g. [\"-y\",\"pkg\"]",
                )?;
                arr.iter()
                    .map(|e| {
                        e.as_str().map(str::to_string).context(
                            "every DAGRON_MCP_STEP_SERVER_ARGS entry must be a string",
                        )
                    })
                    .collect::<Result<Vec<_>>>()?
            }
        };

        let arguments = parse_tool_arguments(arguments)?;

        let timeout_secs = parse_timeout_secs(get("DAGRON_MCP_STEP_TIMEOUT_SECS"))?;

        Ok(Self {
            program,
            args,
            tool,
            arguments,
            output: get("DAGRON_MCP_STEP_OUTPUT").filter(|v| !v.trim().is_empty()),
            timeout_secs,
        })
    }
}

/// Parse the tool arguments. Empty means "no arguments", not "invalid".
///
/// MCP defines `arguments` as an object, so a bare array or scalar is rejected
/// here rather than forwarded for the server to reject less helpfully.
pub fn parse_tool_arguments(raw: &str) -> Result<Value> {
    if raw.trim().is_empty() {
        return Ok(json!({}));
    }
    let v: Value = serde_json::from_str(raw).context("tool arguments must be valid JSON")?;
    if !v.is_object() {
        bail!("tool arguments must be a JSON object, got {}", kind_of(&v));
    }
    Ok(v)
}

fn kind_of(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "a boolean",
        Value::Number(_) => "a number",
        Value::String(_) => "a string",
        Value::Array(_) => "an array",
        Value::Object(_) => "an object",
    }
}

/// Parse `DAGRON_MCP_STEP_TIMEOUT_SECS`: unset is the default, `0` is rejected
/// rather than read as "no timeout".
///
/// Shared with `main`, which must establish the deadline *before* it reads the
/// tool arguments (a stdin/FIFO read can block), so the value it uses to bound
/// that read is the same one the config carries into the tool call.
pub fn parse_timeout_secs(raw: Option<String>) -> Result<u64> {
    match raw {
        None => Ok(DEFAULT_TIMEOUT_SECS),
        Some(raw) => {
            let n: u64 = raw
                .trim()
                .parse()
                .context("DAGRON_MCP_STEP_TIMEOUT_SECS must be a positive whole number")?;
            if n == 0 {
                bail!("DAGRON_MCP_STEP_TIMEOUT_SECS must be >= 1 (or unset)");
            }
            Ok(n)
        }
    }
}

/// What a tool call produced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ToolResult {
    /// The result rendered for a downstream task to consume.
    pub text: String,
    /// The server set `isError`. The call completed; the *tool* failed.
    pub is_error: bool,
}

/// Render an MCP `tools/call` result into the string a task writes.
///
/// All-text content is joined and returned as plain text, because that is what
/// makes `{{ tasks.<name>.output }}` usable in the next task without a JSON
/// step in between — the overwhelmingly common case, and the one worth making
/// ergonomic.
///
/// Anything else (an image, a resource, a mixed list) is returned as the JSON of
/// the `content` array. Flattening those to text would silently drop the parts
/// that are not text, and a step that quietly discards half its result is worse
/// than one that hands over a shape the author has to look at.
pub fn render_result(result: &Value) -> ToolResult {
    let is_error = result.get("isError").and_then(Value::as_bool).unwrap_or(false);
    let content = result.get("content").and_then(Value::as_array);

    let text = match content {
        // No `content` at all: give back the whole result rather than an empty
        // string, so a server that answers in some other shape is visible
        // instead of looking like a tool that returned nothing.
        None => result.to_string(),
        Some(parts) if parts.is_empty() => String::new(),
        Some(parts) => {
            let all_text = parts
                .iter()
                .all(|p| p.get("type").and_then(Value::as_str) == Some("text"));
            if all_text {
                parts
                    .iter()
                    .map(|p| p.get("text").and_then(Value::as_str).unwrap_or_default())
                    .collect::<Vec<_>>()
                    .join("\n")
            } else {
                Value::Array(parts.clone()).to_string()
            }
        }
    };
    ToolResult { text, is_error }
}

/// A live stdio JSON-RPC session with an MCP server.
pub struct Session {
    child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
    next_id: i64,
}

impl Session {
    /// Spawn the server and take its stdio pipes.
    ///
    /// **stderr is inherited, not piped.** It is where a well-behaved MCP server
    /// puts its diagnostics (dagron's own does), and inheriting it lands them in
    /// the task's captured output — so when a tool call fails, the reason is in
    /// the run's logs rather than discarded with the child.
    pub async fn spawn(cfg: &StepConfig) -> Result<Self> {
        let mut child = Command::new(&cfg.program)
            .args(&cfg.args)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .with_context(|| format!("could not start the MCP server {:?}", cfg.program))?;

        let stdin = child.stdin.take().context("the MCP server has no stdin")?;
        let stdout = child.stdout.take().context("the MCP server has no stdout")?;
        Ok(Self { child, stdin, stdout: BufReader::new(stdout).lines(), next_id: 1 })
    }

    /// Send a request and read until its answer arrives.
    ///
    /// Lines that are not this request's response are skipped: a server may emit
    /// notifications (progress, log messages) between a request and its reply,
    /// and they carry no `id` to match. Matching on `id` rather than taking the
    /// next line is what makes that safe.
    async fn request(&mut self, method: &str, params: Value) -> Result<Value> {
        let id = self.next_id;
        self.next_id += 1;
        self.send(&json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params }))
            .await?;

        loop {
            let line = self
                .stdout
                .next_line()
                .await
                .context("reading from the MCP server failed")?
                .with_context(|| {
                    format!("the MCP server closed its output before answering {method:?}")
                })?;
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let msg: Value = match serde_json::from_str(line) {
                Ok(v) => v,
                Err(e) => {
                    // stdout is the protocol channel, so this is the server
                    // misbehaving — but a stray print is not worth failing a
                    // task over when the reply may still be coming.
                    tracing::warn!(error = %e, "ignoring a non-JSON line from the MCP server");
                    continue;
                }
            };
            if msg.get("id").and_then(Value::as_i64) != Some(id) {
                continue;
            }
            if let Some(err) = msg.get("error") {
                let code = err.get("code").and_then(Value::as_i64).unwrap_or(0);
                let message =
                    err.get("message").and_then(Value::as_str).unwrap_or("unknown error");
                bail!("the MCP server rejected {method:?}: {message} (code {code})");
            }
            // JSON-RPC 2.0 (and MCP) require a response to carry exactly one of
            // `result` or `error`. A matched id with neither is malformed — fail
            // rather than return `Null`, which would read downstream as a tool
            // that succeeded and produced nothing.
            return match msg.get("result") {
                Some(result) => Ok(result.clone()),
                None => bail!(
                    "the MCP server's response to {method:?} carried neither a result nor an error"
                ),
            };
        }
    }

    async fn notify(&mut self, method: &str, params: Value) -> Result<()> {
        self.send(&json!({ "jsonrpc": "2.0", "method": method, "params": params })).await
    }

    async fn send(&mut self, msg: &Value) -> Result<()> {
        let mut line = serde_json::to_string(msg)?;
        line.push('\n');
        self.stdin
            .write_all(line.as_bytes())
            .await
            .context("writing to the MCP server failed")?;
        self.stdin.flush().await.context("flushing to the MCP server failed")
    }

    /// The MCP handshake: `initialize`, then the `initialized` notification.
    ///
    /// The notification is not optional politeness — a spec-compliant server may
    /// refuse tool calls until it arrives.
    pub async fn initialize(&mut self) -> Result<Value> {
        let result = self
            .request(
                "initialize",
                json!({
                    "protocolVersion": PROTOCOL_VERSION,
                    "capabilities": {},
                    "clientInfo": { "name": CLIENT_NAME, "version": env!("CARGO_PKG_VERSION") },
                }),
            )
            .await?;
        self.notify("notifications/initialized", json!({})).await?;
        Ok(result)
    }

    /// Call one tool.
    pub async fn call_tool(&mut self, name: &str, arguments: &Value) -> Result<ToolResult> {
        let result = self
            .request("tools/call", json!({ "name": name, "arguments": arguments }))
            .await?;
        Ok(render_result(&result))
    }

    /// Close stdin and reap the child.
    ///
    /// Dropping stdin is what tells a stdio server to exit: its read loop sees
    /// EOF. `kill_on_drop` is the backstop for a server that ignores that, so a
    /// finished task cannot leave a process behind in the runner.
    pub async fn shutdown(mut self) {
        drop(self.stdin);
        if let Err(e) = self.child.kill().await {
            tracing::debug!(error = %e, "the MCP server had already exited");
        }
    }
}

/// Run one step end to end: spawn, handshake, call, shut down — the handshake
/// and call bounded by `deadline`.
///
/// The timeout lives here, not in the caller, so the child is **always** shut
/// down. A caller that wrapped `run_step` in `timeout` would, on expiry, drop
/// this future mid-await and never reach `shutdown` — leaking the child to
/// tokio's best-effort reaping. Here the deadline expiring still falls through
/// to `session.shutdown()`.
pub async fn run_step(cfg: &StepConfig, deadline: tokio::time::Instant) -> Result<ToolResult> {
    let mut session = Session::spawn(cfg).await?;
    let outcome = tokio::time::timeout_at(deadline, async {
        let info = session.initialize().await?;
        let server = info
            .get("serverInfo")
            .and_then(|s| s.get("name"))
            .and_then(Value::as_str)
            .unwrap_or("unknown");
        let protocol = info.get("protocolVersion").and_then(Value::as_str).unwrap_or("unknown");
        tracing::info!(%server, %protocol, "MCP handshake complete");
        session.call_tool(&cfg.tool, &cfg.arguments).await
    })
    .await;
    session.shutdown().await;
    match outcome {
        Ok(result) => result,
        Err(_) => bail!("the MCP tool call did not finish before its deadline"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars<'a>(pairs: &'a [(&'a str, &'a str)]) -> impl Fn(&str) -> Option<String> + 'a {
        move |k| pairs.iter().find(|(n, _)| *n == k).map(|(_, v)| v.to_string())
    }

    #[test]
    fn a_minimal_config_needs_only_a_server_and_a_tool() {
        let cfg = StepConfig::from_vars(
            vars(&[("DAGRON_MCP_STEP_SERVER", "dagron-mcp"), ("DAGRON_MCP_STEP_TOOL", "ping")]),
            "",
        )
        .unwrap();
        assert_eq!(cfg.program, "dagron-mcp");
        assert_eq!(cfg.tool, "ping");
        assert!(cfg.args.is_empty());
        assert_eq!(cfg.arguments, json!({}), "no arguments is an empty object, not an error");
        assert_eq!(cfg.timeout_secs, DEFAULT_TIMEOUT_SECS);
        assert_eq!(cfg.output, None);
    }

    #[test]
    fn a_missing_server_or_tool_is_rejected_by_name() {
        let e = StepConfig::from_vars(vars(&[("DAGRON_MCP_STEP_TOOL", "t")]), "").unwrap_err();
        assert!(e.to_string().contains("DAGRON_MCP_STEP_SERVER"), "got: {e}");
        let e = StepConfig::from_vars(vars(&[("DAGRON_MCP_STEP_SERVER", "s")]), "").unwrap_err();
        assert!(e.to_string().contains("DAGRON_MCP_STEP_TOOL"), "got: {e}");
    }

    /// Server arguments are a JSON array, not a whitespace-split string. A split
    /// would break on any argument containing a space — which is most paths
    /// worth passing — and it would do so silently.
    #[test]
    fn server_arguments_keep_their_spaces() {
        let cfg = StepConfig::from_vars(
            vars(&[
                ("DAGRON_MCP_STEP_SERVER", "npx"),
                ("DAGRON_MCP_STEP_SERVER_ARGS", r#"["-y","@mcp/fs","/data/my files"]"#),
                ("DAGRON_MCP_STEP_TOOL", "read_file"),
            ]),
            "",
        )
        .unwrap();
        assert_eq!(cfg.args, vec!["-y", "@mcp/fs", "/data/my files"]);
    }

    #[test]
    fn malformed_server_arguments_are_rejected_with_the_shape_they_should_have() {
        let bad = |v: &str| {
            StepConfig::from_vars(
                vars(&[
                    ("DAGRON_MCP_STEP_SERVER", "s"),
                    ("DAGRON_MCP_STEP_TOOL", "t"),
                    ("DAGRON_MCP_STEP_SERVER_ARGS", v),
                ]),
                "",
            )
            .unwrap_err()
            .to_string()
        };
        assert!(bad("-y @mcp/fs").contains("JSON array"), "a bare string is not a list");
        assert!(bad(r#"{"a":1}"#).contains("JSON array"));
        assert!(bad("[1,2]").contains("string"), "entries must be strings");
    }

    #[test]
    fn tool_arguments_must_be_an_object() {
        assert_eq!(parse_tool_arguments("").unwrap(), json!({}));
        assert_eq!(parse_tool_arguments("   ").unwrap(), json!({}));
        assert_eq!(parse_tool_arguments(r#"{"path":"/x"}"#).unwrap(), json!({"path": "/x"}));
        let e = parse_tool_arguments("[1]").unwrap_err().to_string();
        assert!(e.contains("an array"), "the message should name what was given: {e}");
        assert!(parse_tool_arguments("not json").is_err());
    }

    #[test]
    fn a_zero_timeout_is_rejected_rather_than_meaning_forever() {
        let e = StepConfig::from_vars(
            vars(&[
                ("DAGRON_MCP_STEP_SERVER", "s"),
                ("DAGRON_MCP_STEP_TOOL", "t"),
                ("DAGRON_MCP_STEP_TIMEOUT_SECS", "0"),
            ]),
            "",
        )
        .unwrap_err()
        .to_string();
        assert!(e.contains(">= 1"), "got: {e}");
    }

    /// The common case, and the one worth making ergonomic: all-text content
    /// becomes plain text, so the next task can use `{{ tasks.x.output }}`
    /// directly instead of parsing JSON to get at a string.
    #[test]
    fn all_text_content_is_returned_as_plain_text() {
        let r = render_result(&json!({
            "content": [{"type": "text", "text": "line one"}, {"type": "text", "text": "line two"}],
        }));
        assert_eq!(r.text, "line one\nline two");
        assert!(!r.is_error);
    }

    /// Mixed content keeps its JSON. Flattening it to the text parts would
    /// silently drop the image, and a step that discards half its result is
    /// worse than one that hands over a shape the author has to look at.
    #[test]
    fn mixed_content_keeps_its_structure_rather_than_losing_the_non_text_parts() {
        let r = render_result(&json!({
            "content": [
                {"type": "text", "text": "here it is"},
                {"type": "image", "data": "iVBOR", "mimeType": "image/png"},
            ],
        }));
        assert!(r.text.contains("iVBOR"), "the image survives: {}", r.text);
        assert!(r.text.contains("here it is"));
        assert!(r.text.starts_with('['), "rendered as the content array");
    }

    #[test]
    fn is_error_is_carried_through() {
        let r = render_result(&json!({
            "content": [{"type": "text", "text": "no such file"}],
            "isError": true,
        }));
        assert!(r.is_error);
        assert_eq!(r.text, "no such file");
    }

    /// A result with no `content` is a server answering in a shape MCP does not
    /// define. Returning the whole thing makes that visible; returning "" would
    /// look like a tool that succeeded and produced nothing.
    #[test]
    fn a_result_without_content_is_shown_whole_rather_than_as_nothing() {
        let r = render_result(&json!({ "unexpected": 1 }));
        assert!(r.text.contains("unexpected"));
        assert!(!r.is_error);
    }

    #[test]
    fn empty_content_is_an_empty_result_not_a_json_literal() {
        let r = render_result(&json!({ "content": [] }));
        assert_eq!(r.text, "");
    }
}
