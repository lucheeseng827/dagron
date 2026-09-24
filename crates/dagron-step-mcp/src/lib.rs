//! dagron MCP-tool step — a workflow task that calls a tool on an MCP server.
//!
//! The mirror image of the LLM step, and of the MCP *server* in
//! [`dagron_mcp`]. That server lets an agent drive dagron; this lets a dagron
//! DAG drive an agent's tools. Once a tool call is a task, it inherits
//! everything a task already has: retries with backoff, a timeout, captured
//! output, artifacts between steps, an approval gate in front of it, and a place
//! in the run's history — none of which an in-process tool call has.
//!
//! **Two targets.** By default the server is spawned as a child and spoken to
//! over stdio — the one transport every MCP server supports. Set
//! `DAGRON_MCP_STEP_GATEWAY` and the step instead POSTs the same JSON-RPC to a
//! gateway over Streamable HTTP, which is how a DAG's tool calls inherit a
//! governance layer: the allow-list, the argument policy, the tool-definition
//! pin and the audit ledger all apply, for one environment variable and no
//! change to the workflow. The two are mutually exclusive by construction (see
//! [`Target`]) because a step either spawns a server or calls one, never both.
//!
//! The stdio path, unchanged: the server is spawned as a child process and spoken to in
//! newline-delimited JSON-RPC over its stdin/stdout, exactly as
//! `crates/dagron-mcp/src/main.rs` reads it. A task's `command` already spawns a
//! process, so a child is native here in a way an HTTP connection is not.
//!
//! Configured entirely by environment, like every other dagron step:
//!
//! | Variable | Meaning |
//! |---|---|
//! | `DAGRON_MCP_STEP_SERVER` | server program to spawn (required, unless a gateway is set) |
//! | `DAGRON_MCP_STEP_GATEWAY` | an MCP gateway to call instead of spawning a server |
//! | `DAGRON_MCP_STEP_GATEWAY_TOKEN` | bearer token, if the gateway requires auth |
//! | `DAGRON_MCP_STEP_SERVER_ARGS` | JSON array of its arguments |
//! | `DAGRON_MCP_STEP_TOOL` | tool name to call (required) |
//! | `DAGRON_MCP_STEP_ARGS` | JSON object of tool arguments (default `{}`) |
//! | `DAGRON_MCP_STEP_ARGS_FILE` | read them from a file instead (`-` = stdin) |
//! | `DAGRON_MCP_STEP_OUTPUT` | write the result here instead of stdout |
//! | `DAGRON_MCP_STEP_TIMEOUT_SECS` | whole-exchange deadline (default 300) |

pub mod gateway;

use std::process::Stdio;

use anyhow::{bail, Context, Result};
use serde_json::{json, Value};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

use crate::gateway::{Gateway, GatewaySession};

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

/// Where a step's tool call goes.
///
/// An enum rather than an optional gateway beside an optional program, so the
/// invalid state — both set, or neither — cannot be constructed. `from_vars`
/// resolves exactly one.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Target {
    /// Spawn the server as a child and speak stdio JSON-RPC to it.
    Stdio {
        /// The MCP server program to spawn.
        program: String,
        /// Its arguments, already split — see [`StepConfig::from_vars`].
        args: Vec<String>,
    },
    /// POST JSON-RPC to a gateway, which routes to the real server and governs
    /// the call on the way through.
    Gateway(Gateway),
}

/// One resolved MCP-tool step.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StepConfig {
    /// Where the call goes: a spawned server, or a gateway.
    pub target: Target,
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

        let tool = req("DAGRON_MCP_STEP_TOOL")?;

        // A gateway wins over a server program, and is not an error alongside
        // one: a workflow that already names its server should be able to gain
        // governance by setting one variable on the task, without editing the
        // step it has been running. The program is then unused, and saying so
        // once is friendlier than refusing to start.
        let gateway = get("DAGRON_MCP_STEP_GATEWAY").filter(|v| !v.trim().is_empty());
        if let Some(raw) = gateway {
            if get("DAGRON_MCP_STEP_SERVER").is_some_and(|p| !p.trim().is_empty()) {
                tracing::info!(
                    "DAGRON_MCP_STEP_GATEWAY is set, so DAGRON_MCP_STEP_SERVER is ignored;                      the gateway routes to the server"
                );
            }
            return Ok(Self {
                target: Target::Gateway(Gateway::parse(
                    &raw,
                    get("DAGRON_MCP_STEP_GATEWAY_TOKEN"),
                )?),
                tool,
                arguments: parse_tool_arguments(arguments)?,
                output: get("DAGRON_MCP_STEP_OUTPUT").filter(|v| !v.trim().is_empty()),
                timeout_secs: parse_timeout_secs(get("DAGRON_MCP_STEP_TIMEOUT_SECS"))?,
            });
        }

        let program = req("DAGRON_MCP_STEP_SERVER")?;

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
            target: Target::Stdio { program, args },
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

/// A spawned MCP server and the pipe halves used to talk to it.
struct ChildIo {
    child: Child,
    stdin: ChildStdin,
    stdout: Lines<BufReader<ChildStdout>>,
}

/// How a [`Session`] carries JSON-RPC.
///
/// The stdio side is boxed: a `Child` plus two pipe halves is ~336 bytes against
/// the gateway session's ~128, and an unboxed enum would make every session —
/// gateway ones included — carry the larger. One allocation per session, once.
enum Transport {
    /// A spawned child, spoken to over its stdio pipes.
    Stdio(Box<ChildIo>),
    /// A gateway, one POST per message, holding whatever session state the
    /// gateway asked the client to carry between them.
    Gateway(GatewaySession),
}

/// A live JSON-RPC session with an MCP server, direct or via a gateway.
pub struct Session {
    transport: Transport,
    next_id: i64,
}

impl Session {
    /// Open the session against whichever target the config resolved.
    ///
    /// For stdio this spawns the server and takes its pipes. **stderr is
    /// inherited, not piped** — it is where a well-behaved MCP server puts its
    /// diagnostics (dagron's own does), and inheriting it lands them in the
    /// task's captured output, so when a tool call fails the reason is in the
    /// run's logs rather than discarded with the child.
    ///
    /// For a gateway there is nothing to open: Streamable HTTP is one
    /// request-response per POST, so the connection is made per message.
    pub async fn connect(cfg: &StepConfig) -> Result<Self> {
        let transport = match &cfg.target {
            Target::Gateway(gw) => Transport::Gateway(GatewaySession::new(gw.clone())),
            Target::Stdio { program, args } => {
                let mut child = Command::new(program)
                    .args(args)
                    .stdin(Stdio::piped())
                    .stdout(Stdio::piped())
                    .stderr(Stdio::inherit())
                    .kill_on_drop(true)
                    .spawn()
                    .with_context(|| format!("could not start the MCP server {program:?}"))?;

                let stdin = child.stdin.take().context("the MCP server has no stdin")?;
                let stdout = child.stdout.take().context("the MCP server has no stdout")?;
                Transport::Stdio(Box::new(ChildIo {
                    child,
                    stdin,
                    stdout: BufReader::new(stdout).lines(),
                }))
            }
        };
        Ok(Self { transport, next_id: 1 })
    }

    /// Send a request and return its result.
    ///
    /// Over stdio, lines that are not this request's response are skipped: a
    /// server may emit notifications (progress, log messages) between a request
    /// and its reply, and they carry no `id` to match. Matching on `id` rather
    /// than taking the next line is what makes that safe. Over HTTP the answer
    /// to a POST is the answer to that request — but a gateway may deliver it as
    /// an event stream carrying notifications first, so the same skipping
    /// happens one level down, in `gateway::sse_message`.
    async fn request(&mut self, method: &str, params: Value) -> Result<Value> {
        let id = self.next_id;
        self.next_id += 1;
        let msg = json!({ "jsonrpc": "2.0", "id": id, "method": method, "params": params });

        // Borrowed, not cloned: the session id a stateful gateway hands back at
        // `initialize` has to survive into the next request, and a clone per
        // message could never carry it.
        if let Transport::Gateway(gw) = &mut self.transport {
            let body = serde_json::to_string(&msg)?;
            let reply = gw
                .post(&body)
                .await?
                .with_context(|| format!("the MCP gateway accepted {method:?} without answering"))?;
            // One POST carries one request, so the answer is this request's by
            // construction — but a gateway that returns someone else's response
            // would otherwise surface as a baffling result downstream rather
            // than as the broken gateway it is.
            match reply.get("id").and_then(Value::as_i64) {
                Some(got) if got != id => bail!(
                    "the MCP gateway answered {method:?} (id {id}) with a response for id {got}"
                ),
                _ => {}
            }
            return interpret(&reply, method, "gateway");
        }

        self.send(&msg).await?;

        let Transport::Stdio(io) = &mut self.transport else {
            unreachable!("the gateway path returned above")
        };
        loop {
            let line = io
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
            return interpret(&msg, method, "server");
        }
    }

    async fn notify(&mut self, method: &str, params: Value) -> Result<()> {
        let msg = json!({ "jsonrpc": "2.0", "method": method, "params": params });
        // A notification has no id and no response; `202 Accepted` (or any 2xx)
        // means delivered. Anything the gateway does answer with is discarded,
        // because there is no request for it to belong to — but the response's
        // headers are still read, so a session id offered here is not lost.
        if let Transport::Gateway(gw) = &mut self.transport {
            return gw.post(&serde_json::to_string(&msg)?).await.map(|_| ());
        }
        self.send(&msg).await
    }

    async fn send(&mut self, msg: &Value) -> Result<()> {
        let Transport::Stdio(io) = &mut self.transport else {
            unreachable!("send is the stdio write path; the gateway posts instead")
        };
        let mut line = serde_json::to_string(msg)?;
        line.push('\n');
        io.stdin.write_all(line.as_bytes()).await.context("writing to the MCP server failed")?;
        io.stdin.flush().await.context("flushing to the MCP server failed")
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
        // From MCP 2025-06-18 every later HTTP request carries the version the
        // handshake settled on — which is the server's answer, not what this
        // client asked for. Over stdio there is no header and nothing to carry.
        if let Transport::Gateway(gw) = &mut self.transport {
            if let Some(v) = result.get("protocolVersion").and_then(Value::as_str) {
                gw.negotiated(v);
            }
        }
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

    /// Close stdin and reap the child; a no-op for a gateway.
    ///
    /// Dropping stdin is what tells a stdio server to exit: its read loop sees
    /// EOF. `kill_on_drop` is the backstop for a server that ignores that, so a
    /// finished task cannot leave a process behind in the runner. A gateway owns
    /// its own upstream's lifetime and outlives this step, so there is nothing
    /// here to tear down.
    pub async fn shutdown(self) {
        match self.transport {
            Transport::Gateway(_) => {}
            Transport::Stdio(io) => {
                let ChildIo { mut child, stdin, .. } = *io;
                drop(stdin);
                if let Err(e) = child.kill().await {
                    tracing::debug!(error = %e, "the MCP server had already exited");
                }
            }
        }
    }
}

/// Turn a JSON-RPC response into its result.
///
/// JSON-RPC 2.0 (and MCP) require a response to carry exactly one of `result` or
/// `error`. One with neither is malformed — fail rather than return `Null`,
/// which would read downstream as a tool that succeeded and produced nothing.
///
/// `peer` is what to call the other end in the message: the gateway's denials
/// are its own, not the server's, and a task log that confuses the two sends
/// the reader to the wrong place.
fn interpret(msg: &Value, method: &str, peer: &str) -> Result<Value> {
    if let Some(err) = msg.get("error") {
        let code = err.get("code").and_then(Value::as_i64).unwrap_or(0);
        let message = err.get("message").and_then(Value::as_str).unwrap_or("unknown error");
        bail!("the MCP {peer} rejected {method:?}: {message} (code {code})");
    }
    match msg.get("result") {
        Some(result) => Ok(result.clone()),
        None => bail!(
            "the MCP {peer}'s response to {method:?} carried neither a result nor an error"
        ),
    }
}

/// Run one step end to end: connect, handshake, call, shut down — the handshake
/// and call bounded by `deadline`.
///
/// The timeout lives here, not in the caller, so the child is **always** shut
/// down. A caller that wrapped `run_step` in `timeout` would, on expiry, drop
/// this future mid-await and never reach `shutdown` — leaking the child to
/// tokio's best-effort reaping. Here the deadline expiring still falls through
/// to `session.shutdown()`.
pub async fn run_step(cfg: &StepConfig, deadline: tokio::time::Instant) -> Result<ToolResult> {
    let mut session = Session::connect(cfg).await?;
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

    /// The spawned program and its arguments, or a panic naming what was there
    /// instead — the tests below are about the stdio target specifically.
    fn stdio(cfg: &StepConfig) -> (&str, &[String]) {
        match &cfg.target {
            Target::Stdio { program, args } => (program.as_str(), args.as_slice()),
            Target::Gateway(gw) => panic!("expected a stdio target, got the gateway {gw:?}"),
        }
    }

    #[test]
    fn a_minimal_config_needs_only_a_server_and_a_tool() {
        let cfg = StepConfig::from_vars(
            vars(&[("DAGRON_MCP_STEP_SERVER", "dagron-mcp"), ("DAGRON_MCP_STEP_TOOL", "ping")]),
            "",
        )
        .unwrap();
        let (program, args) = stdio(&cfg);
        assert_eq!(program, "dagron-mcp");
        assert_eq!(cfg.tool, "ping");
        assert!(args.is_empty());
        assert_eq!(cfg.arguments, json!({}), "no arguments is an empty object, not an error");
        assert_eq!(cfg.timeout_secs, DEFAULT_TIMEOUT_SECS);
        assert_eq!(cfg.output, None);
    }

    /// The whole point of the seam: one variable moves a step from calling a
    /// server directly to calling it through a governed gateway, with nothing
    /// else in the workflow changing.
    #[test]
    fn a_gateway_variable_redirects_the_step_without_touching_anything_else() {
        let cfg = StepConfig::from_vars(
            vars(&[
                ("DAGRON_MCP_STEP_GATEWAY", "127.0.0.1:7878"),
                ("DAGRON_MCP_STEP_TOOL", "list_issues"),
            ]),
            r#"{"repo":"x"}"#,
        )
        .unwrap();
        match &cfg.target {
            Target::Gateway(gw) => {
                assert_eq!(gw.host, "127.0.0.1");
                assert_eq!(gw.port, 7878);
                assert_eq!(gw.path, "/mcp");
            }
            other => panic!("expected a gateway target, got {other:?}"),
        }
        assert_eq!(cfg.tool, "list_issues", "the tool and arguments are untouched");
        assert_eq!(cfg.arguments, json!({"repo": "x"}));
    }

    /// A workflow that already names its server must be able to gain governance
    /// by setting one variable on the task, without editing the step. So the
    /// gateway wins and the program is ignored rather than rejected.
    #[test]
    fn a_gateway_wins_over_a_server_program_rather_than_conflicting_with_it() {
        let cfg = StepConfig::from_vars(
            vars(&[
                ("DAGRON_MCP_STEP_SERVER", "mcp-server-github"),
                ("DAGRON_MCP_STEP_GATEWAY", "http://mcpdef.internal:7878/mcp"),
                ("DAGRON_MCP_STEP_TOOL", "t"),
            ]),
            "",
        )
        .unwrap();
        assert!(matches!(cfg.target, Target::Gateway(_)), "the gateway must win");
    }

    /// Without a gateway the server is still required — the seam is additive,
    /// so it must not have loosened the stdio path's contract.
    #[test]
    fn a_blank_gateway_falls_back_to_requiring_a_server() {
        let e = StepConfig::from_vars(
            vars(&[("DAGRON_MCP_STEP_GATEWAY", "   "), ("DAGRON_MCP_STEP_TOOL", "t")]),
            "",
        )
        .unwrap_err();
        assert!(e.to_string().contains("DAGRON_MCP_STEP_SERVER"), "got: {e}");
    }

    /// A bad gateway address fails at config time, before a task image has done
    /// any work — the error names the variable, not a socket.
    #[test]
    fn a_malformed_gateway_is_rejected_by_name_at_config_time() {
        let e = StepConfig::from_vars(
            vars(&[
                ("DAGRON_MCP_STEP_GATEWAY", "https://gw.internal/mcp"),
                ("DAGRON_MCP_STEP_TOOL", "t"),
            ]),
            "",
        )
        .unwrap_err();
        assert!(e.to_string().contains("DAGRON_MCP_STEP_GATEWAY"), "got: {e}");
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
        assert_eq!(stdio(&cfg).1, ["-y", "@mcp/fs", "/data/my files"]);
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
