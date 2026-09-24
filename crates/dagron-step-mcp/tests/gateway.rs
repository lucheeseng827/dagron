//! End-to-end: a step configured with `DAGRON_MCP_STEP_GATEWAY` drives a real
//! HTTP conversation and returns the tool's result.
//!
//! The unit tests in `src/gateway.rs` prove the request is formatted and the
//! response parsed. This proves the thing that actually matters — that
//! `run_step` completes the MCP handshake and a `tools/call` over the wire —
//! against a stand-in that answers the way MCPdef's Streamable-HTTP listener
//! does: JSON for requests, `202 Accepted` for the notification.

use std::time::Duration;

use dagron_step_mcp::{run_step, StepConfig};
use serde_json::{json, Value};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// What the stand-in gateway saw, so the test can assert on the protocol rather
/// than only on the result.
#[derive(Default)]
struct Seen {
    methods: Vec<String>,
    authorization: Option<String>,
    /// The `Host` header of each request, in order.
    hosts: Vec<String>,
    tool_call: Option<Value>,
}

/// Serve exactly `n` request-response exchanges, then return what was seen.
///
/// One connection per exchange: the step sends `Connection: close`, so each
/// POST is its own TCP connection.
async fn serve(listener: TcpListener, n: usize) -> Seen {
    let mut seen = Seen::default();
    for _ in 0..n {
        let (mut sock, _) = listener.accept().await.expect("accept");
        let req = read_request(&mut sock).await;

        let (head, body) = req.split_once("\r\n\r\n").expect("a request with a body separator");
        if let Some(v) = header(head, "authorization") {
            seen.authorization = Some(v.to_string());
        }
        seen.hosts.push(header(head, "host").unwrap_or_default().to_string());

        let msg: Value = serde_json::from_str(body).expect("the step sent valid JSON");
        let method = msg["method"].as_str().unwrap_or_default().to_string();
        seen.methods.push(method.clone());

        let response = match method.as_str() {
            // A notification: no id, no body, 202 — exactly what the spec says.
            "notifications/initialized" => "HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n".to_string(),
            "initialize" => json_response(&json!({
                "jsonrpc": "2.0",
                "id": msg["id"],
                "result": {
                    "protocolVersion": "2025-11-25",
                    "capabilities": {},
                    "serverInfo": { "name": "mcpdef", "version": "0.2.0" },
                },
            })),
            "tools/call" => {
                seen.tool_call = Some(msg["params"].clone());
                json_response(&json!({
                    "jsonrpc": "2.0",
                    "id": msg["id"],
                    "result": { "content": [{ "type": "text", "text": "governed and allowed" }] },
                }))
            }
            other => panic!("the step sent an unexpected method: {other:?}"),
        };
        sock.write_all(response.as_bytes()).await.expect("write response");
        sock.shutdown().await.ok();
    }
    seen
}

/// One header's value from a raw request head, with its case intact — an
/// `Mcp-Session-Id` is an opaque token a gateway compares byte for byte.
fn header<'a>(head: &'a str, name: &str) -> Option<&'a str> {
    head.lines()
        .find(|l| {
            l.as_bytes().get(name.len()) == Some(&b':')
                && l.get(..name.len()).is_some_and(|n| n.eq_ignore_ascii_case(name))
        })
        .map(|l| l[name.len() + 1..].trim())
}

fn json_response(body: &Value) -> String {
    let body = body.to_string();
    format!(
        "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
        body.len()
    )
}

/// Read until the headers are complete, then the declared body.
async fn read_request(sock: &mut TcpStream) -> String {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 1024];
    loop {
        let n = sock.read(&mut chunk).await.expect("read request");
        assert!(n > 0, "the step closed before sending a complete request");
        buf.extend_from_slice(&chunk[..n]);
        let text = String::from_utf8_lossy(&buf).to_string();
        if let Some((head, body)) = text.split_once("\r\n\r\n") {
            let len: usize = head
                .lines()
                .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
                .and_then(|l| l["content-length:".len()..].trim().parse().ok())
                .unwrap_or(0);
            if body.len() >= len {
                return text;
            }
        }
    }
}

fn config(addr: &str, token: Option<&str>) -> StepConfig {
    StepConfig::from_vars(
        |k| match k {
            "DAGRON_MCP_STEP_GATEWAY" => Some(addr.to_string()),
            "DAGRON_MCP_STEP_GATEWAY_TOKEN" => token.map(str::to_string),
            "DAGRON_MCP_STEP_TOOL" => Some("list_issues".to_string()),
            _ => None,
        },
        r#"{"repo":"acme/widgets"}"#,
    )
    .expect("the gateway config resolves")
}

#[tokio::test]
async fn a_step_pointed_at_a_gateway_completes_the_handshake_and_the_tool_call() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let gateway = tokio::spawn(serve(listener, 3));

    let cfg = config(&addr, None);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let result = run_step(&cfg, deadline).await.expect("the step succeeds through the gateway");

    assert_eq!(result.text, "governed and allowed");
    assert!(!result.is_error);

    let seen = gateway.await.unwrap();
    assert_eq!(
        seen.methods,
        ["initialize", "notifications/initialized", "tools/call"],
        "the full MCP lifecycle must cross the gateway, not just the tool call"
    );
    assert_eq!(
        seen.tool_call.as_ref().unwrap()["name"],
        "list_issues",
        "the tool name is what the gateway routes and allow-lists on"
    );
    assert_eq!(seen.tool_call.unwrap()["arguments"]["repo"], "acme/widgets");
    assert_eq!(seen.authorization, None, "no token configured means no Authorization header");
}

#[tokio::test]
async fn a_configured_token_reaches_the_gateway_as_a_bearer() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    let gateway = tokio::spawn(serve(listener, 3));

    let cfg = config(&addr, Some("s3cret-jwt"));
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    run_step(&cfg, deadline).await.expect("the step succeeds");

    let seen = gateway.await.unwrap();
    assert_eq!(
        seen.authorization.as_deref(),
        Some("Bearer s3cret-jwt"),
        "a gateway with [gateway.auth] on rejects anything else"
    );
}

/// A denial is the interesting case: MCPdef answers a policy deny with a
/// JSON-RPC tool result carrying `isError`, so the model can self-correct. The
/// step must surface that as a task failure with the reason, not as success.
#[tokio::test]
async fn a_gateway_denial_fails_the_step_with_the_reason() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let gateway = tokio::spawn(async move {
        for _ in 0..3 {
            let (mut sock, _) = listener.accept().await.unwrap();
            let req = read_request(&mut sock).await;
            let body = req.split_once("\r\n\r\n").unwrap().1;
            let msg: Value = serde_json::from_str(body).unwrap();
            let response = match msg["method"].as_str().unwrap_or_default() {
                "notifications/initialized" => {
                    "HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n".to_string()
                }
                "initialize" => json_response(&json!({
                    "jsonrpc": "2.0", "id": msg["id"],
                    "result": { "protocolVersion": "2025-11-25", "capabilities": {},
                                "serverInfo": { "name": "mcpdef", "version": "0.2.0" } },
                })),
                _ => json_response(&json!({
                    "jsonrpc": "2.0", "id": msg["id"],
                    "result": {
                        "isError": true,
                        "content": [{ "type": "text",
                            "text": "MCPdef denied: tool 'list_issues' matches deny pattern for 'github'" }],
                    },
                })),
            };
            sock.write_all(response.as_bytes()).await.unwrap();
            sock.shutdown().await.ok();
        }
    });

    let cfg = config(&addr, None);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let result = run_step(&cfg, deadline).await.expect("a denial is a completed call");
    gateway.await.unwrap();

    assert!(result.is_error, "a policy denial must not read as success");
    assert!(
        result.text.contains("MCPdef denied"),
        "the gateway's reason must reach the task log: {}",
        result.text
    );
}

/// A stateful gateway mints a session id at `initialize` and refuses anything
/// that does not carry it back. That is the whole reason the transport holds one
/// session for the step instead of cloning a fresh endpoint per message: a clone
/// has nowhere to keep the id, so the second request would 404 and the docs'
/// promise that any Streamable-HTTP gateway works would be false.
///
/// The negotiated protocol version rides the same way, for the same reason — and
/// it is the server's answer that is echoed, not what this client asked for.
#[tokio::test]
async fn a_session_id_minted_at_initialize_is_echoed_on_every_later_request() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let gateway = tokio::spawn(async move {
        let mut seen: Vec<(Option<String>, Option<String>)> = Vec::new();
        for _ in 0..3 {
            let (mut sock, _) = listener.accept().await.unwrap();
            let req = read_request(&mut sock).await;
            let (head, body) = req.split_once("\r\n\r\n").unwrap();
            let sid = header(head, "mcp-session-id").map(str::to_string);
            seen.push((sid.clone(), header(head, "mcp-protocol-version").map(str::to_string)));

            let msg: Value = serde_json::from_str(body).unwrap();
            let method = msg["method"].as_str().unwrap_or_default().to_string();

            let response = if method == "initialize" {
                // Mixed case on purpose: lower-casing the value on the way in
                // would make every later request fail the comparison below.
                with_header(
                    json_response(&json!({
                        "jsonrpc": "2.0", "id": msg["id"],
                        "result": { "protocolVersion": "2025-11-25", "capabilities": {},
                                    "serverInfo": { "name": "stateful", "version": "0" } },
                    })),
                    "Mcp-Session-Id: AbC-42",
                )
            } else if sid.as_deref() != Some("AbC-42") {
                "HTTP/1.1 404 Not Found\r\nContent-Length: 0\r\n\r\n".to_string()
            } else if method == "notifications/initialized" {
                "HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n".to_string()
            } else {
                json_response(&json!({
                    "jsonrpc": "2.0", "id": msg["id"],
                    "result": { "content": [{ "type": "text", "text": "stateful and allowed" }] },
                }))
            };
            sock.write_all(response.as_bytes()).await.unwrap();
            sock.shutdown().await.ok();
        }
        seen
    });

    let cfg = config(&addr, None);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let result = run_step(&cfg, deadline).await.expect("the step survives a stateful gateway");
    assert_eq!(result.text, "stateful and allowed");

    let seen = gateway.await.unwrap();
    let after = (Some("AbC-42".to_string()), Some("2025-11-25".to_string()));
    assert_eq!(
        seen,
        [(None, None), after.clone(), after],
        "initialize carries neither header; everything after it carries both, with the \
         version the gateway answered rather than the one the client proposed"
    );
}

/// The shape a hyper-based gateway actually produces when it streams: a chunked
/// `text/event-stream` body whose first event is a progress notification.
///
/// Both halves used to be wrong. Chunking was refused outright — on the theory
/// that `Connection: close` forbade it, which RFC 9112 does not — so the SSE
/// branch was unreachable for exactly the gateways that stream. And the first
/// event was returned whatever it was, so a progress notification would have
/// become the tool's result: a task that reads as succeeding with no output.
#[tokio::test]
async fn a_chunked_sse_answer_steps_over_the_progress_notification() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();

    let gateway = tokio::spawn(async move {
        for _ in 0..3 {
            let (mut sock, _) = listener.accept().await.unwrap();
            let req = read_request(&mut sock).await;
            let body = req.split_once("\r\n\r\n").unwrap().1;
            let msg: Value = serde_json::from_str(body).unwrap();
            let response = match msg["method"].as_str().unwrap_or_default() {
                "notifications/initialized" => {
                    "HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n".to_string()
                }
                "initialize" => chunked_sse(&[json!({
                    "jsonrpc": "2.0", "id": msg["id"],
                    "result": { "protocolVersion": "2025-11-25", "capabilities": {},
                                "serverInfo": { "name": "streaming", "version": "0" } },
                })]),
                _ => chunked_sse(&[
                    json!({ "jsonrpc": "2.0", "method": "notifications/progress",
                            "params": { "progress": 1, "total": 2 } }),
                    json!({ "jsonrpc": "2.0", "method": "notifications/message",
                            "params": { "level": "info", "data": "still working" } }),
                    json!({ "jsonrpc": "2.0", "id": msg["id"],
                            "result": { "content": [{ "type": "text", "text": "streamed result" }] } }),
                ]),
            };
            sock.write_all(response.as_bytes()).await.unwrap();
            sock.shutdown().await.ok();
        }
    });

    let cfg = config(&addr, None);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let result = run_step(&cfg, deadline).await.expect("a streamed answer is still an answer");
    gateway.await.unwrap();

    assert!(!result.is_error);
    assert_eq!(
        result.text, "streamed result",
        "the response event is the result, not whichever event arrived first"
    );
}

/// `TcpStream::connect(("[::1]", port))` treats the brackets as part of a
/// hostname and fails to resolve. They belong in the `Host` header and nowhere
/// else, which no test caught until one actually opened the socket.
///
/// Skipped where the sandbox has no IPv6 loopback; `connect_host` is pinned
/// directly by a unit test in `src/gateway.rs` either way.
#[tokio::test]
async fn an_ipv6_gateway_is_reached_with_the_brackets_stripped_from_the_socket() {
    let Ok(listener) = TcpListener::bind("[::1]:0").await else {
        eprintln!("no IPv6 loopback in this environment; skipping");
        return;
    };
    let port = listener.local_addr().unwrap().port();
    let gateway = tokio::spawn(serve(listener, 3));

    let cfg = config(&format!("http://[::1]:{port}/mcp"), None);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    let result = run_step(&cfg, deadline).await.expect("an IPv6 literal must connect");

    assert_eq!(result.text, "governed and allowed");
    let seen = gateway.await.unwrap();
    assert_eq!(
        seen.hosts[0],
        format!("[::1]:{port}"),
        "the Host header keeps the brackets the socket had to drop"
    );
}

/// Add one header line to a response, after the status line.
fn with_header(response: String, line: &str) -> String {
    let at = response.find("\r\n").expect("a status line") + 2;
    let mut out = response;
    out.insert_str(at, &format!("{line}\r\n"));
    out
}

/// Frame SSE events into a chunked body, one chunk per event — what hyper does
/// with a stream it cannot length in advance.
fn chunked_sse(events: &[Value]) -> String {
    let mut out = String::from(
        "HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n",
    );
    for e in events {
        let payload = format!("event: message\ndata: {e}\n\n");
        out.push_str(&format!("{:x}\r\n{payload}\r\n", payload.len()));
    }
    out.push_str("0\r\n\r\n");
    out
}
