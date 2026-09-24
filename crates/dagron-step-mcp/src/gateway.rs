//! Streamable-HTTP transport to an MCP gateway — a hand-rolled HTTP/1.1 client.
//!
//! **Why not an HTTP crate.** This binary ships inside every task image, and the
//! crate docs defend having no HTTP stack for that reason. The sibling
//! `dagron-step-llm` does take `reqwest` + rustls, but it must reach arbitrary
//! TLS endpoints on the public internet. This one talks to a gateway that is, by
//! design, loopback- or cluster-local plain HTTP (MCPdef binds `127.0.0.1` by
//! default). One request, one response, a server we control, no redirects, no
//! cookies, no content negotiation.
//!
//! **What that buys and what it costs.** It buys a step binary that is exactly
//! as large as it was. It costs TLS: `https://` is refused with a message rather
//! than silently downgraded. If a remote, TLS-terminated gateway is ever needed,
//! the honest move is a `gateway-tls` feature that pulls `reqwest` in — not
//! hand-rolling TLS here.
//!
//! **And it costs vigilance.** Everything a client crate would have handled is
//! ours to handle: chunked bodies are decoded here, because chunking is legal on
//! a `Connection: close` response and hyper — what most gateways are built on —
//! chunks anything it cannot length in advance; header values that come from
//! configuration or from the peer are validated here before they are written
//! into a request; and `Mcp-Session-Id` is tracked here, because a stateful
//! gateway answers the second request with 404 without it. Every one of those
//! was a bug in the first cut of this file, which is the honest argument for the
//! dependency and is recorded here rather than in a commit message nobody reads.

use std::borrow::Cow;

use anyhow::{bail, Context, Result};
use serde_json::Value;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

/// Responses larger than this are refused rather than buffered. A gateway
/// answering a tool call has no business sending more, and an unbounded read
/// from a misbehaving peer is how a task runner runs out of memory.
const MAX_RESPONSE_BYTES: usize = 16 * 1024 * 1024;

/// A resolved MCP gateway endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Gateway {
    /// Host as written — brackets included for an IPv6 literal, because the
    /// `Host` header wants them. [`Gateway::connect_host`] strips them again for
    /// the socket, which does not.
    pub host: String,
    pub port: u16,
    /// Request target, always starting with `/`.
    pub path: String,
    /// Bearer token for a gateway with `[gateway.auth]` enabled. Trimmed and
    /// checked at parse time — see [`Gateway::parse`].
    pub token: Option<String>,
}

impl Gateway {
    /// Parse `DAGRON_MCP_STEP_GATEWAY`.
    ///
    /// A bare `host:port` is accepted and read as `http://host:port/mcp`,
    /// because that is what an operator types. The default path is `/mcp` —
    /// MCPdef's single Streamable-HTTP endpoint.
    pub fn parse(raw: &str, token: Option<String>) -> Result<Self> {
        let raw = raw.trim();
        if raw.is_empty() {
            bail!("DAGRON_MCP_STEP_GATEWAY must not be empty");
        }
        if let Some(rest) = raw.strip_prefix("https://") {
            bail!(
                "DAGRON_MCP_STEP_GATEWAY is {raw:?}, but this step speaks plain HTTP only. \
                 Point it at the gateway's loopback or cluster-local address \
                 (http://{rest}), or terminate TLS in front of the step."
            );
        }
        let rest = raw.strip_prefix("http://").unwrap_or(raw);
        if rest.is_empty() {
            bail!("DAGRON_MCP_STEP_GATEWAY {raw:?} has no host");
        }

        let (authority, path) = match rest.find('/') {
            Some(i) => (&rest[..i], rest[i..].to_string()),
            None => (rest, "/mcp".to_string()),
        };
        if authority.is_empty() {
            bail!("DAGRON_MCP_STEP_GATEWAY {raw:?} has no host");
        }

        // Split host:port from the right, so an IPv6 literal in brackets and a
        // bare hostname both land correctly.
        let (host, port) = match authority.rfind(':') {
            // A colon inside `[...]` is part of an IPv6 literal, not a port.
            Some(i) if !authority[i..].contains(']') => {
                let p: u16 = authority[i + 1..].parse().with_context(|| {
                    format!("DAGRON_MCP_STEP_GATEWAY {raw:?} has a non-numeric port")
                })?;
                if p == 0 {
                    bail!("DAGRON_MCP_STEP_GATEWAY {raw:?} has port 0");
                }
                (authority[..i].to_string(), p)
            }
            _ => (authority.to_string(), 80),
        };

        let token = Self::clean_token(token)?;

        // Not an error: a cluster-local gateway on a pod network is a legitimate
        // deployment, and refusing it would send operators to hand-rolled
        // workarounds. But a bearer token on plain HTTP is readable by anything
        // on the path, and that is worth saying out loud once per step.
        if token.is_some() && !is_loopback(&host) {
            tracing::warn!(
                gateway = %host,
                "sending a bearer token to a non-loopback gateway over plain HTTP; anything \
                 on the path can read it. Keep the gateway loopback- or pod-local, or \
                 terminate TLS in front of the step."
            );
        }

        Ok(Self { host, port, path, token })
    }

    /// Trim the token and refuse one that cannot be written into a header.
    ///
    /// A token arrives from the environment, and in a cluster that usually means
    /// a mounted secret — which keeps the trailing newline the file ended with.
    /// It is interpolated straight into a header this module writes by hand, so
    /// an untrimmed one does not produce a subtly wrong token: it produces
    /// `Authorization: Bearer tok\n\r\n` and ends the header block early,
    /// turning the rest of the request into a body the gateway never reads.
    /// Trim, then require what is left to be [`is_visible_ascii`].
    ///
    /// A `b < 0x20` byte rule is not enough: U+0085 encodes as `0xC2 0x85` and
    /// the C1 controls as `0xC2 0x80`..`0xC2 0x9F`, so neither byte trips it.
    /// Rather than enumerate the ways a character can be a control, require the
    /// set a bearer credential is actually made of — RFC 6750's `b64token` is a
    /// strict subset of visible ASCII — which rejects C1, every other non-ASCII
    /// byte, and an interior space, none of which belongs in a credential.
    fn clean_token(token: Option<String>) -> Result<Option<String>> {
        let Some(t) = token.map(|t| t.trim().to_string()).filter(|t| !t.is_empty()) else {
            return Ok(None);
        };
        if !is_visible_ascii(&t) {
            bail!(
                "DAGRON_MCP_STEP_GATEWAY_TOKEN contains a character that cannot be sent in an \
                 HTTP header. A bearer credential is visible ASCII; a newline kept from a \
                 mounted secret file is the usual cause."
            );
        }
        Ok(Some(t))
    }

    /// The `Host` header value, which must carry the port unless it is 80.
    fn host_header(&self) -> String {
        if self.port == 80 {
            self.host.clone()
        } else {
            format!("{}:{}", self.host, self.port)
        }
    }

    /// The host to open a socket to.
    ///
    /// An IPv6 literal's brackets belong in the `Host` header and nowhere else:
    /// `TcpStream::connect(("[::1]", port))` treats `[::1]` as a name to
    /// resolve, and fails. The parser keeps them because the header needs them,
    /// so the socket strips them here.
    fn connect_host(&self) -> &str {
        self.host.strip_prefix('[').and_then(|h| h.strip_suffix(']')).unwrap_or(&self.host)
    }
}

/// Visible ASCII, 0x21 to 0x7E: the bytes a header field value carries with no
/// ambiguity at all — no whitespace an intermediary may fold or strip, nothing
/// that can end a header line, and nothing outside ASCII. Both a bearer token on
/// the way out and an `Mcp-Session-Id` on the way in are checked against it,
/// because both are written into a request this module builds by hand.
fn is_visible_ascii(v: &str) -> bool {
    !v.is_empty() && v.bytes().all(|b| (0x21..=0x7e).contains(&b))
}

/// Is this host one nothing else can be listening on the wire for?
fn is_loopback(host: &str) -> bool {
    let h = host.strip_prefix('[').and_then(|h| h.strip_suffix(']')).unwrap_or(host);
    h.eq_ignore_ascii_case("localhost")
        || h.parse::<std::net::IpAddr>().is_ok_and(|ip| ip.is_loopback())
}

/// A gateway, plus the state it asked this client to carry between messages.
///
/// Streamable HTTP makes one connection per message, so a "session" here is not
/// a socket: it is the `Mcp-Session-Id` a stateful gateway mints on `initialize`
/// and expects echoed on every request after it. MCPdef is stateless and sends
/// none, in which case this stays `None` and no header is written — but the
/// promise is that any Streamable-HTTP gateway works, and a stateful one in
/// front of a stateful server answers the second request with 404 if the header
/// is missing. Cloning a [`Gateway`] per message, as this used to, could never
/// have carried that.
#[derive(Debug)]
pub struct GatewaySession {
    gw: Gateway,
    session_id: Option<String>,
    protocol_version: Option<String>,
}

impl GatewaySession {
    /// Start a session against a parsed endpoint. Nothing is sent yet: the
    /// session id, if there is one, arrives with the first response.
    pub fn new(gw: Gateway) -> Self {
        Self { gw, session_id: None, protocol_version: None }
    }

    /// Record the protocol version the gateway returned from `initialize`.
    ///
    /// From MCP 2025-06-18 a Streamable-HTTP client puts the *negotiated*
    /// version on every request after the handshake, so a gateway can route or
    /// reject on it rather than assume one. Echoing what the server sent is the
    /// spec's rule — not what the client asked for, which may not be what it
    /// got. A version that cannot be written into a header is dropped with a
    /// warning instead of failing a handshake that otherwise worked.
    pub fn negotiated(&mut self, version: &str) {
        if !version.is_empty() && version.bytes().all(|b| (0x21..=0x7e).contains(&b)) {
            self.protocol_version = Some(version.to_string());
        } else {
            tracing::warn!(
                "the MCP gateway negotiated a protocol version that cannot be sent in a header; \
                 later requests will omit MCP-Protocol-Version"
            );
        }
    }

    /// POST one JSON-RPC message and return the response body, if any.
    ///
    /// `Ok(None)` is a `202 Accepted` — the Streamable-HTTP answer to a
    /// notification, which has no body and is not an error.
    ///
    /// `&mut self` because a gateway may hand back a session id that has to
    /// survive into the next POST.
    pub async fn post(&mut self, body: &str) -> Result<Option<Value>> {
        let gw = &self.gw;
        let mut stream = TcpStream::connect((gw.connect_host(), gw.port))
            .await
            .with_context(|| format!("connecting to the MCP gateway at {}", gw.host_header()))?;

        let mut req = format!(
            "POST {} HTTP/1.1\r\n\
             Host: {}\r\n\
             Content-Type: application/json\r\n\
             Accept: application/json, text/event-stream\r\n\
             Content-Length: {}\r\n\
             Connection: close\r\n",
            gw.path,
            gw.host_header(),
            body.len(),
        );
        if let Some(token) = &gw.token {
            // Deliberately not logged anywhere, including on error paths.
            req.push_str(&format!("Authorization: Bearer {token}\r\n"));
        }
        if let Some(id) = &self.session_id {
            req.push_str(&format!("Mcp-Session-Id: {id}\r\n"));
        }
        if let Some(v) = &self.protocol_version {
            req.push_str(&format!("MCP-Protocol-Version: {v}\r\n"));
        }
        req.push_str("\r\n");
        req.push_str(body);

        stream.write_all(req.as_bytes()).await.context("sending to the MCP gateway")?;
        stream.flush().await.context("flushing to the MCP gateway")?;

        // `Connection: close` means the server ends the body by closing, so a
        // read to EOF is the whole response — no keep-alive framing to track.
        // What it does *not* mean is an unchunked body; see `dechunk`.
        let mut buf = Vec::new();
        let mut chunk = [0u8; 8192];
        loop {
            let n = stream.read(&mut chunk).await.context("reading from the MCP gateway")?;
            if n == 0 {
                break;
            }
            if buf.len() + n > MAX_RESPONSE_BYTES {
                bail!("the MCP gateway's response exceeded {MAX_RESPONSE_BYTES} bytes");
            }
            buf.extend_from_slice(&chunk[..n]);
        }

        let res = parse_response(&buf, &gw.host_header(), self.session_id.is_some())?;
        if let Some(id) = res.session_id {
            self.session_id = Some(id);
        }
        Ok(res.message)
    }
}

/// What one HTTP response carried.
#[derive(Debug)]
struct Response {
    /// An `Mcp-Session-Id` the gateway wants echoed from here on, if it sent one.
    session_id: Option<String>,
    /// The JSON-RPC message, or `None` for a `202 Accepted`.
    message: Option<Value>,
}

/// Split an HTTP/1.1 response into status, headers and body, then decode the
/// body into a JSON-RPC message.
///
/// `sent_session` says whether the request carried an `Mcp-Session-Id`, which is
/// what makes a 404 mean "your session expired" rather than "no such path".
fn parse_response(raw: &[u8], authority: &str, sent_session: bool) -> Result<Response> {
    let split = raw
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .context("the MCP gateway's response had no header terminator")?;
    let head = std::str::from_utf8(&raw[..split])
        .context("the MCP gateway sent non-UTF-8 response headers")?;
    let body = &raw[split + 4..];

    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or_default();
    // "HTTP/1.1 200 OK" — the code is the second field.
    let status: u16 = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|c| c.parse().ok())
        .with_context(|| format!("unparseable status line from the MCP gateway: {status_line:?}"))?;

    // The *name* matches case-insensitively; the value comes back as sent. An
    // `Mcp-Session-Id` is an opaque token the gateway compares byte for byte, so
    // lower-casing values here would break the very gateways it exists for.
    let header = |name: &str| -> Option<&str> {
        lines
            .clone()
            .find(|l| {
                l.as_bytes().get(name.len()) == Some(&b':')
                    && l.get(..name.len()).is_some_and(|n| n.eq_ignore_ascii_case(name))
            })
            .map(|l| l[name.len() + 1..].trim())
    };

    let session_id = match header("mcp-session-id").filter(|id| !id.is_empty()) {
        // This goes straight back out in the next request's headers, so it is
        // validated on the way in. The spec says visible ASCII; anything else is
        // a broken gateway, or a header injection attempted through us.
        Some(id) => {
            if !is_visible_ascii(id) {
                bail!(
                    "the MCP gateway at {authority} sent an Mcp-Session-Id that is not visible \
                     ASCII, so it cannot be echoed back safely"
                );
            }
            Some(id.to_string())
        }
        None => None,
    };

    let body = match header("transfer-encoding").map(str::to_ascii_lowercase) {
        // Chunked is legal on a `Connection: close` response (RFC 9112 §6.1),
        // and hyper — which most MCP gateways are built on — chunks any body it
        // cannot length in advance, which is every streamed SSE body. Refusing
        // it, as this module first did, made the SSE branch below unreachable
        // for exactly the servers that stream.
        Some(te) if te == "chunked" => Cow::Owned(dechunk(body)?),
        Some(te) => bail!(
            "the MCP gateway at {authority} sent transfer-encoding {te:?}, which this step does \
             not decode. Only `chunked` and an unencoded body are supported."
        ),
        None => Cow::Borrowed(body),
    };

    // A notification's answer: no body, and not an error.
    if status == 202 {
        return Ok(Response { session_id, message: None });
    }
    if !(200..300).contains(&status) {
        let detail = String::from_utf8_lossy(&body);
        let detail = detail.trim();
        // 401 is the one worth naming, because the fix is a specific env var.
        if status == 401 {
            bail!(
                "the MCP gateway at {authority} rejected the request as unauthenticated (401). \
                 Set DAGRON_MCP_STEP_GATEWAY_TOKEN to a bearer token it accepts."
            );
        }
        // 404 on a request that carried a session id is the spec's way of saying
        // the session is gone — a different problem from a wrong path, and one a
        // task retry genuinely fixes.
        if status == 404 && sent_session {
            bail!(
                "the MCP gateway at {authority} no longer has the session it opened for this \
                 step (404). It expired mid-call; retrying the task starts a new one."
            );
        }
        bail!(
            "the MCP gateway at {authority} answered {status}{}",
            if detail.is_empty() { String::new() } else { format!(": {detail}") }
        );
    }

    let text = std::str::from_utf8(&body).context("the MCP gateway sent a non-UTF-8 body")?;
    let content_type = header("content-type").unwrap_or_default().to_ascii_lowercase();

    if content_type.starts_with("text/event-stream") {
        return sse_message(text).map(|m| Response { session_id, message: Some(m) });
    }
    if text.trim().is_empty() {
        bail!("the MCP gateway answered {status} with an empty body");
    }
    serde_json::from_str(text)
        .map(|m| Response { session_id, message: Some(m) })
        .context("the MCP gateway's response body was not valid JSON")
}

/// Decode a chunked transfer-encoding body.
///
/// The minimum RFC 9112 §7.1 asks of a reader: a hex size with optional
/// `;extension`, CRLF, that many bytes, CRLF, repeating until a zero-size chunk.
/// Trailers after it are ignored — no MCP gateway sends any, and a JSON-RPC
/// message would not change if one did.
fn dechunk(body: &[u8]) -> Result<Vec<u8>> {
    let mut out = Vec::new();
    let mut rest = body;
    loop {
        let eol = rest
            .windows(2)
            .position(|w| w == b"\r\n")
            .context("a chunked body from the MCP gateway ended inside a chunk header")?;
        let head = std::str::from_utf8(&rest[..eol])
            .context("a chunk header from the MCP gateway was not UTF-8")?;
        // `1a;name=value` — the size runs to the first `;`.
        let size_hex = head.split(';').next().unwrap_or_default().trim();
        let size = usize::from_str_radix(size_hex, 16)
            .with_context(|| format!("unparseable chunk size from the MCP gateway: {size_hex:?}"))?;
        rest = &rest[eol + 2..];
        if size == 0 {
            return Ok(out);
        }
        // Against the room left, not the sum: `size` is the peer's hex and can
        // be `usize::MAX`, so `out.len() + size` overflows — a debug build
        // panics here and a release build wraps *past* this check, then panics
        // on the slice below where `size + 2` has wrapped to less than `size`.
        // The subtraction cannot underflow, because the append below is the only
        // thing that grows `out` and it runs only once this check has passed; and
        // bounding `size` here is what keeps `size + 2` from wrapping.
        if size > MAX_RESPONSE_BYTES - out.len() {
            bail!("the MCP gateway's response exceeded {MAX_RESPONSE_BYTES} bytes");
        }
        if rest.len() < size + 2 {
            bail!("a chunked body from the MCP gateway was truncated mid-chunk");
        }
        if &rest[size..size + 2] != b"\r\n" {
            bail!("a chunk from the MCP gateway was not terminated by CRLF");
        }
        out.extend_from_slice(&rest[..size]);
        rest = &rest[size + 2..];
    }
}

/// Pull this POST's JSON-RPC response out of an SSE body.
///
/// Only `data:` lines carry payload, a multi-line event's data fields join with
/// newlines, and a blank line ends an event. Events that are not responses —
/// progress notifications, and requests the server makes of us — are skipped
/// rather than returned, exactly as the stdio path skips lines whose `id` is not
/// the one it asked for. Handing a progress notification back as a tool's result
/// is the failure that reads downstream as a tool which succeeded and said
/// nothing, which is worse than an error.
fn sse_message(text: &str) -> Result<Value> {
    let mut data = String::new();
    let mut saw_data = false;
    for line in text.lines() {
        if let Some(rest) = line.strip_prefix("data:") {
            if !data.is_empty() {
                data.push('\n');
            }
            // Exactly one leading space is the field separator; a second one is
            // part of the value, so `trim_start` would corrupt it.
            data.push_str(rest.strip_prefix(' ').unwrap_or(rest));
            saw_data = true;
        } else if line.is_empty() {
            // Event boundary.
            if let Some(v) = response_event(&data)? {
                return Ok(v);
            }
            data.clear();
        }
        // `event:`, `id:`, `retry:` and `:` comments carry nothing we need.
    }
    // A stream that ends without a final blank line still delivered its event.
    if let Some(v) = response_event(&data)? {
        return Ok(v);
    }
    if saw_data {
        bail!(
            "the MCP gateway's event stream ended without answering this request — it carried \
             only notifications"
        );
    }
    bail!("the MCP gateway's event stream carried no data");
}

/// One event's data as a JSON-RPC *response*, or `None` when it is empty or
/// carries a `method` — which in JSON-RPC 2.0 means a request or a
/// notification, never an answer to one of ours.
fn response_event(data: &str) -> Result<Option<Value>> {
    if data.trim().is_empty() {
        return Ok(None);
    }
    let v: Value =
        serde_json::from_str(data).context("an SSE event from the MCP gateway was not valid JSON")?;
    Ok(if v.get("method").is_some() { None } else { Some(v) })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// The common case: no session id in flight, only the message matters.
    fn message(raw: &[u8]) -> Result<Option<Value>> {
        parse_response(raw, "h", false).map(|r| r.message)
    }

    #[test]
    fn a_bare_authority_defaults_to_http_and_the_mcp_path() {
        let g = Gateway::parse("127.0.0.1:7878", None).unwrap();
        assert_eq!(g.host, "127.0.0.1");
        assert_eq!(g.port, 7878);
        assert_eq!(g.path, "/mcp", "the default path is MCPdef's single endpoint");
        assert_eq!(g.token, None);
        assert_eq!(g.host_header(), "127.0.0.1:7878");
    }

    #[test]
    fn a_full_url_keeps_its_path_and_port_80_is_omitted_from_the_host_header() {
        let g = Gateway::parse("http://gateway.internal/govern", None).unwrap();
        assert_eq!(g.port, 80, "no port means 80");
        assert_eq!(g.path, "/govern");
        assert_eq!(g.host_header(), "gateway.internal", "port 80 is implied, not sent");
    }

    /// TLS is refused loudly rather than downgraded silently — a step that
    /// quietly sent a bearer token in the clear would be worse than one that
    /// fails.
    #[test]
    fn https_is_refused_with_the_plain_http_alternative_in_the_message() {
        let e = Gateway::parse("https://gateway.internal:8443/mcp", None).unwrap_err();
        let msg = e.to_string();
        assert!(msg.contains("plain HTTP only"), "got: {msg}");
        assert!(msg.contains("http://gateway.internal:8443/mcp"), "got: {msg}");
    }

    /// The brackets are kept for the `Host` header and dropped for the socket:
    /// `TcpStream::connect` resolves `[::1]` as a name and fails.
    #[test]
    fn an_ipv6_literal_is_not_split_on_its_own_colons_and_connects_without_brackets() {
        let g = Gateway::parse("http://[::1]:7878/mcp", None).unwrap();
        assert_eq!(g.host, "[::1]");
        assert_eq!(g.port, 7878);
        assert_eq!(g.host_header(), "[::1]:7878", "the header keeps the brackets");
        assert_eq!(g.connect_host(), "::1", "the socket must not see them");

        let g = Gateway::parse("127.0.0.1:7878", None).unwrap();
        assert_eq!(g.connect_host(), "127.0.0.1", "a v4 literal is untouched");
    }

    #[test]
    fn a_blank_token_is_treated_as_absent() {
        let g = Gateway::parse("127.0.0.1:7878", Some("   ".into())).unwrap();
        assert_eq!(g.token, None, "a blank token must not produce an empty Authorization header");
    }

    /// A token mounted from a Kubernetes secret keeps the file's trailing
    /// newline. Stored untrimmed it would terminate the header block early
    /// rather than merely be wrong.
    #[test]
    fn a_token_is_trimmed_and_one_with_an_embedded_control_character_is_refused() {
        let g = Gateway::parse("127.0.0.1:7878", Some("  s3cret-jwt\n".into())).unwrap();
        assert_eq!(g.token.as_deref(), Some("s3cret-jwt"), "whitespace never reaches the header");

        let e = Gateway::parse("127.0.0.1:7878", Some("tok\r\nX-Admin: 1".into()))
            .unwrap_err()
            .to_string();
        assert!(e.contains("cannot be sent"), "got: {e}");
        assert!(!e.contains("X-Admin"), "the token must not be echoed into the error: {e}");

        // A byte-level `< 0x20` rule misses these: U+0085 is `0xC2 0x85` and the
        // C1 range is `0xC2 0x80`..`0xC2 0x9F`, so neither byte trips it. None of
        // them belongs in a bearer credential, and nor does an interior space.
        for bad in ["tok\u{85}en", "tok\u{9b}en", "tok en", "tok\u{feff}"] {
            assert!(
                Gateway::parse("127.0.0.1:7878", Some(bad.into())).is_err(),
                "a token containing {bad:?} must be refused"
            );
        }
    }

    #[test]
    fn a_non_numeric_or_zero_port_is_rejected_by_name() {
        assert!(Gateway::parse("host:not-a-port", None).unwrap_err().to_string().contains("port"));
        assert!(Gateway::parse("host:0", None).unwrap_err().to_string().contains("port 0"));
    }

    #[test]
    fn a_json_response_is_returned_and_202_is_an_accepted_notification() {
        let ok = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\n\r\n{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":{}}";
        assert_eq!(message(ok).unwrap(), Some(json!({"jsonrpc":"2.0","id":1,"result":{}})));

        let accepted = b"HTTP/1.1 202 Accepted\r\nContent-Length: 0\r\n\r\n";
        assert_eq!(
            message(accepted).unwrap(),
            None,
            "202 is the answer to a notification, not a failure"
        );
    }

    #[test]
    fn an_sse_body_yields_the_first_complete_event() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\n\r\nevent: message\ndata: {\"jsonrpc\":\"2.0\",\"id\":7,\"result\":{\"ok\":true}}\n\ndata: {\"ignored\":1}\n\n";
        let v = message(raw).unwrap().unwrap();
        assert_eq!(v["id"], 7);
        assert_eq!(v["result"]["ok"], true);
    }

    /// The stdio path skips lines whose `id` is not the one it asked for; the
    /// SSE path must skip events that are not responses at all, or a progress
    /// notification becomes the tool's result.
    #[test]
    fn sse_notifications_are_skipped_until_the_response_arrives() {
        let stream = "\
event: message\n\
data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{\"progress\":1}}\n\
\n\
event: message\n\
data: {\"jsonrpc\":\"2.0\",\"method\":\"roots/list\",\"id\":99}\n\
\n\
event: message\n\
data: {\"jsonrpc\":\"2.0\",\"id\":7,\"result\":{\"ok\":true}}\n\
\n";
        let v = sse_message(stream).unwrap();
        assert_eq!(v["id"], 7, "a notification and a server request must both be stepped over");

        let only_notes =
            "data: {\"jsonrpc\":\"2.0\",\"method\":\"notifications/progress\",\"params\":{}}\n\n";
        let e = sse_message(only_notes).unwrap_err().to_string();
        assert!(e.contains("only notifications"), "got: {e}");
    }

    /// A multi-line event's data fields join with newlines, and the space after
    /// the colon is optional — some gateways omit it. The spec strips exactly
    /// one, which is what this now does; with a JSON payload the difference
    /// against `trim_start` is not observable, so what is pinned here is the
    /// join and the no-space form.
    #[test]
    fn a_multi_line_data_field_joins_with_newlines_and_its_space_is_optional() {
        let v = sse_message("data: {\"id\":1,\ndata:\"result\":{\"text\":\"x\"}}\n\n").unwrap();
        assert_eq!(v["id"], 1);
        assert_eq!(v["result"]["text"], "x");
    }

    /// Chunking is legal on a `Connection: close` response, and hyper does it
    /// for any body it cannot length in advance. Refusing it made the SSE branch
    /// dead code for every gateway that streams.
    #[test]
    fn a_chunked_body_is_decoded_rather_than_refused() {
        let chunked = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n18\r\n{\"jsonrpc\":\"2.0\",\"id\":3,\r\n11\r\n\"result\":{\"a\":1}}\r\n0\r\n\r\n";
        let v = message(chunked).unwrap().unwrap();
        assert_eq!(v["id"], 3);
        assert_eq!(v["result"]["a"], 1);

        // The shape hyper actually produces for a streamed SSE answer.
        let sse = b"HTTP/1.1 200 OK\r\nContent-Type: text/event-stream\r\nTransfer-Encoding: chunked\r\n\r\n31\r\ndata: {\"jsonrpc\":\"2.0\",\"id\":4,\"result\":{\"b\":2}}\n\n\r\n0\r\n\r\n";
        let v = message(sse).unwrap().unwrap();
        assert_eq!(v["result"]["b"], 2);
    }

    #[test]
    fn a_chunk_extension_is_ignored_and_a_truncated_body_is_an_error() {
        let ext = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n2;why=because\r\n{}\r\n0\r\n\r\n";
        assert_eq!(message(ext).unwrap(), Some(json!({})));

        let cut = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\nff\r\n{}\r\n";
        assert!(message(cut).unwrap_err().to_string().contains("truncated"));
    }

    /// `size` is the peer's hex, so it can be `usize::MAX`. Adding it to
    /// `out.len()` overflows: a debug build panics on the add, a release build
    /// wraps past the limit check and then panics on the slice below, where
    /// `size + 2` has wrapped to less than `size`. Either way the task dies
    /// without the reason ever reaching the run's log.
    #[test]
    fn an_absurd_chunk_size_is_an_error_rather_than_a_panic() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: chunked\r\n\r\n2\r\n{}\r\nffffffffffffffff\r\nxx\r\n";
        assert!(message(raw).unwrap_err().to_string().contains("exceeded"));
    }

    /// Anything other than `chunked` really is undecodable here, and says so
    /// instead of handing compressed bytes to the JSON parser.
    #[test]
    fn an_unsupported_transfer_encoding_names_itself() {
        let gzipped = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nTransfer-Encoding: gzip\r\n\r\n\x1f\x8b";
        let e = message(gzipped).unwrap_err().to_string();
        assert!(e.contains("gzip"), "got: {e}");
    }

    /// A gateway that denies the call answers with an HTTP error; the step must
    /// surface the body, because MCPdef puts the matched rule in it.
    #[test]
    fn a_non_2xx_surfaces_the_body_and_401_names_the_token_variable() {
        let denied = b"HTTP/1.1 403 Forbidden\r\nContent-Type: text/plain\r\n\r\nbad Origin";
        let e = parse_response(denied, "gw:7878", false).unwrap_err().to_string();
        assert!(e.contains("403"), "got: {e}");
        assert!(e.contains("bad Origin"), "the gateway's reason must reach the task log: {e}");

        let unauth = b"HTTP/1.1 401 Unauthorized\r\n\r\n";
        let e = parse_response(unauth, "gw:7878", false).unwrap_err().to_string();
        assert!(e.contains("DAGRON_MCP_STEP_GATEWAY_TOKEN"), "got: {e}");
    }

    /// The same 404 means two different things, and only one of them is fixed by
    /// retrying the task.
    #[test]
    fn a_404_is_an_expired_session_only_when_one_was_sent() {
        let gone = b"HTTP/1.1 404 Not Found\r\n\r\n";
        let e = parse_response(gone, "gw:7878", true).unwrap_err().to_string();
        assert!(e.contains("expired"), "got: {e}");

        let e = parse_response(gone, "gw:7878", false).unwrap_err().to_string();
        assert!(!e.contains("expired"), "a plain 404 is a wrong path, not a lost session: {e}");
    }

    #[test]
    fn a_session_id_is_captured_with_its_case_intact_and_a_malformed_one_is_refused() {
        let raw = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nMcp-Session-Id: AbC-123_xyz\r\n\r\n{\"id\":1,\"result\":{}}";
        let res = parse_response(raw, "h", false).unwrap();
        assert_eq!(
            res.session_id.as_deref(),
            Some("AbC-123_xyz"),
            "a gateway compares the id byte for byte, so case must survive"
        );

        let bad = b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nMcp-Session-Id: has space\r\n\r\n{\"id\":1,\"result\":{}}";
        let e = parse_response(bad, "h", false).unwrap_err().to_string();
        assert!(e.contains("visible ASCII"), "got: {e}");
    }

    #[test]
    fn loopback_is_recognised_in_every_form_an_operator_writes() {
        for host in ["127.0.0.1", "127.9.9.9", "localhost", "LocalHost", "[::1]", "::1"] {
            assert!(is_loopback(host), "{host} is loopback");
        }
        for host in ["10.0.0.5", "gateway.internal", "[2001:db8::1]"] {
            assert!(!is_loopback(host), "{host} is not loopback");
        }
    }
}
