//! The built-in `defer.http` poller: GET a status endpoint, read a verdict out
//! of the JSON.
//!
//! One adapter reaches Databricks, EMR Serverless, Dataproc, Livy, Kyuubi, YARN
//! and a SparkApplication CR, because every one of them answers "is it done?"
//! with a document containing a state field. The consequence worth stating:
//! **dagron carries no vendor code**, so an API change is a YAML edit by the
//! person it affects rather than a dagron release.
//!
//! ## Why this has its own HTTP client
//!
//! [`crate::wait_url`] already polls endpoints, and its client would work. It
//! must not be reused, for one reason: `WAIT_URL_DENY_PRIVATE` is **off by
//! default**, which is the right call there and the wrong one here. A
//! `wait.url` sensor issues an unauthenticated GET and its primary use is an
//! in-cluster address, so blocking private ranges by default would break the
//! feature for the majority. A `defer.http` poll carries a **bearer token**, so
//! the same permissiveness means a workflow author can aim a credential the
//! operator configured at any address the scheduler can reach — including
//! `169.254.169.254`.
//!
//! So the policy inverts: deny-private is **on** unless turned off, and the way
//! to poll an in-cluster endpoint is to name it in `DEFER_HTTP_ALLOW_HOSTS`.
//! That is one line of configuration for the operator who wants it, and a
//! closed door for everyone who has not thought about it.
//!
//! Everything else follows `wait_url`: the filter lives inside the resolver, so
//! the addresses checked are the addresses dialled and a DNS rebind has no
//! window; and redirects are refused outright, because a 3xx target is invisible
//! to both checks.

use std::collections::BTreeSet;

use anyhow::{bail, Context, Result};
use dagron_core::dag::{DeferHttpCancel, DeferHttpSpec, HANDLE_PLACEHOLDER, MAX_EXTERNAL_ERROR_BYTES};
use dagron_core::jsonpred::{Path, Predicate};
use serde_json::Value;

use crate::hooks::Verdict;

/// Per-request deadline. Bounded tightly because the sweep polls a batch and a
/// black-holed endpoint would otherwise hold the reconcile tick; the row is
/// re-parked and tried again in `poll_secs` either way.
const REQUEST_TIMEOUT_SECS: u64 = 15;

/// Ceiling on the status document, enforced **while reading**.
///
/// A deadline bounds how long a response may take, not how large it may be: an
/// endpoint that streams steadily can send gigabytes inside 15 seconds. And
/// this poller runs in the *scheduler*, not in a task — so an oversized body
/// exhausts the control plane and stops every parked job in the fleet, rather
/// than failing the one task that asked for it.
///
/// `Content-Length` is not a substitute. It is the sender's claim about a body
/// it has not finished writing, so it is checked by counting the bytes that
/// actually arrive. 256 KiB is far beyond any real status document; a vendor
/// that genuinely needs more is diagnosable from the refusal, which re-parks
/// the row rather than failing the job.
const MAX_STATUS_BYTES: usize = 256 * 1024;

/// Is the private-address block active? **On unless explicitly disabled** — the
/// inverse of `WAIT_URL_DENY_PRIVATE`, because this client carries credentials.
pub(crate) fn deny_private_enabled() -> bool {
    std::env::var("DEFER_HTTP_DENY_PRIVATE")
        .map(|v| !matches!(v.trim().to_ascii_lowercase().as_str(), "0" | "false" | "off" | "no"))
        .unwrap_or(true)
}

/// Hosts exempt from the private-address block (`DEFER_HTTP_ALLOW_HOSTS`,
/// comma-separated). This is how an operator polls the in-cluster Spark
/// operator or a private Livy: name the host once, deliberately, rather than
/// opening every address to every workflow.
///
/// Matched on the hostname as written in the URL, case-insensitively. Not a
/// pattern language: a wildcard here would be a way to re-open the door by
/// accident, and the set of endpoints an installation polls is small and known.
pub(crate) fn allow_hosts() -> BTreeSet<String> {
    std::env::var("DEFER_HTTP_ALLOW_HOSTS")
        .ok()
        .map(|raw| {
            raw.split(',')
                .map(|h| h.trim().to_ascii_lowercase())
                .filter(|h| !h.is_empty())
                .collect()
        })
        .unwrap_or_default()
}

/// Resolver that drops non-global addresses unless the host is allowlisted.
pub(crate) struct GuardedResolver {
    allow: BTreeSet<String>,
}

impl reqwest::dns::Resolve for GuardedResolver {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        // `exempt` is decided here (it needs `self`); the owned host string is
        // built inside the future, the shape wait_url uses — the boxed future
        // must be 'static, so nothing may borrow from this frame.
        let exempt = self.allow.contains(&name.as_str().to_ascii_lowercase());
        Box::pin(async move {
            let host = name.as_str().to_ascii_lowercase();
            // Collected, not passed through: `lookup_host`'s iterator borrows
            // the host string, and the boxed `Addrs` outlives this frame.
            let resolved: Vec<std::net::SocketAddr> =
                tokio::net::lookup_host((host.as_str(), 0)).await?.collect();
            if exempt {
                return Ok(Box::new(resolved.into_iter()) as reqwest::dns::Addrs);
            }
            let allowed: Vec<std::net::SocketAddr> = resolved
                .into_iter()
                .filter(|sa| !crate::wait_url::is_blocked_ip(sa.ip()))
                .collect();
            if allowed.is_empty() {
                return Err(format!(
                    "defer.http host '{host}' resolves only to non-global addresses. This \
                     poll carries the task's headers, so the block is on by default — name \
                     the host in DEFER_HTTP_ALLOW_HOSTS to permit it deliberately, or set \
                     DEFER_HTTP_DENY_PRIVATE=0 to disable the policy entirely."
                )
                .into());
            }
            Ok(Box::new(allowed.into_iter()) as reqwest::dns::Addrs)
        })
    }
}

/// Build the poller's client.
pub(crate) fn client() -> Result<reqwest::Client> {
    let mut b = reqwest::Client::builder()
        // A 3xx target is invisible to the resolver filter, so a redirect is
        // refused rather than chased — same rule as wait.url, same reason.
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(REQUEST_TIMEOUT_SECS));
    if deny_private_enabled() {
        b = b.dns_resolver(std::sync::Arc::new(GuardedResolver { allow: allow_hosts() }))
            // Without this, a configured HTTP(S)_PROXY defeats the resolver
            // entirely: reqwest resolves the *proxy* host — which passes the
            // filter — and asks it to connect to the private address. The
            // guard would report a clean sweep while the request reached the
            // metadata service, and these requests carry the task's own
            // Authorization header. `wait_url`'s client has called `.no_proxy()`
            // for the same reason since it was written; this one did not.
            .no_proxy();
    }
    b.build().context("building the defer.http client")
}

/// The literal-address check `fetch` and [`send_cancel`] both apply before a
/// request leaves: see the comment at `fetch`'s call site for why it exists.
fn refuse_blocked_literal(url: &str) -> Result<()> {
    if deny_private_enabled() && crate::wait_url::literal_host_blocked(url) {
        bail!(
            "defer.http url resolves to a blocked address literal: {url}. \
             Set DEFER_HTTP_DENY_PRIVATE=0 to allow private targets, or name the \
             host in DEFER_HTTP_ALLOW_HOSTS and address it by name."
        );
    }
    Ok(())
}

/// Fetch the status document. A non-2xx or an unparseable body is an **error**,
/// never a verdict: the sweep re-parks on `Err`, and the alternative is failing
/// a healthy six-hour job because its vendor answered 503 once.
pub(crate) async fn fetch(
    client: &reqwest::Client,
    url: &str,
    headers: &[(String, String)],
) -> Result<Value> {
    // An IP-literal URL never reaches the resolver, so `GuardedResolver` never
    // sees it — `http://169.254.169.254/...` would be fetched with the task's
    // Authorization header attached. Same check `wait_url` applies before its
    // probe, sharing the same tested predicate rather than a second copy of the
    // range list. Hostnames return `false` here and are judged by the resolver,
    // where `DEFER_HTTP_ALLOW_HOSTS` exemptions still apply.
    refuse_blocked_literal(url)?;
    let mut req = client.get(url);
    for (name, value) in headers {
        req = req.header(name.as_str(), value.as_str());
    }
    let resp = req.send().await.with_context(|| format!("GET {url}"))?;
    let status = resp.status();
    let body = read_bounded(resp, url).await?;
    if !status.is_success() {
        // The body can carry the credential back in an error envelope, and it
        // is truncated here because a vendor error page is not a useful log
        // line — the memory bound is `read_bounded`'s, applied while reading.
        bail!("status endpoint returned {status}: {}", truncate(&body, 512));
    }
    serde_json::from_str(&body)
        .with_context(|| format!("status endpoint returned {status} but not JSON"))
}

/// The request `defer.http.cancel` sends for one job: `(method, url, body)`,
/// with `{{ handle }}` bound against the handle on the row. Pure, so the shape
/// is testable without a network.
pub(crate) fn cancel_request(c: &DeferHttpCancel, handle: &str) -> (String, String, Option<String>) {
    (
        c.method(),
        c.url.trim().replace(HANDLE_PLACEHOLDER, handle),
        c.body.as_ref().map(|b| b.replace(HANDLE_PLACEHOLDER, &json_escaped(handle))),
    )
}

/// `handle` as JSON string *content* — escaped, without the surrounding quotes.
///
/// In a body the placeholder sits inside a string literal (`{"run_id": "{{ handle }}"}`),
/// so a raw splice of a handle carrying a `"` or a `\` emits a document the vendor
/// rejects. That reads as a failed cancel, spends all three attempts, and orphans a job
/// that is still running — the failure this block exists to prevent. The URL is spliced
/// raw, as the poll URL has always been: same placeholder, same rules, one behaviour.
fn json_escaped(handle: &str) -> String {
    let quoted = Value::String(handle.to_string()).to_string();
    // `Value::String` always renders with both quotes, and `"` is one ASCII byte.
    quoted[1..quoted.len() - 1].to_string()
}

/// The `content-type` to add for a cancel body, if the block does not carry one.
///
/// reqwest's `header()` APPENDS, so adding a `content-type` the author already set in
/// `defer.http.headers` sends the request with two of them. That is malformed per
/// RFC 9110 and answered `400` by strict servers, the Kubernetes apiserver among them —
/// and someone told the body "is sent as application/json" may well set the header
/// themselves, which is exactly the pairing that breaks.
fn cancel_content_type(headers: &[dagron_core::dag::EnvVar]) -> Option<&'static str> {
    headers
        .iter()
        .all(|h| !h.name.eq_ignore_ascii_case("content-type"))
        .then_some("application/json")
}

/// Whether a cancel's status means the job is gone. 404 and 410 count: the
/// thing we were asked to stop no longer exists, which is the outcome wanted,
/// and retrying a cancel of a finished job would burn the attempt budget to an
/// "orphan" that was never running.
pub(crate) fn cancel_landed(status: u16) -> bool {
    (200..300).contains(&status) || status == 404 || status == 410
}

/// Send `defer.http.cancel` for one job. `Err` on anything but [`cancel_landed`],
/// so the sweep spends an attempt on it and gives up after three.
///
/// Same client, same address guards, same header redaction as the poll: it
/// carries the same bearer token to the same kind of endpoint.
pub(crate) async fn send_cancel(
    client: &reqwest::Client,
    cancel: &DeferHttpCancel,
    handle: &str,
    headers: &[dagron_core::dag::EnvVar],
) -> Result<()> {
    let redactor = dagron_executor::redact::Redactor::from_task_env(headers);
    let (method, url, body) = cancel_request(cancel, handle);
    let run = async {
        refuse_blocked_literal(&url)?;
        let method = reqwest::Method::from_bytes(method.as_bytes())?;
        let mut req = client.request(method.clone(), &url);
        for h in headers {
            req = req.header(h.name.as_str(), h.value.as_str());
        }
        if let Some(b) = body {
            if let Some(ct) = cancel_content_type(headers) {
                req = req.header("content-type", ct);
            }
            req = req.body(b);
        }
        let resp = req.send().await.with_context(|| format!("{method} {url}"))?;
        let status = resp.status();
        if cancel_landed(status.as_u16()) {
            return Ok(());
        }
        let body = read_bounded(resp, &url).await?;
        bail!("cancel endpoint returned {status}: {}", truncate(&body, 512))
    };
    // Redact before the error text goes anywhere, as the poll does.
    run.await.map_err(|e: anyhow::Error| anyhow::anyhow!("{}", redactor.redact(&e.to_string()).into_owned()))
}

/// Decide a verdict from the document. Pure, so every branch is testable
/// without a network.
///
/// **`fail_when` is checked first**, and that ordering is a decision rather than
/// an accident. If an author's two predicates overlap, one of them is wrong —
/// and the two mistakes are not equally expensive. A false failure costs a
/// retry; a false success advances every dependent task on a job that did not
/// produce what they consume. So the more alarming predicate wins, and the
/// overlap is logged by the caller rather than silently resolved.
pub(crate) fn decide(spec: &DeferHttpSpec, doc: &Value) -> Result<Verdict> {
    if let Some(raw) = &spec.fail_when {
        // Re-parsed here rather than carried: validation already proved these
        // parse, so this cannot fail for a spec that was admitted — and
        // re-parsing keeps the poller a pure function of the persisted spec,
        // with no state to get out of step with it.
        let pred = Predicate::parse(raw).map_err(|e| anyhow::anyhow!("fail_when: {e}"))?;
        if pred.eval(doc) {
            let reason = match &spec.error_from {
                Some(p) => {
                    let path = Path::parse(p).map_err(|e| anyhow::anyhow!("error_from: {e}"))?;
                    match path.get(doc).and_then(as_text) {
                        Some(t) => format!("remote job failed ({raw}): {}", truncate(&t, MAX_EXTERNAL_ERROR_BYTES)),
                        None => format!("remote job failed ({raw}); {p} was not present in the response"),
                    }
                }
                None => format!("remote job failed ({raw})"),
            };
            return Ok(Verdict::Failed { reason });
        }
    }
    let succeed = Predicate::parse(&spec.succeed_when)
        .map_err(|e| anyhow::anyhow!("succeed_when: {e}"))?;
    if succeed.eval(doc) {
        // The matched predicate, not the whole document: a status response can
        // be large, and `output` is what a downstream `when:` reads.
        return Ok(Verdict::Succeeded { output: spec.succeed_when.clone() });
    }
    Ok(Verdict::Running)
}

/// Whether both predicates matched — an authoring mistake worth one log line.
pub(crate) fn predicates_overlap(spec: &DeferHttpSpec, doc: &Value) -> bool {
    let Some(f) = &spec.fail_when else { return false };
    let (Ok(fp), Ok(sp)) = (Predicate::parse(f), Predicate::parse(&spec.succeed_when)) else {
        return false;
    };
    fp.eval(doc) && sp.eval(doc)
}

/// A JSON value as human-readable text for a failure reason. An object or array
/// is rendered rather than dropped — some vendors nest the message.
fn as_text(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Null => None,
        other => Some(other.to_string()),
    }
}

/// Truncate on a character boundary, marking that it happened.
/// Read the body chunk by chunk, refusing once it exceeds [`MAX_STATUS_BYTES`].
///
/// `Response::text()` would buffer whatever arrives before anything could look
/// at it, which puts the bound after the damage. Counting as the chunks land
/// means an oversized body stops being read rather than being read in full and
/// rejected afterwards.
///
/// A read error yields the bytes already collected rather than failing: a
/// truncated body fails the JSON parse with a clearer message than a transport
/// error does, and the caller treats either as "no answer" and re-parks.
async fn read_bounded(mut resp: reqwest::Response, url: &str) -> Result<String> {
    let mut buf: Vec<u8> = Vec::new();
    while let Ok(Some(chunk)) = resp.chunk().await {
        admit_chunk(&mut buf, &chunk, url)?;
    }
    Ok(String::from_utf8_lossy(&buf).into_owned())
}

/// Append one chunk, or refuse the whole read. Pure, so the bound is testable
/// without a network — the same reason [`decide`] is.
///
/// The check is `already + arriving`, not `already`: testing only what is
/// already buffered admits one final chunk of any size, which is the whole
/// exposure when the sender controls the chunking.
fn admit_chunk(buf: &mut Vec<u8>, chunk: &[u8], url: &str) -> Result<()> {
    if buf.len() + chunk.len() > MAX_STATUS_BYTES {
        bail!(
            "status endpoint body exceeded {MAX_STATUS_BYTES} bytes and was refused \
             unread: GET {url}. A status document is small, so this is a broken or \
             hostile endpoint — and buffering it would exhaust the scheduler rather \
             than one task."
        );
    }
    buf.extend_from_slice(chunk);
    Ok(())
}

fn truncate(s: &str, max: usize) -> String {
    if s.len() <= max {
        return s.to_string();
    }
    let mut end = max;
    while end > 0 && !s.is_char_boundary(end) {
        end -= 1;
    }
    format!("{}… [truncated, {} bytes total]", &s[..end], s.len())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn spec(succeed: &str, fail: Option<&str>, err: Option<&str>) -> DeferHttpSpec {
        DeferHttpSpec {
            url: "https://h/j".into(),
            headers: vec![],
            succeed_when: succeed.into(),
            fail_when: fail.map(str::to_string),
            error_from: err.map(str::to_string),
            cancel: None,
        }
    }

    #[test]
    fn the_cancel_request_defaults_to_delete_and_binds_the_handle() {
        let del = DeferHttpCancel { url: " https://h/j/{{ handle }} ".into(), method: None, body: None };
        assert_eq!(
            cancel_request(&del, "dagron-abc-0"),
            ("DELETE".to_string(), "https://h/j/dagron-abc-0".to_string(), None)
        );

        let post = DeferHttpCancel {
            url: "https://h/cancel".into(),
            method: Some("post".into()),
            body: Some(r#"{"run_id": "{{ handle }}"}"#.into()),
        };
        let (m, u, b) = cancel_request(&post, "42");
        assert_eq!((m.as_str(), u.as_str()), ("POST", "https://h/cancel"));
        assert_eq!(b.as_deref(), Some(r#"{"run_id": "42"}"#));
    }

    /// A handle is escaped where it lands inside a JSON string literal, so a vendor
    /// id carrying a quote or a backslash still produces a document that parses.
    #[test]
    fn a_handle_with_json_metacharacters_still_yields_a_parseable_body() {
        let c = DeferHttpCancel {
            url: "https://h/cancel".into(),
            method: Some("POST".into()),
            body: Some(r#"{"run_id": "{{ handle }}"}"#.into()),
        };
        // A handle that would otherwise close the string and inject a second key.
        let (_, _, body) = cancel_request(&c, r#"a", "admin": true, "x": "b"#);
        let body = body.expect("a body was configured");
        let v: Value =
            serde_json::from_str(&body).unwrap_or_else(|e| panic!("still parses: {body}: {e}"));
        assert_eq!(v["run_id"], r#"a", "admin": true, "x": "b"#, "round-trips intact");
        assert!(v.get("admin").is_none(), "no key was injected: {body}");

        // A backslash, and the ordinary case, are both unremarkable.
        let (_, _, back) = cancel_request(&c, r"dir\job");
        let v: Value = serde_json::from_str(&back.unwrap()).unwrap();
        assert_eq!(v["run_id"], r"dir\job");
        let (_, _, plain) = cancel_request(&c, "dagron-abc-0");
        assert_eq!(plain.as_deref(), Some(r#"{"run_id": "dagron-abc-0"}"#));
    }

    /// The body's `content-type` is added only when the block does not already carry
    /// one: `header()` appends, and two `Content-Type`s is a malformed request.
    #[test]
    fn a_cancel_body_does_not_stack_a_second_content_type() {
        let hdr = |n: &str| dagron_core::dag::EnvVar {
            name: n.into(),
            value: "application/json".into(),
            value_from: None,
        };
        assert_eq!(cancel_content_type(&[]), Some("application/json"), "none set: we add it");
        assert_eq!(cancel_content_type(&[hdr("authorization")]), Some("application/json"));
        assert_eq!(cancel_content_type(&[hdr("content-type")]), None, "already set: leave it");
        assert_eq!(
            cancel_content_type(&[hdr("Content-Type")]),
            None,
            "header names are case-insensitive"
        );
    }

    #[test]
    fn a_missing_job_counts_as_torn_down_but_a_server_error_does_not() {
        for ok in [200, 202, 204, 404, 410] {
            assert!(cancel_landed(ok), "{ok}");
        }
        for bad in [301, 400, 401, 403, 409, 429, 500, 503] {
            assert!(!cancel_landed(bad), "{bad}");
        }
    }

    #[test]
    fn the_three_verdicts() {
        let s = spec("state == COMPLETED", Some("state in [FAILED, KILLED]"), None);
        assert!(matches!(
            decide(&s, &json!({"state": "COMPLETED"})).unwrap(),
            Verdict::Succeeded { .. }
        ));
        assert!(matches!(
            decide(&s, &json!({"state": "FAILED"})).unwrap(),
            Verdict::Failed { .. }
        ));
        assert!(matches!(decide(&s, &json!({"state": "RUNNING"})).unwrap(), Verdict::Running));
        // A document that has not grown the field yet is Running, not Failed —
        // the missing-path rule, and the reason it is `false` for every form.
        assert!(matches!(decide(&s, &json!({})).unwrap(), Verdict::Running));
    }

    #[test]
    fn the_vendors_own_error_text_becomes_the_failure_reason() {
        let s = spec("state == DONE", Some("state == FAILED"), Some("status.message"));
        let doc = json!({"state": "FAILED", "status": {"message": "container OOMKilled"}});
        let Verdict::Failed { reason } = decide(&s, &doc).unwrap() else { panic!("expected Failed") };
        assert!(reason.contains("container OOMKilled"), "{reason}");
        // …and an error_from that is not there says so rather than looking empty.
        let bare = json!({"state": "FAILED"});
        let Verdict::Failed { reason } = decide(&s, &bare).unwrap() else { panic!() };
        assert!(reason.contains("was not present"), "{reason}");
    }

    /// A vendor stack trace must not reach the task's `output` column in full —
    /// nothing on the write path caps it.
    #[test]
    fn a_huge_error_field_is_truncated_before_it_reaches_the_task() {
        let s = spec("state == DONE", Some("state == FAILED"), Some("err"));
        let doc = json!({"state": "FAILED", "err": "x".repeat(100_000)});
        let Verdict::Failed { reason } = decide(&s, &doc).unwrap() else { panic!() };
        assert!(reason.len() < MAX_EXTERNAL_ERROR_BYTES + 256, "len {}", reason.len());
        assert!(reason.contains("truncated"), "says it truncated: {reason}");
    }

    /// Overlapping predicates resolve to Failed, because a false success
    /// advances dependents on a job that produced nothing and a false failure
    /// only costs a retry.
    #[test]
    fn when_both_predicates_match_the_alarming_one_wins() {
        let s = spec("state != RUNNING", Some("state == FAILED"), None);
        let doc = json!({"state": "FAILED"});
        assert!(predicates_overlap(&s, &doc), "this spec's predicates do overlap");
        assert!(matches!(decide(&s, &doc).unwrap(), Verdict::Failed { .. }));
    }

    #[test]
    fn deny_private_is_on_unless_turned_off_and_the_allowlist_parses() {
        // The inverse of wait_url's default — asserted, because the whole
        // security argument for this module rests on it.
        assert!(
            super::deny_private_enabled(),
            "unset must mean ON here (a defer.http poll carries a bearer token)"
        );
        assert!(allow_hosts().is_empty(), "unset means no exemptions");
    }


    /// The scheduler polls these endpoints, so an unbounded body is not one
    /// task's problem: `resp.text()` would buffer whatever arrived and take the
    /// control plane down, stopping every parked job in the fleet.
    #[test]
    fn an_oversized_body_is_refused_rather_than_buffered() {
        let mut buf = Vec::new();
        let chunk = vec![b'x'; MAX_STATUS_BYTES + 1];
        let err = admit_chunk(&mut buf, &chunk, "http://x/").unwrap_err();
        assert!(err.to_string().contains("exceeded"), "{err}");
        assert!(buf.is_empty(), "the refused chunk is not kept either");
    }

    /// The bound is on the total, not on each chunk — otherwise a sender that
    /// chunks finely streams any size past it.
    #[test]
    fn the_bound_is_cumulative_across_chunks() {
        let mut buf = Vec::new();
        let chunk = vec![b'x'; 1024];
        let mut refused_at = None;
        for i in 0.. {
            if admit_chunk(&mut buf, &chunk, "http://x/").is_err() {
                refused_at = Some(i);
                break;
            }
        }
        assert_eq!(refused_at, Some(MAX_STATUS_BYTES / 1024), "refused exactly at the cap");
        assert!(buf.len() <= MAX_STATUS_BYTES);
    }

    /// The check must not refuse a body that exactly fills the budget: a real
    /// status document that happens to land on the boundary is not hostile.
    #[test]
    fn a_body_exactly_at_the_cap_is_admitted() {
        let mut buf = Vec::new();
        let chunk = vec![b'x'; MAX_STATUS_BYTES];
        admit_chunk(&mut buf, &chunk, "http://x/").expect("exactly at the cap is fine");
        assert_eq!(buf.len(), MAX_STATUS_BYTES);
        admit_chunk(&mut buf, b"x", "http://x/").expect_err("one byte past it is not");
    }

    #[test]
    fn truncate_never_splits_a_character() {
        let s = "é".repeat(100);
        let out = truncate(&s, 11);
        assert!(out.starts_with("ééééé"));
        assert!(out.contains("truncated"));
    }
}
