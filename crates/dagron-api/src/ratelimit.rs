//! Per-client rate limiting for the unauthenticated login endpoint.
//!
//! `POST /api/login` is the one route anyone on the network can make expensive:
//! each call costs an Argon2id verify (~19 MiB of scratch, ~50-100 ms of CPU),
//! and it costs that even for an email that doesn't exist, because the
//! no-such-user path deliberately runs the same work to avoid leaking which
//! accounts are registered. [`crate::pwhash`] caps how much of that runs *at
//! once*; this caps how often a given client may ask for it, which is also the
//! usual brake on password guessing.
//!
//! Fixed window, in memory, per client key. Deliberately not distributed: with N
//! replicas each enforces its own window, so the effective limit is N × the
//! configured one. That is a cost ceiling, not an authentication control — for a
//! real lockout policy, put a shared store behind it.

use std::collections::HashMap;
use std::net::{IpAddr, SocketAddr};
use std::sync::Mutex;
use std::time::{Duration, Instant};

use axum::extract::{ConnectInfo, Request, State};
use axum::http::StatusCode;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};

use crate::state::AppState;

/// Default requests per window, per client.
const DEFAULT_MAX: u32 = 30;
/// Default window length.
const DEFAULT_WINDOW_SECS: u64 = 60;
/// Sweep expired buckets once the map passes this size, so a spray of forged
/// source addresses can't turn the limiter itself into the memory leak.
const SWEEP_THRESHOLD: usize = 4096;

/// Fixed-window counter keyed by client.
pub struct RateLimiter {
    max: u32,
    window: Duration,
    buckets: Mutex<HashMap<String, Bucket>>,
}

struct Bucket {
    /// When the current window started.
    started: Instant,
    /// Requests seen in the current window.
    count: u32,
}

impl RateLimiter {
    /// Build from env: `DAGRON_LOGIN_RATE_LIMIT` (requests, `0` disables) and
    /// `DAGRON_LOGIN_RATE_WINDOW_SECS`.
    pub fn from_env() -> Self {
        let max = std::env::var("DAGRON_LOGIN_RATE_LIMIT")
            .ok()
            .and_then(|v| v.parse::<u32>().ok())
            .unwrap_or(DEFAULT_MAX);
        let window_secs = std::env::var("DAGRON_LOGIN_RATE_WINDOW_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .filter(|&n| n > 0)
            .unwrap_or(DEFAULT_WINDOW_SECS);
        Self {
            max,
            window: Duration::from_secs(window_secs),
            buckets: Mutex::new(HashMap::new()),
        }
    }

    /// Count one request from `key`. `false` = over the limit, reject it.
    /// `DAGRON_LOGIN_RATE_LIMIT=0` disables the check entirely.
    pub fn check(&self, key: &str) -> bool {
        if self.max == 0 {
            return true;
        }
        let now = Instant::now();
        let mut buckets = match self.buckets.lock() {
            Ok(g) => g,
            // A panic in a previous holder must not take login down with it; the
            // counters are advisory, so recover and carry on.
            Err(poisoned) => poisoned.into_inner(),
        };
        if buckets.len() > SWEEP_THRESHOLD {
            let window = self.window;
            buckets.retain(|_, b| now.duration_since(b.started) < window);
        }
        let bucket = buckets
            .entry(key.to_string())
            .or_insert(Bucket { started: now, count: 0 });
        if now.duration_since(bucket.started) >= self.window {
            bucket.started = now;
            bucket.count = 0;
        }
        bucket.count += 1;
        bucket.count <= self.max
    }

    /// Seconds a rejected caller should wait — the remainder of its window.
    fn retry_after_secs(&self) -> u64 {
        self.window.as_secs().max(1)
    }
}

/// Middleware for `POST /api/login`: 429 once a client passes its window budget.
pub async fn login_rate_limit(
    State(state): State<AppState>,
    req: Request,
    next: Next,
) -> Response {
    let key = client_key(&req);
    if !state.login_limiter.check(&key) {
        tracing::warn!(client = %key, "login rate limit exceeded");
        return (
            StatusCode::TOO_MANY_REQUESTS,
            [("Retry-After", state.login_limiter.retry_after_secs().to_string())],
            "too many login attempts",
        )
            .into_response();
    }
    next.run(req).await
}

/// The client this request is attributed to.
///
/// The socket peer is the only value a caller cannot forge, so it is the
/// default. Behind a reverse proxy (the console's Next.js rewrite, an ingress)
/// every request shares that peer, which turns the per-client limit into one
/// global budget — set `DAGRON_TRUST_PROXY_HEADERS=true` there to key off the
/// first `X-Forwarded-For` hop instead. Only do that when something in front is
/// actually rewriting the header, since a direct caller can otherwise set it to
/// anything and get a fresh budget per request.
fn client_key(req: &Request) -> String {
    if trust_proxy_headers() {
        if let Some(fwd) = req.headers().get("x-forwarded-for").and_then(|v| v.to_str().ok()) {
            if let Some(first) = fwd.split(',').next().map(str::trim).filter(|s| !s.is_empty()) {
                // Normalize so "1.2.3.4" and " 1.2.3.4 " share a bucket, and a
                // junk value can't mint unbounded distinct keys of any length.
                if let Ok(ip) = first.parse::<IpAddr>() {
                    return ip.to_string();
                }
            }
        }
    }
    req.extensions()
        .get::<ConnectInfo<SocketAddr>>()
        .map(|ConnectInfo(addr)| addr.ip().to_string())
        // No peer address (not served with connect-info) — one shared bucket is
        // still a ceiling, and a wrong-but-bounded one beats no limit at all.
        .unwrap_or_else(|| "unknown".to_string())
}

fn trust_proxy_headers() -> bool {
    // Read once (LOW_LATENCY §5): a process's environment is fixed after boot,
    // and this sat on the hot path of every rate-limited request.
    static TRUST: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *TRUST.get_or_init(|| {
        std::env::var("DAGRON_TRUST_PROXY_HEADERS")
            .map(|v| matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes"))
            .unwrap_or(false)
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn limiter(max: u32, window_secs: u64) -> RateLimiter {
        RateLimiter {
            max,
            window: Duration::from_secs(window_secs),
            buckets: Mutex::new(HashMap::new()),
        }
    }

    #[test]
    fn allows_up_to_the_limit_then_rejects() {
        let rl = limiter(3, 60);
        assert!(rl.check("a"));
        assert!(rl.check("a"));
        assert!(rl.check("a"));
        assert!(!rl.check("a"), "the 4th request in the window must be rejected");
    }

    #[test]
    fn budgets_are_per_client() {
        let rl = limiter(1, 60);
        assert!(rl.check("a"));
        assert!(!rl.check("a"));
        assert!(rl.check("b"), "one client's spend must not consume another's budget");
    }

    #[test]
    fn the_window_resets() {
        let rl = limiter(1, 0);
        assert!(rl.check("a"));
        // A zero-length window is already elapsed, so the next call starts a new one.
        assert!(rl.check("a"));
    }

    #[test]
    fn zero_disables_the_limit() {
        let rl = limiter(0, 60);
        for _ in 0..1000 {
            assert!(rl.check("a"));
        }
    }

    #[test]
    fn expired_buckets_are_swept() {
        let rl = limiter(1, 0);
        for i in 0..(SWEEP_THRESHOLD + 2) {
            rl.check(&format!("client-{i}"));
        }
        let len = rl.buckets.lock().unwrap().len();
        assert!(len <= SWEEP_THRESHOLD + 1, "map should be swept, got {len} entries");
    }
}
