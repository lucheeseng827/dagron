//! dagron-api's slice of central settings management (docs/LOW_LATENCY.md §5).
//!
//! The API edge is configured by environment variables only — its deployment
//! surface (Helm / compose) is already the reviewed file. What this module
//! adds is the same *governance* the engine got: a registry of every knob this
//! process reads, an effective-configuration line at startup, and a stable
//! fingerprint so a fleet can alert when one replica's configuration drifts
//! from the reviewed set. (The `DAGRON_CONFIG` file/profile layer stays an
//! engine feature — every profile knob is an engine knob.)
//!
//! Per-request `env::var` reads elsewhere in this crate are memoized through
//! `OnceLock` (see `ratelimit`, `login`, `gitrepos`): a process's environment
//! never changes after boot, so reading it per request bought nothing and made
//! the effective configuration harder to reason about. Boot-immutable is the
//! policy, matching the engine.

use std::sync::OnceLock;

/// Every env var this binary reads, with the default shown when unset and
/// whether the value is a secret. Keep sorted; docs/CONFIG.md mirrors it.
pub struct Knob {
    pub name: &'static str,
    pub default: &'static str,
    pub redact: bool,
}

const K: fn(&'static str, &'static str) -> Knob =
    |name, default| Knob { name, default, redact: false };
const KR: fn(&'static str, &'static str) -> Knob =
    |name, default| Knob { name, default, redact: true };

pub fn knobs() -> &'static [Knob] {
    static KNOBS: OnceLock<Vec<Knob>> = OnceLock::new();
    KNOBS.get_or_init(|| {
        vec![
            KR("DAGRON_ADMIN_EMAIL", "— (no admin seed)"),
            K("DAGRON_ADMIN_NAME", "Administrator"),
            KR("DAGRON_ADMIN_PASSWORD", "— (no admin seed)"),
            K("DAGRON_ARTIFACT_DIR", "— (artifact API off)"),
            K("DAGRON_ARTIFACT_MAX_BYTES", "134217728 (128 MiB)"),
            K("DAGRON_ARTIFACT_SYNC_SECS", "60 (0 disables)"),
            K("DAGRON_ARTIFACT_TIER", "— (tiering off)"),
            K("DAGRON_ARTIFACT_UPLINK_BYTES_PER_DAY", "— (unlimited)"),
            K("DAGRON_ARTIFACT_URL", "— (cloud artifact reads off)"),
            KR("DAGRON_AUDIT_SINK_TOKEN", "—"),
            KR("DAGRON_AUDIT_SINK_URL", "— (no central sink)"),
            K("DAGRON_COOKIE_SECURE", "true"),
            K("DAGRON_DB_ACQUIRE_TIMEOUT_MS", "10000"),
            K("DAGRON_DB_MAX_CONNECTIONS", "8"),
            K("DAGRON_DB_MIN_CONNECTIONS", "0 (no warm floor)"),
            K("DAGRON_DB_TEST_BEFORE_ACQUIRE", "true"),
            // The KEK/KMS envelope family (dagron-crypto, docs/CONFIG.md
            // "Encryption at rest"): this binary encrypts environment secrets
            // on write and serves `POST /api/artifacts/rotate`, which reads
            // the `*_OLD` twins for the retiring KEK — so the fingerprint
            // must cover a rotation in flight. Same rows as the engine's
            // registry; key material redacted, selectors/ids/commands shown.
            KR("DAGRON_ENV_KEK", "— (local provider only)"),
            KR("DAGRON_ENV_KEK_OLD", "— (rotation source off)"),
            K("DAGRON_ENV_KEK_PROVIDER", "— (v1 single-key path)"),
            K("DAGRON_ENV_KEK_PROVIDER_OLD", "— (rotation source off)"),
            K("DAGRON_ENV_KMS_ID", "provider default"),
            K("DAGRON_ENV_KMS_ID_OLD", "provider default"),
            K("DAGRON_ENV_KMS_KEY_ID", "—"),
            K("DAGRON_ENV_KMS_KEY_ID_OLD", "—"),
            K("DAGRON_ENV_KMS_KEY_VERSION", "—"),
            K("DAGRON_ENV_KMS_KEY_VERSION_OLD", "—"),
            K("DAGRON_ENV_KMS_TIMEOUT_SECS", "30"),
            K("DAGRON_ENV_KMS_UNWRAP_CMD", "— (command provider only)"),
            K("DAGRON_ENV_KMS_UNWRAP_CMD_OLD", "—"),
            KR("DAGRON_ENV_KMS_VAULT_URL", "—"),
            KR("DAGRON_ENV_KMS_VAULT_URL_OLD", "—"),
            K("DAGRON_ENV_KMS_WRAP_CMD", "— (command provider only)"),
            K("DAGRON_ENV_KMS_WRAP_CMD_OLD", "—"),
            KR("DAGRON_ENV_SECRET_KEY", "— (env-secret store off)"),
            K("DAGRON_GIT_ALLOW_INSECURE", "false"),
            KR("DAGRON_JWT_SECRET", "(required, ≥32 chars)"),
            K("DAGRON_LOGIN_RATE_LIMIT", "30"),
            K("DAGRON_LOGIN_RATE_WINDOW_SECS", "60"),
            K("DAGRON_PW_HASH_CONCURRENCY", "2"),
            K("DAGRON_READY_REQUIRE_LISTENER", "false"),
            K("DAGRON_READY_TIMEOUT_MS", "500 (floor 50)"),
            K("DAGRON_SESSION_TTL_SECS", "604800 (7 days)"),
            K("DAGRON_SHUTDOWN_DRAIN_MS", "15000"),
            K("DAGRON_TRUST_PROXY_HEADERS", "false"),
            KR("DATABASE_LISTEN_URL", "— (share the pool DSN)"),
            KR("DATABASE_URL", "(required)"),
            K("GC_ARCHIVE_DIR", "— (archive reads off)"),
            K("GC_ARCHIVE_URL", "— (archive reads off)"),
            K("GIT_API_BASE", "https://api.github.com"),
            K("GIT_BASE", "main"),
            K("GIT_PATH_PREFIX", "dags/"),
            KR("GIT_REPO", "— (git sync off)"),
            KR("GITHUB_TOKEN", "— (git sync off)"),
            K("LOG_FORMAT", "full"),
            K("LOG_LEVEL", "info"),
            K("LOG_SPAN_EVENTS", "— (off)"),
            K("PORT", "8080"),
            K("RUST_LOG", "— (LOG_LEVEL applies)"),
        ]
    })
}

/// Stable fingerprint of the raw effective configuration (FNV-1a 64 over
/// `name=value` in registry order; unset knobs hash as the bare name). Same
/// construction as the engine's, so dashboards treat both uniformly. Drift
/// detection, not a security hash.
pub fn fingerprint() -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let mut eat = |bytes: &[u8]| {
        for &b in bytes {
            h ^= b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
    };
    for k in knobs() {
        eat(k.name.as_bytes());
        if let Ok(v) = std::env::var(k.name) {
            eat(b"=");
            eat(v.as_bytes());
        }
        eat(b"\n");
    }
    format!("{h:016x}")
}

/// Log the effective configuration once at startup: one line per knob that is
/// explicitly set (secrets redacted) plus the fingerprint — the record an
/// operator diffs against the reviewed deployment when a replica misbehaves.
pub fn log_effective() {
    for k in knobs() {
        if let Ok(v) = std::env::var(k.name) {
            let value = if k.redact { "<redacted>" } else { v.as_str() };
            tracing::info!(name = k.name, value, "config (env)");
        }
    }
    tracing::info!(fingerprint = %fingerprint(), "configuration resolved");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Env-var mutation is process-global; serialize the tests that do it
    /// (same discipline as the engine's config tests).
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    #[test]
    fn fingerprint_is_stable_and_moves_with_knobs() {
        let _guard = ENV_LOCK.lock().unwrap();
        let a = fingerprint();
        assert_eq!(a, fingerprint());
        std::env::set_var("GIT_BASE", "release");
        let b = fingerprint();
        std::env::remove_var("GIT_BASE");
        assert_ne!(a, b);
        assert_eq!(a, fingerprint());
    }
}
