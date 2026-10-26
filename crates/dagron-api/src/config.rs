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
            K("DAGRON_ARTIFACT_PRESIGN", "on (where the store signs)"),
            K("DAGRON_ARTIFACT_PRESIGN_SECS", "900"),
            K("DAGRON_ARTIFACT_RETENTION_DAYS", "— (artifacts are kept)"),
            K("DAGRON_ARTIFACT_SWEEP_SECS", "300 (0 disables)"),
            K("DAGRON_ARTIFACT_SYNC_SECS", "60 (0 disables)"),
            K("DAGRON_ARTIFACT_TIER", "— (tiering off)"),
            K("DAGRON_ARTIFACT_UPLINK_BYTES_PER_DAY", "— (unlimited)"),
            K("DAGRON_ARTIFACT_URL", "— (cloud artifact reads off)"),
            KR("DAGRON_AUDIT_SINK_TOKEN", "—"),
            KR("DAGRON_AUDIT_SINK_URL", "— (no central sink)"),
            // The store a GitHub workflow's artifacts are in, as the engine's.
            K(
                "DAGRON_CACHE_URL",
                "— (workflow artifacts listed, not downloaded)",
            ),
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
            // The `source` of the test CloudEvent the notification settings
            // send, as the engine's own.
            K("DAGRON_EVENTS_SOURCE", "/dagron"),
            K("DAGRON_GIT_ALLOW_INSECURE", "false"),
            KR("DAGRON_JWT_SECRET", "(required, ≥32 chars)"),
            // What a remote runner's task may keep of its log, as the engine
            // reads it for its own tasks.
            // The live log relay: how many runs it follows at once, and the
            // text its streams' channels hold.
            K("DAGRON_LOG_MAX_STREAMS", "2048"),
            K("DAGRON_LOG_MAX_TAILS", "256"),
            K("DAGRON_LOG_RELAY_BYTES", "67108864 (64 MiB)"),
            K("DAGRON_LOG_SEGMENT_BYTES", "8388608 (8 MiB)"),
            K("DAGRON_LOG_STORE", "— (each log in its task row)"),
            K("DAGRON_LOG_STREAM_LINGER_MS", "5000"),
            K("DAGRON_LOG_STREAM_POLL_MS", "1000"),
            K("DAGRON_LOG_TASK_CAP_BYTES", "67108864 (64 MiB)"),
            K("DAGRON_LOGIN_RATE_LIMIT", "30"),
            K("DAGRON_LOGIN_RATE_WINDOW_SECS", "60"),
            // How many runs may wait for one workflow's `max_active_runs`
            // (`when_full`). Read where a run is made, and a run is made
            // here as it is in the engine: set it the same on both.
            K("DAGRON_MAX_WAITING_RUNS", "100 (floor 1)"),
            // `GET /api/metrics` in two tiers: what has not finished, and
            // the history.
            K("DAGRON_METRICS_CACHE_SECS", "2 (0 disables)"),
            K(
                "DAGRON_METRICS_HISTORY_CACHE_SECS",
                "300, 0 with DAGRON_METRICS_CACHE_SECS=0 (0 disables)",
            ),
            // The OIDC issuer (`oidc`): its URL, its keys, and how long
            // a token lasts.
            K(
                "DAGRON_OIDC_ISSUER",
                "— (no issuer: a task is given no token)",
            ),
            K("DAGRON_OIDC_KEYS", "— (with DAGRON_OIDC_ISSUER)"),
            // Enterprise: the command that signs with the newest key, a KMS's.
            K("DAGRON_OIDC_SIGN_CMD", "— (the newest key is a private key)"),
            K("DAGRON_OIDC_TOKEN_SECS", "600 (60 to 3600)"),
            K("DAGRON_PW_HASH_CONCURRENCY", "2"),
            K("DAGRON_READY_REQUIRE_LISTENER", "false"),
            K("DAGRON_READY_TIMEOUT_MS", "500 (floor 50)"),
            // The remote runner protocol (`routes::runner`).
            K("DAGRON_RUNNER_BODY_LIMIT", "16777216 (16 MiB)"),
            K("DAGRON_RUNNER_CLAIM_RECHECK_MS", "1000 (floor 100)"),
            K("DAGRON_RUNNER_MIN_VERSION", "— (any release)"),
            K("DAGRON_SESSION_LEASE_SECS", "90 (floor 15)"),
            K("DAGRON_SESSION_TTL_SECS", "604800 (7 days)"),
            K("DAGRON_SHUTDOWN_DRAIN_MS", "15000"),
            // One node: how often live updates read the SQLite file.
            K("DAGRON_SQLITE_POLL_MS", "500 (floor 50; the sqlite build)"),
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
            // The engine's pools, which a remote runner's claims count
            // against as an engine's do: set them the same on both.
            K("POOLS", "— (no pools)"),
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

/// Every cargo feature the manifest declares that names a part of this binary,
/// in the manifest's order, and whether this build has it. `archive-cloud` is
/// plumbing the three object stores share, and not a part.
const FEATURES: &[(&str, bool)] = &[
    // The datastore, one of the two: what a one-node install is
    // built with is the first thing its startup line says.
    ("postgres", cfg!(feature = "postgres")),
    ("sqlite", cfg!(feature = "sqlite")),
    ("enterprise", cfg!(feature = "enterprise")),
    ("archive-s3", cfg!(feature = "archive-s3")),
    ("archive-gcs", cfg!(feature = "archive-gcs")),
    ("archive-azure", cfg!(feature = "archive-azure")),
    ("otel", cfg!(feature = "otel")),
];

/// The optional parts this binary was built with, as the cargo features that
/// compile them in. A lean build has none. Logged at startup: a setting that
/// needs a part the build lacks is refused only when it is used, and this is
/// how an operator knows what an image can do before asking it to.
pub fn built_with() -> Vec<&'static str> {
    FEATURES
        .iter()
        .filter_map(|(name, built)| built.then_some(*name))
        .collect()
}

/// Log the effective configuration once at startup: one line per knob that is
/// explicitly set (secrets redacted) plus the fingerprint — the record an
/// operator diffs against the reviewed deployment when a replica misbehaves.
/// The last line also names what the binary was built with.
pub fn log_effective() {
    for k in knobs() {
        if let Ok(v) = std::env::var(k.name) {
            let value = if k.redact { "<redacted>" } else { v.as_str() };
            tracing::info!(name = k.name, value, "config (env)");
        }
    }
    let features = match built_with() {
        parts if parts.is_empty() => "none".to_string(),
        parts => parts.join(","),
    };
    tracing::info!(fingerprint = %fingerprint(), %features, "configuration resolved");
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Env-var mutation is process-global; serialize the tests that do it
    /// (same discipline as the engine's config tests).
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// A feature added to the manifest and not to [`FEATURES`] would be a part
    /// an image has and its startup line does not say.
    #[test]
    fn every_part_the_manifest_declares_can_be_said() {
        let manifest = include_str!("../Cargo.toml");
        let table = manifest
            .split("\n[features]")
            .nth(1)
            .and_then(|rest| rest.split("\n[").next())
            .expect("the manifest's [features] table");
        // A feature is a line that starts at the margin with its name: not a
        // comment, and not a line of a list that runs over several.
        let declared: Vec<&str> = table
            .lines()
            .filter(|line| line.starts_with(|c: char| c.is_ascii_lowercase()))
            .filter_map(|line| line.split_once(" = ").map(|(name, _)| name))
            .filter(|name| *name != "archive-cloud" && *name != "default")
            .collect();
        let said: Vec<&str> = FEATURES.iter().map(|(name, _)| *name).collect();
        assert_eq!(said, declared);
    }

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
