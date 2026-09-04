//! Central server-settings management (docs/LOW_LATENCY.md §5).
//!
//! The engine's configuration surface is env vars — ~70 of them across the
//! crates this binary links. This module makes that surface *governable*
//! without changing how any knob is read:
//!
//! * **Registry** — every knob the engine binary reads, in one table, with its
//!   default and whether its value is a secret. `docs/CONFIG.md` documents the
//!   same list; the registry is what makes the rest of this module possible.
//! * **File layer + profiles** (`DAGRON_CONFIG=/etc/dagron/dagron.yaml`) — a
//!   reviewable YAML file layered UNDER the environment (env always wins),
//!   with an optional `profile:` applying a named preset under both. YAML, not
//!   TOML, because everything else dagron reads is YAML. Precedence:
//!   env > file > profile > compiled default.
//! * **Typo detection** — an env var that looks like one of ours but isn't in
//!   the registry warns at startup (`POLL_INTERVAL_M` silently doing nothing
//!   is the failure mode this kills); an unknown key in the config *file* is a
//!   hard startup error, because a reviewed file has no excuse.
//! * **Introspection** — `dagron config [--json]` prints every knob's
//!   effective value (secrets redacted) and where it came from
//!   (env / file / profile / default); the ops API serves the same at
//!   `GET /config`.
//! * **Fingerprint** — a stable 64-bit hash of the raw effective values,
//!   logged at startup and served with the introspection, so a fleet can
//!   alert on "replica 3 is not running the reviewed configuration".
//!
//! Deliberately NOT here: hot reload. Boot-immutability is the audit story a
//! regulated operator wants; the short allow-list of runtime tunables stays a
//! proposal (LOW_LATENCY S-5).

use std::collections::BTreeMap;
use std::sync::OnceLock;

use anyhow::{bail, Context, Result};

/// One configuration knob: its env name, the default shown when unset (display
/// text, not necessarily a parseable value — "—" means "off/unset"), and
/// whether its value must never be printed.
pub struct Knob {
    pub name: &'static str,
    pub default: &'static str,
    pub redact: bool,
}

const K: fn(&'static str, &'static str) -> Knob =
    |name, default| Knob { name, default, redact: false };
const KR: fn(&'static str, &'static str) -> Knob =
    |name, default| Knob { name, default, redact: true };

/// Every env var the engine binary reads (this crate plus the library crates
/// it links: core, executor, source, logging, artifact, crypto, forge,
/// lineage). Mechanically derived from the source; keep sorted by name.
pub fn knobs() -> &'static [Knob] {
    static KNOBS: OnceLock<Vec<Knob>> = OnceLock::new();
    KNOBS.get_or_init(|| {
        vec![
            K("API_ADDR", "— (ops API disabled)"),
            K("AUTO_BACKFILL", "— (off)"),
            K("AUTO_RERUN_FAILED", "— (off)"),
            KR("AZURE_CLIENT_ID", "—"),
            KR("AZURE_CLIENT_SECRET", "—"),
            KR("AZURE_TENANT_ID", "—"),
            K("BACKFILL_PACE_PER_TICK", "1"),
            K("CRON_CONFIG", "— (cron disabled)"),
            K("DAGRON_ARTIFACT_DIR", "— (artifacts off)"),
            K("DAGRON_ARTIFACT_SYNC_SECS", "60 (0 disables)"),
            K("DAGRON_ARTIFACT_TIER", "— (tiering off)"),
            K("DAGRON_ARTIFACT_UPLINK_BYTES_PER_DAY", "— (unlimited)"),
            K("DAGRON_ARTIFACT_URL", "— (cloud artifacts off)"),
            K("DAGRON_BUNDLE_PUBKEYS", "— (signed bundles refused)"),
            K("DAGRON_CLOCK_CHECK_SECS", "30"),
            K("DAGRON_CLOCK_STEP_TOLERANCE_MS", "1000"),
            K("DAGRON_CLOCK_SYNC_FILE", "— (no positive sync evidence)"),
            K("DAGRON_CONSOLE", "— (console mounted)"),
            // The KEK/KMS envelope family (dagron-crypto, docs/CONFIG.md
            // "Encryption at rest"): the current provider's inputs plus their
            // `*_OLD` twins, which the key-rotation sweep reads for the
            // retiring KEK. Registered so the fingerprint covers a rotation
            // in flight and the typo scan stays quiet about legitimate
            // rotation setups. Key material is redacted; selectors, ids, and
            // command lines are what an operator diffs when wrapping fails.
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
            K("DAGRON_MAX_TASKS_PER_RUN", "100000 (compiled ceiling)"),
            K("DAGRON_MIN_FREE_BYTES", "0 (off)"),
            K("DAGRON_PRESSURE_FILE", "— (no pressure gate)"),
            K("DAGRON_READY_TIMEOUT_MS", "500 (floor 50)"),
            K("DAGRON_REDACT_ENV", "true"),
            K("DAGRON_SECRETS_DIR", "—"),
            K("DAGRON_SENSITIVE_ENV_PATTERNS", "built-in patterns"),
            K("DAGRON_TASK_ACTIVE_DEADLINE_SECS", "—"),
            K("DAGRON_TASK_AUTOMOUNT_SA_TOKEN", "— (off)"),
            K("DAGRON_TASK_DROP_ALL_CAPABILITIES", "— (off)"),
            K("DAGRON_TASK_NODE_SELECTOR", "—"),
            K("DAGRON_TASK_READ_ONLY_ROOT_FS", "— (off)"),
            K("DAGRON_TASK_RUNTIME_CLASS", "—"),
            K("DAGRON_TASK_RUN_AS_USER", "—"),
            K("DAGRON_TASK_SECCOMP_RUNTIME_DEFAULT", "— (off)"),
            KR("DATABASE_LISTEN_URL", "— (share the pool DSN)"),
            KR("DATABASE_URL", "postgres://localhost/workflow (pg builds)"),
            K("DATASET_TRIGGERS", "true"),
            K("DB_MAX_CONNECTIONS", "8"),
            K("DB_SCHEDULES", "— (off)"),
            K("DEAD_LETTER_MAX_ATTEMPTS", "3"),
            K("DOCKER_IMAGE", "alpine:latest"),
            K("EXECUTOR", "local"),
            K("GC_ARCHIVE_COMPACT_MIN_AGE_DAYS", "30"),
            K("GC_ARCHIVE_DIR", "— (plain purge)"),
            K("GC_ARCHIVE_URL", "— (plain purge)"),
            K("GC_INTERVAL_SECS", "3600"),
            K("GC_RETENTION_SECS", "— (GC off)"),
            // The API bases are endpoints, not credentials — printable, and an
            // operator diffing /config against the reviewed deployment needs
            // to see a GHE/self-managed override. Only the tokens are secret.
            K("GITHUB_API_BASE", "https://api.github.com"),
            KR("GITHUB_TOKEN", "— (forge feedback off)"),
            K("GITLAB_API_BASE", "https://gitlab.com/api/v4"),
            KR("GITLAB_TOKEN", "— (forge feedback off)"),
            K("K8S_IMAGE", "falls back to DOCKER_IMAGE"),
            K("K8S_NAMESPACE", "default"),
            K("LEADER_LEASE_SECS", "30"),
            K("LEASE_SECS", "30 (floor 3)"),
            K("LOG_FORMAT", "full"),
            K("LOG_LEVEL", "info"),
            K("LOG_SPAN_EVENTS", "— (off)"),
            K("MAX_INFLIGHT_RUNS", "64 (0 disables)"),
            K("MAX_INFLIGHT_TASKS", "0 (off)"),
            K("MQTT_CLEAN_SESSION", "false with MQTT_CLIENT_ID, else true"),
            K("MQTT_CLIENT_ID", "dagron-<uuid>"),
            K("MQTT_DLQ_TOPIC", "— (datastore dead_letters only)"),
            K("MQTT_KEEPALIVE_SECS", "30"),
            KR("MQTT_PASSWORD", "—"),
            K("MQTT_POSITION_FIELD", "— (at-least-once)"),
            K("MQTT_QOS", "1"),
            K("MQTT_TOPIC", "dagron/workflows"),
            // An MQTT origin can carry userinfo (mqtt://user:pass@broker), so it
            // is redacted like every other credential-bearing URL in this registry.
            KR("MQTT_URL", "mqtt://127.0.0.1:1883"),
            K("MQTT_USERNAME", "—"),
            K("OPENLINEAGE_NAMESPACE", "dagron"),
            KR("OPENLINEAGE_URL", "— (lineage off)"),
            KR("OTEL_EXPORTER_OTLP_ENDPOINT", "— (otel export off)"),
            K("POLL_INTERVAL_MS", "500 (floor 10)"),
            K("POOLS", "— (no pools)"),
            K("READY_AGE_ALERT_SECS", "300 (0 disables)"),
            K("READY_AGE_CHECK_INTERVAL_SECS", "60"),
            K("RUNNER_CLASSES", "— (claim every class)"),
            K("RUNNER_GANGS", "— (off)"),
            K("RUST_LOG", "— (LOG_LEVEL applies)"),
            K("SOURCE", "file"),
            K("STREAM_DLQ_PATH", "<STREAM_PATH>.dlq"),
            K("STREAM_FOLLOW", "true"),
            K("STREAM_MAX_PARTITIONS", "unlimited"),
            K("STREAM_MODE", "auto"),
            K("STREAM_OFFSET_PATH", "<STREAM_PATH>.offset"),
            K("STREAM_PATH", "— (required for SOURCE=stream)"),
            K("STREAM_POLL_MS", "500"),
            K("STREAM_SUFFIX", ".ndjson"),
            K("SUBWORKFLOW_MAX_DEPTH", "8"),
            K("SWEEP_INTERVAL_MS", "= POLL_INTERVAL_MS (floor 10)"),
            K("TASK_LEASE_HEARTBEAT", "true"),
            K("WAIT_POLL_SECS", "15"),
            K("WAIT_URL_DENY_PRIVATE", "— (off)"),
            K("WORKER_COUNT", "16 (min 1)"),
            K("WORKFLOW_DIR", "/workflows"),
            K("DIR_POLL_MS", "2000 (floor 100)"),
        ]
    })
}

/// Vars the typo scan must stay silent about: families owned by *other*
/// dagron components (the API edge, the MCP adapter) that legitimately appear
/// in a shared environment — a local `dagron dev` shell exporting the API's
/// `DAGRON_JWT_SECRET`, say — plus the engine's one *unenumerable* family,
/// `DAGRON_SECRET_<NAME>`, whose suffix is author-chosen per secret and so can
/// never be a registry entry.
const FOREIGN_PREFIXES: &[&str] = &[
    "DAGRON_SECRET_",
    "DAGRON_JWT_",
    "DAGRON_COOKIE_",
    "DAGRON_ADMIN_",
    "DAGRON_SESSION_",
    "DAGRON_LOGIN_",
    "DAGRON_TRUST_",
    "DAGRON_PW_",
    "DAGRON_AUDIT_",
    "DAGRON_GIT_",
    "DAGRON_DB_",
    "DAGRON_SHUTDOWN_",
    "DAGRON_ARTIFACT_MAX_",
    "DAGRON_KEK",
    "DAGRON_MCP_",
    "DAGRON_API_",
    // The fleet-link worker (an optional sidecar loop, not the engine) reads its
    // own family; registering it here keeps a unit's shell quiet.
    "DAGRON_FLEET_",
];

/// Prefix families the typo scan claims: an env var starting with one of these
/// that is neither a registered knob nor a known foreign var draws a warning.
/// Deliberately narrow — over-warning trains operators to ignore the signal.
const OUR_PREFIXES: &[&str] = &[
    "DAGRON_",
    "STREAM_",
    "MQTT_",
    "GC_ARCHIVE_",
    "READY_AGE_",
    "MAX_INFLIGHT_",
    "DEAD_LETTER_",
    "SUBWORKFLOW_",
    "OPENLINEAGE_",
    "POLL_INTERVAL",
    "SWEEP_INTERVAL",
    "LEASE_SECS",
];

/// Where an effective value came from, in precedence order.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Source {
    Env,
    File,
    Profile,
    Default,
}

impl Source {
    pub fn as_str(self) -> &'static str {
        match self {
            Source::Env => "env",
            Source::File => "file",
            Source::Profile => "profile",
            Source::Default => "default",
        }
    }
}

/// What the file layer resolved: which file and profile (if any), and which
/// keys the layer itself set (everything else set in the environment was
/// there before us, i.e. `Source::Env`).
#[derive(Default)]
pub struct FileLayer {
    pub path: Option<String>,
    pub profile: Option<String>,
    set_by: BTreeMap<String, Source>,
}

static LAYER: OnceLock<FileLayer> = OnceLock::new();

/// The named profiles: preset knob values applied under the file's own keys.
/// `throughput` is the stock tuning, so its preset is empty on purpose — it
/// exists so a fleet can *declare* the intent in a reviewed file.
fn profile_preset(name: &str) -> Option<&'static [(&'static str, &'static str)]> {
    match name {
        // docs/LOW_LATENCY.md §6 — the trading-desk profile's engine knobs.
        "low-latency" => Some(&[
            ("POLL_INTERVAL_MS", "25"),
            ("SWEEP_INTERVAL_MS", "500"),
            ("LEASE_SECS", "5"),
        ]),
        "throughput" => Some(&[]),
        // docs/CONFIG.md — constrained gateways, robots, vehicles: few workers,
        // slow ticks, small in-flight caps, and a free-disk floor so a full
        // flash device refuses new runs instead of corrupting the datastore.
        "edge" => Some(&[
            ("WORKER_COUNT", "2"),
            ("POLL_INTERVAL_MS", "1000"),
            ("SWEEP_INTERVAL_MS", "5000"),
            ("MAX_INFLIGHT_RUNS", "4"),
            ("MAX_INFLIGHT_TASKS", "64"),
            ("DAGRON_MIN_FREE_BYTES", "67108864"),
        ]),
        _ => None,
    }
}

/// Apply `DAGRON_CONFIG` (and its `profile:`) UNDER the environment: for every
/// key the file or profile provides, set the env var **only if it is not
/// already set**, so explicit environment always wins. Must run before
/// anything reads configuration (including logging init — the file may set
/// `LOG_LEVEL`). Call exactly once; the resolved layer is retained for
/// introspection. Errors are startup-fatal: a reviewed config file that does
/// not parse, names an unknown profile, or carries an unknown key must never
/// half-apply silently.
pub fn apply_file_layer() -> Result<&'static FileLayer> {
    let mut layer = FileLayer::default();
    if let Ok(path) = std::env::var("DAGRON_CONFIG") {
        let path = path.trim().to_string();
        if !path.is_empty() {
            let text = std::fs::read_to_string(&path)
                .with_context(|| format!("reading DAGRON_CONFIG file '{path}'"))?;
            let doc: serde_yaml::Value = serde_yaml::from_str(&text)
                .with_context(|| format!("parsing DAGRON_CONFIG file '{path}' as YAML"))?;
            let map = doc
                .as_mapping()
                .with_context(|| format!("DAGRON_CONFIG file '{path}' must be a YAML mapping"))?;

            // Resolve the profile first so file keys override its preset.
            let mut pending: BTreeMap<String, (String, Source)> = BTreeMap::new();
            if let Some(profile) = map.get("profile") {
                // A non-string profile is a review mistake, not a missing key —
                // silently ignoring `profile: 1` would half-apply the file.
                let Some(profile) = profile.as_str() else {
                    bail!(
                        "DAGRON_CONFIG file '{path}': 'profile' must be a string \
                         (known: low-latency, throughput, edge), got {profile:?}"
                    );
                };
                let preset = profile_preset(profile).with_context(|| {
                    format!(
                        "DAGRON_CONFIG names unknown profile '{profile}' \
                         (known: low-latency, throughput, edge)"
                    )
                })?;
                for (k, v) in preset {
                    pending.insert((*k).to_string(), ((*v).to_string(), Source::Profile));
                }
                layer.profile = Some(profile.to_string());
            }

            let known = |k: &str| knobs().iter().any(|kn| kn.name == k);
            for (key, value) in map {
                let Some(key) = key.as_str() else {
                    bail!("DAGRON_CONFIG file '{path}': non-string key {key:?}");
                };
                if key == "profile" {
                    continue;
                }
                if !known(key) {
                    bail!(
                        "DAGRON_CONFIG file '{path}': unknown key '{key}' — keys are the \
                         engine's env var names (see docs/CONFIG.md)"
                    );
                }
                let rendered = match value {
                    serde_yaml::Value::String(s) => s.clone(),
                    serde_yaml::Value::Number(n) => n.to_string(),
                    serde_yaml::Value::Bool(b) => b.to_string(),
                    other => bail!(
                        "DAGRON_CONFIG file '{path}': key '{key}' must be a scalar, got {other:?}"
                    ),
                };
                pending.insert(key.to_string(), (rendered, Source::File));
            }

            for (key, (value, source)) in pending {
                if std::env::var_os(&key).is_none() {
                    // Single-threaded startup (before the runtime spawns
                    // workers that read env) — same contract as the existing
                    // `dagron dev` set_var.
                    std::env::set_var(&key, &value);
                    layer.set_by.insert(key, source);
                }
            }
            layer.path = Some(path);
        }
    }
    Ok(LAYER.get_or_init(|| layer))
}

fn layer() -> &'static FileLayer {
    LAYER.get_or_init(FileLayer::default)
}

/// The resolved file layer's provenance — (config file path, profile name) —
/// for introspection surfaces outside this module (the ops API's `GET
/// /config` mirrors `dagron config`'s header lines with it).
pub fn layer_info() -> (Option<String>, Option<String>) {
    let l = layer();
    (l.path.clone(), l.profile.clone())
}

/// Every knob's effective value: `(name, display value, source)`. Secrets show
/// `<redacted>`; unset knobs show their documented default. The *display*
/// value is for humans — the fingerprint below hashes raw values instead.
pub fn effective() -> Vec<(&'static str, String, Source)> {
    let layer = layer();
    knobs()
        .iter()
        .map(|k| match std::env::var(k.name) {
            Ok(v) => {
                let source = layer.set_by.get(k.name).copied().unwrap_or(Source::Env);
                let shown = if k.redact { "<redacted>".to_string() } else { v };
                (k.name, shown, source)
            }
            Err(_) => (k.name, k.default.to_string(), Source::Default),
        })
        .collect()
}

/// Stable fingerprint of the raw effective configuration (FNV-1a 64 over
/// `name=value` pairs in registry order; unset knobs hash as their name
/// alone). Two replicas with the same fingerprint run the same knob values —
/// the fleet-drift check. Not a security hash; drift detection only.
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

/// Warn once per env var that matches one of our prefix families but is
/// neither a registered knob nor a known other-component var — the
/// `POLL_INTERVAL_M`-style silent typo. `DAGRON_CONFIG_NO_WARN=1` silences the
/// scan for environments that intentionally carry look-alike vars.
pub fn warn_unknown_vars() {
    if std::env::var_os("DAGRON_CONFIG_NO_WARN").is_some() {
        return;
    }
    let known = |name: &str| {
        name == "DAGRON_CONFIG"
            || name == "DAGRON_CONFIG_NO_WARN"
            || knobs().iter().any(|k| k.name == name)
            || FOREIGN_PREFIXES.iter().any(|p| name.starts_with(p))
    };
    // `vars_os`, not `vars`: the scan must never panic a boot over some
    // unrelated non-UTF-8 variable in the environment. A non-UTF-8 *name*
    // cannot be one of ours (every knob name is ASCII), so it is skipped.
    for (name, _) in std::env::vars_os() {
        let Some(name) = name.to_str() else { continue };
        if OUR_PREFIXES.iter().any(|p| name.starts_with(p)) && !known(name) {
            tracing::warn!(
                var = %name,
                "environment variable looks like a dagron knob but is not one — typo? \
                 (see docs/CONFIG.md; DAGRON_CONFIG_NO_WARN=1 silences this check)"
            );
        }
    }
}

/// `dagron config [--json]` — print every knob's effective value and source.
/// The file layer has already been applied by the caller, so what prints here
/// is exactly what a daemon started in this environment would run with.
pub fn run_cli(args: &[String]) -> Result<()> {
    let json = args.iter().any(|a| a == "--json");
    let layer = layer();
    let rows = effective();
    if json {
        let knobs: Vec<serde_json::Value> = rows
            .iter()
            .map(|(name, value, source)| {
                serde_json::json!({ "name": name, "value": value, "source": source.as_str() })
            })
            .collect();
        let doc = serde_json::json!({
            "fingerprint": fingerprint(),
            "config_file": layer.path,
            "profile": layer.profile,
            "knobs": knobs,
        });
        println!("{}", serde_json::to_string_pretty(&doc)?);
        return Ok(());
    }
    println!("# dagron effective configuration");
    println!("# fingerprint: {}", fingerprint());
    if let Some(p) = &layer.path {
        println!("# config file: {p}");
    }
    if let Some(p) = &layer.profile {
        println!("# profile: {p}");
    }
    let width = knobs().iter().map(|k| k.name.len()).max().unwrap_or(0);
    for (name, value, source) in rows {
        println!("{name:width$}  [{:7}]  {value}", source.as_str());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Env-var mutation is process-global; serialize the tests that do it.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// The fingerprint is stable for a fixed environment and moves when any
    /// registered knob moves.
    #[test]
    fn fingerprint_tracks_knob_changes() {
        let _guard = ENV_LOCK.lock().unwrap();
        let a = fingerprint();
        let b = fingerprint();
        assert_eq!(a, b, "stable for a fixed environment");
        std::env::set_var("WAIT_POLL_SECS", "17");
        let c = fingerprint();
        std::env::remove_var("WAIT_POLL_SECS");
        assert_ne!(a, c, "a changed knob changes the fingerprint");
        assert_eq!(a, fingerprint(), "restoring the env restores it");
    }

    /// Redacted knobs never display their value; unset knobs display defaults.
    #[test]
    fn effective_redacts_and_defaults() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("GITHUB_TOKEN", "ghp_super_secret");
        let rows = effective();
        let (_, shown, source) =
            rows.iter().find(|(n, _, _)| *n == "GITHUB_TOKEN").unwrap();
        assert_eq!(shown, "<redacted>");
        assert!(matches!(source, Source::Env));
        std::env::remove_var("GITHUB_TOKEN");
        let rows = effective();
        let (_, shown, source) =
            rows.iter().find(|(n, _, _)| *n == "EXECUTOR").unwrap();
        if std::env::var_os("EXECUTOR").is_none() {
            assert_eq!(shown, "local");
            assert!(matches!(source, Source::Default));
        }
    }

    /// Profile presets resolve; unknown profiles are an error the file layer
    /// surfaces (exercised through profile_preset directly — the layer itself
    /// is a OnceLock and can only be applied once per process).
    #[test]
    fn profiles_are_known() {
        assert!(profile_preset("low-latency").is_some());
        assert!(profile_preset("throughput").is_some());
        assert!(profile_preset("hyperspeed").is_none());
        let ll = profile_preset("low-latency").unwrap();
        assert!(ll.iter().any(|(k, v)| *k == "POLL_INTERVAL_MS" && *v == "25"));
    }
}
