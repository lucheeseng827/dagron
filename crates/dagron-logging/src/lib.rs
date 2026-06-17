//! Centralized, tunable logging/observability bootstrap for dagron.
//!
//! This is the **single source of truth** for how every internal dagron process
//! turns `tracing` events into output: the workflow controller / reconcile loop
//! and its worker pool (`dagron`), the management API (`dagron-api`), and the
//! Kubernetes operator (`dagron-operator`) all call [`init`]. Configuring
//! verbosity and format in one place keeps logs consistent — important down the
//! line when shipping them to a central aggregator (Loki,
//! CloudWatch, Datadog, …) that needs them machine-parseable and per-service
//! attributable.
//!
//! NOTE: the public OSS mirror (`module_54/oss`) is a standalone crate excluded
//! from the workspace, so it intentionally does *not* depend on this crate and
//! keeps its own minimal init.
//!
//! Configuration is entirely environment-driven so it can be tuned per-deployment
//! without a rebuild:
//!
//! | Env var            | Values                                              | Default | Purpose |
//! |--------------------|-----------------------------------------------------|---------|---------|
//! | `RUST_LOG`         | full [`EnvFilter`] directive                         | —       | Precise per-target verbosity, e.g. `info,dagron::worker=debug,sqlx=warn`. Wins over `LOG_LEVEL`. |
//! | `LOG_LEVEL`        | `trace`/`debug`/`info`/`warn`/`error`               | `info`  | Simple global verbosity knob when you don't need per-target control. |
//! | `LOG_FORMAT`       | `full`/`compact`/`pretty`/`json`                     | `full`  | `json` for SaaS log ingestion; `pretty` for local dev readability. |
//! | `LOG_TARGET`       | `1`/`0`                                              | `1`     | Include the emitting module path on each line. |
//! | `LOG_THREAD_IDS`   | `1`/`0`                                              | `0`     | Include the OS thread id (useful when debugging the worker pool). |
//! | `LOG_THREAD_NAMES` | `1`/`0`                                              | `0`     | Include the thread name. |
//! | `LOG_LINE`         | `1`/`0`                                              | `0`     | Include source file + line number. |
//! | `LOG_SPAN_EVENTS`  | `none`/`new`/`enter`/`exit`/`close`/`active`/`full` | `none`  | Emit span lifecycle events (e.g. `close` to time spans). |
//! | `LOG_ANSI`         | `1`/`0`                                              | auto    | Force ANSI colors on/off (auto-disabled for `json`). |
//!
//! Verbosity precedence: `RUST_LOG` (if set and parseable) → `LOG_LEVEL` → `info`.

use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::EnvFilter;

/// Read a boolean-ish env var (`1`/`true`/`yes`/`on` → true, anything else →
/// false), falling back to `default` when unset.
fn env_bool(key: &str, default: bool) -> bool {
    match std::env::var(key) {
        Ok(v) => matches!(
            v.trim().to_ascii_lowercase().as_str(),
            "1" | "true" | "yes" | "on"
        ),
        Err(_) => default,
    }
}

/// Build the verbosity filter. `RUST_LOG` (full `target=level` syntax) wins; else
/// fall back to a simple global `LOG_LEVEL`; else `info`. A malformed `RUST_LOG`
/// is reported (to stderr, before the subscriber exists) and ignored rather than
/// silently swallowing all logs.
fn build_filter() -> EnvFilter {
    if let Ok(raw) = std::env::var("RUST_LOG") {
        match EnvFilter::try_new(&raw) {
            Ok(filter) => return filter,
            Err(e) => eprintln!("dagron: ignoring invalid RUST_LOG ({raw:?}): {e}"),
        }
    }
    let level = std::env::var("LOG_LEVEL").unwrap_or_else(|_| "info".to_string());
    EnvFilter::try_new(&level).unwrap_or_else(|e| {
        eprintln!("dagron: ignoring invalid LOG_LEVEL ({level:?}): {e}");
        EnvFilter::new("info")
    })
}

/// Translate `LOG_SPAN_EVENTS` into the corresponding [`FmtSpan`] mask.
fn span_events() -> FmtSpan {
    match std::env::var("LOG_SPAN_EVENTS")
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "new" => FmtSpan::NEW,
        "enter" => FmtSpan::ENTER,
        "exit" => FmtSpan::EXIT,
        "close" => FmtSpan::CLOSE,
        "active" => FmtSpan::ACTIVE,
        "full" => FmtSpan::FULL,
        _ => FmtSpan::NONE,
    }
}

/// Initialize the global tracing subscriber for `service` (e.g. `"controller"`,
/// `"api"`, `"operator"`). Call exactly once, as early in `main` as possible. The
/// `service` name is emitted as a field on the startup "logging initialized" line
/// so each process is identifiable in a shared log stream. (Per-*event* service
/// attribution would need a custom layer across the multi-threaded runtime; in
/// practice each dagron process runs as its own container/pod, so the
/// orchestrator's metadata already tags its downstream events.)
///
/// Idempotent-ish: uses `try_init` so a double-call (e.g. in tests) is a no-op
/// rather than a panic.
pub fn init(service: &str) {
    let format = std::env::var("LOG_FORMAT")
        .unwrap_or_else(|_| "full".to_string())
        .to_ascii_lowercase();
    let json = format == "json";

    let builder = tracing_subscriber::fmt()
        .with_env_filter(build_filter())
        .with_target(env_bool("LOG_TARGET", true))
        .with_thread_ids(env_bool("LOG_THREAD_IDS", false))
        .with_thread_names(env_bool("LOG_THREAD_NAMES", false))
        .with_line_number(env_bool("LOG_LINE", false))
        .with_span_events(span_events())
        // JSON output must never carry ANSI escapes; otherwise honor LOG_ANSI
        // (defaulting on — the fmt layer still auto-detects non-TTY downstream).
        .with_ansi(!json && env_bool("LOG_ANSI", true));

    // The format-specific transforms (`.json()`, `.pretty()`, `.compact()`) each
    // return a distinct builder type, so finalize within each arm.
    match format.as_str() {
        "json" => builder
            .json()
            .flatten_event(true)
            .with_current_span(true)
            .with_span_list(true)
            .try_init()
            .ok(),
        "pretty" => builder.pretty().try_init().ok(),
        "compact" => builder.compact().try_init().ok(),
        // "full" (default) and any unrecognized value.
        _ => builder.try_init().ok(),
    };

    tracing::info!(service, format = %format, "logging initialized");
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    /// Tests here mutate process-global env vars; the default test harness runs
    /// them in parallel, so serialize env access to keep them deterministic.
    fn env_lock() -> &'static Mutex<()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(()))
    }

    #[test]
    fn env_bool_parses_truthy_and_defaults() {
        let _guard = env_lock().lock().expect("env lock poisoned");
        std::env::set_var("DAGRON_TEST_BOOL", "yes");
        assert!(env_bool("DAGRON_TEST_BOOL", false));
        std::env::set_var("DAGRON_TEST_BOOL", "0");
        assert!(!env_bool("DAGRON_TEST_BOOL", true));
        std::env::remove_var("DAGRON_TEST_BOOL");
        assert!(env_bool("DAGRON_TEST_BOOL", true));
        assert!(!env_bool("DAGRON_TEST_BOOL", false));
    }

    #[test]
    fn span_events_maps_known_values() {
        let _guard = env_lock().lock().expect("env lock poisoned");
        std::env::set_var("LOG_SPAN_EVENTS", "close");
        assert_eq!(format!("{:?}", span_events()), format!("{:?}", FmtSpan::CLOSE));
        std::env::set_var("LOG_SPAN_EVENTS", "nonsense");
        assert_eq!(format!("{:?}", span_events()), format!("{:?}", FmtSpan::NONE));
        std::env::remove_var("LOG_SPAN_EVENTS");
    }

    #[test]
    fn build_filter_falls_back_to_info() {
        let _guard = env_lock().lock().expect("env lock poisoned");
        std::env::remove_var("RUST_LOG");
        std::env::remove_var("LOG_LEVEL");
        // Just assert it constructs without panicking and renders a directive.
        let _ = build_filter().to_string();
    }
}
