//! Centralized, tunable logging/observability bootstrap for dagron.
//!
//! This is the **single source of truth** for how every internal dagron process
//! turns `tracing` events into output: the workflow controller / reconcile loop
//! and its worker pool (`dagron`) and the management API (`dagron-api`) all call
//! [`init`]. Configuring
//! verbosity and format in one place keeps logs consistent — important down the
//! line when shipping them to a central aggregator (Loki,
//! CloudWatch, Datadog, …) that needs them machine-parseable and per-service
//! attributable.
//!
//! Configuration is entirely environment-driven so it can be tuned per-deployment
//! without a rebuild:
//!
//! | Env var            | Values                                              | Default | Purpose |
//! |--------------------|-----------------------------------------------------|---------|---------|
//! | `RUST_LOG`         | full [`EnvFilter`] directive                         | —       | Precise per-target verbosity, e.g. `info,dagron::worker=debug,sqlx=warn`. Wins over `LOG_LEVEL`. |
//! | `LOG_LEVEL`        | `trace`/`debug`/`info`/`warn`/`error`               | `info`  | Simple global verbosity knob when you don't need per-target control. |
//! | `LOG_FORMAT`       | `full`/`compact`/`pretty`/`json`                     | `full`  | `json` for centralized log ingestion; `pretty` for local dev readability. |
//! | `LOG_TARGET`       | `1`/`0`                                              | `1`     | Include the emitting module path on each line. |
//! | `LOG_THREAD_IDS`   | `1`/`0`                                              | `0`     | Include the OS thread id (useful when debugging the worker pool). |
//! | `LOG_THREAD_NAMES` | `1`/`0`                                              | `0`     | Include the thread name. |
//! | `LOG_LINE`         | `1`/`0`                                              | `0`     | Include source file + line number. |
//! | `LOG_SPAN_EVENTS`  | `none`/`new`/`enter`/`exit`/`close`/`active`/`full` | `none`  | Emit span lifecycle events (e.g. `close` to time spans). |
//! | `LOG_ANSI`         | `1`/`0`                                              | auto    | Force ANSI colors on/off (auto-disabled for `json`). |
//!
//! Verbosity precedence: `RUST_LOG` (if set and parseable) → `LOG_LEVEL` → `info`.
//!
//! ## Reading logs back
//!
//! Everything above is the **emit** side. The [`logfilter`] module (feature
//! `logfilter`) is the **read** side: the filter grammar applied when a human
//! opens a workflow's stored task output. It is the single parser for that
//! grammar, and feature-gated so the engine binary — which only ever writes
//! logs — doesn't link a regex engine.

#[cfg(feature = "logfilter")]
pub mod logfilter;

use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

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

/// Build the fmt output as a boxed [`Layer`] so it can be composed in a
/// `registry()` alongside other layers (the OTLP layer, when `otel` is on). The
/// format-specific transforms (`.json()`/`.pretty()`/`.compact()`) each return a
/// distinct type, so each arm boxes its own. Behavior matches the previous
/// `fmt()`-builder path exactly; the verbosity [`EnvFilter`] is applied once at
/// the registry level so every layer (fmt and OTLP) honors `RUST_LOG`/`LOG_LEVEL`.
fn fmt_layer<S>() -> Box<dyn Layer<S> + Send + Sync>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a> + Send + Sync,
{
    let format = std::env::var("LOG_FORMAT")
        .unwrap_or_else(|_| "full".to_string())
        .to_ascii_lowercase();
    let json = format == "json";

    let base = tracing_subscriber::fmt::layer()
        .with_target(env_bool("LOG_TARGET", true))
        .with_thread_ids(env_bool("LOG_THREAD_IDS", false))
        .with_thread_names(env_bool("LOG_THREAD_NAMES", false))
        .with_line_number(env_bool("LOG_LINE", false))
        .with_span_events(span_events())
        // JSON output must never carry ANSI escapes; otherwise honor LOG_ANSI
        // (defaulting on — the fmt layer still auto-detects non-TTY downstream).
        .with_ansi(!json && env_bool("LOG_ANSI", true));

    match format.as_str() {
        "json" => base
            .json()
            .flatten_event(true)
            .with_current_span(true)
            .with_span_list(true)
            .boxed(),
        "pretty" => base.pretty().boxed(),
        "compact" => base.compact().boxed(),
        // "full" (default) and any unrecognized value.
        _ => base.boxed(),
    }
}

#[cfg(feature = "otel")]
static TRACER_PROVIDER: std::sync::OnceLock<opentelemetry_sdk::trace::SdkTracerProvider> =
    std::sync::OnceLock::new();

/// Flush and stop the OTLP span exporter, if one was installed.
///
/// Call once on a clean shutdown path, after the last work you want traced. The
/// batch exporter holds spans between export intervals, so without this the
/// final window is lost — precisely the spans that explain a shutdown. A no-op
/// when the `otel` feature is off or no exporter was configured.
pub fn shutdown() {
    #[cfg(feature = "otel")]
    if let Some(provider) = TRACER_PROVIDER.get() {
        if let Err(e) = provider.shutdown() {
            eprintln!("dagron: OTLP span exporter shutdown failed: {e}");
        }
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
/// With the `otel` feature built in **and** `OTEL_EXPORTER_OTLP_ENDPOINT` set, an
/// OTLP (HTTP/protobuf) span exporter is also installed (#28 follow-on) so the
/// stack's tracing spans are delivered to a collector; otherwise logging is
/// unchanged.
///
/// Idempotent-ish: uses `try_init` so a double-call (e.g. in tests) is a no-op
/// rather than a panic.
pub fn init(service: &str) {
    let format = std::env::var("LOG_FORMAT").unwrap_or_else(|_| "full".to_string());

    let subscriber = tracing_subscriber::registry()
        .with(build_filter())
        .with(fmt_layer());

    #[cfg(feature = "otel")]
    match otel_layer(service) {
        Some(layer) => {
            subscriber.with(layer).try_init().ok();
        }
        None => {
            subscriber.try_init().ok();
        }
    }
    #[cfg(not(feature = "otel"))]
    subscriber.try_init().ok();

    tracing::info!(service, format = %format.to_ascii_lowercase(), "logging initialized");
}

/// Build the OpenTelemetry OTLP span-export layer (#28 follow-on), or `None` when
/// `OTEL_EXPORTER_OTLP_ENDPOINT` is unset (the exporter stays off) or the exporter
/// can't be built. The exporter reads the standard `OTEL_EXPORTER_OTLP_*` env vars
/// for its endpoint/headers/protocol, so operators tune delivery without a
/// rebuild. A W3C `TraceContext` propagator is installed globally so a
/// `traceparent` injected into a downstream task stitches into the same trace.
///
/// The `SdkTracerProvider` is handed to the global provider registry to keep it
/// (and its background batch exporter) alive for the process lifetime.
#[cfg(feature = "otel")]
fn otel_layer<S>(service: &str) -> Option<Box<dyn Layer<S> + Send + Sync>>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a> + Send + Sync,
{
    use opentelemetry::trace::TracerProvider as _;

    // Propagation is independent of export: it decides how a span context is
    // *encoded* on the wire, not where spans are sent. Installing it before the
    // endpoint gate means an `otel` build that extracts an inbound `traceparent`
    // behaves the same whether or not a collector is configured — otherwise the
    // global propagator stays the no-op default and silently drops the context.
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );

    // Gate on the standard endpoint env var: unset ⇒ exporter stays off.
    let endpoint = std::env::var("OTEL_EXPORTER_OTLP_ENDPOINT")
        .ok()
        .map(|s| s.trim().to_string())
        .filter(|s| !s.is_empty())?;

    // A second call (a double `init` in tests, or an embedder) must not build a
    // *second* provider: only the one in the OnceLock is reachable from
    // `shutdown`, so a second provider's batch queue would never be flushed and
    // its final spans would die with the process. Reuse the stored one instead —
    // the layer it backs is equivalent, and there stays exactly one exporter.
    if let Some(existing) = TRACER_PROVIDER.get() {
        let tracer = existing.tracer("dagron");
        return Some(tracing_opentelemetry::layer().with_tracer(tracer).boxed());
    }

    // Endpoint/headers/protocol come from the standard OTEL env vars; we only
    // need the presence check above to decide whether to enable at all.
    let exporter = match opentelemetry_otlp::SpanExporter::builder()
        .with_http()
        .build()
    {
        Ok(e) => e,
        Err(e) => {
            eprintln!("dagron: OTLP span export disabled (exporter build failed): {e}");
            return None;
        }
    };

    let resource = opentelemetry_sdk::Resource::builder()
        .with_service_name(format!("dagron-{service}"))
        .build();

    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .build();

    let tracer = provider.tracer("dagron");

    // Keep a handle so `shutdown` can flush the batch exporter's queue on a
    // clean exit — otherwise every span buffered since the last export interval
    // dies with the process, which is exactly the tail you most want after a
    // failure. The early return above guarantees this `set` is the first one, so
    // the provider installed globally below is always the one `shutdown` holds.
    let _ = TRACER_PROVIDER.set(provider.clone());

    // Keep the provider alive process-wide.
    opentelemetry::global::set_tracer_provider(provider);

    eprintln!("dagron: OTLP span export enabled → {endpoint}");
    Some(tracing_opentelemetry::layer().with_tracer(tracer).boxed())
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

    /// The OTLP exporter must stay OFF unless `OTEL_EXPORTER_OTLP_ENDPOINT` is set,
    /// even when the `otel` feature is compiled in (#28 follow-on): no endpoint ⇒
    /// no exporter layer, no background exporter thread.
    #[cfg(feature = "otel")]
    #[test]
    fn otel_layer_off_without_endpoint() {
        let _guard = env_lock().lock().expect("env lock poisoned");
        std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
        assert!(super::otel_layer::<tracing_subscriber::Registry>("test").is_none());
        // A blank/whitespace endpoint is treated as unset, too.
        std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "   ");
        assert!(super::otel_layer::<tracing_subscriber::Registry>("test").is_none());
        std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
    }

    /// The OTLP HTTP/protobuf exporter builds successfully with the selected
    /// feature set (#28 follow-on) — a runtime guard that the `http-proto` +
    /// blocking-reqwest transport is wired, not just that it compiles. Builds the
    /// exporter in isolation (no global provider, no background thread) so it has
    /// no side effects on the rest of the suite.
    #[cfg(feature = "otel")]
    #[test]
    fn otlp_http_exporter_builds() {
        // Shares OTEL_EXPORTER_OTLP_ENDPOINT with the gating test above.
        let _guard = env_lock().lock().expect("env lock poisoned");
        let exporter = opentelemetry_otlp::SpanExporter::builder().with_http().build();
        assert!(exporter.is_ok(), "OTLP HTTP exporter must build: {:?}", exporter.err());
    }
}
