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
//! | `LOG_ANSI`         | `1`/`0`                                              | auto    | Force ANSI colors on/off. Unset, lines are coloured only when stdout is a terminal and `NO_COLOR` is not set. Never for `json`. |
//! | `NO_COLOR`         | any value but empty                                 | —       | No colour, as <https://no-color.org> has it: set and not empty. `LOG_ANSI` wins over it. |
//!
//! Verbosity precedence: `RUST_LOG` (if set and parseable) → `LOG_LEVEL` → `info`.
//!
//! ## What release a binary is
//!
//! [`VERSION`] is the release a binary was built for, and
//! [`version_requested`] answers `--version` with it: every dagron binary
//! calls it first, before it reads a setting or starts anything.
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
#[cfg(feature = "otel")]
pub mod otlp_metrics;

use std::ffi::OsStr;
use std::io::IsTerminal;

use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;
use tracing_subscriber::{EnvFilter, Layer};

/// The release this binary was built for: `APP_VERSION` as it was set when the
/// binary was compiled, which a release's build sets (each image's build
/// argument, each published binary's environment). Any other build is `dev`.
///
/// Not the crate's version. The crates of a release are not numbered with it,
/// and the open and the commercial releases are numbered apart, so a number
/// read from a manifest would be the same for every release and right for
/// none.
pub const VERSION: &str = match option_env!("APP_VERSION") {
    Some(version) if !version.is_empty() => version,
    _ => "dev",
};

/// What `--version` prints for `binary`: its name and [`VERSION`], on one
/// line. The name is the build's own, not the one it was run by: an image may
/// install a binary under another name.
pub fn version_line(binary: &str) -> String {
    format!("{binary} {VERSION}")
}

/// Whether a process's arguments (without its own name) ask for its version
/// and for nothing else: `--version` or `-V`, alone.
pub fn asks_version<I, S>(args: I) -> bool
where
    I: IntoIterator<Item = S>,
    S: AsRef<OsStr>,
{
    let mut args = args.into_iter();
    let first = args.next();
    let asked = first
        .as_ref()
        .is_some_and(|arg| arg.as_ref() == "--version" || arg.as_ref() == "-V");
    asked && args.next().is_none()
}

/// Answer `--version`: print [`version_line`] and return `true`, where that
/// is what this process was asked. The caller then returns, having started
/// nothing. Call it first in `main`, before [`init`]: logging writes a line
/// of its own to the same stream.
pub fn version_requested(binary: &str) -> bool {
    if !asks_version(std::env::args_os().skip(1)) {
        return false;
    }
    println!("{}", version_line(binary));
    true
}

/// Whether log lines carry ANSI colour. `LOG_ANSI` decides where it is set.
/// Otherwise colour is for a person at a terminal: on when stdout is one and
/// `NO_COLOR` is not set, and off into a file, a pipe or a container's log,
/// where the escape codes are noise to whatever reads them.
fn ansi(log_ansi: Option<bool>, no_color: bool, terminal: bool) -> bool {
    log_ansi.unwrap_or(terminal && !no_color)
}

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
        // JSON output never carries ANSI escapes. Otherwise LOG_ANSI decides,
        // and unset, a terminal does. The fmt layer does not look for one
        // itself, and an explicit `with_ansi` overrides its reading of
        // NO_COLOR, so both are read here.
        .with_ansi(
            !json
                && ansi(
                    std::env::var_os("LOG_ANSI").map(|_| env_bool("LOG_ANSI", true)),
                    std::env::var_os("NO_COLOR").is_some_and(|v| !v.is_empty()),
                    std::io::stdout().is_terminal(),
                ),
        );

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
#[cfg(feature = "otel")]
static LOGGER_PROVIDER: std::sync::OnceLock<opentelemetry_sdk::logs::SdkLoggerProvider> =
    std::sync::OnceLock::new();

/// Flush and stop the OTLP span and log exporters, if they were installed.
///
/// Call once on a clean shutdown path, after the last work you want traced. The
/// batch exporters hold spans and records between export intervals, so without
/// this the final window is lost — precisely what explains a shutdown. A no-op
/// when the `otel` feature is off or no exporter was configured.
pub fn shutdown() {
    #[cfg(feature = "otel")]
    if let Some(provider) = TRACER_PROVIDER.get() {
        if let Err(e) = provider.shutdown() {
            eprintln!("dagron: OTLP span exporter shutdown failed: {e}");
        }
    }
    #[cfg(feature = "otel")]
    if let Some(provider) = LOGGER_PROVIDER.get() {
        if let Err(e) = provider.shutdown() {
            eprintln!("dagron: OTLP log exporter shutdown failed: {e}");
        }
    }
}

/// The resource every signal a process sends carries: `service.name` is
/// `dagron-<service>`, over what the SDK finds itself (`OTEL_RESOURCE_ATTRIBUTES`
/// among it), so spans, logs and metrics of one process name it alike.
#[cfg(feature = "otel")]
pub(crate) fn otel_resource(service: &str) -> opentelemetry_sdk::Resource {
    opentelemetry_sdk::Resource::builder()
        .with_service_name(service_name(service))
        .build()
}

/// `dagron-<service>`, or `service` itself where it is a binary's whole name
/// already (`dagron-ci`, `dagron-step-sql`): never `dagron-dagron-…`.
pub fn service_name(service: &str) -> String {
    match service.starts_with("dagron-") {
        true => service.to_string(),
        false => format!("dagron-{service}"),
    }
}

/// The service this process named as it began logging ([`init`]), for
/// what it sends outside the logging layers (its metrics), so that every
/// signal of one process has one name.
pub fn service() -> Option<&'static str> {
    SERVICE.get().map(String::as_str)
}

static SERVICE: std::sync::OnceLock<String> = std::sync::OnceLock::new();

/// The endpoint a signal (`TRACES`, `LOGS`, `METRICS`) goes to, when it goes:
/// its own `OTEL_EXPORTER_OTLP_<SIGNAL>_ENDPOINT` or the base
/// `OTEL_EXPORTER_OTLP_ENDPOINT`, and `OTEL_<SIGNAL>_EXPORTER` not `none`. The
/// exporters read the rest of the standard variables themselves.
#[cfg(feature = "otel")]
fn otlp_signal(signal: &str) -> Option<String> {
    let var = |k: String| {
        std::env::var(k).ok().map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
    };
    if var(format!("OTEL_{signal}_EXPORTER")).is_some_and(|v| v.eq_ignore_ascii_case("none")) {
        return None;
    }
    var(format!("OTEL_EXPORTER_OTLP_{signal}_ENDPOINT"))
        .or_else(|| var("OTEL_EXPORTER_OTLP_ENDPOINT".to_string()))
}

/// Initialize the global tracing subscriber for `service` (e.g. `"controller"`,
/// `"api"`, `"operator"`). Call exactly once, as early in `main` as possible. The
/// `service` name is emitted as a field on the startup "logging initialized" line
/// so each process is identifiable in a shared log stream. (Per-*event* service
/// attribution would need a custom layer across the multi-threaded runtime; in
/// practice each dagron process runs as its own container/pod, so the
/// orchestrator's metadata already tags its downstream events.)
///
/// With the `otel` feature built in **and** an OTLP endpoint set
/// (`OTEL_EXPORTER_OTLP_ENDPOINT`, or a signal's own), OTLP (HTTP/protobuf)
/// exporters are also installed: spans (#28 follow-on), and each log event as
/// an OTLP log record, each unless its `OTEL_<SIGNAL>_EXPORTER` is
/// `none`; otherwise logging is unchanged. Metrics go by
/// [`otlp_metrics::MetricsExport`], which a process with metrics starts
/// itself.
///
/// Idempotent-ish: uses `try_init` so a double-call (e.g. in tests) is a no-op
/// rather than a panic.
pub fn init(service: &str) {
    let _ = SERVICE.set(service.to_string());
    let format = std::env::var("LOG_FORMAT").unwrap_or_else(|_| "full".to_string());

    let subscriber = tracing_subscriber::registry()
        .with(build_filter())
        .with(fmt_layer());

    #[cfg(feature = "otel")]
    subscriber
        .with(otel_layer(service))
        .with(otel_logs_layer(service))
        .try_init()
        .ok();
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

    // Gate on the standard endpoint env vars: unset ⇒ exporter stays off.
    let endpoint = otlp_signal("TRACES")?;

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

    let provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(otel_resource(service))
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

/// Targets whose events never become OTLP log records: the exporter's own
/// HTTP stack logs as it sends, and a record of sending a record would be
/// sent in turn, without end. They still reach stdout.
#[cfg(feature = "otel")]
fn exporter_internal(target: &str) -> bool {
    ["opentelemetry", "hyper", "h2", "reqwest", "tower", "rustls", "tonic"]
        .iter()
        .any(|t| target == *t || target.starts_with(&format!("{t}::")) || target.starts_with(&format!("{t}_")))
}

/// Build the OTLP log layer: every event the subscriber lets through
/// also goes to the collector as a log record, with its level, its fields and
/// the span it was in, under the same resource as the spans. `None` when no
/// endpoint is set, `OTEL_LOGS_EXPORTER=none`, or the exporter can't be built.
#[cfg(feature = "otel")]
fn otel_logs_layer<S>(service: &str) -> Option<Box<dyn Layer<S> + Send + Sync>>
where
    S: tracing::Subscriber + for<'a> tracing_subscriber::registry::LookupSpan<'a> + Send + Sync,
{
    use opentelemetry_appender_tracing::layer::OpenTelemetryTracingBridge;
    use tracing_subscriber::filter::filter_fn;

    let endpoint = otlp_signal("LOGS")?;
    let not_internal = filter_fn(|meta| !exporter_internal(meta.target()));
    // One provider per process, as for spans: only the stored one is flushed.
    if let Some(existing) = LOGGER_PROVIDER.get() {
        return Some(OpenTelemetryTracingBridge::new(existing).with_filter(not_internal).boxed());
    }
    let exporter = match opentelemetry_otlp::LogExporter::builder().with_http().build() {
        Ok(e) => e,
        Err(e) => {
            eprintln!("dagron: OTLP log export disabled (exporter build failed): {e}");
            return None;
        }
    };
    let provider = opentelemetry_sdk::logs::SdkLoggerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(otel_resource(service))
        .build();
    let _ = LOGGER_PROVIDER.set(provider.clone());
    eprintln!("dagron: OTLP log export enabled → {endpoint}");
    Some(OpenTelemetryTracingBridge::new(&provider).with_filter(not_internal).boxed())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::{Mutex, OnceLock};

    /// Tests here mutate process-global env vars; the default test harness runs
    /// them in parallel, so serialize env access to keep them deterministic.
    pub(crate) fn env_lock() -> &'static Mutex<()> {
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

    /// Colour is for a person at a terminal. `LOG_ANSI` decides where it is
    /// set. Unset, a file, a pipe or a container's log gets none, and neither
    /// does a terminal under `NO_COLOR`.
    #[test]
    fn lines_are_coloured_for_a_terminal_only_unless_told() {
        // LOG_ANSI, NO_COLOR, a terminal: coloured
        let cases = [
            (None, false, true, true),
            (None, false, false, false),
            (None, true, true, false),
            (None, true, false, false),
            (Some(true), true, false, true),
            (Some(false), false, true, false),
        ];
        for (log_ansi, no_color, terminal, coloured) in cases {
            assert_eq!(
                ansi(log_ansi, no_color, terminal),
                coloured,
                "LOG_ANSI={log_ansi:?} NO_COLOR={no_color} terminal={terminal}"
            );
        }
    }

    /// `--version` or `-V`, alone, asks for the version. With anything beside
    /// it, or anywhere but first, it is the program's own to read.
    #[test]
    fn a_version_is_asked_for_by_the_flag_alone() {
        assert!(asks_version(["--version"]));
        assert!(asks_version(["-V"]));
        let others: [&[&str]; 5] = [
            &[],
            &["version"],
            &["--version", "x"],
            &["run", "--version"],
            &["-v"],
        ];
        for args in others {
            assert!(!asks_version(args), "{args:?}");
        }
    }

    /// The line is the build's name and the release, and a build no release
    /// made says so rather than give a number.
    #[test]
    fn the_version_line_is_the_name_and_the_release() {
        assert_eq!(version_line("dagron-api"), format!("dagron-api {VERSION}"));
        match option_env!("APP_VERSION") {
            Some(version) if !version.is_empty() => assert_eq!(VERSION, version),
            _ => assert_eq!(VERSION, "dev"),
        }
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

    #[test]
    fn a_service_is_named_dagron_once() {
        assert_eq!(service_name("controller"), "dagron-controller");
        assert_eq!(service_name("dagron-ci"), "dagron-ci");
        assert_eq!(service_name("dagron-step-sql"), "dagron-step-sql");
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

    /// Each signal goes where its own endpoint or the base one says, and not at
    /// all when its exporter is `none`.
    #[cfg(feature = "otel")]
    #[test]
    fn each_signal_is_gated_on_an_endpoint_and_its_exporter() {
        let _guard = env_lock().lock().expect("env lock poisoned");
        for k in [
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
            "OTEL_LOGS_EXPORTER",
        ] {
            std::env::remove_var(k);
        }
        assert!(super::otlp_signal("LOGS").is_none());
        assert!(super::otel_logs_layer::<tracing_subscriber::Registry>("test").is_none());
        std::env::set_var("OTEL_EXPORTER_OTLP_LOGS_ENDPOINT", "http://logs:4318/v1/logs");
        assert_eq!(super::otlp_signal("LOGS").as_deref(), Some("http://logs:4318/v1/logs"));
        assert!(super::otlp_signal("TRACES").is_none(), "one signal's endpoint is not another's");
        std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318");
        assert_eq!(super::otlp_signal("TRACES").as_deref(), Some("http://collector:4318"));
        std::env::set_var("OTEL_LOGS_EXPORTER", "none");
        assert!(super::otlp_signal("LOGS").is_none());
        for k in [
            "OTEL_EXPORTER_OTLP_ENDPOINT",
            "OTEL_EXPORTER_OTLP_LOGS_ENDPOINT",
            "OTEL_LOGS_EXPORTER",
        ] {
            std::env::remove_var(k);
        }
    }

    /// The exporter's own HTTP stack is kept out of the log records it sends.
    #[cfg(feature = "otel")]
    #[test]
    fn the_exporters_own_events_are_not_exported() {
        for t in ["hyper", "hyper_util::client", "reqwest::connect", "opentelemetry_sdk", "h2::codec"] {
            assert!(super::exporter_internal(t), "{t}");
        }
        for t in ["dagron", "dagron_engine::worker", "hyperloop", "towers"] {
            assert!(!super::exporter_internal(t), "{t}");
        }
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
