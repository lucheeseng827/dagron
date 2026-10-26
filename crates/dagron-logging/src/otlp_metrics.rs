//! OTLP metrics: what a process serves at `GET /metrics`, pushed to a
//! collector as well.
//!
//! Every dagron process that has metrics renders them as the Prometheus text
//! exposition, from plain atomics and no registry crate. This module reads
//! that text back and sends it as an OTLP `ExportMetricsServiceRequest`
//! (HTTP/protobuf, as the span exporter sends spans), so there is one source
//! of truth for each series and a collector sees exactly what a scrape would:
//!
//! | exposition  | OTLP                                                    |
//! |-------------|---------------------------------------------------------|
//! | `counter`   | a monotonic `Sum`, cumulative                           |
//! | `gauge`     | a `Gauge`                                               |
//! | `histogram` | a `Histogram`, cumulative, its `le` bounds as the bounds |
//! | untyped     | a `Gauge`                                               |
//!
//! Each series' labels are its data point's attributes, and the resource is the
//! one the spans and the logs carry (`service.name` = `dagron-<service>`, and
//! `OTEL_RESOURCE_ATTRIBUTES`). A point's start is when the exporter was made:
//! the counters are counted from process start, which is close enough to it to
//! read a rate from, and a restart shows as a new start.
//!
//! Configured as the OpenTelemetry SDKs are:
//!
//! | Env var                                | Default                         |
//! |----------------------------------------|---------------------------------|
//! | `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT`  | `OTEL_EXPORTER_OTLP_ENDPOINT` + `/v1/metrics` |
//! | `OTEL_EXPORTER_OTLP_METRICS_HEADERS`   | `OTEL_EXPORTER_OTLP_HEADERS`    |
//! | `OTEL_EXPORTER_OTLP_METRICS_TIMEOUT`   | `OTEL_EXPORTER_OTLP_TIMEOUT`, 10000 ms |
//! | `OTEL_METRIC_EXPORT_INTERVAL`          | 60000 ms                        |
//! | `OTEL_METRICS_EXPORTER`                | `otlp`; `none` turns it off     |
//!
//! The export runs on a thread of its own with a blocking client: a caller on
//! an async runtime hands it the text ([`MetricsExport::send`]) and never waits
//! on the collector.

use std::collections::BTreeMap;
use std::sync::mpsc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use opentelemetry_proto::tonic::collector::metrics::v1::ExportMetricsServiceRequest;
use opentelemetry_proto::tonic::common::v1::{any_value, AnyValue, InstrumentationScope, KeyValue};
use opentelemetry_proto::tonic::metrics::v1::{
    metric, number_data_point, AggregationTemporality, Gauge, Histogram, HistogramDataPoint,
    Metric, NumberDataPoint, ResourceMetrics, ScopeMetrics, Sum,
};
use opentelemetry_proto::tonic::resource::v1::Resource;
use prost::Message as _;

/// A metric family as the exposition declares it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Counter,
    Gauge,
    Histogram,
    Untyped,
}

#[derive(Debug, Clone, PartialEq)]
struct Sample {
    /// The sample's own name: a histogram's carry `_bucket`, `_sum`, `_count`.
    name: String,
    labels: Vec<(String, String)>,
    value: f64,
}

#[derive(Debug, Clone)]
struct Family {
    name: String,
    help: String,
    kind: Kind,
    samples: Vec<Sample>,
}

impl Family {
    /// Whether a sample named `sample` is one of this family's.
    fn owns(&self, sample: &str) -> bool {
        let Some(rest) = sample.strip_prefix(self.name.as_str()) else {
            return false;
        };
        match self.kind {
            Kind::Histogram => matches!(rest, "_bucket" | "_sum" | "_count"),
            Kind::Counter => rest.is_empty() || rest == "_total",
            Kind::Gauge | Kind::Untyped => rest.is_empty(),
        }
    }
}

/// Read a text exposition back into its families, in the order they came. A
/// line that does not parse is skipped: the text is our own, and one odd
/// series is no reason to drop the rest.
fn parse(text: &str) -> Vec<Family> {
    let mut families: Vec<Family> = Vec::new();
    let mut help: BTreeMap<String, String> = BTreeMap::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if let Some(rest) = line.strip_prefix('#') {
            let rest = rest.trim_start();
            if let Some(rest) = rest.strip_prefix("HELP ") {
                if let Some((name, text)) = rest.split_once(' ') {
                    help.insert(name.to_string(), unescape_help(text));
                }
            } else if let Some(rest) = rest.strip_prefix("TYPE ") {
                let mut parts = rest.split_whitespace();
                if let (Some(name), Some(kind)) = (parts.next(), parts.next()) {
                    let kind = match kind {
                        "counter" => Kind::Counter,
                        "gauge" => Kind::Gauge,
                        "histogram" => Kind::Histogram,
                        _ => Kind::Untyped,
                    };
                    families.push(Family {
                        name: name.to_string(),
                        help: String::new(),
                        kind,
                        samples: Vec::new(),
                    });
                }
            }
            continue;
        }
        let Some(sample) = parse_sample(line) else {
            continue;
        };
        match families.iter_mut().rev().find(|f| f.owns(&sample.name)) {
            Some(family) => family.samples.push(sample),
            None => families.push(Family {
                name: sample.name.clone(),
                help: String::new(),
                kind: Kind::Untyped,
                samples: vec![sample],
            }),
        }
    }
    for family in &mut families {
        if let Some(text) = help.remove(&family.name) {
            family.help = text;
        }
    }
    families
}

/// `name{a="b",c="d"} value [timestamp]`.
fn parse_sample(line: &str) -> Option<Sample> {
    let name_end = line.find(|c: char| c == '{' || c.is_whitespace())?;
    let name = &line[..name_end];
    let mut rest = &line[name_end..];
    let mut labels = Vec::new();
    if let Some(after) = rest.strip_prefix('{') {
        let mut chars = after.char_indices().peekable();
        loop {
            // Skip a separator, then read `key="value"` or the closing brace.
            while let Some((_, c)) = chars.peek() {
                if *c == ',' || c.is_whitespace() {
                    chars.next();
                } else {
                    break;
                }
            }
            let (start, c) = chars.next()?;
            if c == '}' {
                rest = &after[start + 1..];
                break;
            }
            let eq = after[start..].find('=')? + start;
            let key = after[start..eq].trim().to_string();
            while let Some((i, _)) = chars.peek() {
                if *i <= eq {
                    chars.next();
                } else {
                    break;
                }
            }
            if chars.next()?.1 != '"' {
                return None;
            }
            let mut value = String::new();
            loop {
                let (_, c) = chars.next()?;
                match c {
                    '"' => break,
                    '\\' => match chars.next()?.1 {
                        'n' => value.push('\n'),
                        other => value.push(other),
                    },
                    other => value.push(other),
                }
            }
            labels.push((key, value));
        }
    }
    let value = rest.split_whitespace().next()?;
    let value = match value {
        "+Inf" | "Inf" => f64::INFINITY,
        "-Inf" => f64::NEG_INFINITY,
        "NaN" => f64::NAN,
        v => v.parse().ok()?,
    };
    Some(Sample {
        name: name.to_string(),
        labels,
        value,
    })
}

fn unescape_help(text: &str) -> String {
    let mut out = String::with_capacity(text.len());
    let mut chars = text.chars();
    while let Some(c) = chars.next() {
        if c == '\\' {
            match chars.next() {
                Some('n') => out.push('\n'),
                Some(other) => out.push(other),
                None => out.push('\\'),
            }
        } else {
            out.push(c);
        }
    }
    out
}

fn attributes(labels: &[(String, String)]) -> Vec<KeyValue> {
    labels
        .iter()
        .map(|(k, v)| KeyValue {
            key: k.clone(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue(v.clone())),
            }),
            ..Default::default()
        })
        .collect()
}

/// The OTLP metric for one family, or `None` for a family with no samples.
fn metric(family: &Family, start: u64, now: u64) -> Option<Metric> {
    if family.samples.is_empty() {
        return None;
    }
    let number = |s: &Sample| NumberDataPoint {
        attributes: attributes(&s.labels),
        start_time_unix_nano: start,
        time_unix_nano: now,
        value: Some(number_data_point::Value::AsDouble(s.value)),
        ..Default::default()
    };
    let data = match family.kind {
        Kind::Counter => metric::Data::Sum(Sum {
            data_points: family.samples.iter().map(number).collect(),
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
            is_monotonic: true,
        }),
        Kind::Gauge | Kind::Untyped => metric::Data::Gauge(Gauge {
            data_points: family
                .samples
                .iter()
                .map(|s| NumberDataPoint {
                    start_time_unix_nano: 0,
                    ..number(s)
                })
                .collect(),
        }),
        Kind::Histogram => metric::Data::Histogram(Histogram {
            data_points: histogram_points(family, start, now),
            aggregation_temporality: AggregationTemporality::Cumulative as i32,
        }),
    };
    Some(Metric {
        name: family.name.clone(),
        description: family.help.clone(),
        data: Some(data),
        ..Default::default()
    })
}

/// A histogram family's points, one per label set. The exposition's buckets
/// are cumulative and OTLP's are not: each bucket is its count less the one
/// below it, and the last, past the highest finite bound, is the total less
/// the highest finite bucket.
fn histogram_points(family: &Family, start: u64, now: u64) -> Vec<HistogramDataPoint> {
    #[derive(Default)]
    struct Series {
        buckets: Vec<(f64, f64)>,
        sum: Option<f64>,
        count: Option<f64>,
    }
    let mut series: BTreeMap<Vec<(String, String)>, Series> = BTreeMap::new();
    let mut order: Vec<Vec<(String, String)>> = Vec::new();
    for s in &family.samples {
        let suffix = &s.name[family.name.len()..];
        let mut key: Vec<(String, String)> = s
            .labels
            .iter()
            .filter(|(k, _)| k != "le")
            .cloned()
            .collect();
        key.sort();
        if !series.contains_key(&key) {
            order.push(key.clone());
        }
        let entry = series.entry(key).or_default();
        match suffix {
            "_bucket" => {
                let le = s
                    .labels
                    .iter()
                    .find(|(k, _)| k == "le")
                    .map(|(_, v)| v.as_str());
                let bound = match le {
                    Some("+Inf") | Some("Inf") => f64::INFINITY,
                    Some(v) => match v.parse() {
                        Ok(b) => b,
                        Err(_) => continue,
                    },
                    None => continue,
                };
                entry.buckets.push((bound, s.value));
            }
            "_sum" => entry.sum = Some(s.value),
            "_count" => entry.count = Some(s.value),
            _ => {}
        }
    }
    order
        .into_iter()
        .filter_map(|key| {
            let mut s = series.remove(&key)?;
            s.buckets.sort_by(|a, b| a.0.total_cmp(&b.0));
            let finite: Vec<(f64, f64)> = s
                .buckets
                .iter()
                .copied()
                .filter(|(b, _)| b.is_finite())
                .collect();
            let total = s.count.or_else(|| {
                s.buckets
                    .iter()
                    .find(|(b, _)| b.is_infinite())
                    .map(|(_, c)| *c)
            })?;
            let mut counts = Vec::with_capacity(finite.len() + 1);
            let mut below = 0.0;
            for (_, cumulative) in &finite {
                counts.push((cumulative - below).max(0.0) as u64);
                below = *cumulative;
            }
            counts.push((total - below).max(0.0) as u64);
            Some(HistogramDataPoint {
                attributes: attributes(&key),
                start_time_unix_nano: start,
                time_unix_nano: now,
                count: total as u64,
                sum: s.sum,
                bucket_counts: counts,
                explicit_bounds: finite.into_iter().map(|(b, _)| b).collect(),
                ..Default::default()
            })
        })
        .collect()
}

/// The request for one exposition.
fn request(text: &str, resource: &[KeyValue], start: u64, now: u64) -> ExportMetricsServiceRequest {
    let metrics = parse(text)
        .iter()
        .filter_map(|f| metric(f, start, now))
        .collect();
    ExportMetricsServiceRequest {
        resource_metrics: vec![ResourceMetrics {
            resource: Some(Resource {
                attributes: resource.to_vec(),
                ..Default::default()
            }),
            scope_metrics: vec![ScopeMetrics {
                scope: Some(InstrumentationScope {
                    name: "dagron".to_string(),
                    version: crate::VERSION.to_string(),
                    ..Default::default()
                }),
                metrics,
                ..Default::default()
            }],
            ..Default::default()
        }],
    }
}

fn unix_nanos(t: SystemTime) -> u64 {
    t.duration_since(UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
}

fn env(key: &str) -> Option<String> {
    std::env::var(key)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty())
}

fn env_millis(keys: &[&str], default: u64) -> Duration {
    let ms = keys
        .iter()
        .find_map(|k| env(k).and_then(|v| v.parse::<u64>().ok()))
        .filter(|ms| *ms > 0)
        .unwrap_or(default);
    Duration::from_millis(ms)
}

/// Where metrics go: the signal's own endpoint as it is, or the base one with
/// `/v1/metrics`, as the OTLP exporter spec has it. `None` when neither is set.
fn endpoint() -> Option<String> {
    if let Some(url) = env("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT") {
        return Some(url);
    }
    env("OTEL_EXPORTER_OTLP_ENDPOINT")
        .map(|base| format!("{}/v1/metrics", base.trim_end_matches('/')))
}

/// `k1=v1,k2=v2`, each value percent-decoded, as `OTEL_EXPORTER_OTLP_HEADERS`.
fn headers(raw: &str) -> Vec<(String, String)> {
    raw.split(',')
        .filter_map(|pair| {
            let (k, v) = pair.split_once('=')?;
            let k = k.trim();
            (!k.is_empty()).then(|| (k.to_string(), percent_decode(v.trim())))
        })
        .collect()
}

fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            let hex = |b: u8| (b as char).to_digit(16);
            if let (Some(hi), Some(lo)) = (hex(bytes[i + 1]), hex(bytes[i + 2])) {
                out.push((hi * 16 + lo) as u8);
                i += 3;
                continue;
            }
        }
        out.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

/// Whether OTLP metrics are wanted: an endpoint, and `OTEL_METRICS_EXPORTER`
/// not `none`.
fn wanted() -> Option<String> {
    if env("OTEL_METRICS_EXPORTER").is_some_and(|v| v.eq_ignore_ascii_case("none")) {
        return None;
    }
    endpoint()
}

/// A process's OTLP metrics export: a thread that posts each exposition it is
/// sent to the collector.
pub struct MetricsExport {
    tx: mpsc::SyncSender<String>,
    interval: Duration,
}

impl MetricsExport {
    /// The export the environment asks for, for `service`, or `None`: no
    /// endpoint, `OTEL_METRICS_EXPORTER=none`, or no thread to run it on.
    pub fn from_env(service: &str) -> Option<Self> {
        let endpoint = wanted()?;
        let headers = headers(
            &env("OTEL_EXPORTER_OTLP_METRICS_HEADERS")
                .or_else(|| env("OTEL_EXPORTER_OTLP_HEADERS"))
                .unwrap_or_default(),
        );
        let timeout = env_millis(
            &[
                "OTEL_EXPORTER_OTLP_METRICS_TIMEOUT",
                "OTEL_EXPORTER_OTLP_TIMEOUT",
            ],
            10_000,
        );
        let interval = env_millis(&["OTEL_METRIC_EXPORT_INTERVAL"], 60_000);
        let resource: Vec<KeyValue> = crate::otel_resource(service)
            .iter()
            .map(|(k, v)| KeyValue {
                key: k.to_string(),
                value: Some(AnyValue::from(v.clone())),
                ..Default::default()
            })
            .collect();
        let start = unix_nanos(SystemTime::now());
        // One exposition waits at most: a collector slower than the interval
        // drops the stale one rather than queueing behind it.
        let (tx, rx) = mpsc::sync_channel::<String>(1);
        let target = endpoint.clone();
        std::thread::Builder::new()
            .name("dagron-otlp-metrics".into())
            .spawn(move || {
                // Made here, on its own thread: a blocking client must not be
                // made or dropped on an async runtime's.
                let client = match reqwest::blocking::Client::builder()
                    .timeout(timeout)
                    .build()
                {
                    Ok(c) => c,
                    Err(e) => {
                        eprintln!("dagron: OTLP metric export disabled (client build failed): {e}");
                        return;
                    }
                };
                for text in rx {
                    let body = request(&text, &resource, start, unix_nanos(SystemTime::now()))
                        .encode_to_vec();
                    let req = headers.iter().fold(
                        client
                            .post(&target)
                            .header("content-type", "application/x-protobuf")
                            .body(body),
                        |req, (k, v)| req.header(k.as_str(), v.as_str()),
                    );
                    match req.send() {
                        Ok(res) if res.status().is_success() => {}
                        Ok(res) => eprintln!(
                            "dagron: OTLP metric export: {target} answered {}",
                            res.status()
                        ),
                        Err(e) => eprintln!("dagron: OTLP metric export to {target} failed: {e}"),
                    }
                }
            })
            .ok()?;
        eprintln!("dagron: OTLP metric export enabled → {endpoint}");
        Some(Self { tx, interval })
    }

    /// How often to send: `OTEL_METRIC_EXPORT_INTERVAL`.
    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Hand the exporter an exposition to send. Never blocks: one already
    /// waiting is newer than the collector has room for, and this one is
    /// dropped.
    pub fn send(&self, exposition: String) {
        let _ = self.tx.try_send(exposition);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TEXT: &str = r#"# HELP scheduler_runs_created_total Runs created by this scheduler since boot.
# TYPE scheduler_runs_created_total counter
scheduler_runs_created_total 12
# HELP scheduler_ready_tasks Ready tasks, by runner class.
# TYPE scheduler_ready_tasks gauge
scheduler_ready_tasks{runner_class="default"} 3
scheduler_ready_tasks{runner_class="gpu \"a\""} 1
# HELP task_seconds Task wall time.\nIn seconds.
# TYPE task_seconds histogram
task_seconds_bucket{le="0.5"} 2
task_seconds_bucket{le="1"} 5
task_seconds_bucket{le="+Inf"} 7
task_seconds_sum 9.5
task_seconds_count 7
stray_series 4
"#;

    fn by_name<'a>(req: &'a ExportMetricsServiceRequest, name: &str) -> &'a Metric {
        req.resource_metrics[0].scope_metrics[0]
            .metrics
            .iter()
            .find(|m| m.name == name)
            .unwrap_or_else(|| panic!("no metric {name}"))
    }

    fn double(p: &NumberDataPoint) -> f64 {
        match p.value {
            Some(number_data_point::Value::AsDouble(v)) => v,
            _ => panic!("not a double"),
        }
    }

    #[test]
    fn a_counter_is_a_monotonic_cumulative_sum() {
        let req = request(TEXT, &[], 100, 200);
        let m = by_name(&req, "scheduler_runs_created_total");
        assert_eq!(m.description, "Runs created by this scheduler since boot.");
        let Some(metric::Data::Sum(sum)) = &m.data else {
            panic!("not a sum")
        };
        assert!(sum.is_monotonic);
        assert_eq!(
            sum.aggregation_temporality,
            AggregationTemporality::Cumulative as i32
        );
        assert_eq!(double(&sum.data_points[0]), 12.0);
        assert_eq!(sum.data_points[0].start_time_unix_nano, 100);
        assert_eq!(sum.data_points[0].time_unix_nano, 200);
    }

    #[test]
    fn a_gauge_keeps_each_series_with_its_labels() {
        let req = request(TEXT, &[], 100, 200);
        let Some(metric::Data::Gauge(g)) = &by_name(&req, "scheduler_ready_tasks").data else {
            panic!("not a gauge")
        };
        assert_eq!(g.data_points.len(), 2);
        let label = |p: &NumberDataPoint| match &p.attributes[0].value.as_ref().unwrap().value {
            Some(any_value::Value::StringValue(s)) => s.clone(),
            _ => panic!(),
        };
        assert_eq!(g.data_points[0].attributes[0].key, "runner_class");
        assert_eq!(label(&g.data_points[0]), "default");
        assert_eq!(
            label(&g.data_points[1]),
            "gpu \"a\"",
            "an escaped quote is read back"
        );
        assert_eq!(double(&g.data_points[1]), 1.0);
    }

    #[test]
    fn a_histogram_keeps_its_bounds_and_uncumulates_its_buckets() {
        let req = request(TEXT, &[], 100, 200);
        let m = by_name(&req, "task_seconds");
        assert_eq!(m.description, "Task wall time.\nIn seconds.");
        let Some(metric::Data::Histogram(h)) = &m.data else {
            panic!("not a histogram")
        };
        let p = &h.data_points[0];
        assert_eq!(p.explicit_bounds, vec![0.5, 1.0]);
        assert_eq!(
            p.bucket_counts,
            vec![2, 3, 2],
            "2 ≤ 0.5, 3 in (0.5, 1], 2 above"
        );
        assert_eq!(p.count, 7);
        assert_eq!(p.sum, Some(9.5));
        assert_eq!(p.bucket_counts.iter().sum::<u64>(), p.count);
    }

    #[test]
    fn a_series_without_a_type_is_a_gauge() {
        let req = request(TEXT, &[], 100, 200);
        assert!(matches!(
            by_name(&req, "stray_series").data,
            Some(metric::Data::Gauge(_))
        ));
    }

    #[test]
    fn the_request_carries_the_resource_and_round_trips_as_protobuf() {
        let resource = vec![KeyValue {
            key: "service.name".into(),
            value: Some(AnyValue {
                value: Some(any_value::Value::StringValue("dagron-controller".into())),
            }),
            ..Default::default()
        }];
        let req = request(TEXT, &resource, 1, 2);
        let decoded = ExportMetricsServiceRequest::decode(req.encode_to_vec().as_slice()).unwrap();
        assert_eq!(decoded, req);
        let rm = &decoded.resource_metrics[0];
        assert_eq!(rm.resource.as_ref().unwrap().attributes, resource);
        assert_eq!(rm.scope_metrics[0].scope.as_ref().unwrap().name, "dagron");
        assert_eq!(rm.scope_metrics[0].metrics.len(), 4);
    }

    #[test]
    fn the_engines_own_exposition_parses_whole() {
        // Every family the engine renders comes through with its samples.
        let text = "# TYPE a_total counter\na_total 1\n# TYPE b gauge\nb 2\nnot a sample line {\n";
        let families = parse(text);
        assert_eq!(
            families.len(),
            2,
            "a line that does not parse is skipped: {families:?}"
        );
        assert_eq!(families[0].samples[0].value, 1.0);
    }

    #[test]
    fn headers_are_split_and_decoded() {
        assert_eq!(
            headers("api-key=a%20b, x-tenant = acme ,=bad,novalue"),
            vec![
                ("api-key".to_string(), "a b".to_string()),
                ("x-tenant".to_string(), "acme".to_string())
            ]
        );
        assert_eq!(percent_decode("100%"), "100%");
        assert_eq!(percent_decode("%2Fx"), "/x");
    }

    #[test]
    fn the_endpoint_is_the_signals_own_or_the_base_with_its_path() {
        let _guard = crate::tests::env_lock()
            .lock()
            .unwrap_or_else(|e| e.into_inner());
        std::env::remove_var("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT");
        std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
        std::env::remove_var("OTEL_METRICS_EXPORTER");
        assert_eq!(wanted(), None, "nothing set, nothing sent");
        std::env::set_var("OTEL_EXPORTER_OTLP_ENDPOINT", "http://collector:4318/");
        assert_eq!(
            wanted().as_deref(),
            Some("http://collector:4318/v1/metrics")
        );
        std::env::set_var(
            "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
            "http://m:4318/custom",
        );
        assert_eq!(
            wanted().as_deref(),
            Some("http://m:4318/custom"),
            "the signal's own wins, as it is"
        );
        std::env::set_var("OTEL_METRICS_EXPORTER", "none");
        assert_eq!(
            wanted(),
            None,
            "`none` turns metrics off whatever the endpoint"
        );
        std::env::remove_var("OTEL_EXPORTER_OTLP_METRICS_ENDPOINT");
        std::env::remove_var("OTEL_EXPORTER_OTLP_ENDPOINT");
        std::env::remove_var("OTEL_METRICS_EXPORTER");
    }
}
