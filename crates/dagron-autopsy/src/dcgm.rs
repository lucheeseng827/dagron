//! DCGM — what the GPUs said about themselves.
//!
//! This is the source that turns "job failed" into "GPU 3 on node-47". DCGM is
//! deployed on essentially every production GPU cluster (as `dcgm-exporter`,
//! `nv-hostengine`, or the DCGM diagnostics), and it already records XIDs, ECC
//! counters and row-remap state per device. Nobody joins that against job state,
//! because the health system does not know what a job is and the scheduler does
//! not know what an XID is.
//!
//! Accepts three shapes, because sites export DCGM differently and asking an
//! admin to reshape their telemetry is how a "trivial install" stops being one:
//!
//! 1. **`dcgmi health`/`dcgmi diag` text** — free-form lines naming a host, a
//!    GPU index and an XID or ECC condition.
//! 2. **CSV** with a header (`timestamp,hostname,gpu,xid,message` in any order).
//! 3. **NDJSON** — one JSON object per line, the shape `dcgm-exporter` and most
//!    provider APIs emit.
//!
//! All three land on the same [`Signal`]s, so the correlator never learns which
//! one a site uses.

use crate::nodelist::normalize;
use crate::signal::{Signal, Source};
use crate::timestamp;
use dagron_core::fault::{classify_text, xid_class, Confidence, FaultClass};

/// Parse DCGM output in whichever of the three shapes it arrives in.
///
/// Format detection is by content, not by a flag: the first non-blank line
/// starting with `{` is NDJSON, a line containing `,` and a recognizable header
/// is CSV, anything else is text. Guessing wrong costs nothing — the text
/// parser is a superset that still finds XIDs in a CSV line.
pub fn parse(text: &str) -> Vec<Signal> {
    let first = text.lines().map(str::trim).find(|l| !l.is_empty()).unwrap_or("");
    if first.starts_with('{') {
        return parse_ndjson(text);
    }
    if let Some(sigs) = parse_csv(text) {
        return sigs;
    }
    parse_text(text)
}

/// NDJSON / `dcgm-exporter`-style records. Field names vary by exporter, so
/// each fact is looked up under every spelling seen in the wild rather than
/// pinned to one — an unrecognized field name silently drops the event, and a
/// dropped XID is the diagnosis this tool exists to make.
fn parse_ndjson(text: &str) -> Vec<Signal> {
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Ok(v) = serde_json::from_str::<serde_json::Value>(line) else {
            continue;
        };
        let node = first_str(&v, &["hostname", "host", "node", "Hostname", "instance"])
            .map(|s| normalize(&s))
            .unwrap_or_default();
        if node.is_empty() {
            continue;
        }
        let at = first_str(&v, &["timestamp", "time", "ts", "@timestamp"])
            .and_then(|s| timestamp::parse(&s).ok());
        let Some(at) = at else { continue };
        let gpu = first_num(&v, &["gpu", "gpu_id", "gpuId", "device", "GPU_I_ID", "minor_number"]);
        let message = first_str(&v, &["message", "msg", "error", "text", "description"])
            .unwrap_or_default();
        let xid = first_num(&v, &["xid", "xid_error", "XID"])
            .map(|n| n as u32)
            .or_else(|| dagron_core::fault::parse_xid(&message));
        if let Some(sig) = build(at, &node, gpu, xid, &message) {
            out.push(sig);
        }
    }
    out
}

/// CSV with a header row. Returns `None` when no usable header is present, so
/// the caller falls through to the text parser rather than emitting nothing.
fn parse_csv(text: &str) -> Option<Vec<Signal>> {
    let mut lines = text.lines().filter(|l| !l.trim().is_empty());
    let header = lines.next()?;
    let cols: Vec<String> = header
        .split(',')
        .map(|c| c.trim().trim_matches('"').to_ascii_lowercase())
        .collect();
    let idx = |names: &[&str]| cols.iter().position(|c| names.contains(&c.as_str()));
    let host_i = idx(&["hostname", "host", "node"])?;
    let time_i = idx(&["timestamp", "time", "ts"])?;
    let gpu_i = idx(&["gpu", "gpu_id", "device", "index"]);
    let xid_i = idx(&["xid", "xid_error"]);
    let msg_i = idx(&["message", "msg", "error", "description"]);

    let mut out = Vec::new();
    for line in lines {
        // Deliberately a plain split: DCGM exports do not quote-escape commas,
        // and pulling in a CSV crate for a format this tool merely *tolerates*
        // would be a dependency for the least important input path.
        let f: Vec<&str> = line.split(',').map(str::trim).collect();
        let get = |i: Option<usize>| i.and_then(|i| f.get(i)).map(|s| s.trim_matches('"'));
        let (Some(host), Some(ts)) = (f.get(host_i), f.get(time_i)) else {
            continue;
        };
        let Ok(at) = timestamp::parse(ts.trim_matches('"')) else {
            continue;
        };
        let node = normalize(host.trim_matches('"'));
        let gpu = get(gpu_i).and_then(|s| s.parse::<i64>().ok());
        let message = get(msg_i).unwrap_or("").to_string();
        let xid = get(xid_i)
            .and_then(|s| s.parse::<u32>().ok())
            .or_else(|| dagron_core::fault::parse_xid(&message));
        if let Some(sig) = build(at, &node, gpu, xid, &message) {
            out.push(sig);
        }
    }
    Some(out)
}

/// Free-form `dcgmi` / syslog text. Each line must carry its own timestamp and
/// host, because a line without them cannot be placed in the job's window and
/// an unplaceable signal is worse than no signal — it would match every job.
fn parse_text(text: &str) -> Vec<Signal> {
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let Some(at) = leading_timestamp(line) else { continue };
        let Some((raw_host, node)) = host_token(line) else { continue };
        let gpu = gpu_index(line, Some(raw_host));
        let xid = dagron_core::fault::parse_xid(line).map(|x| x as i64);
        if let Some(sig) = build(at, &node, gpu, xid.map(|x| x as u32), line) {
            out.push(sig);
        }
    }
    out
}

/// Turn one parsed observation into a signal, or `None` when it carries no
/// fault at all (a healthy heartbeat row).
fn build(
    at: chrono::DateTime<chrono::Utc>,
    node: &str,
    gpu: Option<i64>,
    xid: Option<u32>,
    message: &str,
) -> Option<Signal> {
    let device = gpu.map(|g| format!("gpu{g}"));
    if let Some(code) = xid {
        // An XID we have no mapping for is still reported — as `gpu-xid` at low
        // confidence. Dropping it would hide a real device event; promoting it
        // to a known class would invent a disposition for it.
        let class = xid_class(code).unwrap_or(FaultClass::GpuXid);
        let confidence = if xid_class(code).is_some() { Confidence::High } else { Confidence::Low };
        return Some(Signal {
            at,
            node: node.to_string(),
            device,
            rank: None,
            source: Source::Dcgm,
            class,
            confidence,
            detail: if message.trim().is_empty() {
                format!("XID {code} on {node}")
            } else {
                message.trim().to_string()
            },
            dated: true,
        });
    }
    // No XID: fall back to the shared text classifier, which knows ECC,
    // row-remap, NVLink and fallen-off-the-bus prose.
    let c = classify_text(message)?;
    Some(Signal {
        at,
        node: node.to_string(),
        device,
        rank: None,
        source: Source::Dcgm,
        class: c.class,
        confidence: c.confidence,
        detail: c.evidence,
        dated: true,
    })
}

fn first_str(v: &serde_json::Value, keys: &[&str]) -> Option<String> {
    for k in keys {
        match v.get(*k) {
            Some(serde_json::Value::String(s)) => return Some(s.clone()),
            Some(serde_json::Value::Number(n)) => return Some(n.to_string()),
            _ => {}
        }
    }
    None
}

fn first_num(v: &serde_json::Value, keys: &[&str]) -> Option<i64> {
    for k in keys {
        match v.get(*k) {
            Some(serde_json::Value::Number(n)) => return n.as_i64(),
            Some(serde_json::Value::String(s)) => {
                if let Ok(n) = s.trim().parse::<i64>() {
                    return Some(n);
                }
            }
            _ => {}
        }
    }
    None
}

/// A leading timestamp in `[...]`, or the first whitespace-delimited token that
/// parses as one.
fn leading_timestamp(line: &str) -> Option<chrono::DateTime<chrono::Utc>> {
    if let Some(rest) = line.strip_prefix('[') {
        if let Some(end) = rest.find(']') {
            if let Ok(t) = timestamp::parse(&rest[..end]) {
                return Some(t);
            }
        }
    }
    // `2026-08-26T09:57:11 node-47 ...` — a date and a time as one or two tokens.
    let mut it = line.split_whitespace();
    let a = it.next()?;
    if let Ok(t) = timestamp::parse(a) {
        return Some(t);
    }
    let b = it.next()?;
    timestamp::parse(&format!("{a} {b}")).ok()
}

/// The hostname on a line: `host=node-47`, `hostname=node-47`, or a bare token
/// that looks like a node name.
///
/// Returns **both** the raw slice as it appears in the line and the normalized
/// join key. `gpu_index` needs the raw form to locate and skip the host token —
/// on a cluster whose nodes are named `gpu001`, the host is otherwise
/// indistinguishable from a device mention.
fn host_token(line: &str) -> Option<(&str, String)> {
    for tok in line.split_whitespace() {
        let t = tok.trim_matches(|c: char| c == ',' || c == ';');
        for pfx in ["host=", "hostname=", "node=", "Host:", "host:"] {
            if let Some(v) = t.strip_prefix(pfx) {
                if !v.is_empty() {
                    return Some((v, normalize(v)));
                }
            }
        }
    }
    // Fall back to the second token, which is where syslog puts the host.
    let mut it = line.split_whitespace();
    let first = it.next()?;
    let cand = if first.starts_with('[') {
        // `[ts] host ...`
        line.split(']').nth(1)?.split_whitespace().next()?
    } else {
        it.next()?
    };
    let c = cand.trim_matches(|ch: char| ch == ',' || ch == ':');
    // A host token has letters and no `=`; anything else is a field, not a name.
    if c.chars().any(|ch| ch.is_ascii_alphabetic()) && !c.contains('=') {
        Some((c, normalize(c)))
    } else {
        None
    }
}

/// The GPU index named on a line: `GPU 3`, `gpu=3`, `(GPU-3)`, `gpu3`.
///
/// **Skips the host token**, which is why it takes one. A great many HPC
/// clusters name their nodes `gpu001` or `gpu-047`, and this device string
/// lands in the headline verdict, so reading the host as the device drains the
/// wrong hardware. Neither a word-boundary rule nor a "require a separator"
/// rule is enough on its own — `gpu001` *is* at a boundary, and `gpu-047` *has*
/// a separator. The host token is the only thing that reliably distinguishes
/// them, and the caller has already identified it.
///
/// Among the remaining candidates the first one followed by digits wins, so a
/// `gpu` that is part of some other word does not shadow the real device later
/// in the line.
fn gpu_index(line: &str, host_token: Option<&str>) -> Option<i64> {
    let lower = line.to_ascii_lowercase();
    // Byte span of the host token, so occurrences inside it can be skipped.
    // Offsets are shared with `line` because `to_ascii_lowercase` preserves
    // length.
    let host_span = host_token
        .and_then(|h| line.find(h).map(|i| (i, i + h.len())));
    for (at, _) in lower.match_indices("gpu") {
        if host_span.is_some_and(|(lo, hi)| at >= lo && at < hi) {
            continue;
        }
        // `gpu` glued to a preceding alphanumeric is part of a longer word.
        if lower[..at]
            .chars()
            .next_back()
            .is_some_and(|c| c.is_ascii_alphanumeric())
        {
            continue;
        }
        if let Some(n) = index_after(&lower[at + 3..]) {
            return Some(n);
        }
    }
    None
}

/// The digit run following a `gpu` token, skipping the separators that appear
/// between the word and the index. `None` when no digits follow.
fn index_after(rest: &str) -> Option<i64> {
    let mut digits = String::new();
    for ch in rest.chars() {
        if ch.is_ascii_digit() {
            digits.push(ch);
        } else if !digits.is_empty() {
            break;
        } else if ch == ' ' || ch == '=' || ch == '-' || ch == ':' || ch == '_' {
            continue;
        } else {
            break;
        }
    }
    digits.parse().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reads_dcgmi_style_text_with_an_xid() {
        let text = "\
[2026-08-26T09:57:11Z] node-47.hpc.internal GPU 3: NVRM: Xid (PCI:0000:1b:00): 79, GPU has fallen off the bus
[2026-08-26T09:57:12Z] node-48 GPU 0: health check passed
";
        let sigs = parse(text);
        assert_eq!(sigs.len(), 1, "only the fault line becomes a signal");
        assert_eq!(sigs[0].node, "node-47", "FQDN normalized to the join key");
        assert_eq!(sigs[0].device.as_deref(), Some("gpu3"));
        assert_eq!(sigs[0].class, FaultClass::GpuFallenOffBus);
        assert_eq!(sigs[0].confidence, Confidence::High);
    }

    #[test]
    fn reads_ndjson_from_an_exporter() {
        let text = r#"
{"timestamp":"2026-08-26T09:57:11Z","hostname":"node-47","gpu":3,"xid":48,"message":"Double Bit ECC Error"}
{"timestamp":"2026-08-26T09:57:20Z","hostname":"node-48","gpu":1,"message":"all clear"}
"#;
        let sigs = parse(text);
        assert_eq!(sigs.len(), 1);
        assert_eq!(sigs[0].class, FaultClass::GpuEcc);
        assert_eq!(sigs[0].device.as_deref(), Some("gpu3"));
    }

    #[test]
    fn reads_csv_with_the_columns_in_any_order() {
        let text = "\
gpu,hostname,timestamp,xid,message
3,node-47,1756202231,79,GPU has fallen off the bus
";
        let sigs = parse(text);
        assert_eq!(sigs.len(), 1);
        assert_eq!(sigs[0].class, FaultClass::GpuFallenOffBus);
        assert_eq!(sigs[0].node, "node-47");
    }

    #[test]
    fn an_unmapped_xid_is_reported_at_low_confidence_not_dropped_or_promoted() {
        let text = r#"{"timestamp":"2026-08-26T09:57:11Z","hostname":"n1","gpu":0,"xid":211,"message":"novel"}"#;
        let sigs = parse(text);
        assert_eq!(sigs.len(), 1);
        assert_eq!(sigs[0].class, FaultClass::GpuXid);
        assert_eq!(sigs[0].confidence, Confidence::Low, "no invented disposition");
    }

    #[test]
    fn a_line_that_cannot_be_placed_in_time_is_discarded() {
        // An unplaceable signal is worse than none: it would match every job's
        // window and attribute one node's failure to all of them.
        let sigs = parse("node-47 GPU 3: Xid 79 fell off the bus\n");
        assert!(sigs.is_empty());
    }

    #[test]
    fn ecc_prose_without_an_xid_still_classifies() {
        let text = "[2026-08-26T09:57:11Z] node-47 GPU 2: row remap failure detected\n";
        let sigs = parse(text);
        assert_eq!(sigs.len(), 1);
        assert_eq!(sigs[0].class, FaultClass::GpuEcc);
        assert_eq!(sigs[0].device.as_deref(), Some("gpu2"));
    }

    #[test]
    fn gpu_index_reads_the_common_spellings() {
        assert_eq!(gpu_index("GPU 3:", None), Some(3));
        assert_eq!(gpu_index("gpu=11 something", None), Some(11));
        assert_eq!(gpu_index("(GPU-7)", None), Some(7));
        assert_eq!(gpu_index("no device here", None), None);
    }

    #[test]
    fn a_gpu_named_host_does_not_get_read_as_the_device() {
        // Clusters named gpu001 / gpu-047 are everywhere in HPC, and this
        // string lands in the headline verdict — reading the host as the device
        // drains the wrong hardware. Neither a boundary rule nor a
        // require-a-separator rule catches both of these; the host token does.
        assert_eq!(gpu_index("[ts] gpu001 GPU 3: Xid 79", Some("gpu001")), Some(3));
        assert_eq!(gpu_index("[ts] gpu-047 GPU 3: Xid 79", Some("gpu-047")), Some(3));
        assert_eq!(gpu_index("[ts] gpu-node-12 GPU 5: Xid 48", Some("gpu-node-12")), Some(5));
        // And the host alone, with no device named, yields nothing rather than
        // the host's own digits.
        assert_eq!(gpu_index("[ts] gpu001 ECC error", Some("gpu001")), None);
    }

    #[test]
    fn a_gpu_named_host_survives_the_whole_text_path() {
        // The end-to-end version of the case above: the emitted device must be
        // gpu3, not gpu1 (the host's digits).
        let text = "[2026-08-26T09:57:11Z] gpu001 GPU 3: NVRM: Xid (PCI:0000:1b:00): 79, GPU has fallen off the bus\n";
        let sigs = parse(text);
        assert_eq!(sigs.len(), 1);
        assert_eq!(sigs[0].node, "gpu001");
        assert_eq!(sigs[0].device.as_deref(), Some("gpu3"), "the device, not the host");
    }

    #[test]
    fn a_prometheus_instance_label_joins_after_the_port_is_stripped() {
        // dcgm-exporter's `instance` is the scrape target, `node-47:9400`.
        // Left unstripped, the node key never matches the sacct node set and
        // the whole autopsy silently reports a clean cluster.
        let text = r#"{"timestamp":"2026-08-26T09:57:11Z","instance":"node-47:9400","gpu":3,"xid":79,"message":"fell off the bus"}"#;
        let sigs = parse(text);
        assert_eq!(sigs.len(), 1);
        assert_eq!(sigs[0].node, "node-47");
    }
}
