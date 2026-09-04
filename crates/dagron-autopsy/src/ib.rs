//! InfiniBand / RoCE — the fabric's side of the story.
//!
//! The fabric is the one place where a *counter delta* is the evidence, not a
//! log line: `perfquery` and UFM report monotonically increasing error counters,
//! and a healthy cluster has non-zero lifetime counters everywhere. Reporting a
//! fault because `symbol_error > 0` would flag every port on every machine, so
//! this module only ever believes a **change inside the job's window**, which is
//! why it takes two samples rather than one.
//!
//! Two input shapes:
//!
//! 1. **Counter samples** — `perfquery`-style or sysfs
//!    (`/sys/class/infiniband/*/ports/*/counters/*`), one reading per line, two
//!    passes (before/after) so a delta can be computed.
//! 2. **Events** — UFM / `ibwarn` / syslog lines that already state a link
//!    transition. Those are believed as written, dated from the line.

use crate::nodelist::normalize;
use crate::signal::{Signal, Source};
use crate::timestamp;
use chrono::{DateTime, Utc};
use dagron_core::fault::{Confidence, FaultClass};
use std::collections::BTreeMap;

/// The counters worth reading, and what a *rise* in each one means.
///
/// Only counters whose increase implies a real transport problem. Deliberately
/// **not** here: `port_rcv_data`, `port_xmit_wait` and friends — congestion
/// signals that rise on a perfectly healthy busy fabric, and whose inclusion
/// would make every large job look broken.
const ERROR_COUNTERS: &[(&str, &str)] = &[
    ("link_downed", "the link went down and re-trained"),
    ("link_error_recovery", "the link error-recovered"),
    ("symbol_error", "symbol errors — a marginal cable or transceiver"),
    ("port_rcv_errors", "receive errors"),
    ("port_xmit_discards", "transmit discards"),
    ("local_link_integrity_errors", "link-integrity errors"),
    ("excessive_buffer_overrun_errors", "buffer overruns"),
    ("port_rcv_remote_physical_errors", "remote physical errors"),
];

/// One counter reading: which node, which port, which counter, what value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Sample {
    pub node: String,
    pub port: String,
    pub counter: String,
    pub value: u64,
}

/// Parse counter samples in the tolerated shapes:
///
/// * `node-47 mlx5_0:1 symbol_error 12`
/// * `node-47,mlx5_0:1,symbol_error,12`
/// * `node-47 mlx5_0:1 symbol_error=12`
///
/// Unparseable lines are skipped rather than failing: these files are usually
/// produced by a site's own shell loop, and one odd line should not cost the
/// whole fabric picture.
pub fn parse_samples(text: &str) -> Vec<Sample> {
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let f: Vec<&str> = if line.contains(',') {
            line.split(',').map(str::trim).collect()
        } else {
            line.split_whitespace().collect()
        };
        let (node, port, rest) = match f.as_slice() {
            [n, p, r @ ..] if !r.is_empty() => (*n, *p, r),
            _ => continue,
        };
        // `counter value` or `counter=value`.
        let (counter, value) = if rest.len() >= 2 {
            (rest[0], rest[1])
        } else if let Some((c, v)) = rest[0].split_once('=') {
            (c, v)
        } else {
            continue;
        };
        let Ok(value) = value.trim().parse::<u64>() else { continue };
        out.push(Sample {
            node: normalize(node),
            port: port.to_string(),
            counter: counter.trim().to_ascii_lowercase(),
            value,
        });
    }
    out
}

/// Turn a before/after pair of counter dumps into signals.
///
/// Only counters that **rose** produce a signal, and only counters in
/// [`ERROR_COUNTERS`]. A counter present in `after` but absent from `before` is
/// ignored rather than treated as a rise from zero: the usual cause is that the
/// before-pass did not cover that port, and inventing a link flap out of a
/// missing baseline is exactly the false positive that gets a tool uninstalled.
///
/// `at` is the timestamp assigned to every derived signal — a delta has no
/// instant of its own, only the window it was measured over — so pass the
/// window's end and expect [`Signal::dated`] `= false`.
pub fn diff(before: &[Sample], after: &[Sample], at: DateTime<Utc>) -> Vec<Signal> {
    let key = |s: &Sample| (s.node.clone(), s.port.clone(), s.counter.clone());
    let base: BTreeMap<_, u64> = before.iter().map(|s| (key(s), s.value)).collect();

    let mut out = Vec::new();
    for s in after {
        let Some(explanation) = ERROR_COUNTERS
            .iter()
            .find(|(name, _)| *name == s.counter)
            .map(|(_, why)| *why)
        else {
            continue;
        };
        let Some(&prev) = base.get(&key(s)) else { continue };
        if s.value <= prev {
            continue;
        }
        let delta = s.value - prev;
        // A link that went down during the job is a different animal from a
        // handful of symbol errors: the first is almost certainly the cause,
        // the second is a marginal cable that may or may not be.
        let confidence = if s.counter == "link_downed" || s.counter == "link_error_recovery" {
            Confidence::High
        } else if delta >= 100 {
            Confidence::Medium
        } else {
            Confidence::Low
        };
        out.push(Signal {
            at,
            node: s.node.clone(),
            device: Some(s.port.clone()),
            rank: None,
            source: Source::Ib,
            class: FaultClass::FabricIb,
            confidence,
            detail: format!(
                "{} {}: {} +{} during the job window ({})",
                s.node, s.port, s.counter, delta, explanation
            ),
            dated: false,
        });
    }
    out
}

/// Parse fabric *event* lines (UFM, `ibwarn`, syslog), which state a transition
/// outright and carry their own clock.
pub fn parse_events(text: &str) -> Vec<Signal> {
    let mut out = Vec::new();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() || line.starts_with('#') {
            continue;
        }
        let Some(c) = dagron_core::fault::classify_text(line) else { continue };
        if c.class != FaultClass::FabricIb {
            continue;
        }
        let Some(at) = event_timestamp(line) else { continue };
        out.push(Signal {
            at,
            node: event_node(line).unwrap_or_default(),
            device: event_port(line),
            rank: None,
            source: Source::Ib,
            class: FaultClass::FabricIb,
            confidence: c.confidence,
            detail: c.evidence,
            dated: true,
        });
    }
    out
}

fn event_timestamp(line: &str) -> Option<DateTime<Utc>> {
    if let Some(rest) = line.strip_prefix('[') {
        if let Some(end) = rest.find(']') {
            if let Ok(t) = timestamp::parse(&rest[..end]) {
                return Some(t);
            }
        }
    }
    let mut it = line.split_whitespace();
    let a = it.next()?;
    if let Ok(t) = timestamp::parse(a) {
        return Some(t);
    }
    let b = it.next()?;
    timestamp::parse(&format!("{a} {b}")).ok()
}

fn event_node(line: &str) -> Option<String> {
    for tok in line.split_whitespace() {
        let t = tok.trim_matches(',');
        for pfx in ["node=", "host=", "hostname=", "source="] {
            if let Some(v) = t.strip_prefix(pfx) {
                if !v.is_empty() {
                    return Some(normalize(v));
                }
            }
        }
    }
    // `[ts] host ...`
    let after_bracket = line.split(']').nth(1)?.split_whitespace().next()?;
    if after_bracket.chars().any(|c| c.is_ascii_alphabetic()) && !after_bracket.contains('=') {
        return Some(normalize(after_bracket));
    }
    None
}

fn event_port(line: &str) -> Option<String> {
    line.split_whitespace()
        .find(|t| t.starts_with("mlx") && t.contains(':'))
        .map(|t| t.trim_matches(',').to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at() -> DateTime<Utc> {
        DateTime::from_timestamp(1_756_202_321, 0).unwrap()
    }

    #[test]
    fn only_counters_that_rose_produce_signals() {
        // The rule that keeps this from flagging every port on the cluster: a
        // healthy fabric has large lifetime counters, so the *level* says
        // nothing and only the delta does.
        let before = parse_samples(
            "node-47 mlx5_0:1 symbol_error 1200\nnode-47 mlx5_0:1 link_downed 4\n",
        );
        let after = parse_samples(
            "node-47 mlx5_0:1 symbol_error 1200\nnode-47 mlx5_0:1 link_downed 5\n",
        );
        let sigs = diff(&before, &after, at());
        assert_eq!(sigs.len(), 1, "the unchanged 1200 symbol errors say nothing");
        assert_eq!(sigs[0].class, FaultClass::FabricIb);
        assert_eq!(sigs[0].confidence, Confidence::High, "a link went down");
        assert!(sigs[0].detail.contains("link_downed +1"), "{}", sigs[0].detail);
        assert!(!sigs[0].dated, "a delta has a window, not an instant");
    }

    #[test]
    fn a_counter_with_no_baseline_is_ignored_not_treated_as_a_rise_from_zero() {
        // The usual cause is a before-pass that missed the port. Inventing a
        // link flap from a missing baseline is the false positive that gets a
        // diagnostic tool uninstalled.
        let before = parse_samples("node-47 mlx5_0:1 symbol_error 10\n");
        let after = parse_samples(
            "node-47 mlx5_0:1 symbol_error 10\nnode-48 mlx5_0:1 link_downed 3\n",
        );
        assert!(diff(&before, &after, at()).is_empty());
    }

    #[test]
    fn congestion_counters_are_not_treated_as_faults() {
        // port_xmit_wait rises on every healthy busy fabric. Including it would
        // make every large job look broken.
        let before = parse_samples("n1 mlx5_0:1 port_xmit_wait 100\n");
        let after = parse_samples("n1 mlx5_0:1 port_xmit_wait 999999\n");
        assert!(diff(&before, &after, at()).is_empty());
    }

    #[test]
    fn a_large_symbol_error_burst_outranks_a_trickle() {
        let before = parse_samples("n1 mlx5_0:1 symbol_error 0\nn2 mlx5_0:1 symbol_error 0\n");
        let after = parse_samples("n1 mlx5_0:1 symbol_error 3\nn2 mlx5_0:1 symbol_error 5000\n");
        let sigs = diff(&before, &after, at());
        let n1 = sigs.iter().find(|s| s.node == "n1").unwrap();
        let n2 = sigs.iter().find(|s| s.node == "n2").unwrap();
        assert_eq!(n1.confidence, Confidence::Low);
        assert_eq!(n2.confidence, Confidence::Medium);
    }

    #[test]
    fn samples_parse_in_all_three_tolerated_shapes() {
        let a = parse_samples("node-47 mlx5_0:1 symbol_error 12\n");
        let b = parse_samples("node-47,mlx5_0:1,symbol_error,12\n");
        let c = parse_samples("node-47 mlx5_0:1 symbol_error=12\n");
        assert_eq!(a, b);
        assert_eq!(a, c);
        assert_eq!(a[0].value, 12);
        assert_eq!(a[0].node, "node-47");
    }

    #[test]
    fn ufm_event_lines_are_believed_as_written() {
        let text = "[2026-08-26T09:57:05Z] node-47 mlx5_0:1 Port state change: DOWN (was ACTIVE)\n";
        let sigs = parse_events(text);
        assert_eq!(sigs.len(), 1);
        assert_eq!(sigs[0].node, "node-47");
        assert_eq!(sigs[0].device.as_deref(), Some("mlx5_0:1"));
        assert!(sigs[0].dated, "the event carried its own clock");
    }

    #[test]
    fn non_fabric_lines_in_a_fabric_file_are_ignored() {
        // A syslog dump contains everything; only the fabric lines belong here,
        // or one file would be counted twice by two collectors.
        let sigs = parse_events("[2026-08-26T09:57:05Z] node-47 CUDA out of memory\n");
        assert!(sigs.is_empty());
    }
}
