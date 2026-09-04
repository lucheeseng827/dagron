//! NCCL and framework logs — the ranks' own account of the hang.
//!
//! This is the source everyone already reads and nearly everyone reads wrong.
//! A collective timeout is printed by every rank that was *waiting*, which is
//! to say by every **healthy** rank; the rank that died often prints nothing at
//! all, because it died. Attributing the failure to the loudest line in the log
//! is how "NCCL timeout" becomes a network ticket for what was a stuck
//! dataloader.
//!
//! So this module deliberately does two different jobs:
//!
//! 1. It emits [`Signal`]s, all of them marked as symptoms
//!    ([`FaultClass::NcclTimeout`]) unless the library named a transport fault
//!    itself — those signals can only ever *corroborate* a cause found
//!    elsewhere.
//! 2. It extracts the **rank topology of the hang** — who reported the timeout,
//!    who did not, and which node each rank was on. That is the discriminator
//!    the logs actually contain: everyone stuck is a deadlock; everyone-but-one
//!    stuck names the one.
//!
//! Timestamps are frequently absent from these logs. An undated line still
//! carries the rank facts, so it is kept, dated to the job's end, and flagged
//! [`Signal::dated`] `= false` — see there.

use crate::nodelist::normalize;
use crate::signal::{Signal, Source};
use crate::timestamp;
use chrono::{DateTime, Utc};
use dagron_core::fault::{classify_text, Confidence, FaultClass};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

/// What the job's logs revealed, beyond the individual signals.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RankTopology {
    /// Ranks that reported a collective timeout — the ones that were *waiting*.
    pub timed_out: BTreeSet<u32>,
    /// Ranks seen at all, from any line.
    pub seen: BTreeSet<u32>,
    /// rank → node, when a line carried both. Turns "rank 27 hung" into a
    /// hostname the fabric and device signals can be joined against.
    pub rank_node: BTreeMap<u32, String>,
    /// World size, if a line stated it (`nranks=128`).
    pub world_size: Option<u32>,
}

impl RankTopology {
    /// The ranks that everyone else was waiting on: seen (or implied by world
    /// size) but not among those reporting a timeout.
    ///
    /// This is the payoff of parsing rank prefixes at all. On a 128-rank job
    /// where 127 ranks print the watchdog timeout, the 128th is the one that
    /// died — and it is the one that printed nothing, so no amount of reading
    /// the *loud* lines finds it.
    pub fn silent_ranks(&self) -> BTreeSet<u32> {
        let universe: BTreeSet<u32> = match self.world_size {
            Some(n) if n > 0 => (0..n).collect(),
            _ => self.seen.clone(),
        };
        universe.difference(&self.timed_out).copied().collect()
    }
}

#[derive(Debug, Clone, Default)]
pub struct Report {
    pub signals: Vec<Signal>,
    pub topology: RankTopology,
}

/// Parse a job's stdout/stderr.
///
/// `fallback_at` dates lines that carry no timestamp of their own — pass the
/// job's end time. `default_node` is used when a line names a rank but no host
/// (PyTorch's `[rank27]:` prefix does exactly this); pass the job's first node
/// or `None`.
pub fn parse(text: &str, fallback_at: DateTime<Utc>, default_node: Option<&str>) -> Report {
    let mut report = Report::default();
    for line in text.lines() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let rank = rank_of(line);
        let node = node_of(line)
            .or_else(|| default_node.map(normalize))
            .unwrap_or_default();
        if let Some(r) = rank {
            report.topology.seen.insert(r);
            if !node.is_empty() {
                report.topology.rank_node.insert(r, node.clone());
            }
        }
        if let Some(n) = world_size_of(line) {
            report.topology.world_size = Some(n);
        }

        let Some(c) = classify_text(line) else { continue };
        if c.class == FaultClass::NcclTimeout {
            if let Some(r) = rank {
                report.topology.timed_out.insert(r);
            }
        }
        let (at, dated) = match line_timestamp(line) {
            Some(t) => (t, true),
            None => (fallback_at, false),
        };
        report.signals.push(Signal {
            at,
            node,
            device: local_device_of(line),
            rank,
            source: Source::Nccl,
            class: c.class,
            confidence: c.confidence,
            detail: c.evidence,
            dated,
        });
    }
    report
}

/// `[rank27]:`, `[Rank 27]`, `Rank 27:`.
fn rank_of(line: &str) -> Option<u32> {
    let lower = line.to_ascii_lowercase();
    let at = lower.find("rank")?;
    let rest = &lower[at + 4..];
    let mut digits = String::new();
    for ch in rest.chars() {
        if ch.is_ascii_digit() {
            digits.push(ch);
        } else if !digits.is_empty() {
            break;
        } else if ch == ' ' || ch == ':' || ch == '=' || ch == '_' || ch == '-' {
            continue;
        } else {
            // `ranks=`, `rankless`, … — not a rank number.
            return None;
        }
    }
    digits.parse().ok()
}

/// NCCL prefixes every line with `host:pid:tid [dev] …`.
fn node_of(line: &str) -> Option<String> {
    let first = line.split_whitespace().next()?;
    // Two colons and a hostname-looking head.
    let parts: Vec<&str> = first.split(':').collect();
    if parts.len() >= 3 && parts[1].chars().all(|c| c.is_ascii_digit()) && !parts[0].is_empty() {
        let h = parts[0];
        if h.chars().any(|c| c.is_ascii_alphabetic()) {
            return Some(normalize(h));
        }
    }
    // `host=node-47` anywhere on the line.
    for tok in line.split_whitespace() {
        if let Some(v) = tok.trim_matches(',').strip_prefix("host=") {
            if !v.is_empty() {
                return Some(normalize(v));
            }
        }
    }
    None
}

/// The `[3]` in `node-47:1234:1300 [3] NCCL WARN …` — the *local* device index.
fn local_device_of(line: &str) -> Option<String> {
    let open = line.find(" [")?;
    let rest = &line[open + 2..];
    let close = rest.find(']')?;
    let inner = &rest[..close];
    if inner.is_empty() || !inner.chars().all(|c| c.is_ascii_digit()) {
        return None;
    }
    Some(format!("gpu{inner}"))
}

/// `nranks=128`, `nRanks 128`, `world_size=128`.
fn world_size_of(line: &str) -> Option<u32> {
    let lower = line.to_ascii_lowercase();
    for key in ["nranks", "world_size", "worldsize"] {
        if let Some(at) = lower.find(key) {
            let rest = &lower[at + key.len()..];
            let mut digits = String::new();
            for ch in rest.chars() {
                if ch.is_ascii_digit() {
                    digits.push(ch);
                } else if !digits.is_empty() {
                    break;
                } else if ch == ' ' || ch == '=' || ch == ':' {
                    continue;
                } else {
                    break;
                }
            }
            if let Ok(n) = digits.parse::<u32>() {
                if n > 0 {
                    return Some(n);
                }
            }
        }
    }
    None
}

/// A timestamp in a leading `[...]` or as the first token(s).
fn line_timestamp(line: &str) -> Option<DateTime<Utc>> {
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

/// Decide what the rank topology says, on its own, when no device or fabric
/// evidence corroborates anything.
///
/// Returns `None` when the logs do not support any of the three readings —
/// which is the common case and must stay reportable as "we could not tell",
/// not rounded to the nearest plausible story.
pub fn topology_verdict(topo: &RankTopology) -> Option<(FaultClass, Confidence, String)> {
    if topo.timed_out.is_empty() {
        return None;
    }
    let silent = topo.silent_ranks();
    let universe = topo.world_size.map(|n| n as usize).unwrap_or(topo.seen.len());
    if universe == 0 {
        return None;
    }

    // Everyone is waiting and nobody is missing: no rank is late, so the
    // collective itself is inconsistent — a mismatched op order, a rank that
    // never entered, or a shape mismatch. Application, not hardware.
    //
    // **Only when the log stated the world size.** `timed_out` is always a
    // subset of `seen`, so without `nranks=` the inferred universe is whatever
    // ranks happened to be in the file and "nobody is missing" is a fact about
    // the *capture*, not about the job. A truncated stdout holding three ranks
    // of a 128-rank job would otherwise satisfy both conditions — and this
    // verdict is Application disposition at Medium confidence, which sets
    // `retry = false`. An infrastructure fault with a partial log would be
    // filed as the job's own deadlock and never retried.
    if silent.is_empty() && topo.world_size.is_some() && topo.timed_out.len() == universe {
        return Some((
            FaultClass::Deadlock,
            Confidence::Medium,
            format!(
                "all {} ranks reported a collective timeout and none is missing — \
                 no rank was late, so the collective is inconsistent (mismatched op order, \
                 shape, or a rank that never entered)",
                universe
            ),
        ));
    }

    // Almost everyone is waiting on a handful. Those few are the job's problem,
    // and naming them is the difference between a network ticket and a fix.
    if !silent.is_empty() && topo.timed_out.len() >= universe.saturating_sub(silent.len()) {
        let ratio = topo.timed_out.len() as f64 / universe as f64;
        // A couple of ranks silent out of many is a straggler. Half the job
        // silent is not a straggler, it is a partition — and calling that a
        // straggler would point the operator at the wrong half.
        if ratio >= 0.6 && silent.len() * 4 <= universe {
            let names: Vec<String> = silent
                .iter()
                .take(8)
                .map(|r| match topo.rank_node.get(r) {
                    Some(n) => format!("rank {r} ({n})"),
                    None => format!("rank {r}"),
                })
                .collect();
            return Some((
                FaultClass::StragglerRank,
                Confidence::Medium,
                format!(
                    "{} of {} ranks reported a collective timeout; silent: {}{} — \
                     the ranks that printed nothing are the ones the others waited on",
                    topo.timed_out.len(),
                    universe,
                    names.join(", "),
                    if silent.len() > 8 { format!(" (+{} more)", silent.len() - 8) } else { String::new() }
                ),
            ));
        }
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;

    fn t0() -> DateTime<Utc> {
        DateTime::from_timestamp(1_756_202_321, 0).unwrap()
    }

    #[test]
    fn extracts_rank_node_and_device_from_an_nccl_prefix() {
        let text = "node-47:1234:1300 [3] NCCL WARN Call to ncclSystemError failed\n";
        let r = parse(text, t0(), None);
        assert_eq!(r.signals.len(), 1);
        assert_eq!(r.signals[0].node, "node-47");
        assert_eq!(r.signals[0].device.as_deref(), Some("gpu3"));
        assert_eq!(r.signals[0].class, FaultClass::NcclCommAbort);
        assert!(!r.signals[0].dated, "the line carried no clock");
        assert_eq!(r.signals[0].at, t0(), "dated to the job's end instead");
    }

    #[test]
    fn a_torch_rank_prefix_supplies_the_rank_and_inherits_the_job_node() {
        let text = "[rank27]: Watchdog caught collective operation timeout: WorkNCCL(SeqNum=1234, Timeout(ms)=600000)\n";
        let r = parse(text, t0(), Some("node-40.hpc.internal"));
        assert_eq!(r.signals[0].rank, Some(27));
        assert_eq!(r.signals[0].node, "node-40");
        assert!(r.topology.timed_out.contains(&27));
    }

    #[test]
    fn everyone_stuck_and_nobody_missing_reads_as_a_deadlock() {
        let mut text = String::from("node-40:1:1 [0] NCCL INFO nranks=4\n");
        for rank in 0..4 {
            text.push_str(&format!(
                "[rank{rank}]: Watchdog caught collective operation timeout\n"
            ));
        }
        let r = parse(&text, t0(), Some("node-40"));
        assert_eq!(r.topology.world_size, Some(4));
        assert!(r.topology.silent_ranks().is_empty());
        let (class, _, why) = topology_verdict(&r.topology).unwrap();
        assert_eq!(class, FaultClass::Deadlock);
        assert_eq!(class.disposition(), dagron_core::fault::Disposition::Application);
        assert!(why.contains("inconsistent"), "{why}");
    }

    #[test]
    fn everyone_but_one_stuck_names_the_one_that_printed_nothing() {
        // The payoff. Rank 3 died and said nothing; ranks 0-2 are loud. Reading
        // the loud lines gives "NCCL timeout"; reading the silence gives rank 3.
        let mut text = String::from("node-40:1:1 [0] NCCL INFO nranks=4\n");
        for rank in 0..3 {
            text.push_str(&format!(
                "node-4{rank}:1:1 [0] [rank{rank}]: Watchdog caught collective operation timeout\n"
            ));
        }
        let r = parse(&text, t0(), None);
        assert_eq!(r.topology.silent_ranks(), [3].into_iter().collect());
        let (class, _, why) = topology_verdict(&r.topology).unwrap();
        assert_eq!(class, FaultClass::StragglerRank);
        assert!(why.contains("rank 3"), "{why}");
    }

    #[test]
    fn a_partition_is_not_called_a_straggler() {
        // Half the ranks silent is a split, not a slow rank; calling it a
        // straggler points the operator at the wrong half of the job.
        let mut text = String::from("node-40:1:1 [0] NCCL INFO nranks=8\n");
        for rank in 0..4 {
            text.push_str(&format!("[rank{rank}]: Watchdog caught collective operation timeout\n"));
        }
        let r = parse(&text, t0(), None);
        assert_eq!(r.topology.silent_ranks().len(), 4);
        assert!(topology_verdict(&r.topology).is_none(), "declines to guess");
    }

    #[test]
    fn a_truncated_log_is_not_read_as_a_deadlock() {
        // Three ranks' worth of stdout from a large job, with no `nranks=` line
        // in the captured portion. Every rank we can see timed out, so a
        // naive "nobody is missing" test fires — and Deadlock is Application
        // disposition, so the record would say "do not retry" about what may
        // well be a dead GPU.
        let mut text = String::new();
        for rank in 0..3 {
            text.push_str(&format!("[rank{rank}]: Watchdog caught collective operation timeout\n"));
        }
        let r = parse(&text, t0(), None);
        assert_eq!(r.topology.world_size, None, "the capture never stated it");
        assert!(r.topology.silent_ranks().is_empty(), "nothing looks missing…");
        assert!(
            topology_verdict(&r.topology).is_none(),
            "…so the verdict must decline rather than blame the job"
        );

        // The same three ranks, with the world size stated as 3, is a real
        // deadlock and still reads as one.
        let stated = format!("node-40:1:1 [0] NCCL INFO nranks=3\n{text}");
        let r = parse(&stated, t0(), None);
        let (class, _, _) = topology_verdict(&r.topology).unwrap();
        assert_eq!(class, FaultClass::Deadlock);
    }

    #[test]
    fn logs_with_no_timeout_at_all_produce_no_verdict() {
        let r = parse("node-40:1:1 [0] NCCL INFO Channel 00 : 0[0] -> 1[1]\n", t0(), None);
        assert!(topology_verdict(&r.topology).is_none());
    }

    #[test]
    fn a_dated_line_keeps_its_own_clock() {
        let text = "[2026-08-26T09:58:41Z] node-47:1:1 [3] NCCL WARN ncclSystemError\n";
        let r = parse(text, t0(), None);
        assert!(r.signals[0].dated);
        assert_ne!(r.signals[0].at, t0());
    }

    #[test]
    fn nranks_is_not_mistaken_for_a_rank_number() {
        // `nranks=128` contains "rank"; reading 128 as a rank id would invent a
        // rank that does not exist and skew every ratio downstream.
        assert_eq!(rank_of("NCCL INFO nranks=128"), None);
        assert_eq!(rank_of("[rank27]:"), Some(27));
        assert_eq!(rank_of("[Rank 27] Watchdog"), Some(27));
    }
}
