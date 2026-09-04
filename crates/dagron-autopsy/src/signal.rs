//! The common event type every collector produces.
//!
//! DCGM speaks in GPU indices, NCCL speaks in ranks, the fabric speaks in
//! `mlx5_0:1` port names, and `sacct` speaks in job steps. Correlating them
//! means first agreeing on the four facts they all have — **when, where, what
//! kind, and the line that says so** — and that agreement is this struct.
//!
//! Everything the correlator does is sorting and filtering [`Signal`]s. Adding
//! a fifth source (a node health checker, a Kubernetes event stream, a
//! provider's XID API) means writing a parser that emits these, and nothing in
//! `correlate.rs` changes.

use chrono::{DateTime, Utc};
use dagron_core::fault::{Confidence, FaultClass, Precedence};
use serde::{Deserialize, Serialize};

/// Where a signal came from. Kept on the record because provenance is the first
/// thing anyone asks of a verdict they did not expect.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Source {
    /// Slurm accounting (`sacct`) — job state, exit code, node set, window.
    Sacct,
    /// NVIDIA DCGM: XID events, ECC counters, row-remap state.
    Dcgm,
    /// The job's own stdout/stderr: NCCL warnings, watchdog timeouts,
    /// framework tracebacks.
    Nccl,
    /// InfiniBand / RoCE: port counters, link state changes, UFM events.
    Ib,
    /// Kubernetes events / pod status, for the K8s-side half of a job.
    Kube,
}

impl Source {
    pub fn as_str(self) -> &'static str {
        match self {
            Source::Sacct => "sacct",
            Source::Dcgm => "dcgm",
            Source::Nccl => "nccl",
            Source::Ib => "ib",
            Source::Kube => "kube",
        }
    }
}

/// One dated, located observation.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Signal {
    pub at: DateTime<Utc>,
    /// Normalized short hostname ([`crate::nodelist::normalize`]). The join key.
    pub node: String,
    /// The device within the node, when the source names one: `gpu3`,
    /// `mlx5_0:1`. `None` for node-scoped events.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device: Option<String>,
    /// The distributed-job rank, when the source names one. Only NCCL and the
    /// framework logs do, and it is what turns "some rank hung" into "rank 27,
    /// which is on node-47".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rank: Option<u32>,
    pub source: Source,
    pub class: FaultClass,
    pub confidence: Confidence,
    /// The raw line. A signal with no quotable evidence is an assertion.
    pub detail: String,
    /// Whether `at` was read off the line, or inferred from the job's window.
    ///
    /// Framework and NCCL logs frequently carry no timestamp at all — the rank
    /// prefix is there, the clock is not. Such a line still holds the single
    /// most useful fact in the file (*which ranks were stuck*), so it is kept,
    /// dated to the job's end, and flagged. The correlator may filter and rank
    /// on an inferred time; it must never narrate one ("12 s before the
    /// timeout") as if it were measured.
    #[serde(default = "yes")]
    pub dated: bool,
}

fn yes() -> bool {
    true
}

impl Signal {
    pub fn precedence(&self) -> Precedence {
        self.class.precedence()
    }

    /// Sort key for "which of these is most believable as the cause": strongest
    /// precedence first, then earliest.
    ///
    /// **Precedence before time, deliberately.** The earliest event in a
    /// distributed failure is almost always the *symptom* — every healthy rank
    /// notices the hang before the broken device finishes reporting itself, and
    /// the driver's XID lands in syslog seconds after the collective already
    /// timed out. Sorting by time alone reproduces the industry's standard
    /// wrong answer, which is why this key exists as one function instead of an
    /// inline `sort_by_key` someone will later "simplify".
    pub fn cause_rank(&self) -> (std::cmp::Reverse<Precedence>, DateTime<Utc>) {
        (std::cmp::Reverse(self.precedence()), self.at)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sig(class: FaultClass, secs: i64) -> Signal {
        Signal {
            at: DateTime::from_timestamp(1_700_000_000 + secs, 0).unwrap(),
            node: "node-47".into(),
            device: None,
            rank: None,
            source: Source::Dcgm,
            class,
            confidence: Confidence::Medium,
            detail: String::new(),
            dated: true,
        }
    }

    #[test]
    fn a_later_root_cause_outranks_an_earlier_symptom() {
        // The whole ordering policy in one assertion: the watchdog timeout at
        // T+0 is what everyone saw first, and the ECC error at T+12 is what
        // actually happened.
        let mut v = [sig(FaultClass::NcclTimeout, 0), sig(FaultClass::GpuEcc, 12)];
        v.sort_by_key(|s| s.cause_rank());
        assert_eq!(v[0].class, FaultClass::GpuEcc);
    }

    #[test]
    fn among_equals_the_earliest_wins() {
        let mut v = [sig(FaultClass::GpuEcc, 30), sig(FaultClass::FabricIb, 10)];
        v.sort_by_key(|s| s.cause_rank());
        assert_eq!(v[0].class, FaultClass::FabricIb, "same precedence → earliest");
    }
}
