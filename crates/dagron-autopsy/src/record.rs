//! The emitted record — what "job failed" becomes.
//!
//! The output contract, and the point of the whole tool:
//!
//! > Job 88123 failed: GPU 3 on node-47 threw XID 79 at T-90s; node drained;
//! > retry placed elsewhere.
//!
//! Two renderings of the same struct. JSON is the machine contract — stable
//! field names, consumed by a fleet database, a provider's support API, or
//! dagron's own `task_runs.fault_class`. The text rendering is what an operator
//! reads at 3am, and it leads with the answer rather than the evidence.
//!
//! Every field that could be a guess is optional, and the ones that are absent
//! are absent rather than zero. A confidently wrong autopsy is worse than no
//! autopsy: it gets a healthy node drained and a real fault ignored.

use crate::signal::Signal;
use chrono::{DateTime, Utc};
use dagron_core::fault::{Confidence, Disposition, FaultClass};
use serde::{Deserialize, Serialize};

/// One quoted signal in the evidence chain.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Evidence {
    pub at: DateTime<Utc>,
    /// False when `at` was inferred from the job's window rather than read off
    /// the line — see [`Signal::dated`]. Rendered as `~` in the text output so
    /// an inferred time is never mistaken for a measured one.
    pub dated: bool,
    pub node: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rank: Option<u32>,
    pub source: String,
    pub class: FaultClass,
    pub confidence: Confidence,
    pub detail: String,
}

impl From<&Signal> for Evidence {
    fn from(s: &Signal) -> Self {
        Evidence {
            at: s.at,
            dated: s.dated,
            node: s.node.clone(),
            device: s.device.clone(),
            rank: s.rank,
            source: s.source.as_str().to_string(),
            class: s.class,
            confidence: s.confidence,
            detail: s.detail.clone(),
        }
    }
}

/// The single signal the verdict rests on.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FirstFault {
    pub at: DateTime<Utc>,
    pub dated: bool,
    pub node: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device: Option<String>,
    pub source: String,
    pub detail: String,
}

impl From<&Signal> for FirstFault {
    fn from(s: &Signal) -> Self {
        FirstFault {
            at: s.at,
            dated: s.dated,
            node: s.node.clone(),
            device: s.device.clone(),
            source: s.source.as_str().to_string(),
            detail: s.detail.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SilentRank {
    pub rank: u32,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub node: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopologySummary {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub world_size: Option<u32>,
    pub ranks_seen: u32,
    pub ranks_timed_out: Vec<u32>,
    /// The ranks that printed nothing — the ones everyone else was waiting on.
    pub ranks_silent: Vec<SilentRank>,
}

/// What to do about it. The whole reason for classifying at all.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Recommendation {
    /// Whether another attempt is worth anything.
    pub retry: bool,
    /// The node to take out of the pool first. `Some` only for faults that
    /// recur on the same hardware — the retry that lands back on the broken
    /// node is the classic wasted second attempt.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub drain_node: Option<String>,
    /// The `retry_budgets:` entry this fault implies, ready to paste into a
    /// workflow. Concrete, because "consider tuning your retry policy" has
    /// never changed anyone's YAML.
    pub retry_budget_hint: String,
    pub summary: String,
}

impl Recommendation {
    pub fn for_class(class: FaultClass, confidence: Confidence, cause: Option<&Signal>) -> Self {
        let disposition = class.disposition();
        // A low-confidence verdict does not get to act. It reports.
        let acting = confidence >= Confidence::Medium;
        let retry = acting && matches!(disposition, Disposition::Infrastructure | Disposition::Platform);
        let drain_node = (acting && class.should_drain_node())
            .then(|| cause.map(|s| s.node.clone()))
            .flatten()
            .filter(|n| !n.is_empty());

        let budget = class.default_budget();
        let retry_budget_hint = if budget > 0 {
            format!("retry_budgets: {{ {}: {} }}", class.as_str(), budget)
        } else {
            format!(
                "no budget implied for {} — the class declines to have an opinion, so the task's \
                 own max_attempts applies",
                class.as_str()
            )
        };

        let summary = match (disposition, acting) {
            (_, false) => format!(
                "confidence is {confidence}: report this, do not act on it — gather the missing \
                 source (DCGM, fabric counters, or the job's full stdout) and re-run the autopsy"
            ),
            (Disposition::Infrastructure, _) => match &drain_node {
                Some(n) => format!(
                    "drain {n} and retry elsewhere — the job did nothing wrong and the next \
                     attempt must not land back on this hardware"
                ),
                None => "retry elsewhere — infrastructure fault, the job did nothing wrong".into(),
            },
            (Disposition::Application, _) => format!(
                "do not retry — {} reproduces on the next attempt at full cluster cost; fix the \
                 job, then resubmit",
                class.as_str()
            ),
            (Disposition::Platform, _) => {
                "retry per policy — nothing is broken; this is the scheduler or the spot market \
                 taking the allocation back"
                    .into()
            }
            (Disposition::Unknown, _) => {
                "unattributed — do not drain anything, and do not assume it was the network"
                    .into()
            }
        };

        Recommendation { retry, drain_node, retry_budget_hint, summary }
    }
}

/// The fault-attributed job record.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobAutopsy {
    pub job_id: String,
    pub job_name: String,
    /// Slurm's own state word, kept verbatim beside our verdict so the two can
    /// be compared rather than one silently replacing the other.
    pub state: String,
    pub nodes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub started_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ended_at: Option<DateTime<Utc>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub elapsed_secs: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gpus: Option<u32>,
    /// GPU-hours the job spent before dying. The unit the business case is
    /// denominated in — and `None`, never `0`, when the allocation did not name
    /// GPUs.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub gpu_hours_lost: Option<f64>,

    pub class: FaultClass,
    pub disposition: Disposition,
    pub confidence: Confidence,
    /// Why this verdict and not another — in words, on the record.
    pub rationale: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub first_fault: Option<FirstFault>,
    pub affected_nodes: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub rank_topology: Option<TopologySummary>,
    pub recommendation: Recommendation,
    pub evidence: Vec<Evidence>,
    /// What the autopsy could not do. A verdict reached on partial evidence
    /// says so here rather than looking like a complete one.
    pub warnings: Vec<String>,
}

impl JobAutopsy {
    pub fn to_json(&self) -> String {
        serde_json::to_string_pretty(self).unwrap_or_else(|e| format!("{{\"error\":\"{e}\"}}"))
    }

    /// The one-line form: the sentence an operator needs and nothing else.
    pub fn headline(&self) -> String {
        let where_ = match &self.first_fault {
            Some(f) => {
                let device = f.device.as_deref().map(|d| format!("{d} on ")).unwrap_or_default();
                let when = self
                    .ended_at
                    .filter(|_| f.dated)
                    .map(|end| {
                        let d = (end - f.at).num_seconds();
                        if d > 0 {
                            format!(" at T-{d}s")
                        } else {
                            format!(" at T+{}s", -d)
                        }
                    })
                    .unwrap_or_default();
                format!("{device}{}{when}", f.node)
            }
            None => {
                if self.affected_nodes.is_empty() {
                    "no located device".to_string()
                } else {
                    self.affected_nodes.join(", ")
                }
            }
        };
        format!(
            "job {} ({}) failed: {} [{}, {} confidence] — {}",
            self.job_id, self.job_name, where_, self.class, self.confidence, self.recommendation.summary
        )
    }

    /// The full operator rendering. Answer first, evidence second — the inverse
    /// of how the underlying logs present it, which is the reason reading them
    /// takes an hour.
    pub fn to_text(&self) -> String {
        use std::fmt::Write;
        let mut s = String::new();
        let _ = writeln!(s, "job {}  {}", self.job_id, self.job_name);
        let _ = writeln!(
            s,
            "  slurm state   {}   nodes {}{}",
            self.state,
            self.nodes.len(),
            self.gpus.map(|g| format!("   gpus {g}")).unwrap_or_default()
        );
        if let Some(h) = self.gpu_hours_lost {
            let _ = writeln!(s, "  gpu-hours     {h:.1} spent before the failure");
        }
        let _ = writeln!(s);
        let _ = writeln!(
            s,
            "  VERDICT       {}  ({}, {} confidence)",
            self.class, self.disposition, self.confidence
        );
        let _ = writeln!(s, "  why           {}", wrap(&self.rationale, 16));
        if let Some(f) = &self.first_fault {
            let approx = if f.dated { "" } else { "~" };
            let _ = writeln!(
                s,
                "  first fault   {approx}{}  {}{}  via {}",
                f.at.to_rfc3339(),
                f.node,
                f.device.as_deref().map(|d| format!(" {d}")).unwrap_or_default(),
                f.source
            );
        }
        if !self.affected_nodes.is_empty() {
            let _ = writeln!(
                s,
                "  blast radius  {} of {} job nodes: {}",
                self.affected_nodes.len(),
                self.nodes.len(),
                preview(&self.affected_nodes)
            );
        }
        if let Some(t) = &self.rank_topology {
            let _ = writeln!(
                s,
                "  ranks         {} timed out{}{}",
                t.ranks_timed_out.len(),
                t.world_size.map(|w| format!(" of {w}")).unwrap_or_default(),
                if t.ranks_silent.is_empty() {
                    String::new()
                } else {
                    format!(
                        "; silent (waited on): {}",
                        t.ranks_silent
                            .iter()
                            .take(6)
                            .map(|r| match &r.node {
                                Some(n) => format!("{}@{}", r.rank, n),
                                None => r.rank.to_string(),
                            })
                            .collect::<Vec<_>>()
                            .join(", ")
                    )
                }
            );
        }
        let _ = writeln!(s);
        let _ = writeln!(s, "  ACTION        {}", wrap(&self.recommendation.summary, 16));
        let _ = writeln!(s, "  retry         {}", if self.recommendation.retry { "yes" } else { "no" });
        if let Some(n) = &self.recommendation.drain_node {
            let _ = writeln!(s, "  drain         {n}");
        }
        let _ = writeln!(s, "  budget        {}", self.recommendation.retry_budget_hint);

        if !self.evidence.is_empty() {
            let _ = writeln!(s);
            let _ = writeln!(s, "  EVIDENCE      (most believable first)");
            for e in &self.evidence {
                let approx = if e.dated { " " } else { "~" };
                let _ = writeln!(
                    s,
                    "   {approx}{}  {:<7} {:<10}{} {}",
                    e.at.format("%H:%M:%S"),
                    e.source,
                    e.class.as_str(),
                    e.device.as_deref().map(|d| format!(" {d}")).unwrap_or_default(),
                    trunc(&e.detail, 96)
                );
            }
        }
        if !self.warnings.is_empty() {
            let _ = writeln!(s);
            let _ = writeln!(s, "  CAVEATS");
            for w in &self.warnings {
                let _ = writeln!(s, "   - {}", wrap(w, 5));
            }
        }
        s
    }
}

fn trunc(s: &str, n: usize) -> String {
    let s = s.replace('\n', " ");
    if s.chars().count() <= n {
        return s;
    }
    s.chars().take(n).collect::<String>() + "…"
}

/// Wrap continuation lines to an indent so a long rationale stays in the column
/// it started in.
fn wrap(s: &str, indent: usize) -> String {
    const WIDTH: usize = 76;
    let pad = " ".repeat(indent);
    let mut out = String::new();
    let mut col = 0usize;
    for word in s.split_whitespace() {
        if col > 0 && col + 1 + word.chars().count() > WIDTH {
            out.push('\n');
            out.push_str(&pad);
            col = 0;
        } else if col > 0 {
            out.push(' ');
            col += 1;
        }
        out.push_str(word);
        col += word.chars().count();
    }
    out
}

fn preview(nodes: &[String]) -> String {
    if nodes.len() <= 6 {
        return nodes.join(", ");
    }
    format!("{}, … (+{})", nodes[..6].join(", "), nodes.len() - 6)
}

/// Re-exported so consumers of a record do not have to reach into the collector
/// module for the one enum they need to interpret its `source` field.
pub use crate::signal::Source as EvidenceSource;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::signal::Source;

    #[test]
    fn a_low_confidence_verdict_reports_but_does_not_act() {
        // The safety rule: a guess must never drain a node.
        let r = Recommendation::for_class(FaultClass::GpuEcc, Confidence::Low, None);
        assert!(!r.retry);
        assert!(r.drain_node.is_none());
        assert!(r.summary.contains("do not act on it"), "{}", r.summary);
    }

    #[test]
    fn an_application_fault_recommends_against_retrying_in_words_and_in_the_flag() {
        let r = Recommendation::for_class(FaultClass::NanLoss, Confidence::High, None);
        assert!(!r.retry);
        assert!(r.summary.contains("do not retry"), "{}", r.summary);
        assert!(r.retry_budget_hint.contains("nan-loss: 1"), "{}", r.retry_budget_hint);
    }

    #[test]
    fn an_infra_fault_names_the_node_to_drain() {
        let cause = Signal {
            at: DateTime::from_timestamp(1_700_000_000, 0).unwrap(),
            node: "node-47".into(),
            device: Some("gpu3".into()),
            rank: None,
            source: Source::Dcgm,
            class: FaultClass::GpuFallenOffBus,
            confidence: Confidence::High,
            detail: "Xid 79".into(),
            dated: true,
        };
        let r = Recommendation::for_class(FaultClass::GpuFallenOffBus, Confidence::High, Some(&cause));
        assert!(r.retry);
        assert_eq!(r.drain_node.as_deref(), Some("node-47"));
        assert!(r.summary.contains("must not land back on this hardware"), "{}", r.summary);
    }

    #[test]
    fn an_uncorroborated_timeout_declines_to_have_a_budget_opinion() {
        let r = Recommendation::for_class(FaultClass::NcclTimeout, Confidence::Low, None);
        assert!(r.retry_budget_hint.contains("declines"), "{}", r.retry_budget_hint);
    }
}
