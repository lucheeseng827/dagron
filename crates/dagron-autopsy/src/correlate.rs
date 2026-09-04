//! The join — the part nobody ships.
//!
//! Slurm records job state. DCGM records GPU health. NCCL logs sit in stdout. IB
//! counters sit in UFM. All four exist on every production cluster and none of
//! them is joined to the others, because the scheduler is not an observability
//! system and the observability vendors do not own job state. That gap is this
//! file.
//!
//! The join is: **signals on this job's nodes, inside this job's window,
//! ranked by whether they can be believed as a cause.** Three rules do the
//! work, and each exists because the obvious alternative produces the wrong
//! answer:
//!
//! 1. **Filter by the job's node set.** A GPU that died on a node this job
//!    never held is not this job's failure, however dramatic the line. Without
//!    this the tool blames one broken node for every job on the cluster.
//!
//! 2. **Rank by precedence before time.** The earliest signal is almost always
//!    the symptom: every healthy rank notices a hang before the broken device
//!    finishes reporting itself. Sorting by time reproduces the industry's
//!    standard wrong answer — "NCCL timeout, must be the network".
//!
//! 3. **Never promote a symptom.** An uncorroborated collective timeout stays
//!    an uncorroborated collective timeout. It may be demoted to a *supporting*
//!    signal by a real cause, and the rank topology may explain it, but no
//!    amount of it adds up to a device fault.

use crate::nccl::{self, RankTopology};
use crate::record::{Evidence, FirstFault, JobAutopsy, Recommendation};
use crate::sacct::JobRecord;
use crate::signal::Signal;
use chrono::{DateTime, TimeDelta, Utc};
use dagron_core::fault::{Confidence, FaultClass, Precedence};
use std::collections::BTreeSet;

/// Knobs on the join.
#[derive(Debug, Clone)]
pub struct Window {
    /// How far **before** the job's end a signal may sit and still be believed
    /// as its cause. Defaults to the whole job (`None`), which is right for a
    /// job whose start is known; a lookback narrows it when the job ran for
    /// days and only the last minutes matter.
    pub lookback: Option<TimeDelta>,
    /// How far **after** the job's end a signal still counts. Non-zero because
    /// a device that kills a job reports itself *afterwards* as often as
    /// before — the driver's XID lands in syslog seconds after the process is
    /// already gone, and a window that closes at the exit misses exactly the
    /// evidence it was opened for.
    pub grace: TimeDelta,
    /// Slack added to both ends for clock disagreement between sources. The
    /// four feeds are stamped by four different clocks; a systematic offset
    /// silently empties the intersection rather than degrading gracefully.
    pub skew: TimeDelta,
}

impl Default for Window {
    fn default() -> Self {
        Window {
            lookback: None,
            grace: TimeDelta::seconds(120),
            skew: TimeDelta::seconds(30),
        }
    }
}

impl Window {
    /// The concrete `[from, to]` for a job.
    pub fn bounds(&self, job: &JobRecord) -> Option<(DateTime<Utc>, DateTime<Utc>)> {
        let end = job.end.or(job.start)?;
        let start = match (self.lookback, job.start) {
            (Some(lb), _) => end - lb,
            (None, Some(s)) => s,
            // No start recorded: fall back to the lookback default rather than
            // an unbounded window, which would sweep in every event ever
            // recorded on those nodes.
            (None, None) => end - TimeDelta::seconds(900),
        };
        Some((start - self.skew, end + self.grace + self.skew))
    }
}

/// Everything the collectors produced, ready to be joined.
#[derive(Debug, Clone, Default)]
pub struct Inputs {
    pub signals: Vec<Signal>,
    pub topology: RankTopology,
    /// Non-fatal problems worth telling the operator about — a skipped sacct
    /// row, a missing input file. Carried into the record rather than logged
    /// and forgotten: a verdict reached on partial evidence must say so.
    pub warnings: Vec<String>,
}

/// Join the signals against the job and produce the record.
pub fn correlate(job: &JobRecord, inputs: Inputs, window: &Window) -> JobAutopsy {
    let mut warnings = inputs.warnings;
    let bounds = window.bounds(job);

    // ── Rule 1: this job's nodes, this job's window ──────────────────────────
    let mut relevant: Vec<Signal> = Vec::new();
    let mut off_node = 0usize;
    let mut out_of_window = 0usize;
    for s in inputs.signals {
        // An empty node set means sacct never recorded an allocation; there is
        // nothing to filter against, so nothing is filtered — and the record
        // says the verdict rests on an unfiltered set.
        if !job.nodes.is_empty() && !s.node.is_empty() && !job.nodes.contains(&s.node) {
            off_node += 1;
            continue;
        }
        if let Some((from, to)) = bounds {
            if s.at < from || s.at > to {
                out_of_window += 1;
                continue;
            }
        }
        relevant.push(s);
    }
    if job.nodes.is_empty() {
        warnings.push(
            "sacct recorded no node list for this job — signals could not be filtered by node, \
             so evidence from unrelated nodes may appear below"
                .to_string(),
        );
    }

    // ── Rule 2: precedence before time ───────────────────────────────────────
    relevant.sort_by_key(|s| s.cause_rank());

    let root = relevant
        .iter()
        .find(|s| s.precedence() == Precedence::RootCause)
        .cloned();

    // ── Rule 3: a symptom is never promoted ──────────────────────────────────
    let (class, confidence, rationale, first) = match &root {
        Some(cause) => {
            // Corroboration: did a symptom follow this cause, on the job, in
            // the window? Cause-then-effect in the right order is the
            // difference between "a device logged something" and "a device
            // logged something and the job stopped".
            let corroborated = relevant.iter().any(|s| {
                s.precedence() != Precedence::RootCause
                    && (!s.dated || !cause.dated || s.at >= cause.at - TimeDelta::seconds(5))
            });
            let confidence = if corroborated && cause.confidence >= Confidence::Medium {
                Confidence::High
            } else {
                cause.confidence
            };
            let rationale = format!(
                "{} reported {} on {}{}{}",
                cause.source.as_str(),
                cause.class,
                cause.node,
                cause.device.as_deref().map(|d| format!(" ({d})")).unwrap_or_default(),
                if corroborated {
                    ", and the job's own logs show the failure downstream of it"
                } else {
                    ", with no corroborating signal in the job's logs"
                }
            );
            (cause.class, confidence, rationale, Some(FirstFault::from(cause)))
        }
        None => {
            // No device or fabric evidence. The rank topology is the only thing
            // left that can distinguish a deadlock from a straggler — and it
            // frequently can, because the ranks that printed nothing are the
            // ones the others were waiting on.
            match nccl::topology_verdict(&inputs.topology) {
                Some((class, conf, why)) => (class, conf, why, None),
                None => {
                    // Fall back to the strongest thing we have, which may be a
                    // bare symptom. It is reported *as* a symptom.
                    match relevant.first() {
                        Some(s) => (
                            s.class,
                            s.confidence.min(Confidence::Low),
                            format!(
                                "only symptom-level evidence: {} reported {} with nothing \
                                 corroborating it on the job's nodes in its window — this says \
                                 where the job noticed, not where it broke",
                                s.source.as_str(),
                                s.class
                            ),
                            None,
                        ),
                        None => (
                            job.state_class().unwrap_or(FaultClass::Unknown),
                            Confidence::Low,
                            "no device, fabric or log evidence found on the job's nodes in its \
                             window — the failure is unattributed, not benign"
                                .to_string(),
                            None,
                        ),
                    }
                }
            }
        }
    };

    // Blast radius: distinct nodes with any signal. A single node is a device
    // to drain; a third of the job is a fabric domain, and the two want
    // completely different follow-up.
    let affected: BTreeSet<&str> = relevant
        .iter()
        .filter(|s| !s.node.is_empty())
        .map(|s| s.node.as_str())
        .collect();

    let evidence: Vec<Evidence> = relevant.iter().take(EVIDENCE_LIMIT).map(Evidence::from).collect();
    if relevant.len() > EVIDENCE_LIMIT {
        warnings.push(format!(
            "{} further signals matched and are not listed (showing the {EVIDENCE_LIMIT} most \
             believable)",
            relevant.len() - EVIDENCE_LIMIT
        ));
    }
    if off_node > 0 || out_of_window > 0 {
        warnings.push(format!(
            "filtered out {off_node} signal(s) on nodes outside the job and {out_of_window} \
             outside its window"
        ));
    }

    JobAutopsy {
        job_id: job.job_id.clone(),
        job_name: job.name.clone(),
        state: job.state.clone(),
        nodes: job.nodes.iter().cloned().collect(),
        started_at: job.start,
        ended_at: job.end,
        elapsed_secs: job.elapsed_secs,
        gpus: job.gpus,
        gpu_hours_lost: job.gpu_hours(),
        class,
        disposition: class.disposition(),
        confidence,
        rationale,
        first_fault: first,
        affected_nodes: affected.into_iter().map(str::to_string).collect(),
        rank_topology: summarize_topology(&inputs.topology),
        recommendation: Recommendation::for_class(class, confidence, root.as_ref()),
        evidence,
        warnings,
    }
}

/// How many signals travel in the record. A failed 1024-GPU job can produce
/// tens of thousands of matching lines; a record nobody can read is not a
/// diagnosis. The tail is counted in a warning rather than dropped silently.
const EVIDENCE_LIMIT: usize = 20;

fn summarize_topology(t: &RankTopology) -> Option<crate::record::TopologySummary> {
    if t.timed_out.is_empty() && t.seen.is_empty() {
        return None;
    }
    let silent = t.silent_ranks();
    let universe = t.world_size.map(|n| n as usize).unwrap_or(t.seen.len());
    // "Silent" only means something when almost everyone else spoke. On a
    // 128-rank job where three ranks printed a timeout, the other 125 are not
    // suspects — they are ranks whose logs we do not have, and listing them as
    // "waited on" points the operator at 125 healthy machines. Same bar as
    // `nccl::topology_verdict`, so the record never shows a straggler list the
    // verdict itself refused to draw.
    let straggler_shaped =
        !silent.is_empty() && universe > 0 && silent.len() * 4 <= universe;
    Some(crate::record::TopologySummary {
        world_size: t.world_size,
        ranks_seen: t.seen.len() as u32,
        ranks_timed_out: t.timed_out.iter().copied().collect(),
        ranks_silent: if straggler_shaped {
            silent
                .iter()
                .take(32)
                .map(|r| crate::record::SilentRank {
                    rank: *r,
                    node: t.rank_node.get(r).cloned(),
                })
                .collect()
        } else {
            Vec::new()
        },
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::sacct;
    use crate::signal::Source;

    fn job() -> JobRecord {
        let line = "88123|train|FAILED|1:0|2026-08-26T02:00:00|2026-08-26T09:58:41|07:58:41|node-[40-55]|gres/gpu=128,node=16|None\n";
        sacct::parse(line).unwrap().0.remove(0)
    }

    fn sig(node: &str, class: FaultClass, secs: i64, src: Source, conf: Confidence) -> Signal {
        Signal {
            // 2026-08-26T09:58:41Z is 1787738321; offset from there.
            at: DateTime::parse_from_rfc3339("2026-08-26T09:58:41Z").unwrap().with_timezone(&Utc)
                + TimeDelta::seconds(secs),
            node: node.into(),
            device: None,
            rank: None,
            source: src,
            class,
            confidence: conf,
            detail: format!("{class} on {node}"),
            dated: true,
        }
    }

    #[test]
    fn a_device_fault_beats_the_collective_timeout_that_reported_it() {
        // The headline case. Every rank printed a watchdog timeout at T-90s;
        // node-47's GPU threw an XID. The verdict is the XID.
        let inputs = Inputs {
            signals: vec![
                sig("node-40", FaultClass::NcclTimeout, -90, Source::Nccl, Confidence::Low),
                sig("node-41", FaultClass::NcclTimeout, -89, Source::Nccl, Confidence::Low),
                sig("node-47", FaultClass::GpuFallenOffBus, -85, Source::Dcgm, Confidence::High),
            ],
            ..Default::default()
        };
        let a = correlate(&job(), inputs, &Window::default());
        assert_eq!(a.class, FaultClass::GpuFallenOffBus);
        assert_eq!(a.confidence, Confidence::High, "cause plus corroborating symptom");
        assert_eq!(a.first_fault.unwrap().node, "node-47");
        assert!(a.recommendation.drain_node.is_some());
    }

    #[test]
    fn a_fault_on_a_node_this_job_never_held_is_not_this_jobs_fault() {
        // Without the node filter, one broken machine is blamed for every job
        // on the cluster.
        let inputs = Inputs {
            signals: vec![
                sig("node-99", FaultClass::GpuEcc, -100, Source::Dcgm, Confidence::High),
                sig("node-40", FaultClass::NcclTimeout, -90, Source::Nccl, Confidence::Low),
            ],
            ..Default::default()
        };
        let a = correlate(&job(), inputs, &Window::default());
        assert_ne!(a.class, FaultClass::GpuEcc);
        assert_eq!(a.class, FaultClass::NcclTimeout);
        assert_eq!(a.confidence, Confidence::Low);
        assert!(a.rationale.contains("where the job noticed, not where it broke"), "{}", a.rationale);
    }

    #[test]
    fn an_xid_landing_after_the_job_exited_is_still_inside_the_window() {
        // The driver reports the device *after* the process is gone. A window
        // that closes at the exit misses exactly the evidence it was opened for.
        let inputs = Inputs {
            signals: vec![sig("node-47", FaultClass::GpuEcc, 45, Source::Dcgm, Confidence::High)],
            ..Default::default()
        };
        let a = correlate(&job(), inputs, &Window::default());
        assert_eq!(a.class, FaultClass::GpuEcc);
    }

    #[test]
    fn an_event_from_last_week_on_the_same_node_is_out_of_window() {
        let inputs = Inputs {
            signals: vec![sig("node-47", FaultClass::GpuEcc, -86_400 * 7, Source::Dcgm, Confidence::High)],
            ..Default::default()
        };
        let a = correlate(&job(), inputs, &Window::default());
        assert_ne!(a.class, FaultClass::GpuEcc);
        assert!(a.warnings.iter().any(|w| w.contains("outside its window")), "{:?}", a.warnings);
    }

    #[test]
    fn with_no_device_evidence_the_rank_topology_decides() {
        let mut topo = RankTopology { world_size: Some(4), ..Default::default() };
        for r in 0..3 {
            topo.timed_out.insert(r);
            topo.seen.insert(r);
        }
        topo.rank_node.insert(3, "node-47".into());
        let inputs = Inputs {
            signals: vec![sig("node-40", FaultClass::NcclTimeout, -30, Source::Nccl, Confidence::Low)],
            topology: topo,
            ..Default::default()
        };
        let a = correlate(&job(), inputs, &Window::default());
        assert_eq!(a.class, FaultClass::StragglerRank);
        assert!(a.rationale.contains("rank 3"), "{}", a.rationale);
        let t = a.rank_topology.unwrap();
        assert_eq!(t.ranks_silent.len(), 1);
        assert_eq!(t.ranks_silent[0].rank, 3);
    }

    #[test]
    fn no_evidence_at_all_is_reported_as_unattributed_not_as_fine() {
        let a = correlate(&job(), Inputs::default(), &Window::default());
        assert_eq!(a.class, FaultClass::Unknown);
        assert_eq!(a.confidence, Confidence::Low);
        assert!(a.rationale.contains("unattributed, not benign"), "{}", a.rationale);
        assert!(!a.recommendation.retry, "nothing is retried on no evidence");
    }

    #[test]
    fn the_scheduler_state_is_used_when_it_knows_and_nothing_else_does() {
        let line = "9|j|PREEMPTED|0:0|2026-08-26T02:00:00|2026-08-26T09:58:41|07:58:41|node-40|gres/gpu=8,node=1|None\n";
        let j = sacct::parse(line).unwrap().0.remove(0);
        let inputs = Inputs { signals: j.signals(), ..Default::default() };
        let a = correlate(&j, inputs, &Window::default());
        assert_eq!(a.class, FaultClass::Preemption);
        assert!(a.recommendation.retry, "a preempted job is exactly what retry is for");
        assert!(a.recommendation.drain_node.is_none(), "the node is fine");
    }

    #[test]
    fn the_evidence_list_is_bounded_and_says_so() {
        let signals: Vec<Signal> = (0..40)
            .map(|i| sig("node-40", FaultClass::NcclTimeout, -i, Source::Nccl, Confidence::Low))
            .collect();
        let a = correlate(&job(), Inputs { signals, ..Default::default() }, &Window::default());
        assert_eq!(a.evidence.len(), EVIDENCE_LIMIT);
        assert!(a.warnings.iter().any(|w| w.contains("not listed")), "{:?}", a.warnings);
    }

    #[test]
    fn blast_radius_distinguishes_one_device_from_a_fabric_domain() {
        let signals: Vec<Signal> = (40..48)
            .map(|n| sig(&format!("node-{n}"), FaultClass::FabricIb, -10, Source::Ib, Confidence::High))
            .collect();
        let a = correlate(&job(), Inputs { signals, ..Default::default() }, &Window::default());
        assert_eq!(a.affected_nodes.len(), 8);
        assert_eq!(a.class, FaultClass::FabricIb);
    }

    #[test]
    fn a_thin_log_sample_does_not_accuse_every_rank_it_lacks_a_line_for() {
        // Three ranks' worth of log from a 128-rank job. The other 125 are not
        // suspects — their logs are simply not in the file — and listing them
        // as "waited on" points the operator at 125 healthy machines.
        let mut topo = RankTopology { world_size: Some(128), ..Default::default() };
        for r in 0..3 {
            topo.timed_out.insert(r);
            topo.seen.insert(r);
        }
        let a = correlate(&job(), Inputs { topology: topo, ..Default::default() }, &Window::default());
        let t = a.rank_topology.unwrap();
        assert_eq!(t.ranks_timed_out.len(), 3, "what we did see is still reported");
        assert!(t.ranks_silent.is_empty(), "but 125 unheard ranks are not accused");
    }
}
