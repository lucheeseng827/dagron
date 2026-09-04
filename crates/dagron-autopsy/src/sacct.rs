//! Slurm accounting (`sacct`) — the job's identity, node set, and time window.
//!
//! This is the *frame* for the whole autopsy: without a node set and a window
//! there is nothing to intersect the device and fabric signals against, and the
//! tool degenerates into grep. Everything else in the crate is filtered by what
//! this module returns.
//!
//! Reads the parseable form:
//!
//! ```text
//! sacct -j <jobid> -P -n -X=false -o JobID,JobName,State,ExitCode,Start,End,Elapsed,NodeList,AllocTRES,Reason
//! ```
//!
//! `-P` (pipe-separated, no padding) rather than the default table, because the
//! table's column truncation silently mangles long node lists — and a truncated
//! node list is the join key quietly losing members.

use crate::nodelist;
use crate::signal::{Signal, Source};
use crate::timestamp;
use anyhow::{bail, Result};
use chrono::{DateTime, Utc};
use dagron_core::fault::{classify_text, Confidence, FaultClass};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// The default field order this parser expects, in `sacct -o` syntax. Exposed
/// so the CLI can print the exact command a user should run — a field order
/// that drifts between the doc and the parser is a support ticket per site.
pub const FIELDS: &str =
    "JobID,JobName,State,ExitCode,Start,End,Elapsed,NodeList,AllocTRES,Reason";

/// One accounting row, already interpreted.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct JobRecord {
    pub job_id: String,
    pub name: String,
    /// Slurm's state word, verbatim (`FAILED`, `TIMEOUT`, `NODE_FAIL`, …).
    pub state: String,
    /// `ExitCode` as `code:signal`, split.
    pub exit_code: Option<i32>,
    pub signal: Option<i32>,
    pub start: Option<DateTime<Utc>>,
    pub end: Option<DateTime<Utc>>,
    pub elapsed_secs: Option<i64>,
    /// Compressed hostlist as Slurm wrote it — kept for display.
    pub nodelist_raw: String,
    /// Expanded + normalized. **This** is the join key.
    pub nodes: BTreeSet<String>,
    /// GPUs allocated to the job (`gres/gpu=` in AllocTRES). The multiplier on
    /// every wasted-hours number, so an absent value is `None` rather than 0 —
    /// reporting "0 GPU-hours wasted" for an unparsed TRES string would be the
    /// most reassuring possible way to be wrong.
    pub gpus: Option<u32>,
    pub node_count: Option<u32>,
    /// Slurm's `Reason`, when it has one.
    pub reason: Option<String>,
}

impl JobRecord {
    /// Whether this row is the job itself rather than one of its steps
    /// (`88123` vs `88123.batch` / `88123.0`).
    pub fn is_job_level(&self) -> bool {
        !self.job_id.contains('.')
    }

    /// GPU-hours the job consumed before it died — the number the whole
    /// business case is denominated in. `None` when the allocation did not name
    /// GPUs; see [`JobRecord::gpus`].
    pub fn gpu_hours(&self) -> Option<f64> {
        let gpus = self.gpus? as f64;
        let secs = self.elapsed_secs? as f64;
        Some(gpus * secs / 3600.0)
    }

    /// What the *scheduler* thinks happened, before any device evidence.
    ///
    /// Deliberately coarse. Slurm knows a job was preempted, timed out, or that
    /// a node failed under it; it does not and cannot know an ECC error caused
    /// the exit, so `FAILED` maps to `None` here and is left for the collectors
    /// that can see the device.
    pub fn state_class(&self) -> Option<FaultClass> {
        let s = self.state.split_whitespace().next().unwrap_or(&self.state);
        Some(match s.trim_end_matches('+') {
            "TIMEOUT" => FaultClass::WalltimeExceeded,
            "NODE_FAIL" => FaultClass::NodeFail,
            "PREEMPTED" => FaultClass::Preemption,
            "CANCELLED" => FaultClass::Cancelled,
            "OUT_OF_MEMORY" => FaultClass::HostOom,
            "BOOT_FAIL" => FaultClass::NodeFail,
            // FAILED says only "non-zero exit". That is the case this whole
            // tool exists for, and guessing here would pre-empt the evidence.
            _ => return None,
        })
    }

    /// The signals this row contributes: the scheduler's own verdict, plus
    /// anything classifiable in its `Reason` text.
    pub fn signals(&self) -> Vec<Signal> {
        let mut out = Vec::new();
        let Some(at) = self.end.or(self.start) else {
            return out;
        };
        // Node-scoped: sacct reports the allocation, not which node broke. The
        // first node stands in for "the job" so the signal has a location at
        // all; the correlator never uses a sacct signal to name a device.
        let node = self.nodes.iter().next().cloned().unwrap_or_default();
        if let Some(class) = self.state_class() {
            out.push(Signal {
                at,
                node: node.clone(),
                device: None,
                rank: None,
                source: Source::Sacct,
                class,
                confidence: Confidence::High,
                detail: format!("sacct: job {} state {}", self.job_id, self.state),
                dated: true,
            });
        }
        if let Some(reason) = self.reason.as_deref().filter(|r| !r.trim().is_empty() && *r != "None")
        {
            if let Some(c) = classify_text(reason) {
                out.push(Signal {
                    at,
                    node,
                    device: None,
                    rank: None,
                    source: Source::Sacct,
                    class: c.class,
                    confidence: c.confidence,
                    detail: format!("sacct Reason: {reason}"),
                    dated: true,
                });
            }
        }
        out
    }
}

/// Parse `sacct -P -n` output. Tolerates a header line (with `-P` but without
/// `-n`) and skips blanks.
///
/// A row with the wrong field count is skipped with the reason collected rather
/// than failing the parse: sites customize `sacct` output, and one odd step row
/// should not cost the whole autopsy. The skipped count travels back so the
/// caller can say so out loud instead of silently reporting less.
pub fn parse(text: &str) -> Result<(Vec<JobRecord>, Vec<String>)> {
    let mut out = Vec::new();
    let mut warnings = Vec::new();
    for (i, line) in text.lines().enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let f: Vec<&str> = line.split('|').collect();
        if f.len() < 8 {
            warnings.push(format!("sacct line {}: expected >=8 pipe-separated fields ({FIELDS}), got {} — skipped", i + 1, f.len()));
            continue;
        }
        // Header row from a `-P` run without `-n`.
        if f[0].eq_ignore_ascii_case("JobID") {
            continue;
        }
        let nodelist_raw = f[7].trim().to_string();
        let nodes = if nodelist_raw.is_empty()
            || nodelist_raw == "None assigned"
            || nodelist_raw == "None"
        {
            BTreeSet::new()
        } else {
            nodelist::expand_normalized(&nodelist_raw)
        };
        let (exit_code, signal) = parse_exit(f[3]);
        let tres = f.get(8).copied().unwrap_or("");
        out.push(JobRecord {
            job_id: f[0].trim().to_string(),
            name: f[1].trim().to_string(),
            state: f[2].trim().to_string(),
            exit_code,
            signal,
            start: timestamp::parse(f[4]).ok(),
            end: timestamp::parse(f[5]).ok(),
            elapsed_secs: timestamp::parse_elapsed_secs(f[6]),
            nodelist_raw,
            nodes,
            gpus: tres_value(tres, "gres/gpu"),
            node_count: tres_value(tres, "node"),
            reason: f.get(9).map(|s| s.trim().to_string()).filter(|s| !s.is_empty()),
        });
    }
    if out.is_empty() && warnings.is_empty() {
        bail!("no sacct rows parsed (is the output `sacct -P -n -o {FIELDS}`?)");
    }
    Ok((out, warnings))
}

/// `1:0` → (Some(1), Some(0)). Slurm reports the signal after the colon.
fn parse_exit(s: &str) -> (Option<i32>, Option<i32>) {
    let t = s.trim();
    match t.split_once(':') {
        Some((c, sig)) => (c.trim().parse().ok(), sig.trim().parse().ok()),
        None => (t.parse().ok(), None),
    }
}

/// Pull one key out of an `AllocTRES` string:
/// `cpu=1024,gres/gpu=128,mem=8000G,node=16`.
///
/// Real `AllocTRES` may lead with a weight field when the site configures
/// `TRESBillingWeights`. This parser reads `gres/gpu` and `node` and nothing
/// else, so the examples and fixtures here omit that field on purpose: the OSS
/// mirror's forbidden-marker scan tripwires on its name, and carving an
/// exception into a security guard to keep an unused example field would be a
/// poor trade. Parsing is unaffected — an unrecognized key is skipped like any
/// other, which `tres_keys_match_exactly_not_by_substring` covers.
///
/// Exact key match on the segment before `=`, because a substring search for
/// `node` also matches `gres/gpu:node` style keys on some configurations, and
/// silently reading the wrong number here mis-scales every cost figure.
fn tres_value(tres: &str, key: &str) -> Option<u32> {
    for part in tres.split(',') {
        if let Some((k, v)) = part.split_once('=') {
            if k.trim() == key {
                return v.trim().parse().ok();
            }
        }
    }
    None
}

/// Pick the job-level row for `job_id` out of a parsed set, falling back to the
/// first job-level row when no id is given.
pub fn select<'a>(records: &'a [JobRecord], job_id: Option<&str>) -> Option<&'a JobRecord> {
    match job_id {
        Some(id) => records
            .iter()
            .find(|r| r.job_id == id)
            .or_else(|| records.iter().find(|r| r.is_job_level() && r.job_id.starts_with(id))),
        None => records.iter().find(|r| r.is_job_level()),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "\
88123|train_llama_70b|FAILED|1:0|2026-08-26T02:11:03|2026-08-26T09:58:41|07:47:38|node-[40-55]|cpu=1024,gres/gpu=128,mem=8000G,node=16|None
88123.batch|batch|FAILED|1:0|2026-08-26T02:11:03|2026-08-26T09:58:41|07:47:38|node-40|cpu=64,gres/gpu=8,mem=500G,node=1|None
88123.0|python|FAILED|1:0|2026-08-26T02:11:05|2026-08-26T09:58:41|07:47:36|node-[40-55]|cpu=1024,gres/gpu=128,mem=8000G,node=16|None
";

    #[test]
    fn parses_the_job_row_and_expands_its_node_set() {
        let (recs, warns) = parse(SAMPLE).unwrap();
        assert!(warns.is_empty());
        assert_eq!(recs.len(), 3);
        let job = select(&recs, Some("88123")).unwrap();
        assert!(job.is_job_level());
        assert_eq!(job.name, "train_llama_70b");
        assert_eq!(job.state, "FAILED");
        assert_eq!(job.exit_code, Some(1));
        assert_eq!(job.gpus, Some(128));
        assert_eq!(job.node_count, Some(16));
        assert_eq!(job.nodes.len(), 16);
        assert!(job.nodes.contains("node-47"));
        assert!(!job.nodes.contains("node-56"));
        assert_eq!(job.elapsed_secs, Some(28_058));
    }

    #[test]
    fn gpu_hours_is_the_cost_of_the_failure() {
        let (recs, _) = parse(SAMPLE).unwrap();
        let job = select(&recs, Some("88123")).unwrap();
        // 128 GPUs × 7h47m38s.
        let h = job.gpu_hours().unwrap();
        assert!((h - 997.6).abs() < 0.5, "got {h}");
    }

    #[test]
    fn an_unparsed_tres_reports_no_gpus_rather_than_zero_gpus() {
        // "0 GPU-hours wasted" is the most reassuring possible way to be wrong.
        let line = "1|j|FAILED|1:0|2026-01-01T00:00:00|2026-01-01T01:00:00|01:00:00|n1|cpu=8,mem=1G|None\n";
        let (recs, _) = parse(line).unwrap();
        assert_eq!(recs[0].gpus, None);
        assert_eq!(recs[0].gpu_hours(), None);
    }

    #[test]
    fn tres_keys_match_exactly_not_by_substring() {
        assert_eq!(tres_value("cpu=4,node=16,gres/gpu=128", "node"), Some(16));
        assert_eq!(tres_value("gres/gpu=128", "gpu"), None, "not a substring match");
        assert_eq!(tres_value("cpu=8", "gres/gpu"), None);
    }

    #[test]
    fn scheduler_states_map_only_where_slurm_actually_knows() {
        let mk = |state: &str| {
            let line = format!("1|j|{state}|0:0|2026-01-01T00:00:00|2026-01-01T01:00:00|01:00:00|n1|node=1|None\n");
            parse(&line).unwrap().0.remove(0)
        };
        assert_eq!(mk("TIMEOUT").state_class(), Some(FaultClass::WalltimeExceeded));
        assert_eq!(mk("NODE_FAIL").state_class(), Some(FaultClass::NodeFail));
        assert_eq!(mk("PREEMPTED").state_class(), Some(FaultClass::Preemption));
        assert_eq!(mk("OUT_OF_MEMORY").state_class(), Some(FaultClass::HostOom));
        // `CANCELLED by 1234` — Slurm appends the canceller.
        assert_eq!(mk("CANCELLED by 1234").state_class(), Some(FaultClass::Cancelled));
        // The one that matters: FAILED means "non-zero exit" and nothing more.
        // Guessing here would pre-empt the device evidence this tool exists to
        // gather.
        assert_eq!(mk("FAILED").state_class(), None);
    }

    #[test]
    fn a_malformed_row_is_skipped_out_loud_not_silently() {
        let text = format!("{SAMPLE}garbage-without-pipes\n");
        let (recs, warns) = parse(&text).unwrap();
        assert_eq!(recs.len(), 3, "good rows survive");
        assert_eq!(warns.len(), 1);
        assert!(warns[0].contains("skipped"), "{:?}", warns);
    }

    #[test]
    fn a_header_row_from_a_run_without_dash_n_is_ignored() {
        let text = format!("JobID|JobName|State|ExitCode|Start|End|Elapsed|NodeList|AllocTRES|Reason\n{SAMPLE}");
        let (recs, warns) = parse(&text).unwrap();
        assert_eq!(recs.len(), 3);
        assert!(warns.is_empty());
    }

    #[test]
    fn a_pending_job_with_no_allocation_yields_an_empty_node_set() {
        let line = "9|j|PENDING|0:0|Unknown|Unknown||None assigned|cpu=1|Resources\n";
        let (recs, _) = parse(line).unwrap();
        assert!(recs[0].nodes.is_empty());
        assert_eq!(recs[0].start, None, "the `Unknown` sentinel is not the epoch");
        // No window and no nodes → no signals, rather than a signal at 1970.
        assert!(recs[0].signals().is_empty());
    }
}
