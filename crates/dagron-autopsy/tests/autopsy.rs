//! End-to-end: four real-shaped inputs in, one fault-attributed record out.
//!
//! The unit tests prove each parser and the ranking rule in isolation. This
//! file proves the thing the tool is *for* — that on a log set shaped like a
//! real failed training job, the answer is the dead GPU and not the collective
//! timeout that everyone reads first.

use dagron_autopsy::{correlate, dcgm, ib, nccl, sacct, Inputs, Window};
use dagron_core::fault::{Confidence, Disposition, FaultClass};

fn fixture(name: &str) -> String {
    let path = concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/");
    std::fs::read_to_string(format!("{path}{name}"))
        .unwrap_or_else(|e| panic!("reading fixture {name}: {e}"))
}

/// Assemble the pipeline the way `main.rs` does, so the test exercises the same
/// wiring rather than a convenient shortcut.
fn autopsy(job_id: &str, nccl_fixture: &str) -> dagron_autopsy::JobAutopsy {
    autopsy_with(job_id, nccl_fixture, "dcgm.ndjson")
}

fn autopsy_with(job_id: &str, nccl_fixture: &str, dcgm_fixture: &str) -> dagron_autopsy::JobAutopsy {
    let (records, warnings) = sacct::parse(&fixture("sacct.txt")).unwrap();
    let job = sacct::select(&records, Some(job_id)).unwrap().clone();

    let mut signals = job.signals();
    signals.extend(dcgm::parse(&fixture(dcgm_fixture)));

    let fallback_at = job.end.unwrap();
    let first_node = job.nodes.iter().next().cloned();
    let report = nccl::parse(&fixture(nccl_fixture), fallback_at, first_node.as_deref());
    signals.extend(report.signals);

    let before = ib::parse_samples(&fixture("ib-before.txt"));
    let after = ib::parse_samples(&fixture("ib-after.txt"));
    signals.extend(ib::diff(&before, &after, fallback_at));

    correlate(
        &job,
        Inputs { signals, topology: report.topology, warnings },
        &Window::default(),
    )
}

#[test]
fn the_dead_gpu_wins_over_the_collective_timeout_everyone_reads_first() {
    let a = autopsy("88123", "job.out");

    // The verdict: not "NCCL timeout", not "network".
    assert_eq!(a.class, FaultClass::GpuFallenOffBus);
    assert_eq!(a.disposition, Disposition::Infrastructure);
    assert_eq!(a.confidence, Confidence::High);

    // Located to the device, which is the whole product claim.
    let first = a.first_fault.as_ref().expect("a located first fault");
    assert_eq!(first.node, "node-47");
    assert_eq!(first.device.as_deref(), Some("gpu3"));
    assert!(first.dated, "the DCGM line carried its own clock");

    // Acted on: drain the node, retry elsewhere.
    assert!(a.recommendation.retry);
    assert_eq!(a.recommendation.drain_node.as_deref(), Some("node-47"));

    // Denominated in the unit that sells: 128 GPUs × 7h47m38s.
    let lost = a.gpu_hours_lost.expect("gpu-hours");
    assert!((lost - 997.6).abs() < 1.0, "got {lost}");

    // The ECC event on node-99 is a real fault on a node this job never held.
    // Including it would blame one broken machine for every job on the cluster.
    assert!(!a.affected_nodes.contains(&"node-99".to_string()));
    assert!(a.evidence.iter().all(|e| e.node != "node-99"));

    // The timeouts are still on the record — as corroborating evidence, below
    // the cause. They are what makes the verdict High rather than Medium.
    assert!(a.evidence.iter().any(|e| e.class == FaultClass::NcclTimeout));
    assert_eq!(a.evidence[0].class, FaultClass::GpuFallenOffBus, "cause first");

    // The headline is the sentence the pitch promises.
    let h = a.headline();
    assert!(h.contains("gpu3 on node-47"), "{h}");
    assert!(h.contains("gpu-fallen-off-bus"), "{h}");
    assert!(h.contains("T-"), "the offset from the job's end: {h}");
}

#[test]
fn a_nan_loss_is_not_retried_even_though_every_rank_also_timed_out() {
    // The expensive inversion, on a cluster that is *healthy* (clean DCGM).
    // Same watchdog timeouts in the log, but the cause is the job's own
    // arithmetic — and the record must say "do not retry", because the next
    // attempt reproduces it at full cluster cost. A blind retry policy here
    // spends another 997 GPU-hours to learn nothing.
    let a = autopsy_with("88123", "nan_job.out", "dcgm_clean.ndjson");
    assert_eq!(a.class, FaultClass::NanLoss);
    assert_eq!(a.disposition, Disposition::Application);
    assert!(!a.recommendation.retry);
    assert!(a.recommendation.drain_node.is_none(), "nothing is broken");
    assert!(a.recommendation.retry_budget_hint.contains("nan-loss: 1"));
    assert!(a.recommendation.summary.contains("do not retry"), "{}", a.recommendation.summary);
    // The timeout is still on the record, below the cause.
    assert!(a.evidence.iter().any(|e| e.class == FaultClass::NcclTimeout));
}

#[test]
fn a_real_device_fault_outranks_the_jobs_own_nan_print() {
    // The counterpart, and the reason the previous test needs a clean DCGM
    // fixture: when a GPU genuinely fell off the bus in the same window, the
    // device wins — a NaN printed by a rank whose peer just died is a
    // consequence, not the cause.
    let a = autopsy("88123", "nan_job.out");
    assert_eq!(a.class, FaultClass::GpuFallenOffBus);
    assert_eq!(a.recommendation.drain_node.as_deref(), Some("node-47"));
}

#[test]
fn the_text_rendering_leads_with_the_answer() {
    let text = autopsy("88123", "job.out").to_text();
    let head: Vec<&str> = text.lines().take(8).collect();
    let joined = head.join("\n");
    assert!(joined.contains("VERDICT"), "{joined}");
    // Evidence comes after the verdict — the inverse of how the logs present
    // it, which is why reading them takes an hour.
    let verdict_at = text.find("VERDICT").unwrap();
    let evidence_at = text.find("EVIDENCE").unwrap();
    assert!(verdict_at < evidence_at);
    assert!(text.contains("ACTION"));
    assert!(text.contains("node-47"));
}

#[test]
fn the_json_record_round_trips_and_carries_the_machine_contract() {
    let a = autopsy("88123", "job.out");
    let json = a.to_json();
    let v: serde_json::Value = serde_json::from_str(&json).unwrap();
    assert_eq!(v["class"], "gpu-fallen-off-bus");
    assert_eq!(v["disposition"], "infrastructure");
    assert_eq!(v["confidence"], "high");
    assert_eq!(v["first_fault"]["node"], "node-47");
    assert_eq!(v["first_fault"]["device"], "gpu3");
    assert_eq!(v["recommendation"]["drain_node"], "node-47");
    assert_eq!(v["recommendation"]["retry"], true);
    assert_eq!(v["job_id"], "88123");
    assert!(v["gpu_hours_lost"].as_f64().unwrap() > 900.0);

    // Deserializes back into the same record — the contract a fleet database
    // or a provider API reads.
    let back: dagron_autopsy::JobAutopsy = serde_json::from_str(&json).unwrap();
    assert_eq!(back.class, a.class);
    assert_eq!(back.evidence.len(), a.evidence.len());
}

#[test]
fn missing_sources_are_confessed_rather_than_papered_over() {
    // A verdict reached without DCGM is not the same verdict, and the record
    // has to say so or it reads like a complete one.
    let (records, warnings) = sacct::parse(&fixture("sacct.txt")).unwrap();
    let job = sacct::select(&records, Some("88123")).unwrap().clone();
    let mut warnings = warnings;
    warnings.push("no --dcgm input: XID, ECC and row-remap events were not consulted".into());
    let report = nccl::parse(&fixture("job.out"), job.end.unwrap(), None);
    let a = correlate(
        &job,
        Inputs { signals: report.signals, topology: report.topology, warnings },
        &Window::default(),
    );
    assert_ne!(a.class, FaultClass::GpuFallenOffBus, "no DCGM, no device verdict");
    assert!(a.warnings.iter().any(|w| w.contains("no --dcgm input")));
    assert!(a.confidence <= Confidence::Medium);
}

#[test]
fn the_silent_rank_is_named_when_the_logs_are_all_there_is() {
    // No device evidence at all: the only thing that can tell a deadlock from a
    // straggler is who *didn't* print, and it does.
    let (records, warnings) = sacct::parse(&fixture("sacct.txt")).unwrap();
    let job = sacct::select(&records, Some("88123")).unwrap().clone();
    let report = nccl::parse(&fixture("straggler_job.out"), job.end.unwrap(), Some("node-40"));
    let a = correlate(
        &job,
        Inputs { signals: report.signals, topology: report.topology, warnings },
        &Window::default(),
    );
    assert_eq!(a.class, FaultClass::StragglerRank);
    assert_eq!(a.disposition, Disposition::Application);
    assert!(a.rationale.contains("rank 3"), "{}", a.rationale);
    let t = a.rank_topology.expect("topology");
    assert_eq!(t.world_size, Some(4));
    assert_eq!(t.ranks_timed_out, vec![0, 1, 2]);
    assert_eq!(t.ranks_silent[0].rank, 3);
}

#[test]
fn an_unchanged_fabric_contributes_nothing() {
    // Both counter samples are identical in the fixtures. A tool that reported
    // node-40's 1204 lifetime symbol errors as a fault would flag every job.
    let before = ib::parse_samples(&fixture("ib-before.txt"));
    let after = ib::parse_samples(&fixture("ib-after.txt"));
    let at = chrono::Utc::now();
    assert!(ib::diff(&before, &after, at).is_empty());
}

#[test]
fn a_fabric_that_flapped_during_the_job_becomes_the_verdict() {
    // The other half of the pair, and the one the identical fixtures never
    // exercised end to end: same baseline, but node-47's link went down and its
    // symbol errors jumped during the window. With no DCGM evidence, the fabric
    // is the cause — and the fabric signal has to survive the node/window join
    // and the precedence ranking to get there, which unit-testing `ib::diff`
    // alone does not prove.
    let (records, warnings) = sacct::parse(&fixture("sacct.txt")).unwrap();
    let job = sacct::select(&records, Some("88123")).unwrap().clone();
    let fallback_at = job.end.unwrap();

    let mut signals = job.signals();
    signals.extend(dcgm::parse(&fixture("dcgm_clean.ndjson")));
    let report = nccl::parse(&fixture("job.out"), fallback_at, job.nodes.iter().next().map(|s| s.as_str()));
    signals.extend(report.signals);
    signals.extend(ib::diff(
        &ib::parse_samples(&fixture("ib-before.txt")),
        &ib::parse_samples(&fixture("ib-after-flap.txt")),
        fallback_at,
    ));

    let a = correlate(
        &job,
        Inputs { signals, topology: report.topology, warnings },
        &Window::default(),
    );
    assert_eq!(a.class, FaultClass::FabricIb);
    assert_eq!(a.disposition, Disposition::Infrastructure);
    let first = a.first_fault.as_ref().expect("a located first fault");
    assert_eq!(first.node, "node-47");
    assert_eq!(first.device.as_deref(), Some("mlx5_0:1"));
    assert!(a.recommendation.retry);
    assert_eq!(a.recommendation.drain_node.as_deref(), Some("node-47"));
    // node-40's counters did not move, so it is not implicated.
    assert!(
        !a.evidence.iter().any(|e| e.node == "node-40" && e.class == FaultClass::FabricIb),
        "an unchanged port is not evidence"
    );
}
