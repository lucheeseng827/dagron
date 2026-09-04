//! Fault attribution — the vocabulary a failure is recorded in.
//!
//! `status = 'failed'` says the engine gave up. It does not say **why**, and on
//! GPU fleets that difference is the whole cost model: an XID 79 and a NaN loss
//! are both "failed", but one wants an immediate retry on another node and the
//! other wants the run stopped before it burns another eight hours of a
//! thousand GPUs. Retrying the second as if it were the first is how a cluster
//! loses a week.
//!
//! So the taxonomy here is not a log-level enum — it is a **disposition**
//! machine. Every class answers one question: *is another attempt worth
//! anything?* That is [`Disposition`], and it is what the retry budget reads
//! ([`crate::models::should_retry_failed_with_class`]).
//!
//! Deliberately in `dagron-core`, not in the autopsy tool: the engine has to
//! classify a failure at the moment it decides whether to retry, and it cannot
//! reach into a sidecar binary to do it. `dagron-autopsy` is the *correlating*
//! layer on top — it joins these classes across DCGM, NCCL and the fabric — but
//! the words are shared, or the two would disagree about what happened.
//!
//! **The load-bearing honesty rule:** an NCCL collective timeout is a
//! *symptom*, never a cause. The rank that reports it is usually the rank that
//! was still healthy. Classifying it as a network fault is the single most
//! common wrong diagnosis in this domain, so [`FaultClass::NcclTimeout`] is
//! marked [`Precedence::Symptom`] and can only be *demoted* into a root cause
//! by corroborating evidence from another source. Text alone never promotes it.

use serde::{Deserialize, Serialize};
use std::fmt;

/// What another attempt is worth. The retry budget reads this, not the class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Disposition {
    /// The hardware or fabric under the job broke. The same code on another
    /// node very likely succeeds — retry liberally, and get the node out of the
    /// pool so the retry does not land back on it.
    Infrastructure,
    /// The job's own code, data or configuration. Another attempt reproduces
    /// it. Retrying is a pure transfer from the GPU budget to nothing.
    Application,
    /// The scheduler, the quota, or the market took the job away. Not a defect
    /// on either side; whether to retry is a *policy* question (deadline, price)
    /// rather than a diagnostic one.
    Platform,
    /// Not enough evidence to say. Never guessed into one of the others —
    /// an unknown that pretends to be an infra fault costs real GPU-hours.
    Unknown,
}

impl Disposition {
    /// The default attempt budget for a class when the workflow author did not
    /// set one. These are the numbers a retry policy degenerates to when nobody
    /// is thinking about it, so they are deliberately asymmetric.
    pub fn default_budget(self) -> u32 {
        match self {
            // Bounded, not unbounded: an "infra" fault that recurs five times
            // across five different nodes is not an infra fault, it is a wrong
            // classification, and the budget is the thing that stops it.
            Disposition::Infrastructure => 5,
            // One attempt: the one already spent.
            Disposition::Application => 1,
            Disposition::Platform => 3,
            // Falls back to the task's own `max_attempts` — see
            // `should_retry_failed_with_class`. Not a number, on purpose.
            Disposition::Unknown => 0,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Disposition::Infrastructure => "infrastructure",
            Disposition::Application => "application",
            Disposition::Platform => "platform",
            Disposition::Unknown => "unknown",
        }
    }
}

impl fmt::Display for Disposition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Whether a signal can be believed as a *cause*, or is only ever downstream of
/// one. The correlator sorts by this before it sorts by time — the earliest
/// event is not the root cause if the earliest event is a symptom.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum Precedence {
    /// Downstream of something else. Reported by the healthy side of a failure
    /// far more often than by the broken side.
    Symptom = 0,
    /// Could be either. Believed as a cause only when nothing stronger sits in
    /// the same window.
    Ambiguous = 1,
    /// A device or fabric said it broke, in its own words. Believed.
    RootCause = 2,
}

/// The recorded cause of a failed attempt.
///
/// Stored as the kebab-case string in `task_runs.fault_class`, so the set is
/// append-only: renaming a variant rewrites history that is already on disk.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FaultClass {
    // ── Accelerator ─────────────────────────────────────────────────────────
    /// An XID the driver attributes to the device rather than the kernel that
    /// ran on it (see [`xid_class`]).
    GpuXid,
    /// Contained/uncontained ECC, double-bit error, row-remap recording.
    GpuEcc,
    /// XID 79 and friends: the device left the PCIe bus. The node is done.
    GpuFallenOffBus,
    /// NVLink/NVSwitch error — the intra-node fabric, not the network.
    Nvlink,
    /// The device is there but wedged: GSP RPC timeouts, micro-controller halt.
    GpuUnresponsive,

    // ── Fabric / network ────────────────────────────────────────────────────
    /// InfiniBand/RoCE link event: port down, symbol-error burst, link flap.
    FabricIb,
    /// A collective aborted with a transport error the library named itself
    /// (`ncclSystemError`, bootstrap failure, unhandled CUDA error inside NCCL).
    NcclCommAbort,
    /// **Symptom.** A collective did not complete in the watchdog window. Says
    /// where the job noticed, not where it broke.
    NcclTimeout,

    // ── Distributed-job pathologies (application-side) ──────────────────────
    /// Every rank is waiting at the same collective and none of them is late —
    /// a mismatched collective order or a rank that never entered. Code, not
    /// hardware.
    Deadlock,
    /// One rank is measurably behind the rest and the others timed out on it.
    /// Named separately from a deadlock because the fix is different: a
    /// straggler is usually data loading, sharding skew, or one slow node.
    StragglerRank,
    /// A rank is alive but stuck outside the collective — dataloader, object
    /// store, or a mount that stopped answering.
    DataloaderStall,

    // ── Job-side resource / correctness ─────────────────────────────────────
    HostOom,
    GpuOom,
    /// Loss went NaN/Inf, or a divergence guard fired. The most expensive class
    /// to retry and the easiest to detect.
    NanLoss,
    /// The checkpoint the attempt tried to resume from is unreadable or
    /// truncated. Retrying *from that pointer* fails forever; the recovery is
    /// to fall back a checkpoint, which is why it is its own class.
    CheckpointCorrupt,
    /// Non-zero exit with an application traceback and no infra corroboration.
    UserCode,
    /// The task's configuration is wrong (missing env, bad path, image pull).
    Config,

    // ── Storage ─────────────────────────────────────────────────────────────
    /// Shared filesystem said no: stale NFS handle, Lustre EIO, ENOSPC.
    Storage,

    // ── Platform / scheduler ────────────────────────────────────────────────
    /// Spot reclaim, `PREEMPTED`, a scheduler SIGTERM with a grace period.
    Preemption,
    /// Slurm `NODE_FAIL`, a node drained under the job, a health-check kill.
    NodeFail,
    /// The job hit its wall clock, not a defect.
    WalltimeExceeded,
    /// A human or the control plane cancelled it.
    Cancelled,

    /// No signature matched. The honest default.
    Unknown,
}

impl FaultClass {
    pub fn as_str(self) -> &'static str {
        use FaultClass::*;
        match self {
            GpuXid => "gpu-xid",
            GpuEcc => "gpu-ecc",
            GpuFallenOffBus => "gpu-fallen-off-bus",
            Nvlink => "nvlink",
            GpuUnresponsive => "gpu-unresponsive",
            FabricIb => "fabric-ib",
            NcclCommAbort => "nccl-comm-abort",
            NcclTimeout => "nccl-timeout",
            Deadlock => "deadlock",
            StragglerRank => "straggler-rank",
            DataloaderStall => "dataloader-stall",
            HostOom => "host-oom",
            GpuOom => "gpu-oom",
            NanLoss => "nan-loss",
            CheckpointCorrupt => "checkpoint-corrupt",
            UserCode => "user-code",
            Config => "config",
            Storage => "storage",
            Preemption => "preemption",
            NodeFail => "node-fail",
            WalltimeExceeded => "walltime-exceeded",
            Cancelled => "cancelled",
            Unknown => "unknown",
        }
    }

    /// Parse the stored string back. Unknown strings map to
    /// [`FaultClass::Unknown`] rather than erroring: a row written by a newer
    /// build must not crash an older reader mid-rollout.
    pub fn parse(s: &str) -> FaultClass {
        use FaultClass::*;
        match s.trim().to_ascii_lowercase().replace('_', "-").as_str() {
            "gpu-xid" => GpuXid,
            "gpu-ecc" => GpuEcc,
            "gpu-fallen-off-bus" => GpuFallenOffBus,
            "nvlink" => Nvlink,
            "gpu-unresponsive" => GpuUnresponsive,
            "fabric-ib" => FabricIb,
            "nccl-comm-abort" => NcclCommAbort,
            "nccl-timeout" => NcclTimeout,
            "deadlock" => Deadlock,
            "straggler-rank" => StragglerRank,
            "dataloader-stall" => DataloaderStall,
            "host-oom" => HostOom,
            "gpu-oom" => GpuOom,
            "nan-loss" => NanLoss,
            "checkpoint-corrupt" => CheckpointCorrupt,
            "user-code" => UserCode,
            "config" => Config,
            "storage" => Storage,
            "preemption" => Preemption,
            "node-fail" => NodeFail,
            "walltime-exceeded" => WalltimeExceeded,
            "cancelled" | "canceled" => Cancelled,
            _ => Unknown,
        }
    }

    pub fn disposition(self) -> Disposition {
        // Both enums have an `Unknown`, so neither is glob-imported here: the
        // one thing this function must never do is silently resolve the wrong
        // one.
        use FaultClass as F;
        match self {
            F::GpuXid | F::GpuEcc | F::GpuFallenOffBus | F::Nvlink | F::GpuUnresponsive
            | F::FabricIb | F::NcclCommAbort | F::Storage | F::NodeFail => {
                Disposition::Infrastructure
            }
            F::Deadlock | F::StragglerRank | F::DataloaderStall | F::HostOom | F::GpuOom
            | F::NanLoss | F::CheckpointCorrupt | F::UserCode | F::Config => {
                Disposition::Application
            }
            F::Preemption | F::WalltimeExceeded | F::Cancelled => Disposition::Platform,
            // The one that matters: an uncorroborated collective timeout is
            // *not* filed as infrastructure. Doing so is how a stuck dataloader
            // gets retried eleven times on eleven healthy nodes.
            F::NcclTimeout | F::Unknown => Disposition::Unknown,
        }
    }

    pub fn precedence(self) -> Precedence {
        use FaultClass as F;
        match self {
            // The device or the fabric reported its own failure.
            F::GpuXid | F::GpuEcc | F::GpuFallenOffBus | F::Nvlink | F::GpuUnresponsive
            | F::FabricIb | F::Storage | F::NodeFail | F::Preemption => Precedence::RootCause,
            // Job-side facts that are self-evident from the job's own output.
            F::HostOom | F::GpuOom | F::NanLoss | F::CheckpointCorrupt | F::Config
            | F::WalltimeExceeded | F::Cancelled => Precedence::RootCause,
            // Could be the cause, could be downstream of a device that died.
            F::NcclCommAbort | F::Deadlock | F::StragglerRank | F::DataloaderStall
            | F::UserCode => Precedence::Ambiguous,
            F::NcclTimeout | F::Unknown => Precedence::Symptom,
        }
    }

    /// Whether the node this fault came from should be taken out of the pool
    /// before anything is retried onto it. The retry that lands back on the
    /// broken node is the classic wasted second attempt.
    pub fn should_drain_node(self) -> bool {
        use FaultClass::*;
        matches!(
            self,
            GpuXid | GpuEcc | GpuFallenOffBus | Nvlink | GpuUnresponsive | FabricIb | NodeFail
        )
    }

    /// Default attempt budget, from the disposition. `0` means "no opinion —
    /// use the task's own `max_attempts`".
    pub fn default_budget(self) -> u32 {
        self.disposition().default_budget()
    }
}

/// Every class name, in declaration order — the vocabulary, in one place.
///
/// Exists so a validation error can *list the alternatives* instead of telling
/// the author their key was wrong and leaving them to grep the source. The
/// `ALL` array below is what keeps it from drifting: adding a variant without
/// adding it here fails the round-trip test.
pub const FAULT_CLASS_NAMES: &[&str] = &[
    "gpu-xid",
    "gpu-ecc",
    "gpu-fallen-off-bus",
    "nvlink",
    "gpu-unresponsive",
    "fabric-ib",
    "nccl-comm-abort",
    "nccl-timeout",
    "deadlock",
    "straggler-rank",
    "dataloader-stall",
    "host-oom",
    "gpu-oom",
    "nan-loss",
    "checkpoint-corrupt",
    "user-code",
    "config",
    "storage",
    "preemption",
    "node-fail",
    "walltime-exceeded",
    "cancelled",
    "unknown",
];

impl FaultClass {
    /// Every variant, for exhaustive iteration (docs tables, the drift test,
    /// the autopsy tool's `--explain` listing).
    pub const ALL: [FaultClass; 23] = [
        FaultClass::GpuXid,
        FaultClass::GpuEcc,
        FaultClass::GpuFallenOffBus,
        FaultClass::Nvlink,
        FaultClass::GpuUnresponsive,
        FaultClass::FabricIb,
        FaultClass::NcclCommAbort,
        FaultClass::NcclTimeout,
        FaultClass::Deadlock,
        FaultClass::StragglerRank,
        FaultClass::DataloaderStall,
        FaultClass::HostOom,
        FaultClass::GpuOom,
        FaultClass::NanLoss,
        FaultClass::CheckpointCorrupt,
        FaultClass::UserCode,
        FaultClass::Config,
        FaultClass::Storage,
        FaultClass::Preemption,
        FaultClass::NodeFail,
        FaultClass::WalltimeExceeded,
        FaultClass::Cancelled,
        FaultClass::Unknown,
    ];
}

impl fmt::Display for FaultClass {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// How much the classification should be trusted. Recorded alongside the class
/// because a `low` gpu-xid and a `high` gpu-xid warrant different actions, and
/// collapsing them loses the only thing an operator wants to know.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Confidence {
    /// One symptom, nothing corroborating. Report it, do not act on it.
    Low,
    /// A root-cause signature matched, from one source.
    Medium,
    /// A root-cause signature from one source, corroborated by a symptom on the
    /// same node inside the same window — cause and effect, in order.
    High,
}

impl Confidence {
    pub fn as_str(self) -> &'static str {
        match self {
            Confidence::Low => "low",
            Confidence::Medium => "medium",
            Confidence::High => "high",
        }
    }
}

impl fmt::Display for Confidence {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// A classification plus the line that produced it. The evidence travels with
/// the verdict because a fault class with no quotable line is an assertion, and
/// nobody drains a node on an assertion.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Classification {
    pub class: FaultClass,
    pub confidence: Confidence,
    /// The matched line, trimmed and length-capped — this lands in
    /// `task_runs.fault_detail` and in the autopsy record's evidence chain.
    pub evidence: String,
}

impl Classification {
    pub fn new(class: FaultClass, confidence: Confidence, evidence: &str) -> Self {
        Classification {
            class,
            confidence,
            evidence: clip(evidence),
        }
    }
}

/// `fault_detail` is a diagnostic breadcrumb, not a log store. A rank-0 CUDA
/// traceback is tens of kilobytes and there is one per task row.
const EVIDENCE_MAX: usize = 400;

fn clip(s: &str) -> String {
    let t = s.trim();
    if t.chars().count() <= EVIDENCE_MAX {
        return t.to_string();
    }
    let mut out: String = t.chars().take(EVIDENCE_MAX).collect();
    out.push('…');
    out
}

/// Map an NVIDIA XID to a class.
///
/// The split that matters is *whose fault it is*: XIDs in the 13/31/43/45
/// family are the driver reporting that a **kernel** misbehaved (illegal
/// address, graphics exception) — that is the job's code, and no amount of
/// moving it to another GPU helps. The ECC/bus/NVLink family is the device
/// itself. Getting this backwards means either draining healthy nodes for a
/// pointer bug, or retrying a dead GPU forever.
///
/// Codes not in the table return `None` — an unlisted XID is not silently
/// filed as infrastructure.
pub fn xid_class(xid: u32) -> Option<FaultClass> {
    use FaultClass::*;
    Some(match xid {
        // Kernel-side faults: the application's problem.
        13 | 31 | 43 | 45 => UserCode,
        // ECC family.
        48 | 63 | 64 | 92 | 94 | 95 => GpuEcc,
        // NVLink / NVSwitch.
        74 | 80 | 81 => Nvlink,
        // The device left the bus / the board fell over.
        79 => GpuFallenOffBus,
        // Firmware/micro-controller wedged; GSP RPC timeouts.
        62 | 119 | 120 | 121 | 122 => GpuUnresponsive,
        // Robust-channel and engine errors the driver attributes to the device.
        61 | 68 | 69 | 93 => GpuXid,
        _ => return None,
    })
}

/// Whether an XID means the node should leave the pool now.
pub fn xid_is_fatal(xid: u32) -> bool {
    xid_class(xid).is_some_and(|c| c.should_drain_node())
}

/// Extract an XID code from a driver line: `NVRM: Xid (PCI:0000:1b:00): 79, ...`
/// or DCGM's `XID 79`. Returns the first code found.
pub fn parse_xid(line: &str) -> Option<u32> {
    let lower = line.to_ascii_lowercase();
    let at = lower.find("xid")?;
    // Walk forward to the first digit run, but stop at a newline so a stray
    // "Xid" in one line cannot borrow a number from the next.
    let rest = &line[at + 3..];
    let mut digits = String::new();
    for ch in rest.chars() {
        if ch == '\n' {
            break;
        }
        if ch.is_ascii_digit() {
            digits.push(ch);
        } else if !digits.is_empty() {
            break;
        } else if ch == ':' || ch == '(' {
            // `Xid (PCI:0000:1b:00): 79` — the PCI BDF is hex and would be read
            // as the code. Skip to after the closing paren when one is present.
            if ch == '(' {
                if let Some(close) = rest.find(')') {
                    return parse_first_number(&rest[close + 1..]);
                }
            }
        }
    }
    digits.parse().ok()
}

fn parse_first_number(s: &str) -> Option<u32> {
    let mut digits = String::new();
    for ch in s.chars() {
        if ch == '\n' {
            break;
        }
        if ch.is_ascii_digit() {
            digits.push(ch);
        } else if !digits.is_empty() {
            break;
        }
    }
    digits.parse().ok()
}

/// One signature in the text classifier: a needle, the class it implies, and
/// how much that single line is worth on its own.
struct Sig(&'static str, FaultClass, Confidence);

/// Ordered most-specific first. Order **is** the policy: `CUDA out of memory`
/// must be tested before a bare `out of memory`, and `checkpoint` corruption
/// before a generic unpickling traceback, or the coarse rule swallows the
/// precise one. Everything here is matched case-insensitively.
const SIGNATURES: &[Sig] = &[
    // ── Unambiguous job-side facts. First, because they are cheap and certain,
    //    and because an OOM on rank 3 also produces a collective timeout on
    //    every other rank — whichever text we are handed, the OOM is the truth.
    Sig("cuda out of memory", FaultClass::GpuOom, Confidence::High),
    Sig("torch.cuda.outofmemoryerror", FaultClass::GpuOom, Confidence::High),
    Sig("out of memory (oom)", FaultClass::HostOom, Confidence::High),
    Sig("oomkilled", FaultClass::HostOom, Confidence::High),
    Sig("killed process", FaultClass::HostOom, Confidence::Medium),
    Sig("out of memory: killed", FaultClass::HostOom, Confidence::High),
    // Divergence. The most expensive thing to retry.
    Sig("loss is nan", FaultClass::NanLoss, Confidence::High),
    Sig("loss became nan", FaultClass::NanLoss, Confidence::High),
    Sig("nan or inf found in input tensor", FaultClass::NanLoss, Confidence::High),
    Sig("found nan in loss", FaultClass::NanLoss, Confidence::High),
    Sig("gradient overflow detected", FaultClass::NanLoss, Confidence::Low),
    // Checkpoint integrity — before any generic unpickling/EOF signature.
    Sig("unexpected end of file while loading checkpoint", FaultClass::CheckpointCorrupt, Confidence::High),
    Sig("invalid load key", FaultClass::CheckpointCorrupt, Confidence::Medium),
    Sig("unpicklingerror", FaultClass::CheckpointCorrupt, Confidence::Medium),
    Sig("checkpoint file is corrupt", FaultClass::CheckpointCorrupt, Confidence::High),
    Sig("central directory not found", FaultClass::CheckpointCorrupt, Confidence::Medium),
    // ── Device. `xid` itself is handled by parse_xid before this table runs.
    Sig("has fallen off the bus", FaultClass::GpuFallenOffBus, Confidence::High),
    Sig("gpu is lost", FaultClass::GpuFallenOffBus, Confidence::High),
    Sig("uncorrectable ecc error", FaultClass::GpuEcc, Confidence::High),
    Sig("double bit ecc", FaultClass::GpuEcc, Confidence::High),
    Sig("uncontained ecc", FaultClass::GpuEcc, Confidence::High),
    Sig("contained ecc error", FaultClass::GpuEcc, Confidence::High),
    Sig("row remap failure", FaultClass::GpuEcc, Confidence::High),
    Sig("ecc error", FaultClass::GpuEcc, Confidence::Medium),
    Sig("nvlink error", FaultClass::Nvlink, Confidence::High),
    Sig("nvlink is down", FaultClass::Nvlink, Confidence::High),
    Sig("nvswitch fatal", FaultClass::Nvlink, Confidence::High),
    Sig("gsp rpc timeout", FaultClass::GpuUnresponsive, Confidence::High),
    Sig("unable to determine the device handle", FaultClass::GpuUnresponsive, Confidence::Medium),
    Sig("cuda error: unspecified launch failure", FaultClass::GpuUnresponsive, Confidence::Medium),
    Sig("cuda error: ecc uncorrectable", FaultClass::GpuEcc, Confidence::High),
    // Two of the most common CUDA failures in training, and both are the job's
    // own kernel: a device-side assert is an assertion the model's code wrote
    // (an out-of-range embedding index is the classic), and an illegal memory
    // access is a bad pointer. The driver reports them, which makes them look
    // like device faults; they are not, and retrying them on five other nodes
    // reproduces them five times. Same split as XID 13/31/43/45 in `xid_class`.
    Sig("device-side assert triggered", FaultClass::UserCode, Confidence::High),
    Sig("an illegal memory access was encountered", FaultClass::UserCode, Confidence::High),
    // ── Fabric.
    Sig("link_down", FaultClass::FabricIb, Confidence::High),
    Sig("port state change: down", FaultClass::FabricIb, Confidence::High),
    Sig("ib link flap", FaultClass::FabricIb, Confidence::High),
    Sig("ibv_", FaultClass::FabricIb, Confidence::Low),
    Sig("transport retry counter exceeded", FaultClass::FabricIb, Confidence::Medium),
    Sig("rdma_", FaultClass::FabricIb, Confidence::Low),
    // ── NCCL. Order matters twice over: the aborts name a transport fault the
    //    library itself detected, and must be tested before the watchdog
    //    timeout, which is only ever a symptom.
    Sig("ncclsystemerror", FaultClass::NcclCommAbort, Confidence::Medium),
    Sig("ncclinternalerror", FaultClass::NcclCommAbort, Confidence::Medium),
    Sig("ncclunhandledcudaerror", FaultClass::NcclCommAbort, Confidence::Medium),
    Sig("nccl warn bootstrap", FaultClass::NcclCommAbort, Confidence::Medium),
    Sig("unhandled system error", FaultClass::NcclCommAbort, Confidence::Low),
    Sig("watchdog caught collective operation timeout", FaultClass::NcclTimeout, Confidence::Low),
    Sig("nccl watchdog thread terminated", FaultClass::NcclTimeout, Confidence::Low),
    Sig("timeout(ms)=", FaultClass::NcclTimeout, Confidence::Low),
    Sig("some nccl operations have failed or timed out", FaultClass::NcclTimeout, Confidence::Low),
    // ── Storage.
    Sig("stale file handle", FaultClass::Storage, Confidence::High),
    Sig("no space left on device", FaultClass::Storage, Confidence::High),
    Sig("input/output error", FaultClass::Storage, Confidence::Medium),
    Sig("lustre", FaultClass::Storage, Confidence::Low),
    Sig("transport endpoint is not connected", FaultClass::Storage, Confidence::Medium),
    // ── Platform.
    Sig("spot instance termination", FaultClass::Preemption, Confidence::High),
    Sig("preempted", FaultClass::Preemption, Confidence::High),
    Sig("instance is scheduled for termination", FaultClass::Preemption, Confidence::High),
    Sig("node_fail", FaultClass::NodeFail, Confidence::High),
    Sig("node failure", FaultClass::NodeFail, Confidence::High),
    Sig("due to time limit", FaultClass::WalltimeExceeded, Confidence::High),
    Sig("job cancelled", FaultClass::Cancelled, Confidence::High),
    // ── Config. Last of the specific rules: these strings appear inside richer
    //    errors, so anything more precise gets first refusal.
    Sig("imagepullbackoff", FaultClass::Config, Confidence::High),
    Sig("errimagepull", FaultClass::Config, Confidence::High),
    Sig("no such file or directory", FaultClass::Config, Confidence::Low),
    Sig("command not found", FaultClass::Config, Confidence::Medium),
    Sig("permission denied", FaultClass::Config, Confidence::Low),
];

/// Classify failure text — a task's stderr, an `sacct` comment, a DCGM line.
///
/// Returns `None` when nothing matched, which is different from
/// [`FaultClass::Unknown`]: `None` means "this text carries no signal", and the
/// caller may have another source to try. A caller with nothing else left
/// records `Unknown`.
///
/// Scans **line by line, first match wins within a line, table order across
/// lines** — so an OOM on line 200 still beats a watchdog timeout on line 1.
/// That inversion is deliberate: the timeout is almost always printed first and
/// is almost never the cause.
pub fn classify_text(text: &str) -> Option<Classification> {
    if text.trim().is_empty() {
        return None;
    }
    let lower = text.to_ascii_lowercase();

    // XID first: a driver-reported code is stronger than any prose, and it
    // carries a number we can map precisely rather than a phrase we matched.
    if lower.contains("xid") {
        if let Some(code) = parse_xid(text) {
            if let Some(class) = xid_class(code) {
                let line = line_containing(text, &lower, "xid").unwrap_or(text);
                return Some(Classification::new(class, Confidence::High, line));
            }
            // A code we do not have a mapping for. Say so as GpuXid with low
            // confidence rather than inventing a disposition for it.
            let line = line_containing(text, &lower, "xid").unwrap_or(text);
            return Some(Classification::new(FaultClass::GpuXid, Confidence::Low, line));
        }
    }

    // Table order is the priority; the first signature that appears anywhere in
    // the text wins, regardless of which line it is on.
    for Sig(needle, class, conf) in SIGNATURES {
        if let Some(line) = line_containing(text, &lower, needle) {
            return Some(Classification::new(*class, *conf, line));
        }
    }
    None
}

/// The original-case line (from `text`) containing `needle` (searched in the
/// pre-lowered `lower`, so the caller lowercases once per call, not once per
/// signature). Byte offsets are shared because `to_ascii_lowercase` is
/// length-preserving — a Unicode-aware lowercase would not be, which is exactly
/// why this uses the ASCII one.
fn line_containing<'a>(text: &'a str, lower: &str, needle: &str) -> Option<&'a str> {
    let at = lower.find(needle)?;
    let start = text[..at].rfind('\n').map(|i| i + 1).unwrap_or(0);
    let end = text[at..].find('\n').map(|i| at + i).unwrap_or(text.len());
    Some(text[start..end].trim())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xid_codes_split_device_faults_from_kernel_faults() {
        // The whole point of the table: 31 is the job's pointer bug, 79 is a
        // dead board. One must never be filed as the other.
        assert_eq!(xid_class(31), Some(FaultClass::UserCode));
        assert_eq!(xid_class(31).unwrap().disposition(), Disposition::Application);
        assert_eq!(xid_class(79), Some(FaultClass::GpuFallenOffBus));
        assert_eq!(
            xid_class(79).unwrap().disposition(),
            Disposition::Infrastructure
        );
        assert!(xid_is_fatal(79));
        assert!(!xid_is_fatal(31));
        // An unlisted code is not guessed into infrastructure.
        assert_eq!(xid_class(200), None);
    }

    #[test]
    fn parses_xid_past_the_pci_bdf() {
        // The BDF is hex and sits between "Xid" and the code; reading the first
        // digit run naively yields 0 (from 0000:1b:00) and misclassifies.
        let line = "NVRM: Xid (PCI:0000:1b:00): 79, pid=12345, GPU has fallen off the bus.";
        assert_eq!(parse_xid(line), Some(79));
        assert_eq!(parse_xid("[1234.5] XID 48 on GPU 3"), Some(48));
        assert_eq!(parse_xid("no code here"), None);
    }

    #[test]
    fn xid_beats_the_prose_around_it() {
        let text = "\
[rank3]: Watchdog caught collective operation timeout
NVRM: Xid (PCI:0000:1b:00): 48, Double Bit ECC Error";
        let c = classify_text(text).unwrap();
        assert_eq!(c.class, FaultClass::GpuEcc);
        assert_eq!(c.confidence, Confidence::High);
        assert!(c.evidence.contains("Xid"));
    }

    #[test]
    fn an_oom_anywhere_beats_a_watchdog_timeout_on_line_one() {
        // This is the real shape of a failed training log: every healthy rank
        // prints the watchdog timeout, and the one rank that actually died
        // prints the OOM hundreds of lines later.
        let mut text = String::from("[rank0]: Watchdog caught collective operation timeout\n");
        for i in 0..200 {
            text.push_str(&format!("step {i} ok\n"));
        }
        text.push_str("[rank3]: torch.cuda.OutOfMemoryError: CUDA out of memory.\n");
        let c = classify_text(&text).unwrap();
        assert_eq!(c.class, FaultClass::GpuOom);
        assert_eq!(c.class.disposition(), Disposition::Application);
    }

    #[test]
    fn a_bare_collective_timeout_is_never_filed_as_infrastructure() {
        // The single most consequential rule in the file. A timeout with no
        // corroboration is Unknown, so the infra retry budget does not apply
        // and a stuck dataloader is not retried onto ten healthy nodes.
        let c = classify_text("[rank0]: Watchdog caught collective operation timeout, timeout(ms)=600000").unwrap();
        assert_eq!(c.class, FaultClass::NcclTimeout);
        assert_eq!(c.class.disposition(), Disposition::Unknown);
        assert_eq!(c.class.precedence(), Precedence::Symptom);
        assert_eq!(c.confidence, Confidence::Low);
        assert!(!c.class.should_drain_node());
    }

    #[test]
    fn nccl_transport_errors_outrank_the_watchdog_line() {
        let text = "\
[rank0]: Watchdog caught collective operation timeout
node-47:1234:1300 [3] NCCL WARN Call to ncclSystemError failed";
        let c = classify_text(text).unwrap();
        assert_eq!(c.class, FaultClass::NcclCommAbort);
        assert_eq!(c.class.disposition(), Disposition::Infrastructure);
    }

    #[test]
    fn a_kernel_fault_the_driver_reports_is_still_the_jobs_fault() {
        // The driver reports these, which makes them read as device faults.
        // They are the model's own code — an out-of-range index, a bad pointer
        // — and retrying them elsewhere reproduces them elsewhere.
        for text in [
            "RuntimeError: CUDA error: device-side assert triggered",
            "CUDA error: an illegal memory access was encountered",
        ] {
            let c = classify_text(text).unwrap();
            assert_eq!(c.class, FaultClass::UserCode, "{text}");
            assert_eq!(c.class.disposition(), Disposition::Application);
            assert!(!c.class.should_drain_node(), "a healthy node stays in the pool");
        }
        // ...and the device-side CUDA errors are still infrastructure.
        let c = classify_text("CUDA error: ECC uncorrectable error encountered").unwrap();
        assert_eq!(c.class, FaultClass::GpuEcc);
        assert_eq!(c.class.disposition(), Disposition::Infrastructure);
    }

    #[test]
    fn cuda_oom_is_not_read_as_a_host_oom() {
        let c = classify_text("RuntimeError: CUDA out of memory. Tried to allocate 2.00 GiB").unwrap();
        assert_eq!(c.class, FaultClass::GpuOom);
        let c = classify_text("Memory cgroup out of memory: Killed process 4242 (python)").unwrap();
        assert_eq!(c.class, FaultClass::HostOom);
    }

    #[test]
    fn checkpoint_corruption_is_its_own_class_not_user_code() {
        let c = classify_text("_pickle.UnpicklingError: invalid load key, '\\x00'.").unwrap();
        assert_eq!(c.class, FaultClass::CheckpointCorrupt);
        // Application disposition: retrying from the same pointer never works.
        assert_eq!(c.class.disposition(), Disposition::Application);
        assert_eq!(c.class.default_budget(), 1);
    }

    #[test]
    fn empty_and_unmatched_text_yield_none_not_a_guess() {
        assert!(classify_text("").is_none());
        assert!(classify_text("   \n  ").is_none());
        assert!(classify_text("everything is fine, exiting 0").is_none());
    }

    #[test]
    fn evidence_is_the_matched_line_and_is_length_capped() {
        let long = format!("prefix {} CUDA out of memory", "x".repeat(2000));
        let c = classify_text(&long).unwrap();
        assert!(c.evidence.chars().count() <= EVIDENCE_MAX + 1);
        assert!(c.evidence.ends_with('…'));
    }

    #[test]
    fn the_public_name_list_matches_the_variant_list() {
        // Two hand-maintained lists that must not drift: the validation error
        // reads FAULT_CLASS_NAMES, and an omission there means the author is
        // told a legal key is illegal.
        let from_variants: Vec<&str> = FaultClass::ALL.iter().map(|c| c.as_str()).collect();
        assert_eq!(from_variants, FAULT_CLASS_NAMES);
    }

    #[test]
    fn class_strings_round_trip_and_unknown_strings_are_tolerated() {
        for c in [
            FaultClass::GpuXid,
            FaultClass::GpuEcc,
            FaultClass::GpuFallenOffBus,
            FaultClass::Nvlink,
            FaultClass::GpuUnresponsive,
            FaultClass::FabricIb,
            FaultClass::NcclCommAbort,
            FaultClass::NcclTimeout,
            FaultClass::Deadlock,
            FaultClass::StragglerRank,
            FaultClass::DataloaderStall,
            FaultClass::HostOom,
            FaultClass::GpuOom,
            FaultClass::NanLoss,
            FaultClass::CheckpointCorrupt,
            FaultClass::UserCode,
            FaultClass::Config,
            FaultClass::Storage,
            FaultClass::Preemption,
            FaultClass::NodeFail,
            FaultClass::WalltimeExceeded,
            FaultClass::Cancelled,
            FaultClass::Unknown,
        ] {
            assert_eq!(FaultClass::parse(c.as_str()), c, "round trip for {c}");
        }
        // A class written by a newer build must not panic an older reader.
        assert_eq!(FaultClass::parse("gpu-melted"), FaultClass::Unknown);
        assert_eq!(FaultClass::parse("GPU_ECC"), FaultClass::GpuEcc);
    }

    #[test]
    fn precedence_orders_root_causes_above_symptoms() {
        assert!(FaultClass::GpuEcc.precedence() > FaultClass::NcclTimeout.precedence());
        assert!(FaultClass::FabricIb.precedence() > FaultClass::NcclCommAbort.precedence());
        assert!(FaultClass::NcclCommAbort.precedence() > FaultClass::NcclTimeout.precedence());
    }

    #[test]
    fn dispositions_carry_asymmetric_budgets() {
        assert_eq!(Disposition::Infrastructure.default_budget(), 5);
        assert_eq!(Disposition::Application.default_budget(), 1);
        assert_eq!(Disposition::Platform.default_budget(), 3);
        // Unknown declines to have an opinion.
        assert_eq!(Disposition::Unknown.default_budget(), 0);
    }
}
