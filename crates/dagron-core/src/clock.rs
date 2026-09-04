//! Process-wide clock confidence (`synced` / `drifted` / `unknown`) stamped on
//! every run at creation so a record written under an unsynced clock says so.
//!
//! WHY a process-global rather than a handle: the stamp is written inside
//! `db::create_run_inner`, which is reached from the ingest actor, the ops
//! API, the schedule / cron / backfill loops, sub-workflow triggers and
//! dataset fires — six call sites across three crates. Threading a value
//! through every one of them would touch every submit path for something
//! that has exactly one writer (the engine's detector,
//! `dagron-engine/src/clock.rs`) and changes a handful of times per process
//! lifetime. One `RwLock` behind [`publish`] / [`current`] keeps the stamp a
//! single line at every writer, and lets a build with no detector at all —
//! the API gateway, the GitOps worker, a lean daemon — stamp `unknown`
//! truthfully instead of NULL.
//!
//! The datastore never gates anything on this. Recovery, claims and leases
//! run identically under every confidence; the value is evidence about the
//! timestamps, not policy about the work.

use std::sync::RwLock;

use crate::models::ClockConfidence;

/// What the process currently believes about its own wall clock: the verdict,
/// the measured offset behind it (ms, signed — negative when the clock was
/// set back or found behind the datastore), and the evidence that produced
/// it (`sync-file` / `step` / `behind-datastore`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClockStatus {
    pub confidence: ClockConfidence,
    pub offset_ms: Option<i64>,
    pub source: Option<String>,
}

impl ClockStatus {
    /// The default: nothing has assessed the clock. `const` so it can seed
    /// the global without an initializer running.
    pub const fn unknown() -> Self {
        Self { confidence: ClockConfidence::Unknown, offset_ms: None, source: None }
    }

    /// Positive evidence of synchronisation from `source`. Carries no offset:
    /// "synced" is a statement about the present, not a measurement.
    pub fn synced(source: impl Into<String>) -> Self {
        Self { confidence: ClockConfidence::Synced, offset_ms: None, source: Some(source.into()) }
    }

    /// The clock was found wrong by `offset_ms`, according to `source`.
    pub fn drifted(offset_ms: i64, source: impl Into<String>) -> Self {
        Self {
            confidence: ClockConfidence::Drifted,
            offset_ms: Some(offset_ms),
            source: Some(source.into()),
        }
    }
}

impl Default for ClockStatus {
    fn default() -> Self {
        Self::unknown()
    }
}

/// The one process-wide status. A poisoned lock (a writer panicked mid-store)
/// is recovered rather than propagated: the stored value is a plain struct
/// that cannot be half-written, and a create path must never fail because a
/// detector task died.
static STATUS: RwLock<ClockStatus> = RwLock::new(ClockStatus::unknown());

/// Replace the published status. The engine's detector is the only intended
/// writer; it publishes on every change of verdict, not every tick.
pub fn publish(status: ClockStatus) {
    *STATUS.write().unwrap_or_else(|e| e.into_inner()) = status;
}

/// The status as of now — what `db::create_run` stamps on the next run.
pub fn current() -> ClockStatus {
    STATUS.read().unwrap_or_else(|e| e.into_inner()).clone()
}

/// Serializes the tests (in this crate) that publish into the global: the
/// datastore tests assert on what a run was stamped with, and two of them
/// publishing concurrently would race each other's `create_run`.
#[cfg(test)]
pub(crate) static TEST_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

#[cfg(test)]
mod tests {
    use super::*;

    /// The constructors shape the status the datastore stamps: `unknown`
    /// carries nothing, `synced` carries only its evidence, `drifted` carries
    /// the signed measurement too.
    #[test]
    fn constructors_shape_the_status() {
        let u = ClockStatus::unknown();
        assert_eq!(u, ClockStatus::default());
        assert_eq!(u.confidence, ClockConfidence::Unknown);
        assert_eq!((u.offset_ms, u.source), (None, None));

        let s = ClockStatus::synced("sync-file");
        assert_eq!(s.confidence, ClockConfidence::Synced);
        assert_eq!(s.offset_ms, None, "synced is not a measurement");
        assert_eq!(s.source.as_deref(), Some("sync-file"));

        let d = ClockStatus::drifted(-90_000, "behind-datastore");
        assert_eq!(d.confidence, ClockConfidence::Drifted);
        assert_eq!(d.offset_ms, Some(-90_000), "sign is preserved");
        assert_eq!(d.source.as_deref(), Some("behind-datastore"));
    }

    /// `publish` replaces the whole status and `current` reads it back;
    /// the default before any publish is `unknown`.
    #[test]
    fn publish_replaces_and_current_reads_back() {
        let _guard = TEST_LOCK.lock().unwrap_or_else(|e| e.into_inner());
        publish(ClockStatus::unknown());
        assert_eq!(current(), ClockStatus::unknown());

        publish(ClockStatus::drifted(1_500, "step"));
        assert_eq!(current(), ClockStatus::drifted(1_500, "step"));

        // A later publish replaces every field — a stale offset never survives
        // a verdict that has none.
        publish(ClockStatus::synced("sync-file"));
        let now = current();
        assert_eq!(now.confidence, ClockConfidence::Synced);
        assert_eq!(now.offset_ms, None);

        publish(ClockStatus::unknown());
        assert_eq!(current(), ClockStatus::unknown());
    }
}
