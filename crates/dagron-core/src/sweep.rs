//! The reconcile loop's sweeps: each by name, whose work it is, and how often
//! it runs where nothing says otherwise.
//!
//! In this crate, and not the engine's, because the metrics registry renders
//! a series of each sweep by its name.

/// Whose work a sweep does.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Scope {
    /// The installation's: rows any engine may act on, where two engines
    /// acting at once change nothing a second time. One engine a window runs
    /// it, the one that takes its slot (`sweep_slots`), where `SWEEP_SHARE`
    /// is on.
    Installation,
    /// This engine's own: its executions, its daemon's workloads, its
    /// capacity, its settings, or remote calls it claims row by row. Every
    /// engine runs its own.
    Engine,
}

impl Scope {
    pub fn as_str(self) -> &'static str {
        match self {
            Scope::Installation => "installation",
            Scope::Engine => "engine",
        }
    }
}

/// One of the reconcile loop's sweeps, in the order a tick runs them.
///
/// A sweep is the installation's ([`Scope::Installation`]) only where two
/// engines running it at once is safe: a slot is not a lock, and an engine
/// whose tick stalls after it took one may still be at it when the next
/// window goes to another. What must happen once, a schedule's fire or a
/// cron entry, stays on the leadership lease (docs/HA.md §2).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub enum Sweep {
    Orphans,
    Leases,
    Sessions,
    Stops,
    QuotaSummary,
    Deadlines,
    StoppedRuns,
    Admissions,
    Alerts,
    Approvals,
    Subworkflows,
    Fanouts,
    Waits,
    UrlWaits,
    ExternalPolls,
    ExternalCancels,
    DatasetWaits,
    DatasetTriggers,
    Teardowns,
    Gangs,
    Reap,
}

impl Sweep {
    /// Every sweep, in the order of the enum, so `ALL[s as usize] == s`.
    pub const ALL: [Sweep; 21] = [
        Sweep::Orphans,
        Sweep::Leases,
        Sweep::Sessions,
        Sweep::Stops,
        Sweep::QuotaSummary,
        Sweep::Deadlines,
        Sweep::StoppedRuns,
        Sweep::Admissions,
        Sweep::Alerts,
        Sweep::Approvals,
        Sweep::Subworkflows,
        Sweep::Fanouts,
        Sweep::Waits,
        Sweep::UrlWaits,
        Sweep::ExternalPolls,
        Sweep::ExternalCancels,
        Sweep::DatasetWaits,
        Sweep::DatasetTriggers,
        Sweep::Teardowns,
        Sweep::Gangs,
        Sweep::Reap,
    ];

    /// Its name: in `SWEEP_INTERVALS`, in the `sweep` label of its series,
    /// and as its row's key in `sweep_slots`.
    pub fn name(self) -> &'static str {
        match self {
            Sweep::Orphans => "orphans",
            Sweep::Leases => "leases",
            Sweep::Sessions => "sessions",
            Sweep::Stops => "stops",
            Sweep::QuotaSummary => "quota_summary",
            Sweep::Deadlines => "deadlines",
            Sweep::StoppedRuns => "stopped_runs",
            Sweep::Admissions => "admissions",
            Sweep::Alerts => "alerts",
            Sweep::Approvals => "approvals",
            Sweep::Subworkflows => "subworkflows",
            Sweep::Fanouts => "fanouts",
            Sweep::Waits => "waits",
            Sweep::UrlWaits => "url_waits",
            Sweep::ExternalPolls => "external_polls",
            Sweep::ExternalCancels => "external_cancels",
            Sweep::DatasetWaits => "dataset_waits",
            Sweep::DatasetTriggers => "dataset_triggers",
            Sweep::Teardowns => "teardowns",
            Sweep::Gangs => "gangs",
            Sweep::Reap => "reap",
        }
    }

    /// The sweep of that name.
    pub fn from_name(name: &str) -> Option<Sweep> {
        Sweep::ALL.into_iter().find(|s| s.name() == name)
    }

    /// Whose work it does.
    ///
    /// The engine's: the orphan sweep lists its own daemon's workloads; the
    /// stops are of its own executions; the quota summary and the gang check
    /// read its own settings; the teardowns are claimed for its own capacity
    /// and runner classes; the HTTP sensors and the external polls and
    /// cancels are claimed row by row already, so that each remote call is
    /// made once whichever engine makes it.
    pub fn scope(self) -> Scope {
        match self {
            Sweep::Orphans
            | Sweep::Stops
            | Sweep::QuotaSummary
            | Sweep::UrlWaits
            | Sweep::ExternalPolls
            | Sweep::ExternalCancels
            | Sweep::Teardowns
            | Sweep::Gangs => Scope::Engine,
            Sweep::Leases
            | Sweep::Sessions
            | Sweep::Deadlines
            | Sweep::StoppedRuns
            | Sweep::Admissions
            | Sweep::Alerts
            | Sweep::Approvals
            | Sweep::Subworkflows
            | Sweep::Fanouts
            | Sweep::Waits
            | Sweep::DatasetWaits
            | Sweep::DatasetTriggers
            | Sweep::Reap => Scope::Installation,
        }
    }

    /// The setting a sweep's interval is read from where it has one of its
    /// own, older than `SWEEP_INTERVALS`, which then does not name it.
    pub fn own_setting(self) -> Option<&'static str> {
        match self {
            Sweep::Orphans => Some("DAGRON_ORPHAN_SWEEP_SECS"),
            Sweep::Reap => Some("REAP_INTERVAL_MS"),
            _ => None,
        }
    }

    /// The cadence a sweep always had of its own, in milliseconds, where it
    /// had one; the rest ran at `SWEEP_INTERVAL_MS`.
    pub fn own_default_ms(self) -> Option<u64> {
        match self {
            Sweep::DatasetTriggers => Some(5_000),
            Sweep::QuotaSummary | Sweep::Gangs => Some(60_000),
            Sweep::Orphans => Some(300_000),
            Sweep::Reap => Some(5_000),
            _ => None,
        }
    }

    /// Its interval where nothing sets it, in milliseconds: its own cadence,
    /// or `base_ms` (`SWEEP_INTERVAL_MS`), as every sweep ran before each had
    /// an interval.
    pub fn default_ms(self, base_ms: u64) -> u64 {
        self.own_default_ms().unwrap_or(base_ms)
    }

    /// Whether a pass can say how late it was: what it acted on fell due at
    /// a time it reads, a sensor's, a deadline's or a gate's.
    pub fn has_lateness(self) -> bool {
        matches!(
            self,
            Sweep::Deadlines | Sweep::Alerts | Sweep::Approvals | Sweep::Waits | Sweep::UrlWaits
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A sweep's index into a series' array is its place in `ALL`.
    #[test]
    fn every_sweep_is_in_all_at_its_own_place() {
        for (i, s) in Sweep::ALL.into_iter().enumerate() {
            assert_eq!(s as usize, i, "{s:?}");
            assert_eq!(Sweep::from_name(s.name()), Some(s));
        }
        let mut names: Vec<&str> = Sweep::ALL.iter().map(|s| s.name()).collect();
        names.sort_unstable();
        names.dedup();
        assert_eq!(names.len(), Sweep::ALL.len(), "two sweeps share a name");
    }

    /// The defaults are the cadence each sweep had before: the base for
    /// most, and the intervals the few with their own always had.
    #[test]
    fn the_defaults_are_the_cadence_each_had() {
        for s in Sweep::ALL {
            let want = match s {
                Sweep::DatasetTriggers => 5_000,
                Sweep::QuotaSummary | Sweep::Gangs => 60_000,
                Sweep::Orphans => 300_000,
                Sweep::Reap => 5_000,
                _ => 500,
            };
            assert_eq!(s.default_ms(500), want, "{s:?}");
        }
    }

    /// What must run once is not on a slot, and neither is what is the
    /// engine's own.
    #[test]
    fn the_sweeps_of_one_engine_are_not_shared() {
        for s in [
            Sweep::Orphans,
            Sweep::Stops,
            Sweep::QuotaSummary,
            Sweep::Teardowns,
            Sweep::Gangs,
            Sweep::UrlWaits,
            Sweep::ExternalPolls,
            Sweep::ExternalCancels,
        ] {
            assert_eq!(s.scope(), Scope::Engine, "{s:?}");
        }
        assert_eq!(Sweep::Reap.scope(), Scope::Installation);
        assert_eq!(Sweep::Leases.scope(), Scope::Installation);
    }
}
