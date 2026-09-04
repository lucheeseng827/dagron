//! Constrained-host gates: the pressure file (`DAGRON_PRESSURE_FILE`) that
//! pauses new claims while a thermal/battery/maintenance daemon says so, and the
//! free-disk probe behind `DAGRON_MIN_FREE_BYTES`. See docs/CONFIG.md.
//!
//! WHY a file and not a signal or an API call: the things that know a board is
//! too hot, a battery is under 10 %, or a technician has the panel open are
//! host daemons — thermald, a BMS bridge, a maintenance script — and the one
//! interface every one of them already has is `touch` and `rm`. A file is
//! also *state*, not an event: a daemon that restarts mid-throttle re-reads
//! the same verdict, and a gate that survives the gated process's own restart
//! is the only kind worth having on a device nobody is watching.
//!
//! WHAT it gates: claims. Runs are still admitted and queued (the datastore
//! is the buffer), tasks already dispatched finish, recovery sweeps keep
//! running, and the process stays resident — a one-shot `dagron file.yaml`
//! never exits while the pressure file persists, because its runs never
//! drain. Removing the file resumes claims on the next tick with nothing
//! lost.
//!
//! The disk probe is a thin wrapper over `dagron_core::db::free_bytes` — ONE
//! statvfs implementation in the workspace, shared with the datastore's own
//! admission floor, so the headroom the engine logs at boot is the headroom
//! the floor will refuse on.

use std::path::{Path, PathBuf};

use tracing::{info, warn};

/// File bodies that mean "open" even though the file exists — so a daemon can
/// flip the gate by rewriting one byte instead of racing an unlink against its
/// own next write. Matched trimmed and case-insensitively.
const OPEN_SENTINELS: [&str; 4] = ["0", "false", "off", "resume"];

/// Whether the pressure file at `path` is holding claims: the file exists ⇒
/// closed, unless its whole (trimmed) body is one of [`OPEN_SENTINELS`].
///
/// A file that exists but cannot be read — a directory, a permissions
/// mistake, a non-UTF-8 body — counts as **closed**: the daemon that put it
/// there meant something, and a gate that fails open on an unreadable verdict
/// is not a gate. Only "no such file" is open.
pub fn is_closed(path: &Path) -> bool {
    match std::fs::read_to_string(path) {
        Ok(body) => !OPEN_SENTINELS.contains(&body.trim().to_ascii_lowercase().as_str()),
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => false,
        Err(_) => true,
    }
}

/// Bytes available to this process on the filesystem holding `path` — the
/// shared statvfs probe (`dagron_core::db::free_bytes`), re-exported here so
/// the engine's boot report and the datastore's admission floor cannot
/// disagree. Only the SQLite build has a local datastore to report on.
#[cfg_attr(not(feature = "sqlite"), allow(dead_code))]
pub fn free_bytes(path: &Path) -> std::io::Result<u64> {
    dagron_core::db::free_bytes(path)
}

/// `DAGRON_MIN_FREE_BYTES` as the engine reads it for its boot report. The
/// floor itself is enforced inside `dagron_core::db` on the SQLite create
/// path; this read only decides whether there is a headroom line to log.
/// `0` / unset / unparseable = off.
#[cfg_attr(not(feature = "sqlite"), allow(dead_code))]
pub fn min_free_bytes() -> u64 {
    std::env::var("DAGRON_MIN_FREE_BYTES")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(0)
}

/// What one poll of the gate changed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transition {
    /// Same verdict as the last poll.
    Unchanged,
    /// Open → closed: claims just paused.
    Closed,
    /// Closed → open: claims just resumed.
    Opened,
}

/// The claim gate's state: the file to watch and the last verdict, so the
/// *transition* — not every tick — is what gets logged. Warning once per
/// closure (and info once per reopening) is the difference between a log an
/// operator reads and one they filter out.
pub struct PressureGate {
    path: Option<PathBuf>,
    closed: bool,
}

impl PressureGate {
    /// Watch `path`; `None` = no pressure gate (every poll is open).
    pub fn new(path: Option<PathBuf>) -> Self {
        Self { path, closed: false }
    }

    /// From `DAGRON_PRESSURE_FILE` (trimmed; empty = unset).
    pub fn from_env() -> Self {
        let path = std::env::var("DAGRON_PRESSURE_FILE")
            .ok()
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())
            .map(PathBuf::from);
        Self::new(path)
    }

    /// The watched file, if any — for the boot log line.
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// Re-read the file and report whether claims are paused, logging on a
    /// transition. Called once per reconcile tick: one `read_to_string` of a
    /// tiny file (or one `ENOENT`) is nothing next to the tick's own queries.
    pub fn poll(&mut self) -> bool {
        let (closed, transition) = self.observe();
        match (transition, self.path.as_deref()) {
            (Transition::Closed, Some(p)) => warn!(
                path = %p.display(),
                "pressure file present — new task claims paused until it is removed (runs stay queued; in-flight tasks finish)"
            ),
            (Transition::Opened, Some(p)) => {
                info!(path = %p.display(), "pressure file cleared — task claims resumed")
            }
            _ => {}
        }
        closed
    }

    /// The pure state step behind [`poll`](Self::poll): the new verdict and
    /// how it differs from the last one. Split out so the transition logic is
    /// testable without capturing log output.
    pub fn observe(&mut self) -> (bool, Transition) {
        let now = self.path.as_deref().is_some_and(is_closed);
        let transition = match (self.closed, now) {
            (false, true) => Transition::Closed,
            (true, false) => Transition::Opened,
            _ => Transition::Unchanged,
        };
        self.closed = now;
        (now, transition)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_path(tag: &str) -> PathBuf {
        std::env::temp_dir().join(format!("m54-pressure-{tag}-{}", uuid::Uuid::new_v4()))
    }

    #[test]
    fn a_missing_file_is_open() {
        assert!(!is_closed(&temp_path("missing")));
    }

    /// Presence closes the gate whatever the body says — except the four
    /// sentinels, which open it in place so a daemon can flip the verdict by
    /// rewriting one byte. An unreadable "file" (a directory) still closes:
    /// the verdict is unreadable, not absent.
    #[test]
    fn a_present_file_is_closed_unless_it_says_a_sentinel() {
        let p = temp_path("present");
        for body in ["", "1", "thermal: 91C\n", "TRUE", "paused by bms"] {
            std::fs::write(&p, body).unwrap();
            assert!(is_closed(&p), "body {body:?} must close the gate");
        }
        for body in ["0", "false", "off", "resume", "  RESUME \n", "Off"] {
            std::fs::write(&p, body).unwrap();
            assert!(!is_closed(&p), "sentinel {body:?} must open the gate");
        }
        std::fs::remove_file(&p).unwrap();
        std::fs::create_dir(&p).unwrap();
        assert!(is_closed(&p), "a directory at the path is an unreadable verdict, not an open gate");
        std::fs::remove_dir(&p).unwrap();
    }

    /// Each transition is reported exactly once — the closing tick and the
    /// reopening tick — and a steady state (including unlinking an
    /// already-open gate) reports nothing. `poll` is `observe` plus logging.
    #[test]
    fn gate_reports_each_transition_exactly_once() {
        let p = temp_path("gate");
        let mut gate = PressureGate::new(Some(p.clone()));
        assert_eq!(gate.path(), Some(p.as_path()));
        assert_eq!(gate.observe(), (false, Transition::Unchanged), "open at boot, nothing to say");
        std::fs::write(&p, "").unwrap();
        assert_eq!(gate.observe(), (true, Transition::Closed), "the closing tick");
        assert_eq!(gate.observe(), (true, Transition::Unchanged), "…and only that tick");
        std::fs::write(&p, "resume").unwrap();
        assert_eq!(gate.observe(), (false, Transition::Opened));
        assert_eq!(gate.observe(), (false, Transition::Unchanged));
        std::fs::remove_file(&p).unwrap();
        assert_eq!(gate.observe(), (false, Transition::Unchanged), "unlinking an open gate is not a transition");
        std::fs::write(&p, "1").unwrap();
        assert!(gate.poll());
        std::fs::remove_file(&p).unwrap();
        assert!(!gate.poll());
    }

    #[test]
    fn no_path_means_no_gate() {
        let mut gate = PressureGate::new(None);
        assert!(gate.path().is_none());
        for _ in 0..3 {
            assert_eq!(gate.observe(), (false, Transition::Unchanged));
        }
    }

    /// The engine's probe is the datastore's probe: same filesystem, same
    /// figure (to within what a concurrent write can move it), and a missing
    /// path is an error rather than a zero that would read as "full".
    #[test]
    fn free_bytes_is_the_shared_probe() {
        let dir = std::env::temp_dir();
        let ours = free_bytes(&dir).unwrap();
        let core = dagron_core::db::free_bytes(&dir).unwrap();
        assert!(ours > 0);
        assert!(ours.abs_diff(core) < 256 * 1024 * 1024, "ours={ours} core={core}");
        assert!(free_bytes(&temp_path("nope")).is_err());
    }
}
