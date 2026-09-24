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
/// Run-**admission** gate (`DAGRON_ADMISSION_FILE`): while the file says closed,
/// the engine refuses to admit NEW runs.
///
/// # How this differs from the pressure file, which is the whole point
///
/// They gate opposite ends and must not be confused:
///
/// | | `DAGRON_PRESSURE_FILE` | `DAGRON_ADMISSION_FILE` |
/// |---|---|---|
/// | Refuses | task **claims** | new **runs** |
/// | Runs already submitted | stay queued, drain later | unaffected, finish normally |
/// | Answer to a submitter | none — the run is accepted | `503` + `Retry-After` |
///
/// A host that is too hot wants work to *wait*; a host that is not permitted to
/// take on new work wants the submitter *told*, so it can go elsewhere. Queueing
/// silently would be the wrong answer to the second question — the backlog would
/// grow for hours and then all land at once when the gate opened.
///
/// # What it never does
///
/// It does not touch in-flight work. Runs already admitted keep running to
/// completion, their tasks keep being claimed and dispatched, sub-workflows they
/// spawn are still created, and recovery sweeps keep running. This is load-bearing
/// for the first caller (a metering agent that has lost its marketplace): refusing
/// new work is a support ticket, while killing an eight-hour training job because
/// a NAT gateway flapped would break the durability claim the product is sold on.
///
/// # Why a file
///
/// The same argument as the pressure gate, and it shares [`is_closed`] so the two
/// cannot drift: a file is *state* rather than an event, so a gate survives the
/// restart of both the process that set it and the process it gates, and every
/// daemon that might set it already has `touch` and `rm`. On a Marketplace
/// appliance the engine is the OSS binary and the metering agent is a separate
/// unit, so there is no in-process seam for them to share even in principle —
/// a file on the state volume is the only interface they both have.
///
/// Absent file ⇒ open. Present ⇒ closed, unless its body is an [`OPEN_SENTINELS`]
/// value. Unreadable ⇒ closed, because whoever wrote it meant something.
#[derive(Debug, Clone, Default)]
pub struct AdmissionGate {
    path: Option<PathBuf>,
}

impl AdmissionGate {
    /// Watch `path`; `None` = no gate (admission always open).
    pub fn new(path: Option<PathBuf>) -> Self {
        Self { path }
    }

    /// From `DAGRON_ADMISSION_FILE` (trimmed; empty = unset).
    pub fn from_env() -> Self {
        let path = std::env::var("DAGRON_ADMISSION_FILE")
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

    /// Is admission currently closed?
    ///
    /// Deliberately stateless and uncached, unlike [`PressureGate::poll`]. That
    /// one is read once per reconcile tick from a single loop and logs
    /// transitions, so it owns state; this is read from three places (the API
    /// handler, the cron firer, the schedule firer) on a path that is about to
    /// open a database transaction. One `read_to_string` of a tiny file — or one
    /// `ENOENT` — is nothing beside `create_run`, and skipping the cache means a
    /// freshly-written verdict takes effect on the very next submission rather
    /// than up to a TTL later.
    pub fn is_closed(&self) -> bool {
        self.path.as_deref().is_some_and(is_closed)
    }
}

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
    /// Absent file ⇒ open. This is the state every deployment that has not opted
    /// in is in, and a fresh state volume before the first write, so it must not
    /// be a refusal.
    #[test]
    fn an_unset_or_absent_gate_admits() {
        assert!(!AdmissionGate::new(None).is_closed(), "unset gate admits");
        let dir = std::env::temp_dir().join(format!("m54_adm_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let g = AdmissionGate::new(Some(dir.join("nope")));
        assert!(!g.is_closed(), "absent file admits");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Present ⇒ closed, and the open sentinels flip it back without an unlink —
    /// so a writer can toggle the gate by rewriting one byte rather than racing
    /// its own next write against a delete.
    #[test]
    fn presence_closes_and_the_open_sentinels_reopen() {
        let dir = std::env::temp_dir().join(format!("m54_adm_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let f = dir.join("admission");
        let g = AdmissionGate::new(Some(f.clone()));

        std::fs::write(&f, "metering_degraded").unwrap();
        assert!(g.is_closed());

        for open in OPEN_SENTINELS {
            std::fs::write(&f, open).unwrap();
            assert!(!g.is_closed(), "{open:?} must reopen admission");
            std::fs::write(&f, format!("  {}\n", open.to_uppercase())).unwrap();
            assert!(!g.is_closed(), "{open:?} is matched trimmed and case-insensitively");
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// An unreadable verdict counts as CLOSED — the same policy as the pressure
    /// file, and for the same reason: whoever put it there meant something, and
    /// a gate that fails open on a verdict it cannot parse is not a gate. Note
    /// this is safe precisely because ABSENCE is open, so an install that never
    /// opted in can never trip it.
    #[test]
    fn an_unreadable_gate_is_closed() {
        let dir = std::env::temp_dir().join(format!("m54_adm_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        // A directory where a file is expected: exists, cannot be read as text.
        let as_dir = dir.join("admission");
        std::fs::create_dir_all(&as_dir).unwrap();
        assert!(AdmissionGate::new(Some(as_dir)).is_closed());
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The two gates are independent: closing admission must not pause claims,
    /// and vice versa. They answer different questions (§AdmissionGate), and a
    /// deployment can arm either without the other.
    #[test]
    fn the_admission_gate_and_the_pressure_gate_do_not_share_state() {
        let dir = std::env::temp_dir().join(format!("m54_adm_{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).unwrap();
        let adm = dir.join("admission");
        let pres = dir.join("pressure");
        std::fs::write(&adm, "closed").unwrap();

        assert!(AdmissionGate::new(Some(adm)).is_closed());
        let mut claims = PressureGate::new(Some(pres));
        assert!(!claims.poll(), "claims stay open");
        let _ = std::fs::remove_dir_all(&dir);
    }

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
