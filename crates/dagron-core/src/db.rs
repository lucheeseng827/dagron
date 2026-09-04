//! Datastore facade.
//!
//! The datastore *is* the scheduler: every query here is a state-machine
//! transition on `task_runs`, and the lease + version + status contract is the
//! whole correctness story. Exactly one backend is compiled in, selected by
//! Cargo feature:
//!
//! * `sqlite` (default) — zero-infra single-node path; optimistic CAS claim.
//! * `postgres` — v2 horizontal scale; `FOR UPDATE SKIP LOCKED` claim +
//!   `LISTEN/NOTIFY` event-driven wake for N coordination-free workers.
//!
//! Both backends expose the identical API — `Pool`, `Waker`, and the `create_run`
//! / `claim_ready` / `mark_task_*` / `is_run_complete` family — so `main.rs` and
//! the reconcile loop are backend-agnostic. Switching backends is a feature flag
//! plus a connection string, exactly as the design intends.

#[cfg(all(feature = "sqlite", feature = "postgres"))]
compile_error!("enable only one DB backend: `sqlite` or `postgres`, not both");

#[cfg(not(any(feature = "sqlite", feature = "postgres")))]
compile_error!("enable a DB backend: build with `--features sqlite` (default) or `--features postgres`");

#[cfg(feature = "sqlite")]
mod sqlite;
#[cfg(feature = "sqlite")]
pub use sqlite::*;

#[cfg(feature = "postgres")]
mod postgres;
#[cfg(feature = "postgres")]
pub use postgres::*;

/// The claim-time task lease window, in seconds: `LEASE_SECS` (default 30,
/// floor 3). Read once and cached — the claim path runs every tick. The floor
/// is 3 — not 1 — because the worker heartbeat renews every floor(lease/3) s
/// with a 1 s minimum: below a 3 s window that minimum interval could no
/// longer fit two missed renewals inside the lease, and the crash-recovery
/// sweep would race healthy tasks.
///
/// This was the hard-coded `+30 seconds` in both backends' claim SQL
/// (docs/HA.md planned the knob; the low-latency profile needs it: a crashed
/// scheduler's task waits out this window before any peer reclaims it, and 30 s
/// is an outage at market open). The lease heartbeat renews by this same value,
/// and the engine derives its renew interval as a third of it, so shortening
/// the window keeps the healthy-task contract: a live task's lease always sits
/// between ⅔ and one full window in the future, and two consecutive missed
/// renewals still leave headroom before the expired-lease sweep can reclaim it.
///
/// Shared by both backends so they can never drift.
pub fn lease_secs() -> i64 {
    static LEASE_SECS: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *LEASE_SECS.get_or_init(|| {
        std::env::var("LEASE_SECS")
            .ok()
            .and_then(|v| v.trim().parse::<i64>().ok())
            .unwrap_or(30)
            .max(3)
    })
}

/// Bytes available to this process on the filesystem holding `path` — the
/// probe behind the SQLite free-disk admission floor (`DAGRON_MIN_FREE_BYTES`)
/// and the engine's constrained-host boot report.
///
/// ONE implementation, here, because two callers need it (the admission
/// check in `db::sqlite` and the engine's `pressure` module) and a second
/// statvfs wrapper is exactly the kind of thing that drifts. This one reports
/// `f_bavail` — blocks available to an *unprivileged* process; root's
/// reserved blocks are not headroom the daemon can use — times `f_frsize`,
/// the fragment size `f_bavail` is counted in (not `f_bsize`).
///
/// `path` may be a directory or a file on the filesystem of interest and must
/// exist. Errors are the OS's (`ENOENT`, `EACCES`, …). Callers on the
/// admission path FAIL OPEN on an error: a probe that cannot run is not
/// evidence that the disk is full.
#[cfg(unix)]
pub fn free_bytes(path: &std::path::Path) -> std::io::Result<u64> {
    use std::os::unix::ffi::OsStrExt;
    let c_path = std::ffi::CString::new(path.as_os_str().as_bytes())?;
    // SAFETY: `statvfs` reads a NUL-terminated path (`CString` guarantees the
    // terminator) and writes into the zeroed, correctly-sized struct passed by
    // pointer; nothing escapes the call.
    let mut st: libc::statvfs = unsafe { std::mem::zeroed() };
    let rc = unsafe { libc::statvfs(c_path.as_ptr(), &mut st) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    // The field types are libc-specific — u64 on glibc x86_64, but u32 /
    // c_ulong on 32-bit and the BSDs — so the widening casts are real on the
    // targets the edge profile is built for (an armv7 musl gateway among
    // them), even though clippy on this host sees them as no-ops.
    #[allow(clippy::unnecessary_cast)]
    let free = (st.f_bavail as u64).saturating_mul(st.f_frsize as u64);
    Ok(free)
}

/// Non-unix hosts have no statvfs; the probe reports `Unsupported` and every
/// caller fails open, exactly as it would on a probe error.
#[cfg(not(unix))]
pub fn free_bytes(_path: &std::path::Path) -> std::io::Result<u64> {
    Err(std::io::Error::new(
        std::io::ErrorKind::Unsupported,
        "free-disk probe (statvfs) is only implemented for unix hosts",
    ))
}

#[cfg(test)]
mod free_bytes_tests {
    use super::free_bytes;

    /// The probe reports real headroom for a filesystem that exists, and the
    /// figure is the unprivileged view — never more than the whole disk.
    #[cfg(unix)]
    #[test]
    fn reports_headroom_for_the_temp_dir() {
        let dir = std::env::temp_dir();
        let free = free_bytes(&dir).expect("temp dir is on a real filesystem");
        assert!(free > 0, "a usable temp dir has some room");
        // A file on the same filesystem probes the same figure (modulo
        // concurrent writes): the caller may hand either.
        let file = dir.join(format!("m54-free-bytes-{}", uuid::Uuid::new_v4()));
        std::fs::write(&file, b"x").unwrap();
        let via_file = free_bytes(&file).unwrap();
        std::fs::remove_file(&file).unwrap();
        assert!(via_file.abs_diff(free) < 256 * 1024 * 1024, "dir={free} file={via_file}");
    }

    /// A path that does not exist is an error, not zero: zero would read as
    /// "full" and make a typo'd datastore path refuse every run.
    #[test]
    fn a_missing_path_is_an_error_not_zero() {
        let missing = std::env::temp_dir().join(format!("m54-no-such-{}", uuid::Uuid::new_v4()));
        assert!(free_bytes(&missing).is_err());
    }
}
