//! Wall-clock confidence detector for disconnected units: boot plausibility
//! against the datastore, wall-vs-monotonic step detection, and positive sync
//! evidence (`DAGRON_CLOCK_SYNC_FILE`). Publishes into `dagron_core::clock`.
//!
//! WHY this exists: every timestamp dagron writes comes from the process's
//! wall clock. In a datacentre NTP makes that benign; on a gateway that boots
//! with no network and no RTC battery the clock can read 1970 — or last
//! Tuesday — and nothing in the record says so. The engine keeps scheduling
//! regardless (a wrong clock must never stop recovery; the lease arithmetic
//! is a single wall-clock read per claim for exactly that reason), but every
//! run it creates carries a verdict on the clock it was stamped under, so an
//! auditor can tell evidence from a guess.
//!
//! Three signals, cheapest first:
//! * **Boot plausibility.** If `now` is earlier than the newest run already
//!   on disk, the clock is behind the datastore — certain, and free to check.
//! * **Step detection.** Every `DAGRON_CLOCK_CHECK_SECS` the wall clock's
//!   movement is compared with the monotonic clock's over the same interval.
//!   They agree to within scheduler jitter; a disagreement past
//!   `DAGRON_CLOCK_STEP_TOLERANCE_MS` is the wall clock being *set* — by a
//!   time daemon finally reaching a server, by an operator, by a firmware
//!   quirk. The runs in flight straddle that discontinuity and are re-stamped
//!   `drifted`.
//! * **Positive evidence.** A file the host's time daemon maintains (a
//!   `chrony` `-s`-style sync marker, a systemd-timesyncd unit that
//!   `touch`es on sync) is the only thing that can say `synced`; the absence
//!   of a detected problem is not synchronisation. Re-read every tick so the
//!   verdict follows the daemon.
//!
//! Precedence: evidence of sync beats a remembered step beats the boot finding
//! beats nothing (`unknown`). Nothing here gates recovery, claims or leases.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use tracing::{info, warn};

use dagron_core::clock::{self, ClockStatus};
use dagron_core::db;
use dagron_core::metrics::Metrics;

/// `clock_source` spellings, shared with the API docs.
pub const SOURCE_SYNC_FILE: &str = "sync-file";
pub const SOURCE_STEP: &str = "step";
pub const SOURCE_BEHIND_DATASTORE: &str = "behind-datastore";

/// The detector's knobs (all registered in `config::knobs()`).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Config {
    /// Step-check cadence; `0` disables the periodic check (the boot
    /// plausibility probe and one sync-file read still run).
    pub check_secs: u64,
    /// How far wall and monotonic may disagree over one interval before it
    /// counts as a step. Below ~100 ms is scheduler jitter on a loaded board.
    pub step_tolerance_ms: i64,
    /// The positive-evidence file, if configured.
    pub sync_file: Option<PathBuf>,
}

impl Config {
    pub const DEFAULT_CHECK_SECS: u64 = 30;
    pub const DEFAULT_STEP_TOLERANCE_MS: i64 = 1_000;

    pub fn from_env() -> Self {
        Self::parse(
            std::env::var("DAGRON_CLOCK_CHECK_SECS").ok().as_deref(),
            std::env::var("DAGRON_CLOCK_STEP_TOLERANCE_MS").ok().as_deref(),
            std::env::var("DAGRON_CLOCK_SYNC_FILE").ok().as_deref(),
        )
    }

    /// Pure parse of the three knobs: unset / unparseable → the default, a
    /// negative tolerance → `0` (every disagreement is a step), a blank sync
    /// path → none.
    pub fn parse(check_secs: Option<&str>, tolerance_ms: Option<&str>, sync_file: Option<&str>) -> Self {
        Self {
            check_secs: check_secs
                .and_then(|v| v.trim().parse().ok())
                .unwrap_or(Self::DEFAULT_CHECK_SECS),
            step_tolerance_ms: tolerance_ms
                .and_then(|v| v.trim().parse::<i64>().ok())
                .unwrap_or(Self::DEFAULT_STEP_TOLERANCE_MS)
                .max(0),
            sync_file: sync_file.map(str::trim).filter(|p| !p.is_empty()).map(PathBuf::from),
        }
    }

    fn sync_file_present(&self) -> bool {
        self.sync_file.as_deref().is_some_and(|p| p.exists())
    }
}

/// One paired reading of the two clocks: wall time as Unix milliseconds and
/// the monotonic clock as milliseconds since the detector's epoch. Plain
/// integers rather than `DateTime` / `Instant` so the skew arithmetic is a
/// pure function a test can feed synthetic readings to.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Sample {
    pub wall_ms: i64,
    pub mono_ms: i64,
}

impl Sample {
    fn take(epoch: Instant) -> Self {
        Self {
            wall_ms: chrono::Utc::now().timestamp_millis(),
            mono_ms: i64::try_from(epoch.elapsed().as_millis()).unwrap_or(i64::MAX),
        }
    }
}

/// How much further (ms) the wall clock moved than the monotonic clock did
/// between two samples. `0` = they agree; positive = the wall clock jumped
/// forward (was set ahead); negative = it was set back. Saturating, so a
/// synthetic extreme cannot panic the detector.
pub fn step_ms(prev: Sample, cur: Sample) -> i64 {
    cur.wall_ms
        .saturating_sub(prev.wall_ms)
        .saturating_sub(cur.mono_ms.saturating_sub(prev.mono_ms))
}

/// Whether a skew is a step rather than jitter: strictly past the tolerance.
pub fn is_step(skew_ms: i64, tolerance_ms: i64) -> bool {
    skew_ms.saturating_abs() > tolerance_ms.max(0)
}

/// Boot plausibility: how far (ms, negative) `now` sits *behind* the newest
/// run's `created_at`, or `None` when it is within `tolerance_ms`, the store is
/// empty, or the stamp does not parse (an unreadable stamp is not evidence of
/// anything). The sign matches [`step_ms`]: behind is negative.
///
/// The tolerance is not decoration. A datastore can be shared by several
/// schedulers, and two healthy hosts disagree by milliseconds all the time —
/// NTP disciplines to an offset, not to zero. Flagging any negative offset
/// would turn ordinary inter-host dispersion into a `drifted` verdict on every
/// boot, which is how a signal meant to separate evidence from guesswork
/// becomes noise nobody reads.
pub fn behind_datastore_ms(
    now: chrono::DateTime<chrono::Utc>,
    latest_created_at: Option<&str>,
    tolerance_ms: i64,
) -> Option<i64> {
    let latest = chrono::DateTime::parse_from_rfc3339(latest_created_at?)
        .ok()?
        .with_timezone(&chrono::Utc);
    let offset = (now - latest).num_milliseconds();
    (offset < -tolerance_ms.max(0)).then_some(offset)
}

/// The verdict, in precedence order: positive sync evidence, then a
/// remembered step, then the boot finding, then `unknown`. A step is
/// remembered for the life of the process — without evidence the clock was
/// subsequently corrected, "it moved once" is the truthful state — and the
/// sync file, being re-read every tick, is what can lift it.
pub fn verdict(
    sync_file_present: bool,
    last_step_ms: Option<i64>,
    boot_behind_ms: Option<i64>,
) -> ClockStatus {
    if sync_file_present {
        return ClockStatus::synced(SOURCE_SYNC_FILE);
    }
    if let Some(ms) = last_step_ms {
        return ClockStatus::drifted(ms, SOURCE_STEP);
    }
    if let Some(ms) = boot_behind_ms {
        return ClockStatus::drifted(ms, SOURCE_BEHIND_DATASTORE);
    }
    ClockStatus::unknown()
}

/// Run the boot assessment now (awaited, so the first run this process
/// creates is already stamped truthfully), publish it, then spawn the
/// periodic step check. Never fails the boot: a probe error is a warning and
/// the verdict stays `unknown` until evidence arrives.
pub async fn start(pool: db::Pool, metrics: Arc<Metrics>, cfg: Config) {
    let epoch = Instant::now();
    let boot_behind = match db::latest_run_created_at(&pool).await {
        Ok(latest) => {
            behind_datastore_ms(chrono::Utc::now(), latest.as_deref(), cfg.step_tolerance_ms)
        }
        Err(e) => {
            warn!(error = %e, "clock plausibility probe failed — confidence stays unknown until evidence arrives");
            None
        }
    };
    if let Some(ms) = boot_behind {
        warn!(
            behind_ms = -ms,
            "wall clock reads EARLIER than the newest run on disk — clock is behind the datastore"
        );
        // Only the embedded backend re-stamps what is already in flight. A
        // SQLite datastore has one writer, so those runs are this unit's and
        // this verdict is about them. A Postgres datastore is shared by N
        // coordination-free schedulers, and `mark_runs_clock_drifted` is not
        // scoped to a worker — one replica rebooting a few milliseconds behind
        // a peer would overwrite the honest verdict on every running run in
        // the cluster, including runs created under perfectly good clocks.
        // The verdict below still governs everything *this* engine creates.
        #[cfg(feature = "sqlite")]
        restamp_running(&pool, ms, SOURCE_BEHIND_DATASTORE).await;
    }
    let status = verdict(cfg.sync_file_present(), None, boot_behind);
    info!(
        confidence = status.confidence.as_str(),
        source = status.source.as_deref().unwrap_or("—"),
        check_secs = cfg.check_secs,
        step_tolerance_ms = cfg.step_tolerance_ms,
        sync_file = %cfg.sync_file.as_deref().map(|p| p.display().to_string()).unwrap_or_else(|| "—".into()),
        "clock confidence assessed"
    );
    clock::publish(status);

    if cfg.check_secs == 0 {
        info!("clock step check disabled (DAGRON_CLOCK_CHECK_SECS=0) — confidence updates only from the boot probe");
        return;
    }
    tokio::spawn(run(pool, metrics, cfg, epoch, boot_behind));
}

/// Re-stamp the runs in flight `drifted` — best-effort: the verdict is still
/// published for every run created from here on even if this write fails.
///
/// Embedded backend only, at every call site. `mark_runs_clock_drifted` is not
/// scoped to a worker, so on a Postgres datastore shared by N coordination-free
/// schedulers this would overwrite the honest verdict on every running run in
/// the cluster, not just this engine's.
#[cfg(feature = "sqlite")]
async fn restamp_running(pool: &db::Pool, offset_ms: i64, source: &str) {
    match db::mark_runs_clock_drifted(pool, offset_ms, source).await {
        Ok(n) if n > 0 => info!(runs = n, source, "running runs re-stamped clock_confidence=drifted"),
        Ok(_) => {}
        Err(e) => warn!(error = %e, source, "could not re-stamp running runs"),
    }
}

/// The periodic step check. `tokio::time::sleep` paces on the monotonic
/// clock, so a wall-clock step cannot stretch or collapse the interval it is
/// being measured over.
async fn run(
    pool: db::Pool,
    metrics: Arc<Metrics>,
    cfg: Config,
    epoch: Instant,
    boot_behind: Option<i64>,
) {
    let mut prev = Sample::take(epoch);
    let mut last_step: Option<i64> = None;
    loop {
        tokio::time::sleep(Duration::from_secs(cfg.check_secs)).await;
        let cur = Sample::take(epoch);
        let skew = step_ms(prev, cur);
        prev = cur;
        if is_step(skew, cfg.step_tolerance_ms) {
            metrics.inc_clock_steps();
            last_step = Some(skew);
            warn!(
                skew_ms = skew,
                tolerance_ms = cfg.step_tolerance_ms,
                "wall clock stepped against the monotonic clock — clock confidence downgraded"
            );
            // Same reasoning as the boot probe above: an unscoped re-stamp on a
            // shared Postgres would mark peers' runs drifted too.
            #[cfg(feature = "sqlite")]
            restamp_running(&pool, skew, SOURCE_STEP).await;
        }
        let next = verdict(cfg.sync_file_present(), last_step, boot_behind);
        if next != clock::current() {
            info!(
                confidence = next.confidence.as_str(),
                source = next.source.as_deref().unwrap_or("—"),
                offset_ms = ?next.offset_ms,
                "clock confidence changed"
            );
            clock::publish(next);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dagron_core::models::ClockConfidence;

    fn s(wall_ms: i64, mono_ms: i64) -> Sample {
        Sample { wall_ms, mono_ms }
    }

    /// Two clocks that advanced by the same amount show no skew, and jitter
    /// inside the tolerance is not a step.
    #[test]
    fn agreeing_clocks_have_no_step() {
        assert_eq!(step_ms(s(1_000, 0), s(31_000, 30_000)), 0);
        let jitter = step_ms(s(1_000, 0), s(31_040, 30_000));
        assert_eq!(jitter, 40);
        assert!(!is_step(jitter, 1_000));
        assert!(!is_step(-40, 1_000), "jitter is unsigned in effect");
    }

    /// A wall clock set ahead reads as a positive step, one set back as a
    /// negative one, sign preserved (the run record carries it); extremes
    /// saturate rather than panic.
    #[test]
    fn forward_and_backward_steps_are_signed() {
        let ahead = step_ms(s(0, 0), s(35_000, 30_000));
        assert_eq!(ahead, 5_000);
        assert!(is_step(ahead, 1_000));
        let back = step_ms(s(1_700_000_000_000, 0), s(1_600_000_000_000 + 30_000, 30_000));
        assert_eq!(back, -100_000_000_000);
        assert!(is_step(back, 1_000));
        // A synthetic 1970 → 2026 jump and its inverse cannot overflow.
        assert!(is_step(step_ms(s(0, 0), s(i64::MAX, 30_000)), 1_000));
        assert!(is_step(step_ms(s(i64::MAX, 0), s(i64::MIN, 30_000)), 1_000));
    }

    /// The tolerance is a strict bound (a skew exactly at it is jitter), and a
    /// negative tolerance means zero — never "nothing is ever a step".
    #[test]
    fn tolerance_is_strict_and_never_negative() {
        assert!(!is_step(1_000, 1_000));
        assert!(is_step(1_001, 1_000));
        assert!(is_step(1, -5), "a negative tolerance clamps to 0");
        assert!(!is_step(0, -5));
    }

    /// Boot plausibility flags only a clock that reads earlier than the newest
    /// run on disk; a later clock, an empty store, or an unparseable stamp is
    /// not evidence of anything.
    #[test]
    fn boot_probe_flags_only_a_clock_behind_the_datastore() {
        let now = chrono::Utc::now();
        let ahead = (now + chrono::TimeDelta::seconds(90)).to_rfc3339();
        let behind = (now - chrono::TimeDelta::seconds(90)).to_rfc3339();
        let found = behind_datastore_ms(now, Some(&ahead), 1_000).expect("clock is behind");
        assert!((-90_500..=-89_500).contains(&found), "≈ -90 s, got {found}");
        assert_eq!(behind_datastore_ms(now, Some(&behind), 1_000), None, "clock is ahead of the store — plausible");
        assert_eq!(behind_datastore_ms(now, None, 1_000), None, "empty store");
        assert_eq!(behind_datastore_ms(now, Some("not a timestamp"), 1_000), None);

        // Inter-host dispersion is not a verdict: a peer's run stamped a few
        // hundred ms ahead is what two healthy NTP-disciplined hosts look like.
        let peer = (now + chrono::TimeDelta::milliseconds(300)).to_rfc3339();
        assert_eq!(
            behind_datastore_ms(now, Some(&peer), 1_000),
            None,
            "within tolerance — ordinary skew between hosts sharing a datastore"
        );
        assert!(
            behind_datastore_ms(now, Some(&peer), 100).is_some(),
            "past the tolerance it is still reported"
        );
    }

    /// Precedence: positive sync evidence beats a remembered step beats the
    /// boot finding beats nothing — each carrying its own source and offset.
    #[test]
    fn verdict_precedence_is_evidence_then_step_then_boot_then_unknown() {
        assert_eq!(verdict(false, None, None), ClockStatus::unknown());

        let boot = verdict(false, None, Some(-90_000));
        assert_eq!(boot.confidence, ClockConfidence::Drifted);
        assert_eq!(boot.source.as_deref(), Some(SOURCE_BEHIND_DATASTORE));
        assert_eq!(boot.offset_ms, Some(-90_000));

        let step = verdict(false, Some(4_000), Some(-90_000));
        assert_eq!(step.source.as_deref(), Some(SOURCE_STEP), "a step outranks the boot finding");
        assert_eq!(step.offset_ms, Some(4_000));

        let synced = verdict(true, Some(4_000), Some(-90_000));
        assert_eq!(synced.confidence, ClockConfidence::Synced);
        assert_eq!(synced.source.as_deref(), Some(SOURCE_SYNC_FILE));
        assert_eq!(synced.offset_ms, None, "synced is not a measurement");
    }

    /// Knob parsing: defaults when unset or unparseable, `0` disables the
    /// periodic check, whitespace is tolerated, a negative tolerance clamps to
    /// zero, and a blank sync path is no sync file.
    #[test]
    fn config_parses_defaults_and_zero_disables() {
        let d = Config::parse(None, None, None);
        assert_eq!(d.check_secs, Config::DEFAULT_CHECK_SECS);
        assert_eq!(d.step_tolerance_ms, Config::DEFAULT_STEP_TOLERANCE_MS);
        assert_eq!(d.sync_file, None);
        assert!(!d.sync_file_present());

        let c = Config::parse(Some("0"), Some(" 250 "), Some(" /run/chrony/synced "));
        assert_eq!(c.check_secs, 0, "0 disables the step check");
        assert_eq!(c.step_tolerance_ms, 250);
        assert_eq!(c.sync_file.as_deref(), Some(std::path::Path::new("/run/chrony/synced")));

        let c = Config::parse(Some("abc"), Some("-5"), Some("   "));
        assert_eq!(c.check_secs, Config::DEFAULT_CHECK_SECS, "unparseable → default");
        assert_eq!(c.step_tolerance_ms, 0, "negative clamps to 0");
        assert_eq!(c.sync_file, None, "blank path is no file");

        // Presence is re-read live: a file that appears after parse is seen.
        let p = std::env::temp_dir().join(format!("m54-sync-{}", uuid::Uuid::new_v4()));
        let c = Config::parse(None, None, p.to_str());
        assert!(!c.sync_file_present());
        std::fs::write(&p, "").unwrap();
        assert!(c.sync_file_present());
        std::fs::remove_file(&p).unwrap();
        assert!(!c.sync_file_present());
    }
}
