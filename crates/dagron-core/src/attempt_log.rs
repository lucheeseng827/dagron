//! How much of a superseded attempt's output to keep.
//!
//! `task_runs.output` is singular. Every writer overwrites it, so the log has
//! only ever shown the most recent attempt: for a `repeat:` loop that is 1 of N
//! iterations, and for an ordinary retry it is the attempt that passed rather
//! than the ones that explain why it needed to.
//!
//! Keeping them all is not affordable, and the reason is worth stating because
//! it decides the shape of everything here. Retaining whole attempts multiplies
//! three quantities and **none of them is bounded**: executor output has no cap
//! on the write path, `RepeatSpec.max_iterations` is a `u32`, and
//! `GC_RETENTION_SECS` is unset — GC off — by default. So this module keeps a
//! bounded **tail** of each superseded attempt, capped per attempt and per
//! task, which turns the added storage into a product of two constants that are
//! known before the run starts:
//!
//! ```text
//! added bytes ≤ iterating_tasks × keep × bytes
//! ```
//!
//! Measured at the defaults, that is 228 KiB for a 1000-iteration loop and
//! **zero** for a task that neither loops nor retries — the cost is paid only
//! by the tasks whose logs are currently wrong. `docs/ITERATION-LOGS.md` has
//! the measurements and the rest of the reasoning.

/// Tail bytes kept per superseded attempt when `DAGRON_ATTEMPT_LOG_BYTES` is
/// unset. A screenful and then some: enough to see what an iteration printed
/// and why `until` did not hold, without deciding to be a log store.
pub const DEFAULT_BYTES: usize = 4096;

/// Superseded attempts kept per task when `DAGRON_ATTEMPT_LOG_KEEP` is unset.
pub const DEFAULT_KEEP: usize = 50;

/// Hard cap on how many attempts a single read returns.
///
/// Retention is normally bounded by `DAGRON_ATTEMPT_LOG_KEEP`, but that knob
/// accepts `0` for "unlimited" — which would otherwise make the read path
/// unbounded too, loading, transforming and serializing every row a long poll
/// ever wrote. A ceiling here is not a second retention policy: the rows are
/// still on disk, and the response says so by starting above attempt 1.
///
/// The newest are kept, matching `tail` in the log filter and the retention
/// window itself — a loop is diagnosed from where it stopped.
pub const MAX_READ: usize = 200;

/// Ceiling on `DAGRON_ATTEMPT_LOG_BYTES`. The knob exists to trade a little
/// disk for a readable loop history; letting it be set to a value that makes
/// one iteration cost a megabyte would re-create the unbounded case this module
/// exists to avoid, one environment variable at a time.
pub const MAX_BYTES: usize = 64 * 1024;

/// Why an attempt ended — recorded on the row because "attempt 4 of a loop
/// whose condition had not held yet" and "attempt 4 of something that keeps
/// crashing" read identically otherwise.
///
/// Deliberately not the task's `status` vocabulary: the task is not terminal
/// when either of these is written, and a row saying `failed` next to a task
/// that went on to succeed is a worse lie than no row at all.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AttemptEnd {
    /// A `repeat:` pass finished successfully but `until` did not hold yet.
    Iteration,
    /// The attempt errored and another one is being scheduled.
    Failed,
}

impl AttemptEnd {
    pub fn as_str(self) -> &'static str {
        match self {
            AttemptEnd::Iteration => "iteration",
            AttemptEnd::Failed => "failed",
        }
    }
}

/// The retention policy in force, read from the environment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Policy {
    /// Tail bytes per superseded attempt. `0` disables retention entirely.
    pub bytes: usize,
    /// Superseded attempts kept per task. `0` = unlimited.
    pub keep: usize,
}

impl Default for Policy {
    fn default() -> Self {
        Policy { bytes: DEFAULT_BYTES, keep: DEFAULT_KEEP }
    }
}

impl Policy {
    /// Read the knobs. Per call rather than cached, matching
    /// [`crate::expand`]'s task ceiling: an operator who has run out of disk
    /// should be able to turn this off with a restart and no rebuild.
    ///
    /// An unparseable value falls back to the default rather than erroring.
    /// This is a log-retention knob, not a correctness one, and refusing to
    /// start the engine over a typo in it would be the worse failure.
    pub fn from_env() -> Self {
        let bytes = read("DAGRON_ATTEMPT_LOG_BYTES")
            .map(|n| n.min(MAX_BYTES))
            .unwrap_or(DEFAULT_BYTES);
        let keep = read("DAGRON_ATTEMPT_LOG_KEEP").unwrap_or(DEFAULT_KEEP);
        Policy { bytes, keep }
    }

    /// Whether anything is retained at all. `false` is a complete opt-out: no
    /// table write, no extra transaction work, byte-for-byte the consumption
    /// this feature did not exist.
    pub fn enabled(self) -> bool {
        self.bytes > 0
    }

    /// The slice of `output` to store, and whether anything was dropped.
    ///
    /// Keeps the **end**, matching `tail` in the log filter — a loop that ran
    /// out of iterations is diagnosed from where it stopped, not from where it
    /// started. Cuts forward to a character boundary, so the stored tail is
    /// always valid UTF-8 and is never longer than the cap.
    pub fn tail(self, output: &str) -> (&str, bool) {
        if output.len() <= self.bytes {
            return (output, false);
        }
        let mut cut = output.len() - self.bytes;
        while cut < output.len() && !output.is_char_boundary(cut) {
            cut += 1;
        }
        (&output[cut..], true)
    }

    /// The highest attempt number that may be **deleted** after `attempt` has
    /// been stored, or `None` when nothing falls out of the window.
    ///
    /// A range delete on the primary key rather than a "keep the newest N"
    /// subquery: `attempt` only ever increases for a given task, so the rows to
    /// evict are exactly a prefix, and one `attempt <= n` is an index seek
    /// instead of a scan per iteration of every loop.
    pub fn evict_below(self, attempt: i64) -> Option<i64> {
        if self.keep == 0 {
            return None;
        }
        let keep = i64::try_from(self.keep).unwrap_or(i64::MAX);
        let cutoff = attempt.saturating_sub(keep);
        (cutoff >= 1).then_some(cutoff)
    }
}

fn read(key: &str) -> Option<usize> {
    std::env::var(key).ok()?.trim().parse::<usize>().ok()
}



#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_short_output_is_stored_whole_and_not_marked_truncated() {
        let p = Policy { bytes: 16, keep: 5 };
        assert_eq!(p.tail("hello"), ("hello", false));
        // Exactly at the cap is still whole — the boundary belongs to "kept".
        assert_eq!(p.tail("0123456789abcdef"), ("0123456789abcdef", false));
    }

    #[test]
    fn the_tail_is_kept_not_the_head() {
        let p = Policy { bytes: 4, keep: 5 };
        let (kept, truncated) = p.tail("abcdefghij");
        assert_eq!(kept, "ghij", "the end is what says why the loop stopped");
        assert!(truncated);
    }

    #[test]
    fn the_cut_never_splits_a_character() {
        // 4 bytes of a 3-byte scalar: the cut has to move forward, which means
        // storing *less* than the cap rather than invalid UTF-8.
        let s = "aa€€"; // 2 + 3 + 3 = 8 bytes
        let p = Policy { bytes: 4, keep: 5 };
        let (kept, truncated) = p.tail(s);
        assert_eq!(kept, "€", "moved forward to the boundary");
        assert!(kept.len() <= 4, "never exceeds the cap");
        assert!(truncated);
        assert!(std::str::from_utf8(kept.as_bytes()).is_ok());
    }

    #[test]
    fn zero_bytes_is_a_complete_opt_out() {
        assert!(!Policy { bytes: 0, keep: 50 }.enabled());
        assert!(Policy::default().enabled());
    }

    #[test]
    fn eviction_is_a_prefix_and_only_once_the_window_is_full() {
        let p = Policy { bytes: 4096, keep: 3 };
        // Nothing to drop until more than `keep` attempts exist.
        assert_eq!(p.evict_below(1), None);
        assert_eq!(p.evict_below(3), None);
        // Attempt 4 stored → attempt 1 falls out.
        assert_eq!(p.evict_below(4), Some(1));
        assert_eq!(p.evict_below(10), Some(7));
    }

    #[test]
    fn keep_zero_means_unlimited_and_never_evicts() {
        let p = Policy { bytes: 4096, keep: 0 };
        assert_eq!(p.evict_below(1_000_000), None);
    }

    #[test]
    fn a_huge_keep_cannot_underflow_into_deleting_everything() {
        let p = Policy { bytes: 4096, keep: usize::MAX };
        assert_eq!(p.evict_below(5), None, "saturating, not wrapping");
    }

    #[test]
    fn the_byte_knob_cannot_widen_past_the_compiled_ceiling() {
        // Same rule as DAGRON_MAX_TASKS_PER_RUN: the knob tightens a bound or
        // loosens it within a compiled limit, and never past it.
        temp_env("DAGRON_ATTEMPT_LOG_BYTES", Some("999999999"), || {
            assert_eq!(Policy::from_env().bytes, MAX_BYTES);
        });
        temp_env("DAGRON_ATTEMPT_LOG_BYTES", Some("not a number"), || {
            assert_eq!(Policy::from_env().bytes, DEFAULT_BYTES, "a typo is not fatal");
        });
        temp_env("DAGRON_ATTEMPT_LOG_BYTES", Some("0"), || {
            assert!(!Policy::from_env().enabled(), "0 still turns it off");
        });
    }

    /// `std::env::set_var` is process-global and the db tests read these keys on
    /// the write path, so this takes [`env_lock`] and restores what it found.
    fn temp_env(key: &str, val: Option<&str>, f: impl FnOnce()) {
        let _g = crate::env_lock();
        let prev = std::env::var(key).ok();
        match val {
            Some(v) => unsafe { std::env::set_var(key, v) },
            None => unsafe { std::env::remove_var(key) },
        }
        f();
        match prev {
            Some(v) => unsafe { std::env::set_var(key, v) },
            None => unsafe { std::env::remove_var(key) },
        }
    }
}
