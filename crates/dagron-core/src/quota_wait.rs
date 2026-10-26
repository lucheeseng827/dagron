//! Quota back-pressure: when a refused task probes again, and when it stops.
//!
//! A pod the Kubernetes apiserver refuses because the namespace is at its
//! `ResourceQuota` never started. That is not a failed attempt — nothing ran,
//! and running it again in a minute is the whole remedy — so the datastore
//! hands the claim back instead: the row returns to `ready` with a future
//! `scheduled_at` and the attempt the claim consumed is given back. This module
//! decides the two things that transition needs to know: how long until the
//! next probe, and whether the task has waited long enough to give up.
//!
//! It is pure on purpose. Both backends call [`step`] from inside their
//! `requeue_quota_wait` transaction, so the SQLite and Postgres paths cannot
//! come to disagree about the schedule, and the schedule is testable without a
//! datastore.
//!
//! The unit is an **episode**: consecutive refusals of one task with no claim
//! that got further in between. Its clock and its count live on the task row
//! (`quota_wait_since`, `quota_waits`), so for a task at rest in the queue a
//! restart or a hand-over to another scheduler continues the episode instead of
//! restarting the bound. A task whose probe was in flight when its scheduler
//! stopped is the exception: lease recovery returns the row without re-arming
//! the marker, so its next refusal opens a new episode, and, as with any lease
//! recovery, that dispatch's attempt is not given back (see
//! `db::requeue_quota_wait`).

use chrono::{DateTime, TimeDelta, Utc};

/// Delay before the first re-probe, in seconds. Doubles with every refusal in
/// the episode, up to [`RETRY_MAX_SECS`].
pub const RETRY_BASE_SECS: i64 = 5;

/// Ceiling on the un-jittered delay between probes, in seconds. This is what
/// bounds steady-state probe load: N waiters cost the apiserver about N/60 pod
/// creates a second, however long they wait.
pub const RETRY_MAX_SECS: i64 = 60;

/// Characters of the refusal sentence kept on the row (`quota_wait_detail`).
/// It is a breadcrumb for whoever opens the task, not a log store.
pub const DETAIL_MAX_CHARS: usize = 400;

/// The jitter band, in thousandths of the delay: ±25 %.
const JITTER_MIN_PERMILLE: i64 = 750;
const JITTER_MAX_PERMILLE: i64 = 1250;

/// What one refusal does to the episode it belongs to.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Step {
    /// Keep waiting: hand the task back to probe again at `retry_at`.
    Wait {
        /// When the episode began — the first refusal, carried forward.
        since: DateTime<Utc>,
        /// Refusals in the episode, counting this one.
        waits: i64,
        retry_at: DateTime<Utc>,
    },
    /// The episode has lasted `max_wait_secs` or longer: stop waiting.
    GiveUp {
        /// Refusals in the episode, counting this one.
        waits: i64,
        waited_secs: i64,
    },
}

/// Decide what a refusal at `now` does.
///
/// `prior` is the episode this refusal continues — when it began and how many
/// refusals it has seen — or `None` when this is the first. `jitter_permille`
/// comes from [`jitter_permille`]; anything outside its band is pulled into it,
/// so no argument can produce a probe for the instant it was refused.
///
/// The delay is [`RETRY_BASE_SECS`] doubled per prior refusal and capped at
/// [`RETRY_MAX_SECS`] (5, 10, 20, 40, 60, 60, … before jitter). `retry_at` is
/// then clamped to the episode's deadline, so the last probe lands on the
/// deadline itself rather than up to a minute past it — and that probe, if it
/// is refused too, is the one that returns [`Step::GiveUp`].
pub fn step(
    now: DateTime<Utc>,
    prior: Option<(DateTime<Utc>, i64)>,
    max_wait_secs: i64,
    jitter_permille: i64,
) -> Step {
    let (since, prior_waits) = prior.unwrap_or((now, 0));
    let prior_waits = prior_waits.max(0);
    let waits = prior_waits.saturating_add(1);
    // Clamped at 0: a `since` ahead of `now` is a clock that stepped back, and
    // a negative wait would read as "not waiting" to everything downstream.
    let waited_secs = (now - since).num_seconds().max(0);
    if waited_secs >= max_wait_secs {
        return Step::GiveUp { waits, waited_secs };
    }

    // The shift is capped where the delay already is: 5 << 4 is past the
    // ceiling, and an uncapped shift overflows on a long episode.
    let base = (RETRY_BASE_SECS << prior_waits.min(6)).min(RETRY_MAX_SECS);
    let permille = jitter_permille.clamp(JITTER_MIN_PERMILLE, JITTER_MAX_PERMILLE);
    // Seconds times thousandths is milliseconds.
    let retry_at = now + TimeDelta::milliseconds(base * permille);
    // `None` only for a bound too large to be a date at all, which no probe
    // will ever reach — so there is nothing to clamp to.
    let deadline = TimeDelta::try_seconds(max_wait_secs).and_then(|d| since.checked_add_signed(d));
    Step::Wait {
        since,
        waits,
        retry_at: deadline.map_or(retry_at, |d| retry_at.min(d)),
    }
}

/// The jitter for one refusal, in thousandths of the delay: 750..=1250.
///
/// Derived from `(task_id, fence)` rather than drawn: the delay a given claim
/// produces is then the same on every replica and in every test, while the
/// tasks of one burst — refused in the same tick, which is how a namespace
/// fills — still spread out instead of probing again together. The fence moves
/// on every claim, so one task does not keep one offset for a whole episode.
pub fn jitter_permille(task_id: &str, fence: i64) -> i64 {
    // FNV-1a, 64-bit: no dependency and no per-process seed.
    const OFFSET_BASIS: u64 = 0xcbf2_9ce4_8422_2325;
    const PRIME: u64 = 0x0000_0100_0000_01b3;
    let mut hash = OFFSET_BASIS;
    for byte in task_id.bytes().chain(fence.to_le_bytes()) {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(PRIME);
    }
    let span = (JITTER_MAX_PERMILLE - JITTER_MIN_PERMILLE + 1) as u64;
    JITTER_MIN_PERMILLE + (hash % span) as i64
}

/// The refusal sentence as it is stored on the row: trimmed and cut to
/// [`DETAIL_MAX_CHARS`] characters, never through the middle of one.
pub fn clip_detail(detail: &str) -> String {
    detail.trim().chars().take(DETAIL_MAX_CHARS).collect()
}

/// What `db::requeue_quota_wait` did with a refused claim.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum QuotaRequeue {
    /// The task is back in the queue and its attempt was given back.
    Requeued {
        /// Refusals in the episode, counting this one.
        waits: i64,
        /// Seconds since the episode's first refusal.
        waited_secs: i64,
        /// Seconds until the row is claimable again.
        retry_in_secs: i64,
    },
    /// The episode outlasted the bound. **Nothing was written**: the caller
    /// still holds the claim, and fails the task under the same fence.
    Expired {
        /// Refusals in the episode, counting this one.
        waits: i64,
        waited_secs: i64,
    },
    /// The claim was already gone — reclaimed, cancelled, or settled by an
    /// earlier call with this fence. Nothing was written; count nothing.
    Stale,
}

/// Tasks waiting on quota right now, fleet-wide: how many, and when the
/// longest-waiting one was first refused (`quota_wait_since`, RFC-3339).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct QuotaWaitStats {
    pub waiting: i64,
    pub oldest_since: Option<String>,
}

impl QuotaWaitStats {
    /// Seconds the longest-waiting task has been waiting, clamped at 0; 0 when
    /// nothing is waiting.
    pub fn oldest_age_secs(&self, now: DateTime<Utc>) -> i64 {
        self.oldest_since
            .as_deref()
            .and_then(|t| DateTime::parse_from_rfc3339(t).ok())
            .map(|t| (now - t.with_timezone(&Utc)).num_seconds().max(0))
            .unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn at(secs: i64) -> DateTime<Utc> {
        DateTime::from_timestamp(1_800_000_000 + secs, 0).unwrap()
    }

    const SIX_HOURS: i64 = 21_600;

    #[test]
    fn the_first_refusal_opens_an_episode_and_probes_again_in_five_seconds() {
        let now = at(0);
        assert_eq!(
            step(now, None, SIX_HOURS, 1000),
            Step::Wait { since: now, waits: 1, retry_at: at(5) },
        );
    }

    #[test]
    fn the_delay_doubles_to_a_minute_and_stays_there() {
        let since = at(0);
        let now = at(100);
        let delays: Vec<i64> = (0..8)
            .map(|prior_waits| match step(now, Some((since, prior_waits)), SIX_HOURS, 1000) {
                Step::Wait { since: s, waits, retry_at } => {
                    assert_eq!(s, since, "the episode's clock is carried, not restarted");
                    assert_eq!(waits, prior_waits + 1);
                    (retry_at - now).num_seconds()
                }
                other => panic!("still inside the bound, got {other:?}"),
            })
            .collect();
        assert_eq!(delays, vec![5, 10, 20, 40, 60, 60, 60, 60]);
    }

    /// A long episode must not shift its way into an overflow, and a count the
    /// row cannot really hold must not turn the delay negative.
    #[test]
    fn an_absurd_refusal_count_still_waits_a_minute() {
        let now = at(0);
        for prior_waits in [63, 64, 10_000, i64::MAX, -5] {
            let Step::Wait { retry_at, .. } = step(now, Some((now, prior_waits)), SIX_HOURS, 1000)
            else {
                panic!("still inside the bound");
            };
            let delay = (retry_at - now).num_seconds();
            assert!((5..=60).contains(&delay), "prior_waits={prior_waits} gave {delay}s");
        }
    }

    #[test]
    fn jitter_scales_the_delay_and_cannot_leave_its_band() {
        let now = at(0);
        let delay_ms = |permille: i64| match step(now, None, SIX_HOURS, permille) {
            Step::Wait { retry_at, .. } => (retry_at - now).num_milliseconds(),
            other => panic!("still inside the bound, got {other:?}"),
        };
        assert_eq!(delay_ms(750), 3_750);
        assert_eq!(delay_ms(1250), 6_250);
        // Out-of-band arguments are pulled in rather than obeyed: no probe for
        // the instant it was refused, and none a year out.
        assert_eq!(delay_ms(0), 3_750);
        assert_eq!(delay_ms(-1000), 3_750);
        assert_eq!(delay_ms(i64::MAX), 6_250);
    }

    #[test]
    fn jitter_is_deterministic_and_stays_in_its_band() {
        let mut seen = std::collections::BTreeSet::new();
        for task in 0..200 {
            let id = format!("task-{task}");
            for fence in 1..6 {
                let j = jitter_permille(&id, fence);
                assert!((750..=1250).contains(&j), "{id}/{fence} gave {j}");
                assert_eq!(j, jitter_permille(&id, fence), "same claim, same jitter");
                seen.insert(j);
            }
        }
        // A burst refused in one tick has to spread out, and one task must not
        // keep one offset across its claims.
        assert!(seen.len() > 300, "1000 claims landed on only {} offsets", seen.len());
        assert_ne!(jitter_permille("task-0", 1), jitter_permille("task-0", 2));
    }

    #[test]
    fn the_last_probe_lands_on_the_deadline() {
        let since = at(0);
        // 55 s into a 60 s bound, with a minute's delay due.
        let Step::Wait { retry_at, .. } = step(at(55), Some((since, 5)), 60, 1000) else {
            panic!("five seconds of the bound are left");
        };
        assert_eq!(retry_at, at(60), "clamped to since + max_wait, not a minute out");
        // Well inside the bound the clamp does nothing.
        let Step::Wait { retry_at, .. } = step(at(55), Some((since, 0)), SIX_HOURS, 1000) else {
            panic!("inside the bound");
        };
        assert_eq!(retry_at, at(60));
    }

    #[test]
    fn it_gives_up_exactly_at_the_deadline() {
        let since = at(0);
        let one_ms_short = at(60) - TimeDelta::milliseconds(1);
        assert!(
            matches!(step(one_ms_short, Some((since, 4)), 60, 1000), Step::Wait { .. }),
            "the bound has not elapsed yet"
        );
        assert_eq!(
            step(at(60), Some((since, 4)), 60, 1000),
            Step::GiveUp { waits: 5, waited_secs: 60 },
        );
        assert_eq!(
            step(at(75), Some((since, 4)), 60, 1000),
            Step::GiveUp { waits: 5, waited_secs: 75 },
        );
    }

    /// A zero bound waits for nothing. The engine never asks — zero is its off
    /// switch and it takes the failure path before reaching the datastore — but
    /// the answer here must not be "wait forever".
    #[test]
    fn a_zero_bound_gives_up_on_the_first_refusal() {
        assert_eq!(step(at(0), None, 0, 1000), Step::GiveUp { waits: 1, waited_secs: 0 });
    }

    #[test]
    fn a_bound_too_large_to_be_a_date_waits_without_panicking() {
        let now = at(0);
        assert_eq!(
            step(now, None, i64::MAX, 1000),
            Step::Wait { since: now, waits: 1, retry_at: at(5) },
        );
    }

    /// `since` ahead of `now` is a clock that stepped back between two
    /// refusals. The episode keeps waiting on its normal schedule.
    #[test]
    fn a_clock_that_stepped_back_does_not_end_the_episode() {
        let Step::Wait { since, waits, retry_at } = step(at(0), Some((at(30), 2)), 60, 1000) else {
            panic!("nothing has elapsed");
        };
        assert_eq!((since, waits, retry_at), (at(30), 3, at(20)));
    }

    #[test]
    fn the_detail_is_cut_at_400_characters_not_bytes() {
        assert_eq!(clip_detail("  pod refused by quota \n"), "pod refused by quota");
        // 3-byte scalars: a byte cut at 400 would land inside one.
        let long = "€".repeat(DETAIL_MAX_CHARS + 50);
        let clipped = clip_detail(&long);
        assert_eq!(clipped.chars().count(), DETAIL_MAX_CHARS);
        assert_eq!(clipped.len(), DETAIL_MAX_CHARS * 3);
        let exact = "x".repeat(DETAIL_MAX_CHARS);
        assert_eq!(clip_detail(&exact), exact, "exactly at the cap is kept whole");
    }

    #[test]
    fn the_oldest_wait_has_an_age_and_no_waiter_has_none() {
        let now = at(500);
        let stats = QuotaWaitStats { waiting: 2, oldest_since: Some(at(380).to_rfc3339()) };
        assert_eq!(stats.oldest_age_secs(now), 120);
        assert_eq!(QuotaWaitStats::default().oldest_age_secs(now), 0);
        let garbled = QuotaWaitStats { waiting: 1, oldest_since: Some("yesterday".into()) };
        assert_eq!(garbled.oldest_age_secs(now), 0);
    }
}
