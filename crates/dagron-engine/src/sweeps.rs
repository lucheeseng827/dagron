//! When each of the reconcile loop's sweeps runs: an
//! interval of its own for each (`SWEEP_INTERVALS`), and, for those that are
//! the installation's, one engine a window (`SWEEP_SHARE`, the `sweep_slots`
//! table).
//!
//! A sweep is due by this engine's own clock once its interval has passed
//! since it last ran here, or was left to another engine. The tick asks for
//! the slots of the installation's sweeps that are due, in one statement, and
//! runs those it took and its own; the rest wait a full interval. A slot is
//! taken for nine tenths of the interval, so a lone engine, which asks again
//! one interval after its last claim came back, always finds its own slot
//! due.

use std::collections::BTreeMap;
use std::future::Future;
use std::time::{Duration, Instant};

use anyhow::{bail, Result};
use dagron_core::db::SweepSlot;
use dagron_core::sweep::{Scope, Sweep};

const N: usize = Sweep::ALL.len();

/// The fraction of a sweep's interval its slot is taken for.
const SLOT_SHARE: f64 = 0.9;

/// The least interval a sweep may be set to, as `SWEEP_INTERVAL_MS`'s.
const FLOOR_MS: u64 = 10;

/// The longest interval a sweep may have: a day. One far longer is a slot
/// Postgres cannot write, and the engine would stop at its first claim.
/// `SWEEP_INTERVALS` refuses one longer; `SWEEP_INTERVAL_MS` and
/// `REAP_INTERVAL_MS` longer are a day.
const CEILING_MS: u64 = 86_400_000;

/// When each sweep last ran, how often each runs, and whether the
/// installation's are shared.
pub struct SweepClock {
    every: [Duration; N],
    last: [Option<Instant>; N],
    /// A sweep this engine does not run at all: its part is switched off here
    /// (`DATASET_TRIGGERS=0`, no `DAGRON_INSTALLATION` for the orphans, no
    /// quota wait, no gangs). It is never due, and no slot is asked for it.
    off: [bool; N],
    /// Where each interval was set: `SWEEP_INTERVALS`, a sweep's own
    /// setting, `SWEEP_INTERVAL_MS`, or the default.
    source: [&'static str; N],
    /// The first pass after a start runs every sweep due here, whatever the
    /// slots said: recovery must not wait a window.
    first: bool,
    share: bool,
}

/// What one tick of the loop runs of the sweeps.
#[derive(Debug, Default)]
pub struct Due {
    run: [bool; N],
    /// The turn of each slot this engine took, for its hand-back.
    turn: [Option<i64>; N],
    /// Some sweep was due here, whether it runs here or was left to another.
    any: bool,
}

impl Due {
    /// Whether any sweep was due at this tick, here or elsewhere.
    pub fn any(&self) -> bool {
        self.any
    }

    /// Whether this tick runs `sweep` here.
    pub fn runs(&self, sweep: Sweep) -> bool {
        self.run[sweep as usize]
    }

    /// The turn of `sweep`'s slot where this engine took it.
    pub fn turn(&self, sweep: Sweep) -> Option<i64> {
        self.turn[sweep as usize]
    }
}

impl SweepClock {
    /// The clock as the environment sets it: `SWEEP_INTERVALS` over
    /// `base_ms` (`SWEEP_INTERVAL_MS`), the orphan sweep's and the reap's
    /// intervals from their own settings, and `SWEEP_SHARE`. A setting that
    /// cannot be read stops the start, and says what it may be.
    pub fn from_env(base_ms: u64, orphans: Duration, reap: Duration) -> Result<Self> {
        let set = parse_intervals(&std::env::var("SWEEP_INTERVALS").unwrap_or_default())?;
        let share = parse_share(std::env::var("SWEEP_SHARE").ok().as_deref())?;
        let given = |name: &str| std::env::var_os(name).is_some();
        Ok(Self::new(base_ms, given, &set, orphans, reap, share))
    }

    /// `given` says whether the environment sets the setting it is named:
    /// an interval is said to be its setting's only where it is set.
    pub(crate) fn new(
        base_ms: u64,
        given: impl Fn(&str) -> bool,
        set: &BTreeMap<Sweep, u64>,
        orphans: Duration,
        reap: Duration,
        share: bool,
    ) -> Self {
        // The base and the reap's interval are at most a day, as those of
        // `SWEEP_INTERVALS` are: each is a slot's interval too.
        let base_ms = base_ms.min(CEILING_MS);
        let reap = reap.min(Duration::from_millis(CEILING_MS));
        let mut every = [Duration::ZERO; N];
        let mut source = ["default"; N];
        let own = |s: Sweep| {
            s.own_setting()
                .filter(|name| given(name))
                .unwrap_or("default")
        };
        for s in Sweep::ALL {
            let i = s as usize;
            (every[i], source[i]) = match (set.get(&s), s) {
                (Some(ms), _) => (Duration::from_millis(*ms), "SWEEP_INTERVALS"),
                (None, Sweep::Orphans) => (orphans, own(s)),
                (None, Sweep::Reap) => (reap, own(s)),
                (None, _) if s.own_default_ms().is_none() && given("SWEEP_INTERVAL_MS") => {
                    (Duration::from_millis(base_ms), "SWEEP_INTERVAL_MS")
                }
                (None, _) => (Duration::from_millis(s.default_ms(base_ms)), "default"),
            };
        }
        Self {
            every,
            last: [None; N],
            off: [false; N],
            source,
            first: true,
            share,
        }
    }

    /// Never run `sweep` here: its part is off on this engine.
    pub fn switch_off(&mut self, sweep: Sweep) {
        self.off[sweep as usize] = true;
    }

    /// Whether the installation's sweeps are shared with the other engines.
    pub fn share(&self) -> bool {
        self.share
    }

    /// `sweep`'s interval here.
    pub fn every(&self, sweep: Sweep) -> Duration {
        self.every[sweep as usize]
    }

    /// Where `sweep`'s interval was set.
    pub fn source(&self, sweep: Sweep) -> &'static str {
        self.source[sweep as usize]
    }

    /// Whether `sweep` is switched off here.
    pub fn is_off(&self, sweep: Sweep) -> bool {
        self.off[sweep as usize]
    }

    /// `sweep`'s interval as `/metrics` gives it: 0 where it is switched off
    /// here, so that an alert on a sweep that has not run for some intervals
    /// leaves out one that never runs on this engine.
    pub fn shown_every(&self, sweep: Sweep) -> Duration {
        if self.is_off(sweep) {
            Duration::ZERO
        } else {
            self.every(sweep)
        }
    }

    /// Whether `sweep` is due by this engine's own clock at `now`.
    pub fn due(&self, sweep: Sweep, now: Instant) -> bool {
        let i = sweep as usize;
        !self.off[i]
            && self.last[i].is_none_or(|t| now.saturating_duration_since(t) >= self.every[i])
    }

    /// `sweep` ran at `now`, here or on the engine that took its slot.
    pub fn ran(&mut self, sweep: Sweep, now: Instant) {
        self.last[sweep as usize] = Some(now);
    }

    /// `sweep` read a full batch: it is due again at once.
    pub fn again(&mut self, sweep: Sweep) {
        self.last[sweep as usize] = None;
    }

    /// How long a slot of `sweep` is taken for, in seconds.
    pub fn slot_secs(&self, sweep: Sweep) -> f64 {
        self.every[sweep as usize].as_secs_f64() * SLOT_SHARE
    }

    /// The sweeps a tick at `now` begins with that are due here, in tick
    /// order. The reap is not one: the end of the tick asks for its own slot.
    pub fn due_now(&self, now: Instant) -> Vec<Sweep> {
        Sweep::ALL
            .into_iter()
            .filter(|s| *s != Sweep::Reap && self.due(*s, now))
            .collect()
    }

    /// Of the sweeps `due`, the slots to ask for, with how long each is
    /// taken for: the installation's, where they are shared.
    pub fn slots_for(&self, due: &[Sweep]) -> Vec<(&'static str, f64)> {
        if !self.share {
            return Vec::new();
        }
        due.iter()
            .filter(|s| s.scope() == Scope::Installation)
            .map(|s| (s.name(), self.slot_secs(*s)))
            .collect()
    }

    /// The sweeps a tick that begins at `now` runs here: those due by this
    /// engine's clock, of which the installation's slots are asked for
    /// through `claim` (`db::claim_sweep_slots`) and settled against what it
    /// took. `elsewhere` is told of each left to another engine.
    ///
    /// Each is stamped as run when the claim has come back, not when the tick
    /// began: the database stamped what it took after this engine asked, by
    /// however long the ask took to reach it, so the next ask here, a whole
    /// interval after this stamp, begins at least an interval after that one,
    /// and a lone engine finds its own slot due however much longer this
    /// claim took to arrive than the next.
    pub async fn take<C, F>(
        &mut self,
        now: Instant,
        claim: C,
        elsewhere: impl FnMut(Sweep),
    ) -> Result<Due>
    where
        C: FnOnce(Vec<(&'static str, f64)>) -> F,
        F: Future<Output = Result<Vec<SweepSlot>>>,
    {
        let due = self.due_now(now);
        let wanted = self.slots_for(&due);
        let taken = if wanted.is_empty() {
            Vec::new()
        } else {
            claim(wanted).await?
        };
        Ok(self.settle(&due, &taken, Instant::now(), elsewhere))
    }

    /// Settle the sweeps `due` at `now` against the slots `taken`: which this
    /// tick runs here. Each is stamped as run at `now`, here or elsewhere, so
    /// one left to another engine waits a full interval. The first pass after
    /// a start runs them all. `elsewhere` is told of each left to another.
    pub fn settle(
        &mut self,
        due: &[Sweep],
        taken: &[SweepSlot],
        now: Instant,
        mut elsewhere: impl FnMut(Sweep),
    ) -> Due {
        let mut tick = Due {
            any: !due.is_empty(),
            ..Due::default()
        };
        for s in due {
            let i = *s as usize;
            let slot = taken.iter().find(|slot| slot.name == s.name());
            tick.turn[i] = slot.map(|slot| slot.turn);
            let here = self.first || !self.share || s.scope() == Scope::Engine || slot.is_some();
            tick.run[i] = here;
            if !here {
                elsewhere(*s);
            }
            self.ran(*s, now);
        }
        self.first = false;
        tick
    }
}

/// `SWEEP_INTERVALS`: `name:ms,…`, as `POOLS` is written. A name that is no
/// sweep's, or the orphan sweep's or the reap's (each has its own setting),
/// a value that is no whole number of milliseconds or is above a day, or a
/// sweep named twice, is an error. An interval below 10 ms is 10 ms.
pub fn parse_intervals(raw: &str) -> Result<BTreeMap<Sweep, u64>> {
    let mut set = BTreeMap::new();
    for entry in raw.split(',').map(str::trim).filter(|e| !e.is_empty()) {
        let Some((name, ms)) = entry.split_once(':') else {
            bail!("invalid SWEEP_INTERVALS entry '{entry}': expected name:ms");
        };
        let name = name.trim();
        let Some(sweep) = Sweep::from_name(name) else {
            bail!(
                "SWEEP_INTERVALS names '{name}', which is no sweep; the sweeps are {}",
                settable().join(", ")
            );
        };
        if let Some(own) = sweep.own_setting() {
            bail!("SWEEP_INTERVALS names '{name}', whose interval is {own}");
        }
        let Ok(ms) = ms.trim().parse::<u64>() else {
            bail!(
                "invalid SWEEP_INTERVALS interval for '{name}': '{}'",
                ms.trim()
            );
        };
        if ms > CEILING_MS {
            bail!(
                "SWEEP_INTERVALS interval for '{name}' is {ms} ms: it is at most a day \
                 ({CEILING_MS} ms)"
            );
        }
        if set.insert(sweep, ms.max(FLOOR_MS)).is_some() {
            bail!("SWEEP_INTERVALS names '{name}' twice");
        }
    }
    Ok(set)
}

/// The sweeps `SWEEP_INTERVALS` may name.
fn settable() -> Vec<&'static str> {
    Sweep::ALL
        .into_iter()
        .filter(|s| s.own_setting().is_none())
        .map(Sweep::name)
        .collect()
}

/// `SWEEP_SHARE`: `on` (the default: also unset or empty, `true`, `1` or
/// `yes`) or `off` (also `false`, `0` or `no`), which runs every sweep on
/// every engine as before; trimmed, in any case. Anything else is an error.
pub fn parse_share(raw: Option<&str>) -> Result<bool> {
    match raw.map(|v| v.trim().to_ascii_lowercase()).as_deref() {
        None | Some("") | Some("on") | Some("true") | Some("1") | Some("yes") => Ok(true),
        Some("off") | Some("false") | Some("0") | Some("no") => Ok(false),
        Some(other) => bail!("SWEEP_SHARE is '{other}': it is on or off"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const SECOND: Duration = Duration::from_secs(1);

    fn clock(base_ms: u64, share: bool) -> SweepClock {
        SweepClock::new(
            base_ms,
            |_| false,
            &BTreeMap::new(),
            Duration::from_secs(300),
            Duration::from_secs(5),
            share,
        )
    }

    fn slot(sweep: Sweep, turn: i64) -> SweepSlot {
        SweepSlot {
            name: sweep.name().to_string(),
            turn,
            cursor: None,
        }
    }

    #[test]
    fn sweep_intervals_parse_a_name_and_milliseconds_each() {
        let set = parse_intervals(" waits:250 , approvals:5000,leases:3").unwrap();
        assert_eq!(set[&Sweep::Waits], 250);
        assert_eq!(set[&Sweep::Approvals], 5000);
        assert_eq!(set[&Sweep::Leases], 10, "the floor is 10 ms");
        assert!(parse_intervals("").unwrap().is_empty());
        assert!(parse_intervals(" , ").unwrap().is_empty());

        let unknown = parse_intervals("wiats:250").unwrap_err().to_string();
        assert!(unknown.contains("'wiats'"), "{unknown}");
        assert!(
            unknown.contains("leases") && unknown.contains("gangs"),
            "{unknown}"
        );
        assert!(!unknown.contains("reap"), "{unknown}");
        let own = parse_intervals("reap:100").unwrap_err().to_string();
        assert!(own.contains("REAP_INTERVAL_MS"), "{own}");
        assert!(parse_intervals("orphans:100").is_err());
        assert!(parse_intervals("waits:soon").is_err());
        assert!(parse_intervals("waits:-1").is_err());
        assert!(parse_intervals("waits").is_err());
        assert!(parse_intervals("waits:1,waits:2").is_err());

        // At most a day: one far longer is a slot Postgres cannot write.
        assert_eq!(
            parse_intervals("waits:86400000").unwrap()[&Sweep::Waits],
            86_400_000
        );
        let long = parse_intervals("waits:86400001").unwrap_err().to_string();
        assert!(long.contains("'waits'") && long.contains("a day"), "{long}");
        assert!(parse_intervals(&format!("waits:{}", u64::MAX)).is_err());
    }

    #[test]
    fn sweep_share_is_on_unless_set_off() {
        assert!(parse_share(None).unwrap());
        assert!(parse_share(Some(" ON ")).unwrap());
        assert!(!parse_share(Some("off")).unwrap());
        assert!(!parse_share(Some("0")).unwrap());
        assert!(parse_share(Some("sometimes")).is_err());
    }

    /// With nothing set, each sweep runs as it did before it had an interval
    /// of its own: the base, which is `POLL_INTERVAL_MS` unless
    /// `SWEEP_INTERVAL_MS` says otherwise, and the few that always had their
    /// own cadence keep it.
    #[test]
    fn defaults_are_todays_cadence() {
        let c = clock(500, true);
        for s in Sweep::ALL {
            let want = match s {
                Sweep::DatasetTriggers => 5_000,
                Sweep::QuotaSummary | Sweep::Gangs => 60_000,
                Sweep::Orphans => 300_000,
                Sweep::Reap => 5_000,
                _ => 500,
            };
            assert_eq!(c.every(s), Duration::from_millis(want), "{s:?}");
        }
        assert_eq!(c.source(Sweep::Waits), "default");
        assert_eq!(
            c.source(Sweep::Reap),
            "default",
            "REAP_INTERVAL_MS is not set"
        );
        assert_eq!(c.source(Sweep::Orphans), "default");

        let set = parse_intervals("waits:250").unwrap();
        let given = |name: &str| ["SWEEP_INTERVAL_MS", "REAP_INTERVAL_MS"].contains(&name);
        let c = SweepClock::new(100, given, &set, SECOND, SECOND, true);
        assert_eq!(c.every(Sweep::Waits), Duration::from_millis(250));
        assert_eq!(c.source(Sweep::Waits), "SWEEP_INTERVALS");
        assert_eq!(c.every(Sweep::Leases), Duration::from_millis(100));
        assert_eq!(c.source(Sweep::Leases), "SWEEP_INTERVAL_MS");
        assert_eq!(c.every(Sweep::Gangs), Duration::from_secs(60));
        assert_eq!(c.source(Sweep::Reap), "REAP_INTERVAL_MS");
        assert_eq!(c.source(Sweep::Orphans), "default");
    }

    /// `SWEEP_INTERVAL_MS` and `REAP_INTERVAL_MS` far past a day are a day:
    /// a slot far longer is one Postgres cannot write.
    #[test]
    fn an_interval_is_at_most_a_day() {
        let far = Duration::from_millis(u64::MAX);
        let c = SweepClock::new(u64::MAX, |_| true, &BTreeMap::new(), SECOND, far, true);
        let day = Duration::from_millis(CEILING_MS);
        assert_eq!(c.every(Sweep::Leases), day);
        assert_eq!(c.every(Sweep::Reap), day);
        let wanted = c.slots_for(&Sweep::ALL);
        assert!(!wanted.is_empty());
        for (name, secs) in wanted {
            assert!(secs <= 86_400.0, "{name}: {secs}");
        }
    }

    /// The first pass after a start runs every sweep due here, slot or no
    /// slot, and asks for the installation's slots all the same.
    #[test]
    fn the_first_pass_runs_every_sweep() {
        let mut c = clock(500, true);
        c.switch_off(Sweep::Gangs);
        let now = Instant::now();
        let due = c.due_now(now);
        assert!(due.contains(&Sweep::Leases) && due.contains(&Sweep::Orphans));
        assert!(
            !due.contains(&Sweep::Gangs),
            "a sweep switched off is never due"
        );
        assert!(
            !due.contains(&Sweep::Reap),
            "the reap asks for its slot itself"
        );
        let wanted = c.slots_for(&due);
        assert!(wanted.iter().any(|(name, _)| *name == "leases"));
        assert!(!wanted.iter().any(|(name, _)| *name == "orphans"));

        let mut left = Vec::new();
        let tick = c.settle(&due, &[], now, |s| left.push(s));
        assert!(
            due.iter().all(|s| tick.runs(*s)),
            "the first pass runs them all"
        );
        assert!(left.is_empty());
        assert!(c.due_now(now).is_empty(), "each waits its interval now");
    }

    /// A window another engine took is not run here, and is not asked for
    /// again until a whole interval has passed.
    #[test]
    fn a_sweep_taken_elsewhere_waits_a_full_interval() {
        let mut c = clock(500, true);
        let start = Instant::now();
        c.settle(&c.due_now(start), &[], start, |_| {});

        let now = start + Duration::from_millis(500);
        let due = c.due_now(now);
        assert!(due.contains(&Sweep::Leases) && due.contains(&Sweep::Stops));
        let taken = [slot(Sweep::Waits, 7)];
        let mut left = Vec::new();
        let tick = c.settle(&due, &taken, now, |s| left.push(s));
        assert!(tick.runs(Sweep::Waits) && tick.turn(Sweep::Waits) == Some(7));
        assert!(!tick.runs(Sweep::Leases), "another engine took the window");
        assert!(left.contains(&Sweep::Leases));
        assert!(
            tick.runs(Sweep::Stops),
            "an engine's own sweep needs no slot"
        );
        assert!(!left.contains(&Sweep::Stops));

        assert!(!c.due(Sweep::Leases, now + Duration::from_millis(499)));
        assert!(c.due(Sweep::Leases, now + Duration::from_millis(500)));
    }

    /// A pass that read a full batch is due again on the next tick.
    #[test]
    fn a_full_batch_is_due_again_at_once() {
        let mut c = clock(500, true);
        let now = Instant::now();
        c.settle(&c.due_now(now), &[], now, |_| {});
        assert!(!c.due(Sweep::Waits, now));
        c.again(Sweep::Waits);
        assert!(c.due(Sweep::Waits, now));
        assert!(!c.due(Sweep::Fanouts, now));
    }

    /// With `SWEEP_SHARE=off` no slot is asked for, and every sweep due runs
    /// here, as every engine ran every sweep before.
    #[test]
    fn share_off_runs_everything_here() {
        let mut c = clock(500, false);
        let start = Instant::now();
        c.settle(&c.due_now(start), &[], start, |_| {});
        let now = start + SECOND;
        let due = c.due_now(now);
        assert!(c.slots_for(&due).is_empty());
        let mut left = Vec::new();
        let tick = c.settle(&due, &[], now, |s| left.push(s));
        assert!(due.iter().all(|s| tick.runs(*s)));
        assert!(left.is_empty());
    }

    /// A sweep switched off here shows no interval, so that the alert on a
    /// sweep that has not run leaves it out; the rest show their own.
    #[test]
    fn a_switched_off_sweep_shows_no_interval() {
        let mut c = clock(500, true);
        c.switch_off(Sweep::Gangs);
        c.switch_off(Sweep::Orphans);
        assert_eq!(c.shown_every(Sweep::Gangs), Duration::ZERO);
        assert_eq!(c.shown_every(Sweep::Orphans), Duration::ZERO);
        assert_eq!(c.every(Sweep::Gangs), Duration::from_secs(60), "still set");
        assert_eq!(c.shown_every(Sweep::Leases), Duration::from_millis(500));
        assert_eq!(c.shown_every(Sweep::Reap), Duration::from_secs(5));
    }

    /// A lone engine whose claim was slow to reach the database, and whose
    /// next claim was not, still finds its own slot due at the next ask: the
    /// clock is stamped when the claim came back, after the database stamped
    /// the slot, so the next ask begins a whole interval after that stamp.
    /// Timed from the start of the tick, the second ask would come 70 ms after
    /// the slot was stamped, before its window of 90 ms was out, and find it
    /// taken by this engine itself.
    #[tokio::test]
    async fn a_lone_engine_finds_its_own_slot_due_however_long_its_claim_took() {
        let table = std::sync::Mutex::new(std::collections::HashMap::<String, Instant>::new());
        let mut c = clock(100, true);
        for (pass, delay) in [30u64, 0, 30, 0, 30, 0].into_iter().enumerate() {
            while !c.due(Sweep::Leases, Instant::now()) {
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
            let table = &table;
            let claim = |wanted: Vec<(&'static str, f64)>| async move {
                // The pool and the network between the ask and the database.
                tokio::time::sleep(Duration::from_millis(delay)).await;
                let stamp = Instant::now();
                let mut table = table.lock().unwrap();
                let mut taken = Vec::new();
                for (name, secs) in wanted {
                    if table.get(name).is_none_or(|due| *due <= stamp) {
                        table.insert(name.to_string(), stamp + Duration::from_secs_f64(secs));
                        taken.push(slot(Sweep::from_name(name).unwrap(), pass as i64 + 1));
                    }
                }
                Ok(taken)
            };
            let mut left = Vec::new();
            let tick = c
                .take(Instant::now(), claim, |s| left.push(s))
                .await
                .unwrap();
            assert!(
                tick.runs(Sweep::Leases) && tick.turn(Sweep::Leases).is_some(),
                "pass {pass}: the engine's own slot was not due ({left:?} left elsewhere)"
            );
            assert!(left.is_empty(), "pass {pass}: {left:?}");
        }
    }

    /// A slot is taken for nine tenths of its sweep's interval: an engine
    /// asks again one interval later, and so finds its own slot due.
    #[test]
    fn the_slot_window_is_nine_tenths_of_the_interval() {
        let c = clock(500, true);
        assert!((c.slot_secs(Sweep::Leases) - 0.45).abs() < 1e-9);
        assert!((c.slot_secs(Sweep::DatasetTriggers) - 4.5).abs() < 1e-9);
        let wanted = c.slots_for(&[Sweep::Leases, Sweep::UrlWaits, Sweep::Waits]);
        let names: Vec<&str> = wanted.iter().map(|(name, _)| *name).collect();
        assert_eq!(
            names,
            ["leases", "waits"],
            "the HTTP sensors are claimed row by row"
        );
    }
}
