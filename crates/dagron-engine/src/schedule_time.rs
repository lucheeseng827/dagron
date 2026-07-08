//! Timezone-aware cron fire-time computation (fast-win #5).
//!
//! The single source of truth for "when does this schedule fire next", shared by
//! the file-cron loop ([`crate::cron`]), the DB-schedule loop
//! ([`crate::schedule`]), and the auto-backfill catch-up sweep
//! ([`crate::backfill`]). A schedule carries an IANA timezone (e.g.
//! `America/New_York`); its cron expression is interpreted in that zone so a
//! "02:00 daily" job keeps firing at 02:00 wall-clock across DST transitions,
//! and every fire time is returned in UTC (the datastore's canonical form).
//!
//! An empty or `"UTC"` zone is plain UTC — the historical behavior — so existing
//! schedules are unaffected.

use std::collections::BTreeMap;

use chrono::{DateTime, Datelike, Timelike, Utc};
use chrono_tz::Tz;
use cron::Schedule;

/// Strictly parse an IANA timezone name. Empty / whitespace → UTC. Returns an
/// error on an unknown name — used at the write path (dagron-api) to reject a
/// bad timezone with a 400 rather than silently storing it.
pub fn validate_tz(tz: &str) -> anyhow::Result<Tz> {
    let name = tz.trim();
    if name.is_empty() {
        return Ok(Tz::UTC);
    }
    name.parse::<Tz>().map_err(|_| {
        anyhow::anyhow!("unknown timezone '{tz}' (use an IANA name like 'America/New_York')")
    })
}

/// Lenient parse for the engine's fire loops: the value was validated when the
/// schedule was written, so a row that somehow holds a bad zone degrades to UTC
/// (with the caller free to log) rather than wedging the loop.
pub fn parse_tz_or_utc(tz: &str) -> Tz {
    validate_tz(tz).unwrap_or(Tz::UTC)
}

/// Next fire strictly after `after` for an already-parsed schedule, interpreting
/// the schedule in `tz`; the result is converted back to UTC. `None` when the
/// schedule has no upcoming fire.
pub fn next_fire_in_tz(schedule: &Schedule, tz: Tz, after: DateTime<Utc>) -> Option<DateTime<Utc>> {
    schedule
        .after(&after.with_timezone(&tz))
        .next()
        .map(|d| d.with_timezone(&Utc))
}

/// Parse `cron_expr` + `tz`, then return the next fire strictly after `after`
/// (in UTC). Errors on a malformed cron expression or an unknown timezone.
pub fn next_fire_after(
    cron_expr: &str,
    tz: &str,
    after: DateTime<Utc>,
) -> anyhow::Result<Option<DateTime<Utc>>> {
    use std::str::FromStr;
    let schedule = Schedule::from_str(cron_expr)
        .map_err(|e| anyhow::anyhow!("invalid cron '{cron_expr}': {e}"))?;
    let tz = validate_tz(tz)?;
    Ok(next_fire_in_tz(&schedule, tz, after))
}

// ── Schedule gates: `when:` (per-fire conditional) + `stopStrategy` ─────────────

/// Build the calendar context a `when:` gate is evaluated against, computed in
/// the schedule's timezone so `weekday`/`hour`/`day` match the operator's locale.
/// Exposed variables (all integers unless noted):
///   `scheduled_time` (RFC-3339), `hour` (0–23), `minute` (0–59), `day` (1–31),
///   `month` (1–12), `weekday` (1=Mon … 7=Sun), `day_of_year` (1–366),
///   `days_in_month` (28–31) — so e.g. `{{ day }} == {{ days_in_month }}` is
///   "last day of month" and `{{ weekday }} <= 5` is "weekdays only".
pub fn gate_context(scheduled_utc: DateTime<Utc>, tz: Tz) -> BTreeMap<String, String> {
    let local = scheduled_utc.with_timezone(&tz);
    let mut ctx = BTreeMap::new();
    ctx.insert("scheduled_time".to_string(), scheduled_utc.to_rfc3339());
    ctx.insert("hour".to_string(), local.hour().to_string());
    ctx.insert("minute".to_string(), local.minute().to_string());
    ctx.insert("day".to_string(), local.day().to_string());
    ctx.insert("month".to_string(), local.month().to_string());
    ctx.insert(
        "weekday".to_string(),
        local.weekday().number_from_monday().to_string(),
    );
    ctx.insert("day_of_year".to_string(), local.ordinal().to_string());
    ctx.insert(
        "days_in_month".to_string(),
        days_in_month(local.year(), local.month()).to_string(),
    );
    ctx
}

/// Evaluate a `when:` gate: substitute `ctx`, then evaluate the comparison with
/// the same engine as task-level `when:`. `Ok(true)` fires, `Ok(false)` skips;
/// `Err` on a malformed expression (the caller fires-on-error and logs, so a
/// typo never silently stops a pipeline).
pub fn passes_when(when_expr: &str, ctx: &BTreeMap<String, String>) -> anyhow::Result<bool> {
    let rendered = dagron_core::expand::substitute(when_expr, ctx);
    dagron_core::expand::eval_when(&rendered)
}

/// Evaluate a `stopStrategy` expression against a schedule's run outcome counts.
/// `succeeded`/`failed`/`total` are substituted, then the comparison is
/// evaluated: `Ok(true)` means the schedule should auto-stop. `Err` on a
/// malformed expression (the caller keeps running and logs).
pub fn should_stop(
    stop_expr: &str,
    succeeded: i64,
    failed: i64,
    total: i64,
) -> anyhow::Result<bool> {
    let mut ctx = BTreeMap::new();
    ctx.insert("succeeded".to_string(), succeeded.to_string());
    ctx.insert("failed".to_string(), failed.to_string());
    ctx.insert("total".to_string(), total.to_string());
    let rendered = dagron_core::expand::substitute(stop_expr, &ctx);
    dagron_core::expand::eval_when(&rendered)
}

/// Days in a given month (28–31), used for "last day of month" gates.
fn days_in_month(year: i32, month: u32) -> u32 {
    let (y, m) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    chrono::NaiveDate::from_ymd_opt(y, m, 1)
        .and_then(|d| d.pred_opt())
        .map(|d| d.day())
        .unwrap_or(28)
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::TimeZone;
    use std::str::FromStr;

    #[test]
    fn empty_and_utc_are_utc() {
        assert_eq!(validate_tz("").unwrap(), Tz::UTC);
        assert_eq!(validate_tz("  ").unwrap(), Tz::UTC);
        assert_eq!(validate_tz("UTC").unwrap(), Tz::UTC);
    }

    #[test]
    fn unknown_timezone_is_rejected() {
        let err = validate_tz("Mars/Olympus_Mons").unwrap_err().to_string();
        assert!(err.contains("unknown timezone"), "got: {err}");
        // …but the lenient path never panics.
        assert_eq!(parse_tz_or_utc("Mars/Olympus_Mons"), Tz::UTC);
    }

    #[test]
    fn fires_at_local_wall_clock_across_dst() {
        // "02:00 every day" in New York. In winter (EST, UTC-5) that is 07:00 UTC;
        // in summer (EDT, UTC-4) it is 06:00 UTC — the same wall clock, a
        // different UTC instant. A naive UTC schedule could not express this.
        let sched = Schedule::from_str("0 0 2 * * *").unwrap();
        let ny: Tz = "America/New_York".parse().unwrap();

        // Just after 2025-01-10 00:00 UTC → next NY-02:00 is 2025-01-10 07:00 UTC.
        let winter_after = Utc.with_ymd_and_hms(2025, 1, 10, 0, 0, 0).unwrap();
        let winter = next_fire_in_tz(&sched, ny, winter_after).unwrap();
        assert_eq!(winter, Utc.with_ymd_and_hms(2025, 1, 10, 7, 0, 0).unwrap());

        // Just after 2025-07-10 00:00 UTC → next NY-02:00 is 2025-07-10 06:00 UTC.
        let summer_after = Utc.with_ymd_and_hms(2025, 7, 10, 0, 0, 0).unwrap();
        let summer = next_fire_in_tz(&sched, ny, summer_after).unwrap();
        assert_eq!(summer, Utc.with_ymd_and_hms(2025, 7, 10, 6, 0, 0).unwrap());

        // The convenience wrapper agrees, and UTC is unchanged from today's behavior.
        let utc_next = next_fire_after("0 0 2 * * *", "UTC", winter_after)
            .unwrap()
            .unwrap();
        assert_eq!(
            utc_next,
            Utc.with_ymd_and_hms(2025, 1, 10, 2, 0, 0).unwrap()
        );
    }

    #[test]
    fn bad_cron_is_an_error() {
        assert!(next_fire_after("not a cron", "UTC", Utc::now()).is_err());
    }

    #[test]
    fn gate_context_computes_calendar_fields_in_tz() {
        // 2025-01-31 08:30 UTC is 2025-01-31 03:30 in New York (a Friday).
        let t = Utc.with_ymd_and_hms(2025, 1, 31, 8, 30, 0).unwrap();
        let ny: Tz = "America/New_York".parse().unwrap();
        let ctx = gate_context(t, ny);
        assert_eq!(ctx["hour"], "3");
        assert_eq!(ctx["day"], "31");
        assert_eq!(ctx["month"], "1");
        assert_eq!(ctx["weekday"], "5"); // Friday, ISO Mon=1
        assert_eq!(ctx["days_in_month"], "31");
    }

    #[test]
    fn when_gate_last_day_of_month_and_weekdays() {
        let utc = Tz::UTC;
        // Jan 31 is the last day → "last day of month" gate fires.
        let last = gate_context(Utc.with_ymd_and_hms(2025, 1, 31, 0, 0, 0).unwrap(), utc);
        assert!(passes_when("{{ day }} == {{ days_in_month }}", &last).unwrap());
        // Jan 30 is not → the gate skips.
        let not_last = gate_context(Utc.with_ymd_and_hms(2025, 1, 30, 0, 0, 0).unwrap(), utc);
        assert!(!passes_when("{{ day }} == {{ days_in_month }}", &not_last).unwrap());
        // Saturday (2025-02-01) fails a "weekdays only" gate; Monday passes.
        let sat = gate_context(Utc.with_ymd_and_hms(2025, 2, 1, 0, 0, 0).unwrap(), utc);
        assert!(!passes_when("{{ weekday }} <= 5", &sat).unwrap());
        let mon = gate_context(Utc.with_ymd_and_hms(2025, 2, 3, 0, 0, 0).unwrap(), utc);
        assert!(passes_when("{{ weekday }} <= 5", &mon).unwrap());
    }

    #[test]
    fn stop_strategy_evaluates_outcome_counts() {
        // "stop after 1 success": trips once succeeded reaches 1.
        assert!(!should_stop("{{ succeeded }} >= 1", 0, 0, 0).unwrap());
        assert!(should_stop("{{ succeeded }} >= 1", 1, 0, 3).unwrap());
        // "stop after 3 failures".
        assert!(!should_stop("{{ failed }} >= 3", 5, 2, 7).unwrap());
        assert!(should_stop("{{ failed }} >= 3", 5, 3, 8).unwrap());
        // Malformed expression surfaces as an error (caller keeps running).
        assert!(should_stop("{{ nope }} >= 1", 0, 0, 0).is_err());
    }

    #[test]
    fn days_in_month_edges() {
        assert_eq!(days_in_month(2025, 2), 28); // non-leap Feb
        assert_eq!(days_in_month(2024, 2), 29); // leap Feb
        assert_eq!(days_in_month(2025, 12), 31); // December (year rollover path)
        assert_eq!(days_in_month(2025, 4), 30);
    }
}
