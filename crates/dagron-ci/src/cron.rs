//! A GitHub workflow's `on.schedule` cron, read as GitHub reads it: five
//! fields (minute, hour, day of the month, month, day of the week), in UTC,
//! as POSIX has them.
//!
//! - A field is `*`, a number, a range (`1-5`), a list of those (`1,3,5`),
//!   and any of them with a step (`*/15`, `0-30/10`). A month or a day of
//!   the week may be named (`JAN`, `MON`), and Sunday is 0 or 7.
//! - Where both the day of the month and the day of the week are restricted
//!   (neither begins with `*`), a day matches either, as POSIX says; a
//!   field that begins with `*` (`*/2`) is no restriction to that rule, as
//!   cron itself (Vixie's, cronie) reads it.
//! - GitHub runs a schedule at most every five minutes: [`Cron::next`] is
//!   never sooner than that after the fire before it.

use std::fmt;

/// The shortest time between two fires of one schedule, as GitHub's.
pub const SHORTEST: i64 = 5 * 60;

/// A cron that was read.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Cron {
    minutes: [bool; 60],
    hours: [bool; 24],
    days: [bool; 32],
    months: [bool; 13],
    weekdays: [bool; 7],
    /// Whether the day of the month and the day of the week were each
    /// restricted (not `*`).
    days_restricted: bool,
    weekdays_restricted: bool,
}

/// Why a cron is not one.
#[derive(Debug, PartialEq, Eq)]
pub struct CronError(pub String);

impl fmt::Display for CronError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

const MONTHS: [&str; 12] = [
    "JAN", "FEB", "MAR", "APR", "MAY", "JUN", "JUL", "AUG", "SEP", "OCT", "NOV", "DEC",
];
const WEEKDAYS: [&str; 7] = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];

impl Cron {
    /// Read `text`: five fields separated by spaces.
    pub fn parse(text: &str) -> Result<Cron, CronError> {
        let fields: Vec<&str> = text.split_whitespace().collect();
        let [minute, hour, day, month, weekday] = fields.as_slice() else {
            return Err(CronError(format!(
                "has {} fields, and a cron has five: minute, hour, day of the month, month, \
                 day of the week",
                fields.len()
            )));
        };
        let mut cron = Cron {
            minutes: [false; 60],
            hours: [false; 24],
            days: [false; 32],
            months: [false; 13],
            weekdays: [false; 7],
            days_restricted: !day.starts_with('*'),
            weekdays_restricted: !weekday.starts_with('*'),
        };
        field(minute, 0, 59, &[], "minute", &mut cron.minutes)?;
        field(hour, 0, 23, &[], "hour", &mut cron.hours)?;
        field(day, 1, 31, &[], "day of the month", &mut cron.days)?;
        field(month, 1, 12, &MONTHS, "month", &mut cron.months)?;
        // Sunday is 0 or 7.
        let mut weekdays = [false; 8];
        field(weekday, 0, 7, &WEEKDAYS, "day of the week", &mut weekdays)?;
        for (d, on) in weekdays.iter().enumerate() {
            if *on {
                cron.weekdays[d % 7] = true;
            }
        }
        Ok(cron)
    }

    /// Whether the minute that begins at `secs` (seconds since the epoch,
    /// UTC) is one the cron fires at.
    pub fn fires_at(&self, secs: i64) -> bool {
        let days = secs.div_euclid(86_400);
        let in_day = secs.rem_euclid(86_400);
        let (_, month, day) = civil(days);
        let weekday = (days + 4).rem_euclid(7) as usize; // 1970-01-01 was a Thursday
        let day_ok = match (self.days_restricted, self.weekdays_restricted) {
            (true, true) => self.days[day as usize] || self.weekdays[weekday],
            (true, false) => self.days[day as usize],
            (false, true) => self.weekdays[weekday],
            (false, false) => true,
        };
        day_ok
            && self.months[month as usize]
            && self.hours[(in_day / 3600) as usize]
            && self.minutes[((in_day % 3600) / 60) as usize]
    }

    /// The first minute after `after` (seconds since the epoch, UTC) the
    /// cron fires at, and no sooner than [`SHORTEST`] after `last`, the fire
    /// before, where there was one. `None` where it fires at none in the
    /// next five years (`0 0 31 2 *`).
    pub fn next(&self, after: i64, last: Option<i64>) -> Option<i64> {
        let from = match last {
            Some(last) => after.max(last + SHORTEST - 1),
            None => after,
        };
        // The minute after `from`'s.
        let mut t = from.div_euclid(60) * 60 + 60;
        let end = t + 5 * 366 * 86_400;
        while t < end {
            let days = t.div_euclid(86_400);
            let (_, month, day) = civil(days);
            let weekday = (days + 4).rem_euclid(7) as usize;
            let day_ok = match (self.days_restricted, self.weekdays_restricted) {
                (true, true) => self.days[day as usize] || self.weekdays[weekday],
                (true, false) => self.days[day as usize],
                (false, true) => self.weekdays[weekday],
                (false, false) => true,
            };
            if !day_ok || !self.months[month as usize] {
                // The next day.
                t = (days + 1) * 86_400;
                continue;
            }
            if self.fires_at(t) {
                return Some(t);
            }
            t += 60;
        }
        None
    }
}

/// One field into `set`, of the values `low..=high`; `names` are the names
/// of its values from `low`, where it has some.
fn field(
    text: &str,
    low: u32,
    high: u32,
    names: &[&str],
    what: &str,
    set: &mut [bool],
) -> Result<(), CronError> {
    let value = |part: &str| -> Result<u32, CronError> {
        if let Some(at) = names.iter().position(|n| n.eq_ignore_ascii_case(part)) {
            // A month's names count from 1, a weekday's from 0.
            return Ok(at as u32 + if what == "month" { 1 } else { 0 });
        }
        let n: u32 = part
            .parse()
            .map_err(|_| CronError(format!("its {what} {part:?} is not a number")))?;
        if n < low || n > high {
            return Err(CronError(format!(
                "its {what} {n} is not between {low} and {high}"
            )));
        }
        Ok(n)
    };
    for item in text.split(',') {
        if item.is_empty() {
            return Err(CronError(format!("its {what} has an empty item")));
        }
        let (range, step) = match item.split_once('/') {
            Some((range, step)) => {
                let step: u32 = step
                    .parse()
                    .ok()
                    .filter(|s| *s > 0)
                    .ok_or_else(|| CronError(format!("its {what} step {step:?} is no step")))?;
                (range, step)
            }
            None => (item, 1),
        };
        let (from, to) = if range == "*" {
            (low, high)
        } else if let Some((a, b)) = range.split_once('-') {
            let (a, b) = (value(a)?, value(b)?);
            if a > b {
                return Err(CronError(format!(
                    "its {what} range {range} runs backwards"
                )));
            }
            (a, b)
        } else {
            let a = value(range)?;
            // `5/15` is 5, 20, 35, 50: from it to the field's end.
            (a, if item.contains('/') { high } else { a })
        };
        let mut v = from;
        while v <= to {
            set[v as usize] = true;
            match v.checked_add(step) {
                Some(next) => v = next,
                None => break,
            }
        }
    }
    Ok(())
}

/// The year, month (1-12) and day (1-31) of `days` since 1970-01-01:
/// Howard Hinnant's `civil_from_days`.
pub(crate) fn civil(days: i64) -> (i64, u32, u32) {
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36_524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = (doy - (153 * mp + 2) / 5 + 1) as u32;
    let m = if mp < 10 { mp + 3 } else { mp - 9 } as u32;
    (if m <= 2 { y + 1 } else { y }, m, d)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2026-10-17T00:00:00Z, a Saturday.
    const SAT: i64 = 1_792_195_200;

    #[test]
    fn the_calendar_is_utc_s() {
        assert_eq!(civil(0), (1970, 1, 1));
        assert_eq!(civil(SAT / 86_400), (2026, 10, 17));
        assert_eq!(civil(11_016), (2000, 2, 29), "a leap day");
    }

    #[test]
    fn a_cron_is_read_as_posix_reads_one() {
        let every_15 = Cron::parse("*/15 * * * *").unwrap();
        assert_eq!(every_15.next(SAT, None), Some(SAT + 15 * 60));
        let weekdays_at_nine = Cron::parse("0 9 * * MON-FRI").unwrap();
        // Saturday: the next is Monday's, at nine.
        assert_eq!(
            weekdays_at_nine.next(SAT, None),
            Some(SAT + 2 * 86_400 + 9 * 3600)
        );
        // Sunday is 0 and 7.
        let sunday = Cron::parse("30 2 * * 7").unwrap();
        assert_eq!(
            sunday.next(SAT, None),
            Some(SAT + 86_400 + 2 * 3600 + 30 * 60)
        );
        assert_eq!(Cron::parse("30 2 * * 0").unwrap(), sunday);
        // Both days restricted: either matches. The 1st of November, or a
        // Monday, whichever comes first: Monday the 19th.
        let either = Cron::parse("0 0 1 * 1").unwrap();
        assert_eq!(either.next(SAT, None), Some(SAT + 2 * 86_400));
        // A month by name, a list, a range with a step.
        let odd = Cron::parse("0-30/10 3,15 * dec *").unwrap();
        assert!(odd.fires_at(SAT + (31 + 30) * 86_400 + 15 * 3600 + 20 * 60));
        assert!(!odd.fires_at(SAT + 15 * 3600 + 20 * 60), "October");
    }

    #[test]
    fn steps_ranges_and_the_ends_of_months_and_years() {
        // `5/15`: from 5 to the field's end.
        let c = Cron::parse("5/15 * * * *").unwrap();
        assert_eq!(c.next(SAT, None), Some(SAT + 5 * 60));
        assert_eq!(c.next(SAT + 5 * 60, None), Some(SAT + 20 * 60));
        // A range up to Sunday as 7.
        let weekend = Cron::parse("0 12 * * 5-7").unwrap();
        assert_eq!(weekend.next(SAT, None), Some(SAT + 12 * 3600), "Saturday");
        // A step past the field's end, and one past any number.
        assert_eq!(
            Cron::parse("0/90 * * * *").unwrap(),
            Cron::parse("0 * * * *").unwrap()
        );
        assert_eq!(
            Cron::parse("5/4294967295 * * * *").unwrap(),
            Cron::parse("5 * * * *").unwrap()
        );
        // The last day of a month, and the new year.
        let last_of_october = Cron::parse("59 23 31 10 *").unwrap();
        assert_eq!(
            last_of_october.next(SAT, None),
            Some(SAT + 14 * 86_400 + 23 * 3600 + 59 * 60)
        );
        let new_year = Cron::parse("0 0 1 1 *").unwrap();
        // 2027-01-01T00:00:00Z.
        assert_eq!(new_year.next(SAT, None), Some(1_798_761_600));
        // A step from `*` in a day field restricts nothing for the either
        // rule: `*/2` days AND Mondays, as cron reads it.
        let even_monday = Cron::parse("0 0 */2 * 1").unwrap();
        // Monday the 19th is an odd day of the month (19 - 1 = 18, a step of
        // 2 from 1): it matches; Tuesday the 20th does not.
        assert!(even_monday.fires_at(SAT + 2 * 86_400));
        assert!(!even_monday.fires_at(SAT + 3 * 86_400));
    }

    #[test]
    fn a_schedule_fires_at_most_every_five_minutes() {
        let every_minute = Cron::parse("* * * * *").unwrap();
        assert_eq!(every_minute.next(SAT, None), Some(SAT + 60));
        assert_eq!(
            every_minute.next(SAT + 60, Some(SAT + 60)),
            Some(SAT + 6 * 60)
        );
    }

    #[test]
    fn what_is_no_cron_says_why() {
        for (text, why) in [
            ("* * * *", "has 4 fields"),
            ("60 * * * *", "its minute 60 is not between 0 and 59"),
            ("* * * 13 *", "its month 13 is not between 1 and 12"),
            ("*/0 * * * *", "is no step"),
            ("5-1 * * * *", "runs backwards"),
            ("x * * * *", "is not a number"),
            ("1,,2 * * * *", "an empty item"),
        ] {
            let e = Cron::parse(text).unwrap_err();
            assert!(e.0.contains(why), "{text}: {e}");
        }
        assert_eq!(Cron::parse("0 0 31 2 *").unwrap().next(SAT, None), None);
    }
}
