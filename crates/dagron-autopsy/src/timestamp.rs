//! One timestamp parser, shared by every collector.
//!
//! The four sources disagree about time format and, worse, about time *zone*:
//! `sacct` writes `2026-08-26T02:11:03` with no offset (the controller's local
//! clock), DCGM writes either an ISO-8601 string or a Unix epoch depending on
//! how it was exported, NCCL log lines carry whatever the framework's logger
//! was configured with, and IB counters are usually sampled by something that
//! stamps epoch seconds.
//!
//! Correlation is a time-window join, so a systematic offset between two
//! sources does not degrade the answer gracefully — it silently empties the
//! intersection and the tool reports a healthy cluster. Hence: one parser, an
//! explicit "no offset means UTC" rule stated once here and in the docs, and a
//! `--clock-skew-secs` knob on the window rather than a guess per source.

use anyhow::{bail, Result};
use chrono::{DateTime, NaiveDateTime, TimeZone, Utc};

/// Parse a timestamp from any of the forms the collectors encounter.
///
/// In order: RFC-3339 (with offset — believed as written), a bare
/// `YYYY-MM-DDTHH:MM:SS` or `YYYY-MM-DD HH:MM:SS` (**assumed UTC**), and Unix
/// epoch seconds or milliseconds.
///
/// The assume-UTC rule is a decision, not an accident: a naive local timestamp
/// silently reinterpreted is a whole-cluster time shift, and the honest fix is
/// to run the collectors against UTC clocks (or pass the offset in the input),
/// not to have this function guess which zone a hostname implies.
pub fn parse(s: &str) -> Result<DateTime<Utc>> {
    let t = s.trim();
    if t.is_empty() {
        bail!("empty timestamp");
    }
    // Slurm writes these for a job that never started or never ended. They are
    // not parse failures, but they are not times either — the caller decides.
    if matches!(t, "Unknown" | "None" | "N/A" | "NONE") {
        bail!("non-time sentinel '{t}'");
    }
    if let Ok(dt) = DateTime::parse_from_rfc3339(t) {
        return Ok(dt.with_timezone(&Utc));
    }
    for fmt in ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f", "%Y/%m/%d %H:%M:%S%.f"] {
        if let Ok(n) = NaiveDateTime::parse_from_str(t, fmt) {
            return Ok(Utc.from_utc_datetime(&n));
        }
    }
    // Epoch. Milliseconds are distinguished by magnitude: 10 digits is seconds
    // until the year 2286, 13 is milliseconds — the usual heuristic, spelled
    // out because a silent 1000× error here shifts every window by 30 years.
    if let Ok(n) = t.parse::<i64>() {
        let dt = if t.len() >= 13 {
            DateTime::from_timestamp_millis(n)
        } else {
            DateTime::from_timestamp(n, 0)
        };
        if let Some(dt) = dt {
            return Ok(dt);
        }
    }
    // A float epoch, which is what several DCGM exporters emit.
    if let Ok(f) = t.parse::<f64>() {
        if let Some(dt) = DateTime::from_timestamp(f.trunc() as i64, 0) {
            return Ok(dt);
        }
    }
    bail!("unrecognized timestamp '{t}'")
}

/// Parse a Slurm `Elapsed` duration: `[DD-]HH:MM:SS`.
pub fn parse_elapsed_secs(s: &str) -> Option<i64> {
    let t = s.trim();
    if t.is_empty() {
        return None;
    }
    let (days, hms) = match t.split_once('-') {
        Some((d, rest)) => (d.parse::<i64>().ok()?, rest),
        None => (0, t),
    };
    let parts: Vec<&str> = hms.split(':').collect();
    let (h, m, sec): (i64, i64, i64) = match parts.as_slice() {
        [h, m, s] => (h.parse().ok()?, m.parse().ok()?, s.parse().ok()?),
        // Slurm shortens sub-hour durations to MM:SS.
        [m, s] => (0, m.parse().ok()?, s.parse().ok()?),
        _ => return None,
    };
    Some(days * 86_400 + h * 3_600 + m * 60 + sec)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_every_form_the_collectors_hand_us() {
        let expect = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        assert_eq!(parse("2023-11-14T22:13:20Z").unwrap(), expect);
        assert_eq!(parse("2023-11-14T22:13:20").unwrap(), expect, "naive is UTC");
        assert_eq!(parse("2023-11-14 22:13:20").unwrap(), expect);
        assert_eq!(parse("1700000000").unwrap(), expect);
        assert_eq!(parse("1700000000000").unwrap(), expect, "13 digits = millis");
        assert_eq!(parse("1700000000.457").unwrap(), expect);
    }

    #[test]
    fn an_explicit_offset_is_believed_rather_than_reinterpreted() {
        // +08:00 is Johor/Singapore, which is where this gets exercised.
        let dt = parse("2023-11-15T06:13:20+08:00").unwrap();
        assert_eq!(dt, DateTime::from_timestamp(1_700_000_000, 0).unwrap());
    }

    #[test]
    fn slurm_sentinels_are_rejected_rather_than_becoming_the_epoch() {
        // `Unknown` parsed as 0 would put the job's window in 1970 and empty
        // every intersection — a silent "clean cluster" verdict.
        for s in ["Unknown", "None", "N/A", ""] {
            assert!(parse(s).is_err(), "{s} must not parse");
        }
    }

    #[test]
    fn elapsed_covers_the_day_and_short_forms() {
        assert_eq!(parse_elapsed_secs("07:47:38"), Some(28_058));
        assert_eq!(parse_elapsed_secs("2-03:00:00"), Some(183_600));
        assert_eq!(parse_elapsed_secs("04:20"), Some(260));
        assert_eq!(parse_elapsed_secs("garbage"), None);
    }
}
