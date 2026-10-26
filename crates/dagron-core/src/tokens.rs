//! The model tokens a task spent: a task says so on a line of its own output,
//! `dagron::tokens=<n>`, and the engine takes that line out of the output
//! before it keeps it. In every build: what a task printed reads the same to
//! `{{ tasks.X.output }}`, `when:` and `env_from` whether or not the build
//! keeps the meter.
//!
//! The meter, which keeps what each run spent and holds a run, and the runs
//! it starts, to its `budget: { tokens }`, is the enterprise build's. It
//! believes what a task reports: a budget that a task's own code cannot talk
//! its way around is kept by a gateway in front of the model, by a claim of
//! the run's identity.

/// What a task prints, at the start of a line of its own, before the number
/// of tokens it spent.
pub const REPORT: &str = "dagron::tokens=";

/// The most tokens the meter counts of one thing: a task's `tokens:`, a
/// `budget: { tokens }`, a report, and what a run has spent or reserved. A
/// report of more is kept as this, and what is spent stops at it, so that the
/// meter's sums stay well within a signed 64-bit integer whatever a task
/// prints (about 2.8 × 10^14, far past any budget).
pub const MOST: u64 = 1 << 48;

/// A task's report taken out of its output. Every line that starts with
/// [`REPORT`] is removed, and the last of them that says a number is what it
/// spent; `None` where none does.
pub fn take_report(output: &mut String) -> Option<u64> {
    if !output.contains(REPORT) {
        return None;
    }
    let mut spent = None;
    let mut kept = String::with_capacity(output.len());
    for line in output.split_inclusive('\n') {
        match line.trim_end_matches(['\n', '\r']).strip_prefix(REPORT) {
            Some(n) => spent = n.trim().parse().ok().or(spent),
            None => kept.push_str(line),
        }
    }
    *output = kept;
    spent
}

#[cfg(feature = "enterprise")]
include!("tokens_ee.rs");

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_report_is_taken_out_and_the_rest_is_kept_as_it_was() {
        let mut output = "the answer\ndagron::tokens=120\n".to_string();
        assert_eq!(take_report(&mut output), Some(120));
        assert_eq!(output, "the answer\n");

        // The last that says a number; every report line goes.
        let mut output =
            "dagron::tokens=5\na\ndagron::tokens=oops\r\nb\ndagron::tokens=7".to_string();
        assert_eq!(take_report(&mut output), Some(7));
        assert_eq!(output, "a\nb\n");

        // None at all, or only one in the middle of a line: nothing changes.
        let mut output = "say dagron::tokens=9\n".to_string();
        assert_eq!(take_report(&mut output), None);
        assert_eq!(output, "say dagron::tokens=9\n");
        let mut output = String::from("plain");
        assert_eq!(take_report(&mut output), None);
        assert_eq!(output, "plain");
    }
}
