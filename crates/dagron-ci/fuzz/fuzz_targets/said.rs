//! The lines of a job's log that it says of itself: its tests' report and
//! its steps' summaries. Any of them can be written by the job's own
//! script, and they are read by dagron-ci, outside the job.
#![no_main]

use libfuzzer_sys::fuzz_target;

fuzz_target!(|text: &str| {
    let lines: Vec<&str> = text.lines().collect();
    dagron_ci::fuzzing::said(&lines);
});
