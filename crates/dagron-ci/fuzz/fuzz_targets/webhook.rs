//! A delivery's body, as each kind of event dagron-ci takes: a forge's own
//! bytes, read after its signature and before anything is written. The
//! first byte picks the kind, and the rest is the body.
#![no_main]

use dagron_ci::github;
use libfuzzer_sys::fuzz_target;

const KINDS: [&str; 10] = [
    "push",
    "pull_request",
    "merge_group",
    "workflow_job",
    "check_run",
    "check_suite",
    "ping",
    "delete",
    "create",
    "installation",
];

fuzz_target!(|data: &[u8]| {
    let Some((kind, body)) = data.split_first() else {
        return;
    };
    let _ = github::parse(KINDS[*kind as usize % KINDS.len()], body);
    dagron_ci::fuzzing::open_pulls(body);
});
