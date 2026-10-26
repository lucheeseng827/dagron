//! A repository's `.dagron/ci.yaml`: which workflow files run, and what
//! their labels and actions mean here.
#![no_main]

use dagron_ci::pipeline::github;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|text: &str| {
    let _ = github::settings(text);
});
