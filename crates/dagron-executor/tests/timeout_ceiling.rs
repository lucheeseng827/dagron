//! The task-duration ceiling, against a real process.
//!
//! `executor.rs` unit-tests the two pure halves for every input. What they cannot
//! reach is the part that decides whether the feature exists at all: that the
//! environment variable is spelled the way the operator will spell it, that the
//! `OnceLock` reads it, and that a task asking for an hour is actually killed at
//! the ceiling rather than merely being told a smaller number.
//!
//! Its own test binary on purpose. The ceiling is cached process-wide on first
//! use, so a test that sets it would fix it for every other test sharing the
//! process — this file holds exactly one.

use dagron_executor::executor::run_command;
use std::time::Instant;

/// Well under `DEFAULT_TASK_TIMEOUT_SECS`, so "the ceiling applied" and "the
/// default applied" cannot produce the same result.
const CEILING_SECS: u64 = 2;

#[tokio::test]
async fn a_task_asking_for_an_hour_is_killed_at_the_ceiling() {
    // Before any call, so the OnceLock caches this rather than "unset".
    std::env::set_var("DAGRON_MAX_TASK_TIMEOUT_SECS", CEILING_SECS.to_string());

    let started = Instant::now();
    let err = run_command(
        &["sleep".to_string(), "3600".to_string()],
        Some(3600), // what the workflow asked for
        &[],
    )
    .await
    .expect_err("a task over the ceiling must not run to completion");
    let elapsed = started.elapsed();

    let timed_out = err
        .downcast_ref::<dagron_executor::executor::TimeoutError>()
        .unwrap_or_else(|| panic!("expected a timeout, got: {err}"));

    // The deadline that fired, not just that one did. This is what separates the
    // ceiling from the 25 s default, and it does so without depending on how fast
    // the box is: a regression landing on the default reports 25 here.
    assert_eq!(
        timed_out.secs, CEILING_SECS,
        "the ceiling should be the deadline in force, not the {} s default",
        dagron_executor::executor::DEFAULT_TASK_TIMEOUT_SECS
    );

    // And that the process really was killed at it rather than merely told a
    // smaller number. Below the default on purpose: at `< 30` this assertion
    // passed with the ceiling entirely disabled, because the run then took the
    // 25 s default and 25 is under 30. Loose enough for a shared CI box, tight
    // enough that only the ceiling can satisfy it.
    assert!(
        elapsed.as_secs() < 10,
        "clamped to {CEILING_SECS}s, so this should end in ~{CEILING_SECS}s, took {elapsed:?}"
    );
}
