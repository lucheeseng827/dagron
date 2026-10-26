//! What remote runners said of their tasks, applied by this engine as
//! it applies a result of its own workers.
//!
//! A runner takes a task through dagron-api, runs it, and says how it came
//! out; dagron-api keeps that word (`runner_results`) and wakes the task's
//! run. Any engine takes it, as a [`TaskResult`], into the loop that applies
//! its own: so a remote task is retried, repeated, deferred, memoized and
//! dead-lettered exactly as a local one is, by the one copy of that logic.
//! The task's claim is the runner session's, and every mark the loop makes is
//! fenced by it: a word that came too late changes nothing.

use std::time::Instant;

use dagron_core::dag;
use dagron_core::models::RunnerResult;
use dagron_core::runner::Outcome;
use dagron_executor::worker::{Stopped, TaskResult};

/// How many runners' results one tick takes.
pub(crate) const PER_TICK: i64 = 256;

/// A runner's word on a task as the result the loop applies. `None` for a task
/// that is gone, or a word nobody can read: it is dropped, as there is nothing
/// to apply it to.
pub(crate) fn as_task_result(r: RunnerResult) -> Option<TaskResult> {
    r.run_id.as_ref()?;
    let outcome = Outcome::parse(&r.outcome)?;
    // The spec the task was made with, for what the loop reads of it: its
    // retries, `repeat:`, `defer:`, `cache:` and `produces:`. dagron-api read
    // it before it gave the task out, so it reads.
    let spec: Option<dag::TaskSpec> = r
        .input
        .as_deref()
        .and_then(|json| serde_json::from_str(json).ok());
    let (max_attempts, retry_delay_secs, retry_max_delay_secs, retry_on_timeout) = match &spec {
        Some(s) => (
            s.max_attempts,
            s.retry_delay_secs,
            s.retry_max_delay_secs,
            s.retry_on_timeout.unwrap_or(true),
        ),
        None => (1, 0, None, true),
    };
    Some(TaskResult {
        task_id: r.task_id,
        worker_id: r.claimed_by,
        success: outcome == Outcome::Succeeded,
        timed_out: outcome == Outcome::TimedOut,
        backpressure: outcome == Outcome::Backpressure,
        output: r.output,
        attempt: r.attempt,
        max_attempts,
        retry_delay_secs,
        retry_max_delay_secs,
        retry_on_timeout,
        fence: r.fence,
        finished: Instant::now(),
        spec: spec.map(Box::new),
        // A task stopped because it was asked to, whose work the runner saw
        // gone: cancelled, as one of this engine's would be.
        stopped: if outcome == Outcome::Cancelled {
            Stopped::Gone
        } else {
            Stopped::No
        },
        teardown: false,
        // dagron-api gives no task of a run that is not trusted to a runner.
        confined: false,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn word(outcome: &str, input: Option<&str>) -> RunnerResult {
        RunnerResult {
            task_id: "t1".into(),
            claimed_by: "runner:r1/s1".into(),
            fence: 7,
            outcome: outcome.into(),
            exit_code: Some(3),
            output: Some("boom".into()),
            run_id: Some("run1".into()),
            attempt: 1,
            input: input.map(str::to_string),
        }
    }

    #[test]
    fn a_runner_s_word_is_a_result_under_its_session_s_claim_and_fence() {
        let spec = r#"{"name":"build","command":["make"],"max_attempts":3,"retry_delay_secs":5,"retry_on_timeout":false}"#;
        let r = as_task_result(word("timed_out", Some(spec))).unwrap();
        assert_eq!(r.worker_id, "runner:r1/s1");
        assert_eq!((r.fence, r.attempt), (7, 1));
        assert!(!r.success && r.timed_out && !r.backpressure);
        assert_eq!(
            (r.max_attempts, r.retry_delay_secs, r.retry_on_timeout),
            (3, 5, false)
        );
        assert_eq!(r.stopped, Stopped::No);
        assert_eq!(r.spec.as_ref().unwrap().name, "build");
        assert_eq!(r.output.as_deref(), Some("boom"));
    }

    #[test]
    fn each_outcome_says_what_the_loop_reads_of_it() {
        let ok = as_task_result(word("succeeded", None)).unwrap();
        assert!(ok.success && ok.spec.is_none() && ok.max_attempts == 1);
        assert!(
            as_task_result(word("backpressure", None))
                .unwrap()
                .backpressure
        );
        assert!(!as_task_result(word("failed", None)).unwrap().success);
        assert_eq!(
            as_task_result(word("cancelled", None)).unwrap().stopped,
            Stopped::Gone
        );
    }

    #[test]
    fn a_word_on_a_task_that_is_gone_or_that_cannot_be_read_is_none() {
        let mut gone = word("succeeded", None);
        gone.run_id = None;
        assert!(as_task_result(gone).is_none());
        assert!(as_task_result(word("lost", None)).is_none());
    }
}
