//! What became of a task's steps: written to `CI_RESULT_FILE` when it is set,
//! and always logged as the last line, `::dagron-ci::result::{…}`.
//!
//! Each step has an `outcome` (what happened) and a `conclusion` (what it
//! counts as): a step that fails under `continue_on_error` has the outcome
//! `failure` and the conclusion `success`, as GitHub reports one.

use serde_json::{json, Value};

use crate::protocol::Step;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Outcome {
    Success,
    Failure,
    /// It ran past its `timeout_secs`, and was stopped.
    TimedOut,
    /// The task was cancelled while it ran.
    Cancelled,
    /// It never ran: an earlier step ended the task, or the task was
    /// cancelled first.
    Skipped,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Outcome::Success => "success",
            Outcome::Failure => "failure",
            Outcome::TimedOut => "timed_out",
            Outcome::Cancelled => "cancelled",
            Outcome::Skipped => "skipped",
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct StepResult {
    pub n: usize,
    pub name: String,
    pub outcome: Outcome,
    pub conclusion: Outcome,
    /// What its `sh` exited with: 128 plus the signal for one a signal ended,
    /// as a shell reports it. `None` for a step that never ran.
    pub exit_code: Option<i32>,
    /// The signal that ended it, if one did.
    pub signal: Option<i32>,
    /// When it started, in milliseconds since the Unix epoch.
    pub started_at_ms: Option<u64>,
    pub duration_ms: Option<u64>,
    pub timeout_secs: Option<u64>,
}

impl StepResult {
    pub fn skipped(step: &Step) -> StepResult {
        StepResult {
            n: step.n,
            name: step.name.clone(),
            outcome: Outcome::Skipped,
            conclusion: Outcome::Skipped,
            exit_code: None,
            signal: None,
            started_at_ms: None,
            duration_ms: None,
            timeout_secs: step.timeout.map(|t| t.as_secs()),
        }
    }

    fn to_json(&self) -> Value {
        let mut step = json!({
            "n": self.n,
            "name": self.name,
            "outcome": self.outcome.as_str(),
            "conclusion": self.conclusion.as_str(),
        });
        let fields = [
            ("exit_code", self.exit_code.map(Value::from)),
            ("signal", self.signal.map(Value::from)),
            ("started_at_ms", self.started_at_ms.map(Value::from)),
            ("duration_ms", self.duration_ms.map(Value::from)),
            ("timeout_secs", self.timeout_secs.map(Value::from)),
        ];
        for (key, value) in fields {
            if let Some(value) = value {
                step[key] = value;
            }
        }
        step
    }
}

/// The whole task's result.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct JobResult {
    /// What the task exits with: 0, the code of the step that ended it, or
    /// 128 plus the signal that cancelled it.
    pub exit_code: i32,
    /// The signal that cancelled the task, if one did.
    pub cancelled_by: Option<i32>,
    pub steps: Vec<StepResult>,
    /// The step protocol the steps were handed on in.
    pub protocol: u32,
    /// Under protocol 3: what the job says to the jobs after it.
    pub said: Option<Said>,
    /// What the steps wrote to their summaries, in the steps' order. Not in
    /// the result's JSON: it is said in lines of its own
    /// ([`crate::report::summary_lines`]).
    pub summary: String,
}

/// What a job says to the jobs after it: its result, one of GitHub's four
/// words ([`crate::job`]), and its outputs.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Said {
    pub result: &'static str,
    pub outputs: Vec<(String, String)>,
}

impl JobResult {
    pub fn to_json(&self) -> Value {
        let mut result = json!({
            "protocol": self.protocol,
            "exit_code": self.exit_code,
            "cancelled_by": self.cancelled_by,
            "steps": self.steps.iter().map(StepResult::to_json).collect::<Vec<_>>(),
        });
        if let Some(said) = &self.said {
            result["result"] = json!(said.result);
            result["outputs"] = said
                .outputs
                .iter()
                .map(|(name, value)| (name.clone(), json!(value)))
                .collect::<serde_json::Map<_, _>>()
                .into();
        }
        result
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;

    #[test]
    fn a_result_names_each_step_and_leaves_out_what_did_not_happen() {
        let step = Step {
            n: 2,
            name: "deploy \"prod\"".into(),
            run: "make".into(),
            timeout: Some(Duration::from_secs(60)),
            continue_on_error: false,
            env: Vec::new(),
            ..Step::default()
        };
        let result = JobResult {
            protocol: 1,
            exit_code: 3,
            cancelled_by: None,
            said: None,
            summary: String::new(),
            steps: vec![
                StepResult {
                    n: 1,
                    name: "build".into(),
                    outcome: Outcome::Failure,
                    conclusion: Outcome::Success,
                    exit_code: Some(3),
                    signal: None,
                    started_at_ms: Some(1_759_660_800_000),
                    duration_ms: Some(42),
                    timeout_secs: None,
                },
                StepResult::skipped(&step),
            ],
        };
        assert_eq!(
            result.to_json(),
            json!({
                "protocol": 1,
                "exit_code": 3,
                "cancelled_by": null,
                "steps": [
                    {"n": 1, "name": "build", "outcome": "failure", "conclusion": "success",
                     "exit_code": 3, "started_at_ms": 1_759_660_800_000_u64, "duration_ms": 42},
                    {"n": 2, "name": "deploy \"prod\"", "outcome": "skipped",
                     "conclusion": "skipped", "timeout_secs": 60},
                ],
            })
        );
        // One line, whatever a name holds, so it can be logged as one.
        assert!(!result.to_json().to_string().contains('\n'));
    }
}
