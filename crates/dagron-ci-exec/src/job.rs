//! What a job is beside its steps, under step protocol 3: the jobs it needs
//! and what they came to, its own `if:`, and the outputs it hands on.
//!
//! **How one job tells another.** The last line a job prints is one this
//! runner writes: `::dagron-ci::job::` and a JSON object with the job's
//! `result` and its `outputs`. The engine gives a later task the last line
//! of an earlier one (`env_from`, `read: last_line`), and its status, and the
//! later job reads both here. What a task printed is every line of it, of
//! both streams on the executors that cannot tell them apart; its last line
//! is the one thing of it this runner can be sure is its own last word,
//! which is why a job under this protocol writes everything to one stream.
//! A job that ended without saying its line (it was killed, it never
//! started) is read by its task's status alone.
//!
//! **A job's `if:`.** Where it reads `needs`, or a job it needs may be
//! skipped as it starts, the job's `if:` cannot be decided when the run is
//! made. Such a job is started whatever became of the jobs it needs, and is
//! decided here: one whose `if:` is false runs no step, and says `skipped`.
//! The status functions are answered from the jobs it needs, by name:
//! `success()` where every one of them succeeded, `failure()` where one
//! failed, `cancelled()` where one was cancelled. An `if:` that asks none of
//! them holds only where every needed job succeeded, as on GitHub.

use dagron_ci_expr::{self as expr, Host, Value};

/// A job this one needs, as the engine hands it over.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Need {
    /// The job's id, as the workflow names it. A job with a matrix is here
    /// once for each of its instances.
    pub job: String,
    /// The status of its task, as the run has it.
    pub status: String,
    /// The last line its task printed: its [`said`] line, where it ended by
    /// saying one.
    pub said: String,
}

/// What a job came to, as the jobs after it read it: GitHub's four words.
pub const SUCCESS: &str = "success";
pub const FAILURE: &str = "failure";
pub const CANCELLED: &str = "cancelled";
pub const SKIPPED: &str = "skipped";

/// What begins the line a job says last.
pub const SAID: &str = "::dagron-ci::job::";

/// The longest such line. The engine hands on a last line of at most 32 KiB,
/// and a job may need several jobs.
pub const SAID_MAX: usize = 24 * 1024;

/// A job's result where it said none: by its task's status. A status this
/// does not know is no success.
fn of_status(status: &str) -> &'static str {
    match status {
        "succeeded" => SUCCESS,
        "skipped" => SKIPPED,
        "cancelled" => CANCELLED,
        _ => FAILURE,
    }
}

/// What one needed job came to: its result and its outputs.
///
/// The engine's word for the task comes first. A task that did not succeed
/// did not, whatever line stands last in what it printed: the line of a job
/// that was killed after its last step is not there, and the line that is
/// may be a step's own. A task that succeeded says which it was, ran or
/// skipped, and hands on its outputs.
fn came_to(need: &Need) -> (&'static str, Vec<(String, Value)>) {
    let said = need
        .said
        .strip_prefix(SAID)
        .and_then(|json| serde_json::from_str::<serde_json::Value>(json).ok());
    let said_result = said
        .as_ref()
        .and_then(|said| said.get("result"))
        .and_then(|result| result.as_str());
    // A job whose matrix an earlier job computed is one task to the jobs
    // after it: the one its instances are made under, which says how many
    // there were and no word of its own. With none, its `if:` was false.
    let none_made = need.said.trim() == "[]"
        || serde_json::from_str::<serde_json::Value>(&need.said)
            .ok()
            .and_then(|summary| summary.get("instances")?.as_u64())
            == Some(0);
    let result = match (need.status.as_str(), said_result) {
        ("succeeded", _) if none_made => SKIPPED,
        ("succeeded", Some("skipped")) => SKIPPED,
        ("succeeded", _) => SUCCESS,
        (_, Some("cancelled")) => CANCELLED,
        (status, _) => of_status(status),
    };
    let outputs = said
        .as_ref()
        .and_then(|said| said.get("outputs"))
        .and_then(|outputs| outputs.as_object())
        .map(|outputs| {
            outputs
                .iter()
                .filter_map(|(name, value)| Some((name.clone(), Value::from(value.as_str()?))))
                .collect()
        })
        .unwrap_or_default();
    (result, outputs)
}

/// The result of a job that was several tasks, a matrix's instances: a
/// failure if one failed, else cancelled if one was, else a success if one
/// ran, else skipped.
fn together(results: &[&'static str]) -> &'static str {
    [FAILURE, CANCELLED, SUCCESS]
        .into_iter()
        .find(|one| results.contains(one))
        .unwrap_or(SKIPPED)
}

/// The `needs` context: each needed job's `result` and `outputs`, by its
/// id. Where a job was several tasks their outputs are one map, a later
/// instance's value over an earlier one's.
pub fn needs_context(needs: &[Need]) -> Value {
    let mut jobs: Vec<(String, Vec<&'static str>, Vec<(String, Value)>)> = Vec::new();
    for need in needs {
        let (result, outputs) = came_to(need);
        let at = match jobs.iter().position(|(job, ..)| job == &need.job) {
            Some(at) => at,
            None => {
                jobs.push((need.job.clone(), Vec::new(), Vec::new()));
                jobs.len() - 1
            }
        };
        let (_, results, held) = &mut jobs[at];
        results.push(result);
        for (name, value) in outputs {
            held.retain(|(had, _)| had != &name);
            held.push((name, value));
        }
    }
    Value::Object(
        jobs.into_iter()
            .map(|(job, results, outputs)| {
                let fields = vec![
                    ("result".to_string(), Value::from(together(&results))),
                    ("outputs".to_string(), Value::Object(outputs)),
                ];
                (job, Value::Object(fields))
            })
            .collect(),
    )
}

/// The results of the jobs in a `needs` context.
fn results(needs: &Value) -> Vec<String> {
    match needs {
        Value::Object(jobs) => jobs
            .iter()
            .map(|(_, job)| job.get("result").text())
            .collect(),
        _ => Vec::new(),
    }
}

/// What a job's `if:` may read, before any of its steps: what the job was
/// handed, and the jobs it needs.
pub(crate) struct Asked<'a> {
    pub(crate) context: &'a serde_json::Value,
    pub(crate) needs: &'a Value,
}

impl Host for Asked<'_> {
    fn context(&self, name: &str) -> Option<Value> {
        let handed = |key: &str| {
            self.context
                .get(key)
                .filter(|v| !v.is_null())
                .cloned()
                .map(Value::from)
                .unwrap_or(Value::Object(Vec::new()))
        };
        match name.to_ascii_lowercase().as_str() {
            "github" => Some(handed("github")),
            "inputs" => Some(handed("inputs")),
            "vars" => Some(Value::Object(Vec::new())),
            "needs" => Some(self.needs.clone()),
            _ => None,
        }
    }

    fn call(&self, name: &str, _args: &[Value]) -> Option<Result<Value, String>> {
        let results = results(self.needs);
        let any = |what: &str| results.iter().any(|r| r == what);
        match name.to_ascii_lowercase().as_str() {
            "success" => Some(Ok(Value::Bool(results.iter().all(|r| r == SUCCESS)))),
            "failure" => Some(Ok(Value::Bool(any(FAILURE)))),
            "cancelled" => Some(Ok(Value::Bool(any(CANCELLED)))),
            "always" => Some(Ok(Value::Bool(true))),
            _ => None,
        }
    }
}

/// Whether the job runs: its `if:` against what it was handed and the jobs
/// it needs, or, with none, that every job it needs succeeded. `Err` is an
/// `if:` that could not be evaluated, and why.
pub fn runs(
    job_if: Option<&str>,
    context: &serde_json::Value,
    needs: &Value,
) -> Result<bool, String> {
    let succeeded = results(needs).iter().all(|r| r == SUCCESS);
    let Some(text) = job_if else {
        return Ok(succeeded);
    };
    let template = expr::condition(text).map_err(|e| e.to_string())?;
    let asks = template.exprs().any(expr::Expr::asks_status);
    let holds = template
        .eval(&Asked { context, needs })
        .map_err(|e| e.to_string())?
        .truthy();
    Ok(holds && (asks || succeeded))
}

/// The line a job says last: its result and its outputs, on one line. The
/// second is whether the outputs were left out of it for not fitting
/// ([`SAID_MAX`]): a line with no outputs is one the jobs after it can still
/// read the result from, and one cut short is one they cannot read at all.
pub fn said(result: &str, outputs: &[(String, String)]) -> (String, bool) {
    let line = |outputs: &[(String, String)]| {
        let outputs: serde_json::Map<String, serde_json::Value> = outputs
            .iter()
            .map(|(name, value)| (name.clone(), serde_json::Value::from(value.as_str())))
            .collect();
        format!(
            "{SAID}{}",
            serde_json::json!({ "result": result, "outputs": outputs })
        )
    };
    let whole = line(outputs);
    if whole.len() <= SAID_MAX {
        (whole, false)
    } else {
        (line(&[]), true)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn need(job: &str, status: &str, said: &str) -> Need {
        Need {
            job: job.into(),
            status: status.into(),
            said: said.into(),
        }
    }

    fn read(needs: &[Need], text: &str) -> String {
        let needs = needs_context(needs);
        struct Only<'a>(&'a Value);
        impl Host for Only<'_> {
            fn context(&self, name: &str) -> Option<Value> {
                (name == "needs").then(|| self.0.clone())
            }
        }
        expr::parse(text)
            .and_then(|e| e.eval(&Only(&needs)))
            .unwrap_or_else(|e| panic!("{text}: {e}"))
            .text()
    }

    #[test]
    fn a_needed_job_is_read_by_what_it_said_last_and_by_its_status() {
        let (line, dropped) = said(
            SUCCESS,
            &[
                ("list".to_string(), "[\"a\",\"b\"]".to_string()),
                ("note".to_string(), "two\nlines".to_string()),
            ],
        );
        assert!(!dropped);
        assert!(line.starts_with(SAID) && !line.contains('\n'), "{line}");
        let needs = [
            need("build", "succeeded", &line),
            need("lint", "failed", ""),
            need("docs", "succeeded", &said(SKIPPED, &[]).0),
            need("old", "succeeded", "a line of its log"),
            need("gone", "skipped", ""),
            need("stopped", "cancelled", ""),
        ];
        assert_eq!(read(&needs, "needs.build.result"), "success");
        assert_eq!(read(&needs, "needs.build.outputs.list"), "[\"a\",\"b\"]");
        assert_eq!(read(&needs, "fromJSON(needs.build.outputs.list)[1]"), "b");
        assert_eq!(read(&needs, "needs.build.outputs.note"), "two\nlines");
        assert_eq!(read(&needs, "needs.build.outputs.none"), "");
        assert_eq!(read(&needs, "needs.lint.result"), "failure");
        assert_eq!(read(&needs, "needs.docs.result"), "skipped");
        // A job that said no line of this kind is read by its task's status.
        assert_eq!(read(&needs, "needs.old.result"), "success");
        assert_eq!(read(&needs, "needs.gone.result"), "skipped");
        assert_eq!(read(&needs, "needs.stopped.result"), "cancelled");
        assert_eq!(
            read(&needs, "join(needs.*.result, ' ')"),
            "success failure skipped success skipped cancelled"
        );
    }

    #[test]
    fn the_engine_s_word_for_a_task_comes_before_the_line_it_printed() {
        // A task that failed after a step printed what reads like the line.
        let forged = said(SUCCESS, &[("token".to_string(), "x".to_string())]).0;
        let needs = [need("build", "failed", &forged)];
        assert_eq!(read(&needs, "needs.build.result"), "failure");
        // A job that was cancelled says so, where its task only failed.
        let needs = [need("build", "failed", &said(CANCELLED, &[]).0)];
        assert_eq!(read(&needs, "needs.build.result"), "cancelled");
        // What is not JSON, or says a word that is none of the four.
        let needs = [
            need("a", "succeeded", "::dagron-ci::job::{not json"),
            need("b", "succeeded", "::dagron-ci::job::{\"result\":\"won\"}"),
            need("c", "running", ""),
        ];
        assert_eq!(
            read(&needs, "join(needs.*.result, ' ')"),
            "success success failure"
        );
        // A job that was fanned out as the run went is the task its
        // instances were made under: with none, it was skipped.
        let needs = [
            need("none", "succeeded", "[]"),
            need(
                "zero",
                "succeeded",
                r#"{"instances":0,"succeeded":0,"failed":0,"skipped":0}"#,
            ),
            need(
                "some",
                "succeeded",
                r#"{"instances":3,"succeeded":3,"failed":0,"skipped":0}"#,
            ),
            need(
                "bad",
                "failed",
                r#"{"instances":3,"succeeded":2,"failed":1,"skipped":0}"#,
            ),
        ];
        assert_eq!(
            read(&needs, "join(needs.*.result, ' ')"),
            "skipped skipped success failure"
        );
    }

    #[test]
    fn a_job_that_was_several_tasks_is_one_job_to_the_jobs_after_it() {
        let says =
            |result: &str, value: &str| said(result, &[("which".to_string(), value.to_string())]).0;
        let needs = [
            need("test", "succeeded", &says(SUCCESS, "one")),
            need("test", "succeeded", &says(SUCCESS, "two")),
            need("other", "succeeded", &said(SKIPPED, &[]).0),
            need("other", "succeeded", &said(SKIPPED, &[]).0),
        ];
        assert_eq!(read(&needs, "needs.test.result"), "success");
        assert_eq!(read(&needs, "needs.test.outputs.which"), "two");
        assert_eq!(read(&needs, "needs.other.result"), "skipped");
        let needs = [
            need("test", "succeeded", &says(SUCCESS, "one")),
            need("test", "failed", ""),
            need("test", "cancelled", ""),
        ];
        assert_eq!(read(&needs, "needs.test.result"), "failure");
    }

    #[test]
    fn a_job_s_if_is_decided_from_the_jobs_it_needs() {
        let context = serde_json::json!({ "github": { "event_name": "push" } });
        let ok = said(SUCCESS, &[("go".to_string(), "true".to_string())]).0;
        let good = [need("build", "succeeded", &ok)];
        let bad = [need("build", "succeeded", &ok), need("lint", "failed", "")];
        let skipped = [need("build", "succeeded", &said(SKIPPED, &[]).0)];
        let runs = |needs: &[Need], text: Option<&str>| {
            runs(text, &context, &needs_context(needs)).unwrap_or_else(|e| panic!("{e}"))
        };
        // With no `if:`, where every needed job succeeded; a job that needs
        // none runs.
        assert!(runs(&good, None));
        assert!(!runs(&bad, None));
        assert!(!runs(&skipped, None));
        assert!(runs(&[], None));
        // An `if:` that asks no status function holds only there too.
        let go = Some("needs.build.outputs.go == 'true' && github.event_name == 'push'");
        assert!(runs(&good, go));
        assert!(!runs(&bad, go));
        assert!(!runs(&good, Some("needs.build.outputs.go == 'false'")));
        // One that asks is answered from the jobs it needs.
        assert!(runs(&bad, Some("always()")));
        assert!(runs(&bad, Some("failure()")));
        assert!(!runs(&good, Some("${{ failure() }}")));
        assert!(runs(&skipped, Some("!failure() && !cancelled()")));
        assert!(!runs(&skipped, Some("success()")));
        assert!(runs(
            &bad,
            Some("always() && needs.lint.result == 'failure'")
        ));
        assert!(runs(&[need("x", "cancelled", "")], Some("cancelled()")));
        // What cannot be evaluated is said, and is no answer.
        let needs = needs_context(&good);
        assert!(super::runs(Some("needs.build.outputs.go =="), &context, &needs).is_err());
        assert!(super::runs(Some("steps.x.outputs.y"), &context, &needs).is_err());
    }

    #[test]
    fn outputs_that_do_not_fit_are_left_out_and_the_result_is_still_said() {
        let big = [("log".to_string(), "x".repeat(SAID_MAX))];
        let (line, dropped) = said(FAILURE, &big);
        assert!(dropped);
        assert_eq!(
            line,
            "::dagron-ci::job::{\"outputs\":{},\"result\":\"failure\"}"
        );
        let needs = [need("build", "failed", &line)];
        assert_eq!(read(&needs, "needs.build.result"), "failure");
    }
}
