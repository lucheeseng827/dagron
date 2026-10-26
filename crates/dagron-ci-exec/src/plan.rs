//! A job's matrix, where an earlier job computes it: `dagron-ci-exec
//! matrix`.
//!
//! A matrix that is written out is expanded when the run is made. One that
//! is an expression of what an earlier job handed on
//! (`matrix: { module: ${{ fromJSON(needs.find.outputs.list) }} }`) cannot
//! be: no job has run. Such a job is made in two tasks. The first is this
//! one, started when the jobs it needs are over: it reads what they came to,
//! decides the job's `if:`, evaluates the matrix, expands it by the rule a
//! written matrix is expanded by ([`dagron_ci_expr::matrix`]), and prints
//! the job's instances. The engine then makes a task of each (dagron's
//! `with_output_of:`).
//!
//! What it prints is a JSON array and nothing else, on its standard output,
//! each element an instance: its `label`, its `matrix` and `strategy`
//! contexts, and `b`, which is two opening braces. That last is for the
//! text the instances carry: the engine fills `{{ item… }}` into it as it
//! makes them, a second filling of text the first already restored, and a
//! `{{` of the workflow's own is written so that both hand it on
//! (`{{ ci_braces }} item.b }}`, in dagron-ci's GitHub front-end).
//!
//! A job whose `if:` is false has no instance: the array is empty.

use std::collections::BTreeMap;

use dagron_ci_expr::matrix::{self, Matrix, Row};
use dagron_ci_expr::{self as expr, Value as Ev};
use serde_json::{json, Value};

use crate::job::{Asked, Need};

/// The most instances a matrix may come to, as the front-end holds a
/// workflow's jobs to.
pub const MAX_INSTANCES: usize = 256;

/// The instances of the job whose plan `vars` carry, as the JSON array to
/// print. `Err` is why there is none to print.
///
/// | Variable | |
/// |---|---|
/// | `CI_MATRIX_PLAN` | the matrix as the workflow writes it, as JSON: `{"whole": EXPR}`, or `{"axes": [[NAME, VALUES or {"expr": EXPR}], …], "include": [ROW, …], "exclude": [ROW, …]}`, a row a list of `[KEY, VALUE]`; and `"fail_fast"` |
/// | `CI_CONTEXT` | the job's contexts, as under step protocol 2 |
/// | `CI_JOB_IF`, `CI_NEED_<k>_…` | as under step protocol 3 |
pub fn plan(vars: &BTreeMap<String, String>) -> Result<String, String> {
    let text = |name: &str| vars.get(name).map(String::as_str);
    let context: Value = match text("CI_CONTEXT") {
        Some(json) => {
            serde_json::from_str(json).map_err(|e| format!("CI_CONTEXT is not JSON: {e}"))?
        }
        None => Value::Null,
    };
    let count: usize = match text("CI_NEED_COUNT") {
        None => 0,
        Some(count) => count
            .parse()
            .ok()
            .filter(|&count| count <= crate::protocol::MAX_NEEDS)
            .ok_or_else(|| format!("CI_NEED_COUNT={count:?} is not a number of tasks"))?,
    };
    let mut needs = Vec::with_capacity(count);
    for k in 1..=count {
        let own = |key: &str| {
            text(&format!("CI_NEED_{k}_{key}"))
                .unwrap_or_default()
                .to_string()
        };
        let job = own("JOB");
        if job.is_empty() {
            return Err(format!("CI_NEED_{k}_JOB is not set: the job it is"));
        }
        needs.push(Need {
            job,
            status: own("STATUS"),
            said: own("SAID"),
        });
    }
    let needs = crate::job::needs_context(&needs);
    let job_if = text("CI_JOB_IF").filter(|text| !text.trim().is_empty());
    if !crate::job::runs(job_if, &context, &needs).map_err(|e| format!("the job's if: {e}"))? {
        return Ok("[]".to_string());
    }

    let plan: Value = serde_json::from_str(
        text("CI_MATRIX_PLAN").ok_or("CI_MATRIX_PLAN is not set: the job's matrix")?,
    )
    .map_err(|e| format!("CI_MATRIX_PLAN is not JSON: {e}"))?;
    let asked = Asked {
        context: &context,
        needs: &needs,
    };
    let evaluate = |what: &str, source: &str| -> Result<Value, String> {
        let value: Ev = expr::parse(source)
            .and_then(|e| e.eval(&asked))
            .map_err(|e| format!("{what}: {e}"))?;
        Ok(Value::from(&value))
    };
    let matrix = match plan.get("whole").and_then(Value::as_str) {
        Some(source) => whole(evaluate("strategy.matrix", source)?)?,
        None => written(&plan, &evaluate)?,
    };
    for (axis, values) in &matrix.axes {
        if values.is_empty() {
            return Err(format!(
                "strategy.matrix.{axis} came to no value: a matrix's axis has at least one"
            ));
        }
    }
    let rows = matrix::rows(&matrix, MAX_INSTANCES)
        .map_err(|_| format!("strategy.matrix comes to more than {MAX_INSTANCES} combinations"))?;
    if rows.is_empty() {
        return Err("strategy.matrix leaves no combination to run".to_string());
    }
    let fail_fast = plan
        .get("fail_fast")
        .and_then(Value::as_bool)
        .unwrap_or(true);
    let total = rows.len();
    let instances: Vec<Value> = matrix::labelled(rows)
        .into_iter()
        .enumerate()
        .map(|(index, (label, row))| {
            json!({
                "label": label,
                "matrix": row.into_iter().collect::<serde_json::Map<_, _>>(),
                "strategy": { "fail-fast": fail_fast, "job-index": index, "job-total": total },
                "b": "{{",
            })
        })
        .collect();
    Ok(Value::Array(instances).to_string())
}

/// A row, from a JSON object: its keys as the object has them.
fn row_of(what: &str, value: &Value) -> Result<Row, String> {
    value
        .as_object()
        .map(|row| row.iter().map(|(k, v)| (k.clone(), v.clone())).collect())
        .ok_or_else(|| format!("{what} is not a mapping"))
}

/// A matrix that is one expression's value: an object of axes, with
/// `include` and `exclude` where it has them.
fn whole(value: Value) -> Result<Matrix, String> {
    let Value::Object(map) = value else {
        return Err("strategy.matrix came to what is no mapping of axes".to_string());
    };
    let mut matrix = Matrix::default();
    for (key, value) in map {
        match (key.as_str(), value) {
            ("include" | "exclude", Value::Array(rows)) => {
                let rows = rows
                    .iter()
                    .map(|row| row_of(&format!("a row of strategy.matrix.{key}"), row))
                    .collect::<Result<Vec<_>, _>>()?;
                if key == "include" {
                    matrix.include = rows;
                } else {
                    matrix.exclude = rows;
                }
            }
            (_, Value::Array(values)) => matrix.axes.push((key, values)),
            _ => return Err(format!("strategy.matrix.{key} came to what is no list")),
        }
    }
    Ok(matrix)
}

/// A matrix written axis by axis, an axis its values or an expression for
/// them, in the order the workflow writes them.
fn written(
    plan: &Value,
    evaluate: &dyn Fn(&str, &str) -> Result<Value, String>,
) -> Result<Matrix, String> {
    let pairs = |value: &Value| -> Option<Vec<(String, Value)>> {
        value
            .as_array()?
            .iter()
            .map(|pair| Some((pair.get(0)?.as_str()?.to_string(), pair.get(1)?.clone())))
            .collect()
    };
    let malformed = || "CI_MATRIX_PLAN is not a matrix's plan".to_string();
    let mut matrix = Matrix::default();
    for (axis, values) in plan.get("axes").and_then(pairs).ok_or_else(malformed)? {
        let at = format!("strategy.matrix.{axis}");
        let values = match values.get("expr").and_then(Value::as_str) {
            Some(source) => evaluate(&at, source)?,
            None => values,
        };
        let Value::Array(values) = values else {
            return Err(format!("{at} came to what is no list"));
        };
        matrix.axes.push((axis, values));
    }
    for (key, rows) in [
        ("include", &mut matrix.include),
        ("exclude", &mut matrix.exclude),
    ] {
        for row in plan
            .get(key)
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
        {
            rows.push(pairs(row).ok_or_else(malformed)?);
        }
    }
    Ok(matrix)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn found(list: &str, go: &str) -> String {
        crate::job::said(
            crate::job::SUCCESS,
            &[
                ("list".to_string(), list.to_string()),
                ("go".to_string(), go.to_string()),
            ],
        )
        .0
    }

    fn planned(plan: &str, said: &str, job_if: &str) -> Result<Value, String> {
        super::plan(&vars(&[
            ("CI_CONTEXT", r#"{"github":{"event_name":"push"}}"#),
            ("CI_JOB_IF", job_if),
            ("CI_NEED_COUNT", "1"),
            ("CI_NEED_1_JOB", "find"),
            ("CI_NEED_1_STATUS", "succeeded"),
            ("CI_NEED_1_SAID", said),
            ("CI_MATRIX_PLAN", plan),
        ]))
        .map(|printed| serde_json::from_str(&printed).unwrap())
    }

    #[test]
    fn an_axis_an_earlier_job_computed_is_expanded_with_the_ones_written_out() {
        let plan = r#"{"axes": [["os", ["linux", "mac"]], ["module", {"expr": "fromJSON(needs.find.outputs.list)"}]],
                       "exclude": [[["os", "mac"], ["module", "b"]]], "include": [[["module", "a"], ["slow", true]]],
                       "fail_fast": false}"#;
        let instances = planned(plan, &found(r#"["a","b"]"#, "yes"), "").unwrap();
        let labels: Vec<&str> = instances
            .as_array()
            .unwrap()
            .iter()
            .map(|i| i["label"].as_str().unwrap())
            .collect();
        assert_eq!(labels, ["linux-a-true", "linux-b", "mac-a-true"]);
        assert_eq!(
            instances[0],
            json!({
                "label": "linux-a-true",
                "matrix": { "os": "linux", "module": "a", "slow": true },
                "strategy": { "fail-fast": false, "job-index": 0, "job-total": 3 },
                "b": "{{",
            })
        );
        assert_eq!(instances[2]["strategy"]["job-index"], 2);
    }

    #[test]
    fn a_matrix_that_is_one_expression_is_the_mapping_it_comes_to() {
        let whole = r#"{"whole": "fromJSON(needs.find.outputs.list)"}"#;
        let said = found(
            r#"{"node":[18,20],"include":[{"node":20,"lts":true}]}"#,
            "yes",
        );
        let instances = planned(whole, &said, "").unwrap();
        assert_eq!(instances.as_array().unwrap().len(), 2);
        assert_eq!(instances[1]["matrix"], json!({ "node": 20, "lts": true }));
        assert_eq!(instances[1]["label"], "20-true");
        for (said, why) in [
            (found(r#"["a"]"#, "yes"), "no mapping of axes"),
            (
                found(r#"{"node":18}"#, "yes"),
                "strategy.matrix.node came to what is no list",
            ),
            (found("not json", "yes"), "strategy.matrix: "),
        ] {
            let err = planned(whole, &said, "").unwrap_err();
            assert!(err.contains(why), "{err}");
        }
    }

    #[test]
    fn a_job_whose_if_is_false_has_no_instance_and_one_with_no_value_is_refused() {
        let plan = r#"{"axes": [["module", {"expr": "fromJSON(needs.find.outputs.list)"}]]}"#;
        // Its `if:` is decided before its matrix is read: a job that is not
        // to run may have none worth reading.
        let gated = "needs.find.outputs.go == 'yes'";
        assert_eq!(planned(plan, &found("[]", "no"), gated).unwrap(), json!([]));
        assert_eq!(
            planned(plan, &found("oops", "no"), gated).unwrap(),
            json!([])
        );
        assert_eq!(
            planned(plan, &found(r#"["a"]"#, "yes"), gated).unwrap()[0]["label"],
            "a"
        );
        // The job it needs failed: with no `if:` it does not run either.
        let failed = super::plan(&vars(&[
            ("CI_NEED_COUNT", "1"),
            ("CI_NEED_1_JOB", "find"),
            ("CI_NEED_1_STATUS", "failed"),
            ("CI_MATRIX_PLAN", plan),
        ]));
        assert_eq!(failed.as_deref(), Ok("[]"));
        // An axis with nothing in it is no matrix, as on GitHub.
        let err = planned(plan, &found("[]", "yes"), "").unwrap_err();
        assert!(
            err.contains("strategy.matrix.module came to no value"),
            "{err}"
        );
        let err = planned(plan, &found(r#""a""#, "yes"), "").unwrap_err();
        assert!(
            err.contains("strategy.matrix.module came to what is no list"),
            "{err}"
        );
        // Too many to be a workflow's jobs.
        let long: Vec<u32> = (0..300).collect();
        let err = planned(
            plan,
            &found(&serde_json::to_string(&long).unwrap(), "yes"),
            "",
        )
        .unwrap_err();
        assert!(err.contains("more than 256 combinations"), "{err}");
        // And what is no plan at all.
        for (plan, why) in [("{}", "not a matrix's plan"), ("[", "not JSON")] {
            let err = planned(plan, &found("[]", "yes"), "").unwrap_err();
            assert!(err.contains(why), "{err}");
        }
    }
}
