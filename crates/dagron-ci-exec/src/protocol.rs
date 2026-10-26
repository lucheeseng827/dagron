//! The step protocol: how dagron-ci hands a task's steps to whatever runs
//! them. Version 1 is environment variables only:
//!
//! | Variable | |
//! |---|---|
//! | `CI_STEP_PROTOCOL` | `1` |
//! | `CI_STEP_COUNT` | how many steps; each `n` from 1 has the variables below |
//! | `CI_STEP_<n>_RUN` | the step's script, run as `sh -ec SCRIPT` |
//! | `CI_STEP_<n>_NAME` | its name, for the markers |
//! | `CI_STEP_<n>_TIMEOUT_SECS` | optional: how long it may run |
//! | `CI_STEP_<n>_CONTINUE_ON_ERROR` | optional: `true` lets the task go on when it fails |
//! | `CI_STEP_<n>_ENV_<NAME>` | each of its own variables, set as `NAME` for it alone |
//!
//! Scripts, names and values travel as values, never as command text, so
//! nothing in them is parsed by whatever runs the steps.
//!
//! Version 2 is version 1 and, for the steps of a job that came from a
//! GitHub workflow ([`crate::workflow`]):
//!
//! | Variable | |
//! |---|---|
//! | `CI_STEP_PROTOCOL` | `2` |
//! | `CI_CONTEXT` | the job's contexts as JSON: `github`, `inputs`, `matrix`, `strategy`, and `env` (the workflow's and the job's variables) |
//! | `CI_STEP_<n>_ID` | optional: its id, which later steps read its outputs by |
//! | `CI_STEP_<n>_SHELL` | optional: `bash`, `sh`, `python`, `pwsh`, a command with `{0}` for the script's file, or `auto` (unset is `auto`): `bash` where there is one, else `sh` |
//! | `CI_STEP_<n>_WORKDIR` | optional: where it starts, in the task's directory |
//! | `CI_STEP_<n>_IF` | optional: its condition, an expression |
//! | `CI_STEP_<n>_ENVX_<NAME>` | a variable whose value holds expressions, evaluated as the step starts |
//! | `CI_STEP_<n>_EXPR_<k>` | from 1: the expression the script's `__CI_E<k>` variable is set to the value of |
//!
//! A step's `RUN`, `NAME`, `WORKDIR` and `ENVX` values may hold `${{ … }}`.
//!
//! Version 3 is version 2 and what a job is beside its steps
//! ([`crate::job`]):
//!
//! | Variable | |
//! |---|---|
//! | `CI_STEP_PROTOCOL` | `3` |
//! | `CI_JOB_IF` | optional: the job's own condition, decided before its first step |
//! | `CI_NEED_COUNT` | how many tasks the job needs, 0 where unset |
//! | `CI_NEED_<k>_JOB` | from 1: the id of the job that task is, or is an instance of |
//! | `CI_NEED_<k>_STATUS` | the task's status, as the engine has it |
//! | `CI_NEED_<k>_SAID` | the last line the task printed |
//! | `CI_OUTPUT_COUNT` | how many outputs the job hands on, 0 where unset |
//! | `CI_MATRIX`, `CI_STRATEGY` | optional, as JSON: the `matrix` and `strategy` contexts of an instance that was made as the run went ([`crate::plan`]), in place of `CI_CONTEXT`'s |
//! | `CI_OUTPUT_<k>_NAME`, `CI_OUTPUT_<k>_VALUE` | from 1: an output's name, and its value, which may hold `${{ … }}` |
//!
//! Under it the job writes everything to its standard output, its steps'
//! standard error too, and the last line of it is the job's own:
//! `::dagron-ci::job::` and its result and outputs as JSON.
//!
//! Version 5 is version 2 and what is done around a GitLab job's steps
//! ([`crate::artifacts`]): `CI_ARTIFACT_DOWNLOAD`, fetched before them;
//! `CI_ARTIFACT_UPLOAD_*`, packed and uploaded after; and
//! `CI_JOB_ALLOW_FAILURE=true`, a failure of the job's own ended as success
//! with a warning that says it failed. It is not version 3 or 4: a GitLab
//! job is no GitHub job.
//!
//! Version 4 is version 3 and a step that uses an action
//! ([`crate::workflow`]): a directory of the workspace, or of a repository
//! the executor fetched before the job started, whose `action.yml` is read,
//! and its steps run, as the step starts.
//!
//! | Variable | |
//! |---|---|
//! | `CI_STEP_PROTOCOL` | `4` |
//! | `CI_STEP_<n>_USES` | optional: the action, as `uses:` names it ([`dagron_ci_expr::uses`]): `./` and a directory of the workspace, or `owner/repo[/dir]@ref`, which is in `$DAGRON_ACTIONS_DIR/<owner>/<repo>/<ref, each / written %2F>[/dir]`. Such a step has no `RUN` |
//! | `CI_STEP_<n>_WITH_<k>_NAME`, `CI_STEP_<n>_WITH_<k>_VALUE` | from 1: an input the step gives the action, and its value, which may hold `${{ … }}` |

use std::collections::BTreeMap;
use std::ffi::{OsStr, OsString};
use std::path::PathBuf;
use std::time::Duration;

/// The step protocol a native pipeline's steps are handed on in, which the
/// shell fallback speaks too.
pub const PROTOCOL: u32 = 1;

/// The step protocols this runner speaks.
pub const PROTOCOLS: [u32; 5] = [1, 2, 3, 4, 5];

/// The most inputs a step may give an action.
pub const MAX_WITH: usize = 100;

/// The most tasks a job may need, and the most outputs it may hand on.
pub const MAX_NEEDS: usize = 256;
pub const MAX_OUTPUTS: usize = 100;

/// The most steps one task may have.
pub const MAX_STEPS: usize = 1000;

/// How long a step has, once asked to stop, before it is killed: `timeout -k
/// 10`'s figure, which the shell fallback uses.
pub const DEFAULT_GRACE: Duration = Duration::from_secs(10);

const MAX_GRACE_SECS: u64 = 600;
const MAX_TIMEOUT_SECS: u64 = 7 * 24 * 3600;

/// One step, as the protocol gives it.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Step {
    /// Its number, from 1.
    pub n: usize,
    pub name: String,
    /// The script, for `sh -ec`; under protocol 2, for the step's shell.
    pub run: String,
    pub timeout: Option<Duration>,
    pub continue_on_error: bool,
    /// Its own variables, set for it alone.
    pub env: Vec<(OsString, OsString)>,
    // Protocol 2 alone, each where the step has it.
    /// Its id.
    pub id: Option<String>,
    /// Its shell.
    pub shell: Option<String>,
    /// Where it starts.
    pub workdir: Option<String>,
    /// Its `if:`.
    pub condition: Option<String>,
    /// Variables whose values hold expressions, by name.
    pub late_env: Vec<(String, String)>,
    /// What its script's `__CI_E<k>` variables stand for, from 1.
    pub exprs: Vec<String>,
    // Protocol 4 alone.
    /// The local action it uses, `./` and a directory of the workspace.
    pub uses: Option<String>,
    /// The inputs it gives that action, each a name and a text that may
    /// hold expressions.
    pub with: Vec<(String, String)>,
    // The step runner's own, never the protocol's.
    /// A program and its arguments, run in place of a script: a JavaScript
    /// action's, on Node.js.
    pub argv: Vec<String>,
}

/// What a task runs, read from its environment.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Job {
    pub steps: Vec<Step>,
    /// `CI_RESULT_FILE`: where the result is written, as well as logged.
    pub result_file: Option<PathBuf>,
    /// `CI_STEP_GRACE_SECS`: how long a step asked to stop (a timeout, a
    /// cancelled task) has before it is killed.
    pub grace: Duration,
    /// The protocol the steps were handed on in: 1 or 2.
    pub protocol: u32,
    /// `CI_CONTEXT`, from protocol 2: the job's contexts.
    pub context: Option<serde_json::Value>,
    /// `CI_JOB_IF`, under protocol 3: the job's own condition.
    pub job_if: Option<String>,
    /// `CI_NEED_<k>_…`, under protocol 3: the tasks the job needs.
    pub needs: Vec<crate::job::Need>,
    /// `CI_OUTPUT_<k>_…`, under protocol 3: the outputs it hands on, each
    /// a name and a text that may hold expressions.
    pub outputs: Vec<(String, String)>,
    /// `CI_MATRIX` and `CI_STRATEGY`, under protocol 3: the two contexts
    /// of an instance that was made as the run went, which the job's
    /// `CI_CONTEXT` was written too early to hold.
    pub matrix: Option<serde_json::Value>,
    pub strategy: Option<serde_json::Value>,
    /// `CI_REPORT_JUNIT` and `CI_REPORT_SARIF`: the reports the job
    /// writes, each a path in its workspace, one to a line. Read when the
    /// steps are over, whatever became of them.
    pub reports: crate::report::Reports,
}

/// Read a job from environment variables, or say what is wrong with them.
pub fn read(vars: impl IntoIterator<Item = (OsString, OsString)>) -> Result<Job, String> {
    let vars: BTreeMap<OsString, OsString> = vars.into_iter().collect();
    let text = |name: &str| -> Result<Option<String>, String> {
        match vars.get(OsStr::new(name)) {
            None => Ok(None),
            Some(value) => value
                .to_str()
                .map(|v| Some(v.to_string()))
                .ok_or_else(|| format!("{name} is not UTF-8")),
        }
    };

    let protocol =
        match text("CI_STEP_PROTOCOL")?.as_deref() {
            Some("1") => 1,
            Some("2") => 2,
            Some("3") => 3,
            Some("4") => 4,
            // A GitLab job's: version 2, and what is done around its steps
            // ([`crate::artifacts`]), which `main` reads for itself. Its steps
            // are run as version 2's.
            Some("5") => 2,
            Some(other) => {
                return Err(format!(
                "CI_STEP_PROTOCOL={other:?} is not a step protocol this dagron-ci-exec speaks; \
                 it speaks 1, 2, 3, 4 and 5"
            ))
        }
        None => {
            return Err(
                "CI_STEP_PROTOCOL is not set: dagron-ci-exec runs the steps of a task dagron-ci \
                 compiled"
                    .into(),
            )
        }
    };
    let count = match text("CI_STEP_COUNT")? {
        Some(count) => count
            .parse::<usize>()
            .ok()
            .filter(|&c| c <= MAX_STEPS)
            .ok_or_else(|| {
                format!("CI_STEP_COUNT={count:?} is not a number from 0 to {MAX_STEPS}")
            })?,
        None => return Err("CI_STEP_COUNT is not set".into()),
    };
    let past = format!("CI_STEP_{}_RUN", count + 1);
    if vars.contains_key(OsStr::new(&past)) {
        return Err(format!(
            "CI_STEP_COUNT={count}, and {past} is set: the task's steps are not all counted"
        ));
    }

    let mut steps = Vec::with_capacity(count);
    for n in 1..=count {
        // Under protocol 4 a step may use an action in place of a script.
        let uses = if protocol >= 4 {
            text(&format!("CI_STEP_{n}_USES"))?.filter(|uses| !uses.trim().is_empty())
        } else {
            None
        };
        if let Some(uses) = &uses {
            let action = dagron_ci_expr::uses::local(uses).is_some()
                || dagron_ci_expr::uses::remote(uses).is_some()
                || dagron_ci_expr::uses::docker(uses).is_some();
            if !action {
                return Err(format!(
                    "CI_STEP_{n}_USES={uses:?} is not an action: ./ and a directory of the \
                     workspace with no .. in it, owner/repo@ref, or docker:// and an image"
                ));
            }
        }
        let run = text(&format!("CI_STEP_{n}_RUN"))?.filter(|run| !run.trim().is_empty());
        let run = match (run, &uses) {
            (Some(run), _) => run,
            (None, Some(_)) => String::new(),
            (None, None) => {
                return Err(format!(
                    "CI_STEP_{n}_RUN is not set: step {n} has no script"
                ))
            }
        };
        let name = text(&format!("CI_STEP_{n}_NAME"))?.unwrap_or_else(|| format!("step {n}"));
        let timeout = match text(&format!("CI_STEP_{n}_TIMEOUT_SECS"))? {
            None => None,
            Some(secs) => Some(Duration::from_secs(
                secs.parse::<u64>()
                    .ok()
                    .filter(|s| (1..=MAX_TIMEOUT_SECS).contains(s))
                    .ok_or_else(|| {
                        format!(
                            "CI_STEP_{n}_TIMEOUT_SECS={secs:?} is not a number of seconds from 1 \
                             to {MAX_TIMEOUT_SECS}"
                        )
                    })?,
            )),
        };
        let continue_on_error = match text(&format!("CI_STEP_{n}_CONTINUE_ON_ERROR"))?.as_deref() {
            None | Some("false") => false,
            Some("true") => true,
            Some(other) => {
                return Err(format!(
                    "CI_STEP_{n}_CONTINUE_ON_ERROR={other:?} must be true or false"
                ))
            }
        };
        let prefix = format!("CI_STEP_{n}_ENV_");
        let mut env = Vec::new();
        for (key, value) in &vars {
            let Some(name) = key.to_str().and_then(|k| k.strip_prefix(&prefix)) else {
                continue;
            };
            if !is_variable_name(name) {
                return Err(format!("{prefix}{name}: {name:?} is not a variable name"));
            }
            env.push((OsString::from(name), value.clone()));
        }
        let mut step = Step {
            n,
            name,
            run,
            timeout,
            continue_on_error,
            env,
            ..Step::default()
        };
        if protocol >= 2 {
            let own = |key: &str| text(&format!("CI_STEP_{n}_{key}"));
            step.id = own("ID")?;
            step.shell = own("SHELL")?;
            step.workdir = own("WORKDIR")?;
            step.condition = own("IF")?;
            let prefix = format!("CI_STEP_{n}_ENVX_");
            for (key, value) in &vars {
                let Some(name) = key.to_str().and_then(|k| k.strip_prefix(&prefix)) else {
                    continue;
                };
                if !is_variable_name(name) {
                    return Err(format!("{prefix}{name}: {name:?} is not a variable name"));
                }
                let value = value
                    .to_str()
                    .ok_or_else(|| format!("{prefix}{name} is not UTF-8"))?;
                step.late_env.push((name.to_string(), value.to_string()));
            }
            while let Some(expression) = own(&format!("EXPR_{}", step.exprs.len() + 1))? {
                step.exprs.push(expression);
            }
        }
        if uses.is_some() {
            let own = |key: &str| text(&format!("CI_STEP_{n}_{key}"));
            while let Some(name) = own(&format!("WITH_{}_NAME", step.with.len() + 1))? {
                if step.with.len() == MAX_WITH {
                    return Err(format!(
                        "step {n} gives its action more than {MAX_WITH} inputs"
                    ));
                }
                if name.is_empty() {
                    let k = step.with.len() + 1;
                    return Err(format!(
                        "CI_STEP_{n}_WITH_{k}_NAME is empty: an input has a name"
                    ));
                }
                let value =
                    own(&format!("WITH_{}_VALUE", step.with.len() + 1))?.unwrap_or_default();
                step.with.push((name, value));
            }
            step.uses = uses;
        }
        steps.push(step);
    }
    let context = match text("CI_CONTEXT")? {
        Some(json) if protocol >= 2 => {
            Some(serde_json::from_str(&json).map_err(|e| format!("CI_CONTEXT is not JSON: {e}"))?)
        }
        _ => None,
    };
    // What a job is beside its steps: protocol 3's.
    let counted = |name: &str, most: usize| -> Result<usize, String> {
        match text(name)? {
            None => Ok(0),
            Some(count) => count
                .parse::<usize>()
                .ok()
                .filter(|&count| count <= most)
                .ok_or_else(|| format!("{name}={count:?} is not a number from 0 to {most}")),
        }
    };
    let (mut job_if, mut needs, mut outputs) = (None, Vec::new(), Vec::new());
    let (mut matrix, mut strategy) = (None, None);
    if protocol >= 3 {
        for (name, into) in [("CI_MATRIX", &mut matrix), ("CI_STRATEGY", &mut strategy)] {
            if let Some(json) = text(name)?.filter(|json| !json.trim().is_empty()) {
                *into = Some(
                    serde_json::from_str::<serde_json::Value>(&json)
                        .ok()
                        .filter(serde_json::Value::is_object)
                        .ok_or_else(|| format!("{name} is not a JSON object"))?,
                );
            }
        }
        job_if = text("CI_JOB_IF")?.filter(|text| !text.trim().is_empty());
        for k in 1..=counted("CI_NEED_COUNT", MAX_NEEDS)? {
            let own = |key: &str| text(&format!("CI_NEED_{k}_{key}"));
            needs.push(crate::job::Need {
                job: own("JOB")?
                    .filter(|job| !job.is_empty())
                    .ok_or_else(|| format!("CI_NEED_{k}_JOB is not set: the job it is"))?,
                status: own("STATUS")?.unwrap_or_default(),
                said: own("SAID")?.unwrap_or_default(),
            });
        }
        for k in 1..=counted("CI_OUTPUT_COUNT", MAX_OUTPUTS)? {
            let own = |key: &str| text(&format!("CI_OUTPUT_{k}_{key}"));
            let name = own("NAME")?
                .filter(|name| !name.is_empty())
                .ok_or_else(|| format!("CI_OUTPUT_{k}_NAME is not set: the output's name"))?;
            outputs.push((name, own("VALUE")?.unwrap_or_default()));
        }
    }

    let result_file = vars
        .get(OsStr::new("CI_RESULT_FILE"))
        .filter(|path| !path.is_empty())
        .map(PathBuf::from);
    let paths = |name: &str| -> Result<Vec<PathBuf>, String> {
        Ok(text(name)?
            .unwrap_or_default()
            .lines()
            .map(str::trim)
            .filter(|line| !line.is_empty())
            .map(PathBuf::from)
            .collect())
    };
    let reports = crate::report::Reports {
        junit: paths("CI_REPORT_JUNIT")?,
        sarif: paths("CI_REPORT_SARIF")?,
    };
    let grace = match text("CI_STEP_GRACE_SECS")? {
        None => DEFAULT_GRACE,
        Some(secs) => Duration::from_secs(
            secs.parse::<u64>()
                .ok()
                .filter(|&s| s <= MAX_GRACE_SECS)
                .ok_or_else(|| {
                    format!(
                        "CI_STEP_GRACE_SECS={secs:?} is not a number from 0 to {MAX_GRACE_SECS}"
                    )
                })?,
        ),
    };
    Ok(Job {
        steps,
        result_file,
        grace,
        protocol,
        context,
        job_if,
        needs,
        outputs,
        matrix,
        strategy,
        reports,
    })
}

/// A letter or `_`, then letters, digits or `_`: what a shell can name.
pub(crate) fn is_variable_name(name: &str) -> bool {
    let mut chars = name.chars();
    chars
        .next()
        .is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(pairs: &[(&str, &str)]) -> Vec<(OsString, OsString)> {
        pairs
            .iter()
            .map(|(k, v)| (OsString::from(k), OsString::from(v)))
            .collect()
    }

    fn base() -> Vec<(&'static str, &'static str)> {
        vec![("CI_STEP_PROTOCOL", "1"), ("CI_STEP_COUNT", "2")]
    }

    #[test]
    fn a_task_s_steps_are_read_with_everything_they_carry() {
        let mut pairs = base();
        pairs.extend([
            ("CI_STEP_1_NAME", "build"),
            ("CI_STEP_1_RUN", "make"),
            ("CI_STEP_1_TIMEOUT_SECS", "600"),
            ("CI_STEP_1_ENV_PROFILE", "release"),
            ("CI_STEP_1_ENV_FOO_ENV_BAR", "x"),
            ("CI_STEP_2_RUN", "make test"),
            ("CI_STEP_2_CONTINUE_ON_ERROR", "true"),
            // Step 11's variable is not step 1's.
            ("CI_STEP_11_ENV_OTHER", "no"),
            ("CI_RESULT_FILE", "/workspace/.dagron-ci/result.json"),
            ("CI_STEP_GRACE_SECS", "3"),
            ("PATH", "/usr/bin"),
        ]);
        let job = read(vars(&pairs)).unwrap();
        assert_eq!(job.steps.len(), 2);
        let build = &job.steps[0];
        assert_eq!(
            (build.n, build.name.as_str(), build.run.as_str()),
            (1, "build", "make")
        );
        assert_eq!(build.timeout, Some(Duration::from_secs(600)));
        assert!(!build.continue_on_error);
        assert_eq!(
            build.env,
            vec![
                (OsString::from("FOO_ENV_BAR"), OsString::from("x")),
                (OsString::from("PROFILE"), OsString::from("release")),
            ]
        );
        let test = &job.steps[1];
        assert_eq!(test.name, "step 2", "a step without a name is numbered");
        assert!(test.continue_on_error && test.env.is_empty() && test.timeout.is_none());
        assert_eq!(
            job.result_file,
            Some(PathBuf::from("/workspace/.dagron-ci/result.json"))
        );
        assert_eq!(job.grace, Duration::from_secs(3));

        let none = read(vars(&[("CI_STEP_PROTOCOL", "1"), ("CI_STEP_COUNT", "0")])).unwrap();
        assert!(none.steps.is_empty() && none.result_file.is_none());
        assert_eq!(none.grace, DEFAULT_GRACE);
    }

    #[test]
    fn an_environment_that_is_not_protocol_1_is_refused() {
        let with = |extra: &[(&'static str, &'static str)]| {
            let mut pairs = base();
            pairs.extend([("CI_STEP_1_RUN", "a"), ("CI_STEP_2_RUN", "b")]);
            pairs.extend_from_slice(extra);
            read(vars(&pairs))
        };
        assert!(with(&[]).is_ok());
        for (bad, why) in [
            (vec![("CI_STEP_PROTOCOL", "6")], "speaks 1, 2, 3, 4 and 5"),
            (vec![("CI_STEP_COUNT", "x")], "CI_STEP_COUNT"),
            (vec![("CI_STEP_COUNT", "1001")], "CI_STEP_COUNT"),
            (vec![("CI_STEP_3_RUN", "c")], "not all counted"),
            (vec![("CI_STEP_2_RUN", " ")], "step 2 has no script"),
            (vec![("CI_STEP_1_TIMEOUT_SECS", "0")], "TIMEOUT_SECS"),
            (vec![("CI_STEP_1_TIMEOUT_SECS", "1m")], "TIMEOUT_SECS"),
            (
                vec![("CI_STEP_1_CONTINUE_ON_ERROR", "yes")],
                "true or false",
            ),
            (vec![("CI_STEP_1_ENV_1BAD", "x")], "not a variable name"),
            (vec![("CI_STEP_GRACE_SECS", "601")], "CI_STEP_GRACE_SECS"),
        ] {
            let mut pairs = base();
            pairs.extend([("CI_STEP_1_RUN", "a"), ("CI_STEP_2_RUN", "b")]);
            // The bad value replaces the good one.
            pairs.retain(|(k, _)| !bad.iter().any(|(b, _)| b == k));
            pairs.extend(bad.iter().copied());
            let err = read(vars(&pairs)).unwrap_err();
            assert!(err.contains(why), "{bad:?}: {err}");
        }
        let unset = read(vars(&[("PATH", "/bin")])).unwrap_err();
        assert!(unset.contains("CI_STEP_PROTOCOL is not set"), "{unset}");
        let uncounted = read(vars(&[("CI_STEP_PROTOCOL", "1")])).unwrap_err();
        assert!(
            uncounted.contains("CI_STEP_COUNT is not set"),
            "{uncounted}"
        );
    }

    #[test]
    fn protocol_2_adds_what_a_github_step_has() {
        let job = read(vars(&[
            ("CI_STEP_PROTOCOL", "2"),
            ("CI_STEP_COUNT", "2"),
            ("CI_CONTEXT", "{\"matrix\":{\"os\":\"linux\"}}"),
            ("CI_STEP_1_RUN", "echo ${__CI_E1} ${__CI_E2}"),
            ("CI_STEP_1_ID", "build"),
            ("CI_STEP_1_SHELL", "bash"),
            ("CI_STEP_1_WORKDIR", "crates/${{ matrix.os }}"),
            ("CI_STEP_1_IF", "always()"),
            ("CI_STEP_1_ENV_PLAIN", "1"),
            ("CI_STEP_1_ENVX_LATE", "${{ env.X }}"),
            ("CI_STEP_1_EXPR_1", "github.head_ref"),
            ("CI_STEP_1_EXPR_2", "inputs.name"),
            // Not counted: there is no third.
            ("CI_STEP_1_EXPR_4", "never read"),
            ("CI_STEP_2_RUN", "true"),
        ]))
        .unwrap();
        assert_eq!(job.protocol, 2);
        assert_eq!(job.context.as_ref().unwrap()["matrix"]["os"], "linux");
        let build = &job.steps[0];
        assert_eq!(build.id.as_deref(), Some("build"));
        assert_eq!(build.shell.as_deref(), Some("bash"));
        assert_eq!(build.workdir.as_deref(), Some("crates/${{ matrix.os }}"));
        assert_eq!(build.condition.as_deref(), Some("always()"));
        assert_eq!(
            build.env,
            vec![(OsString::from("PLAIN"), OsString::from("1"))]
        );
        assert_eq!(
            build.late_env,
            [("LATE".to_string(), "${{ env.X }}".to_string())]
        );
        assert_eq!(build.exprs, ["github.head_ref", "inputs.name"]);
        let plain = &job.steps[1];
        assert!(plain.id.is_none() && plain.shell.is_none() && plain.exprs.is_empty());

        // Under protocol 1 none of it is read: a native step has none.
        let one = read(vars(&[
            ("CI_STEP_PROTOCOL", "1"),
            ("CI_STEP_COUNT", "1"),
            ("CI_STEP_1_RUN", "true"),
            ("CI_STEP_1_SHELL", "bash"),
            ("CI_CONTEXT", "not even JSON"),
        ]))
        .unwrap();
        assert_eq!(one.protocol, 1);
        assert!(one.steps[0].shell.is_none() && one.context.is_none());
        let bad = read(vars(&[
            ("CI_STEP_PROTOCOL", "2"),
            ("CI_STEP_COUNT", "0"),
            ("CI_CONTEXT", "{nope"),
        ]))
        .unwrap_err();
        assert!(bad.contains("CI_CONTEXT is not JSON"), "{bad}");
    }

    #[test]
    fn protocol_4_adds_a_step_that_uses_a_local_action() {
        let job = read(vars(&[
            ("CI_STEP_PROTOCOL", "4"),
            ("CI_STEP_COUNT", "2"),
            ("CI_STEP_1_RUN", ""),
            ("CI_STEP_1_USES", "./.github/actions/greet"),
            ("CI_STEP_1_WITH_1_NAME", "who"),
            ("CI_STEP_1_WITH_1_VALUE", "${{ github.actor }}"),
            ("CI_STEP_1_WITH_2_NAME", "loud"),
            ("CI_STEP_1_WITH_2_VALUE", "true"),
            // Not counted: there is no third.
            ("CI_STEP_1_WITH_4_NAME", "never"),
            ("CI_STEP_2_RUN", "true"),
        ]))
        .unwrap();
        assert_eq!(job.protocol, 4);
        let greet = &job.steps[0];
        assert_eq!(greet.uses.as_deref(), Some("./.github/actions/greet"));
        assert_eq!(greet.run, "", "a step that uses an action has no script");
        assert_eq!(
            greet.with,
            [
                ("who".to_string(), "${{ github.actor }}".to_string()),
                ("loud".to_string(), "true".to_string()),
            ]
        );
        assert!(job.steps[1].uses.is_none() && job.steps[1].with.is_empty());

        // An action of another repository is one too, as `uses:` names it.
        let remote = read(vars(&[
            ("CI_STEP_PROTOCOL", "4"),
            ("CI_STEP_COUNT", "1"),
            ("CI_STEP_1_USES", "actions/setup-node@v4"),
        ]))
        .unwrap();
        assert_eq!(
            remote.steps[0].uses.as_deref(),
            Some("actions/setup-node@v4")
        );
        for (uses, why) in [
            ("actions/setup-node", "is not an action"),
            ("docker://", "is not an action"),
            ("docker://a b", "is not an action"),
            ("./../outside", "is not an action"),
            ("./a/../../outside", "is not an action"),
            (".//etc", "is not an action"),
        ] {
            let err = read(vars(&[
                ("CI_STEP_PROTOCOL", "4"),
                ("CI_STEP_COUNT", "1"),
                ("CI_STEP_1_USES", uses),
            ]))
            .unwrap_err();
            assert!(err.contains(why), "{uses}: {err}");
        }
        let unnamed = read(vars(&[
            ("CI_STEP_PROTOCOL", "4"),
            ("CI_STEP_COUNT", "1"),
            ("CI_STEP_1_USES", "./a"),
            ("CI_STEP_1_WITH_1_NAME", ""),
        ]))
        .unwrap_err();
        assert!(unnamed.contains("an input has a name"), "{unnamed}");
        // Under protocol 3 a step uses nothing, and one with no script is
        // refused as before.
        let three = read(vars(&[
            ("CI_STEP_PROTOCOL", "3"),
            ("CI_STEP_COUNT", "1"),
            ("CI_STEP_1_USES", "./a"),
        ]))
        .unwrap_err();
        assert!(three.contains("step 1 has no script"), "{three}");
    }
}
