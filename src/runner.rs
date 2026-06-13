// SPDX-License-Identifier: Apache-2.0
//! In-memory DAG runner.
//!
//! A dependency-driven scheduler: every task with all of its dependencies
//! satisfied runs concurrently through an [`Executor`]. On success its dependents
//! are decremented and become eligible; on failure (after retries) its entire
//! downstream subtree is skipped. This is the zero-infra reference scheduler —
//! durable, multi-node backends live in separate distributions but execute the
//! same DAG model.

use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use tokio::task::JoinSet;
use tokio::time::{sleep, Duration};

use crate::dag::{DagGraph, TaskSpec};
use crate::executor::{ExecContext, Executor};

/// Terminal state of a single task.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskState {
    Succeeded,
    Failed,
    /// An upstream dependency did not succeed, so this task never ran.
    Skipped,
}

impl std::fmt::Display for TaskState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = match self {
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
            Self::Skipped => "skipped",
        };
        write!(f, "{s}")
    }
}

/// Outcome of one task.
#[derive(Debug, Clone)]
pub struct TaskReport {
    pub name: String,
    pub state: TaskState,
    pub attempts: u32,
    pub output: String,
}

/// Outcome of one DAG run.
#[derive(Debug, Clone)]
pub struct RunReport {
    pub dag: String,
    pub tasks: Vec<TaskReport>,
    pub succeeded: bool,
}

/// Parse, validate, and run a DAG to completion against `executor`.
///
/// Returns once every task has reached a terminal state. `succeeded` is true only
/// if no task failed.
pub async fn run_dag(yaml: &str, executor: Arc<dyn Executor>) -> Result<RunReport> {
    let dag = Arc::new(DagGraph::from_yaml(yaml)?);

    let mut remaining: HashMap<String, usize> = HashMap::new();
    let mut dependents: HashMap<String, Vec<String>> = HashMap::new();
    for task in &dag.spec.tasks {
        remaining.insert(task.name.clone(), dag.dep_count(&task.name));
        for dep in &task.depends_on {
            dependents
                .entry(dep.clone())
                .or_default()
                .push(task.name.clone());
        }
    }

    let mut states: HashMap<String, TaskState> = HashMap::new();
    let mut reports: Vec<TaskReport> = Vec::new();

    // Seed the ready set with the roots (no dependencies).
    let mut ready: Vec<String> = dag
        .spec
        .tasks
        .iter()
        .filter(|t| remaining[&t.name] == 0)
        .map(|t| t.name.clone())
        .collect();

    let mut running: JoinSet<TaskReport> = JoinSet::new();

    loop {
        for name in std::mem::take(&mut ready) {
            let spec = dag.task_spec(&name).expect("ready task exists").clone();
            let exec = executor.clone();
            running.spawn(async move { run_one(spec, exec).await });
        }

        let report = match running.join_next().await {
            Some(joined) => joined?, // propagate panics in a task
            None => break,           // nothing running and nothing newly ready
        };

        let name = report.name.clone();
        let state = report.state;
        states.insert(name.clone(), state);
        reports.push(report);

        let children = dependents.get(&name).cloned().unwrap_or_default();
        for child in children {
            if states.contains_key(&child) {
                continue; // already decided (e.g. skipped via another parent)
            }
            if state == TaskState::Succeeded {
                let r = remaining.get_mut(&child).expect("child tracked");
                *r -= 1;
                if *r == 0 {
                    ready.push(child);
                }
            } else {
                cascade_skip(child, &dependents, &mut states, &mut reports);
            }
        }
    }

    let succeeded = reports.iter().all(|r| r.state != TaskState::Failed);
    Ok(RunReport {
        dag: dag.spec.name.clone(),
        tasks: reports,
        succeeded,
    })
}

/// Mark `start` and every transitive dependent skipped (a parent didn't succeed).
fn cascade_skip(
    start: String,
    dependents: &HashMap<String, Vec<String>>,
    states: &mut HashMap<String, TaskState>,
    reports: &mut Vec<TaskReport>,
) {
    let mut stack = vec![start];
    while let Some(name) = stack.pop() {
        if states.contains_key(&name) {
            continue;
        }
        states.insert(name.clone(), TaskState::Skipped);
        reports.push(TaskReport {
            name: name.clone(),
            state: TaskState::Skipped,
            attempts: 0,
            output: "skipped: an upstream dependency did not succeed".to_string(),
        });
        if let Some(children) = dependents.get(&name) {
            stack.extend(children.iter().cloned());
        }
    }
}

/// Run one task with retries and exponential backoff.
async fn run_one(spec: TaskSpec, exec: Arc<dyn Executor>) -> TaskReport {
    let max = spec.max_attempts.max(1);
    let mut last_output = String::new();

    for attempt in 1..=max {
        let ctx = ExecContext {
            command: spec.command.clone(),
            timeout_secs: spec.timeout_secs,
        };
        match exec.execute(&ctx).await {
            Ok(out) if out.success => {
                return TaskReport {
                    name: spec.name,
                    state: TaskState::Succeeded,
                    attempts: attempt,
                    output: out.output,
                };
            }
            Ok(out) => last_output = out.output,
            Err(e) => last_output = format!("error: {e}"),
        }
        if attempt < max && spec.retry_delay_secs > 0 {
            let backoff = spec
                .retry_delay_secs
                .saturating_mul(2u64.saturating_pow(attempt - 1));
            sleep(Duration::from_secs(backoff)).await;
        }
    }

    TaskReport {
        name: spec.name,
        state: TaskState::Failed,
        attempts: max,
        output: last_output,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::executor::{ExecContext, ExecOutput, Executor};
    use async_trait::async_trait;
    use std::collections::HashSet;

    /// Executor whose success/failure is keyed off the command's first arg.
    struct ScriptedExecutor {
        fail: HashSet<String>,
    }

    #[async_trait]
    impl Executor for ScriptedExecutor {
        async fn execute(&self, ctx: &ExecContext) -> Result<ExecOutput> {
            let name = ctx.command.first().cloned().unwrap_or_default();
            Ok(ExecOutput {
                success: !self.fail.contains(&name),
                output: name,
            })
        }
    }

    fn state_of<'a>(r: &'a RunReport, name: &str) -> &'a TaskState {
        &r.tasks.iter().find(|t| t.name == name).unwrap().state
    }

    #[tokio::test]
    async fn diamond_all_succeed() {
        let yaml = r#"
name: diamond
tasks:
  - { name: a, command: ["a"] }
  - { name: b, command: ["b"], depends_on: ["a"] }
  - { name: c, command: ["c"], depends_on: ["a"] }
  - { name: d, command: ["d"], depends_on: ["b", "c"] }
"#;
        let exec = Arc::new(ScriptedExecutor {
            fail: HashSet::new(),
        });
        let report = run_dag(yaml, exec).await.unwrap();
        assert!(report.succeeded);
        assert_eq!(report.tasks.len(), 4);
    }

    #[tokio::test]
    async fn failure_cascades_skip_downstream() {
        let yaml = r#"
name: cascade
tasks:
  - { name: a, command: ["a"] }
  - { name: b, command: ["b"], depends_on: ["a"] }
  - { name: c, command: ["c"], depends_on: ["b"] }
  - { name: indep, command: ["indep"] }
"#;
        let exec = Arc::new(ScriptedExecutor {
            fail: HashSet::from(["b".to_string()]),
        });
        let report = run_dag(yaml, exec).await.unwrap();
        assert!(!report.succeeded);
        assert_eq!(*state_of(&report, "a"), TaskState::Succeeded);
        assert_eq!(*state_of(&report, "b"), TaskState::Failed);
        assert_eq!(*state_of(&report, "c"), TaskState::Skipped);
        assert_eq!(*state_of(&report, "indep"), TaskState::Succeeded);
    }
}
