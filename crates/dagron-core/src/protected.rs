//! A protected environment, to a run that is not trusted: an environment
//! whose policy names `approvers` is used by such a run only once one of
//! them has approved it.
//!
//! The approval is an ordinary gate (`type: approval`), which the engine puts
//! before each task of such a run that names the environment, as the run is
//! made ([`gated`]): the task depends on it, and it on what the task depended
//! on, so it asks when the task would otherwise start, with what came before
//! it to read. Everything a gate has, it has: it is listed among the
//! approvals, decided by its approvers alone, and rejected when nobody
//! answers in [`GATE_TIMEOUT_SECS`].
//!
//! The run's own author writes no gate: a confined run may not ask this
//! installation's people for a decision ([`crate::confine`]). This one is the
//! operator's, written by the policy, and put in after the run was held to
//! what it may have.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};

use crate::dag::{DagGraph, TaskSpec};
use crate::envpolicy::{self, EnvironmentPolicy};
use crate::models::RunTrigger;

/// How long a protected environment's gate waits for a person before it is
/// taken as refused, and its task is skipped.
pub const GATE_TIMEOUT_SECS: u64 = 86_400;

/// One task that waits on a gate: its place in the spec, its environment,
/// and who may approve.
type Asked = (usize, String, Vec<String>);

/// The graph a run started by `trigger` is made of, with a gate before each
/// task that uses a protected environment ([`with_gates`]). `None` where it
/// is the one given: a trusted run's, and a run none of whose environments
/// asks it for an approval.
pub async fn gated(
    pool: &crate::db::Pool,
    dag: &DagGraph,
    trigger: Option<&RunTrigger>,
) -> Result<Option<DagGraph>> {
    let class = envpolicy::class_of(trigger);
    if !crate::confine::confined(class) {
        return Ok(None);
    }
    let mut policies: BTreeMap<String, Option<EnvironmentPolicy>> = BTreeMap::new();
    let mut asked: Vec<Asked> = Vec::new();
    for (i, task) in dag.spec.tasks.iter().enumerate() {
        if task.is_approval() {
            continue;
        }
        let Some(env) = task
            .environment
            .as_deref()
            .or(dag.spec.environment.as_deref())
        else {
            continue;
        };
        if !policies.contains_key(env) {
            let policy = crate::db::environment_policy(pool, env)
                .await
                .with_context(|| format!("loading the policy of environment '{env}'"))?;
            policies.insert(env.to_string(), policy);
        }
        let approvers = envpolicy::approvers_for(policies[env].as_ref(), class);
        if !approvers.is_empty() {
            asked.push((i, env.to_string(), approvers.to_vec()));
        }
    }
    if asked.is_empty() {
        return Ok(None);
    }
    let actor = trigger.and_then(|t| t.actor.as_deref());
    with_gates(dag, &asked, class, actor).map(Some)
}

/// `dag` with a gate before each task `asked` names: one that its
/// `approvers` decide, which depends on what the task depended on, and runs
/// as the task's own `when:` and trigger rule say; and the task depending on
/// it as well.
fn with_gates(
    dag: &DagGraph,
    asked: &[Asked],
    class: &str,
    actor: Option<&str>,
) -> Result<DagGraph> {
    let mut spec = dag.spec.clone();
    let mut names: BTreeSet<String> = spec.tasks.iter().map(|t| t.name.clone()).collect();
    let mut gates = Vec::with_capacity(asked.len());
    for (i, env, approvers) in asked {
        let task = &mut spec.tasks[*i];
        let mut name = format!("{}.environment", task.name);
        let mut n = 2;
        while names.contains(&name) {
            name = format!("{}.environment-{n}", task.name);
            n += 1;
        }
        names.insert(name.clone());
        let who = match actor {
            Some(actor) => format!("{actor}'s run"),
            None => "A run".to_string(),
        };
        let gate: TaskSpec = serde_json::from_value(serde_json::json!({
            "name": name,
            "type": "approval",
            "depends_on": task.depends_on,
            "when": task.when,
            "trigger_rule": task.trigger_rule,
            "approvers": approvers,
            "approval_message": format!(
                "{who}, which is {class}, asks to use the protected environment '{env}' in task \
                 '{}'. Approve it to let that task run.",
                task.name
            ),
            "approval_timeout_secs": GATE_TIMEOUT_SECS,
            "approval_on_timeout": "reject",
        }))
        .context("making a protected environment's gate")?;
        task.depends_on.push(name);
        gates.push(gate);
    }
    spec.tasks.extend(gates);
    let mut graph = DagGraph::from_spec(spec)?;
    graph.parameters = dag.parameters.clone();
    Ok(graph)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dag(yaml: &str) -> DagGraph {
        DagGraph::from_yaml(yaml).unwrap()
    }

    /// Each task that names the environment waits on a gate of its own,
    /// asked after what the task waited on, and decided by the policy's
    /// approvers; a task that names none waits on nothing new.
    #[test]
    fn a_task_of_a_protected_environment_waits_on_a_gate_of_its_own() {
        let graph = dag(
            "name: ci\ntasks:\n  - { name: test, command: [\"true\"] }\n  \
             - { name: deploy, command: [\"true\"], depends_on: [test], environment: prod, when: \"{{ tasks.test.output }} == ok\" }\n",
        );
        let asked = vec![(1, "prod".to_string(), vec!["group:release".to_string()])];
        let gated = with_gates(&graph, &asked, "agent", Some("agent:fixer")).unwrap();
        let task = |name: &str| gated.spec.tasks.iter().find(|t| t.name == name).unwrap();
        let gate = task("deploy.environment");
        assert!(gate.is_approval());
        assert_eq!(gate.depends_on, ["test"]);
        assert_eq!(gate.approvers, ["group:release"]);
        assert_eq!(gate.when, task("deploy").when);
        assert!(
            gate.when
                .as_deref()
                .is_some_and(|w| w.contains("tasks.test.output")),
            "{:?}",
            gate.when
        );
        assert_eq!(gate.approval_timeout_secs, Some(GATE_TIMEOUT_SECS));
        assert_eq!(gate.approval_on_timeout.as_deref(), Some("reject"));
        assert!(
            gate.approval_message.as_deref().unwrap().starts_with(
                "agent:fixer's run, which is agent, asks to use the protected environment 'prod' in task 'deploy'."
            ),
            "{:?}",
            gate.approval_message
        );
        assert_eq!(task("deploy").depends_on, ["test", "deploy.environment"]);
        assert!(task("test").depends_on.is_empty());

        // A name the workflow has already is not taken.
        let taken = dag(
            "name: ci\ntasks:\n  - { name: deploy, command: [\"true\"] }\n  \
             - { name: deploy.environment, command: [\"true\"] }\n",
        );
        let asked = vec![(0, "prod".to_string(), vec!["ops@acme.example".to_string()])];
        let gated = with_gates(&taken, &asked, "agent", None).unwrap();
        let gate = gated.spec.tasks.last().unwrap();
        assert_eq!(gate.name, "deploy.environment-2");
        assert!(gate
            .approval_message
            .as_deref()
            .unwrap()
            .starts_with("A run, which is agent"));
    }
}
