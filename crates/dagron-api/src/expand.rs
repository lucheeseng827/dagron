//! Chain saved workflows: resolve every `workflow_ref` into a flat, leaf-only DAG.
//!
//! A workflow authored in the UI can use a task to **call another saved
//! workflow** instead of running a command:
//!
//! ```yaml
//! name: nightly
//! tasks:
//!   - { name: prepare, command: ["sh", "-c", "echo prep"] }
//!   - { name: etl,     workflow_ref: daily-etl, depends_on: [prepare] }   # ← chain
//!   - { name: notify,  command: ["sh", "-c", "echo done"], depends_on: [etl] }
//! ```
//!
//! At run-creation (the `POST /api/runs` and `POST /api/workflows/:id/run`
//! paths) dagron-api loads the referenced workflow's spec from the `workflows`
//! table and **inlines** its tasks in place of the call task, namespaced under
//! the call's name (`etl.<task>`). Dependencies are rewired so the call's
//! upstreams feed the sub-DAG's roots and the call's downstreams wait on its
//! exits — exactly the engine's inline-template expansion ([`crate::dag`] in the
//! engine), but the "template" is a stored workflow resolved over the DB.
//!
//! The result is an ordinary flat DAG: the engine and its executors never see a
//! `workflow_ref`, so nothing downstream changes — a chained workflow is just a
//! bigger DAG. References resolve recursively (a child may chain its own
//! children); cross-workflow cycles, runaway nesting, and reference explosions
//! fail loudly with a 400 rather than looping or exhausting memory.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use axum::http::StatusCode;

use crate::routes::control::{validate_graph, DagSpecInput, TaskSpecInput};
use crate::state::AppState;

/// Bound on `workflow_ref` nesting depth. A chain deeper than this is almost
/// certainly an unintended cycle the name-based guard didn't catch; fail rather
/// than recurse without end.
const MAX_DEPTH: usize = 32;
/// Bound on the total expanded task count so a wide/deep reference fan-out fails
/// loudly instead of exhausting memory.
const MAX_TASKS: usize = 10_000;

/// Resolve every `workflow_ref` in `spec` into a flat, leaf-only [`DagSpecInput`].
///
/// Two phases: (1) load every transitively-referenced workflow spec from the DB
/// once, then (2) expand purely in memory. Splitting them keeps the recursive
/// rewiring synchronous and unit-testable without a database.
pub(crate) async fn expand_workflow_refs(
    state: &AppState,
    spec: DagSpecInput,
) -> Result<DagSpecInput, (StatusCode, String)> {
    // Fast path: a workflow with no chains needs no DB work or rewiring.
    if !references_any(&spec) {
        return Ok(spec);
    }
    let refs = collect_referenced_specs(state, &spec).await?;
    expand_pure(spec, &refs).map_err(|m| (StatusCode::BAD_REQUEST, m))
}

/// True if any task in `spec` chains another workflow.
fn references_any(spec: &DagSpecInput) -> bool {
    spec.tasks.iter().any(|t| t.workflow_ref.is_some())
}

/// The names a spec directly chains (one entry per `workflow_ref` task).
fn direct_refs(spec: &DagSpecInput) -> Vec<String> {
    spec.tasks
        .iter()
        .filter_map(|t| t.workflow_ref.clone())
        .collect()
}

/// Load every workflow transitively referenced from `root`, keyed by name. Dedups
/// by name, so even a reference cycle terminates here (the cycle itself is
/// reported during expansion, with the offending chain in the message).
async fn collect_referenced_specs(
    state: &AppState,
    root: &DagSpecInput,
) -> Result<HashMap<String, DagSpecInput>, (StatusCode, String)> {
    let mut out: HashMap<String, DagSpecInput> = HashMap::new();
    let mut stack: Vec<String> = direct_refs(root);
    while let Some(name) = stack.pop() {
        if out.contains_key(&name) {
            continue;
        }
        let yaml: Option<String> = sqlx::query_scalar("SELECT spec FROM workflows WHERE name = $1")
            .bind(&name)
            .fetch_optional(&state.read_pool)
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, "loading referenced workflow");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal server error".to_string(),
                )
            })?;
        let yaml = yaml.ok_or_else(|| {
            (
                StatusCode::BAD_REQUEST,
                format!("references unknown workflow '{name}' — save that workflow first"),
            )
        })?;
        let child: DagSpecInput = serde_yaml::from_str(&yaml).map_err(|e| {
            (
                StatusCode::BAD_REQUEST,
                format!("referenced workflow '{name}' has an invalid spec: {e}"),
            )
        })?;
        for r in direct_refs(&child) {
            stack.push(r);
        }
        out.insert(name, child);
    }
    Ok(out)
}

/// One task's expansion: the produced leaf tasks plus the boundary node sets used
/// to rewire dependencies in the enclosing list.
struct Expanded {
    /// Output tasks with no *internal* dependency — they inherit the call's
    /// external `depends_on`.
    roots: Vec<String>,
    /// Output tasks nothing internal depends on — the call's dependents attach here.
    exits: Vec<String>,
    /// Fully-wired leaf tasks (root nodes have empty `depends_on`, filled by the
    /// enclosing list).
    tasks: Vec<TaskSpecInput>,
}

/// Expand `root`'s tasks into a flat DAG using the preloaded reference specs.
fn expand_pure(
    root: DagSpecInput,
    refs: &HashMap<String, DagSpecInput>,
) -> Result<DagSpecInput, String> {
    let mut budget = MAX_TASKS;
    // The path seeds cross-workflow cycle detection: the root's own name plus the
    // chain of references currently being expanded.
    let mut path: Vec<String> = vec![root.name.clone()];
    let e = expand_list(&root.tasks, "", refs, &mut path, &mut budget)?;
    // Defense-in-depth: the rewired flat DAG must still be acyclic with unique
    // names (it always is for acyclic inputs, but cheap to assert).
    validate_graph(&root.name, &e.tasks).map_err(|(_, m)| m)?;
    Ok(DagSpecInput {
        name: root.name,
        tasks: e.tasks,
    })
}

/// Expand a list of sibling tasks, wiring their inter-dependencies. Mirrors the
/// engine's `expand::expand_list`: expand each task on its own, then rewire each
/// sub-DAG's roots onto the exit sets of the siblings the task depends on.
fn expand_list(
    tasks: &[TaskSpecInput],
    prefix: &str,
    refs: &HashMap<String, DagSpecInput>,
    path: &mut Vec<String>,
    budget: &mut usize,
) -> Result<Expanded, String> {
    // Expand each task first (deps wired in a second pass).
    let mut per: BTreeMap<String, Expanded> = BTreeMap::new();
    for t in tasks {
        if per.contains_key(&t.name) {
            return Err(format!("duplicate task name '{}'", t.name));
        }
        let e = expand_one(t, prefix, refs, path, budget)?;
        per.insert(t.name.clone(), e);
    }

    // Sibling names that something depends on — used to compute the list's exits.
    let mut depended: BTreeSet<&str> = BTreeSet::new();
    for t in tasks {
        for d in &t.depends_on {
            depended.insert(d.as_str());
        }
    }

    let mut out_tasks: Vec<TaskSpecInput> = Vec::new();
    let mut list_roots: Vec<String> = Vec::new();
    let mut list_exits: Vec<String> = Vec::new();

    for t in tasks {
        // External deps for this task = the exit nodes of each sibling it depends on.
        let mut dep_exits: Vec<String> = Vec::new();
        for d in &t.depends_on {
            let de = per
                .get(d)
                .ok_or_else(|| format!("task '{}' depends on unknown task '{}'", t.name, d))?;
            dep_exits.extend(de.exits.iter().cloned());
        }

        let e = &per[&t.name];
        let rootset: BTreeSet<&str> = e.roots.iter().map(String::as_str).collect();
        for task in &e.tasks {
            let mut task = task.clone();
            if rootset.contains(task.name.as_str()) {
                task.depends_on = dep_exits.clone();
            }
            out_tasks.push(task);
        }

        if t.depends_on.is_empty() {
            list_roots.extend(e.roots.iter().cloned());
        }
        if !depended.contains(t.name.as_str()) {
            list_exits.extend(e.exits.iter().cloned());
        }
    }

    Ok(Expanded {
        roots: list_roots,
        exits: list_exits,
        tasks: out_tasks,
    })
}

/// Expand a single task: a leaf becomes one task; a `workflow_ref` call expands
/// the referenced workflow's tasks (recursively) under a `name.` prefix.
fn expand_one(
    task: &TaskSpecInput,
    prefix: &str,
    refs: &HashMap<String, DagSpecInput>,
    path: &mut Vec<String>,
    budget: &mut usize,
) -> Result<Expanded, String> {
    let base = format!("{prefix}{}", task.name);
    match &task.workflow_ref {
        Some(ref_name) => {
            if !task.command.is_empty() {
                return Err(format!(
                    "task '{}' sets both `command` and `workflow_ref` — a task is one or the other",
                    task.name
                ));
            }
            if path.iter().any(|n| n == ref_name) {
                return Err(format!(
                    "workflow reference cycle: {} -> {ref_name}",
                    path.join(" -> ")
                ));
            }
            if path.len() >= MAX_DEPTH {
                return Err(format!(
                    "workflow nesting exceeded depth {MAX_DEPTH} at task '{}' (reference cycle?)",
                    task.name
                ));
            }
            let child = refs.get(ref_name).ok_or_else(|| {
                format!(
                    "task '{}' references unknown workflow '{}'",
                    task.name, ref_name
                )
            })?;

            let sub_prefix = format!("{base}.");
            path.push(ref_name.clone());
            let e = expand_list(&child.tasks, &sub_prefix, refs, path, budget)?;
            path.pop();

            if e.tasks.is_empty() {
                return Err(format!(
                    "task '{}' references workflow '{}', which has no tasks",
                    task.name, ref_name
                ));
            }
            Ok(e)
        }
        None => {
            if task.command.is_empty() {
                return Err(format!(
                    "task '{}' has neither a `command` (leaf) nor a `workflow_ref` (chain)",
                    task.name
                ));
            }
            if *budget == 0 {
                return Err(format!(
                    "workflow expanded past {MAX_TASKS} tasks — a reference fan-out blew up"
                ));
            }
            *budget -= 1;
            Ok(Expanded {
                roots: vec![base.clone()],
                exits: vec![base.clone()],
                tasks: vec![leaf(task, base)],
            })
        }
    }
}

/// Materialize an inlined leaf: the called task's fields under the namespaced
/// `name`, with `depends_on` cleared (the enclosing list wires it) and no
/// `workflow_ref` (it has been resolved).
fn leaf(task: &TaskSpecInput, name: String) -> TaskSpecInput {
    TaskSpecInput {
        name,
        command: task.command.clone(),
        depends_on: vec![],
        input: task.input.clone(),
        max_attempts: task.max_attempts,
        retry_delay_secs: task.retry_delay_secs,
        timeout_secs: task.timeout_secs,
        docker_image: task.docker_image.clone(),
        workflow_ref: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn parse(yaml: &str) -> DagSpecInput {
        serde_yaml::from_str(yaml).expect("parse spec")
    }

    /// name -> sorted depends_on, for order-independent assertions.
    fn deps(spec: &DagSpecInput) -> BTreeMap<String, Vec<String>> {
        spec.tasks
            .iter()
            .map(|t| {
                let mut d = t.depends_on.clone();
                d.sort();
                (t.name.clone(), d)
            })
            .collect()
    }

    fn names(spec: &DagSpecInput) -> Vec<String> {
        let mut n: Vec<String> = spec.tasks.iter().map(|t| t.name.clone()).collect();
        n.sort();
        n
    }

    const ETL: &str = r#"
name: etl
tasks:
  - { name: build,   command: ["true"] }
  - { name: process, command: ["true"], depends_on: [build] }
  - { name: publish, command: ["true"], depends_on: [process] }
"#;

    #[test]
    fn leaf_only_passthrough() {
        let root = parse(
            r#"
name: w
tasks:
  - { name: a, command: ["true"] }
  - { name: b, command: ["true"], depends_on: [a] }
"#,
        );
        let out = expand_pure(root, &HashMap::new()).unwrap();
        assert_eq!(names(&out), vec!["a", "b"]);
        assert_eq!(deps(&out)["b"], vec!["a"]);
    }

    #[test]
    fn workflow_ref_inlines_and_rewires_deps() {
        // prepare -> etl(build->process->publish) -> notify
        let root = parse(
            r#"
name: parent
tasks:
  - { name: prepare, command: ["true"] }
  - { name: etl,     workflow_ref: etl, depends_on: [prepare] }
  - { name: notify,  command: ["true"], depends_on: [etl] }
"#,
        );
        let mut refs = HashMap::new();
        refs.insert("etl".to_string(), parse(ETL));
        let out = expand_pure(root, &refs).unwrap();

        assert_eq!(
            names(&out),
            vec![
                "etl.build",
                "etl.process",
                "etl.publish",
                "notify",
                "prepare"
            ]
        );
        let d = deps(&out);
        // sub-DAG root inherits the call's upstream …
        assert_eq!(d["etl.build"], vec!["prepare"]);
        assert_eq!(d["etl.process"], vec!["etl.build"]);
        assert_eq!(d["etl.publish"], vec!["etl.process"]);
        // … and the call's downstream waits on the sub-DAG's exit.
        assert_eq!(d["notify"], vec!["etl.publish"]);
        // Every surviving task is a runnable leaf — no refs leak to the engine.
        assert!(out
            .tasks
            .iter()
            .all(|t| t.workflow_ref.is_none() && !t.command.is_empty()));
    }

    #[test]
    fn nested_refs_resolve_recursively() {
        let root = parse("name: r\ntasks:\n  - { name: a, workflow_ref: c }\n");
        let mut refs = HashMap::new();
        refs.insert(
            "c".to_string(),
            parse("name: c\ntasks:\n  - { name: b, workflow_ref: g }\n"),
        );
        refs.insert(
            "g".to_string(),
            parse("name: g\ntasks:\n  - { name: step, command: [\"true\"] }\n"),
        );
        let out = expand_pure(root, &refs).unwrap();
        assert_eq!(names(&out), vec!["a.b.step"]);
        assert_eq!(out.tasks[0].command, vec!["true"]);
    }

    #[test]
    fn fan_in_over_two_chained_workflows() {
        // two parallel calls to the same workflow, joined by a leaf
        let root = parse(
            r#"
name: parent
tasks:
  - { name: left,  workflow_ref: etl }
  - { name: right, workflow_ref: etl }
  - { name: join,  command: ["true"], depends_on: [left, right] }
"#,
        );
        let mut refs = HashMap::new();
        refs.insert("etl".to_string(), parse(ETL));
        let out = expand_pure(root, &refs).unwrap();
        // join fans in over both sub-DAG exits
        assert_eq!(deps(&out)["join"], vec!["left.publish", "right.publish"]);
    }

    #[test]
    fn self_reference_is_rejected() {
        let root = parse("name: a\ntasks:\n  - { name: t, workflow_ref: a }\n");
        let mut refs = HashMap::new();
        refs.insert(
            "a".to_string(),
            parse("name: a\ntasks:\n  - { name: t, workflow_ref: a }\n"),
        );
        let err = expand_pure(root, &refs).unwrap_err();
        assert!(err.contains("cycle"), "got: {err}");
    }

    #[test]
    fn mutual_reference_cycle_is_rejected() {
        let root = parse("name: a\ntasks:\n  - { name: t, workflow_ref: b }\n");
        let mut refs = HashMap::new();
        refs.insert(
            "a".to_string(),
            parse("name: a\ntasks:\n  - { name: t, workflow_ref: b }\n"),
        );
        refs.insert(
            "b".to_string(),
            parse("name: b\ntasks:\n  - { name: t, workflow_ref: a }\n"),
        );
        let err = expand_pure(root, &refs).unwrap_err();
        assert!(err.contains("cycle"), "got: {err}");
    }

    #[test]
    fn unknown_reference_is_rejected() {
        let root = parse("name: w\ntasks:\n  - { name: a, workflow_ref: nope }\n");
        let err = expand_pure(root, &HashMap::new()).unwrap_err();
        assert!(err.contains("unknown workflow 'nope'"), "got: {err}");
    }

    #[test]
    fn task_with_neither_command_nor_ref_is_rejected() {
        let root = parse("name: w\ntasks:\n  - { name: a }\n");
        let err = expand_pure(root, &HashMap::new()).unwrap_err();
        assert!(err.contains("neither"), "got: {err}");
    }

    #[test]
    fn task_with_both_command_and_ref_is_rejected() {
        let root =
            parse("name: w\ntasks:\n  - { name: a, command: [\"true\"], workflow_ref: x }\n");
        let err = expand_pure(root, &HashMap::new()).unwrap_err();
        assert!(err.contains("both"), "got: {err}");
    }
}
