//! Chain saved workflows: resolve every `workflow_ref` into a flat DAG.
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
//! At run creation dagron-api loads the referenced workflow's spec from the
//! `workflows` table and **inlines** its tasks in place of the call task,
//! namespaced under the call's name (`etl.<task>`). Dependencies are rewired so
//! the call's upstreams feed the sub-DAG's roots and the call's downstreams wait
//! on its exits. The result is an ordinary DAG the engine understands —
//! `workflow_ref` never reaches it.
//!
//! **This rewiring happens in YAML space, deliberately.** Every task field is
//! copied as an opaque mapping rather than through a typed mirror of
//! `dag::TaskSpec`. The previous implementation re-declared the spec types here
//! and in `routes::control`, and every field those mirrors had not learned about
//! was silently dropped on submit — fan-out, `resources`, `wait`, `cache`,
//! `pool`, `priority`, `produces`, and more. Copying mappings cannot drift: a
//! field added to the engine's spec flows through untouched, including one added
//! after this code was written.
//!
//! References resolve recursively (a child may chain its own children);
//! cross-workflow cycles, runaway nesting, and reference explosions fail loudly
//! with a 400 rather than looping or exhausting memory.

use std::collections::{BTreeSet, HashMap};

use axum::http::StatusCode;
use serde_yaml::{Mapping, Value};

use crate::state::AppState;

/// Bound on `workflow_ref` nesting depth. A chain deeper than this is almost
/// certainly an unintended cycle the name-based guard didn't catch.
const MAX_DEPTH: usize = 32;
/// Bound on the total expanded task count so a wide/deep fan-out fails loudly
/// instead of exhausting memory.
const MAX_TASKS: usize = 10_000;

type ApiError = (StatusCode, String);

fn bad(msg: impl Into<String>) -> ApiError {
    (StatusCode::BAD_REQUEST, msg.into())
}

/// Resolve every `workflow_ref` in `yaml`, returning the flattened spec YAML.
///
/// Returns the input untouched when nothing chains, so the common path pays only
/// one parse and no database work.
pub(crate) async fn expand_workflow_refs(
    state: &AppState,
    yaml: &str,
) -> Result<String, ApiError> {
    let root: Value = serde_yaml::from_str(yaml).map_err(|e| bad(format!("invalid spec: {e}")))?;
    if direct_refs(&root).is_empty() {
        return Ok(yaml.to_string());
    }
    let refs = collect_referenced_specs(state, &root).await?;
    let expanded = expand_pure(root, &refs)?;
    serde_yaml::to_string(&expanded)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("re-serializing spec: {e}")))
}

/// The task list of a spec, or an empty slice when absent/misshapen — malformed
/// specs are rejected by the engine's parser later with a better message than
/// anything this module could produce.
fn tasks_of(spec: &Value) -> &[Value] {
    spec.get("tasks").and_then(Value::as_sequence).map(|s| s.as_slice()).unwrap_or(&[])
}

fn str_field(task: &Value, key: &str) -> Option<String> {
    task.get(key).and_then(Value::as_str).map(str::to_string)
}

fn depends_on(task: &Value) -> Vec<String> {
    task.get("depends_on")
        .and_then(Value::as_sequence)
        .map(|s| s.iter().filter_map(|v| v.as_str().map(str::to_string)).collect())
        .unwrap_or_default()
}

/// The names a spec directly chains (one entry per `workflow_ref` task).
fn direct_refs(spec: &Value) -> Vec<String> {
    tasks_of(spec).iter().filter_map(|t| str_field(t, "workflow_ref")).collect()
}

/// Load every workflow transitively referenced from `root`, keyed by name. Dedups
/// by name, so even a reference cycle terminates here (the cycle itself is
/// reported during expansion, with the offending chain in the message).
async fn collect_referenced_specs(
    state: &AppState,
    root: &Value,
) -> Result<HashMap<String, Value>, ApiError> {
    let mut out: HashMap<String, Value> = HashMap::new();
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
                (StatusCode::INTERNAL_SERVER_ERROR, "internal server error".to_string())
            })?;
        let yaml = yaml.ok_or_else(|| {
            bad(format!("references unknown workflow '{name}' — save that workflow first"))
        })?;
        let child: Value = serde_yaml::from_str(&yaml)
            .map_err(|e| bad(format!("referenced workflow '{name}' has an invalid spec: {e}")))?;
        // Inlining copies a child's `tasks:` into the root but not its
        // `templates:` block, so a child that calls its own templates would land
        // in the root with nothing to resolve against — the engine would then
        // fail with a bare "unknown template". Say so here, where the cause is
        // still visible.
        if child.get("templates").and_then(Value::as_sequence).is_some_and(|t| !t.is_empty()) {
            return Err(bad(format!(
                "referenced workflow '{name}' declares `templates:` — a chained workflow is \
                 inlined task-by-task, so its templates would not come with it. Move those \
                 templates into this workflow and call them with `template:`, or drop the \
                 `workflow_ref` and inline the steps directly."
            )));
        }
        for r in direct_refs(&child) {
            stack.push(r);
        }
        out.insert(name, child);
    }
    Ok(out)
}

/// One task's expansion: the produced tasks plus the boundary node sets used to
/// rewire dependencies in the enclosing list.
struct Expanded {
    /// Tasks with no *internal* dependency — they inherit the call's external
    /// `depends_on`.
    roots: Vec<String>,
    /// Tasks nothing internal depends on — the call's dependents attach here.
    exits: Vec<String>,
    /// Fully-wired tasks (roots have empty `depends_on`, filled by the caller).
    tasks: Vec<Value>,
}

/// Expand `root`'s tasks in place, using the preloaded reference specs.
fn expand_pure(mut root: Value, refs: &HashMap<String, Value>) -> Result<Value, ApiError> {
    let mut budget = MAX_TASKS;
    let tasks = expand_list(tasks_of(&root), "", refs, 0, &mut budget, &mut Vec::new())?;
    let map = root.as_mapping_mut().ok_or_else(|| bad("spec must be a mapping"))?;
    map.insert(Value::from("tasks"), Value::Sequence(tasks));
    Ok(root)
}

/// Expand a task list, rewiring each call task's neighbours around its sub-DAG.
/// `prefix` namespaces inlined names; `chain` carries the reference path for
/// cycle reporting.
fn expand_list(
    tasks: &[Value],
    prefix: &str,
    refs: &HashMap<String, Value>,
    depth: usize,
    budget: &mut usize,
    chain: &mut Vec<String>,
) -> Result<Vec<Value>, ApiError> {
    if depth > MAX_DEPTH {
        return Err(bad(format!("workflow_ref nesting exceeds {MAX_DEPTH} levels")));
    }
    // Per-task expansion first, so the rewiring pass below can map a call task's
    // name to the boundary nodes that replaced it.
    let mut expansions: Vec<(String, Expanded)> = Vec::new();
    for task in tasks {
        let Some(name) = str_field(task, "name") else {
            return Err(bad("every task needs a `name`"));
        };
        let expanded = expand_one(task, &name, prefix, refs, depth, budget, chain)?;
        expansions.push((name, expanded));
    }

    // Rewire: a dependency on a call task becomes a dependency on that call's
    // exits; a call's roots inherit the call's own upstreams (already rewired).
    let boundary: HashMap<&str, (&Vec<String>, &Vec<String>)> =
        expansions.iter().map(|(n, e)| (n.as_str(), (&e.roots, &e.exits))).collect();
    let mut out: Vec<Value> = Vec::new();
    for (name, expanded) in &expansions {
        // The call's own upstreams, resolved through any upstream call tasks.
        let original = tasks.iter().find(|t| str_field(t, "name").as_deref() == Some(name));
        let mut upstreams: Vec<String> = Vec::new();
        for dep in original.map(depends_on).unwrap_or_default() {
            match boundary.get(dep.as_str()) {
                Some((_, exits)) => upstreams.extend(exits.iter().cloned()),
                // A dep on a name that is not in this list is left verbatim; the
                // engine's validator reports it with full context.
                None => upstreams.push(qualify(prefix, &dep)),
            }
        }
        for mut task in expanded.tasks.clone() {
            let tname = str_field(&task, "name").unwrap_or_default();
            if expanded.roots.contains(&tname) {
                let mut deps: BTreeSet<String> = depends_on(&task).into_iter().collect();
                deps.extend(upstreams.iter().cloned());
                set_depends_on(&mut task, deps.into_iter().collect())?;
            }
            out.push(task);
        }
    }
    Ok(out)
}

/// Expand one task: a plain task is copied (namespaced); a `workflow_ref` task is
/// replaced by the referenced workflow's tasks, namespaced under the call's name.
fn expand_one(
    task: &Value,
    name: &str,
    prefix: &str,
    refs: &HashMap<String, Value>,
    depth: usize,
    budget: &mut usize,
    chain: &mut Vec<String>,
) -> Result<Expanded, ApiError> {
    let qualified = qualify(prefix, name);
    let Some(target) = str_field(task, "workflow_ref") else {
        // Ordinary task: copy verbatim, only the name is namespaced. Every other
        // field — including ones this crate has never heard of — rides along.
        if *budget == 0 {
            return Err(bad(format!("expanded workflow exceeds {MAX_TASKS} tasks")));
        }
        *budget -= 1;
        let mut copy = task.clone();
        set_str(&mut copy, "name", &qualified)?;
        // Deps are cleared here and rewritten by the caller, which is the only
        // place that knows whether a named dep expanded into a sub-DAG (attach to
        // its exits) or stayed one task (namespace it). Qualifying them here too
        // left the pre-expansion name behind next to the rewired one.
        set_depends_on(&mut copy, Vec::new())?;
        return Ok(Expanded {
            roots: vec![qualified.clone()],
            exits: vec![qualified.clone()],
            tasks: vec![copy],
        });
    };

    if chain.contains(&target) {
        chain.push(target.clone());
        return Err(bad(format!("workflow_ref cycle: {}", chain.join(" → "))));
    }
    // A call task is a placeholder, not a task: anything else on it would be
    // dropped by the inlining below — the exact silent-drop class this file
    // exists to prevent — so reject it rather than lose it.
    if let Some(map) = task.as_mapping() {
        let extra: Vec<String> = map
            .keys()
            .filter_map(Value::as_str)
            .filter(|k| !matches!(*k, "name" | "workflow_ref" | "depends_on"))
            .map(str::to_string)
            .collect();
        if !extra.is_empty() {
            return Err(bad(format!(
                "task '{name}' uses workflow_ref, so these fields are not supported here: {}",
                extra.join(", ")
            )));
        }
    }
    let child = refs
        .get(&target)
        .ok_or_else(|| bad(format!("references unknown workflow '{target}'")))?;
    let child_tasks = tasks_of(child);
    if child_tasks.is_empty() {
        return Err(bad(format!("referenced workflow '{target}' has no tasks")));
    }

    chain.push(target.clone());
    let inner = expand_list(child_tasks, &qualified, refs, depth + 1, budget, chain)?;
    chain.pop();

    // Boundaries of the inlined sub-DAG: roots depend on nothing inside it,
    // exits have nothing inside depending on them.
    let inner_names: BTreeSet<String> =
        inner.iter().filter_map(|t| str_field(t, "name")).collect();
    let mut depended_on: BTreeSet<String> = BTreeSet::new();
    let mut roots: Vec<String> = Vec::new();
    for t in &inner {
        let deps = depends_on(t);
        for d in &deps {
            depended_on.insert(d.clone());
        }
        if deps.iter().all(|d| !inner_names.contains(d)) {
            if let Some(n) = str_field(t, "name") {
                roots.push(n);
            }
        }
    }
    let exits: Vec<String> =
        inner_names.iter().filter(|n| !depended_on.contains(*n)).cloned().collect();

    Ok(Expanded { roots, exits, tasks: inner })
}

fn qualify(prefix: &str, name: &str) -> String {
    if prefix.is_empty() {
        name.to_string()
    } else {
        format!("{prefix}.{name}")
    }
}

fn set_str(task: &mut Value, key: &str, val: &str) -> Result<(), ApiError> {
    task.as_mapping_mut()
        .ok_or_else(|| bad("each task must be a mapping"))?
        .insert(Value::from(key), Value::from(val));
    Ok(())
}

fn set_depends_on(task: &mut Value, deps: Vec<String>) -> Result<(), ApiError> {
    let map: &mut Mapping =
        task.as_mapping_mut().ok_or_else(|| bad("each task must be a mapping"))?;
    map.insert(
        Value::from("depends_on"),
        Value::Sequence(deps.into_iter().map(Value::from).collect()),
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn v(yaml: &str) -> Value {
        serde_yaml::from_str(yaml).expect("parse spec")
    }

    fn names(spec: &Value) -> Vec<String> {
        tasks_of(spec).iter().filter_map(|t| str_field(t, "name")).collect()
    }

    fn deps_of(spec: &Value, name: &str) -> Vec<String> {
        tasks_of(spec)
            .iter()
            .find(|t| str_field(t, "name").as_deref() == Some(name))
            .map(depends_on)
            .unwrap_or_default()
    }

    #[test]
    fn leaf_only_passthrough() {
        let spec = v("name: p\ntasks:\n  - name: a\n    command: [\"true\"]\n  - name: b\n    command: [\"true\"]\n    depends_on: [a]\n");
        let out = expand_pure(spec, &HashMap::new()).unwrap();
        assert_eq!(names(&out), vec!["a", "b"]);
        assert_eq!(deps_of(&out, "b"), vec!["a"]);
    }

    /// The regression this rewrite exists for: fields the API never modelled
    /// (fan-out, sensors, cache, pool, priority, produces, resources) must
    /// survive expansion untouched, including inside an inlined child.
    #[test]
    fn unmodelled_fields_survive() {
        let child = v("name: c\ntasks:\n  - name: gate\n    type: wait\n    wait: { for: 5m }\n");
        let refs = HashMap::from([("c".to_string(), child)]);
        let spec = v("name: p\ntasks:\n  - name: shard\n    with_items: [a, b]\n    pool: etl\n    priority: 9\n    cache: { key: k }\n    produces: [\"s3://x\"]\n    resources: { gpu: { count: 2 } }\n    command: [\"echo\", \"{{ item }}\"]\n  - name: call\n    workflow_ref: c\n    depends_on: [shard]\n");
        let out = expand_pure(spec, &refs).unwrap();

        let shard = tasks_of(&out).iter().find(|t| str_field(t, "name").as_deref() == Some("shard")).unwrap();
        assert!(shard.get("with_items").is_some(), "with_items dropped");
        assert_eq!(shard.get("pool").and_then(Value::as_str), Some("etl"));
        assert_eq!(shard.get("priority").and_then(Value::as_u64), Some(9));
        assert!(shard.get("cache").is_some(), "cache dropped");
        assert!(shard.get("produces").is_some(), "produces dropped");
        assert!(shard.get("resources").is_some(), "resources dropped");

        // The inlined sensor keeps `type` AND its `wait:` block — dropping the
        // latter made the engine dispatch it to a worker, where it failed.
        let gate = tasks_of(&out).iter().find(|t| str_field(t, "name").as_deref() == Some("call.gate")).unwrap();
        assert_eq!(gate.get("type").and_then(Value::as_str), Some("wait"));
        assert!(gate.get("wait").is_some(), "wait block dropped");
        assert_eq!(deps_of(&out, "call.gate"), vec!["shard"]);
    }

    #[test]
    fn inlines_and_rewires_a_reference() {
        let child = v("name: c\ntasks:\n  - name: x\n    command: [\"true\"]\n  - name: y\n    command: [\"true\"]\n    depends_on: [x]\n");
        let refs = HashMap::from([("c".to_string(), child)]);
        let spec = v("name: p\ntasks:\n  - name: pre\n    command: [\"true\"]\n  - name: call\n    workflow_ref: c\n    depends_on: [pre]\n  - name: post\n    command: [\"true\"]\n    depends_on: [call]\n");
        let out = expand_pure(spec, &refs).unwrap();
        assert_eq!(names(&out), vec!["pre", "call.x", "call.y", "post"]);
        assert_eq!(deps_of(&out, "call.x"), vec!["pre"], "root inherits the call's upstream");
        assert_eq!(deps_of(&out, "call.y"), vec!["call.x"], "internal edge preserved");
        assert_eq!(deps_of(&out, "post"), vec!["call.y"], "dependent attaches to the exit");
    }

    #[test]
    fn rejects_extra_fields_on_a_call_task() {
        let child = v("name: c\ntasks:\n  - name: x\n    command: [\"true\"]\n");
        let refs = HashMap::from([("c".to_string(), child)]);
        let spec = v("name: p\ntasks:\n  - name: call\n    workflow_ref: c\n    pool: etl\n");
        let err = expand_pure(spec, &refs).unwrap_err();
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(err.1.contains("pool"), "got: {}", err.1);
    }

    #[test]
    fn rejects_a_reference_cycle() {
        let a = v("name: a\ntasks:\n  - name: t\n    workflow_ref: b\n");
        let b = v("name: b\ntasks:\n  - name: t\n    workflow_ref: a\n");
        let refs = HashMap::from([("a".to_string(), a), ("b".to_string(), b.clone())]);
        let spec = v("name: root\ntasks:\n  - name: call\n    workflow_ref: b\n");
        let err = expand_pure(spec, &refs).unwrap_err();
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(err.1.contains("cycle"), "got: {}", err.1);
    }

    #[test]
    fn rejects_unknown_reference() {
        let spec = v("name: p\ntasks:\n  - name: call\n    workflow_ref: nope\n");
        let err = expand_pure(spec, &HashMap::new()).unwrap_err();
        assert!(err.1.contains("unknown workflow 'nope'"), "got: {}", err.1);
    }
}
