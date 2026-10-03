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

use std::borrow::Cow;
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

/// Resolve every `workflow_ref` in the already-parsed `root` (the caller's one
/// Value parse of `yaml` — LOW_LATENCY R-3), returning the flattened spec YAML.
///
/// Borrows the input untouched when nothing chains, so the common path pays no
/// parse, no re-serialization, and no database work here.
pub(crate) async fn expand_workflow_refs<'y>(
    state: &AppState,
    root: Value,
    yaml: &'y str,
) -> Result<Cow<'y, str>, ApiError> {
    if direct_refs(&root).is_empty() {
        return Ok(Cow::Borrowed(yaml));
    }
    let refs = collect_referenced_specs(state, &root).await?;
    let expanded = expand_pure(root, &refs)?;
    serde_yaml::to_string(&expanded)
        .map(Cow::Owned)
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
pub(crate) fn direct_refs(spec: &Value) -> Vec<String> {
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
    // Fetch one nesting LEVEL per query — `WHERE name = ANY(...)` — instead of
    // the old one-query-per-reference loop (LOW_LATENCY R-3): a spec chaining
    // ten siblings paid ten serial round trips; now it pays one, and depth
    // alone (bounded by MAX_DEPTH, in practice one or two) adds queries.
    let mut frontier: Vec<String> = direct_refs(root);
    while !frontier.is_empty() {
        frontier.sort();
        frontier.dedup();
        frontier.retain(|n| !out.contains_key(n));
        if frontier.is_empty() {
            break;
        }
        let rows: Vec<(String, String)> =
            sqlx::query_as("SELECT name, spec FROM workflows WHERE name = ANY($1)")
                .bind(&frontier)
                .fetch_all(&state.read_pool)
                .await
                .map_err(|e| {
                    tracing::error!(error = ?e, "loading referenced workflows");
                    (StatusCode::INTERNAL_SERVER_ERROR, "internal server error".to_string())
                })?;
        let mut by_name: HashMap<String, String> = rows.into_iter().collect();
        let mut next: Vec<String> = Vec::new();
        for name in frontier.drain(..) {
            let yaml = by_name.remove(&name).ok_or_else(|| {
                bad(format!("references unknown workflow '{name}' — save that workflow first"))
            })?;
            let child: Value = serde_yaml::from_str(&yaml)
                .map_err(|e| bad(format!("referenced workflow '{name}' has an invalid spec: {e}")))?;
            // Same reasoning as `templates:` below: a child's imports would not
            // come with its tasks.
            if !direct_uses(&child).is_empty() {
                return Err(bad(format!(
                    "referenced workflow '{name}' declares `use:` — a chained workflow is inlined \
                     task-by-task, so its imported templates would not come with it. Add the same \
                     `use:` to this workflow, or drop the `workflow_ref` and inline the steps directly."
                )));
            }
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
                next.push(r);
            }
            out.insert(name, child);
        }
        frontier = next;
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


// ── Template libraries: `use:` ────────────────────────────────────────────────
//
// A saved workflow can serve as a **template library**: it declares `templates:`
// (and, because `tasks:` is required, `tasks: []`) and other workflows import
// from it instead of pasting the same sub-DAG into every spec:
//
// ```yaml
// use:
//   - iac-library              # every template the library declares
//   - ci-library/build         # one template, plus the templates it calls
// tasks:
//   - { name: prod, template: tf_stage, arguments: { env: prod } }
// ```
//
// Imports are resolved here, in YAML space, before the engine's parser runs: the
// library's templates are appended to the spec's own `templates:` block and `use:`
// is removed, so the engine never sees an import. As with `workflow_ref`, the
// reference is by name and resolves to the library as saved *now* — save the
// library under a new name to pin a spec to a fixed copy.

/// The libraries a spec imports (the raw `use:` entries).
pub(crate) fn direct_uses(spec: &Value) -> Vec<String> {
    spec.get("use")
        .and_then(Value::as_sequence)
        .map(|s| s.iter().filter_map(|v| v.as_str().map(str::trim).map(str::to_string)).collect())
        .unwrap_or_default()
}

/// Split a `use:` entry into `(library, template)`.
fn split_use(entry: &str) -> Result<(&str, Option<&str>), ApiError> {
    let (lib, tpl) = match entry.split_once('/') {
        Some((l, t)) => (l.trim(), Some(t.trim())),
        None => (entry.trim(), None),
    };
    if lib.is_empty() || tpl.is_some_and(str::is_empty) || tpl.is_some_and(|t| t.contains('/')) {
        return Err(bad(format!(
            "`use:` entry '{entry}' is not valid — write `<library>` or `<library>/<template>`"
        )));
    }
    Ok((lib, tpl))
}

fn template_name(t: &Value) -> Option<String> {
    str_field(t, "name")
}

/// The templates `name` needs from `lib`: itself and, transitively, every sibling
/// its tasks call with `template:`.
fn template_closure(lib: &str, templates: &[Value], name: &str) -> Result<Vec<String>, ApiError> {
    let find = |n: &str| templates.iter().find(|t| template_name(t).as_deref() == Some(n));
    if find(name).is_none() {
        let have: Vec<String> = templates.iter().filter_map(template_name).collect();
        return Err(bad(format!(
            "library '{lib}' has no template '{name}' (it declares: {})",
            if have.is_empty() { "none".to_string() } else { have.join(", ") }
        )));
    }
    let mut out: Vec<String> = Vec::new();
    let mut stack = vec![name.to_string()];
    while let Some(n) = stack.pop() {
        if out.contains(&n) {
            continue;
        }
        // A callee the library does not declare is left for the engine to report
        // as an unknown template, with the task that calls it.
        if let Some(t) = find(&n) {
            for task in tasks_of(t) {
                if let Some(callee) = str_field(task, "template") {
                    stack.push(callee);
                }
            }
            out.push(n);
        }
    }
    Ok(out)
}

/// Append the templates named by `root`'s `use:` to its `templates:` block and
/// drop `use:`. Pure: `libs` is every library the spec names, already loaded.
fn merge_uses(mut root: Value, libs: &HashMap<String, Value>) -> Result<Value, ApiError> {
    let entries = direct_uses(&root);
    // template name -> the library it came from ("" for the spec's own).
    let mut owner: HashMap<String, String> = HashMap::new();
    if let Some(local) = root.get("templates").and_then(Value::as_sequence) {
        for t in local {
            if let Some(n) = template_name(t) {
                owner.insert(n, String::new());
            }
        }
    }
    let mut imported: Vec<Value> = Vec::new();
    for entry in &entries {
        let (lib, only) = split_use(entry)?;
        let spec = libs.get(lib).ok_or_else(|| {
            bad(format!("`use:` names unknown workflow '{lib}' — save that library first"))
        })?;
        let templates: &[Value] =
            spec.get("templates").and_then(Value::as_sequence).map(|s| s.as_slice()).unwrap_or(&[]);
        if templates.is_empty() {
            return Err(bad(format!("`use:` names '{lib}', which declares no `templates:`")));
        }
        let wanted: Vec<String> = match only {
            Some(t) => template_closure(lib, templates, t)?,
            None => templates.iter().filter_map(template_name).collect(),
        };
        for name in wanted {
            match owner.get(&name) {
                Some(o) if o == lib => continue, // the same template imported twice
                Some(o) => {
                    let from =
                        if o.is_empty() { "this spec".to_string() } else { format!("library '{o}'") };
                    return Err(bad(format!(
                        "template '{name}' is defined by {from} and imported from library '{lib}' — \
                         rename one, or import only the templates you need with `use: {lib}/<template>`"
                    )));
                }
                None => {}
            }
            if let Some(t) =
                templates.iter().find(|t| template_name(t).as_deref() == Some(name.as_str()))
            {
                imported.push(t.clone());
                owner.insert(name, lib.to_string());
            }
        }
    }
    let map = root.as_mapping_mut().ok_or_else(|| bad("spec must be a mapping"))?;
    map.remove(Value::from("use"));
    let slot = map.entry(Value::from("templates")).or_insert_with(|| Value::Sequence(Vec::new()));
    match slot {
        Value::Sequence(seq) => seq.extend(imported),
        Value::Null => *slot = Value::Sequence(imported),
        _ => return Err(bad("`templates:` must be a list")),
    }
    Ok(root)
}

/// Resolve `root`'s `use:` imports. Borrows `yaml` untouched when the spec
/// imports nothing, so the common path pays no parse and no database work.
pub(crate) async fn resolve_template_uses<'y>(
    state: &AppState,
    root: Value,
    yaml: &'y str,
) -> Result<(Value, Cow<'y, str>), ApiError> {
    let entries = direct_uses(&root);
    if entries.is_empty() {
        return Ok((root, Cow::Borrowed(yaml)));
    }
    let mut names: Vec<String> = Vec::new();
    for e in &entries {
        names.push(split_use(e)?.0.to_string());
    }
    names.sort();
    names.dedup();
    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT name, spec FROM workflows WHERE name = ANY($1)")
            .bind(&names)
            .fetch_all(&state.read_pool)
            .await
            .map_err(|e| {
                tracing::error!(error = ?e, "loading template libraries");
                (StatusCode::INTERNAL_SERVER_ERROR, "internal server error".to_string())
            })?;
    let mut libs: HashMap<String, Value> = HashMap::new();
    for (name, spec_yaml) in rows {
        let spec: Value = serde_yaml::from_str(&spec_yaml)
            .map_err(|e| bad(format!("library '{name}' has an invalid spec: {e}")))?;
        // One level only: a library that imports another would make a spec's
        // templates depend on a chain nobody can see from the spec.
        if !direct_uses(&spec).is_empty() {
            return Err(bad(format!(
                "library '{name}' uses other libraries — a library must declare its templates \
                 itself; import both libraries from the spec instead"
            )));
        }
        libs.insert(name, spec);
    }
    let merged = merge_uses(root, &libs)?;
    let yaml = serde_yaml::to_string(&merged)
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("re-serializing spec: {e}")))?;
    Ok((merged, Cow::Owned(yaml)))
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

    // ── `use:` template libraries ────────────────────────────────────────────

    const LIB: &str = "name: lib\ntasks: []\ntemplates:\n  - name: stage\n    tasks:\n      - { name: plan, command: [\"true\"] }\n      - { name: gate, template: guard, depends_on: [plan] }\n  - name: guard\n    tasks:\n      - { name: check, command: [\"true\"] }\n  - name: unrelated\n    tasks:\n      - { name: x, command: [\"true\"] }\n";

    fn libs() -> HashMap<String, Value> {
        HashMap::from([("lib".to_string(), v(LIB))])
    }

    fn template_names(spec: &Value) -> Vec<String> {
        spec.get("templates")
            .and_then(Value::as_sequence)
            .map(|s| s.iter().filter_map(|t| str_field(t, "name")).collect())
            .unwrap_or_default()
    }

    #[test]
    fn use_of_a_whole_library_imports_every_template_and_drops_the_key() {
        let spec = v("name: p\nuse: [lib]\ntasks:\n  - { name: a, template: stage }\n");
        let out = merge_uses(spec, &libs()).unwrap();
        assert_eq!(template_names(&out), vec!["stage", "guard", "unrelated"]);
        assert!(out.get("use").is_none(), "the engine must never see `use:`");
        assert_eq!(names(&out), vec!["a"], "tasks are untouched");
    }

    #[test]
    fn use_of_one_template_brings_the_templates_it_calls_and_no_others() {
        let spec = v("name: p\nuse: [lib/stage]\ntasks:\n  - { name: a, template: stage }\n");
        let out = merge_uses(spec, &libs()).unwrap();
        let mut got = template_names(&out);
        got.sort();
        assert_eq!(got, vec!["guard", "stage"], "`unrelated` must not come along");
    }

    #[test]
    fn imported_templates_keep_the_specs_own_and_import_order() {
        let spec = v("name: p\nuse: [lib/guard]\ntemplates:\n  - { name: mine, tasks: [{ name: t, command: [\"true\"] }] }\ntasks: []\n");
        let out = merge_uses(spec, &libs()).unwrap();
        assert_eq!(template_names(&out), vec!["mine", "guard"]);
    }

    #[test]
    fn importing_the_same_template_twice_is_not_a_clash() {
        let spec = v("name: p\nuse: [lib, lib/stage]\ntasks: []\n");
        let out = merge_uses(spec, &libs()).unwrap();
        assert_eq!(template_names(&out), vec!["stage", "guard", "unrelated"]);
    }

    #[test]
    fn a_local_template_with_an_imported_name_is_refused() {
        let spec = v("name: p\nuse: [lib/guard]\ntemplates:\n  - { name: guard, tasks: [{ name: t, command: [\"true\"] }] }\ntasks: []\n");
        let err = merge_uses(spec, &libs()).unwrap_err();
        assert_eq!(err.0, StatusCode::BAD_REQUEST);
        assert!(err.1.contains("'guard'") && err.1.contains("this spec"), "got: {}", err.1);
    }

    #[test]
    fn two_libraries_defining_one_name_is_refused() {
        let other = v("name: other\ntasks: []\ntemplates:\n  - { name: guard, tasks: [{ name: t, command: [\"true\"] }] }\n");
        let mut l = libs();
        l.insert("other".to_string(), other);
        let spec = v("name: p\nuse: [lib/guard, other]\ntasks: []\n");
        let err = merge_uses(spec, &l).unwrap_err();
        assert!(err.1.contains("library 'lib'") && err.1.contains("'other'"), "got: {}", err.1);
    }

    #[test]
    fn unknown_library_template_and_malformed_entries_name_the_problem() {
        let e = |u: &str| merge_uses(v(&format!("name: p\nuse: [{u}]\ntasks: []\n")), &libs()).unwrap_err().1;
        assert!(e("nope").contains("unknown workflow 'nope'"));
        let msg = e("lib/missing");
        assert!(msg.contains("no template 'missing'") && msg.contains("stage"), "got: {msg}");
        assert!(e("lib/").contains("not valid"));
        assert!(e("/stage").contains("not valid"));
        assert!(e("a/b/c").contains("not valid"));
    }

    #[test]
    fn a_library_without_templates_is_refused() {
        let empty = v("name: empty\ntasks: []\n");
        let l = HashMap::from([("empty".to_string(), empty)]);
        let err = merge_uses(v("name: p\nuse: [empty]\ntasks: []\n"), &l).unwrap_err();
        assert!(err.1.contains("declares no `templates:`"), "got: {}", err.1);
    }

    #[test]
    fn a_spec_without_use_is_left_alone() {
        let spec = v("name: p\ntemplates: []\ntasks: []\n");
        assert!(direct_uses(&spec).is_empty());
    }

    /// The shipped IaC library and the workflows that import it: merge, then hand
    /// the result to the engine's own parser, exactly as `build_dag` does.
    fn expand_iac_example(file_yaml: &str, params: &[(&str, &str)]) -> dagron_core::dag::DagGraph {
        let lib = include_str!("../../../examples/iac/iac-library.yaml");
        let libs = HashMap::from([("iac-library".to_string(), v(lib))]);
        let merged = merge_uses(v(file_yaml), &libs).expect("merge");
        let yaml = serde_yaml::to_string(&merged).unwrap();
        let overrides: std::collections::BTreeMap<String, String> =
            params.iter().map(|(k, val)| (k.to_string(), val.to_string())).collect();
        dagron_core::dag::DagGraph::from_yaml_with_params(&yaml, &overrides)
            .unwrap_or_else(|e| panic!("the engine refused the merged spec: {e:#}"))
    }

    fn command_of(g: &dagron_core::dag::DagGraph, task: &str) -> String {
        g.task_spec(task).unwrap_or_else(|| panic!("no task {task}")).command.join(" ")
    }

    #[test]
    fn the_terraform_example_expands_and_tofu_switches_every_binary() {
        let wf = include_str!("../../../examples/iac/promote_terraform.yaml");
        let g = expand_iac_example(wf, &[("promote_through", "prod")]);
        for t in ["checks.init", "dev.plan", "staging.review", "prod.apply", "notify_failure"] {
            assert!(g.task_spec(t).is_some(), "missing {t}");
        }
        assert!(g.task_spec("dev.review").is_none(), "an ungated stage has no review task");
        assert!(command_of(&g, "dev.plan").contains("terraform -chdir='./infra'"));

        let g = expand_iac_example(wf, &[("tool", "tofu"), ("promote_through", "prod")]);
        for t in ["checks.fmt", "checks.init", "dev.plan", "staging.apply", "prod.plan"] {
            let c = command_of(&g, t);
            assert!(c.contains("tofu ") && !c.contains("terraform"), "{t}: {c}");
        }
    }

    #[test]
    fn the_pulumi_example_imports_one_template_and_expands() {
        let wf = include_str!("../../../examples/iac/promote_pulumi.yaml");
        let g = expand_iac_example(wf, &[("promote_through", "prod")]);
        let apply = command_of(&g, "prod.apply");
        assert!(apply.contains("pulumi up") && apply.contains("--stack 'prod'"), "{apply}");
        assert!(g.task_spec("checks.init").is_none(), "only pulumi_stage was imported");
    }
}
