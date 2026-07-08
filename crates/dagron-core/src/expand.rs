//! Template expansion: turn a workflow that *calls other workflows*
//! into a flat, leaf-only DAG the engine can run unchanged.
//!
//! A [`DagSpec`] may declare reusable sub-DAGs under `templates:` and invoke them
//! from a task via `template:` (+ `arguments:`). The expander rewrites every such
//! **call task** into the template's tasks, so by the time [`crate::dag::DagGraph`]
//! builds the graph there are only **leaf tasks** (each with a `command`). Because
//! expansion happens at run-creation time, the reconcile loop, claim path, and
//! executors need no changes — a sub-workflow is just a bigger DAG.
//!
//! Supported patterns (see `examples/templates/`):
//!   * **call / DAG-of-DAGs** — a task runs a template instead of a container.
//!   * **parameters** — `{{ name }}` substitution; template scope is isolated, fed
//!     by the caller's `arguments` over the template's `parameters` defaults.
//!   * **fan-out / map** — `with_items` (literal list) or `with_param` (a param
//!     holding a JSON array); `{{ item }}` / `{{ item.key }}` / `{{ index }}`.
//!   * **recursion** — a template calls itself with changed arguments; a `when:`
//!     guard provides the base case, and a depth cap is the safety net.
//!   * **conditional** — `when: "{{ x }} > 0"` skips a task (and its sub-DAG).
//!
//! ## Dependency rewiring
//! Each task expands to a set of output tasks with two boundary sets: **roots**
//! (no internal dependency — they inherit the call's external `depends_on`) and
//! **exits** (nothing internal depends on them — the call's dependents attach
//! here). This keeps ordering correct across arbitrarily nested expansions.

use crate::dag::{DagSpec, TaskSpec, TemplateSpec};
use anyhow::{bail, Result};
use serde_json::Value;
use std::collections::{BTreeMap, BTreeSet};

/// Safety net against runaway / non-terminating recursion. A `when:` guard is the
/// intended base case; this just bounds a buggy one instead of OOMing.
const MAX_DEPTH: usize = 64;
/// Bound the total expanded task count so a fan-out × recursion blow-up fails
/// loudly rather than exhausting memory.
const MAX_TASKS: usize = 100_000;

/// Result of expanding one task (or a list of tasks): the produced leaf tasks
/// plus the boundary node-name sets used to rewire dependencies.
struct Expanded {
    /// Output task names with no *internal* dependency — they receive the caller's
    /// external `depends_on`.
    roots: Vec<String>,
    /// Output task names nothing internal depends on — the caller's dependents
    /// attach to these.
    exits: Vec<String>,
    /// The fully-wired leaf tasks (root nodes have empty `depends_on`, to be
    /// filled by the enclosing list).
    tasks: Vec<TaskSpec>,
}

impl Expanded {
    fn empty() -> Self {
        Self { roots: vec![], exits: vec![], tasks: vec![] }
    }
}

/// Expand all template calls in `spec` into a flat, leaf-only [`DagSpec`].
pub fn expand(spec: DagSpec) -> Result<DagSpec> {
    let mut templates: BTreeMap<String, &TemplateSpec> = BTreeMap::new();
    for t in &spec.templates {
        if templates.insert(t.name.clone(), t).is_some() {
            bail!("duplicate template name '{}'", t.name);
        }
    }

    let mut budget = MAX_TASKS;
    let mut e = expand_list(&spec.tasks, "", &spec.parameters, 0, &templates, &mut budget)?;

    wire_hooks(&mut e.tasks)?;

    Ok(DagSpec {
        name: spec.name,
        parameters: BTreeMap::new(),
        templates: vec![],
        run_timeout_secs: spec.run_timeout_secs,
        deadline: spec.deadline,
        notify: spec.notify,
        result_from: spec.result_from,
        tasks: e.tasks,
    })
}

/// Wire lifecycle-hook tasks (fast-win #11): a `hook:` task is auto-wired to
/// depend on every non-hook task and given the trigger rule its hook implies —
/// `on_exit` → `all_done` (a finalizer that runs whatever happened),
/// `on_failure` → `one_failed` (runs only if some task failed). This makes hooks
/// pure sugar over the existing dependency + trigger-rule machinery.
fn wire_hooks(tasks: &mut [TaskSpec]) -> Result<()> {
    let non_hook: Vec<String> =
        tasks.iter().filter(|t| t.hook.is_none()).map(|t| t.name.clone()).collect();
    for t in tasks.iter_mut() {
        let Some(hook) = t.hook.clone() else { continue };
        let rule = match hook.as_str() {
            "on_exit" => "all_done",
            "on_failure" => "one_failed",
            other => bail!("task '{}' has invalid hook '{other}' (expected on_exit or on_failure)", t.name),
        };
        // Depend on every non-hook task (not on other hooks, so hooks run in
        // parallel at the end); its trigger_rule decides whether it fires.
        t.depends_on = non_hook.iter().filter(|n| **n != t.name).cloned().collect();
        t.trigger_rule = Some(rule.to_string());
    }
    Ok(())
}

/// Expand a list of sibling tasks, wiring their inter-dependencies.
fn expand_list(
    tasks: &[TaskSpec],
    prefix: &str,
    ctx: &BTreeMap<String, String>,
    depth: usize,
    templates: &BTreeMap<String, &TemplateSpec>,
    budget: &mut usize,
) -> Result<Expanded> {
    // Expand each task on its own first (deps wired in a second pass).
    let mut per: BTreeMap<String, Expanded> = BTreeMap::new();
    let mut order: Vec<String> = Vec::with_capacity(tasks.len());
    for t in tasks {
        if per.contains_key(&t.name) {
            bail!("duplicate task name '{}'", t.name);
        }
        let e = expand_one(t, prefix, ctx, depth, templates, budget)?;
        order.push(t.name.clone());
        per.insert(t.name.clone(), e);
    }

    // Names that are depended on by some sibling — used to compute list exits.
    let mut depended: BTreeSet<&str> = BTreeSet::new();
    for t in tasks {
        for d in &t.depends_on {
            depended.insert(d.as_str());
        }
    }

    let mut out_tasks: Vec<TaskSpec> = Vec::new();
    let mut list_roots: Vec<String> = Vec::new();
    let mut list_exits: Vec<String> = Vec::new();

    for t in tasks {
        // External deps for this task = the exit nodes of each sibling it depends on.
        let mut dep_exits: Vec<String> = Vec::new();
        for d in &t.depends_on {
            let de = per.get(d).ok_or_else(|| {
                anyhow::anyhow!("task '{}' depends on unknown task '{}'", t.name, d)
            })?;
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

    Ok(Expanded { roots: list_roots, exits: list_exits, tasks: out_tasks })
}

/// Expand a single task into leaf tasks, handling fan-out, conditionals, and
/// template calls (recursively).
fn expand_one(
    task: &TaskSpec,
    prefix: &str,
    ctx: &BTreeMap<String, String>,
    depth: usize,
    templates: &BTreeMap<String, &TemplateSpec>,
    budget: &mut usize,
) -> Result<Expanded> {
    let is_call = task.template.is_some();
    // An approval gate (#19) is a leaf that carries no command — it waits for a
    // human — so it is exempt from the "exactly one of command/template" rule
    // (but it still may not be a template call).
    if task.is_approval() {
        if is_call {
            bail!("approval task '{}' cannot also be a `template` call", task.name);
        }
        if !task.command.is_empty() {
            bail!("approval task '{}' cannot set a `command` (it waits for a human)", task.name);
        }
    } else if is_call == !task.command.is_empty() {
        bail!(
            "task '{}' must set exactly one of `command` (leaf) or `template` (call)",
            task.name
        );
    }
    if task.with_items.is_some() && task.with_param.is_some() {
        bail!("task '{}' sets both with_items and with_param", task.name);
    }
    if task.instance_key.is_some() && task.with_items.is_none() && task.with_param.is_none() {
        bail!(
            "task '{}' sets instance_key without with_items/with_param (nothing to label)",
            task.name
        );
    }

    // Resolve the fan-out items: None ⇒ a single (un-indexed) instance.
    let items: Vec<Option<Value>> = resolve_items(task, ctx)?;
    let fanned = task.with_items.is_some() || task.with_param.is_some();
    // A fan-out over an empty list produces zero tasks, which would silently
    // unblock anything depending on it (the dependent gets no predecessors).
    // Surface it instead of corrupting the dependency chain.
    if fanned && items.is_empty() {
        bail!(
            "task '{}' fans out over an empty list (with_items/with_param resolved to [])",
            task.name
        );
    }

    let mut acc = Expanded::empty();
    let mut seen_labels: BTreeSet<String> = BTreeSet::new();
    for (i, item) in items.into_iter().enumerate() {
        // Per-instance scope: the caller ctx plus item/index bindings.
        let mut inst_ctx = ctx.clone();
        if let Some(item) = &item {
            bind_item(&mut inst_ctx, item);
            inst_ctx.insert("index".to_string(), i.to_string());
        }

        // Conditional guard — the recursion base case.
        if let Some(cond) = &task.when {
            if !eval_when(&substitute(cond, &inst_ctx))? {
                continue; // skip this instance entirely
            }
        }

        let base = if fanned {
            // Human-readable instance label (`instance_key`) over the bare
            // fan-out index when the author asked for one.
            let label = match &task.instance_key {
                Some(key) => {
                    let label = sanitize_label(&substitute(key, &inst_ctx));
                    if label.is_empty() {
                        bail!(
                            "task '{}' instance_key '{}' rendered empty for item {} \
                             (after sanitizing to [A-Za-z0-9_.-])",
                            task.name,
                            key,
                            i
                        );
                    }
                    if !seen_labels.insert(label.clone()) {
                        bail!(
                            "task '{}' instance_key '{}' rendered duplicate label '{}' \
                             — labels must be unique within the fan-out",
                            task.name,
                            key,
                            label
                        );
                    }
                    label
                }
                None => i.to_string(),
            };
            format!("{prefix}{}.{}", task.name, label)
        } else {
            format!("{prefix}{}", task.name)
        };

        let inst = if is_call {
            expand_call(task, &base, &inst_ctx, depth, templates, budget)?
        } else {
            if *budget == 0 {
                bail!("expansion exceeded {MAX_TASKS} tasks (fan-out × recursion blow-up?)");
            }
            *budget -= 1;
            let leaf = build_leaf(task, &base, &inst_ctx);
            Expanded { roots: vec![base.clone()], exits: vec![base], tasks: vec![leaf] }
        };
        acc.roots.extend(inst.roots);
        acc.exits.extend(inst.exits);
        acc.tasks.extend(inst.tasks);
    }
    Ok(acc)
}

/// Expand a `template:` call into its sub-DAG with an isolated parameter scope.
fn expand_call(
    task: &TaskSpec,
    base: &str,
    inst_ctx: &BTreeMap<String, String>,
    depth: usize,
    templates: &BTreeMap<String, &TemplateSpec>,
    budget: &mut usize,
) -> Result<Expanded> {
    if depth + 1 > MAX_DEPTH {
        bail!(
            "template recursion exceeded depth {MAX_DEPTH} at task '{}' (missing a `when:` base case?)",
            task.name
        );
    }
    let name = task.template.as_deref().unwrap();
    let tmpl = templates
        .get(name)
        .ok_or_else(|| anyhow::anyhow!("task '{}' calls unknown template '{}'", task.name, name))?;

    // Isolated scope: template parameter defaults, overridden by the caller's
    // arguments (themselves resolved in the caller's scope).
    let mut call_ctx: BTreeMap<String, String> = tmpl.parameters.clone();
    for (k, v) in &task.arguments {
        call_ctx.insert(k.clone(), substitute(v, inst_ctx));
    }

    let sub_prefix = format!("{base}.");
    expand_list(&tmpl.tasks, &sub_prefix, &call_ctx, depth + 1, templates, budget)
}

/// Materialize a leaf task with all string fields substituted in `ctx`.
fn build_leaf(task: &TaskSpec, name: &str, ctx: &BTreeMap<String, String>) -> TaskSpec {
    TaskSpec {
        name: name.to_string(),
        command: task.command.iter().map(|a| substitute(a, ctx)).collect(),
        depends_on: vec![], // wired by the enclosing list
        input: task.input.clone(),
        trigger_rule: task.trigger_rule.clone(),
        hook: task.hook.clone(),
        allow_failure: task.allow_failure,
        task_type: task.task_type.clone(),
        approval_timeout_secs: task.approval_timeout_secs,
        approval_on_timeout: task.approval_on_timeout.clone(),
        max_attempts: task.max_attempts,
        retry_delay_secs: task.retry_delay_secs,
        retry_max_delay_secs: task.retry_max_delay_secs,
        timeout_secs: task.timeout_secs,
        docker_image: task.docker_image.as_ref().map(|s| substitute(s, ctx)),
        env: task
            .env
            .iter()
            .map(|e| crate::dag::EnvVar {
                name: e.name.clone(),
                value: substitute(&e.value, ctx),
                value_from: e.value_from.clone(),
            })
            .collect(),
        resources: task.resources.clone(),
        service_account: task.service_account.as_ref().map(|s| substitute(s, ctx)),
        // call-only fields never survive on a leaf
        template: None,
        arguments: BTreeMap::new(),
        with_items: None,
        with_param: None,
        when: None,
        instance_key: None,
    }
}

/// Sanitize a rendered `instance_key` label: keep `[A-Za-z0-9_-]` (`.` is the
/// expansion hierarchy separator, so it is excluded), map runs of anything else
/// to a single `-`, and cap the length so labels stay usable in task names,
/// logs, and the UI.
fn sanitize_label(raw: &str) -> String {
    const MAX_LABEL: usize = 64;
    let mut out = String::with_capacity(raw.len().min(MAX_LABEL));
    let mut last_dash = false;
    for ch in raw.chars() {
        if out.len() >= MAX_LABEL {
            break;
        }
        if ch.is_ascii_alphanumeric() || matches!(ch, '_' | '-') {
            out.push(ch);
            last_dash = false;
        } else if !last_dash {
            out.push('-');
            last_dash = true;
        }
    }
    out.trim_matches('-').to_string()
}

/// Resolve a task's fan-out items. `None` element list `[None]` means "one plain
/// instance"; otherwise one `Some(item)` per fan-out element.
fn resolve_items(task: &TaskSpec, ctx: &BTreeMap<String, String>) -> Result<Vec<Option<Value>>> {
    if let Some(items) = &task.with_items {
        return Ok(items.iter().cloned().map(Some).collect());
    }
    if let Some(param) = &task.with_param {
        let raw = substitute(param, ctx);
        let parsed: Value = serde_json::from_str(&raw)
            .map_err(|e| anyhow::anyhow!("with_param '{}' is not valid JSON: {e}", param))?;
        let arr = parsed
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("with_param '{}' did not resolve to a JSON array", param))?;
        return Ok(arr.iter().cloned().map(Some).collect());
    }
    Ok(vec![None])
}

/// Bind `{{ item }}` (and `{{ item.key }}` for object items) into the scope.
fn bind_item(ctx: &mut BTreeMap<String, String>, item: &Value) {
    ctx.insert("item".to_string(), plain(item));
    if let Some(obj) = item.as_object() {
        for (k, v) in obj {
            ctx.insert(format!("item.{k}"), plain(v));
        }
    }
}

/// Render a JSON value as a bare scalar (no quotes for strings/numbers/bools).
fn plain(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        Value::Number(n) => n.to_string(),
        Value::Bool(b) => b.to_string(),
        Value::Null => String::new(),
        other => other.to_string(),
    }
}

/// Replace every `{{ key }}` (optional surrounding whitespace) with `ctx[key]`.
/// Unknown keys are left verbatim so partial templates round-trip harmlessly.
/// Public so the engine's schedule gates (`when:`/`stop:`) evaluate expressions
/// with the same substitution the template expander uses.
pub fn substitute(s: &str, ctx: &BTreeMap<String, String>) -> String {
    if !s.contains("{{") {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < s.len() {
        if i + 1 < s.len() && bytes[i] == b'{' && bytes[i + 1] == b'{' {
            if let Some(close) = s[i + 2..].find("}}") {
                let key = s[i + 2..i + 2 + close].trim();
                if let Some(val) = ctx.get(key) {
                    out.push_str(val);
                } else if let Some(val) = eval_arith(key, ctx) {
                    // Simple `a OP b` integer/float arithmetic (e.g. `{{ n - 1 }}`)
                    // — what makes counting recursion terminate.
                    out.push_str(&val);
                } else {
                    out.push_str(&s[i..i + 2 + close + 2]); // leave verbatim
                }
                i = i + 2 + close + 2;
                continue;
            }
        }
        // push one char (handle UTF-8 boundaries)
        let ch = s[i..].chars().next().unwrap();
        out.push(ch);
        i += ch.len_utf8();
    }
    out
}

/// Evaluate a whitespace-separated `a OP b` arithmetic expression (`+ - * / %`),
/// resolving operands from `ctx` or as numeric literals. Returns `None` if it
/// isn't a 3-token arithmetic expression or an operand can't be resolved — the
/// caller then leaves the `{{ … }}` verbatim. Integer results render without a
/// trailing `.0`. This is the minimal arithmetic that lets a recursive template
/// decrement its counter (`arguments: { n: "{{ n - 1 }}" }`).
fn eval_arith(expr: &str, ctx: &BTreeMap<String, String>) -> Option<String> {
    let toks: Vec<&str> = expr.split_whitespace().collect();
    if toks.len() != 3 {
        return None;
    }
    let resolve = |t: &str| -> Option<f64> {
        ctx.get(t).and_then(|v| v.trim().parse::<f64>().ok()).or_else(|| t.parse::<f64>().ok())
    };
    let a = resolve(toks[0])?;
    let b = resolve(toks[2])?;
    let r = match toks[1] {
        "+" => a + b,
        "-" => a - b,
        "*" => a * b,
        "/" if b != 0.0 => a / b,
        "%" if b != 0.0 => a % b,
        _ => return None,
    };
    Some(if r.fract() == 0.0 {
        // Clamp before the cast so an absurd result formats sanely rather than
        // relying on saturating-cast behaviour (workflow counters never get here).
        (r.clamp(i64::MIN as f64, i64::MAX as f64) as i64).to_string()
    } else {
        r.to_string()
    })
}

/// Evaluate a `when:` expression: `LHS OP RHS` (ops `==,!=,<=,>=,<,>`) or a bare
/// truthy value. Numeric comparison when both sides parse as numbers, else string.
/// Public so the engine's schedule gates reuse the identical comparison semantics
/// as task-level `when:`.
pub fn eval_when(expr: &str) -> Result<bool> {
    let expr = expr.trim();
    for op in ["<=", ">=", "==", "!=", "<", ">"] {
        if let Some(idx) = expr.find(op) {
            let lhs = expr[..idx].trim();
            let rhs = expr[idx + op.len()..].trim();
            let (lf, rf) = (lhs.parse::<f64>(), rhs.parse::<f64>());
            return Ok(match (lf, rf) {
                (Ok(a), Ok(b)) => match op {
                    "<=" => a <= b,
                    ">=" => a >= b,
                    "<" => a < b,
                    ">" => a > b,
                    "==" => a == b,
                    "!=" => a != b,
                    _ => unreachable!(),
                },
                _ => match op {
                    "==" => lhs == rhs,
                    "!=" => lhs != rhs,
                    _ => bail!("non-numeric operands for '{op}' in when: '{expr}'"),
                },
            });
        }
    }
    // Bare value: false-y set, else truthy.
    Ok(!matches!(expr, "" | "false" | "0" | "no"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    /// Parse YAML → expand → return (sorted task names, name→sorted-deps map).
    fn run(yaml: &str) -> (Vec<String>, BTreeMap<String, Vec<String>>) {
        let spec: DagSpec = serde_yaml::from_str(yaml).expect("parse");
        let e = expand(spec).expect("expand");
        let mut names: Vec<String> = e.tasks.iter().map(|t| t.name.clone()).collect();
        names.sort();
        let mut deps = BTreeMap::new();
        for t in &e.tasks {
            let mut d = t.depends_on.clone();
            d.sort();
            deps.insert(t.name.clone(), d);
        }
        (names, deps)
    }

    fn cmd_of<'a>(spec: &'a DagSpec, name: &str) -> &'a [String] {
        &spec.tasks.iter().find(|t| t.name == name).unwrap().command
    }
    fn expanded(yaml: &str) -> DagSpec {
        expand(serde_yaml::from_str(yaml).unwrap()).unwrap()
    }

    #[test]
    fn leaf_only_passthrough() {
        let (names, deps) = run(
            r#"
name: w
tasks:
  - { name: a, command: ["true"] }
  - { name: b, command: ["true"], depends_on: [a] }
"#,
        );
        assert_eq!(names, vec!["a", "b"]);
        assert_eq!(deps["b"], vec!["a"]);
    }

    #[test]
    fn call_expands_and_rewires_deps() {
        // pre → call(process) → post ; process = e -> t -> l
        let (names, deps) = run(
            r#"
name: w
templates:
  - name: process
    tasks:
      - { name: e, command: ["true"] }
      - { name: t, command: ["true"], depends_on: [e] }
      - { name: l, command: ["true"], depends_on: [t] }
tasks:
  - { name: pre, command: ["true"] }
  - { name: call, template: process, depends_on: [pre] }
  - { name: post, command: ["true"], depends_on: [call] }
"#,
        );
        assert_eq!(names, vec!["call.e", "call.l", "call.t", "post", "pre"]);
        // sub-DAG root inherits the call's external dep …
        assert_eq!(deps["call.e"], vec!["pre"]);
        assert_eq!(deps["call.t"], vec!["call.e"]);
        assert_eq!(deps["call.l"], vec!["call.t"]);
        // … and the call's dependent attaches to the sub-DAG's exit.
        assert_eq!(deps["post"], vec!["call.l"]);
    }

    #[test]
    fn parameters_substitute_in_isolated_scope() {
        let spec = expanded(
            r#"
name: w
parameters: { greeting: "hi" }
templates:
  - name: say
    parameters: { who: "world" }
    tasks:
      - { name: msg, command: ["echo", "{{ greeting }}-{{ who }}"] }
tasks:
  - { name: c, template: say, arguments: { who: "{{ greeting }}-dagron" } }
"#,
        );
        // template scope is isolated: {{greeting}} is NOT visible inside `say`
        // (left verbatim); {{who}} came from the caller's argument, which WAS
        // resolved in the caller's scope (so greeting → "hi").
        assert_eq!(cmd_of(&spec, "c.msg"), &["echo", "{{ greeting }}-hi-dagron"]);
    }

    #[test]
    fn fan_out_with_items() {
        let (names, deps) = run(
            r#"
name: w
templates:
  - name: shard
    parameters: { id: "0" }
    tasks: [ { name: work, command: ["echo", "{{ id }}"] } ]
tasks:
  - name: map
    template: shard
    with_items: ["a", "b", "c"]
    arguments: { id: "{{ item }}" }
  - { name: reduce, command: ["true"], depends_on: [map] }
"#,
        );
        assert!(names.contains(&"map.0.work".to_string()));
        assert!(names.contains(&"map.1.work".to_string()));
        assert!(names.contains(&"map.2.work".to_string()));
        // reduce fans-in over all three shard exits
        assert_eq!(deps["reduce"], vec!["map.0.work", "map.1.work", "map.2.work"]);
    }

    #[test]
    fn with_param_parses_json_array() {
        let spec = expanded(
            r#"
name: w
parameters: { shards: "[10, 20]" }
templates:
  - name: s
    parameters: { n: "0" }
    tasks: [ { name: w, command: ["echo", "{{ n }}"] } ]
tasks:
  - { name: m, template: s, with_param: "{{ shards }}", arguments: { n: "{{ item }}" } }
"#,
        );
        assert_eq!(cmd_of(&spec, "m.0.w"), &["echo", "10"]);
        assert_eq!(cmd_of(&spec, "m.1.w"), &["echo", "20"]);
    }

    #[test]
    fn recursion_terminates_via_when_with_arithmetic() {
        // countdown(n): tick(n) ; then recurse(n-1) guarded by `when n > 0`.
        let spec = expanded(
            r#"
name: w
parameters: { start: "2" }
templates:
  - name: countdown
    parameters: { n: "0" }
    tasks:
      - { name: tick, command: ["echo", "{{ n }}"] }
      - name: rec
        template: countdown
        when: "{{ n }} > 0"
        arguments: { n: "{{ n - 1 }}" }
        depends_on: [tick]
tasks:
  - { name: go, template: countdown, arguments: { n: "{{ start }}" } }
"#,
        );
        // n = 2,1,0 → three ticks; arithmetic decremented the counter each level.
        let ticks: Vec<&str> = spec
            .tasks
            .iter()
            .filter(|t| t.name.ends_with("tick"))
            .map(|t| t.command[1].as_str())
            .collect();
        let mut vals: Vec<&str> = ticks.clone();
        vals.sort();
        assert_eq!(vals, vec!["0", "1", "2"], "tasks: {ticks:?}");
    }

    #[test]
    fn unbounded_recursion_hits_depth_cap() {
        let err = expand(
            serde_yaml::from_str(
                r#"
name: w
templates:
  - name: loop
    tasks: [ { name: again, template: loop } ]
tasks:
  - { name: go, template: loop }
"#,
            )
            .unwrap(),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("recursion exceeded depth"), "got: {err}");
    }

    #[test]
    fn empty_fan_out_is_rejected() {
        let spec: DagSpec = serde_yaml::from_str(
            r#"
name: w
templates: [ { name: h, tasks: [ { name: w, command: ["true"] } ] } ]
tasks:
  - { name: m, template: h, with_items: [] }
"#,
        )
        .unwrap();
        let err = expand(spec).unwrap_err().to_string();
        assert!(err.contains("empty list"), "got: {err}");
    }

    #[test]
    fn empty_with_param_fan_out_is_rejected() {
        let spec: DagSpec = serde_yaml::from_str(
            r#"
name: w
parameters: { shards: "[]" }
templates: [ { name: h, tasks: [ { name: w, command: ["true"] } ] } ]
tasks:
  - { name: m, template: h, with_param: "{{ shards }}" }
"#,
        )
        .unwrap();
        let err = expand(spec).unwrap_err().to_string();
        assert!(err.contains("empty list"), "got: {err}");
    }

    #[test]
    fn conditional_skips_leaf() {
        let (names, _deps) = run(
            r#"
name: w
tasks:
  - { name: always, command: ["true"] }
  - { name: never, command: ["true"], when: "0" }
"#,
        );
        assert_eq!(names, vec!["always"]);
    }

    #[test]
    fn rejects_task_with_both_command_and_template() {
        let spec: DagSpec = serde_yaml::from_str(
            r#"
name: w
templates: [ { name: t, tasks: [ { name: x, command: ["true"] } ] } ]
tasks:
  - { name: bad, command: ["true"], template: t }
"#,
        )
        .unwrap();
        assert!(expand(spec).is_err());
    }

    #[test]
    fn every_example_template_expands_and_builds() {
        // The example catalog ships with the product (engine image), not this
        // crate, so it lives at the module root's `examples/` (two levels up from
        // crates/dagron-core).
        let dir = concat!(env!("CARGO_MANIFEST_DIR"), "/../../examples/templates");
        let mut checked = 0;
        for entry in std::fs::read_dir(dir).expect("read examples/templates") {
            let path = entry.unwrap().path();
            if path.extension().and_then(|s| s.to_str()) != Some("yaml") {
                continue;
            }
            let yaml = std::fs::read_to_string(&path).unwrap();
            // from_yaml runs parse → expand → build → cycle/leaf validation.
            crate::dag::DagGraph::from_yaml(&yaml)
                .unwrap_or_else(|e| panic!("{}: {e}", path.display()));
            checked += 1;
        }
        assert!(checked >= 13, "expected the example catalog, found {checked}");
    }

    #[test]
    fn instance_key_labels_fan_out_instances() {
        let (names, deps) = run(
            r#"
name: w
tasks:
  - name: sync
    command: ["echo", "{{ item.region }}"]
    with_items:
      - { region: "us-east-1" }
      - { region: "eu west 2" }
    instance_key: "{{ item.region }}"
  - { name: done, command: ["true"], depends_on: [sync] }
"#,
        );
        // Readable labels replace bare indexes; the space sanitized to a dash.
        assert!(names.contains(&"sync.us-east-1".to_string()), "names: {names:?}");
        assert!(names.contains(&"sync.eu-west-2".to_string()), "names: {names:?}");
        assert_eq!(deps["done"], vec!["sync.eu-west-2", "sync.us-east-1"]);
    }

    #[test]
    fn instance_key_duplicate_labels_rejected() {
        let spec: DagSpec = serde_yaml::from_str(
            r#"
name: w
tasks:
  - name: m
    command: ["true"]
    with_items: ["a!", "a?"]
    instance_key: "{{ item }}"
"#,
        )
        .unwrap();
        // Both items sanitize to "a" — must fail loudly, not silently collide.
        let err = expand(spec).unwrap_err().to_string();
        assert!(err.contains("duplicate label"), "got: {err}");
    }

    #[test]
    fn instance_key_without_fan_out_rejected() {
        let spec: DagSpec = serde_yaml::from_str(
            r#"
name: w
tasks:
  - { name: solo, command: ["true"], instance_key: "x" }
"#,
        )
        .unwrap();
        let err = expand(spec).unwrap_err().to_string();
        assert!(err.contains("without with_items/with_param"), "got: {err}");
    }

    #[test]
    fn sanitize_label_rules() {
        assert_eq!(sanitize_label("us-east-1"), "us-east-1");
        assert_eq!(sanitize_label("a b//c"), "a-b-c"); // runs collapse to one dash
        assert_eq!(sanitize_label("shard.7"), "shard-7"); // '.' is the hierarchy separator
        assert_eq!(sanitize_label("!!!"), ""); // nothing usable survives
        assert_eq!(sanitize_label(&"x".repeat(100)).len(), 64); // length cap
    }

    #[test]
    fn hooks_wire_to_all_non_hook_tasks_with_a_trigger_rule() {
        let spec = expanded(
            r#"
name: w
tasks:
  - { name: a, command: ["true"] }
  - { name: b, command: ["true"], depends_on: [a] }
  - { name: notify, command: ["echo"], hook: on_exit }
  - { name: alert, command: ["echo"], hook: on_failure }
"#,
        );
        let by = |n: &str| spec.tasks.iter().find(|t| t.name == n).unwrap().clone();
        let mut nd = by("notify").depends_on;
        nd.sort();
        assert_eq!(nd, vec!["a", "b"], "on_exit hook depends on all non-hook tasks");
        assert_eq!(by("notify").trigger_rule.as_deref(), Some("all_done"));
        assert_eq!(by("alert").trigger_rule.as_deref(), Some("one_failed"));
        // Hooks do not depend on each other.
        assert!(!by("alert").depends_on.contains(&"notify".to_string()));
        // The whole thing still builds (no cycle, valid graph).
        crate::dag::DagGraph::from_spec(spec).unwrap();
    }

    #[test]
    fn depending_on_a_hook_is_rejected() {
        let err = crate::dag::DagGraph::from_yaml(
            "name: w\ntasks:\n  - { name: fin, command: [\"echo\"], hook: on_exit }\n  - { name: x, command: [\"true\"], depends_on: [fin] }\n",
        )
        .err()
        .expect("depending on a hook must be rejected")
        .to_string();
        assert!(err.contains("cannot depend on hook"), "got: {err}");
    }

    #[test]
    fn when_evaluator() {
        assert!(eval_when("3 > 1").unwrap());
        assert!(!eval_when("1 > 3").unwrap());
        assert!(eval_when("2 == 2").unwrap());
        assert!(eval_when("a != b").unwrap());
        assert!(eval_when("true").unwrap());
        assert!(!eval_when("false").unwrap());
        assert!(!eval_when("0").unwrap());
    }
}
