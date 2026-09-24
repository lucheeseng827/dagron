//! Compile a planner state plan into a dagron run graph.
//!
//! This is the whole load-bearing step of the component: `PlanResponse` in,
//! [`DagSpec`] out. Everything else in this crate is transport around it.
//!
//! ## Where `depends_on` comes from
//!
//! Contract **v2** plans carry the planner's own edge set, already narrowed to the
//! models the plan rebuilds. That is the authoritative answer and the common case:
//! nothing is guessed and the caller supplies nothing.
//!
//! It has to be carried explicitly because the rest of the plan cannot stand in for
//! it. A plan is topologically ordered, but order is not adjacency — it says `b`
//! *may* run after `a`, not that it *must*. And each model's `reason` names a
//! single `because_of` cause, which is an explanation, not a dependency list: a
//! model downstream of both `a` and `b` is blamed on one of them, so edges built
//! from attributions are under-constrained and a rebuild can start before one of
//! its inputs finishes.
//!
//! So [`Ordering::Derived`] (the default) reads, in descending order of trust: the
//! plan's `depends_on`, then a caller-supplied `graph`, then `because_of`. Only the
//! last is lossy, and it is reached only for a v1 plan with no `graph`.
//! [`Ordering::Sequential`] remains available and chains the plan into one file —
//! always correct, no parallelism — for callers who would rather not reason about
//! any of this.
//!
//! Whatever the source, edges are filtered to models appearing **earlier in the
//! plan**, so a malformed or stale `graph` can never produce a cycle that dagron
//! would reject at submit time.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::spec::{DagSpec, EnvVar, SecretRef, TaskSpec};
use crate::wire::{PlanModel, PlanResponse, Reason, Unit};

/// How to derive task dependencies from a plan.
#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Ordering {
    /// Edges from the plan's own `depends_on` (contract v2), else the supplied
    /// `graph`, else that model's `because_of`. Only the last is lossy; see
    /// [`derived_edges`] for why, and when each source is reached.
    #[default]
    Derived,
    /// One task after another in plan order. Correct under any input.
    Sequential,
}

/// Knobs for [`compile`]. Everything has a defensible default except
/// `command_template`, which only the operator can know.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct CompileOptions {
    /// Workflow name for the generated spec.
    #[serde(default = "default_workflow_name")]
    pub workflow_name: String,
    /// The argv to run per model. Supports `{{ model }}`, `{{ unit }}`,
    /// `{{ partitions }}` (comma-joined; empty for a full-model rebuild); from
    /// contract v3 `{{ replace }}`, `{{ partition_column }}` and `{{ unique_key }}`,
    /// which expand to the planner's declaration and are empty when the plan carries
    /// none; and from contract v4 `{{ sql }}` and `{{ dialect }}` — the rendered
    /// statements (see [`crate::wire::Sql::script`]) and their dialect. Each placeholder
    /// is written `{{ name }}` or `{{name}}`; anything else in braces is left alone for
    /// the command's own templating.
    ///
    /// e.g. `["sh", "-c", "dbt run --select {{ model }}"]`, or `["dagron-step-sql"]`
    /// with `env: { SQL_STATEMENT: "{{ sql }}", SQL_MODE: script, … }`.
    pub command_template: Vec<String>,
    #[serde(default)]
    pub ordering: Ordering,
    #[serde(default)]
    pub max_attempts: Option<u32>,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    /// Env applied to every generated task, with the same placeholders expanded per
    /// task as `command_template` (from contract v4). That is what lets a statement
    /// travel in an environment variable — `SQL_STATEMENT: "{{ sql }}"` — instead of
    /// being quoted into a shell command line.
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    /// Env resolved from dagron secrets: variable name → secret name, compiled to
    /// `value_from: { secret: … }` on every task. The credential never travels in
    /// the plan or this envelope — only the secret's name does — and dagron resolves
    /// it at dispatch without storing it. How a warehouse password reaches
    /// `dagron-step-sql` (`{"SQL_PASSWORD": "WAREHOUSE_PASSWORD"}`). Not expanded.
    #[serde(default)]
    pub secret_env: BTreeMap<String, String>,
    /// Tags for the generated workflow. Defaults to `["state-plan"]` so these runs
    /// are filterable in the console away from ordinary workflows.
    #[serde(default = "default_tags")]
    pub tags: Vec<String>,
}

fn default_workflow_name() -> String {
    "state-plan".to_string()
}

fn default_tags() -> Vec<String> {
    vec!["state-plan".to_string()]
}

impl Default for CompileOptions {
    fn default() -> Self {
        Self {
            workflow_name: default_workflow_name(),
            command_template: Vec::new(),
            ordering: Ordering::default(),
            max_attempts: None,
            timeout_secs: None,
            env: BTreeMap::new(),
            secret_env: BTreeMap::new(),
            tags: default_tags(),
        }
    }
}

/// What the `state` endpoints accept: a plan, optionally the real project graph,
/// and how to turn them into tasks.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PlanEnvelope {
    pub plan: PlanResponse,
    /// `model -> upstream models`. Two distinct uses, both optional:
    ///
    /// * **Edges**, for plans from a contract-v1 producer that cannot carry their
    ///   own. A v2 plan's `depends_on` wins over this — see the module docs.
    /// * **Project size**, which only this can supply. A plan sees the models it
    ///   selected, never the ones it skipped, so the pruning percentage in an
    ///   explanation is reportable only when this is given.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub graph: Option<BTreeMap<String, Vec<String>>>,
    #[serde(default)]
    pub options: CompileOptions,
}

/// Why a plan could not become a run graph.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CompileError {
    /// The planner itself failed; there is nothing to compile.
    PlannerFailed { code: String, message: String },
    /// A plan with no models. Not an error upstream — it is the good case, "nothing
    /// to rebuild" — but there is no run to submit for it.
    EmptyPlan,
    /// No `command_template`: nothing would run.
    NoCommand,
    /// The template references `{{ sql }}` but this model carries no SQL, or only
    /// blank statements — the plan was produced without a dialect. Expanding it to
    /// nothing would hand the task an empty statement, so it is refused here rather
    /// than failing at run time.
    MissingSql { model: String },
    /// A variable set both literally (`env`) and from a secret (`secret_env`): which
    /// one the task gets would be an accident of ordering, so neither is chosen.
    EnvConflict { name: String },
}

impl std::fmt::Display for CompileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::PlannerFailed { code, message } => {
                write!(f, "planner failed [{code}]: {message}")
            }
            Self::EmptyPlan => write!(f, "plan is empty: nothing to rebuild"),
            Self::NoCommand => write!(f, "options.command_template is required"),
            Self::MissingSql { model } => write!(
                f,
                "the command or env uses {{{{ sql }}}} but model `{model}` carries no runnable SQL — \
                 plan with a dialect (e.g. `freshet submit --sql postgres`)"
            ),
            Self::EnvConflict { name } => write!(
                f,
                "`{name}` is in both options.env and options.secret_env: set it one way"
            ),
        }
    }
}

impl std::error::Error for CompileError {}

/// Compile a plan envelope into a dagron workflow spec.
pub fn compile(env: &PlanEnvelope) -> Result<DagSpec, CompileError> {
    if let Some(err) = &env.plan.error {
        return Err(CompileError::PlannerFailed {
            code: err.code.clone(),
            message: err.message.clone(),
        });
    }
    if env.plan.models.is_empty() {
        return Err(CompileError::EmptyPlan);
    }
    if env.options.command_template.is_empty() {
        return Err(CompileError::NoCommand);
    }
    if let Some(name) = env.options.secret_env.keys().find(|k| env.options.env.contains_key(*k)) {
        return Err(CompileError::EnvConflict { name: name.clone() });
    }
    let wants_sql = env
        .options
        .command_template
        .iter()
        .chain(env.options.env.values())
        .any(|t| t.contains("{{ sql }}") || t.contains("{{sql}}"));
    if wants_sql {
        // Blank counts as missing: a script of only empty statements would compile
        // here and be refused by the step only after the workflow was submitted.
        let runnable = |sql: &crate::wire::Sql| sql.statements.iter().any(|s| !s.trim().is_empty());
        if let Some(m) = env.plan.models.iter().find(|m| !m.sql.as_ref().is_some_and(runnable)) {
            return Err(CompileError::MissingSql { model: m.name.clone() });
        }
    }

    // Model name -> task name, and the plan position that makes edge filtering
    // acyclic. Built in one pass so later models can resolve earlier ones.
    let mut task_names: BTreeMap<&str, String> = BTreeMap::new();
    let mut position: BTreeMap<&str, usize> = BTreeMap::new();
    let mut taken: BTreeSet<String> = BTreeSet::new();
    for (i, model) in env.plan.models.iter().enumerate() {
        let name = unique_task_name(&model.name, &mut taken);
        task_names.insert(model.name.as_str(), name);
        position.insert(model.name.as_str(), i);
    }

    let mut tasks = Vec::with_capacity(env.plan.models.len());
    for (i, model) in env.plan.models.iter().enumerate() {
        let depends_on = match env.options.ordering {
            Ordering::Sequential => env
                .plan
                .models
                .get(i.wrapping_sub(1))
                .filter(|_| i > 0)
                .and_then(|prev| task_names.get(prev.name.as_str()))
                .cloned()
                .into_iter()
                .collect(),
            Ordering::Derived => derived_edges(model, i, env, &task_names, &position),
        };

        let values = placeholder_values(model);
        tasks.push(TaskSpec {
            name: task_names[model.name.as_str()].clone(),
            command: env.options.command_template.iter().map(|arg| expand(arg, &values)).collect(),
            depends_on,
            input: Some(task_input(model)),
            max_attempts: env.options.max_attempts,
            timeout_secs: env.options.timeout_secs,
            env: env
                .options
                .env
                .iter()
                .map(|(k, v)| EnvVar { name: k.clone(), value: expand(v, &values), value_from: None })
                .chain(env.options.secret_env.iter().map(|(k, secret)| EnvVar {
                    name: k.clone(),
                    value: String::new(),
                    value_from: Some(SecretRef { secret: secret.clone() }),
                }))
                .collect(),
        });
    }

    Ok(DagSpec { name: env.options.workflow_name.clone(), tags: env.options.tags.clone(), tasks })
}

/// Edges for one model under [`Ordering::Derived`], in descending order of trust.
///
/// 1. **The plan's own `depends_on`** (contract v2). The planner computed the plan
///    and knows the project graph, so this is the authoritative answer and needs
///    no help from the caller. Exact.
/// 2. **A caller-supplied `graph`**, for plans from a v1 producer that cannot
///    carry edges. Exact if the caller's graph is accurate.
/// 3. **The `because_of` attribution**, when there is nothing else. Under-
///    constrained by construction — it names one cause, not the edge set — and
///    kept only so a v1 plan with no `graph` still produces *some* ordering
///    rather than a flat fan-out.
///
/// Every source is filtered to models that are in the plan AND appear before this
/// one, which is what keeps the result acyclic even if a caller's graph is stale.
/// That filter is a no-op for source 1 — the planner already restricts to plan
/// members and emits them in topological order — and a real guard for the others.
fn derived_edges(
    model: &PlanModel,
    idx: usize,
    env: &PlanEnvelope,
    task_names: &BTreeMap<&str, String>,
    position: &BTreeMap<&str, usize>,
) -> Vec<String> {
    let upstreams: Vec<&str> = match (&model.depends_on, env.graph.as_ref().and_then(|g| g.get(&model.name))) {
        (Some(deps), _) => deps.iter().map(String::as_str).collect(),
        (None, Some(ups)) => ups.iter().map(String::as_str).collect(),
        (None, None) => PlanResponse::attribution(model).into_iter().collect(),
    };

    let mut edges: Vec<String> = upstreams
        .into_iter()
        .filter(|up| position.get(up).is_some_and(|&p| p < idx))
        .filter_map(|up| task_names.get(up).cloned())
        .collect();
    edges.sort();
    edges.dedup();
    edges
}

/// Every placeholder's value for one model.
///
/// The replace placeholders expand to the *declaration*, never to SQL: dagron does
/// not know which warehouse runs the command, so `{{ replace }}` becomes
/// `insert_overwrite`, not an `INSERT OVERWRITE` statement. `{{ sql }}` is the
/// exception that proves it: the planner rendered those statements for a dialect it
/// was told, and they reach the task byte for byte.
///
/// An absent value expands to the empty string rather than leaving the braces in
/// place. A literal `{{ replace }}` reaching a shell would be a confusing failure.
/// (`{{ sql }}` with no SQL never gets this far — [`compile`] refuses it.)
fn placeholder_values(model: &PlanModel) -> BTreeMap<&'static str, String> {
    let (unit, partitions) = match &model.unit {
        Unit::FullModel => ("full_model", String::new()),
        Unit::Partitions(ps) => ("partitions", ps.join(",")),
    };
    let replace = model.replace.as_ref();
    BTreeMap::from([
        ("model", model.name.clone()),
        ("unit", unit.to_string()),
        ("partitions", partitions),
        ("replace", replace.map(|r| r.strategy.as_str().to_string()).unwrap_or_default()),
        ("partition_column", replace.and_then(|r| r.partition_column.clone()).unwrap_or_default()),
        ("unique_key", replace.map(|r| r.unique_key.join(",")).unwrap_or_default()),
        ("sql", model.sql.as_ref().map(|s| s.script()).unwrap_or_default()),
        ("dialect", model.sql.as_ref().map(|s| s.dialect.clone()).unwrap_or_default()),
    ])
}

/// Substitute the placeholders in one template, in a **single pass**.
///
/// Single-pass is the correctness property, not a style choice. The previous
/// implementation chained `str::replace` once per placeholder, so text a substitution
/// had just inserted was scanned again by every later one: a model whose rendered SQL
/// contained the literal `{{ model }}` (in a string or a comment) had it rewritten
/// inside the statement. Here the output of a substitution is never re-read.
///
/// Only the two spellings this crate has always accepted — `{{ name }}` and
/// `{{name}}` — and only known names are substituted; anything else in braces is
/// the command's own templating and passes through untouched.
fn expand(template: &str, values: &BTreeMap<&'static str, String>) -> String {
    let mut out = String::with_capacity(template.len());
    let mut rest = template;
    while let Some(at) = rest.find("{{") {
        out.push_str(&rest[..at]);
        let tail = &rest[at..];
        let hit = values.iter().find_map(|(name, value)| {
            [format!("{{{{ {name} }}}}"), format!("{{{{{name}}}}}")]
                .into_iter()
                .find(|spelling| tail.starts_with(spelling.as_str()))
                .map(|spelling| (spelling.len(), value))
        });
        match hit {
            Some((len, value)) => {
                out.push_str(value);
                rest = &tail[len..];
            }
            None => {
                out.push_str("{{");
                rest = &tail[2..];
            }
        }
    }
    out.push_str(rest);
    out
}

/// The planner's own facts, preserved on the task. The task name is sanitized and
/// possibly suffixed, so it is not a reliable key back to the model — this is.
fn task_input(model: &PlanModel) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    obj.insert("model".into(), model.name.clone().into());
    match &model.reason {
        Reason::DirectlyChanged => {
            obj.insert("reason".into(), "directly_changed".into());
        }
        Reason::Downstream { because_of, via_columns } => {
            obj.insert("reason".into(), "downstream".into());
            obj.insert("because_of".into(), because_of.clone().into());
            obj.insert("via_columns".into(), via_columns.clone().into());
        }
    }
    match &model.unit {
        Unit::FullModel => {
            obj.insert("unit".into(), "full_model".into());
        }
        Unit::Partitions(ps) => {
            obj.insert("unit".into(), "partitions".into());
            obj.insert("partitions".into(), ps.clone().into());
        }
    }
    // Contract v3. Recorded on the task rather than only substituted into the
    // command, so a run's history says what it meant to do to the table even when
    // the command never referenced it — including, above all, that it widened.
    if let Some(r) = &model.replace {
        obj.insert("replace".into(), r.strategy.as_str().into());
        if let Some(col) = &r.partition_column {
            obj.insert("partition_column".into(), col.clone().into());
        }
        if !r.unique_key.is_empty() {
            obj.insert("unique_key".into(), r.unique_key.clone().into());
        }
        if let Some(w) = &r.widened {
            obj.insert("replace_widened".into(), w.explain().into());
        }
    }
    // Contract v4. The statements themselves, not a digest: a run's history should
    // say exactly what it asked the warehouse to do, and whether that could be left
    // half-done.
    if let Some(sql) = &model.sql {
        obj.insert("sql_dialect".into(), sql.dialect.clone().into());
        obj.insert("sql_statements".into(), sql.statements.clone().into());
        obj.insert("sql_atomic".into(), sql.atomic.into());
    }
    serde_json::Value::Object(obj)
}

/// Sanitize a model name into a dagron task name, keeping it unique within the run.
///
/// Model names are dotted and schema-qualified (`analytics.stg_orders`); dagron
/// gives `.` its own meaning in fan-out instance names (`<task>.<label>`), so
/// passing one through would produce a task that reads like an expansion instance.
fn unique_task_name(model: &str, taken: &mut BTreeSet<String>) -> String {
    let mut base: String = model
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() || c == '_' || c == '-' { c } else { '_' })
        .collect();
    if base.is_empty() {
        base = "model".to_string();
    }

    if taken.insert(base.clone()) {
        return base;
    }
    // Sanitizing can collide two distinct models (`a.b` and `a-b` both become
    // `a_b`). Suffix until free rather than silently emitting a duplicate task
    // name, which dagron rejects at parse time.
    for n in 2.. {
        let candidate = format!("{base}_{n}");
        if taken.insert(candidate.clone()) {
            return candidate;
        }
    }
    unreachable!("an unbounded search for a free name cannot fall through")
}
