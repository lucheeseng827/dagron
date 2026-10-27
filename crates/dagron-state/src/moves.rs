//! Compile a warehouse move into a dagron run graph: each unit an unload, a load
//! and a parity check, in that order.
//!
//! A *move* is Freshet's other document beside the plan (`freshet move --json`,
//! contract [`MOVE_CONTRACT_VERSION`]). It names, for each unit (one partition of
//! one model, or one whole relation), the statements that unload it from the source
//! into Parquet on a stage, the statements that load it into the target, and what
//! to compare on each side afterwards. This crate turns that into tasks. As with a
//! plan, it writes no SQL: the statements reach the tasks byte for byte.
//!
//! ```text
//!   <unit>__unload  ──►  <unit>__load  ──►  <unit>__parity
//!   dagron-step-sql      dagron-step-sql    dagron-step-sql parity
//!   (SQL_ENGINE: the     (the target, or    (both stores; exit 2 when
//!    source, or duckdb)   duckdb)            not shown equal)
//! ```
//!
//! **Units are independent of one another.** A copy reads a relation's stored
//! rows, not a model's SQL, so nothing orders one unit after another, and dagron
//! runs them as wide as the run's concurrency allows. That is the point of moving
//! by partition: a failure retries one partition, and the cut-over is decided on
//! per-partition parity.
//!
//! **Connections are the caller's**, per kind of task: `options.unload`,
//! `options.load` and `options.parity`, each an `env` and a `secret_env` the way a
//! plan's options are. The move names no host and holds no credential, and this
//! crate adds none. It does set the variables only the move can know
//! (`SQL_ENGINE`, `SQL_MODE`, `SQL_STATEMENT`, and the parity sides' `ENGINE`,
//! `SCHEMA`, `TABLE`, `RELATION` and `WHERE`), and it refuses options that try to set
//! them too.

use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

use crate::compile::{expand, unique_task_name};
use crate::spec::{DagSpec, EnvVar, SecretRef, TaskSpec};

/// The move contract this crate reads. Freshet keeps the same string as
/// `freshet_embed::MOVE_CONTRACT_VERSION`.
pub const MOVE_CONTRACT_VERSION: &str = "freshet-move/1";

// ---------------------------------------------------------------------------
// The wire (mirrored, like the plan's: the producer is never linked)
// ---------------------------------------------------------------------------

/// `freshet move --json`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct Move {
    pub contract: String,
    pub from: String,
    pub to: String,
    pub stage: String,
    pub units: Vec<MoveUnit>,
    #[serde(default)]
    pub not_moved: Vec<NotMoved>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct MoveUnit {
    pub model: String,
    /// `None` is the whole relation.
    pub partition: Option<Partition>,
    pub location: String,
    pub unload: Step,
    pub load: Step,
    pub parity: Parity,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct Partition {
    pub column: String,
    pub key: String,
    #[serde(default)]
    pub partition_type: String,
}

/// Statements one task runs, in order, on one session.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct Step {
    /// The `SQL_ENGINE` that runs them.
    pub engine: String,
    pub statements: Vec<String>,
    pub atomic: bool,
}

impl Step {
    /// The statements as one script, each terminated with `;`, as Freshet's own
    /// `script()` joins them.
    pub fn script(&self) -> String {
        self.statements.iter().map(|s| format!("{s};")).collect::<Vec<_>>().join("\n")
    }
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct Parity {
    pub source: ParitySide,
    pub target: ParitySide,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct ParitySide {
    pub engine: String,
    pub schema: Option<String>,
    pub table: String,
    pub relation: String,
    pub predicate: Option<String>,
}

#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct NotMoved {
    pub model: String,
    pub reason: String,
}

// ---------------------------------------------------------------------------
// Options and the envelope
// ---------------------------------------------------------------------------

/// Env for one kind of task, with the same placeholders expanded per unit:
/// `{{ model }}`, `{{ partition }}` (the key, empty for a whole relation) and
/// `{{ location }}`. `secret_env` is variable name → dagron secret name, compiled to
/// `value_from`, and never expanded.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct TaskEnv {
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub secret_env: BTreeMap<String, String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct MoveOptions {
    #[serde(default = "default_workflow_name")]
    pub workflow_name: String,
    #[serde(default = "default_tags")]
    pub tags: Vec<String>,
    #[serde(default)]
    pub max_attempts: Option<u32>,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    /// Compile only the parity checks: for a side-by-side period, when both
    /// warehouses already hold the partitions and the question is whether they
    /// still agree.
    #[serde(default)]
    pub check_only: bool,
    /// The argv of the unload and load tasks.
    #[serde(default = "default_sql_command")]
    pub sql_command: Vec<String>,
    /// The argv of the parity tasks.
    #[serde(default = "default_parity_command")]
    pub parity_command: Vec<String>,
    /// How the unload reaches the source (or, on a Postgres source, the PG*
    /// environment DuckDB attaches it through) and the stage.
    #[serde(default)]
    pub unload: TaskEnv,
    /// How the load reaches the target and the stage.
    #[serde(default)]
    pub load: TaskEnv,
    /// How the parity check reaches both stores: `PARITY_SOURCE_DSN`,
    /// `PARITY_SOURCE_PASSWORD`, `PARITY_TARGET_DSN`, ….
    #[serde(default)]
    pub parity: TaskEnv,
}

fn default_workflow_name() -> String {
    "warehouse-move".into()
}

fn default_tags() -> Vec<String> {
    vec!["warehouse-move".into()]
}

fn default_sql_command() -> Vec<String> {
    vec!["dagron-step-sql".into()]
}

fn default_parity_command() -> Vec<String> {
    vec!["dagron-step-sql".into(), "parity".into()]
}

impl Default for MoveOptions {
    fn default() -> Self {
        MoveOptions {
            workflow_name: default_workflow_name(),
            tags: default_tags(),
            max_attempts: None,
            timeout_secs: None,
            check_only: false,
            sql_command: default_sql_command(),
            parity_command: default_parity_command(),
            unload: TaskEnv::default(),
            load: TaskEnv::default(),
            parity: TaskEnv::default(),
        }
    }
}

/// What `POST /moves` accepts.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct MoveEnvelope {
    #[serde(rename = "move")]
    pub moved: Move,
    #[serde(default)]
    pub options: MoveOptions,
}

/// Why a move could not become a run graph.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum MoveCompileError {
    /// A move with no units: nothing to copy, so no run.
    EmptyMove,
    /// A contract this build does not read. A later revision may change what a
    /// field means, so it is refused rather than read as this one.
    Contract { got: String },
    /// An option sets a variable only the move can know.
    Reserved { kind: &'static str, name: String },
    /// A variable in both `env` and `secret_env` of one kind of task.
    EnvConflict { kind: &'static str, name: String },
    /// A unit whose step has no statement to run.
    EmptyStep { unit: String, step: &'static str },
}

impl std::fmt::Display for MoveCompileError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::EmptyMove => write!(f, "the move has no units: nothing to copy"),
            Self::Contract { got } => write!(
                f,
                "this build reads {MOVE_CONTRACT_VERSION} moves, and this one says {got:?}"
            ),
            Self::Reserved { kind, name } => write!(
                f,
                "options.{kind} sets `{name}`, which the move itself sets for every task: drop it"
            ),
            Self::EnvConflict { kind, name } => write!(
                f,
                "`{name}` is in both options.{kind}.env and options.{kind}.secret_env: set it one way"
            ),
            Self::EmptyStep { unit, step } => write!(f, "unit `{unit}` has no {step} statement"),
        }
    }
}

impl std::error::Error for MoveCompileError {}

// ---------------------------------------------------------------------------
// Compile
// ---------------------------------------------------------------------------

const SQL_RESERVED: &[&str] = &["SQL_ENGINE", "SQL_MODE", "SQL_STATEMENT", "SQL_STATEMENT_FILE"];
const PARITY_RESERVED: &[&str] = &["ENGINE", "SCHEMA", "TABLE", "RELATION", "WHERE"];

pub fn compile_move(env: &MoveEnvelope) -> Result<DagSpec, MoveCompileError> {
    let m = &env.moved;
    let o = &env.options;
    if m.contract != MOVE_CONTRACT_VERSION {
        return Err(MoveCompileError::Contract { got: m.contract.clone() });
    }
    if m.units.is_empty() {
        return Err(MoveCompileError::EmptyMove);
    }
    for (kind, task_env) in [("unload", &o.unload), ("load", &o.load), ("parity", &o.parity)] {
        if let Some(name) = task_env.secret_env.keys().find(|k| task_env.env.contains_key(*k)) {
            return Err(MoveCompileError::EnvConflict { kind, name: name.clone() });
        }
        let reserved = |name: &str| match kind {
            "parity" => ["PARITY_SOURCE_", "PARITY_TARGET_"]
                .iter()
                .any(|p| name.strip_prefix(p).is_some_and(|rest| PARITY_RESERVED.contains(&rest))),
            _ => SQL_RESERVED.contains(&name),
        };
        if let Some(name) = task_env.env.keys().chain(task_env.secret_env.keys()).find(|k| reserved(k)) {
            return Err(MoveCompileError::Reserved { kind, name: name.clone() });
        }
    }

    let mut taken = BTreeSet::new();
    let mut tasks = Vec::new();
    for unit in &m.units {
        let label = match &unit.partition {
            Some(p) => format!("{}__{}_{}", unit.model, p.column, p.key),
            None => unit.model.clone(),
        };
        if !o.check_only {
            for (step, s) in [("unload", &unit.unload), ("load", &unit.load)] {
                if !s.statements.iter().any(|st| !st.trim().is_empty()) {
                    return Err(MoveCompileError::EmptyStep { unit: label, step });
                }
            }
        }
        let values = BTreeMap::from([
            ("model", unit.model.clone()),
            ("partition", unit.partition.as_ref().map(|p| p.key.clone()).unwrap_or_default()),
            ("location", unit.location.clone()),
        ]);

        let mut previous: Option<String> = None;
        if !o.check_only {
            for (kind, step, task_env) in [("unload", &unit.unload, &o.unload), ("load", &unit.load, &o.load)] {
                let mut vars = vec![
                    EnvVar { name: "SQL_ENGINE".into(), value: step.engine.clone(), value_from: None },
                    EnvVar { name: "SQL_MODE".into(), value: "script".into(), value_from: None },
                    EnvVar { name: "SQL_STATEMENT".into(), value: step.script(), value_from: None },
                ];
                vars.extend(user_env(task_env, &values));
                let task = unique_task_name(&format!("{label}__{kind}"), &mut taken);
                tasks.push(TaskSpec {
                    name: task.clone(),
                    command: o.sql_command.clone(),
                    depends_on: previous.iter().cloned().collect(),
                    input: Some(task_input(unit, kind, Some(step))),
                    max_attempts: o.max_attempts,
                    timeout_secs: o.timeout_secs,
                    env: vars,
                });
                previous = Some(task);
            }
        }

        let mut vars = Vec::new();
        for (prefix, side) in [("PARITY_SOURCE_", &unit.parity.source), ("PARITY_TARGET_", &unit.parity.target)] {
            let mut set = |k: &str, v: &str| vars.push(EnvVar { name: format!("{prefix}{k}"), value: v.into(), value_from: None });
            set("ENGINE", &side.engine);
            set("TABLE", &side.table);
            set("RELATION", &side.relation);
            if let Some(s) = &side.schema {
                set("SCHEMA", s);
            }
            if let Some(w) = &side.predicate {
                set("WHERE", w);
            }
        }
        vars.extend(user_env(&o.parity, &values));
        let task = unique_task_name(&format!("{label}__parity"), &mut taken);
        tasks.push(TaskSpec {
            name: task,
            command: o.parity_command.clone(),
            depends_on: previous.into_iter().collect(),
            input: Some(task_input(unit, "parity", None)),
            max_attempts: o.max_attempts,
            timeout_secs: o.timeout_secs,
            env: vars,
        });
    }
    Ok(DagSpec { name: o.workflow_name.clone(), tags: o.tags.clone(), tasks })
}

fn user_env(task_env: &TaskEnv, values: &BTreeMap<&'static str, String>) -> Vec<EnvVar> {
    task_env
        .env
        .iter()
        .map(|(k, v)| EnvVar { name: k.clone(), value: expand(v, values), value_from: None })
        .chain(task_env.secret_env.iter().map(|(k, secret)| EnvVar {
            name: k.clone(),
            value: String::new(),
            value_from: Some(SecretRef { secret: secret.clone() }),
        }))
        .collect()
}

/// The move's own facts, on the task: which unit, which step, the statements it
/// ran. A run's history then says what it copied, from where, to where.
fn task_input(unit: &MoveUnit, step: &str, sql: Option<&Step>) -> serde_json::Value {
    let mut obj = serde_json::Map::new();
    obj.insert("model".into(), unit.model.clone().into());
    obj.insert("move_step".into(), step.into());
    obj.insert("location".into(), unit.location.clone().into());
    if let Some(p) = &unit.partition {
        obj.insert("partition_column".into(), p.column.clone().into());
        obj.insert("partition".into(), p.key.clone().into());
    }
    if let Some(s) = sql {
        obj.insert("sql_engine".into(), s.engine.clone().into());
        obj.insert("sql_statements".into(), s.statements.clone().into());
        obj.insert("sql_atomic".into(), s.atomic.into());
    }
    serde_json::Value::Object(obj)
}
