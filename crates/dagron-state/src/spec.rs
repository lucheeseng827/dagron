//! The minimal slice of dagron's workflow spec this crate emits.
//!
//! dagron's real spec lives in `dagron_core::dag::DagSpec` and is much larger
//! (templates, fan-out, budgets, retry classes, pools…). This crate emits only the
//! fields a compiled state plan actually sets, for the reason spelled out in
//! `Cargo.toml`: linking `dagron-core` here would drag its "exactly one DB backend"
//! constraint into `dagron-api`.
//!
//! The risk of a hand-kept subset is drift. It is closed by
//! `tests/dagron_compat.rs`, which parses this module's output through the real
//! `DagGraph::from_yaml` — the same parse → expand → validate path every submit
//! goes through. If the two models ever disagree, that test fails.
//!
//! Every field is `skip_serializing_if`-empty, so the emitted YAML carries only
//! what was set and stays readable as a PR artifact.

use serde::{Deserialize, Serialize};

/// A dagron workflow: a name and a set of tasks.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct DagSpec {
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
    pub tasks: Vec<TaskSpec>,
}

/// One leaf task: an argv, its dependencies, and its retry/timeout envelope.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq)]
pub struct TaskSpec {
    pub name: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub command: Vec<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub depends_on: Vec<String>,
    /// Free-form JSON carried with the task. This crate uses it to preserve the
    /// planner's own facts — the unsanitized model name, why it is in the plan,
    /// and which partitions it covers — so a run can be traced back to the plan
    /// that produced it even though the task name had to be sanitized.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub input: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_attempts: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_secs: Option<u64>,
    /// Environment for the task, in dagron's own shape: a **list** of
    /// `{ name, value }`, which is what `dagron_core::dag::TaskSpec::env` parses.
    ///
    /// It was a map here, and dagron's parser rejects a map — so every plan compiled
    /// with `options.env` produced YAML dagron refused, unnoticed only because no
    /// compatibility test set an env. `tests/dagron_compat.rs` now does.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub env: Vec<EnvVar>,
}

/// One task environment variable, as dagron spells it: a literal `value`, or a
/// `value_from` secret that dagron resolves at dispatch and never stores.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct EnvVar {
    pub name: String,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub value: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub value_from: Option<SecretRef>,
}

/// `value_from: { secret: NAME }` — dagron-core's `SecretRef`.
#[derive(Clone, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct SecretRef {
    pub secret: String,
}

impl TaskSpec {
    /// The literal value of the environment variable `name`, if the task sets one.
    pub fn env_value(&self, name: &str) -> Option<&str> {
        self.env
            .iter()
            .find(|e| e.name == name && e.value_from.is_none())
            .map(|e| e.value.as_str())
    }

    /// The secret the environment variable `name` is resolved from, if it is one.
    pub fn env_secret(&self, name: &str) -> Option<&str> {
        self.env
            .iter()
            .find(|e| e.name == name)
            .and_then(|e| e.value_from.as_ref())
            .map(|s| s.secret.as_str())
    }
}

impl DagSpec {
    /// Render as the workflow YAML `POST /api/runs` accepts.
    pub fn to_yaml(&self) -> Result<String, serde_yaml::Error> {
        serde_yaml::to_string(self)
    }
}
