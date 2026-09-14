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

use std::collections::BTreeMap;

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
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub env: BTreeMap<String, String>,
}

impl DagSpec {
    /// Render as the workflow YAML `POST /api/runs` accepts.
    pub fn to_yaml(&self) -> Result<String, serde_yaml::Error> {
        serde_yaml::to_string(self)
    }
}
