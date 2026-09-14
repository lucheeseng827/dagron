//! The planner's frozen JSON wire contract, as dagron reads it.
//!
//! These types mirror the planner's `planner-embed::PlanResponse` — the single
//! JSON funnel every planner binding (C-ABI, WASM, Python, CLI) marshals through.
//! They are **deliberately duplicated** rather than imported: `crates/` is mirrored
//! to the public dagron repo, where the planner's source does not exist, and the
//! whole point of the planner is that it is embeddable by orchestrators that are
//! not dagron.
//!
//! Duplication needs a guard. Upstream has one for free — `planner-embed` converts
//! via an exhaustive, no-wildcard match, so adding a `BackfillReason` variant fails
//! to compile until the contract is updated. This side has no compiler link to
//! upstream, so it is guarded instead by [`WIRE_CONTRACT_VERSION`] and the
//! golden-JSON fixture in `tests/wire_contract.rs`. A contract change must touch
//! both halves.
//!
//! Only the *response* half is mirrored. dagron never builds a `PlanRequest` — it
//! receives a plan someone else computed. That asymmetry is the wire coupling
//! working as intended.
//!
//! ## Versions
//!
//! * **v1** — `name` / `reason` / `unit`. Execution edges had to be guessed from
//!   the single-cause `reason`, which is under-constrained (see [`crate::compile`]).
//! * **v2** — adds [`PlanModel::depends_on`], the planner's own plan-restricted
//!   edge set. Additive and backward compatible: a v1 payload still parses, and a
//!   v2 consumer falls back for it.

use serde::{Deserialize, Serialize};

/// The contract revision this crate reads. Bump when a field or variant changes,
/// and update the golden fixture in the same commit.
pub const WIRE_CONTRACT_VERSION: &str = "planner-embed/3";

/// A planner result: the minimal, topologically-ordered set of models to rebuild.
///
/// `next_state` is carried opaquely as raw JSON. dagron has no business
/// interpreting fingerprints or watermarks — it hands the snapshot back to
/// whoever owns the state backend after a successful run, and a structural mirror
/// of it here would be a second place to keep in sync for no gain.
#[derive(Clone, Debug, Default, Deserialize, Serialize)]
pub struct PlanResponse {
    #[serde(default)]
    pub models: Vec<PlanModel>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub next_state: Option<serde_json::Value>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<PlanError>,
}

/// One model in the plan: what to rebuild, why, at what granularity, and what it
/// must wait for.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PlanModel {
    pub name: String,
    pub reason: Reason,
    pub unit: Unit,
    /// Upstream models this plan also rebuilds, already narrowed to plan members
    /// by the planner. Added in contract v2.
    ///
    /// `Option`, not `Vec`, and that distinction is load-bearing: `None` means the
    /// producer predates the field (v1) and edges must be derived some other way;
    /// `Some([])` is a v2 producer stating authoritatively that this model waits
    /// for nothing. Collapsing the two would silently downgrade every v2 plan
    /// whose first model happens to be a root.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depends_on: Option<Vec<String>>,
    /// What this rebuild does to the target relation. Added in contract v3.
    ///
    /// A plain `Option`, unlike [`Self::depends_on`], and the asymmetry is
    /// deliberate rather than an oversight. There, absence had to be
    /// distinguishable from emptiness because "waits for nothing" and "cannot
    /// tell you what it waits for" demand opposite handling. Here both readings
    /// of `None` — a v2 producer that cannot say, and a v3 producer whose model
    /// declared nothing — mean the same thing to this crate: apply no
    /// replace-specific behaviour. Nothing downstream would branch on the
    /// difference, so encoding one would be ceremony.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub replace: Option<Replace>,
}

/// How a rebuild's rows reach the target relation.
///
/// Mirrors the planner's resolved replace op. This crate does **not** render it:
/// turning `insert_overwrite` into a statement is engine-specific, and dagron has
/// no idea which warehouse is on the other end of a task's command. What it does
/// is carry the facts onto the task — as `input`, and as template placeholders —
/// so the operator's own command can act on them.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Replace {
    pub strategy: ReplaceStrategy,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub partition_column: Option<String>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub unique_key: Vec<String>,
    pub target: ReplaceTarget,
    /// Present when the planner could not honour the declared strategy and
    /// widened to a full refresh. Surfaced prominently by `explain`: it is the
    /// difference between rewriting two partitions and rewriting the table, and
    /// a reviewer should never have to infer it.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub widened: Option<Widening>,
}

/// The replacement semantics, named as the warehouses name them.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplaceStrategy {
    InsertOverwrite,
    DeleteInsert,
    Merge,
    ReplaceWhere,
    FullRefresh,
}

impl ReplaceStrategy {
    /// The token substituted for `{{ replace }}` — the same spelling the config
    /// used, so an operator's command and their declaration read alike.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::InsertOverwrite => "insert_overwrite",
            Self::DeleteInsert => "delete_insert",
            Self::Merge => "merge",
            Self::ReplaceWhere => "replace_where",
            Self::FullRefresh => "full_refresh",
        }
    }
}

/// What the write covers.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ReplaceTarget {
    Whole,
    Partitions(Vec<String>),
}

/// Why the planner widened a declaration it could not honour.
#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case", tag = "widened_because")]
pub enum Widening {
    NotIncremental { materialization: String },
    WholeModelInPlan,
    NoPartitionColumn { declared: ReplaceStrategy },
    NoUniqueKey,
}

impl Widening {
    /// One sentence an operator can act on.
    pub fn explain(&self) -> String {
        match self {
            Self::NotIncremental { materialization } => format!(
                "the model is a {materialization}, so it has no partitions to replace in place"
            ),
            Self::WholeModelInPlan => {
                "the plan rebuilds this model in full, so there is nothing to scope to".to_string()
            }
            Self::NoPartitionColumn { declared } => format!(
                "`{}` needs a partition_column and the project declares none",
                declared.as_str()
            ),
            Self::NoUniqueKey => {
                "`merge` needs a unique_key and the project declares none".to_string()
            }
        }
    }
}

/// Why a model is in the plan. Mirrors `planner_core::BackfillReason`.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Reason {
    /// Its own SQL changed.
    DirectlyChanged,
    /// An upstream model changed in a column this model consumes.
    Downstream {
        because_of: String,
        #[serde(default)]
        via_columns: Vec<String>,
    },
}

/// How much of a model to rebuild. Mirrors `planner_core::BackfillUnit`.
#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Unit {
    FullModel,
    Partitions(Vec<String>),
}

/// A structured planning failure. When present, `models` is empty.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct PlanError {
    pub code: String,
    pub message: String,
}

impl PlanResponse {
    /// The upstream model this one was attributed to, if any.
    pub fn attribution(model: &PlanModel) -> Option<&str> {
        match &model.reason {
            Reason::DirectlyChanged => None,
            Reason::Downstream { because_of, .. } => Some(because_of.as_str()),
        }
    }
}
