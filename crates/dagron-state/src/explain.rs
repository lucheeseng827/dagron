//! Explain a state plan: what will rebuild, why, and what it saved.
//!
//! The compile step answers *what runs*. This answers *why* — and it is the half
//! that has to be legible to a person reviewing a pull request, not just to the
//! scheduler. It renders two artifacts from the same pass:
//!
//! * **Markdown** — a GitHub-flavored summary table, for a PR comment. Same output
//!   shape as `dagron-plan`, which answers the equivalent question for workflow
//!   changes ("what does this change do before I merge it").
//! * **Mermaid** — a `flowchart TD` of the plan, with directly-changed models
//!   styled apart from ones pulled in downstream. GitHub renders Mermaid natively
//!   in comments, so this needs no client library to be useful.
//!
//! ## The pruning number
//!
//! When the caller supplies a `graph`, the total project size is known, so the
//! summary can state the thing the planner actually exists to do: *N of M models,
//! P% pruned*. Without a graph the plan alone cannot know how many models it did
//! **not** select, and the summary says so rather than implying a saving it cannot
//! measure. Overstating that number would undercut the one claim worth making.

use std::collections::{BTreeMap, BTreeSet};

use serde::Serialize;

use crate::compile::PlanEnvelope;
use crate::spec::DagSpec;
use crate::wire::{PlanModel, Reason, Replace, ReplaceTarget, Unit, Widening};

/// A rendered explanation of a plan.
#[derive(Clone, Debug, Serialize)]
pub struct Explanation {
    /// One-line summary, e.g. "2 of 5 models rebuild — 60% pruned".
    pub summary: String,
    /// GitHub-flavored markdown: the summary, a per-model table, and the graph.
    pub markdown: String,
    /// A Mermaid `flowchart TD` of the plan. Rendered natively by GitHub.
    pub mermaid: String,
    /// Per-model rows, for a client that would rather lay this out itself.
    pub rows: Vec<ExplainRow>,
}

/// One model's line in the explanation.
#[derive(Clone, Debug, Serialize)]
pub struct ExplainRow {
    /// The model name as the planner knows it (not the sanitized task name).
    pub model: String,
    /// The dagron task this model became.
    pub task: String,
    /// `directly_changed` or `downstream`.
    pub reason: String,
    /// For a downstream model, the upstream it was attributed to.
    pub because_of: Option<String>,
    /// The columns that carried the change down.
    pub via_columns: Vec<String>,
    /// Human-readable unit: "full model" or "N partitions".
    pub unit: String,
    /// The tasks this one waits on.
    pub depends_on: Vec<String>,
    /// What the rebuild does to the relation, e.g. "insert_overwrite over 2
    /// partitions on `dt`". `None` when the plan declares nothing (contract v2,
    /// or a v3 model with no declaration — see `wire::PlanModel::replace`).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub writes: Option<String>,
    /// Why the planner widened the declared strategy, when it did. Kept separate
    /// from `writes` so a client can style it as the warning it is.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub widened: Option<String>,
    /// The dialect the planner rendered this model's SQL for, when it did (v4).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql_dialect: Option<String>,
    /// How many statements that SQL is.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sql_statements: Option<usize>,
    /// `true` when the planner said the statements are **not** all-or-nothing: a
    /// failure part-way can leave the relation half-written. Kept as its own field,
    /// like `widened`, so a client can style it as the warning it is.
    #[serde(skip_serializing_if = "std::ops::Not::not")]
    pub non_atomic: bool,
}

/// Render an explanation for a plan and the spec it compiled to.
///
/// Takes both because they carry different halves of the answer: the plan knows
/// *why* each model is here, the spec knows *what it became* after name
/// sanitization and edge derivation. Explaining from either alone would describe a
/// run that is not the one being submitted.
pub fn explain(envelope: &PlanEnvelope, spec: &DagSpec) -> Explanation {
    let rows: Vec<ExplainRow> = envelope
        .plan
        .models
        .iter()
        .zip(spec.tasks.iter())
        .map(|(model, task)| ExplainRow {
            model: model.name.clone(),
            task: task.name.clone(),
            reason: reason_word(model).to_string(),
            because_of: match &model.reason {
                Reason::DirectlyChanged => None,
                Reason::Downstream { because_of, .. } => Some(because_of.clone()),
            },
            via_columns: match &model.reason {
                Reason::DirectlyChanged => Vec::new(),
                Reason::Downstream { via_columns, .. } => via_columns.clone(),
            },
            unit: unit_words(model),
            depends_on: task.depends_on.clone(),
            writes: model.replace.as_ref().map(writes_words),
            widened: model.replace.as_ref().and_then(|r| r.widened.as_ref()).map(Widening::explain),
            sql_dialect: model.sql.as_ref().map(|s| s.dialect.clone()),
            sql_statements: model.sql.as_ref().map(|s| s.statements.len()),
            non_atomic: model.sql.as_ref().is_some_and(|s| !s.atomic),
        })
        .collect();

    let summary = summarize(envelope, rows.len());
    let mermaid = mermaid(&rows);
    let markdown = markdown(&summary, &rows, &mermaid);

    Explanation { summary, markdown, mermaid, rows }
}

/// One model's write, in words. Names the declaration, never a statement — the
/// same line this crate draws everywhere else about the engine.
fn writes_words(r: &Replace) -> String {
    let scope = match &r.target {
        ReplaceTarget::Whole => "the whole relation".to_string(),
        ReplaceTarget::Partitions(ps) => {
            let plural = if ps.len() == 1 { "partition" } else { "partitions" };
            match &r.partition_column {
                Some(col) => format!("{} {plural} on `{col}`", ps.len()),
                None => format!("{} {plural}", ps.len()),
            }
        }
    };
    let key = if r.unique_key.is_empty() {
        String::new()
    } else {
        let cols = r.unique_key.iter().map(|c| format!("`{c}`")).collect::<Vec<_>>().join(", ");
        format!(" on key {cols}")
    };
    format!("`{}` over {scope}{key}", r.strategy.as_str())
}

/// The headline. States a pruning percentage only when the project size is known.
fn summarize(envelope: &PlanEnvelope, selected: usize) -> String {
    let plural = if selected == 1 { "model" } else { "models" };
    match project_size(envelope) {
        // A graph that does not even cover the plan is not a project census;
        // reporting a percentage off it would invent a saving.
        Some(total) if total >= selected && total > 0 => {
            let pruned = total - selected;
            let pct = (pruned as f64 / total as f64 * 100.0).round() as u64;
            format!("{selected} of {total} {plural} rebuild — {pct}% pruned")
        }
        _ => format!("{selected} {plural} rebuild (project size unknown — pass `graph` to report pruning)"),
    }
}

/// Distinct models named anywhere in the supplied graph, plus the plan itself.
/// `None` when no graph was given: the plan alone cannot see what it skipped.
fn project_size(envelope: &PlanEnvelope) -> Option<usize> {
    let graph = envelope.graph.as_ref()?;
    let mut all: BTreeSet<&str> = BTreeSet::new();
    for (model, upstreams) in graph {
        all.insert(model.as_str());
        all.extend(upstreams.iter().map(String::as_str));
    }
    all.extend(envelope.plan.models.iter().map(|m| m.name.as_str()));
    Some(all.len())
}

fn reason_word(model: &PlanModel) -> &'static str {
    match model.reason {
        Reason::DirectlyChanged => "directly_changed",
        Reason::Downstream { .. } => "downstream",
    }
}

fn unit_words(model: &PlanModel) -> String {
    match &model.unit {
        Unit::FullModel => "full model".to_string(),
        Unit::Partitions(ps) if ps.len() == 1 => "1 partition".to_string(),
        Unit::Partitions(ps) => format!("{} partitions", ps.len()),
    }
}

/// Mermaid ids that are not usable as node names.
///
/// `end` terminates a `subgraph` block, so a bare `end["end"]` is parsed as syntax
/// and the whole diagram fails to render — not just that node. Mermaid is
/// case-sensitive here: `End` and `END` are ordinary identifiers and are left
/// alone. A model really can be called `end` (it is a legal SQL identifier when
/// quoted, and a legal dagron task name), so this is reachable, not theoretical.
const MERMAID_RESERVED_IDS: [&str; 1] = ["end"];

/// Map each task name to the id the diagram will use for it.
///
/// Almost always the task name itself. A reserved name gets `_` appended until the
/// result collides with nothing — and the collision check matters: task-name
/// sanitization turns any invalid character into `_`, so a project containing both
/// `end` and `end.` would otherwise produce two nodes called `end_`.
fn node_ids(rows: &[ExplainRow]) -> BTreeMap<&str, String> {
    let taken: BTreeSet<&str> = rows.iter().map(|r| r.task.as_str()).collect();
    rows.iter()
        .map(|row| {
            let mut id = row.task.clone();
            while MERMAID_RESERVED_IDS.contains(&id.as_str()) || (id != row.task && taken.contains(id.as_str()))
            {
                id.push('_');
            }
            (row.task.as_str(), id)
        })
        .collect()
}

/// A Mermaid `flowchart TD` of the plan.
///
/// Node ids come from [`node_ids`]: sanitized task names are already
/// `[A-Za-z0-9_-]` — the character class Mermaid accepts unquoted — so the only
/// remaining hazard is a name that collides with Mermaid's own grammar. The label
/// carries the real model name in quotes, where any character is fine except a
/// quote, which is stripped.
fn mermaid(rows: &[ExplainRow]) -> String {
    let ids = node_ids(rows);
    let id_of = |task: &str| ids.get(task).cloned().unwrap_or_else(|| task.to_string());

    let mut out = String::from("flowchart TD\n");
    for row in rows {
        out.push_str(&format!(
            "    {}[\"{}\"]\n",
            id_of(&row.task),
            row.model.replace('"', "")
        ));
    }
    for row in rows {
        for dep in &row.depends_on {
            out.push_str(&format!("    {} --> {}\n", id_of(dep), id_of(&row.task)));
        }
    }
    // Two classes, because the distinction is the whole point of the planner: one
    // set of models you edited, another set it decided you also have to rebuild.
    out.push_str("    classDef changed fill:#fde68a,stroke:#b45309,color:#000\n");
    out.push_str("    classDef downstream fill:#dbeafe,stroke:#1d4ed8,color:#000\n");
    for (class, want) in [("changed", "directly_changed"), ("downstream", "downstream")] {
        let members: Vec<String> =
            rows.iter().filter(|r| r.reason == want).map(|r| id_of(&r.task)).collect();
        if !members.is_empty() {
            out.push_str(&format!("    class {} {class}\n", members.join(",")));
        }
    }
    out
}

/// The PR comment: summary, table, graph.
fn markdown(summary: &str, rows: &[ExplainRow], mermaid: &str) -> String {
    let mut out = format!("## State plan\n\n**{summary}**\n\n");

    // Widenings first, before the table. A reviewer skimming a PR comment should
    // not have to read a 40-row table to discover that one of them is about to
    // rewrite a whole table instead of two partitions.
    let widened: Vec<&ExplainRow> = rows.iter().filter(|r| r.widened.is_some()).collect();
    if !widened.is_empty() {
        let plural = if widened.len() == 1 { "model" } else { "models" };
        out.push_str(&format!(
            "> [!WARNING]\n> **{} {plural} widened to a full refresh of the whole relation.**\n",
            widened.len()
        ));
        for row in &widened {
            out.push_str(&format!(
                "> - `{}` — {}\n",
                row.model,
                row.widened.as_deref().unwrap_or_default()
            ));
        }
        out.push('\n');
    }

    // Same reasoning, for a write the warehouse cannot do atomically: if its run
    // fails part-way, the relation is left half-written until a rerun finishes it.
    let non_atomic: Vec<&ExplainRow> = rows.iter().filter(|r| r.non_atomic).collect();
    if !non_atomic.is_empty() {
        let plural = if non_atomic.len() == 1 { "model is" } else { "models are" };
        out.push_str(&format!(
            "> [!CAUTION]\n> **{} {plural} written by statements that are not atomic** — a run \
             that fails part-way leaves the relation half-written until it is rerun.\n",
            non_atomic.len()
        ));
        for row in &non_atomic {
            out.push_str(&format!("> - `{}` ({})\n", row.model, row.sql_dialect.as_deref().unwrap_or_default()));
        }
        out.push('\n');
    }
    if let Some(dialect) = rows.iter().find_map(|r| r.sql_dialect.as_deref()) {
        let statements: usize = rows.iter().filter_map(|r| r.sql_statements).sum();
        out.push_str(&format!("Rendered as **{dialect}** SQL: {statements} statement(s) across {} model(s).\n\n", rows.len()));
    }

    let show_writes = rows.iter().any(|r| r.writes.is_some());
    if show_writes {
        out.push_str("| Model | Why | Unit | Waits on | Writes |\n|---|---|---|---|---|\n");
    } else {
        out.push_str("| Model | Why | Unit | Waits on |\n|---|---|---|---|\n");
    }
    for row in rows {
        let why = match (&row.because_of, row.via_columns.is_empty()) {
            (None, _) => "directly changed".to_string(),
            (Some(up), true) => format!("downstream of `{up}`"),
            (Some(up), false) => {
                let cols =
                    row.via_columns.iter().map(|c| format!("`{c}`")).collect::<Vec<_>>().join(", ");
                format!("downstream of `{up}` via {cols}")
            }
        };
        let waits = if row.depends_on.is_empty() {
            "—".to_string()
        } else {
            row.depends_on.iter().map(|d| format!("`{d}`")).collect::<Vec<_>>().join(", ")
        };
        if show_writes {
            let writes = match (&row.writes, &row.widened) {
                (None, _) => "—".to_string(),
                (Some(w), None) => w.clone(),
                (Some(w), Some(_)) => format!("⚠️ {w}"),
            };
            out.push_str(&format!(
                "| `{}` | {} | {} | {} | {} |\n",
                row.model, why, row.unit, waits, writes
            ));
        } else {
            out.push_str(&format!("| `{}` | {} | {} | {} |\n", row.model, why, row.unit, waits));
        }
    }
    out.push_str(&format!("\n```mermaid\n{mermaid}```\n"));
    out
}
