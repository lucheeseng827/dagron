//! `dagron plan` — diff two workflow specs the way the engine sees them.
//!
//! Both sides are resolved through the exact parse → template-expansion →
//! validation pipeline every submit path uses ([`DagGraph::from_yaml`]), so the
//! plan reflects **what would actually run**, not a textual YAML diff: template
//! calls and `with_items` fan-outs are flattened to concrete leaf tasks before
//! comparison. The output is a PR-friendly markdown summary plus a Mermaid graph
//! of the resulting DAG with added/changed tasks flagged.

use std::collections::{BTreeMap, BTreeSet};

use anyhow::Result;
use dagron_core::dag::{DagGraph, TaskSpec};

/// A normalized, comparable view of a leaf task (post-expansion) — the execution
/// fields a reviewer cares about when a workflow changes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskView {
    pub command: Vec<String>,
    /// Sorted for order-independent comparison.
    pub depends_on: Vec<String>,
    pub image: Option<String>,
    pub env: BTreeMap<String, String>,
    /// `value_from` secret refs (env name → secret name), so a change in which
    /// secret an env pulls from is visible even though the literal value is empty.
    pub env_secret: BTreeMap<String, String>,
    pub max_attempts: u32,
    pub retry_delay_secs: u64,
    pub retry_max_delay_secs: Option<u64>,
    pub timeout_secs: Option<u64>,
    /// Semantics a reviewer must see: how the task runs relative to its deps and
    /// whether it's a hook / optional / an approval gate.
    pub trigger_rule: Option<String>,
    pub hook: Option<String>,
    pub allow_failure: bool,
    pub task_type: Option<String>,
    pub approval_timeout_secs: Option<u64>,
    pub approval_on_timeout: Option<String>,
}

impl TaskView {
    fn from_spec(t: &TaskSpec) -> Self {
        let mut depends_on = t.depends_on.clone();
        depends_on.sort();
        TaskView {
            command: t.command.clone(),
            depends_on,
            image: t.docker_image.clone(),
            env: t
                .env
                .iter()
                .map(|e| (e.name.clone(), e.value.clone()))
                .collect(),
            env_secret: t
                .env
                .iter()
                .filter_map(|e| e.value_from.as_ref().map(|s| (e.name.clone(), s.secret.clone())))
                .collect(),
            max_attempts: t.max_attempts,
            retry_delay_secs: t.retry_delay_secs,
            retry_max_delay_secs: t.retry_max_delay_secs,
            timeout_secs: t.timeout_secs,
            trigger_rule: t.trigger_rule.clone(),
            hook: t.hook.clone(),
            allow_failure: t.allow_failure,
            task_type: t.task_type.clone(),
            approval_timeout_secs: t.approval_timeout_secs,
            approval_on_timeout: t.approval_on_timeout.clone(),
        }
    }

    /// Human-readable field-level differences from `self` (base) to `head`.
    fn diff_fields(&self, head: &TaskView) -> Vec<String> {
        let mut out = Vec::new();
        if self.command != head.command {
            out.push(format!(
                "command: {} → {}",
                fmt_cmd(&self.command),
                fmt_cmd(&head.command)
            ));
        }
        if self.depends_on != head.depends_on {
            out.push(format!(
                "depends_on: [{}] → [{}]",
                self.depends_on.join(", "),
                head.depends_on.join(", ")
            ));
        }
        if self.image != head.image {
            out.push(format!(
                "image: {} → {}",
                opt(&self.image),
                opt(&head.image)
            ));
        }
        if self.max_attempts != head.max_attempts {
            out.push(format!(
                "max_attempts: {} → {}",
                self.max_attempts, head.max_attempts
            ));
        }
        if self.retry_delay_secs != head.retry_delay_secs {
            out.push(format!(
                "retry_delay_secs: {} → {}",
                self.retry_delay_secs, head.retry_delay_secs
            ));
        }
        if self.retry_max_delay_secs != head.retry_max_delay_secs {
            out.push(format!(
                "retry_max_delay_secs: {} → {}",
                opt_num(self.retry_max_delay_secs),
                opt_num(head.retry_max_delay_secs)
            ));
        }
        if self.timeout_secs != head.timeout_secs {
            out.push(format!(
                "timeout_secs: {} → {}",
                opt_num(self.timeout_secs),
                opt_num(head.timeout_secs)
            ));
        }
        if self.trigger_rule != head.trigger_rule {
            out.push(format!(
                "trigger_rule: {} → {}",
                opt(&self.trigger_rule),
                opt(&head.trigger_rule)
            ));
        }
        if self.hook != head.hook {
            out.push(format!("hook: {} → {}", opt(&self.hook), opt(&head.hook)));
        }
        if self.allow_failure != head.allow_failure {
            out.push(format!(
                "allow_failure: {} → {}",
                self.allow_failure, head.allow_failure
            ));
        }
        if self.task_type != head.task_type {
            out.push(format!(
                "type: {} → {}",
                opt(&self.task_type),
                opt(&head.task_type)
            ));
        }
        if self.approval_timeout_secs != head.approval_timeout_secs {
            out.push(format!(
                "approval_timeout_secs: {} → {}",
                opt_num(self.approval_timeout_secs),
                opt_num(head.approval_timeout_secs)
            ));
        }
        if self.approval_on_timeout != head.approval_on_timeout {
            out.push(format!(
                "approval_on_timeout: {} → {}",
                opt(&self.approval_on_timeout),
                opt(&head.approval_on_timeout)
            ));
        }
        // Per-key env diff (added / removed / changed), so a one-var change reads
        // as one line rather than "env changed".
        let keys: BTreeSet<&String> = self.env.keys().chain(head.env.keys()).collect();
        for k in keys {
            match (self.env.get(k), head.env.get(k)) {
                (Some(a), Some(b)) if a != b => out.push(format!("env.{k}: \"{a}\" → \"{b}\"")),
                (Some(a), None) => out.push(format!("env.{k}: \"{a}\" → (removed)")),
                (None, Some(b)) => out.push(format!("env.{k}: (added) → \"{b}\"")),
                _ => {}
            }
        }
        // `value_from` secret refs (which secret each env pulls from).
        let sk: BTreeSet<&String> = self.env_secret.keys().chain(head.env_secret.keys()).collect();
        for k in sk {
            match (self.env_secret.get(k), head.env_secret.get(k)) {
                (Some(a), Some(b)) if a != b => {
                    out.push(format!("env.{k}.value_from: {a} → {b}"))
                }
                (Some(a), None) => out.push(format!("env.{k}.value_from: {a} → (removed)")),
                (None, Some(b)) => out.push(format!("env.{k}.value_from: (added) → {b}")),
                _ => {}
            }
        }
        out
    }
}

/// The computed difference between two resolved DAGs.
pub struct Plan {
    pub base_name: String,
    pub head_name: String,
    pub added: Vec<String>,
    pub removed: Vec<String>,
    /// `(task_name, field-level change lines)`.
    pub changed: Vec<(String, Vec<String>)>,
    /// A run-level `run_timeout_secs` change, if any: `(before, after)`.
    pub run_timeout: Option<(Option<u64>, Option<u64>)>,
    /// Other root-level workflow-field changes: `(field label, before, after)` for
    /// `deadline`, `notify`, and `result_from`.
    pub root_changes: Vec<(&'static str, Option<String>, Option<String>)>,
    // Head DAG shape, for the Mermaid rendering.
    head_order: Vec<String>,
    head_tasks: BTreeMap<String, TaskView>,
}

impl Plan {
    /// True when base and head differ in any compared dimension.
    pub fn has_changes(&self) -> bool {
        !self.added.is_empty()
            || !self.removed.is_empty()
            || !self.changed.is_empty()
            || self.run_timeout.is_some()
            || !self.root_changes.is_empty()
    }

    /// A GitHub-flavored markdown report: a summary line, per-category sections,
    /// and a Mermaid graph of the head DAG with added/changed tasks flagged.
    pub fn to_markdown(&self) -> String {
        let mut s = String::new();
        s.push_str("## dagron plan\n\n");
        if !self.has_changes() {
            s.push_str(&format!(
                "No changes. `{}` resolves identically.\n",
                self.head_name
            ));
            return s;
        }
        s.push_str(&format!(
            "Workflow **{}**: {} added, {} removed, {} changed",
            self.head_name,
            self.added.len(),
            self.removed.len(),
            self.changed.len()
        ));
        if self.run_timeout.is_some() {
            s.push_str(", run timeout changed");
        }
        if !self.root_changes.is_empty() {
            s.push_str(", workflow settings changed");
        }
        s.push_str(" (tasks shown are post-expansion leaf tasks).\n");

        if let Some((before, after)) = &self.run_timeout {
            s.push_str(&format!(
                "\n**Run timeout:** {} → {}\n",
                opt_num(*before),
                opt_num(*after)
            ));
        }
        for (label, before, after) in &self.root_changes {
            s.push_str(&format!(
                "\n**{label}:** {} → {}\n",
                opt(before),
                opt(after)
            ));
        }
        if !self.added.is_empty() {
            s.push_str("\n### Added tasks\n");
            for name in &self.added {
                let v = &self.head_tasks[name];
                s.push_str(&format!("- `{name}` — {}\n", fmt_cmd(&v.command)));
            }
        }
        if !self.removed.is_empty() {
            s.push_str("\n### Removed tasks\n");
            for name in &self.removed {
                s.push_str(&format!("- `{name}`\n"));
            }
        }
        if !self.changed.is_empty() {
            s.push_str("\n### Changed tasks\n");
            for (name, fields) in &self.changed {
                s.push_str(&format!("- `{name}`\n"));
                for f in fields {
                    s.push_str(&format!("  - {f}\n"));
                }
            }
        }
        s.push_str("\n### Resulting DAG\n\n");
        s.push_str(&self.to_mermaid());
        s.push('\n');
        s
    }

    /// A Mermaid `flowchart` of the head DAG. Added tasks get the `added` class,
    /// changed tasks the `changed` class, so the graph reads as a review artifact.
    pub fn to_mermaid(&self) -> String {
        let added: BTreeSet<&String> = self.added.iter().collect();
        let changed: BTreeSet<&String> = self.changed.iter().map(|(n, _)| n).collect();
        // Stable short ids (Mermaid ids can't hold dots/brackets from task names).
        let id: BTreeMap<&String, String> = self
            .head_order
            .iter()
            .enumerate()
            .map(|(i, n)| (n, format!("n{i}")))
            .collect();

        let mut s = String::from("```mermaid\nflowchart TD\n");
        for name in &self.head_order {
            let label = name.replace('"', "'");
            s.push_str(&format!("  {}[\"{}\"]\n", id[name], label));
        }
        for name in &self.head_order {
            for dep in &self.head_tasks[name].depends_on {
                if let Some(dep_id) = id.get(dep) {
                    s.push_str(&format!("  {} --> {}\n", dep_id, id[name]));
                }
            }
        }
        let added_ids: Vec<&str> = self
            .head_order
            .iter()
            .filter(|n| added.contains(n))
            .map(|n| id[n].as_str())
            .collect();
        let changed_ids: Vec<&str> = self
            .head_order
            .iter()
            .filter(|n| changed.contains(n))
            .map(|n| id[n].as_str())
            .collect();
        s.push_str("  classDef added fill:#e6ffed,stroke:#2da44e;\n");
        s.push_str("  classDef changed fill:#fff8c5,stroke:#bf8700;\n");
        if !added_ids.is_empty() {
            s.push_str(&format!("  class {} added;\n", added_ids.join(",")));
        }
        if !changed_ids.is_empty() {
            s.push_str(&format!("  class {} changed;\n", changed_ids.join(",")));
        }
        s.push_str("```\n");
        s
    }
}

/// A spec resolved through parse → expand → validate: the root-level fields a
/// reviewer cares about plus the leaf-task views.
struct Resolved {
    name: String,
    run_timeout_secs: Option<u64>,
    /// Compact string summaries for change detection.
    deadline: Option<String>,
    notify: Option<String>,
    result_from: Option<String>,
    views: BTreeMap<String, TaskView>,
    order: Vec<String>,
}

fn resolve(yaml: &str) -> Result<Resolved> {
    let graph = DagGraph::from_yaml(yaml)?;
    let spec = graph.spec;
    let order: Vec<String> = spec.tasks.iter().map(|t| t.name.clone()).collect();
    let views = spec
        .tasks
        .iter()
        .map(|t| (t.name.clone(), TaskView::from_spec(t)))
        .collect();
    Ok(Resolved {
        name: spec.name,
        run_timeout_secs: spec.run_timeout_secs,
        // `within` is the human duration string; notify is compared via Debug so
        // any nested field (provider/repo/sha/context/url) change is caught.
        deadline: spec.deadline.as_ref().map(|d| d.within.clone()),
        notify: spec.notify.as_ref().map(|n| format!("{n:?}")),
        result_from: spec.result_from.clone(),
        views,
        order,
    })
}

/// Compute the [`Plan`] between two spec YAMLs. Either side failing to parse /
/// expand / validate is an error (a plan on an invalid DAG is meaningless).
pub fn plan(base_yaml: &str, head_yaml: &str) -> Result<Plan> {
    let base = resolve(base_yaml)?;
    let head = resolve(head_yaml)?;

    let mut added = Vec::new();
    let mut changed = Vec::new();
    for name in &head.order {
        match base.views.get(name) {
            None => added.push(name.clone()),
            Some(b) => {
                let fields = b.diff_fields(&head.views[name]);
                if !fields.is_empty() {
                    changed.push((name.clone(), fields));
                }
            }
        }
    }
    let mut removed: Vec<String> = base
        .views
        .keys()
        .filter(|n| !head.views.contains_key(*n))
        .cloned()
        .collect();
    removed.sort();

    let run_timeout = (base.run_timeout_secs != head.run_timeout_secs)
        .then_some((base.run_timeout_secs, head.run_timeout_secs));

    // Other root-level workflow fields, one entry per changed field.
    let mut root_changes: Vec<(&'static str, Option<String>, Option<String>)> = Vec::new();
    if base.deadline != head.deadline {
        root_changes.push(("deadline", base.deadline, head.deadline));
    }
    if base.notify != head.notify {
        root_changes.push(("notify", base.notify, head.notify));
    }
    if base.result_from != head.result_from {
        root_changes.push(("result_from", base.result_from, head.result_from));
    }

    Ok(Plan {
        base_name: base.name,
        head_name: head.name,
        added,
        removed,
        changed,
        run_timeout,
        root_changes,
        head_order: head.order,
        head_tasks: head.views,
    })
}

fn fmt_cmd(cmd: &[String]) -> String {
    if cmd.is_empty() {
        "(none)".to_string()
    } else {
        format!("`{}`", cmd.join(" "))
    }
}

fn opt(v: &Option<String>) -> String {
    v.clone().unwrap_or_else(|| "(none)".to_string())
}

fn opt_num(v: Option<u64>) -> String {
    v.map(|n| n.to_string())
        .unwrap_or_else(|| "(none)".to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    const BASE: &str = r#"
name: etl
tasks:
  - { name: extract, command: ["fetch"] }
  - { name: transform, command: ["run"], depends_on: [extract], timeout_secs: 60 }
"#;

    #[test]
    fn no_change_is_empty_plan() {
        let p = plan(BASE, BASE).unwrap();
        assert!(!p.has_changes());
        assert!(p.to_markdown().contains("No changes"));
    }

    #[test]
    fn detects_add_remove_change() {
        let head = r#"
name: etl
tasks:
  - { name: extract, command: ["fetch", "--v2"] }
  - { name: load, command: ["ship"], depends_on: [extract] }
"#;
        let p = plan(BASE, head).unwrap();
        assert!(p.has_changes());
        assert_eq!(p.added, vec!["load"]);
        assert_eq!(p.removed, vec!["transform"]);
        assert_eq!(p.changed.len(), 1);
        assert_eq!(p.changed[0].0, "extract");
        assert!(p.changed[0].1.iter().any(|f| f.contains("command")));
        let md = p.to_markdown();
        assert!(md.contains("1 added, 1 removed, 1 changed"));
        assert!(md.contains("```mermaid"));
    }

    #[test]
    fn diffs_the_resolved_dag_not_the_text() {
        // Same fan-out expressed two ways resolves to the same leaf tasks, so the
        // plan is empty even though the YAML text differs.
        let a = r#"
name: w
tasks:
  - name: s
    command: ["echo", "{{ item }}"]
    with_items: ["a", "b"]
"#;
        let b = r#"
name: w
parameters: { xs: "[\"a\", \"b\"]" }
tasks:
  - name: s
    command: ["echo", "{{ item }}"]
    with_param: "{{ xs }}"
"#;
        let p = plan(a, b).unwrap();
        assert!(
            !p.has_changes(),
            "resolved DAGs are identical: {:?}",
            p.changed
        );
    }

    #[test]
    fn run_timeout_change_detected() {
        let head = BASE.replace("name: etl", "name: etl\nrun_timeout_secs: 300");
        let p = plan(BASE, &head).unwrap();
        assert_eq!(p.run_timeout, Some((None, Some(300))));
        assert!(p.to_markdown().contains("Run timeout"));
    }

    #[test]
    fn task_semantics_and_root_fields_are_diffed() {
        // A trigger_rule flip on a task is now a visible change (was invisible).
        let head = BASE.replace(
            r#"depends_on: [extract], timeout_secs: 60 }"#,
            r#"depends_on: [extract], timeout_secs: 60, trigger_rule: all_done }"#,
        );
        let p = plan(BASE, &head).unwrap();
        assert_eq!(p.changed.len(), 1, "trigger_rule change detected");
        assert!(p.changed[0].1.iter().any(|f| f.contains("trigger_rule")));

        // Root-level result_from / deadline changes surface too.
        let head2 = BASE.replace(
            "name: etl",
            "name: etl\nresult_from: transform\ndeadline: { in: 45m }",
        );
        let p2 = plan(BASE, &head2).unwrap();
        assert!(p2.has_changes());
        let labels: Vec<_> = p2.root_changes.iter().map(|(l, ..)| *l).collect();
        assert!(labels.contains(&"result_from"), "got {labels:?}");
        assert!(labels.contains(&"deadline"), "got {labels:?}");
        let md = p2.to_markdown();
        assert!(md.contains("result_from") && md.contains("deadline"));
    }

    #[test]
    fn invalid_spec_is_an_error() {
        let bad = "name: x\ntasks:\n  - { name: a, command: [\"true\"], depends_on: [ghost] }\n";
        assert!(plan(BASE, bad).is_err());
    }
}
