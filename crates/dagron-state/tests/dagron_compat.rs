//! Proof that this crate's hand-kept spec subset still agrees with dagron's real one.
//!
//! `spec.rs` mirrors a slice of `dagron_core::dag::DagSpec` rather than importing
//! it, because a build-time `dagron-core` dependency would drag that crate's
//! "exactly one DB backend" constraint into `dagron-api` (see `Cargo.toml`). The
//! cost of a hand-kept subset is drift; this test is what makes that cost payable.
//! `dagron-core` is a DEV-dependency, so it never enters a dependent's build graph.
//!
//! It parses generated YAML through `DagGraph::from_yaml` — the same parse →
//! expand → validate path `POST /api/runs` uses, so a spec that passes here is one
//! dagron will actually accept.

use std::collections::BTreeMap;

use dagron_core::dag::DagGraph;
use dagron_state::compile::{compile, CompileOptions, Ordering, PlanEnvelope};
use dagron_state::wire::{PlanModel, PlanResponse, Reason, Unit};

fn plan_of(models: Vec<PlanModel>) -> PlanResponse {
    PlanResponse { models, ..Default::default() }
}

fn direct(name: &str) -> PlanModel {
    PlanModel {
        name: name.into(),
        reason: Reason::DirectlyChanged,
        unit: Unit::FullModel,
        depends_on: None,
        replace: None,
    }
}

fn compile_yaml(env: &PlanEnvelope) -> String {
    compile(env).expect("plan must compile").to_yaml().expect("spec must render")
}

fn options() -> CompileOptions {
    CompileOptions {
        command_template: vec!["sh".into(), "-c".into(), "dbt run --select {{ model }}".into()],
        max_attempts: Some(3),
        timeout_secs: Some(600),
        ..Default::default()
    }
}

#[test]
fn a_compiled_plan_is_a_valid_dagron_workflow() {
    let env = PlanEnvelope {
        plan: plan_of(vec![
            direct("stg_orders"),
            PlanModel {
                name: "mart_revenue".into(),
                reason: Reason::Downstream {
                    because_of: "stg_orders".into(),
                    via_columns: vec!["amount".into()],
                },
                unit: Unit::Partitions(vec!["2026-09-01".into()]),
                depends_on: None,
                replace: None,
            },
        ]),
        graph: None,
        options: options(),
    };

    let yaml = compile_yaml(&env);
    let graph = DagGraph::from_yaml(&yaml)
        .unwrap_or_else(|e| panic!("dagron rejected a compiled plan: {e}\n---\n{yaml}"));

    // The edge survived the round trip as a real graph edge, not just as text.
    let mart = graph.task_spec("mart_revenue").expect("task must exist");
    assert_eq!(mart.depends_on, vec!["stg_orders"]);
    assert_eq!(mart.command, vec!["sh", "-c", "dbt run --select mart_revenue"]);
}

#[test]
fn sanitized_and_deduplicated_names_survive_dagron_validation() {
    // Duplicate task names are a parse-time rejection in dagron, so the collision
    // suffixing in `unique_task_name` has to hold up against the real validator.
    let env = PlanEnvelope {
        plan: plan_of(vec![
            direct("analytics.stg_orders"),
            direct("analytics-stg_orders"),
            direct("analytics/stg_orders"),
        ]),
        graph: None,
        options: options(),
    };

    let yaml = compile_yaml(&env);
    let graph = DagGraph::from_yaml(&yaml)
        .unwrap_or_else(|e| panic!("dagron rejected sanitized names: {e}\n---\n{yaml}"));

    // `.` and `/` collide into one sanitized name (so the second is suffixed);
    // `-` is already a legal dagron task character and stands on its own.
    for name in ["analytics_stg_orders", "analytics-stg_orders", "analytics_stg_orders_2"] {
        assert!(graph.task_spec(name).is_some(), "{name} must be a task");
    }
}

#[test]
fn a_sequential_plan_is_a_valid_chain() {
    let mut options = options();
    options.ordering = Ordering::Sequential;
    let env = PlanEnvelope {
        plan: plan_of(vec![direct("a"), direct("b"), direct("c")]),
        graph: None,
        options,
    };

    let yaml = compile_yaml(&env);
    let graph = DagGraph::from_yaml(&yaml)
        .unwrap_or_else(|e| panic!("dagron rejected a sequential plan: {e}\n---\n{yaml}"));
    assert_eq!(graph.task_spec("c").unwrap().depends_on, vec!["b"]);
}

#[test]
fn a_multi_parent_plan_from_an_explicit_graph_validates() {
    let mut graph_edges = BTreeMap::new();
    graph_edges.insert("mart".to_string(), vec!["a".to_string(), "b".to_string()]);

    let env = PlanEnvelope {
        plan: plan_of(vec![
            direct("a"),
            direct("b"),
            PlanModel {
                name: "mart".into(),
                reason: Reason::Downstream {
                    because_of: "a".into(),
                    via_columns: vec!["x".into()],
                },
                unit: Unit::FullModel,
                depends_on: None,
                replace: None,
            },
        ]),
        graph: Some(graph_edges),
        options: options(),
    };

    let yaml = compile_yaml(&env);
    let graph = DagGraph::from_yaml(&yaml)
        .unwrap_or_else(|e| panic!("dagron rejected a fan-in plan: {e}\n---\n{yaml}"));
    assert_eq!(graph.task_spec("mart").unwrap().depends_on, vec!["a", "b"]);
}
