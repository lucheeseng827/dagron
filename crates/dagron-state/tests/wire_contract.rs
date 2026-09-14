//! The golden guard on the duplicated wire contract.
//!
//! The planner's `planner-embed` owns the frozen contract and guards its own half
//! with an exhaustive, no-wildcard match — adding a `BackfillReason` variant fails
//! to compile there until the contract is updated. This crate has no compiler link
//! to that, so this fixture is the guard on the dagron half: it is a byte-for-byte
//! sample of what `planner-embed::PlanResponse` actually serializes, and it must
//! keep deserializing here.
//!
//! If this test fails, the contract moved. Fix `wire.rs` and bump
//! `WIRE_CONTRACT_VERSION` — do not edit the fixture to match the code.

use dagron_state::compile::{compile, CompileOptions, PlanEnvelope};
use dagron_state::wire::{
    PlanResponse, Reason, ReplaceStrategy, ReplaceTarget, Unit, Widening, WIRE_CONTRACT_VERSION,
};

/// Exactly the shape `planner-embed` emits: externally-tagged, snake_case enums,
/// `next_state` present, `error` omitted.
const GOLDEN: &str = r#"{
  "models": [
    { "name": "stg_orders", "reason": "directly_changed", "unit": "full_model" },
    {
      "name": "mart_revenue",
      "reason": { "downstream": { "because_of": "stg_orders", "via_columns": ["amount", "qty"] } },
      "unit": { "partitions": ["2026-09-01", "2026-09-02"] }
    }
  ],
  "next_state": { "fingerprints": { "stg_orders": 12345, "mart_revenue": 67890 } }
}"#;

/// Captured verbatim from the real planner after seeding state and editing both
/// roots, so `mart_revenue` arrives as a genuine downstream fan-in:
///
/// ```sh
/// freshet plan --project ./models --state ./state.json --commit   # seed
/// # edit stg_orders.sql AND dim_customer.sql
/// freshet plan --project ./models --state ./state.json --json
/// ```
///
/// This is the whole case for contract v2 in one payload: the attribution blames
/// `dim_customer` alone, while the model genuinely waits for `stg_orders` too.
const CAPTURED_V2_FAN_IN: &str = r#"{
  "models": [
    { "name": "dim_customer", "reason": "directly_changed", "unit": "full_model", "depends_on": [] },
    { "name": "stg_orders",   "reason": "directly_changed", "unit": "full_model", "depends_on": [] },
    {
      "name": "mart_revenue",
      "reason": { "downstream": { "because_of": "dim_customer", "via_columns": ["region"] } },
      "unit": "full_model",
      "depends_on": ["dim_customer", "stg_orders"]
    }
  ]
}"#;

/// Captured verbatim from the real planner, on a project that declares replace
/// strategies in `incremental.json`:
///
/// ```sh
/// freshet restate --project ./models --model events --partitions 2026-06-20 --json
/// ```
///
/// One payload holding all three v3 cases: a partition-wise strategy honoured,
/// a `merge` honoured on a key with no partition column, and a widening — the
/// `user_dim` table cannot be replaced partition-wise, so the planner escalated
/// to a full refresh and said why.
const CAPTURED_V3_WITH_A_WIDENING: &str = r#"{
  "models": [
    {
      "name": "events",
      "reason": "directly_changed",
      "unit": { "partitions": ["2026-06-20"] },
      "depends_on": [],
      "replace": {
        "strategy": "insert_overwrite",
        "partition_column": "dt",
        "target": { "partitions": ["2026-06-20"] }
      }
    },
    {
      "name": "daily_rollup",
      "reason": { "downstream": { "because_of": "events", "via_columns": ["dt"] } },
      "unit": { "partitions": ["2026-06-20"] },
      "depends_on": ["events"],
      "replace": {
        "strategy": "merge",
        "unique_key": ["dt"],
        "target": { "partitions": ["2026-06-20"] }
      }
    },
    {
      "name": "user_dim",
      "reason": { "downstream": { "because_of": "events", "via_columns": ["last_seen", "user_id"] } },
      "unit": "full_model",
      "depends_on": ["events"],
      "replace": {
        "strategy": "full_refresh",
        "target": "whole",
        "widened": { "widened_because": "not_incremental", "materialization": "table" }
      }
    }
  ]
}"#;

#[test]
fn a_v3_plan_carries_what_each_rebuild_does_to_the_relation() {
    let plan: PlanResponse =
        serde_json::from_str(CAPTURED_V3_WITH_A_WIDENING).expect("real v3 output must parse");

    let events = plan.models.iter().find(|m| m.name == "events").unwrap();
    let r = events.replace.as_ref().expect("a declared model carries its op");
    assert_eq!(r.strategy, ReplaceStrategy::InsertOverwrite);
    assert_eq!(r.partition_column.as_deref(), Some("dt"));
    assert_eq!(r.target, ReplaceTarget::Partitions(vec!["2026-06-20".into()]));
    assert!(r.widened.is_none());

    // merge scopes on a key, so it needs no partition column and is not widened.
    let rollup = plan.models.iter().find(|m| m.name == "daily_rollup").unwrap();
    let r = rollup.replace.as_ref().unwrap();
    assert_eq!(r.strategy, ReplaceStrategy::Merge);
    assert_eq!(r.unique_key, ["dt"]);
    assert!(r.partition_column.is_none());
    assert!(r.widened.is_none());

    let dim = plan.models.iter().find(|m| m.name == "user_dim").unwrap();
    let r = dim.replace.as_ref().unwrap();
    assert_eq!(r.strategy, ReplaceStrategy::FullRefresh);
    assert_eq!(r.target, ReplaceTarget::Whole);
    match r.widened.as_ref().expect("a widened op must say why") {
        Widening::NotIncremental { materialization } => assert_eq!(materialization, "table"),
        other => panic!("expected not_incremental, got {other:?}"),
    }
}

#[test]
fn a_widening_explains_itself_in_one_actionable_sentence() {
    let plan: PlanResponse = serde_json::from_str(CAPTURED_V3_WITH_A_WIDENING).unwrap();
    let dim = plan.models.iter().find(|m| m.name == "user_dim").unwrap();
    let said = dim.replace.as_ref().unwrap().widened.as_ref().unwrap().explain();
    assert!(said.contains("table"), "it should name the materialization: {said}");
    assert!(
        said.contains("no partitions to replace in place"),
        "and say what that means for the write: {said}"
    );
}

#[test]
fn a_v2_plan_reports_no_replace_and_that_is_not_an_error() {
    // The forward-compatibility direction that matters: a producer that predates
    // v3 keeps working, and its silence reads as "no replace-specific behaviour"
    // rather than as a parse failure or a guessed default.
    let plan: PlanResponse = serde_json::from_str(CAPTURED_V2_FAN_IN).unwrap();
    assert!(plan.models.iter().all(|m| m.replace.is_none()));
    assert!(
        plan.models.iter().all(|m| m.depends_on.is_some()),
        "while its v2 half still parses as v2"
    );
}

#[test]
fn a_v2_plan_carries_edges_the_attribution_cannot_express() {
    let plan: PlanResponse =
        serde_json::from_str(CAPTURED_V2_FAN_IN).expect("real v2 planner output must parse");

    let mart = plan.models.iter().find(|m| m.name == "mart_revenue").unwrap();
    match &mart.reason {
        Reason::Downstream { because_of, .. } => assert_eq!(
            because_of, "dim_customer",
            "the attribution names ONE parent"
        ),
        other => panic!("expected downstream, got {other:?}"),
    }
    assert_eq!(
        mart.depends_on.as_deref(),
        Some(["dim_customer".to_string(), "stg_orders".to_string()].as_slice()),
        "but the model waits for BOTH — this is the gap v2 closes"
    );

    // Roots say so authoritatively, rather than being indistinguishable from v1.
    let stg = plan.models.iter().find(|m| m.name == "stg_orders").unwrap();
    assert_eq!(stg.depends_on.as_deref(), Some([].as_slice()));
}

#[test]
fn a_v2_fan_in_compiles_to_both_edges_without_a_caller_supplied_graph() {
    // The end-to-end payoff: no `graph`, no `sequential`, and the run graph is
    // still exact. Under v1 this same plan produced a single edge.
    let plan: PlanResponse = serde_json::from_str(CAPTURED_V2_FAN_IN).unwrap();
    let spec = compile(&PlanEnvelope {
        plan,
        graph: None,
        options: CompileOptions {
            command_template: vec!["sh".into(), "-c".into(), "dbt run -s {{ model }}".into()],
            ..Default::default()
        },
    })
    .expect("must compile");

    let mart = spec.tasks.iter().find(|t| t.name == "mart_revenue").unwrap();
    assert_eq!(mart.depends_on, vec!["dim_customer", "stg_orders"]);
}

#[test]
fn a_v1_plan_still_parses_and_reports_no_edges() {
    // Backward compatibility is the reason `depends_on` is an Option: a v1 payload
    // must not look like a v2 plan whose models happen to wait for nothing.
    let plan: PlanResponse = serde_json::from_str(GOLDEN).expect("v1 payload must still parse");
    for m in &plan.models {
        assert!(m.depends_on.is_none(), "{} must report absent, not empty", m.name);
    }
}

#[test]
fn the_golden_plan_deserializes() {
    let plan: PlanResponse = serde_json::from_str(GOLDEN).expect("golden plan must parse");

    assert_eq!(plan.models.len(), 2);
    assert!(plan.error.is_none());
    assert!(plan.next_state.is_some(), "the snapshot is carried through opaquely");

    assert!(matches!(plan.models[0].reason, Reason::DirectlyChanged));
    assert!(matches!(plan.models[0].unit, Unit::FullModel));

    match &plan.models[1].reason {
        Reason::Downstream { because_of, via_columns } => {
            assert_eq!(because_of, "stg_orders");
            assert_eq!(via_columns, &["amount", "qty"]);
        }
        other => panic!("expected downstream, got {other:?}"),
    }
    match &plan.models[1].unit {
        Unit::Partitions(ps) => assert_eq!(ps, &["2026-09-01", "2026-09-02"]),
        other => panic!("expected partitions, got {other:?}"),
    }
}

#[test]
fn a_cold_plan_without_state_or_error_parses() {
    // Both optional fields are `skip_serializing_if` upstream, so a minimal plan
    // omits them entirely rather than sending nulls.
    let plan: PlanResponse =
        serde_json::from_str(r#"{"models":[{"name":"a","reason":"directly_changed","unit":"full_model"}]}"#)
            .expect("a plan with no state and no error must parse");
    assert_eq!(plan.models.len(), 1);
    assert!(plan.next_state.is_none());
}

#[test]
fn an_empty_plan_parses_as_success_not_failure() {
    let plan: PlanResponse = serde_json::from_str(r#"{"models":[]}"#).unwrap();
    assert!(plan.models.is_empty());
    assert!(plan.error.is_none());
}

#[test]
fn the_golden_plan_compiles_end_to_end() {
    let plan: PlanResponse = serde_json::from_str(GOLDEN).unwrap();
    let envelope = PlanEnvelope {
        plan,
        graph: None,
        options: CompileOptions {
            command_template: vec!["sh".into(), "-c".into(), "dbt run -s {{ model }}".into()],
            ..Default::default()
        },
    };

    let spec = compile(&envelope).unwrap();
    assert_eq!(spec.tasks.len(), 2);
    assert_eq!(spec.tasks[1].depends_on, vec!["stg_orders"]);
    assert!(spec.to_yaml().unwrap().contains("mart_revenue"));
}

#[test]
fn the_contract_version_is_stamped() {
    assert_eq!(WIRE_CONTRACT_VERSION, "planner-embed/3");
}

/// Captured verbatim from the real planner, not hand-written:
///
/// ```sh
/// planner plan --project ./models --state ./state.json --commit   # seed state
/// # edit stg_orders.sql
/// planner plan --project ./models --state ./state.json --json
/// ```
///
/// A hand-written fixture only proves this crate agrees with itself. This one
/// proves it agrees with the planner's `planner-cli` as actually built.
const CAPTURED_FROM_PLANNER_CLI: &str = r#"{
  "models": [
    {
      "name": "stg_orders",
      "reason": "directly_changed",
      "unit": "full_model"
    },
    {
      "name": "mart_revenue",
      "reason": {
        "downstream": {
          "because_of": "stg_orders",
          "via_columns": [
            "amount",
            "order_id"
          ]
        }
      },
      "unit": "full_model"
    }
  ]
}"#;

#[test]
fn real_planner_output_compiles_into_a_run_graph() {
    let plan: PlanResponse =
        serde_json::from_str(CAPTURED_FROM_PLANNER_CLI).expect("real planner output must parse");

    let spec = compile(&PlanEnvelope {
        plan,
        graph: None,
        options: CompileOptions {
            command_template: vec!["sh".into(), "-c".into(), "dbt run -s {{ model }}".into()],
            ..Default::default()
        },
    })
    .expect("real planner output must compile");

    assert_eq!(spec.tasks.len(), 2);
    assert_eq!(spec.tasks[0].name, "stg_orders");
    // The planner's column-level attribution became a real dependency edge.
    assert_eq!(spec.tasks[1].depends_on, vec!["stg_orders"]);
    assert_eq!(spec.tasks[1].input.as_ref().unwrap()["via_columns"][0], "amount");
}
