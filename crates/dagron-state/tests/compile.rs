//! Compilation behaviour: edges, ordering, naming, rendering, refusals.

use std::collections::BTreeMap;

use dagron_state::compile::{compile, CompileError, CompileOptions, Ordering, PlanEnvelope};
use dagron_state::wire::{Replace, ReplaceStrategy, ReplaceTarget, Widening, PlanError, PlanModel, PlanResponse, Reason, Unit};

fn cmd() -> Vec<String> {
    vec!["sh".into(), "-c".into(), "build {{ model }}".into()]
}

fn opts() -> CompileOptions {
    CompileOptions { command_template: cmd(), ..Default::default() }
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

fn downstream(name: &str, because_of: &str) -> PlanModel {
    PlanModel {
        name: name.into(),
        reason: Reason::Downstream {
            because_of: because_of.into(),
            via_columns: vec!["amount".into()],
        },
        unit: Unit::FullModel,
        depends_on: None,
        replace: None,
    }
}

fn envelope(models: Vec<PlanModel>) -> PlanEnvelope {
    PlanEnvelope {
        plan: PlanResponse { models, ..Default::default() },
        graph: None,
        options: opts(),
    }
}

#[test]
fn attribution_becomes_a_dependency_edge() {
    let spec = compile(&envelope(vec![direct("stg"), downstream("mart", "stg")])).unwrap();
    assert_eq!(spec.tasks[0].depends_on, Vec::<String>::new());
    assert_eq!(spec.tasks[1].depends_on, vec!["stg"]);
}

#[test]
fn an_explicit_graph_supplies_the_edges_attribution_cannot() {
    // `mart` consumes both `a` and `b`, but the planner blamed only `a`. Without a
    // graph the `b` edge is invisible — that is the documented limitation.
    let plain = compile(&envelope(vec![direct("a"), direct("b"), downstream("mart", "a")])).unwrap();
    assert_eq!(plain.tasks[2].depends_on, vec!["a"], "attribution alone sees one edge");

    let mut graph = BTreeMap::new();
    graph.insert("mart".to_string(), vec!["a".to_string(), "b".to_string()]);
    let mut env = envelope(vec![direct("a"), direct("b"), downstream("mart", "a")]);
    env.graph = Some(graph);

    let exact = compile(&env).unwrap();
    assert_eq!(exact.tasks[2].depends_on, vec!["a", "b"], "the graph sees both");
}

#[test]
fn a_stale_graph_cannot_introduce_a_cycle() {
    // A graph naming a model that comes LATER in the plan (or is absent entirely)
    // would produce an edge dagron rejects. Both are filtered out.
    let mut graph = BTreeMap::new();
    graph.insert("a".to_string(), vec!["mart".to_string(), "deleted_model".to_string()]);
    let mut env = envelope(vec![direct("a"), downstream("mart", "a")]);
    env.graph = Some(graph);

    let spec = compile(&env).unwrap();
    assert_eq!(spec.tasks[0].depends_on, Vec::<String>::new(), "forward + unknown edges dropped");
    assert_eq!(spec.tasks[1].depends_on, vec!["a"]);
}

#[test]
fn sequential_ordering_chains_the_whole_plan() {
    let mut env = envelope(vec![direct("a"), direct("b"), direct("c")]);
    env.options.ordering = Ordering::Sequential;

    let spec = compile(&env).unwrap();
    assert_eq!(spec.tasks[0].depends_on, Vec::<String>::new());
    assert_eq!(spec.tasks[1].depends_on, vec!["a"]);
    assert_eq!(spec.tasks[2].depends_on, vec!["b"]);
}

#[test]
fn dotted_model_names_are_sanitized_and_kept_unique() {
    // `.` is dagron's fan-out instance separator, so a schema-qualified model name
    // would read as an expansion instance. Two names can sanitize to the same
    // string; the second must not silently become a duplicate task name.
    let spec = compile(&envelope(vec![
        direct("analytics.stg_orders"),
        direct("analytics/stg_orders"),
        direct("analytics-stg_orders"),
    ]))
    .unwrap();

    // `.` and `/` both sanitize to `_`, so these two collide and the second is
    // suffixed. `-` is already legal in a dagron task name and passes through, so
    // the third does NOT collide with them.
    assert_eq!(spec.tasks[0].name, "analytics_stg_orders");
    assert_eq!(spec.tasks[1].name, "analytics_stg_orders_2");
    assert_eq!(spec.tasks[2].name, "analytics-stg_orders");
    // The unsanitized name survives on the task, because the task name no longer
    // keys back to the model.
    assert_eq!(spec.tasks[1].input.as_ref().unwrap()["model"], "analytics/stg_orders");
}

#[test]
fn partitions_render_into_the_command_and_the_input() {
    let mut env = envelope(vec![PlanModel {
        name: "events".into(),
        reason: Reason::DirectlyChanged,
        unit: Unit::Partitions(vec!["2026-09-01".into(), "2026-09-02".into()]),
        depends_on: None,
        replace: None,
    }]);
    env.options.command_template =
        vec!["sh".into(), "-c".into(), "build {{ model }} --parts {{ partitions }}".into()];

    let spec = compile(&env).unwrap();
    assert_eq!(spec.tasks[0].command[2], "build events --parts 2026-09-01,2026-09-02");
    assert_eq!(spec.tasks[0].input.as_ref().unwrap()["unit"], "partitions");
}

#[test]
fn a_full_model_rebuild_renders_empty_partitions() {
    let mut env = envelope(vec![direct("a")]);
    env.options.command_template = vec!["sh".into(), "-c".into(), "b {{ unit }}/{{ partitions }}".into()];
    let spec = compile(&env).unwrap();
    assert_eq!(spec.tasks[0].command[2], "b full_model/");
}

#[test]
fn the_reason_is_preserved_for_explainability() {
    let spec = compile(&envelope(vec![direct("stg"), downstream("mart", "stg")])).unwrap();
    let input = spec.tasks[1].input.as_ref().unwrap();
    assert_eq!(input["reason"], "downstream");
    assert_eq!(input["because_of"], "stg");
    assert_eq!(input["via_columns"][0], "amount");
}

#[test]
fn a_planner_failure_is_not_compiled_into_a_run() {
    let env = PlanEnvelope {
        plan: PlanResponse {
            models: vec![],
            next_state: None,
            error: Some(PlanError { code: "parse".into(), message: "bad sql".into() }),
        },
        graph: None,
        options: opts(),
    };
    assert!(matches!(compile(&env), Err(CompileError::PlannerFailed { .. })));
}

#[test]
fn an_empty_plan_is_refused_distinctly_from_a_failure() {
    // "Nothing to rebuild" is the planner's good outcome. It still has no run in
    // it, but the caller must be able to tell it apart from an error.
    assert_eq!(compile(&envelope(vec![])), Err(CompileError::EmptyPlan));
}

#[test]
fn a_plan_with_no_command_is_refused() {
    let mut env = envelope(vec![direct("a")]);
    env.options.command_template = vec![];
    assert_eq!(compile(&env), Err(CompileError::NoCommand));
}

// ── contract v2: the planner supplies the edges ─────────────────────────────

fn with_deps(name: &str, deps: &[&str]) -> PlanModel {
    PlanModel {
        name: name.into(),
        reason: Reason::DirectlyChanged,
        unit: Unit::FullModel,
        depends_on: Some(deps.iter().map(|d| d.to_string()).collect()),
        replace: None,
    }
}

#[test]
fn the_plans_own_edges_are_used_when_present() {
    // No graph, no sequential — and `mart` still waits for both parents. Under v1
    // this required the caller to supply the project graph by hand.
    let spec = compile(&envelope(vec![
        with_deps("a", &[]),
        with_deps("b", &[]),
        with_deps("mart", &["a", "b"]),
    ]))
    .unwrap();

    assert_eq!(spec.tasks[0].depends_on, Vec::<String>::new());
    assert_eq!(spec.tasks[2].depends_on, vec!["a", "b"]);
}

#[test]
fn the_plans_edges_beat_a_caller_supplied_graph() {
    // The planner computed the plan and knows the project; a caller's graph is a
    // v1 crutch that can be stale. When both exist the planner wins.
    let mut graph = BTreeMap::new();
    graph.insert("mart".to_string(), vec!["a".to_string()]); // stale: misses b
    let mut env = envelope(vec![with_deps("a", &[]), with_deps("b", &[]), with_deps("mart", &["a", "b"])]);
    env.graph = Some(graph);

    let spec = compile(&env).unwrap();
    assert_eq!(spec.tasks[2].depends_on, vec!["a", "b"], "the stale graph must not win");
}

#[test]
fn an_empty_edge_list_means_waits_for_nothing_not_fall_back() {
    // `Some([])` is authoritative. If it were confused with absent, this model
    // would fall through to its `because_of` and gain an edge the planner denied.
    let downstream_with_no_deps = PlanModel {
        name: "mart".into(),
        reason: Reason::Downstream { because_of: "a".into(), via_columns: vec!["x".into()] },
        unit: Unit::FullModel,
        depends_on: Some(vec![]),
        replace: None,
    };
    let spec = compile(&envelope(vec![with_deps("a", &[]), downstream_with_no_deps])).unwrap();

    assert_eq!(
        spec.tasks[1].depends_on,
        Vec::<String>::new(),
        "an explicit empty list is an answer, not a missing one"
    );
}

#[test]
fn a_v1_plan_still_falls_back_through_graph_then_attribution() {
    // Both legacy paths stay reachable, in order.
    let mut graph = BTreeMap::new();
    graph.insert("mart".to_string(), vec!["a".to_string(), "b".to_string()]);
    let mut env = envelope(vec![direct("a"), direct("b"), downstream("mart", "a")]);
    env.graph = Some(graph);
    assert_eq!(compile(&env).unwrap().tasks[2].depends_on, vec!["a", "b"], "graph is used when v1");

    let no_graph = envelope(vec![direct("a"), direct("b"), downstream("mart", "a")]);
    assert_eq!(
        compile(&no_graph).unwrap().tasks[2].depends_on,
        vec!["a"],
        "attribution is the last resort, and is the under-constrained one"
    );
}

#[test]
fn planner_edges_are_still_filtered_against_plan_position() {
    // Defence in depth. The planner already restricts to plan members in topo
    // order, so this filter should be a no-op for v2 — but a forward edge from a
    // malformed producer would make dagron reject the whole run graph.
    let spec = compile(&envelope(vec![
        with_deps("a", &["mart", "ghost"]), // forward + unknown
        with_deps("mart", &["a"]),
    ]))
    .unwrap();

    assert_eq!(spec.tasks[0].depends_on, Vec::<String>::new());
    assert_eq!(spec.tasks[1].depends_on, vec!["a"]);
}

#[test]
fn sequential_ordering_still_overrides_planner_edges() {
    let mut env = envelope(vec![with_deps("a", &[]), with_deps("b", &[]), with_deps("mart", &["a", "b"])]);
    env.options.ordering = Ordering::Sequential;

    let spec = compile(&env).unwrap();
    assert_eq!(spec.tasks[2].depends_on, vec!["b"], "sequential chains regardless of edges");
}

#[test]
fn a_partial_graph_falls_through_per_model_not_per_envelope() {
    // The lookup is `graph.get(model)`, so a v1 envelope whose graph covers only
    // some models is not all-or-nothing: covered models get the graph's edges,
    // uncovered ones still fall through to their attribution.
    let mut graph = BTreeMap::new();
    graph.insert("mart".to_string(), vec!["a".to_string(), "b".to_string()]);
    // `other` is deliberately absent from the graph.

    let mut env = envelope(vec![
        direct("a"),
        direct("b"),
        downstream("mart", "a"),
        downstream("other", "b"),
    ]);
    env.graph = Some(graph);

    let spec = compile(&env).unwrap();
    assert_eq!(spec.tasks[2].depends_on, vec!["a", "b"], "covered by the graph");
    assert_eq!(
        spec.tasks[3].depends_on,
        vec!["b"],
        "not in the graph, so it falls through to `because_of`"
    );
}

// ------------------------------------------------------- contract v3: replace --

fn replaced(name: &str, r: Replace) -> PlanModel {
    let mut m = direct(name);
    m.replace = Some(r);
    m
}

fn insert_overwrite(col: &str, parts: &[&str]) -> Replace {
    Replace {
        strategy: ReplaceStrategy::InsertOverwrite,
        partition_column: Some(col.to_string()),
        unique_key: Vec::new(),
        target: ReplaceTarget::Partitions(parts.iter().map(|s| s.to_string()).collect()),
        widened: None,
    }
}

#[test]
fn the_replace_placeholders_expand_to_the_declaration_not_to_sql() {
    let env = PlanEnvelope {
        plan: PlanResponse {
            models: vec![replaced("events", insert_overwrite("dt", &["2026-06-20"]))],
            ..Default::default()
        },
        graph: None,
        options: CompileOptions {
            command_template: vec![
                "sh".into(),
                "-c".into(),
                "run --select {{ model }} --strategy {{ replace }} --on {{ partition_column }}".into(),
            ],
            ..Default::default()
        },
    };
    let spec = compile(&env).unwrap();
    assert_eq!(
        spec.tasks[0].command[2],
        "run --select events --strategy insert_overwrite --on dt",
        "the placeholder carries the planner's word, not a statement — dagron does \
         not know which warehouse is on the other end of this command"
    );
}

#[test]
fn the_unique_key_placeholder_joins_on_commas_and_is_empty_without_one() {
    let merge = Replace {
        strategy: ReplaceStrategy::Merge,
        partition_column: None,
        unique_key: vec!["id".into(), "dt".into()],
        target: ReplaceTarget::Partitions(vec!["2026-06-20".into()]),
        widened: None,
    };
    let env = PlanEnvelope {
        plan: PlanResponse {
            models: vec![replaced("a", merge), direct("b")],
            ..Default::default()
        },
        graph: None,
        options: CompileOptions {
            command_template: vec!["sh".into(), "-c".into(), "run [{{ unique_key }}]".into()],
            ..Default::default()
        },
    };
    let spec = compile(&env).unwrap();
    assert_eq!(spec.tasks[0].command[2], "run [id,dt]");
    assert_eq!(
        spec.tasks[1].command[2], "run []",
        "a model with no replace op must not leave `{{{{ unique_key }}}}` in the argv — \
         a literal placeholder reaching a shell is a worse failure than an empty one"
    );
}

#[test]
fn the_task_records_the_replace_facts_even_when_the_command_ignores_them() {
    let env = PlanEnvelope {
        plan: PlanResponse {
            models: vec![replaced("events", insert_overwrite("dt", &["2026-06-20", "2026-06-21"]))],
            ..Default::default()
        },
        graph: None,
        options: CompileOptions {
            command_template: vec!["sh".into(), "-c".into(), "run {{ model }}".into()],
            ..Default::default()
        },
    };
    let spec = compile(&env).unwrap();
    let input = spec.tasks[0].input.as_ref().unwrap();
    assert_eq!(input["replace"], "insert_overwrite");
    assert_eq!(input["partition_column"], "dt");
    assert!(
        input.get("replace_widened").is_none(),
        "an honoured declaration has nothing to warn about"
    );
}

#[test]
fn a_widening_is_recorded_on_the_task_so_the_run_history_shows_it() {
    let widened = Replace {
        strategy: ReplaceStrategy::FullRefresh,
        partition_column: None,
        unique_key: Vec::new(),
        target: ReplaceTarget::Whole,
        widened: Some(Widening::NoUniqueKey),
    };
    let env = PlanEnvelope {
        plan: PlanResponse { models: vec![replaced("events", widened)], ..Default::default() },
        graph: None,
        options: CompileOptions {
            command_template: vec!["sh".into(), "-c".into(), "run {{ model }}".into()],
            ..Default::default()
        },
    };
    let spec = compile(&env).unwrap();
    let input = spec.tasks[0].input.as_ref().unwrap();
    assert_eq!(input["replace"], "full_refresh");
    let said = input["replace_widened"].as_str().unwrap();
    assert!(
        said.contains("unique_key"),
        "the run record should say why it rewrote the whole table: {said}"
    );
}

#[test]
fn a_v2_plan_compiles_exactly_as_before() {
    let env = PlanEnvelope {
        plan: PlanResponse { models: vec![direct("a"), direct("b")], ..Default::default() },
        graph: None,
        options: CompileOptions {
            command_template: vec!["sh".into(), "-c".into(), "run {{ model }}".into()],
            ..Default::default()
        },
    };
    let spec = compile(&env).unwrap();
    for task in &spec.tasks {
        let input = task.input.as_ref().unwrap();
        assert!(input.get("replace").is_none(), "no replace key at all, not a null");
        assert!(input.get("replace_widened").is_none());
    }
}
