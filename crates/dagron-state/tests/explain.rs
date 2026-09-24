//! The explanation: what it says, what it refuses to claim, and the graph it draws.

use std::collections::BTreeMap;

use dagron_state::compile::{compile, CompileOptions, Ordering, PlanEnvelope};
use dagron_state::explain::explain;
use dagron_state::wire::{Replace, ReplaceStrategy, ReplaceTarget, Widening, PlanModel, PlanResponse, Reason, Unit};

fn direct(name: &str) -> PlanModel {
    PlanModel {
        name: name.into(),
        reason: Reason::DirectlyChanged,
        unit: Unit::FullModel,
        depends_on: None,
        replace: None,
        sql: None,
    }
}

fn downstream(name: &str, because_of: &str, cols: &[&str]) -> PlanModel {
    PlanModel {
        name: name.into(),
        reason: Reason::Downstream {
            because_of: because_of.into(),
            via_columns: cols.iter().map(|c| c.to_string()).collect(),
        },
        unit: Unit::FullModel,
        depends_on: None,
        replace: None,
        sql: None,
    }
}

fn envelope(models: Vec<PlanModel>) -> PlanEnvelope {
    PlanEnvelope {
        plan: PlanResponse { models, ..Default::default() },
        graph: None,
        options: CompileOptions {
            command_template: vec!["sh".into(), "-c".into(), "build {{ model }}".into()],
            ..Default::default()
        },
    }
}

fn explained(env: &PlanEnvelope) -> dagron_state::explain::Explanation {
    let spec = compile(env).expect("must compile");
    explain(env, &spec)
}

#[test]
fn the_summary_reports_pruning_only_when_the_project_size_is_known() {
    // The planner's whole value claim is "we did NOT rebuild most of your project".
    // Without a graph the plan cannot see what it skipped, so it must not imply a
    // saving it has no way to measure.
    let without = explained(&envelope(vec![direct("a"), downstream("b", "a", &["x"])]));
    assert!(without.summary.contains("2 models rebuild"), "got: {}", without.summary);
    assert!(without.summary.contains("project size unknown"), "got: {}", without.summary);
    assert!(!without.summary.contains('%'), "must not claim a percentage: {}", without.summary);

    let mut graph = BTreeMap::new();
    graph.insert("b".to_string(), vec!["a".to_string()]);
    graph.insert("c".to_string(), vec!["a".to_string()]);
    graph.insert("d".to_string(), vec!["c".to_string()]);
    let mut with = envelope(vec![direct("a"), downstream("b", "a", &["x"])]);
    with.graph = Some(graph);

    // a, b, c, d = 4 models; 2 selected → 50% pruned.
    assert_eq!(explained(&with).summary, "2 of 4 models rebuild — 50% pruned");
}

#[test]
fn a_graph_smaller_than_the_plan_reports_no_percentage() {
    // A graph that does not even cover the plan is not a project census. Reporting
    // a percentage off it would invent a saving (or produce a negative one).
    let mut graph = BTreeMap::new();
    graph.insert("a".to_string(), vec![]);
    let mut env = envelope(vec![direct("a"), direct("b"), direct("c")]);
    env.graph = Some(graph);

    // The graph names only `a`, but the union with the plan is a,b,c = 3 of 3.
    let summary = explained(&env).summary;
    assert!(summary.contains("3 of 3"), "got: {summary}");
    assert!(summary.contains("0% pruned"), "got: {summary}");
}

#[test]
fn one_model_is_singular() {
    assert!(explained(&envelope(vec![direct("solo")])).summary.starts_with("1 model rebuild"));
}

#[test]
fn the_markdown_names_the_columns_that_carried_the_change() {
    // The column-level attribution is what distinguishes this from a naive
    // "rebuild everything downstream", so it has to reach the reader.
    let ex = explained(&envelope(vec![direct("stg"), downstream("mart", "stg", &["amount", "qty"])]));
    assert!(ex.markdown.contains("downstream of `stg` via `amount`, `qty`"), "got:\n{}", ex.markdown);
    assert!(ex.markdown.contains("| `stg` | directly changed |"), "got:\n{}", ex.markdown);
    // The graph is embedded in a fenced block GitHub renders natively.
    assert!(ex.markdown.contains("```mermaid"));
}

#[test]
fn the_mermaid_graph_carries_the_edges_and_classes_both_kinds() {
    let ex = explained(&envelope(vec![direct("stg"), downstream("mart", "stg", &["amount"])]));
    assert!(ex.mermaid.starts_with("flowchart TD"));
    assert!(ex.mermaid.contains("stg --> mart"), "got:\n{}", ex.mermaid);
    assert!(ex.mermaid.contains("class stg changed"), "got:\n{}", ex.mermaid);
    assert!(ex.mermaid.contains("class mart downstream"), "got:\n{}", ex.mermaid);
}

#[test]
fn a_plan_of_only_direct_changes_emits_no_downstream_class() {
    // An empty `class  downstream` line is invalid Mermaid and would break the
    // whole diagram, so the class must be omitted rather than emitted empty.
    let ex = explained(&envelope(vec![direct("a"), direct("b")]));
    assert!(ex.mermaid.contains("class a,b changed"), "got:\n{}", ex.mermaid);
    assert!(!ex.mermaid.contains("class  downstream"), "got:\n{}", ex.mermaid);
    assert!(!ex.mermaid.contains("class \n"), "got:\n{}", ex.mermaid);
}

#[test]
fn rows_carry_the_unsanitized_model_and_the_sanitized_task() {
    // The console renders from rows, and needs both: the model name is what the
    // user wrote, the task name is what they will find in the run.
    let ex = explained(&envelope(vec![direct("analytics.stg_orders")]));
    assert_eq!(ex.rows[0].model, "analytics.stg_orders");
    assert_eq!(ex.rows[0].task, "analytics_stg_orders");
    // Mermaid node ids must be the sanitized names, or the diagram will not parse.
    assert!(ex.mermaid.contains("analytics_stg_orders[\"analytics.stg_orders\"]"));
}

#[test]
fn partition_units_are_counted_not_listed() {
    let mut env = envelope(vec![PlanModel {
        name: "events".into(),
        reason: Reason::DirectlyChanged,
        unit: Unit::Partitions(vec!["d1".into(), "d2".into(), "d3".into()]),
        depends_on: None,
        replace: None,
        sql: None,
    }]);
    env.options.ordering = Ordering::Derived;
    let ex = explained(&env);
    assert_eq!(ex.rows[0].unit, "3 partitions");

    let one = explained(&envelope(vec![PlanModel {
        name: "events".into(),
        reason: Reason::DirectlyChanged,
        unit: Unit::Partitions(vec!["d1".into()]),
        depends_on: None,
        replace: None,
        sql: None,
    }]));
    assert_eq!(one.rows[0].unit, "1 partition", "singular, not '1 partitions'");
}

#[test]
fn rows_line_up_with_the_spec_that_will_actually_run() {
    // explain() zips plan models against compiled tasks; if those ever fell out of
    // step the report would describe a different run than the one submitted.
    let ex = explained(&envelope(vec![direct("a"), downstream("b", "a", &["x"]), downstream("c", "b", &["y"])]));
    assert_eq!(ex.rows.len(), 3);
    assert_eq!(ex.rows[2].model, "c");
    assert_eq!(ex.rows[2].depends_on, vec!["b"]);
    assert_eq!(ex.rows[0].depends_on, Vec::<String>::new());
}

#[test]
fn a_model_named_end_does_not_break_the_mermaid_diagram() {
    // `end` terminates a `subgraph`, so a bare `end["end"]` node is parsed as
    // syntax and the ENTIRE diagram fails to render — not just that node.
    let ex = explained(&envelope(vec![direct("end"), downstream("mart", "end", &["x"])]));

    assert!(!ex.mermaid.contains("\n    end["), "a bare `end` node id breaks the diagram:\n{}", ex.mermaid);
    assert!(ex.mermaid.contains("end_[\"end\"]"), "got:\n{}", ex.mermaid);
    // The remapped id has to be used for the edge and the class too, or the
    // diagram references a node that was never declared.
    assert!(ex.mermaid.contains("end_ --> mart"), "got:\n{}", ex.mermaid);
    assert!(ex.mermaid.contains("class end_ changed"), "got:\n{}", ex.mermaid);
    // The dagron task name is untouched — only the diagram id is remapped.
    assert_eq!(ex.rows[0].task, "end");
}

#[test]
fn remapping_end_cannot_collide_with_a_sanitized_name() {
    // Sanitization turns any invalid character into `_`, so `end.` already becomes
    // `end_`. Naively appending one `_` to the reserved `end` would produce a
    // duplicate node id.
    let ex = explained(&envelope(vec![direct("end"), direct("end.")]));

    assert_eq!(ex.rows[1].task, "end_", "sanitization still owns this name");
    assert!(ex.mermaid.contains("end__[\"end\"]"), "reserved id must skip past it:\n{}", ex.mermaid);
    assert!(ex.mermaid.contains("end_[\"end.\"]"), "got:\n{}", ex.mermaid);
    // Two distinct declarations, no duplicate ids.
    assert!(ex.mermaid.contains("class end__,end_ changed"), "got:\n{}", ex.mermaid);
}

#[test]
fn capitalized_end_is_not_reserved_and_is_left_alone() {
    // Mermaid is case-sensitive here; only lowercase `end` is the keyword.
    let ex = explained(&envelope(vec![direct("End")]));
    assert!(ex.mermaid.contains("End[\"End\"]"), "got:\n{}", ex.mermaid);
}

// ------------------------------------------------------- contract v3: replace --

fn with_replace(name: &str, r: Replace) -> PlanModel {
    let mut m = direct(name);
    m.replace = Some(r);
    m
}

#[test]
fn a_widening_leads_the_comment_rather_than_hiding_in_the_table() {
    let widened = Replace {
        strategy: ReplaceStrategy::FullRefresh,
        partition_column: None,
        unique_key: Vec::new(),
        target: ReplaceTarget::Whole,
        widened: Some(Widening::NotIncremental { materialization: "table".into() }),
    };
    let mut models: Vec<PlanModel> = (0..6).map(|i| direct(&format!("filler_{i}"))).collect();
    models.push(with_replace("user_dim", widened));

    let md = explained(&envelope(models)).markdown;
    let warning = md.find("[!WARNING]").expect("a widening must be called out");
    let table = md.find("| Model |").expect("the table is still there");
    assert!(
        warning < table,
        "the callout must come before the table: a reviewer skimming 7 rows should \
         not have to find the one that rewrites a whole relation"
    );
    assert!(md.contains("`user_dim` — the model is a table"), "and it should name it: {md}");
}

#[test]
fn the_writes_column_appears_only_when_a_plan_has_something_to_say() {
    let plain = explained(&envelope(vec![direct("a"), direct("b")])).markdown;
    assert!(
        plain.contains("| Model | Why | Unit | Waits on |"),
        "a v2 plan keeps the four-column table it always had: {plain}"
    );
    assert!(!plain.contains("[!WARNING]"));

    let honoured = Replace {
        strategy: ReplaceStrategy::InsertOverwrite,
        partition_column: Some("dt".into()),
        unique_key: Vec::new(),
        target: ReplaceTarget::Partitions(vec!["2026-06-20".into(), "2026-06-21".into()]),
        widened: None,
    };
    let md = explained(&envelope(vec![with_replace("events", honoured), direct("b")])).markdown;
    assert!(md.contains("| Model | Why | Unit | Waits on | Writes |"), "{md}");
    assert!(md.contains("`insert_overwrite` over 2 partitions on `dt`"), "{md}");
    assert!(md.contains("| — |"), "a model with no op gets a dash, not a blank: {md}");
    assert!(!md.contains("[!WARNING]"), "an honoured declaration is not a warning");
}

#[test]
fn the_rows_carry_writes_and_widening_separately_for_a_client_that_styles_them() {
    let widened = Replace {
        strategy: ReplaceStrategy::FullRefresh,
        partition_column: None,
        unique_key: Vec::new(),
        target: ReplaceTarget::Whole,
        widened: Some(Widening::NoUniqueKey),
    };
    let ex = explained(&envelope(vec![with_replace("events", widened), direct("plain")]));

    let events = ex.rows.iter().find(|r| r.model == "events").unwrap();
    assert_eq!(events.writes.as_deref(), Some("`full_refresh` over the whole relation"));
    assert!(events.widened.as_deref().unwrap().contains("unique_key"));

    let plain = ex.rows.iter().find(|r| r.model == "plain").unwrap();
    assert!(plain.writes.is_none());
    assert!(plain.widened.is_none());
}

#[test]
fn one_partition_is_singular() {
    let one = Replace {
        strategy: ReplaceStrategy::DeleteInsert,
        partition_column: Some("dt".into()),
        unique_key: Vec::new(),
        target: ReplaceTarget::Partitions(vec!["2026-06-20".into()]),
        widened: None,
    };
    let ex = explained(&envelope(vec![with_replace("events", one)]));
    assert_eq!(
        ex.rows[0].writes.as_deref(),
        Some("`delete_insert` over 1 partition on `dt`")
    );
}

#[test]
fn a_non_atomic_write_is_called_out_before_the_table() {
    use dagron_state::wire::Sql;
    let mut m = direct("events");
    m.sql = Some(Sql { dialect: "databricks".into(), statements: vec!["DELETE …".into(), "INSERT …".into()], atomic: false });
    let mut ok = direct("daily");
    ok.sql = Some(Sql { dialect: "databricks".into(), statements: vec!["CREATE OR REPLACE TABLE …".into()], atomic: true });
    let md = explained(&envelope(vec![m, ok])).markdown;
    let caution = md.find("[!CAUTION]").expect("a caution callout");
    assert!(caution < md.find("| Model |").unwrap(), "before the table:\n{md}");
    assert!(md.contains("> - `events` (databricks)"), "{md}");
    assert!(!md.contains("> - `daily`"), "an atomic write is not called out:\n{md}");
    assert!(md.contains("Rendered as **databricks** SQL: 3 statement(s) across 2 model(s)."), "{md}");
}
