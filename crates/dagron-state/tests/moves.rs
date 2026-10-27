//! Compiling a warehouse move: the `freshet move --json` document Freshet emits,
//! captured in `tests/fixtures/freshet_move_v1.json`, into unload → load → parity
//! tasks per unit.
//!
//! The fixture is the producer's own output, not a hand-written imitation, so a
//! change on either side of the contract that the other did not follow fails here.

use std::collections::BTreeMap;

use dagron_state::moves::{compile_move, Move, MoveCompileError, MoveEnvelope, MoveOptions, TaskEnv, MOVE_CONTRACT_VERSION};
use dagron_state::spec::TaskSpec;

fn captured() -> Move {
    serde_json::from_str(include_str!("fixtures/freshet_move_v1.json")).expect("the captured move parses")
}

fn envelope(options: MoveOptions) -> MoveEnvelope {
    MoveEnvelope { moved: captured(), options }
}

fn options() -> MoveOptions {
    MoveOptions {
        unload: TaskEnv {
            env: BTreeMap::from([
                ("SQL_DSN".into(), ":memory:".into()),
                ("PGHOST".into(), "old-warehouse".into()),
                ("PGDATABASE".into(), "analytics".into()),
                ("FRESHET_UNIT".into(), "{{ model }}@{{ partition }}".into()),
            ]),
            secret_env: BTreeMap::from([("SQL_PASSWORD".into(), "OLD_WAREHOUSE_PASSWORD".into())]),
        },
        load: TaskEnv {
            env: BTreeMap::from([("SQL_DSN".into(), "https://acme.snowflakecomputing.com/?warehouse=LOAD".into())]),
            secret_env: BTreeMap::from([("SQL_PASSWORD".into(), "SNOWFLAKE_PAT".into())]),
        },
        parity: TaskEnv {
            env: BTreeMap::from([
                ("PARITY_SOURCE_DSN".into(), "postgresql://reader@old-warehouse/analytics".into()),
                ("PARITY_TARGET_DSN".into(), "https://acme.snowflakecomputing.com/?warehouse=CHECK".into()),
            ]),
            secret_env: BTreeMap::from([
                ("PARITY_SOURCE_PASSWORD".into(), "OLD_WAREHOUSE_PASSWORD".into()),
                ("PARITY_TARGET_PASSWORD".into(), "SNOWFLAKE_PAT".into()),
            ]),
        },
        ..MoveOptions::default()
    }
}

fn task<'a>(tasks: &'a [TaskSpec], name: &str) -> &'a TaskSpec {
    tasks.iter().find(|t| t.name == name).unwrap_or_else(|| {
        panic!("no task {name}: {:?}", tasks.iter().map(|t| &t.name).collect::<Vec<_>>())
    })
}

#[test]
fn each_unit_is_an_unload_then_a_load_then_a_parity_check() {
    let spec = compile_move(&envelope(options())).unwrap();
    assert_eq!(spec.name, "warehouse-move");
    assert_eq!(spec.tags, ["warehouse-move"]);
    assert_eq!(spec.tasks.len(), 9, "three units, three steps each");

    let names: Vec<&str> = spec.tasks.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(
        names[..3],
        ["events__dt_2026-06-20__unload", "events__dt_2026-06-20__load", "events__dt_2026-06-20__parity"]
    );
    assert_eq!(task(&spec.tasks, "events__dt_2026-06-20__unload").depends_on, Vec::<String>::new(), "units do not wait on each other");
    assert_eq!(task(&spec.tasks, "events__dt_2026-06-20__load").depends_on, ["events__dt_2026-06-20__unload"]);
    assert_eq!(task(&spec.tasks, "events__dt_2026-06-20__parity").depends_on, ["events__dt_2026-06-20__load"]);
    assert_eq!(task(&spec.tasks, "daily__unload").depends_on, Vec::<String>::new());
}

#[test]
fn the_move_sets_what_only_it_knows_and_the_caller_the_rest() {
    let m = captured();
    let spec = compile_move(&envelope(options())).unwrap();

    let unload = task(&spec.tasks, "events__dt_2026-06-20__unload");
    assert_eq!(unload.command, ["dagron-step-sql"]);
    assert_eq!(unload.env_value("SQL_ENGINE"), Some("duckdb"), "a Postgres source is unloaded through DuckDB");
    assert_eq!(unload.env_value("SQL_MODE"), Some("script"));
    let script = unload.env_value("SQL_STATEMENT").unwrap();
    assert_eq!(script, m.units[0].unload.statements.iter().map(|s| format!("{s};")).collect::<Vec<_>>().join("\n"), "byte for byte");
    assert!(script.contains("ATTACH '' AS freshet_pg (TYPE postgres, READ_ONLY);"), "{script}");
    assert_eq!(unload.env_value("PGHOST"), Some("old-warehouse"));
    assert_eq!(unload.env_value("FRESHET_UNIT"), Some("events@2026-06-20"), "placeholders expand per unit");
    assert_eq!(unload.env_secret("SQL_PASSWORD"), Some("OLD_WAREHOUSE_PASSWORD"), "a secret's name, never its value");

    let load = task(&spec.tasks, "events__dt_2026-06-20__load");
    assert_eq!(load.env_value("SQL_ENGINE"), Some("snowflake"));
    assert!(load.env_value("SQL_STATEMENT").unwrap().starts_with("BEGIN TRANSACTION;\nDELETE FROM ANALYTICS.MARTS.events WHERE dt IN (DATE '2026-06-20');"));
    assert_eq!(load.env_secret("SQL_PASSWORD"), Some("SNOWFLAKE_PAT"));

    let parity = task(&spec.tasks, "events__dt_2026-06-20__parity");
    assert_eq!(parity.command, ["dagron-step-sql", "parity"]);
    for (k, v) in [
        ("PARITY_SOURCE_ENGINE", "postgres"),
        ("PARITY_SOURCE_SCHEMA", "public"),
        ("PARITY_SOURCE_TABLE", "events"),
        ("PARITY_SOURCE_RELATION", "public.events"),
        ("PARITY_SOURCE_WHERE", "dt IN (DATE '2026-06-20')"),
        ("PARITY_TARGET_ENGINE", "snowflake"),
        ("PARITY_TARGET_SCHEMA", "ANALYTICS.MARTS"),
        ("PARITY_TARGET_RELATION", "ANALYTICS.MARTS.events"),
        ("PARITY_TARGET_WHERE", "dt IN (DATE '2026-06-20')"),
        ("PARITY_SOURCE_DSN", "postgresql://reader@old-warehouse/analytics"),
    ] {
        assert_eq!(parity.env_value(k), Some(v), "{k}");
    }
    assert_eq!(parity.env_secret("PARITY_TARGET_PASSWORD"), Some("SNOWFLAKE_PAT"));

    let whole = task(&spec.tasks, "daily__parity");
    assert_eq!(whole.env_value("PARITY_SOURCE_WHERE"), None, "a whole relation compares every row");

    let input = unload.input.as_ref().unwrap();
    assert_eq!(input["move_step"], "unload");
    assert_eq!(input["partition"], "2026-06-20");
    assert_eq!(input["location"], "s3://acme-moves/2026-10/events.dt=2026-06-20/");
    assert_eq!(input["sql_engine"], "duckdb");
}

#[test]
fn check_only_compiles_just_the_parity_checks() {
    let spec = compile_move(&envelope(MoveOptions { check_only: true, ..options() })).unwrap();
    assert_eq!(spec.tasks.len(), 3);
    assert!(spec.tasks.iter().all(|t| t.name.ends_with("__parity") && t.depends_on.is_empty()));
}

#[test]
fn what_cannot_be_compiled_is_refused_and_named() {
    let mut wrong = envelope(options());
    wrong.moved.contract = "freshet-move/2".into();
    assert_eq!(compile_move(&wrong).unwrap_err(), MoveCompileError::Contract { got: "freshet-move/2".into() });
    assert_eq!(MOVE_CONTRACT_VERSION, captured().contract, "the fixture is the contract this build reads");

    let mut empty = envelope(options());
    empty.moved.units.clear();
    assert_eq!(compile_move(&empty).unwrap_err(), MoveCompileError::EmptyMove);

    let mut o = options();
    o.load.env.insert("SQL_ENGINE".into(), "postgres".into());
    assert_eq!(compile_move(&envelope(o)).unwrap_err(), MoveCompileError::Reserved { kind: "load", name: "SQL_ENGINE".into() });
    let mut o = options();
    o.parity.env.insert("PARITY_TARGET_WHERE".into(), "true".into());
    assert!(matches!(compile_move(&envelope(o)).unwrap_err(), MoveCompileError::Reserved { kind: "parity", .. }));
    let mut o = options();
    o.unload.env.insert("SQL_PASSWORD".into(), "x".into());
    assert_eq!(compile_move(&envelope(o)).unwrap_err(), MoveCompileError::EnvConflict { kind: "unload", name: "SQL_PASSWORD".into() });
}

/// `dagron-state move` reads `freshet move --json` as it comes, unwrapped.
#[test]
fn the_cli_compiles_the_producer_s_output_unwrapped() {
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_dagron-state"))
        .args(["move", concat!(env!("CARGO_MANIFEST_DIR"), "/tests/fixtures/freshet_move_v1.json")])
        .output()
        .unwrap();
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    let yaml = String::from_utf8_lossy(&out.stdout);
    assert!(yaml.contains("name: events__dt_2026-06-20__unload"), "{yaml}");
    assert!(yaml.contains("- dagron-step-sql\n  - parity"), "{yaml}");
}
