//! The DuckDB transport, driven through the built binary against a stand-in CLI.
//!
//! The stand-in is a shell script this test writes: it records the arguments, the
//! `PGPASSWORD` it was given and the script on its stdin, then exits as the script
//! asks. It pins what the step does with a child process: stdin, the exit status,
//! stderr, stdout kept off the task's output, the deadline. What DuckDB itself does
//! with the SQL is not tested here; the move rehearsal runs that against a real
//! DuckDB and a real Postgres.

#![cfg(unix)]

use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Output;

const STAND_IN: &str = r#"#!/bin/sh
printf '%s\n' "$@" > "$OUT_DIR/args"
printf '%s' "$PGPASSWORD" > "$OUT_DIR/pgpassword"
cat > "$OUT_DIR/stdin"
case "$(cat "$OUT_DIR/stdin")" in
  *FAIL*) echo "Catalog Error: Table with name nope does not exist!" >&2; exit 1 ;;
  *HANG*) exec sleep 30 ;;
esac
echo "┌───────┐"
echo "│ Count │"
echo "│     3 │"
echo "└───────┘"
"#;

/// One stand-in for every test, written before any test spawns anything. A file
/// written while another test forks can be "busy" to exec(2): the forked child
/// holds the write end until its own exec. `OUT_DIR` keeps the tests apart.
fn stand_in() -> &'static Path {
    static CLI: std::sync::OnceLock<PathBuf> = std::sync::OnceLock::new();
    CLI.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("dagron-step-sql-duckdb-cli-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let cli = dir.join("duckdb");
        std::fs::write(&cli, STAND_IN).unwrap();
        std::fs::set_permissions(&cli, std::fs::Permissions::from_mode(0o755)).unwrap();
        cli
    })
}

/// Every test starts here, so the stand-in exists before its first spawn.
fn workdir(name: &str) -> PathBuf {
    stand_in();
    let dir = std::env::temp_dir().join(format!("dagron-step-sql-duckdb-{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn step(dir: &Path, env: &[(&str, &str)]) -> Output {
    std::process::Command::new(env!("CARGO_BIN_EXE_dagron-step-sql"))
        .env_clear()
        .env("PATH", std::env::var("PATH").unwrap_or_default())
        .env("OUT_DIR", dir)
        .env("SQL_DUCKDB", stand_in())
        .env("SQL_ENGINE", "duckdb")
        .env("SQL_DSN", ":memory:")
        .envs(env.iter().copied())
        .output()
        .unwrap()
}

fn read(dir: &Path, name: &str) -> String {
    std::fs::read_to_string(dir.join(name)).unwrap_or_default()
}

#[test]
fn a_script_goes_on_stdin_and_the_cli_s_tables_stay_off_stdout() {
    let dir = workdir("ok");
    let script = "INSTALL postgres;\nLOAD postgres;\nATTACH '' AS freshet_pg (TYPE postgres, READ_ONLY);\nCOPY (SELECT 1) TO '/tmp/x' (FORMAT parquet);";
    let out = step(&dir, &[("SQL_MODE", "script"), ("SQL_STATEMENT", script), ("SQL_PASSWORD", "pg-secret"), ("LOG_LEVEL", "warn")]);
    assert!(out.status.success(), "{}", String::from_utf8_lossy(&out.stderr));
    assert_eq!(read(&dir, "args"), "-bail\n:memory:\n", "stop at the first error, on the DSN's database");
    assert_eq!(read(&dir, "stdin").trim_end(), script, "the script, on stdin and not in argv");
    assert_eq!(read(&dir, "pgpassword"), "pg-secret", "the password reaches libpq as PGPASSWORD");
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(!stdout.contains("Count"), "the CLI's result tables never reach the task's output: {stdout}");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_failing_statement_fails_the_step_with_duckdb_s_message() {
    let dir = workdir("fail");
    let out = step(&dir, &[("SQL_MODE", "script"), ("SQL_STATEMENT", "SELECT * FROM nope; -- FAIL")]);
    assert!(!out.status.success());
    let e = String::from_utf8_lossy(&out.stderr);
    assert!(e.contains("exit 1") && e.contains("Table with name nope does not exist"), "{e}");
    assert!(e.contains("rolled back"), "says what happened to an open transaction: {e}");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_script_still_running_at_the_deadline_is_killed() {
    let dir = workdir("hang");
    let started = std::time::Instant::now();
    let out = step(&dir, &[("SQL_MODE", "exec"), ("SQL_STATEMENT", "-- HANG"), ("SQL_TIMEOUT_SECS", "1")]);
    assert!(!out.status.success());
    assert!(started.elapsed() < std::time::Duration::from_secs(10), "killed at the deadline, not waited out");
    let e = String::from_utf8_lossy(&out.stderr);
    assert!(e.contains("SQL_TIMEOUT_SECS (1s)") && e.contains("killed"), "{e}");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn duckdb_keeps_no_rows_and_says_so_before_starting_anything() {
    let dir = workdir("rows");
    for mode in ["rows", "scalar"] {
        let out = step(&dir, &[("SQL_MODE", mode), ("SQL_STATEMENT", "SELECT 1"), ("DAGRON_ARTIFACTS", dir.to_str().unwrap())]);
        assert!(!out.status.success());
        let e = String::from_utf8_lossy(&out.stderr);
        assert!(e.contains("`exec` and `script` only"), "{mode}: {e}");
    }
    assert!(!dir.join("stdin").exists(), "the CLI was never started");
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn a_missing_cli_names_the_variable_that_finds_it() {
    let dir = workdir("missing");
    let out = std::process::Command::new(env!("CARGO_BIN_EXE_dagron-step-sql"))
        .env_clear()
        .env("SQL_DUCKDB", dir.join("no-such-duckdb"))
        .env("SQL_ENGINE", "duckdb")
        .env("SQL_DSN", ":memory:")
        .env("SQL_STATEMENT", "SELECT 1")
        .output()
        .unwrap();
    assert!(!out.status.success());
    assert!(String::from_utf8_lossy(&out.stderr).contains("SQL_DUCKDB"), "{}", String::from_utf8_lossy(&out.stderr));
    std::fs::remove_dir_all(&dir).ok();
}
