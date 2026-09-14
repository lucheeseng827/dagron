//! The CLI's contract, exercised through the built binary.
//!
//! These go through `main.rs` rather than the library because the bugs this file
//! guards against live in argument handling — the layer a library test cannot see.

use std::io::Write;
use std::process::{Command, Stdio};

fn bin() -> &'static str {
    env!("CARGO_BIN_EXE_dagron-state")
}

/// An envelope that already carries its own command, so a CLI flag has something
/// to override.
const ENVELOPE_WITH_COMMAND: &str = r#"{
  "plan": { "models": [ { "name": "m", "reason": "directly_changed", "unit": "full_model" } ] },
  "options": { "command_template": ["sh", "-c", "ENVELOPE_COMMAND {{ model }}"] }
}"#;

const BARE_PLAN: &str = r#"{"models":[{"name":"m","reason":"directly_changed","unit":"full_model"}]}"#;

fn run(args: &[&str], stdin: &str) -> (String, String, Option<i32>) {
    let mut child = Command::new(bin())
        .args(args)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .expect("binary must run");
    // A broken pipe here is not a failure: the binary validates its flags BEFORE
    // reading stdin (so `plan` with no --command fails fast), which closes the
    // pipe while we are still writing. That ordering is the desired behaviour.
    let _ = child.stdin.as_mut().unwrap().write_all(stdin.as_bytes());
    drop(child.stdin.take());
    let out = child.wait_with_output().unwrap();
    (
        String::from_utf8_lossy(&out.stdout).into_owned(),
        String::from_utf8_lossy(&out.stderr).into_owned(),
        out.status.code(),
    )
}

#[test]
fn an_explicit_command_overrides_the_envelope_even_when_it_equals_the_fallback() {
    // The regression: when "absent" was collapsed into the placeholder string, the
    // override test became a string comparison — so passing exactly the fallback
    // text looked identical to passing nothing, and the envelope's own command was
    // compiled instead. Silently, into YAML someone would then run.
    let (stdout, stderr, code) =
        run(&["plan", "-", "--command", "echo {{ model }}"], ENVELOPE_WITH_COMMAND);

    assert_eq!(code, Some(0), "stderr: {stderr}");
    assert!(stdout.contains("echo m"), "the typed command must win:\n{stdout}");
    assert!(!stdout.contains("ENVELOPE_COMMAND"), "the envelope's command must lose:\n{stdout}");
}

#[test]
fn an_explicit_command_overrides_the_envelope_in_the_ordinary_case_too() {
    let (stdout, _, code) = run(&["plan", "-", "--command", "CLI_COMMAND {{ model }}"], ENVELOPE_WITH_COMMAND);
    assert_eq!(code, Some(0));
    assert!(stdout.contains("CLI_COMMAND m"), "got:\n{stdout}");
}

#[test]
fn without_the_flag_the_envelopes_own_command_is_kept() {
    // This is the assertion the test name promises, and it is only reachable
    // because `plan` accepts a command from the envelope. An earlier version
    // passed --command here and so asserted the override instead — the opposite
    // of its own name.
    let (yaml, stderr, code) = run(&["plan", "-"], ENVELOPE_WITH_COMMAND);

    assert_eq!(code, Some(0), "an envelope command is a command; stderr: {stderr}");
    assert!(yaml.contains("ENVELOPE_COMMAND m"), "the envelope's command survives:\n{yaml}");
    assert!(!yaml.contains("echo m"), "the placeholder must not appear:\n{yaml}");
}

#[test]
fn plan_refuses_when_no_command_exists_anywhere_and_explain_does_not() {
    // A bare planner response carries no command, and no flag was given, so
    // `plan` would have to invent the argv it emits. It refuses instead.
    let (_, stderr, code) = run(&["plan", "-"], BARE_PLAN);
    assert_eq!(code, Some(1), "plan emits YAML someone runs; it cannot guess");
    assert!(stderr.contains("needs a command"), "stderr: {stderr}");

    let (stdout, _, code) = run(&["explain", "-"], BARE_PLAN);
    assert_eq!(code, Some(0), "explain never shows the command, so it needs no flag");
    assert!(stdout.contains("State plan"));
}

#[test]
fn a_bare_planner_response_is_accepted_unwrapped() {
    // `freshet plan --json | dagron-state explain -` is the documented pipe; it
    // only works if a response with no `plan` key is detected as such.
    let (stdout, stderr, code) = run(&["explain", "-"], BARE_PLAN);
    assert_eq!(code, Some(0), "stderr: {stderr}");
    assert!(stdout.contains("| `m` |"), "got:\n{stdout}");
}

#[test]
fn exit_code_two_is_opt_in_for_the_ci_gate() {
    let (_, _, code) = run(&["explain", "-", "--exit-code"], BARE_PLAN);
    assert_eq!(code, Some(2), "non-empty plan under --exit-code");

    let (stdout, _, code) = run(&["explain", "-", "--exit-code"], r#"{"models":[]}"#);
    assert_eq!(code, Some(0), "an empty plan is success, not a gate failure");
    assert!(stdout.contains("No models to rebuild"));
}

#[test]
fn an_unknown_flag_is_refused_rather_than_read_as_a_path() {
    let (_, stderr, code) = run(&["explain", "-x"], BARE_PLAN);
    assert_eq!(code, Some(1));
    assert!(stderr.contains("unknown flag"), "stderr: {stderr}");
}
