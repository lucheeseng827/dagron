//! The two implementations of the step protocol agree: a task dagron-ci
//! compiles behaves the same whether its command hands the steps to
//! dagron-ci-exec or runs them as the shell fallback. Same output, same
//! stderr (markers and the steps' own lines, in order), same exit code; the
//! binary adds its result line, and only that.
//!
//! One difference is the fallback shell's, and not the protocol's: a shell
//! reports a foreground command a signal ended, in its own words (dash:
//! `Killed`; bash, otherwise). The end marker's code says the same for both,
//! so that one line is set aside before the two are compared.

#![cfg(unix)]

use std::collections::BTreeMap;
use std::process::Command;

use dagron_ci::pipeline::{self, compile};

const BIN: &str = env!("CARGO_BIN_EXE_dagron-ci-exec");

/// Where the compiled command looks first; where the test cannot keep it out.
const INSTALLED: &str = "/dagron/bin/dagron-ci-exec";

/// The one task `pipeline` compiles to for a push to main: its command, and
/// its environment as dagron gives it (task defaults, then its own, with the
/// run's parameters filled in).
fn compiled(pipeline: &str) -> (Vec<String>, Vec<(String, String)>) {
    let pipeline = pipeline::parse(pipeline).unwrap_or_else(|e| panic!("{e:#?}"));
    let event = compile::Event {
        kind: "push".into(),
        git_ref: "refs/heads/main".into(),
        sha: "4b4708e725f29891d22b4c96af9f0a403ff51c7c".into(),
        trust: "trusted".into(),
        repo: "acme/widgets".into(),
        forge_host: "github.com".into(),
        ..compile::Event::default()
    };
    let compiled = match compile::compile(&pipeline, &event, &compile::Settings::default()) {
        Ok(compile::Outcome::Run(compiled)) => compiled,
        other => panic!("the pipeline does not run: {other:?}"),
    };
    let doc: serde_yaml::Value = serde_yaml::from_str(&compiled.yaml).unwrap();
    let fill = |value: &str| {
        compiled
            .parameters
            .iter()
            .fold(value.to_string(), |v, (name, param)| {
                v.replace(&format!("{{{{ {name} }}}}"), param)
            })
    };
    let entries = |list: &serde_yaml::Value| -> Vec<(String, String)> {
        list.as_sequence()
            .map(|seq| {
                seq.iter()
                    .map(|e| {
                        (
                            e["name"].as_str().unwrap().to_string(),
                            fill(e["value"].as_str().unwrap()),
                        )
                    })
                    .collect()
            })
            .unwrap_or_default()
    };
    let task = &doc["tasks"][0];
    let mut env = entries(&doc["task_defaults"]["env"]);
    env.extend(entries(&task["env"]));
    let command = task["command"]
        .as_sequence()
        .unwrap()
        .iter()
        .map(|a| a.as_str().unwrap().to_string())
        .collect();
    (command, env)
}

struct Ran {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

/// Run the compiled command with `env`, `PATH` as given.
fn run(command: &[String], env: &[(String, String)], path: &str) -> Ran {
    let out = Command::new(&command[0])
        .args(&command[1..])
        .env_clear()
        .env("PATH", path)
        .envs(env.iter().cloned())
        .output()
        .unwrap();
    Ran {
        code: out.status.code(),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
    }
}

/// Run `pipeline`'s task both ways and hold them to each other.
fn conforms(pipeline: &str) -> Ran {
    assert!(
        !std::path::Path::new(INSTALLED).exists(),
        "{INSTALLED} exists here, so the fallback cannot be run"
    );
    let (command, env) = compiled(pipeline);
    let path = std::env::var("PATH").unwrap();
    let fallback = run(&command, &env, &path);
    assert!(
        !fallback.stderr.contains("::dagron-ci::result::"),
        "the fallback ran dagron-ci-exec: {}",
        fallback.stderr
    );

    let dir = tempfile::tempdir().unwrap();
    std::os::unix::fs::symlink(BIN, dir.path().join("dagron-ci-exec")).unwrap();
    let exec = run(&command, &env, &format!("{}:{path}", dir.path().display()));
    let (markers, result) = exec
        .stderr
        .trim_end_matches('\n')
        .rsplit_once('\n')
        .unwrap_or(("", exec.stderr.trim_end_matches('\n')));
    assert!(
        result.starts_with("::dagron-ci::result::"),
        "the command did not hand its steps to dagron-ci-exec: {}",
        exec.stderr
    );
    let markers = if markers.is_empty() {
        String::new()
    } else {
        format!("{markers}\n")
    };

    assert_eq!(exec.code, fallback.code, "exit codes\n{pipeline}");
    assert_eq!(exec.stdout, fallback.stdout, "stdout\n{pipeline}");
    assert_eq!(
        markers,
        without_signal_reports(&fallback.stderr),
        "stderr\n{pipeline}"
    );
    fallback
}

/// `stderr` without the lines a shell prints for a step a signal ended: one
/// of dash's descriptions, right before an end marker whose code is 128 plus
/// a signal.
fn without_signal_reports(stderr: &str) -> String {
    const REPORTS: &[&str] = &[
        "Killed",
        "Terminated",
        "Aborted",
        "Hangup",
        "Quit",
        "Segmentation fault",
    ];
    let lines: Vec<&str> = stderr.lines().collect();
    let mut kept = String::new();
    for (i, line) in lines.iter().enumerate() {
        let report = REPORTS.iter().any(|r| {
            line.strip_prefix(r)
                .is_some_and(|rest| rest.is_empty() || rest == " (core dumped)")
        });
        let signalled_end = lines.get(i + 1).is_some_and(|next| {
            next.starts_with("::dagron-ci::step::")
                && next
                    .rsplit_once("::end::")
                    .and_then(|(_, code)| code.parse::<i32>().ok())
                    .is_some_and(|code| code > 128)
        });
        if !(report && signalled_end) {
            kept.push_str(line);
            kept.push('\n');
        }
    }
    kept
}

fn one_task(steps: &str) -> String {
    format!("name: conformance\non: {{ push: }}\ntasks:\n  - name: job\n    steps:\n{steps}")
}

#[test]
fn the_compiler_and_the_runner_speak_one_protocol() {
    assert_eq!(compile::STEP_PROTOCOL, dagron_ci_exec::PROTOCOL);
    // A task the validator takes is one the runner takes.
    assert_eq!(pipeline::MAX_STEPS, dagron_ci_exec::protocol::MAX_STEPS);
}

/// A task with the most steps the validator takes starts: its command is one
/// argument, which the kernel refuses at 128 KiB, and each step adds to it.
#[test]
fn a_task_with_the_most_steps_starts() {
    let ran = conforms(&one_task(
        &"      - run: \"true\"\n".repeat(pipeline::MAX_STEPS),
    ));
    assert_eq!(ran.code, Some(0));
    assert_eq!(ran.stderr.matches("::start::").count(), pipeline::MAX_STEPS);
}

#[test]
fn order_output_and_the_failure_that_ends_a_task() {
    let ran = conforms(&one_task(
        "      - run: echo '[\"a\",\"b\"]'; echo progress >&2\n\
         \x20     - name: second\n        run: echo two; exit 3\n\
         \x20     - run: echo never\n",
    ));
    assert_eq!(ran.code, Some(3));
    assert_eq!(ran.stdout, "[\"a\",\"b\"]\ntwo\n");
}

#[test]
fn a_step_that_may_fail() {
    let ran = conforms(&one_task(
        "      - run: exit 7\n        continue_on_error: true\n      - run: echo after\n",
    ));
    assert_eq!((ran.code, ran.stdout.as_str()), (Some(0), "after\n"));
}

#[test]
fn variables_directories_and_set_e_are_each_step_s_own() {
    let ran = conforms(&one_task(
        "      - run: printf '%s\\n' \"$GREETING\"; cd /; export LEAK=1\n        \
         env: { GREETING: \"hi $(id) `id` ${HOME}\" }\n\
         \x20     - run: echo \"${GREETING:-unset} ${LEAK:-unset} $(pwd)\"; echo \"$CI_REF\"\n\
         \x20     - run: false; echo unreachable\n",
    ));
    assert_eq!(ran.code, Some(1));
    assert!(ran
        .stdout
        .starts_with("hi $(id) `id` ${HOME}\nunset unset "));
    assert!(ran.stdout.ends_with("refs/heads/main\n"), "{}", ran.stdout);
}

#[test]
fn a_name_and_a_script_that_look_like_commands_are_text() {
    let ran = conforms(&one_task(
        "      - name: \"$(touch pwned) `id`\"\n        run: echo 'quoted; $(not run)'\n",
    ));
    assert_eq!(ran.stdout, "quoted; $(not run)\n");
    assert!(ran
        .stderr
        .starts_with("::dagron-ci::step::1::start::$(touch pwned) `id`\n"));
}

#[test]
fn a_step_killed_by_a_signal() {
    let ran = conforms(&one_task(
        "      - run: kill -9 $$\n      - run: echo never\n",
    ));
    assert_eq!(ran.code, Some(137));
}

#[test]
fn only_a_signal_report_is_set_aside() {
    let end = "::dagron-ci::step::1::end::";
    assert_eq!(
        without_signal_reports(&format!("out\nKilled\n{end}137\n")),
        format!("out\n{end}137\n")
    );
    // A step that prints the word itself, or exits on its own.
    let kept = format!("Killed\n{end}1\n");
    assert_eq!(without_signal_reports(&kept), kept);
    let kept = format!("Killed by user\n{end}137\n");
    assert_eq!(without_signal_reports(&kept), kept);
}

#[test]
fn a_step_past_its_timeout() {
    let timeout = Command::new("sh")
        .args(["-c", "command -v timeout"])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    if !timeout {
        return eprintln!("no timeout(1) here for the fallback - skipping");
    }
    let ran = conforms(&one_task(
        "      - run: sleep 30\n        timeout_secs: 1\n      - run: echo never\n",
    ));
    assert_eq!(ran.code, Some(124));
}

#[test]
fn the_environment_carries_every_step() {
    let (_, env) = compiled(&one_task(
        "      - run: a\n        timeout_secs: 5\n        continue_on_error: true\n      - run: b\n",
    ));
    let env: BTreeMap<String, String> = env.into_iter().collect();
    assert_eq!(env["CI_STEP_PROTOCOL"], "1");
    assert_eq!(env["CI_STEP_COUNT"], "2");
    assert_eq!(env["CI_STEP_1_TIMEOUT_SECS"], "5");
    assert_eq!(env["CI_STEP_1_CONTINUE_ON_ERROR"], "true");
    assert!(!env.contains_key("CI_STEP_2_TIMEOUT_SECS"));
    let job = dagron_ci_exec::read(env.into_iter().map(|(k, v)| (k.into(), v.into()))).unwrap();
    assert_eq!(job.steps.len(), 2);
    assert!(job.steps[0].continue_on_error && !job.steps[1].continue_on_error);
}
