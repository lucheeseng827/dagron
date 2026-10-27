//! dagron-ci-exec through its binary: the steps of a protocol-1 environment,
//! run by a real `sh`.

#![cfg(unix)]

use std::io::{BufRead, BufReader, Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

use serde_json::Value;

const BIN: &str = env!("CARGO_BIN_EXE_dagron-ci-exec");

/// A step as the protocol carries it.
#[derive(Clone, Default)]
struct S {
    name: Option<&'static str>,
    run: String,
    timeout: Option<u64>,
    continue_on_error: bool,
    env: Vec<(&'static str, String)>,
}

fn s(run: &str) -> S {
    S {
        run: run.into(),
        ..S::default()
    }
}

/// The runner, with exactly the protocol-1 variables for `steps`.
fn job(steps: &[S]) -> Command {
    let mut cmd = Command::new(BIN);
    cmd.env_clear()
        .env("PATH", std::env::var("PATH").unwrap())
        .env("CI_STEP_PROTOCOL", "1")
        .env("CI_STEP_COUNT", steps.len().to_string())
        .stdin(Stdio::null());
    for (i, step) in steps.iter().enumerate() {
        let n = i + 1;
        if let Some(name) = step.name {
            cmd.env(format!("CI_STEP_{n}_NAME"), name);
        }
        cmd.env(format!("CI_STEP_{n}_RUN"), &step.run);
        if let Some(t) = step.timeout {
            cmd.env(format!("CI_STEP_{n}_TIMEOUT_SECS"), t.to_string());
        }
        if step.continue_on_error {
            cmd.env(format!("CI_STEP_{n}_CONTINUE_ON_ERROR"), "true");
        }
        for (k, v) in &step.env {
            cmd.env(format!("CI_STEP_{n}_ENV_{k}"), v);
        }
    }
    cmd
}

struct Ran {
    code: Option<i32>,
    stdout: String,
    /// stderr without the result line.
    stderr: String,
    result: Value,
}

fn finish(out: std::process::Output) -> Ran {
    let stderr = String::from_utf8_lossy(&out.stderr).into_owned();
    let (body, last) = stderr
        .trim_end_matches('\n')
        .rsplit_once('\n')
        .unwrap_or(("", stderr.trim_end_matches('\n')));
    let result = last
        .strip_prefix("::dagron-ci::result::")
        .map(|json| serde_json::from_str(json).unwrap())
        .unwrap_or(Value::Null);
    let stderr = if result.is_null() {
        stderr.clone()
    } else if body.is_empty() {
        String::new()
    } else {
        format!("{body}\n")
    };
    Ran {
        code: out.status.code(),
        stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr,
        result,
    }
}

fn run(cmd: &mut Command) -> Ran {
    finish(cmd.output().unwrap())
}

fn outcomes(result: &Value) -> Vec<(String, String)> {
    result["steps"]
        .as_array()
        .unwrap()
        .iter()
        .map(|s| {
            (
                s["outcome"].as_str().unwrap().to_string(),
                s["conclusion"].as_str().unwrap().to_string(),
            )
        })
        .collect()
}

/// Whether `pid` is a live process: a zombie is not.
fn alive(pid: i32) -> bool {
    match std::fs::read_to_string(format!("/proc/{pid}/stat")) {
        Ok(stat) => stat
            .rsplit_once(')')
            .and_then(|(_, rest)| rest.trim_start().chars().next())
            .is_some_and(|state| state != 'Z'),
        Err(_) => false,
    }
}

/// Wait up to `secs` for `pid` to be gone; kill it if it is not, so a failed
/// test leaves nothing behind.
fn gone_within(pid: i32, secs: u64) -> bool {
    let until = Instant::now() + Duration::from_secs(secs);
    while Instant::now() < until {
        if !alive(pid) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(20));
    }
    // SAFETY: plain integers.
    unsafe { libc::kill(pid, libc::SIGKILL) };
    false
}

fn read_pid(path: &Path) -> i32 {
    let until = Instant::now() + Duration::from_secs(10);
    loop {
        if let Ok(text) = std::fs::read_to_string(path) {
            if let Ok(pid) = text.trim().parse() {
                return pid;
            }
        }
        assert!(Instant::now() < until, "{} never written", path.display());
        std::thread::sleep(Duration::from_millis(10));
    }
}

#[test]
fn steps_run_in_order_their_stdout_is_the_output_and_markers_go_to_stderr() {
    let ran = run(&mut job(&[
        S {
            name: Some("list"),
            ..s("echo '[\"a\",\"b\"]'; echo progress >&2")
        },
        s("echo second"),
    ]));
    assert_eq!(ran.code, Some(0), "{}", ran.stderr);
    assert_eq!(ran.stdout, "[\"a\",\"b\"]\nsecond\n");
    assert_eq!(
        ran.stderr,
        "::dagron-ci::step::1::start::list\nprogress\n::dagron-ci::step::1::end::0\n\
         ::dagron-ci::step::2::start::step 2\n::dagron-ci::step::2::end::0\n"
    );
    assert_eq!(ran.result["exit_code"], 0);
    assert_eq!(ran.result["protocol"], 1);
    let first = &ran.result["steps"][0];
    assert_eq!(
        (first["n"].as_u64(), first["name"].as_str()),
        (Some(1), Some("list"))
    );
    assert!(first["started_at_ms"].as_u64().unwrap() > 1_700_000_000_000);
    assert!(first["duration_ms"].is_u64());
}

#[test]
fn a_failing_step_ends_the_task_with_its_code_and_the_rest_are_skipped() {
    let ran = run(&mut job(&[
        s("echo one"),
        s("echo two; exit 3"),
        s("echo never"),
    ]));
    assert_eq!(ran.code, Some(3), "{}", ran.stderr);
    assert_eq!(ran.stdout, "one\ntwo\n");
    assert!(!ran.stderr.contains("step::3"), "{}", ran.stderr);
    assert_eq!(
        outcomes(&ran.result),
        [
            ("success".into(), "success".into()),
            ("failure".into(), "failure".into()),
            ("skipped".into(), "skipped".into()),
        ]
    );
    assert_eq!(ran.result["exit_code"], 3);
}

#[test]
fn a_step_that_may_fail_does_not_end_the_task() {
    let ran = run(&mut job(&[
        S {
            continue_on_error: true,
            ..s("exit 4")
        },
        s("echo after"),
    ]));
    assert_eq!(ran.code, Some(0), "{}", ran.stderr);
    assert!(
        ran.stderr
            .contains("::dagron-ci::step::1::end::4\n::dagron-ci::step::1::continued\n"),
        "{}",
        ran.stderr
    );
    assert_eq!(ran.stdout, "after\n");
    assert_eq!(
        outcomes(&ran.result)[0],
        ("failure".to_string(), "success".to_string())
    );
}

#[test]
fn set_e_holds_and_each_step_has_its_own_variables_and_directory() {
    let dir = tempfile::tempdir().unwrap();
    let ran = run(job(&[
        S {
            env: vec![("GREETING", "hi $(touch /tmp/x) `id`".into())],
            ..s("printf '%s\\n' \"$GREETING\"; cd /; export LEAK=1; false; echo unreachable")
        },
        s("echo \"${GREETING:-unset} ${LEAK:-unset} $(pwd)\""),
    ])
    .current_dir(dir.path()));
    assert_eq!(ran.code, Some(1), "{}", ran.stderr);
    assert_eq!(
        ran.stdout, "hi $(touch /tmp/x) `id`\n",
        "a value is never parsed, and set -e ends the step"
    );

    let ran = run(job(&[
        S {
            env: vec![("GREETING", "hi".into())],
            ..s("cd /; export LEAK=1")
        },
        s("echo \"${GREETING:-unset} ${LEAK:-unset} $(pwd)\""),
    ])
    .current_dir(dir.path()));
    let here = dir.path().canonicalize().unwrap();
    assert_eq!(
        ran.stdout,
        format!("unset unset {}\n", here.display()),
        "a step's variables, exports and directory are its own"
    );
}

#[test]
fn a_step_past_its_timeout_is_stopped_with_124() {
    let started = Instant::now();
    let ran = run(&mut job(&[
        S {
            timeout: Some(1),
            ..s("sleep 30")
        },
        s("echo never"),
    ]));
    assert_eq!(ran.code, Some(124), "{}", ran.stderr);
    assert!(started.elapsed() < Duration::from_secs(5));
    assert_eq!(
        outcomes(&ran.result)[0],
        ("timed_out".to_string(), "timed_out".to_string())
    );
    assert_eq!(ran.result["steps"][0]["timeout_secs"], 1);
    assert_eq!(ran.stdout, "");
}

#[test]
fn a_step_that_ignores_sigterm_is_killed_after_the_grace_with_137() {
    let started = Instant::now();
    let ran = run(job(&[S {
        timeout: Some(1),
        // sleep inherits the ignored SIGTERM.
        ..s("trap '' TERM; sleep 30")
    }])
    .env("CI_STEP_GRACE_SECS", "1"));
    assert_eq!(
        ran.code,
        Some(137),
        "timeout(1)'s code for a killed step: {}",
        ran.stderr
    );
    assert!(started.elapsed() < Duration::from_secs(6));
    assert_eq!(ran.result["steps"][0]["signal"], libc::SIGKILL);
    assert_eq!(ran.result["steps"][0]["outcome"], "timed_out");
}

#[test]
fn a_step_that_may_time_out_does_not_end_the_task() {
    let ran = run(&mut job(&[
        S {
            timeout: Some(1),
            continue_on_error: true,
            ..s("sleep 30")
        },
        s("echo after"),
    ]));
    assert_eq!(ran.code, Some(0), "{}", ran.stderr);
    assert_eq!(ran.stdout, "after\n");
    assert_eq!(
        outcomes(&ran.result)[0],
        ("timed_out".to_string(), "success".to_string())
    );
}

/// A cancelled task's signal reaches the running step, which may clean up;
/// no further step starts, and the task exits 128 plus the signal.
#[test]
fn a_cancelled_task_stops_its_step_and_starts_no_other() {
    let mut child = job(&[
        s("trap 'echo cleaned-up; exit 9' TERM; echo ready; while true; do sleep 0.05 || true; done"),
        s("echo never"),
    ])
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()
    .unwrap();
    let mut stdout = BufReader::new(child.stdout.take().unwrap());
    let mut stderr = child.stderr.take().unwrap();
    let stderr = std::thread::spawn(move || {
        let mut text = Vec::new();
        stderr.read_to_end(&mut text).unwrap();
        text
    });
    let mut line = String::new();
    stdout.read_line(&mut line).unwrap();
    assert_eq!(line, "ready\n");
    let started = Instant::now();
    // SAFETY: plain integers.
    unsafe { libc::kill(child.id() as i32, libc::SIGTERM) };
    // Bounded: a runner that does not pass the signal on would wait forever.
    let status = loop {
        if let Some(status) = child.try_wait().unwrap() {
            break status;
        }
        if started.elapsed() > Duration::from_secs(10) {
            child.kill().unwrap();
            child.wait().unwrap();
            panic!("the cancelled task did not end");
        }
        std::thread::sleep(Duration::from_millis(20));
    };
    assert!(started.elapsed() < Duration::from_secs(5));
    let mut rest = String::new();
    stdout.read_to_string(&mut rest).unwrap();
    let ran = finish(std::process::Output {
        status,
        stdout: Vec::new(),
        stderr: stderr.join().unwrap(),
    });
    assert_eq!(ran.code, Some(128 + libc::SIGTERM), "{}", ran.stderr);
    assert_eq!(
        rest, "cleaned-up\n",
        "the step got the signal and cleaned up"
    );
    assert!(
        ran.stderr
            .ends_with("::dagron-ci::step::1::end::9\n::dagron-ci::cancelled::15\n"),
        "{}",
        ran.stderr
    );
    assert_eq!(
        outcomes(&ran.result),
        [
            ("cancelled".into(), "cancelled".into()),
            ("skipped".into(), "skipped".into()),
        ]
    );
    assert_eq!(ran.result["cancelled_by"], libc::SIGTERM);
}

/// What a step leaves running lives on through the later steps (a service
/// they use), and is killed when the task ends. It holds the task's output
/// open, as a real one does: the output, which an executor reads to its end,
/// must end with the task, not with the leftover.
#[cfg(target_os = "linux")]
#[test]
fn what_a_step_leaves_running_is_killed_when_the_task_ends() {
    let dir = tempfile::tempdir().unwrap();
    let pidfile = dir.path().join("pid");
    let env = vec![("PIDFILE", pidfile.display().to_string())];
    let child = job(&[
        S {
            env: env.clone(),
            ..s("sleep 300 & echo $! > \"$PIDFILE\"")
        },
        S {
            env,
            ..s("kill -0 \"$(cat \"$PIDFILE\")\" && echo still-running")
        },
    ])
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()
    .unwrap();
    let (tx, rx) = std::sync::mpsc::channel();
    std::thread::spawn(move || {
        let _ = tx.send(child.wait_with_output());
    });
    let out = rx.recv_timeout(Duration::from_secs(10));
    let pid = read_pid(&pidfile);
    let Ok(out) = out else {
        // SAFETY: plain integers.
        unsafe { libc::kill(pid, libc::SIGKILL) };
        panic!("the task's output did not end: sleep {pid}, left running, holds it");
    };
    let ran = finish(out.unwrap());
    assert_eq!(ran.code, Some(0), "{}", ran.stderr);
    assert_eq!(
        ran.stdout, "still-running\n",
        "it ran on through the next step"
    );
    assert!(gone_within(pid, 3), "sleep {pid} outlived its task");
}

#[cfg(target_os = "linux")]
#[test]
fn the_running_step_dies_with_the_runner() {
    let dir = tempfile::tempdir().unwrap();
    let pidfile = dir.path().join("pid");
    let mut child = job(&[S {
        env: vec![("PIDFILE", pidfile.display().to_string())],
        ..s("echo $$ > \"$PIDFILE\"; exec sleep 300")
    }])
    .stdout(Stdio::null())
    .stderr(Stdio::null())
    .spawn()
    .unwrap();
    let pid = read_pid(&pidfile);
    std::thread::sleep(Duration::from_millis(50));
    child.kill().unwrap();
    child.wait().unwrap();
    assert!(
        gone_within(pid, 3),
        "the step {pid} outlived the runner killed with SIGKILL"
    );
}

/// A step reads no input: what the runner was given on stdin is not its.
#[test]
fn a_step_reads_no_input() {
    let mut child = job(&[s("cat; echo done")])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    child
        .stdin
        .take()
        .unwrap()
        .write_all(b"input meant for nobody\n")
        .unwrap();
    let ran = finish(child.wait_with_output().unwrap());
    assert_eq!(ran.code, Some(0), "{}", ran.stderr);
    assert_eq!(ran.stdout, "done\n");
}

#[test]
fn the_result_is_written_where_asked_as_well_as_logged() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("deep/er/result.json");
    let ran = run(job(&[s("true")]).env("CI_RESULT_FILE", &path));
    assert_eq!(ran.code, Some(0));
    let written: Value = serde_json::from_str(&std::fs::read_to_string(&path).unwrap()).unwrap();
    assert_eq!(written, ran.result);
    let names: Vec<String> = std::fs::read_dir(path.parent().unwrap())
        .unwrap()
        .map(|e| e.unwrap().file_name().to_string_lossy().into_owned())
        .collect();
    assert_eq!(names, ["result.json"], "nothing partial is left beside it");
}

#[test]
fn an_environment_dagron_ci_did_not_write_is_refused() {
    let dir = tempfile::tempdir().unwrap();
    let marker = dir.path().join("ran");
    let touch = format!("touch {}", marker.display());
    let cases: [(&str, Option<&str>, &str); 3] = [
        ("CI_STEP_PROTOCOL", None, "CI_STEP_PROTOCOL is not set"),
        ("CI_STEP_PROTOCOL", Some("6"), "speaks 1, 2, 3, 4 and 5"),
        ("CI_STEP_COUNT", Some("2"), "CI_STEP_2_RUN is not set"),
    ];
    for (var, value, why) in cases {
        let mut cmd = job(&[s(&touch)]);
        match value {
            Some(v) => cmd.env(var, v),
            None => cmd.env_remove(var),
        };
        let ran = run(&mut cmd);
        assert_eq!(ran.code, Some(125), "{var}={value:?}");
        assert!(ran.stderr.contains(why), "{var}={value:?}: {}", ran.stderr);
        assert!(!marker.exists(), "{var}={value:?}: a step ran");
    }
}

#[test]
fn it_speaks_its_own_protocol_and_no_other() {
    let code = |args: &[&str]| Command::new(BIN).args(args).status().unwrap().code();
    assert_eq!(code(&["--speaks", "1"]), Some(0));
    assert_eq!(code(&["--speaks", "2"]), Some(0));
    assert_eq!(code(&["--speaks", "3"]), Some(0));
    assert_eq!(code(&["--speaks", "4"]), Some(0));
    assert_eq!(code(&["--speaks", "5"]), Some(0));
    assert_eq!(code(&["--speaks", "6"]), Some(1));
    assert_eq!(code(&["--speaks", "one"]), Some(1));
    assert_eq!(code(&["--speaks"]), Some(125));
    let version = Command::new(BIN).arg("--version").output().unwrap();
    assert!(String::from_utf8_lossy(&version.stdout).contains("(step protocols 1, 2, 3, 4 and 5)"));
}

/// `--with node` installs the Node.js beside the binary too, links as
/// links, or fails where there is none; and nothing else is taken.
#[test]
fn install_with_node_copies_the_node_beside_it_or_fails() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let bin = dir.path().join("bin");
    let out = Command::new(BIN)
        .args(["install"])
        .arg(&bin)
        .args(["--with", "node"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(125));
    assert!(
        String::from_utf8_lossy(&out.stderr).contains("this image holds no Node.js to install"),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let out = Command::new(BIN)
        .args(["install"])
        .arg(&bin)
        .args(["--with", "python"])
        .output()
        .unwrap();
    assert_eq!(out.status.code(), Some(125));
    assert!(String::from_utf8_lossy(&out.stderr).contains("install takes --with node"));

    // An image with one: the binary, and a node directory beside it.
    let image = dir.path().join("image");
    std::fs::create_dir_all(image.join("node/bin")).unwrap();
    std::fs::create_dir_all(image.join("node/lib")).unwrap();
    std::fs::copy(BIN, image.join("dagron-ci-exec")).unwrap();
    std::fs::write(image.join("node/bin/node"), "#!/bin/sh\necho node\n").unwrap();
    std::fs::set_permissions(
        image.join("node/bin/node"),
        std::fs::Permissions::from_mode(0o755),
    )
    .unwrap();
    std::fs::write(image.join("node/lib/ld-x.so.1"), "loader").unwrap();
    std::os::unix::fs::symlink("ld-x.so.1", image.join("node/lib/libc.x.so.1")).unwrap();
    // The binary was just copied: exec(2) can find it "busy" while a fork made by
    // another test in this process still holds the copy's write end open.
    let mut install = Command::new(image.join("dagron-ci-exec"));
    install.args(["install"]).arg(&bin).args(["--with", "node"]);
    let mut out = install.output();
    for _ in 0..100 {
        match out {
            Err(ref e) if e.kind() == std::io::ErrorKind::ExecutableFileBusy => {
                std::thread::sleep(Duration::from_millis(20));
                out = install.output();
            }
            _ => break,
        }
    }
    let out = out.unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let node = bin.join("node/bin/node");
    assert_eq!(
        std::fs::metadata(&node).unwrap().permissions().mode() & 0o777,
        0o755
    );
    assert_eq!(
        std::fs::read_link(bin.join("node/lib/libc.x.so.1")).unwrap(),
        std::path::Path::new("ld-x.so.1"),
        "a link is copied as a link"
    );
    assert!(!bin.join(".node.partial").exists());
    // Installed again, over what was there, it is the same.
    let again = Command::new(image.join("dagron-ci-exec"))
        .args(["install"])
        .arg(&bin)
        .args(["--with", "node"])
        .output()
        .unwrap();
    assert!(
        again.status.success(),
        "{}",
        String::from_utf8_lossy(&again.stderr)
    );
    assert!(node.is_file());
}

#[test]
fn install_puts_a_working_copy_where_asked() {
    use std::os::unix::fs::PermissionsExt;

    let dir = tempfile::tempdir().unwrap();
    let bin = dir.path().join("dagron/bin");
    let out = Command::new(BIN).arg("install").arg(&bin).output().unwrap();
    assert!(
        out.status.success(),
        "{}",
        String::from_utf8_lossy(&out.stderr)
    );
    let installed = bin.join("dagron-ci-exec");
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        installed.display().to_string()
    );
    let mode = std::fs::metadata(&installed).unwrap().permissions().mode();
    assert_eq!(mode & 0o777, 0o755);
    assert!(Command::new(&installed)
        .args(["--speaks", "1"])
        .status()
        .unwrap()
        .success());
}

#[test]
fn a_step_whose_shell_cannot_start_fails_with_127() {
    let ran = run(job(&[s("true"), s("true")]).env("PATH", "/nonexistent"));
    assert_eq!(ran.code, Some(127), "{}", ran.stderr);
    assert!(
        ran.stderr
            .contains("::dagron-ci::warning::step 1 could not start sh"),
        "{}",
        ran.stderr
    );
    assert_eq!(ran.result["steps"][1]["outcome"], "skipped");
}

/// The lines of the runner's own that begin with `kind`, each without it.
fn said<'a>(ran: &'a Ran, kind: &str) -> Vec<&'a str> {
    let prefix = format!("::dagron-ci::{kind}::");
    ran.stderr
        .lines()
        .filter_map(|line| line.strip_prefix(prefix.as_str()))
        .collect()
}

/// A step may leave anything where its summary's file was. A pipe there is
/// not waited on once the steps are over: the job ends as its steps did,
/// and says no summary.
#[test]
fn a_pipe_where_the_summary_was_holds_up_nothing() {
    let mut child = job(&[s(
        "rm -f \"$CI_STEP_SUMMARY\" && mkfifo \"$CI_STEP_SUMMARY\"",
    )])
    .stdout(Stdio::piped())
    .stderr(Stdio::piped())
    .spawn()
    .unwrap();
    let deadline = Instant::now() + Duration::from_secs(20);
    while child.try_wait().unwrap().is_none() {
        if Instant::now() > deadline {
            let _ = child.kill();
            panic!("the job waited on the pipe where its summary was");
        }
        std::thread::sleep(Duration::from_millis(50));
    }
    let ran = finish(child.wait_with_output().unwrap());
    assert_eq!(ran.code, Some(0), "{}", ran.stderr);
    assert!(said(&ran, "summary").is_empty(), "{}", ran.stderr);
}

#[test]
fn a_job_says_its_report_and_its_steps_summary_before_its_result() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("junit.xml"),
        r#"<testsuite>
  <testcase classname="t" name="ok" file="src/a.rs" line="2"/>
  <testcase classname="t" name="bad" file="src/a.rs" line="7"><failure message="left != right"/></testcase>
</testsuite>"#,
    )
    .unwrap();
    // The second step fails: the report is read, and the summary said,
    // whatever became of the steps.
    let ran = run(job(&[
        s("echo '## Built' >> \"$CI_STEP_SUMMARY\""),
        s("echo 'one of two failed' >> \"$CI_STEP_SUMMARY\"; exit 3"),
    ])
    .current_dir(dir.path())
    .env("CI_REPORT_JUNIT", "junit.xml\nmissing.xml"));
    assert_eq!(ran.code, Some(3));

    let report: Value = serde_json::from_str(said(&ran, "report")[0]).unwrap();
    assert_eq!(
        (report["tests"].as_u64(), report["failures"].as_u64()),
        (Some(2), Some(1))
    );
    let problem = report["problems"][0].as_str().unwrap();
    assert!(
        problem.starts_with("missing.xml could not be read"),
        "{problem}"
    );
    let annotations = said(&ran, "annotation");
    assert_eq!(annotations.len(), 1);
    assert_eq!(
        serde_json::from_str::<Value>(annotations[0]).unwrap(),
        serde_json::json!({
            "path": "src/a.rs", "start_line": 7, "end_line": 7,
            "level": "failure", "title": "t.bad", "message": "left != right",
        })
    );
    assert_eq!(
        serde_json::from_str::<Value>(said(&ran, "failed")[0]).unwrap(),
        serde_json::json!({ "name": "t.bad", "message": "left != right" })
    );
    let summary: String = said(&ran, "summary")
        .iter()
        .map(|part| serde_json::from_str::<String>(part).unwrap())
        .collect();
    assert_eq!(summary, "## Built\none of two failed");
    // The result is still the runner's last line: this test's own reading
    // of a job takes it from there, and found it.
    assert_eq!(ran.result["exit_code"], 3);

    // A job told of no report, whose steps wrote no summary, says neither.
    let ran = run(&mut job(&[s("true")]));
    assert!(said(&ran, "report").is_empty() && said(&ran, "summary").is_empty());
}

/// A GitLab job (step protocol 5) that may fail, and fails, ends as
/// succeeded and says it failed; one that may not ends as it failed. One
/// whose artifacts cannot be fetched runs no step.
#[test]
fn a_gitlab_job_that_may_fail_ends_as_succeeded_and_says_so() {
    let gitlab = |allowed: bool| {
        let mut cmd = job(&[s("exit 3")]);
        cmd.env("CI_STEP_PROTOCOL", "5");
        if allowed {
            cmd.env("CI_JOB_ALLOW_FAILURE", "true");
        }
        cmd
    };
    let ran = run(&mut gitlab(true));
    assert_eq!(ran.code, Some(0), "{}", ran.stderr);
    assert!(
        ran.stderr
            .contains("::dagron-ci::warning::the job failed (exit 3), and may: allow_failure"),
        "{}",
        ran.stderr
    );
    assert_eq!(ran.result["exit_code"], 0);
    let ran = run(&mut gitlab(false));
    assert_eq!(ran.code, Some(3), "{}", ran.stderr);

    // One that may fail, too: its artifacts are not its own failure.
    let mut fetching = job(&[s("echo ran")]);
    fetching
        .env("CI_STEP_PROTOCOL", "5")
        .env("CI_JOB_ALLOW_FAILURE", "true")
        .env("CI_ARTIFACT_DOWNLOAD", "build")
        .env_remove("ACTIONS_RESULTS_URL")
        .env_remove("ACTIONS_RUNTIME_TOKEN");
    let ran = run(&mut fetching);
    assert_eq!(ran.code, Some(1), "{}", ran.stderr);
    assert!(
        ran.stderr.contains("could not be fetched"),
        "{}",
        ran.stderr
    );
    assert!(!ran.stdout.contains("ran"), "no step ran: {}", ran.stdout);
}
