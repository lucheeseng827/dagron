//! A GitHub workflow's job, from its file to its steps' output: read and
//! compiled by dagron-ci's own front-end, and run by this binary under step
//! protocol 2, with a real shell. What a step is promised by GitHub's
//! runner is held here: its `if:` against how the steps before went, the
//! contexts its expressions read, the files it writes for the steps after
//! it, its shell and its directory.

#![cfg(unix)]

use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::process::{Command, Stdio};
use std::time::Duration;

use dagron_ci::pipeline::compile::{self, Event, Outcome};
use dagron_ci::pipeline::github;
use serde_json::{json, Value};

const BIN: &str = env!("CARGO_BIN_EXE_dagron-ci-exec");

/// `text` with each `{{ name }}` that is a parameter's filled in: once, from
/// the start, what was filled in not read again, and anything else left as
/// it is. As dagron fills a task's text before it starts the task.
fn filled(text: &str, parameters: &BTreeMap<String, String>) -> String {
    let mut out = String::new();
    let mut rest = text;
    while let Some(open) = rest.find("{{") {
        let Some(close) = rest[open + 2..].find("}}") else {
            break;
        };
        let end = open + 2 + close + 2;
        out.push_str(&rest[..open]);
        match parameters.get(rest[open + 2..end - 2].trim()) {
            Some(value) => out.push_str(value),
            None => out.push_str(&rest[open..end]),
        }
        rest = &rest[end..];
    }
    out + rest
}

/// The environment dagron-ci compiles `job` of `workflow` to, for a push to
/// main, with the run's parameters filled in: what the engine would start
/// the job's container with.
fn compiled(workflow: &str, job: &str) -> BTreeMap<String, String> {
    let settings = github::Settings {
        runs_on: BTreeMap::from([("ubuntu-latest".to_string(), "ubuntu:24.04".to_string())]),
        ..Default::default()
    };
    let event = Event {
        kind: "push".into(),
        git_ref: "refs/heads/main".into(),
        sha: "c0ffee".repeat(6) + "c0ff",
        trust: "trusted".into(),
        repo: "acme/widgets".into(),
        forge_host: "github.com".into(),
        ..Default::default()
    };
    let lowered = github::parse(workflow, &settings.provided)
        .unwrap_or_else(|problems| panic!("{problems:#?}"))
        .lower("ci", &event, &settings)
        .unwrap_or_else(|problems| panic!("{problems:#?}"));
    let Outcome::Run(compiled) =
        compile::compile(&lowered.pipeline, &event, &Default::default()).unwrap()
    else {
        panic!("the workflow does not run for a push");
    };
    let workflow: Value = serde_yaml::from_str(&compiled.yaml).unwrap();
    // The workflow's own parameters, with the event's over them.
    let mut parameters: BTreeMap<String, String> = workflow["parameters"]
        .as_object()
        .unwrap()
        .iter()
        .map(|(name, value)| (name.clone(), value.as_str().unwrap().to_string()))
        .collect();
    parameters.extend(compiled.parameters.clone());
    let task = workflow["tasks"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["name"] == job)
        .unwrap_or_else(|| panic!("no job {job} in {}", compiled.yaml));
    task["env"]
        .as_array()
        .unwrap()
        .iter()
        // A secret's reference is the engine's to fill as the job is
        // dispatched: a test gives the job its value, where it reads one.
        .filter(|e| e.get("value_from").is_none())
        .map(|e| {
            (
                e["name"].as_str().unwrap().to_string(),
                filled(e["value"].as_str().unwrap(), &parameters),
            )
        })
        .collect()
}

/// What a job printed, told apart: under step protocol 3 it is one stream.
struct Ran {
    code: Option<i32>,
    /// What its steps printed, to either of their streams.
    stdout: String,
    /// The runner's own lines, but for the two it ends with.
    stderr: String,
    /// Its result line, the last but one.
    result: Value,
    /// Its last line: what it says to the jobs after it.
    said: Value,
}

impl Ran {
    /// `outcome/conclusion` of each step, in order.
    fn steps(&self) -> Vec<String> {
        self.result["steps"]
            .as_array()
            .unwrap()
            .iter()
            .map(|s| {
                format!(
                    "{}/{}",
                    s["outcome"].as_str().unwrap(),
                    s["conclusion"].as_str().unwrap()
                )
            })
            .collect()
    }
}

fn command(env: &BTreeMap<String, String>, dir: &std::path::Path) -> Command {
    let mut cmd = Command::new(BIN);
    cmd.env_clear()
        .env("PATH", std::env::var("PATH").unwrap())
        .env("TMPDIR", dir)
        .envs(env)
        .current_dir(dir)
        .stdin(Stdio::null());
    cmd
}

/// [`run_in`], where `env`'s own `PATH`, when it has one, is the job's,
/// and no step has a shell from it: a job's lines are then its markers.
fn run_in_with_path(env: &BTreeMap<String, String>, dir: &std::path::Path) -> Ran {
    let out = command(env, dir).output().unwrap();
    let all = String::from_utf8_lossy(&out.stdout).into_owned();
    Ran {
        code: out.status.code(),
        stdout: String::new(),
        stderr: all,
        result: Value::Null,
        said: Value::Null,
    }
}

fn run(env: &BTreeMap<String, String>) -> Ran {
    let dir = tempfile::tempdir().unwrap();
    run_in(env, dir.path())
}

/// [`run`], in a workspace that already holds what the job reads.
fn run_in(env: &BTreeMap<String, String>, dir: &std::path::Path) -> Ran {
    let out = command(env, dir).output().unwrap();
    assert!(
        out.stderr.is_empty(),
        "a job writes to one stream: {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let all = String::from_utf8_lossy(&out.stdout).into_owned();
    let mut lines: Vec<&str> = all.lines().collect();
    let mut ending = |with: &str| -> Value {
        lines
            .pop()
            .and_then(|line| line.strip_prefix(with))
            .map(|json| serde_json::from_str(json).unwrap())
            .unwrap_or_else(|| panic!("no {with} line where one ends {all}"))
    };
    let said = ending("::dagron-ci::job::");
    let result = ending("::dagron-ci::result::");
    let (own, steps): (Vec<&str>, Vec<&str>) = lines
        .into_iter()
        .partition(|line| line.starts_with("::dagron-ci::"));
    let joined = |lines: Vec<&str>| lines.iter().map(|line| format!("{line}\n")).collect();
    Ran {
        code: out.status.code(),
        stdout: joined(steps),
        stderr: joined(own),
        result,
        said,
    }
}

const WORKFLOW: &str = r#"
on: push
env:
  GREETING: hello
jobs:
  build:
    runs-on: ubuntu-latest
    strategy:
      matrix:
        crate: [core]
    env:
      LEVEL: job
    steps:
      - uses: actions/checkout@v4
      - id: first
        name: First ${{ matrix.crate }}
        run: |
          echo "$GREETING from ${{ matrix.crate }} at ${{ github.sha }}, level $LEVEL"
          echo "branch is ${{ github.ref_name }}"
          echo 'a template is text: {{ ci_sha }} {{ci_ref}} {{.Names}}{{end}}'
          echo "ADDED=by-first" >> "$GITHUB_ENV"
          echo "answer=42" >> "$GITHUB_OUTPUT"
          { echo "many<<EOF"; echo "a"; echo "b"; echo "EOF"; } >> "$GITHUB_OUTPUT"
          mkdir -p tools && printf '#!/bin/sh\necho the tool ran\n' > tools/mytool && chmod +x tools/mytool
          echo "$PWD/tools" >> "$GITHUB_PATH"
          echo "$GITHUB_ACTIONS $CI $GITHUB_REPOSITORY $GITHUB_REF_NAME $GITHUB_JOB $RUNNER_OS"
      - name: Reads what the first one set
        env:
          ANSWER: ${{ steps.first.outputs.answer }}
          MANY: ${{ steps.first.outputs.many }}
          BOTH: ${{ env.ADDED }}-${{ env.GREETING }}-${{ env.LEVEL }}
          LEVEL: step
        run: |
          echo "ADDED=$ADDED ANSWER=$ANSWER BOTH=$BOTH LEVEL=$LEVEL"
          echo "$MANY" | tr '\n' '+'; echo
          mytool
          echo "outcome=${{ steps.first.outcome }} status=${{ job.status }} os=${{ runner.os }}"
      - name: In a directory, with sh
        shell: sh
        working-directory: tools
        run: ls
      - name: Skipped by its if
        if: matrix.crate == 'other'
        run: echo never
      - id: broke
        name: Fails
        run: |
          echo "about to fail"
          false
          echo "not reached: the shell stops at the first failure"
      - name: Not run after a failure
        run: echo never
      - name: Runs after a failure
        if: failure() && steps.first.outputs.answer == '42'
        run: echo "cleaning up, status=${{ job.status }}, broke=${{ steps.broke.outcome }}"
      - name: Always
        if: ${{ always() }}
        shell: sh -u {0}
        run: echo "always, with a shell of its own"
"#;

#[test]
fn a_job_s_steps_run_as_github_s_runner_would_run_them() {
    let env = compiled(WORKFLOW, "build.core");
    assert_eq!(env["CI_STEP_PROTOCOL"], "3");
    assert_eq!(
        env["CI_STEP_COUNT"], "8",
        "the checkout is the workspace's own, and no step"
    );
    let ran = run(&env);
    assert_eq!(
        ran.steps(),
        [
            "success/success",
            "success/success",
            "success/success",
            "skipped/skipped",
            "failure/failure",
            "skipped/skipped",
            "success/success",
            "success/success",
        ],
        "{}\n{}",
        ran.stdout,
        ran.stderr
    );
    assert_eq!(
        ran.code,
        Some(1),
        "the job fails with its failing step's code"
    );
    assert_eq!(ran.result["protocol"], 3);
    let sha = "c0ffee".repeat(6) + "c0ff";
    let lines: Vec<&str> = ran.stdout.lines().collect();
    assert_eq!(
        lines,
        [
            // An expression no outsider chooses is pasted, and a branch's
            // name is a variable's value.
            &format!("hello from core at {sha}, level job"),
            "branch is main",
            // What dagron would fill in a native pipeline's script is a
            // workflow file's own text, and is printed as it is written.
            "a template is text: {{ ci_sha }} {{ci_ref}} {{.Names}}{{end}}",
            "true true acme/widgets main build Linux",
            // What the first step wrote, read by the second: a variable, an
            // output with one line and one with two, a directory on PATH.
            // Its own variable is over the job's.
            "ADDED=by-first ANSWER=42 BOTH=by-first-hello-step LEVEL=step",
            "a+b+",
            "the tool ran",
            "outcome=success status=success os=Linux",
            "mytool",
            "about to fail",
            "cleaning up, status=failure, broke=failure",
            "always, with a shell of its own",
        ],
        "{}",
        ran.stderr
    );
    // A step's name is evaluated, and the markers are the log's as before.
    for marker in [
        "::dagron-ci::step::1::start::First core",
        "::dagron-ci::step::1::end::0",
        "::dagron-ci::step::5::start::Fails",
        "::dagron-ci::step::5::end::1",
        "::dagron-ci::step::8::end::0",
    ] {
        assert!(
            ran.stderr.lines().any(|l| l == marker),
            "{marker} not in {}",
            ran.stderr
        );
    }
    assert!(
        !ran.stderr.contains("step::4::start") && !ran.stderr.contains("step::6::start"),
        "a step that is skipped starts nothing"
    );
}

#[test]
fn what_a_step_gets_wrong_fails_that_step_and_says_what() {
    let workflow = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - name: May fail
        continue-on-error: true
        run: exit 7
      - name: Runs, since that one may fail
        run: echo "status=${{ job.status }}"
      - name: Writes what is no variable
        continue-on-error: true
        run: echo "this is not a variable" >> "$GITHUB_ENV"
      - name: Sets the runner's own
        run: |
          echo "GITHUB_SHA=forged" >> "$GITHUB_ENV"
          echo "MINE=kept" >> "$GITHUB_ENV"
      - name: Reads them
        run: echo "sha=$GITHUB_SHA mine=$MINE"
      - name: Nowhere
        continue-on-error: true
        working-directory: no/such/place
        run: echo never
      - name: A function called wrong
        continue-on-error: true
        run: echo "${{ hashFiles() }}"
      - name: Last
        run: echo last
"#;
    let ran = run(&compiled(workflow, "build"));
    assert_eq!(ran.code, Some(0), "{}", ran.stderr);
    assert_eq!(
        ran.steps(),
        [
            "failure/success",
            "success/success",
            "failure/success",
            "success/success",
            "success/success",
            "failure/success",
            "failure/success",
            "success/success",
        ],
        "{}",
        ran.stderr
    );
    let sha = "c0ffee".repeat(6) + "c0ff";
    assert_eq!(
        ran.stdout.lines().collect::<Vec<_>>(),
        ["status=success", &format!("sha={sha} mine=kept"), "last"]
    );
    for said in [
        "::dagron-ci::step::1::continued",
        "::dagron-ci::error::step 3: GITHUB_ENV: \"this is not a variable\" is neither NAME=value nor NAME<<DELIMITER",
        "::dagron-ci::warning::step 4 may not set GITHUB_SHA: it is the runner's own",
        "::dagron-ci::error::step 6: its working-directory",
        "::dagron-ci::error::step 7: its script: hashFiles: names no files",
    ] {
        assert!(ran.stderr.contains(said), "{said} not in {}", ran.stderr);
    }
}

#[test]
fn a_cancelled_job_starts_no_further_step_whatever_its_if_says() {
    let workflow = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - name: Long
        run: |
          echo started
          sleep 60
      - name: Always
        if: always()
        run: echo never
"#;
    let dir = tempfile::tempdir().unwrap();
    let mut child = command(&compiled(workflow, "build"), dir.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .unwrap();
    // One stream: the runner's own lines, and what the step printed.
    let mut reader = BufReader::new(child.stdout.take().unwrap());
    let mut seen: Vec<String> = Vec::new();
    while seen.last().map(String::as_str) != Some("started") {
        let mut line = String::new();
        assert!(
            reader.read_line(&mut line).unwrap() > 0,
            "it ended before its step said anything: {seen:?}"
        );
        seen.push(line.trim_end().to_string());
    }
    // SAFETY: kill takes plain integers.
    unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGTERM);
    }
    let started = std::time::Instant::now();
    let mut rest = String::new();
    std::io::Read::read_to_string(&mut reader, &mut rest).unwrap();
    let status = child.wait().unwrap();
    assert!(started.elapsed() < Duration::from_secs(30));
    assert_eq!(status.code(), Some(143));
    seen.extend(rest.lines().map(str::to_string));
    let all = seen.join("\n");
    assert!(all.contains("::dagron-ci::cancelled::15"), "{all}");
    assert!(!all.contains("step::2::start"), "{all}");
    let result: Value = seen
        .iter()
        .rev()
        .find_map(|line| line.strip_prefix("::dagron-ci::result::"))
        .map(|json| serde_json::from_str(json).unwrap())
        .unwrap();
    assert_eq!(result["steps"][0]["outcome"], "cancelled");
    assert_eq!(result["steps"][1]["outcome"], "skipped");
    // And it says so to the jobs after it, last.
    assert_eq!(
        seen.last().map(String::as_str),
        Some("::dagron-ci::job::{\"outputs\":{},\"result\":\"cancelled\"}")
    );
    // Nothing of the job's own directory is left under the one it was given.
    let left: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.file_name().to_string_lossy().into_owned())
        .filter(|name| name.starts_with("dagron-ci-"))
        .collect();
    assert!(left.is_empty(), "{left:?}");
}

const HANDS: &str = r#"
on: push
jobs:
  detect:
    runs-on: ubuntu-latest
    outputs:
      changed: ${{ steps.find.outputs.changed }}
      list: ${{ steps.find.outputs.list }}
      none: ${{ steps.nobody.outputs.x }}
    steps:
      - id: find
        run: |
          echo "changed=true" >> "$GITHUB_OUTPUT"
          echo 'list=["a","b"]' >> "$GITHUB_OUTPUT"
          echo "said to its error stream" >&2
  build:
    needs: detect
    if: needs.detect.outputs.changed == 'true'
    runs-on: ubuntu-latest
    steps:
      - run: echo "first of ${{ fromJSON(needs.detect.outputs.list)[0] }}, detect ${{ needs.detect.result }}"
      - if: needs.detect.outputs.changed != 'true'
        run: echo never
  after:
    needs: [detect, build]
    runs-on: ubuntu-latest
    steps:
      - run: echo after
"#;

/// `env`, with what the engine gives a job of the tasks it needs, in their
/// order: each one's status, and the last line it printed.
fn given(mut env: BTreeMap<String, String>, needs: &[(&str, &str)]) -> BTreeMap<String, String> {
    for (k, (status, said)) in needs.iter().enumerate() {
        env.insert(format!("CI_NEED_{}_STATUS", k + 1), status.to_string());
        env.insert(format!("CI_NEED_{}_SAID", k + 1), said.to_string());
    }
    env
}

#[test]
fn a_job_reads_the_jobs_it_needs_and_says_its_own_word_last() {
    // A job says what it came to, and its outputs, on the last line of the
    // one stream it writes, its steps' error stream being part of it.
    let detect = run(&compiled(HANDS, "detect"));
    assert_eq!(detect.code, Some(0), "{}", detect.stderr);
    assert_eq!(
        detect.said,
        serde_json::json!({
            "result": "success",
            "outputs": { "changed": "true", "list": "[\"a\",\"b\"]", "none": "" },
        })
    );
    assert_eq!(detect.stdout, "said to its error stream\n");
    let said = format!("::dagron-ci::job::{}", detect.said);
    let skipped = "::dagron-ci::job::{\"outputs\":{},\"result\":\"skipped\"}";

    // The job that needs it reads its result and outputs, in its `if:` and
    // in its steps.
    let build = run(&given(compiled(HANDS, "build"), &[("succeeded", &said)]));
    assert_eq!(build.code, Some(0), "{}", build.stderr);
    assert_eq!(build.stdout, "first of a, detect success\n");
    assert_eq!(build.steps(), ["success/success", "skipped/skipped"]);
    assert_eq!(build.said["result"], "success");

    // Where its `if:` is false it runs no step, and says so. That is no
    // failure: it exits 0.
    let unchanged =
        "::dagron-ci::job::{\"outputs\":{\"changed\":\"false\"},\"result\":\"success\"}";
    let build = run(&given(
        compiled(HANDS, "build"),
        &[("succeeded", unchanged)],
    ));
    assert_eq!(build.code, Some(0), "{}", build.stderr);
    assert_eq!(build.stdout, "");
    assert_eq!(build.steps(), ["skipped/skipped", "skipped/skipped"]);
    assert_eq!(
        build.said.to_string(),
        &skipped["::dagron-ci::job::".len()..]
    );
    assert!(
        build
            .stderr
            .lines()
            .any(|l| l == "::dagron-ci::job::skipped"),
        "{}",
        build.stderr
    );
    // And where the job it needs failed, and said nothing.
    let build = run(&given(
        compiled(HANDS, "build"),
        &[("failed", "a line of its log")],
    ));
    assert_eq!(
        (build.code, build.said["result"].as_str()),
        (Some(0), Some("skipped"))
    );

    // A job with no `if:` runs where every job it needs succeeded. One of
    // them skipped is not that, as on GitHub, and it is skipped with it.
    let after = |build: (&str, &str)| {
        run(&given(
            compiled(HANDS, "after"),
            &[("succeeded", &said), build],
        ))
    };
    let ran = after((
        "succeeded",
        "::dagron-ci::job::{\"outputs\":{},\"result\":\"success\"}",
    ));
    assert_eq!(
        (ran.stdout.as_str(), ran.said["result"].as_str()),
        ("after\n", Some("success"))
    );
    let ran = after(("succeeded", skipped));
    assert_eq!(
        (ran.stdout.as_str(), ran.said["result"].as_str()),
        ("", Some("skipped"))
    );
    let ran = after(("failed", ""));
    assert_eq!(
        (ran.code, ran.said["result"].as_str()),
        (Some(0), Some("skipped"))
    );
}

const SUMMARISED: &str = r###"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - name: first
        run: |
          echo "discarded" >> "$GITHUB_STEP_SUMMARY"
          echo "## Built" > "$GITHUB_STEP_SUMMARY"
      - name: quiet
        run: "true"
      - name: last
        run: echo "3 tests passed" >> "$GITHUB_STEP_SUMMARY"
"###;

/// Each step has a summary file of its own, as on GitHub's runner: a step
/// that writes its file anew loses its own lines and no other step's. What
/// they hold when the job is over is said, in the steps' order.
#[test]
fn what_steps_write_to_their_summaries_is_said_when_the_job_is_over() {
    let ran = run(&compiled(SUMMARISED, "build"));
    assert_eq!(ran.code, Some(0), "{}", ran.stderr);
    let summary: String = ran
        .stderr
        .lines()
        .filter_map(|line| line.strip_prefix("::dagron-ci::summary::"))
        .map(|part| serde_json::from_str::<String>(part).unwrap())
        .collect();
    assert_eq!(summary, "## Built\n\n3 tests passed");
}

const USES_LOCAL: &str = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - id: before
        run: echo "before"
      - id: greet
        uses: ./.github/actions/greet
        env:
          OUTER: from-the-step
        with:
          who: ${{ github.ref_name }}
          extra: 1
      - name: Reads the action's outputs
        run: |
          echo "said=${{ steps.greet.outputs.said }} outcome=${{ steps.greet.outcome }} env=$FROM_ACTION"
          p="${{ steps.greet.outputs.path }}"; echo "path ends ${p##*/}"
      - id: broke
        uses: ./.github/actions/fails
        continue-on-error: true
      - run: echo "broke=${{ steps.broke.outcome }}/${{ steps.broke.conclusion }} status=${{ job.status }}"
      - uses: ./.github/actions/docker
        continue-on-error: true
      - uses: ./.github/actions/missing
        continue-on-error: true
"#;

/// The actions [`USES_LOCAL`] uses, each a file of its workspace.
const ACTIONS: &[(&str, &str)] = &[
    (
        "greet/action.yml",
        r#"
name: Greet
inputs:
  who:
    required: true
  greeting:
    default: hello
  where:
    default: ${{ github.repository }}
outputs:
  said:
    value: ${{ steps.say.outputs.said }}
  path:
    value: ${{ github.action_path }}
runs:
  using: composite
  steps:
    - id: say
      shell: bash
      env:
        WHO: ${{ inputs.who }}
      run: |
        echo "${{ inputs.greeting }}, $WHO, in ${{ inputs.where }}"
        echo "said=${{ inputs.greeting }} $WHO" >> "$GITHUB_OUTPUT"
        echo "FROM_ACTION=set" >> "$GITHUB_ENV"
        echo "action path ends ${GITHUB_ACTION_PATH##*/}, step env $OUTER"
    - name: Sees its own steps
      shell: sh
      run: echo "own steps ${{ steps.say.outcome }}, outer '${{ steps.before.outcome }}'"
    - uses: ./.github/actions/inner
      with:
        n: 2
    - shell: bash
      if: failure()
      run: echo never
"#,
    ),
    (
        "inner/action.yaml",
        r#"
inputs:
  n: {}
runs:
  using: composite
  steps:
    - shell: bash
      run: echo "inner got ${{ inputs.n }}"
"#,
    ),
    (
        "fails/action.yml",
        r#"
runs:
  using: composite
  steps:
    - shell: bash
      run: |
        echo "failing in an action"
        exit 3
    - shell: bash
      run: echo "not after a failure"
    - shell: bash
      if: failure()
      run: echo "the action cleans up"
"#,
    ),
    (
        "docker/action.yml",
        "runs:\n  using: docker\n  image: Dockerfile\n",
    ),
];

/// A step that uses a local action runs its steps in its place, as GitHub's
/// runner does: the action is read from the workspace as the step starts,
/// its inputs are its steps' context, its steps' are their own, and its
/// outputs are the step's.
#[test]
fn a_local_action_s_steps_run_in_the_place_of_the_step_that_uses_it() {
    let env = compiled(USES_LOCAL, "build");
    assert_eq!(env["CI_STEP_PROTOCOL"], "4");
    assert_eq!(env["CI_STEP_2_USES"], "./.github/actions/greet");
    assert_eq!(
        env["CI_STEP_2_RUN"], "",
        "a step that uses an action has no script"
    );
    let with: Vec<(&str, &str)> = (1..=2)
        .map(|k| {
            (
                env[&format!("CI_STEP_2_WITH_{k}_NAME")].as_str(),
                env[&format!("CI_STEP_2_WITH_{k}_VALUE")].as_str(),
            )
        })
        .collect();
    assert_eq!(with, [("extra", "1"), ("who", "${{ github.ref_name }}")]);

    let dir = tempfile::tempdir().unwrap();
    for (file, text) in ACTIONS {
        let path = dir.path().join(".github/actions").join(file);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }
    let ran = run_in(&env, dir.path());
    assert_eq!(
        ran.steps(),
        [
            "success/success",
            "success/success",
            "success/success",
            "failure/success",
            "success/success",
            "failure/success",
            "failure/success",
        ],
        "{}\n{}",
        ran.stdout,
        ran.stderr
    );
    assert_eq!(ran.code, Some(0), "{}", ran.stderr);
    let (named, printed): (Vec<&str>, Vec<&str>) =
        ran.stdout.lines().partition(|line| line.starts_with("-- "));
    assert_eq!(
        printed,
        [
            "before",
            // The input it was given, read as the step reads its own, and
            // the defaults of those it was not.
            "hello, main, in acme/widgets",
            "action path ends greet, step env from-the-step",
            // Its steps' `steps` are their own.
            "own steps success, outer ''",
            // An action it uses, with what it gives that one.
            "inner got 2",
            // Its outputs are the step's, and what it set is the job's.
            "said=hello main outcome=success env=set",
            "path ends greet",
            // A step of it that fails fails the action: the rest of it runs
            // only where its if asks how the action has gone.
            "failing in an action",
            "the action cleans up",
            "broke=failure/success status=success",
        ],
        "{}",
        ran.stderr
    );
    // Its steps are lines of the step that uses it, and no steps of the
    // job's: they have no markers of their own. A step with no name is
    // named by its script, evaluated, as GitHub names it.
    assert_eq!(
        named,
        [
            "-- Run echo \"hello, $WHO, in acme/widgets\"",
            "-- Sees its own steps",
            "-- Run ./.github/actions/inner",
            "-- Run echo \"inner got 2\"",
            "-- Run echo \"failing in an action\"",
            "-- Run echo \"the action cleans up\"",
        ]
    );
    for (marker, why) in [
        (
            "::dagron-ci::step::2::start::Run ./.github/actions/greet",
            "the step starts, named by its action as GitHub names it",
        ),
        ("::dagron-ci::step::2::end::0", "and ends, once"),
        (
            "::dagron-ci::step::4::end::3",
            "with its failing step's code",
        ),
        ("::dagron-ci::step::4::continued", "and may fail"),
    ] {
        assert!(
            ran.stderr.lines().any(|l| l == marker),
            "{why}: {marker} not in {}",
            ran.stderr
        );
    }
    assert_eq!(
        ran.stderr.matches("::dagron-ci::step::2::start::").count(),
        1,
        "{}",
        ran.stderr
    );
    for said in [
        "::dagron-ci::warning::step 2: ./.github/actions/greet declares no input extra",
        "::dagron-ci::error::step 6: ./.github/actions/docker: action.yml: its runs.image is \
         \"Dockerfile\", which is neither an image a registry has (docker://image) nor a \
         Dockerfile of its directory",
        "::dagron-ci::error::step 7: ./.github/actions/missing: there is no action.yml",
    ] {
        assert!(ran.stderr.contains(said), "{said} not in {}", ran.stderr);
    }
}

/// An action of another repository, which the executor fetched before the
/// job started into the directory `DAGRON_ACTIONS_DIR` names, runs as a
/// local one does, from its own directory of that repository, and is told
/// which it is. One that was not fetched, one whose file leads out of what
/// was fetched, and one whose step uses another repository's action fail
/// their step, and say why.
#[test]
fn a_fetched_action_of_another_repository_runs_from_where_it_was_fetched() {
    let workflow = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - id: hello
        uses: octo/hello/sub@v1
        with:
          who: rig
      - run: echo "said ${{ steps.hello.outputs.said }}"
      - uses: octo/hello@releases/v2
        continue-on-error: true
      - uses: octo/missing@v1
        continue-on-error: true
      - uses: octo/leaky@v1
        continue-on-error: true
      - uses: octo/nested@v1
        continue-on-error: true
      - uses: octo/outer@v1
"#;
    let actions = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(
        outside.path().join("action.yml"),
        "runs:\n  using: composite\n  steps:\n    - shell: sh\n      run: echo outside\n",
    )
    .unwrap();
    let put = |dir: &str, text: &str| {
        let at = actions.path().join(dir);
        std::fs::create_dir_all(&at).unwrap();
        std::fs::write(at.join("action.yml"), text).unwrap();
    };
    put(
        "octo/hello/v1/sub",
        r#"
inputs:
  who: {}
outputs:
  said:
    value: ${{ steps.say.outputs.said }}
runs:
  using: composite
  steps:
    - id: say
      shell: sh
      run: |
        echo "hello ${{ inputs.who }} from $GITHUB_ACTION_REPOSITORY@$GITHUB_ACTION_REF, ${GITHUB_ACTION_PATH#"$DAGRON_ACTIONS_DIR"/}"
        echo "said=hello" >> "$GITHUB_OUTPUT"
"#,
    );
    put(
        "octo/hello/releases%2Fv2",
        "runs:\n  using: composite\n  steps:\n    - shell: sh\n      run: echo \"the ref with a slash\"\n",
    );
    std::fs::create_dir_all(actions.path().join("octo/leaky/v1")).unwrap();
    std::os::unix::fs::symlink(
        outside.path().join("action.yml"),
        actions.path().join("octo/leaky/v1/action.yml"),
    )
    .unwrap();
    put(
        "octo/nested/v1",
        "runs:\n  using: composite\n  steps:\n    - uses: actions/setup-node@v4\n",
    );
    // An action whose step uses one of another repository that was fetched
    // with it: run as a workflow's own step's.
    put(
        "octo/outer/v1",
        "runs:\n  using: composite\n  steps:\n    - uses: octo/hello@releases/v2\n",
    );

    let mut env = compiled(workflow, "build");
    env.insert(
        "DAGRON_ACTIONS_DIR".into(),
        actions.path().display().to_string(),
    );
    let workspace = tempfile::tempdir().unwrap();
    let ran = run_in(&env, workspace.path());
    assert_eq!(
        ran.steps(),
        [
            "success/success",
            "success/success",
            "success/success",
            "failure/success",
            "failure/success",
            "failure/success",
            "success/success",
        ],
        "{}\n{}",
        ran.stdout,
        ran.stderr
    );
    let printed: Vec<&str> = ran
        .stdout
        .lines()
        .filter(|line| !line.starts_with("-- "))
        .collect();
    assert_eq!(
        printed,
        [
            "hello rig from octo/hello@v1, octo/hello/v1/sub",
            "said hello",
            "the ref with a slash",
            "the ref with a slash",
        ]
    );
    for said in [
        "octo/missing@v1 was not fetched before the job started",
        "octo/leaky@v1: action.yml leads out of the repository the action was fetched from",
        "actions/setup-node@v4 was not fetched before the job started: dagron-ci fetches the actions an action uses",
    ] {
        assert!(ran.stderr.contains(said), "{said} not in {}", ran.stderr);
    }
}

/// Whether this machine has a Node.js on `PATH`, which a JavaScript
/// action's test runs on: the step runner has none installed beside it
/// here.
fn node_on_path() -> bool {
    std::env::var_os("PATH")
        .is_some_and(|path| std::env::split_paths(&path).any(|dir| dir.join("node").is_file()))
}

/// A JavaScript action runs its scripts on Node.js, as GitHub's runner
/// does: `pre`, then `main` with each input as `INPUT_<NAME>` (its default
/// where it was not given one) and what `pre` kept as `STATE_<name>`; what
/// `main` writes to `GITHUB_OUTPUT` is the step's outputs; and `post`,
/// with what `main` kept, when the job's steps are over, the last action's
/// first, where its `post-if` holds. A `main` that fails fails its step,
/// and its `post` runs all the same. Skips where no Node.js is on PATH.
#[test]
fn a_javascript_action_runs_its_scripts_on_node() {
    if !node_on_path() {
        eprintln!("no node on PATH - skipping");
        return;
    }
    let workflow = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - id: js
        uses: ./js
        with:
          who: rig
      - run: echo "said ${{ steps.js.outputs.said }}"
      - uses: ./failing
        continue-on-error: true
      - uses: ./outside
        continue-on-error: true
      - run: echo "the steps are over"
"#;
    let dir = tempfile::tempdir().unwrap();
    let put = |file: &str, text: &str| {
        let path = dir.path().join(file);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    };
    put(
        "js/action.yml",
        "inputs:\n  who: {}\n  greeting: { default: hi }\noutputs:\n  said: { description: what it said }\nruns:\n  using: node20\n  pre: pre.js\n  main: index.js\n  post: post.js\n",
    );
    put(
        "js/pre.js",
        "require('fs').appendFileSync(process.env.GITHUB_STATE, 'from=pre\\n'); console.log('pre ran');\n",
    );
    put(
        "js/index.js",
        r#"const fs = require('fs');
const e = process.env;
console.log(`${e.INPUT_GREETING} ${e.INPUT_WHO}, kept by ${e.STATE_from}, as ${e.GITHUB_ACTION}, event ${fs.existsSync(e.GITHUB_EVENT_PATH)}`);
fs.appendFileSync(e.GITHUB_OUTPUT, `said=${e.INPUT_GREETING} ${e.INPUT_WHO}\n`);
fs.appendFileSync(e.GITHUB_STATE, `greeted=${e.INPUT_WHO}\n`);
"#,
    );
    put(
        "js/post.js",
        "console.log(`post: ${process.env.STATE_greeted} was greeted, from is ${process.env.STATE_from || 'gone'}`);\n",
    );
    put(
        "failing/action.yml",
        "runs:\n  using: node24\n  main: main.js\n  post: post.js\n",
    );
    put(
        "failing/main.js",
        "console.log('failing'); process.exit(3);\n",
    );
    put(
        "failing/post.js",
        "console.log('the failing one cleans up');\n",
    );
    put(
        "outside/action.yml",
        "runs:\n  using: node20\n  main: /etc/hostname\n",
    );

    let ran = run_in(&compiled(workflow, "build"), dir.path());
    assert_eq!(
        ran.steps(),
        [
            "success/success",
            "success/success",
            "failure/success",
            "failure/success",
            "success/success",
        ],
        "{}\n{}",
        ran.stdout,
        ran.stderr
    );
    assert_eq!(ran.code, Some(0), "{}", ran.stderr);
    let printed: Vec<&str> = ran
        .stdout
        .lines()
        .filter(|line| !line.starts_with("-- "))
        .collect();
    assert_eq!(
        printed,
        [
            "pre ran",
            "hi rig, kept by pre, as js, event true",
            "said hi rig",
            "failing",
            "the steps are over",
            // The posts, the last action's first.
            "the failing one cleans up",
            "post: rig was greeted, from is gone",
        ],
        "{}",
        ran.stderr
    );
    assert!(
        ran.stderr.contains("::dagron-ci::step::3::end::3"),
        "a main that fails fails its step with its code: {}",
        ran.stderr
    );
    assert!(
        ran.stderr.contains(
            "./outside: action.yml: its script /etc/hostname is no file of the workspace"
        ),
        "{}",
        ran.stderr
    );

    // A post that fails fails the job, and says so.
    put("js/post.js", "process.exit(5);\n");
    let ran = run_in(&compiled(workflow, "build"), dir.path());
    assert_eq!(ran.code, Some(5), "{}", ran.stderr);
    assert!(
        ran.stderr.contains("error::step 1: Post Run ./js exited 5"),
        "{}",
        ran.stderr
    );
    assert_eq!(ran.said["result"], "failure");

    // With no Node.js to run it on, the step fails, and says why.
    let mut env = compiled(workflow, "build");
    env.insert("PATH".into(), "/no/such/dir".into());
    let ran = run_in_with_path(&env, dir.path());
    assert!(
        ran.stderr
            .contains("./js is a JavaScript action, and this job has no Node.js to run it on"),
        "{}",
        ran.stderr
    );
}

/// A job given an OIDC identity by the engine has GitHub's two variables
/// for it, the URL with a query of its own (`@actions/core` adds
/// `&audience=`); a job given none has neither. No step moves them for the
/// steps after it.
#[test]
fn a_job_with_an_identity_has_github_s_variables_for_it() {
    let workflow = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: |
          echo "url=$ACTIONS_ID_TOKEN_REQUEST_URL token=$ACTIONS_ID_TOKEN_REQUEST_TOKEN"
          echo "ACTIONS_ID_TOKEN_REQUEST_URL=https://elsewhere.example/steal?" >> "$GITHUB_ENV"
      - run: echo "still $ACTIONS_ID_TOKEN_REQUEST_URL"
"#;
    let mut env = compiled(workflow, "build");
    env.insert(
        "DAGRON_OIDC_REQUEST_URL".into(),
        "http://oidc.example:8088/oidc/token".into(),
    );
    env.insert("DAGRON_OIDC_REQUEST_TOKEN".into(), "dgo_abc".into());
    let ran = run(&env);
    assert_eq!(
        ran.stdout.lines().collect::<Vec<_>>(),
        [
            // The request token is the job's credential: masked, as on
            // GitHub. `***` is printed only where it is the token.
            "url=http://oidc.example:8088/oidc/token?api-version=2.0 token=***",
            "still http://oidc.example:8088/oidc/token?api-version=2.0",
        ],
        "{}",
        ran.stderr
    );
    assert!(
        ran.stderr
            .contains("may not set ACTIONS_ID_TOKEN_REQUEST_URL: it is the runner's own"),
        "{}",
        ran.stderr
    );
    // None given: neither is set.
    let ran = run(&compiled(workflow, "build"));
    assert!(ran.stdout.starts_with("url= token=\n"), "{}", ran.stdout);
}

/// `hashFiles` hashes the job's workspace's files its patterns match, as a
/// cache's key is made of them: in a step's script, its `env:`, its `if:`
/// and an action's `with:`; read against the workspace whatever the step's
/// `working-directory`; with a pattern under `github.workspace`; and empty
/// where none match, which an `if:` skips on.
#[test]
fn hash_files_is_of_the_workspace_s_files_its_patterns_match() {
    use sha2::{Digest, Sha256};
    let workflow = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - env:
          KEY: deps-${{ hashFiles('**/Cargo.lock', '!vendor') }}
        run: echo "key=$KEY"
      - run: echo "none=[${{ hashFiles('**/*.nothing') }}]"
      - if: hashFiles('**/Cargo.lock') != ''
        run: echo "locked"
      - if: hashFiles('**/*.nothing') != ''
        run: echo "never"
      - uses: ./.github/actions/key
        with:
          key: deps-${{ hashFiles('**/Cargo.lock', '!vendor') }}
      - working-directory: crates/a
        run: echo "root=${{ hashFiles('Cargo.lock') }}"
      - run: echo "under=${{ hashFiles(format('{0}/Cargo.lock', github.workspace)) }}"
"#;
    let action = r#"
name: key
inputs:
  key:
    required: true
runs:
  using: composite
  steps:
    - shell: sh
      run: echo "with=${{ inputs.key }}"
"#;
    let dir = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(dir.path().join("crates/a")).unwrap();
    std::fs::create_dir_all(dir.path().join("vendor/x")).unwrap();
    std::fs::create_dir_all(dir.path().join(".github/actions/key")).unwrap();
    std::fs::write(dir.path().join(".github/actions/key/action.yml"), action).unwrap();
    std::fs::write(dir.path().join("Cargo.lock"), "root").unwrap();
    std::fs::write(dir.path().join("crates/a/Cargo.lock"), "a").unwrap();
    std::fs::write(dir.path().join("vendor/x/Cargo.lock"), "vendored").unwrap();
    // The job's workspace is where it starts.
    let ran = run_in(&compiled(workflow, "build"), dir.path());
    let of = |texts: &[&str]| -> String {
        let mut outer = Sha256::new();
        for text in texts {
            outer.update(Sha256::digest(text.as_bytes()));
        }
        outer
            .finalize()
            .iter()
            .map(|b| format!("{b:02x}"))
            .collect()
    };
    let deps = of(&["root", "a"]);
    let root = of(&["root"]);
    // The runner's own line before an action's step is not the step's.
    assert_eq!(
        ran.stdout
            .lines()
            .filter(|l| !l.starts_with("-- Run "))
            .collect::<Vec<_>>(),
        [
            format!("key=deps-{deps}"),
            "none=[]".to_string(),
            "locked".to_string(),
            format!("with=deps-{deps}"),
            format!("root={root}"),
            format!("under={root}"),
        ],
        "{}",
        ran.stderr
    );
}

/// A job the engine gave a `GITHUB_TOKEN` reads it as GitHub's runner's
/// does: `secrets.GITHUB_TOKEN` and `github.token`, each a variable of the
/// script and never its text; and no step sets it for the steps after it.
#[test]
fn a_job_s_token_is_read_as_secrets_github_token_and_github_token() {
    let workflow = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - env:
          FROM_SECRETS: ${{ secrets.GITHUB_TOKEN }}
        run: |
          echo "secrets=$FROM_SECRETS github=${{ github.token }}"
          echo "GITHUB_TOKEN=forged" >> "$GITHUB_ENV"
      - run: echo "still $GITHUB_TOKEN"
"#;
    let mut env = compiled(workflow, "build");
    env.insert("GITHUB_TOKEN".into(), "ghs_job".into());
    assert!(
        !env["CI_STEP_1_RUN"].contains("github.token"),
        "an expression an outsider may choose is a variable: {}",
        env["CI_STEP_1_RUN"]
    );
    let ran = run(&env);
    // The token is the job's credential: masked wherever it is printed, as
    // on GitHub. `***` is printed only where it is the token, and "still
    // ***" is the token, not the forged value.
    assert_eq!(
        ran.stdout.lines().collect::<Vec<_>>(),
        ["secrets=*** github=***", "still ***"],
        "{}",
        ran.stderr
    );
    assert!(
        ran.stderr.contains("may not set GITHUB_TOKEN"),
        "{}",
        ran.stderr
    );
    // None given: both read as empty.
    let ran = run(&compiled(workflow, "build"));
    assert!(
        ran.stdout.starts_with("secrets= github=\n"),
        "{}",
        ran.stdout
    );
}

/// A job's own lines are its protocol, whatever its steps mask: a value as
/// short as `1` masks no part of the job's word, and an output that holds
/// a masked value is not handed on, as on GitHub. A process a step left
/// running, in a session of its own, prints after the steps are over: none
/// of it is printed, the job's word stays its last line, and the job does
/// not wait on it.
#[test]
fn the_job_s_word_is_its_own_whatever_is_masked_or_left_running() {
    let workflow = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    outputs:
      kept: ${{ steps.say.outputs.kept }}
      hidden: ${{ steps.say.outputs.hidden }}
    steps:
      - id: say
        run: |
          echo "::add-mask::1"
          echo "::add-mask::hunter2"
          echo "kept=plain" >> "$GITHUB_OUTPUT"
          echo "hidden=pw is hunter2" >> "$GITHUB_OUTPUT"
          setsid sh -c 'sleep 1; echo late from a leftover' &
          echo "step 1 done"
"#;
    let started = std::time::Instant::now();
    let ran = run(&compiled(workflow, "build"));
    let took = started.elapsed();
    assert_eq!(ran.code, Some(0), "{}", ran.stderr);
    assert_eq!(
        ran.said,
        serde_json::json!({ "result": "success", "outputs": { "kept": "plain" } }),
        "{}",
        ran.stderr
    );
    assert!(
        ran.stderr
            .contains("the output hidden holds a masked value: it is not handed on"),
        "{}",
        ran.stderr
    );
    assert_eq!(ran.result["exit_code"], 0, "the result's JSON is JSON");
    // The leftover's line, where the job took more than its second, is
    // among the steps' lines, and never after the job's word.
    let lines: Vec<&str> = ran
        .stdout
        .lines()
        .filter(|l| *l != "late from a leftover")
        .collect();
    assert_eq!(
        lines,
        ["::add-mask::***", "::add-mask::***", "step *** done"]
    );
    assert!(
        took < std::time::Duration::from_secs(4),
        "the job waited on what a step left running: {took:?}"
    );
}

/// The runner's lines are whole and in their place whatever a step prints
/// as they are written: a step's last line without its end, and a process
/// of a step's printing all the while, cut none of them and are none of
/// them.
#[test]
fn the_runner_s_lines_are_whole_whatever_a_step_prints_beside_them() {
    let workflow = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    outputs:
      n: ${{ steps.last.outputs.n }}
    steps:
      - run: |
          ( i=0; while [ $i -lt 3000 ]; do printf 'noise '; i=$((i+1)); done; echo ) &
          printf 'one'
      - run: printf 'two'
      - run: printf 'three'
      - id: last
        run: echo "n=3" >> "$GITHUB_OUTPUT"
"#;
    let ran = run(&compiled(workflow, "build"));
    assert_eq!(ran.code, Some(0), "{}", ran.stderr);
    assert_eq!(
        ran.said,
        serde_json::json!({ "result": "success", "outputs": { "n": "3" } }),
        "{}",
        ran.stderr
    );
    for n in 1..=4 {
        assert!(
            ran.stderr
                .lines()
                .any(|l| l == format!("::dagron-ci::step::{n}::end::0")),
            "step {n}'s end: {}",
            ran.stderr
        );
    }
    let all = format!("{}{}", ran.stdout, ran.stderr);
    assert!(!all.contains('\u{1e}'), "no mark is printed: {all}");
    for word in ["one", "two", "three"] {
        assert!(ran.stdout.contains(word), "{word}: {}", ran.stdout);
    }
}

/// Each of the runner's lines is after every line of the steps' before it,
/// a step's last line without its end among them, and before every line
/// of the steps' after it.
#[test]
fn the_runner_s_lines_are_in_their_place_among_the_steps() {
    let workflow = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: printf one
      - run: echo two
"#;
    let dir = tempfile::tempdir().unwrap();
    let out = command(&compiled(workflow, "build"), dir.path())
        .output()
        .unwrap();
    let all = String::from_utf8_lossy(&out.stdout).into_owned();
    let at = |what: &dyn Fn(&str) -> bool| {
        all.lines()
            .position(what)
            .unwrap_or_else(|| panic!("not printed: {all}"))
    };
    let order = [
        at(&|l| l == "one"),
        at(&|l| l == "::dagron-ci::step::1::end::0"),
        at(&|l| l.starts_with("::dagron-ci::step::2::start::")),
        at(&|l| l == "two"),
        at(&|l| l == "::dagron-ci::step::2::end::0"),
        at(&|l| l.starts_with("::dagron-ci::result::")),
    ];
    assert!(order.windows(2).all(|w| w[0] < w[1]), "{order:?}: {all}");
}

/// A step that reads the job's stream itself (as its user may, by
/// `/proc/self/fd/1`) takes lines of the steps' from it, and nothing of
/// the runner's: the job ends, and says what it did.
#[test]
fn a_step_that_reads_the_job_s_stream_takes_no_line_of_the_runner_s() {
    let workflow = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    outputs:
      n: ${{ steps.last.outputs.n }}
    steps:
      - run: |
          timeout 3 cat < /proc/self/fd/1 > /dev/null &
          echo "taken, maybe"
      - run: echo two
      - id: last
        run: echo "n=3" >> "$GITHUB_OUTPUT"
"#;
    let started = std::time::Instant::now();
    let ran = run(&compiled(workflow, "build"));
    assert_eq!(ran.code, Some(0), "{}", ran.stderr);
    assert_eq!(
        ran.said,
        serde_json::json!({ "result": "success", "outputs": { "n": "3" } }),
        "{}",
        ran.stderr
    );
    for n in 1..=3 {
        assert!(
            ran.stderr
                .lines()
                .any(|l| l == format!("::dagron-ci::step::{n}::end::0")),
            "step {n}'s end: {}",
            ran.stderr
        );
    }
    assert!(started.elapsed() < std::time::Duration::from_secs(10));
}

/// A secret of the repository's the engine gave the job (`CI_SECRET_NAME`)
/// is what a step reads as `secrets.NAME`, in its script, `env:` and
/// `with:`, and is masked wherever it is printed; one it was not given is
/// empty; and no step forges one for the steps after it.
#[test]
fn a_repository_s_secret_is_read_by_its_name_and_masked() {
    let workflow = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - env:
          FROM_ENV: ${{ secrets.NPM_TOKEN }}
        run: |
          echo "script=${{ secrets.NPM_TOKEN }} env=$FROM_ENV missing=[${{ secrets.NOT_GIVEN }}]"
          echo "tail=${FROM_ENV#npm_}"
          echo "CI_SECRET_NPM_TOKEN=forged" >> "$GITHUB_ENV"
      - run: echo "still ${{ secrets.NPM_TOKEN }}"
      # Where the workflow does not put it, a step has none.
      - run: echo "inherited=[$CI_SECRET_NPM_TOKEN]"
"#;
    let mut env = compiled(workflow, "build");
    env.insert("CI_SECRET_NPM_TOKEN".into(), "npm_s3cret".into());
    let ran = run(&env);
    assert_eq!(
        ran.stdout.lines().collect::<Vec<_>>(),
        [
            "script=*** env=*** missing=[]",
            // A piece of it is no mask's: the value reached the step.
            "tail=s3cret",
            "still ***",
            "inherited=[]",
        ],
        "{}",
        ran.stderr
    );
    assert!(
        ran.stderr.contains("may not set CI_SECRET_NPM_TOKEN"),
        "{}",
        ran.stderr
    );
}

/// A secret a job's step gives an action (`with:`) is the action's input;
/// the action's own steps, and its inputs' defaults, read no `secrets`, as
/// on GitHub.
#[test]
fn a_secret_given_an_action_is_its_input_and_the_action_reads_none_itself() {
    let workflow = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: ./.github/actions/echo
        with:
          token: ${{ secrets.NPM_TOKEN }}
"#;
    let action = r#"
name: Echo
inputs:
  token:
    required: true
  fallback:
    default: "[${{ secrets.NPM_TOKEN }}]"
runs:
  using: composite
  steps:
    - shell: bash
      env:
        TOKEN: ${{ inputs.token }}
        OWN: "[${{ secrets.NPM_TOKEN }}]"
        FALLBACK: ${{ inputs.fallback }}
      run: echo "tail=${TOKEN#npm_} own=$OWN fallback=$FALLBACK"
"#;
    let mut env = compiled(workflow, "build");
    env.insert("CI_SECRET_NPM_TOKEN".into(), "npm_s3cret".into());
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join(".github/actions/echo/action.yml");
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(path, action).unwrap();
    let ran = run_in(&env, dir.path());
    assert_eq!(ran.code, Some(0), "{}", ran.stderr);
    let said: Vec<&str> = ran
        .stdout
        .lines()
        .filter(|l| l.starts_with("tail="))
        .collect();
    assert_eq!(said, ["tail=s3cret own=[] fallback=[]"], "{}", ran.stdout);
}

/// GitHub's workflow commands as a job's steps print them: a value a step
/// asks to have masked is `***` from then on, in its steps and the ones
/// after; `::stop-commands::` stops that until its token; and `::error::`
/// (or `::warning::`, `::notice::`) is followed by the annotation line
/// dagron-ci reads, with a file under the workspace read as the rest.
#[test]
fn workflow_commands_mask_and_annotate_as_github_s_runner_does() {
    let workflow = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: |
          echo "before hunter2"
          echo "::add-mask::hunter2"
          echo "after hunter2"
      - run: |
          echo "next step hunter2" >&2
          echo "::stop-commands::pause"
          echo "::add-mask::visible"
          echo "::pause::"
          echo "said visible"
      - run: |
          echo "::error file=$PWD/src/lib.rs,line=3,title=Broken::boom hunter2"
          echo "::warning::careful"
"#;
    let dir = tempfile::tempdir().unwrap();
    let ws = dir.path().canonicalize().unwrap().display().to_string();
    let ran = run_in(&compiled(workflow, "build"), dir.path());
    assert_eq!(
        ran.stdout.lines().collect::<Vec<_>>(),
        [
            "before hunter2",
            "::add-mask::***",
            "after ***",
            "next step ***",
            "::stop-commands::pause",
            "::add-mask::visible",
            "::pause::",
            "said visible",
            "::error file=__WS__/src/lib.rs,line=3,title=Broken::boom ***",
            "::warning::careful",
        ]
        .map(|line| line.replace("__WS__", &ws))
        .to_vec(),
        "{}",
        ran.stderr
    );
    let annotations: Vec<serde_json::Value> = ran
        .stderr
        .lines()
        .filter_map(|line| line.strip_prefix("::dagron-ci::annotation::"))
        .map(|json| serde_json::from_str(json).unwrap())
        .collect();
    assert_eq!(annotations.len(), 2, "{}", ran.stderr);
    assert_eq!(annotations[0]["path"], "src/lib.rs");
    assert_eq!(annotations[0]["start_line"], 3);
    assert_eq!(annotations[0]["level"], "failure");
    assert_eq!(annotations[0]["title"], "Broken");
    assert_eq!(annotations[0]["message"], "boom ***");
    assert_eq!(annotations[1]["path"], ".github");
    assert_eq!(annotations[1]["level"], "warning");
}

/// An action's steps are held to what GitHub holds them to, and what one
/// gets wrong fails the step that uses it, and says what.
#[test]
fn what_an_action_gets_wrong_fails_the_step_that_uses_it() {
    let workflow = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - uses: ./a
        continue-on-error: true
      - uses: ./b
        continue-on-error: true
      - uses: ./c
        continue-on-error: true
      - uses: ./d
        continue-on-error: true
      - uses: ./e
        continue-on-error: true
      - uses: ./f
        continue-on-error: true
      - uses: ./g
      - run: echo "the job goes on"
"#;
    let dir = tempfile::tempdir().unwrap();
    // An action of a file outside the workspace, which links lead to.
    let outside = tempfile::tempdir().unwrap();
    std::fs::write(
        outside.path().join("action.yml"),
        "runs:\n  using: composite\n  steps:\n    - shell: sh\n      run: echo outside\n",
    )
    .unwrap();
    std::os::unix::fs::symlink(outside.path(), dir.path().join("e")).unwrap();
    std::fs::create_dir_all(dir.path().join("f")).unwrap();
    std::os::unix::fs::symlink(
        outside.path().join("action.yml"),
        dir.path().join("f/action.yml"),
    )
    .unwrap();
    for (name, text) in [
        // A run step with no shell.
        ("a", "runs:\n  using: composite\n  steps:\n    - run: echo hi\n"),
        // An action that uses another of someone else's.
        (
            "b",
            "runs:\n  using: composite\n  steps:\n    - uses: actions/setup-node@v4\n",
        ),
        // An input, in a script for a shell that reads no variables.
        (
            "c",
            "inputs:\n  x: {}\nruns:\n  using: composite\n  steps:\n    - shell: python\n      run: print('${{ inputs.x }}')\n",
        ),
        // An action that uses itself.
        (
            "d",
            "runs:\n  using: composite\n  steps:\n    - uses: ./d\n",
        ),
        // An action a link within the workspace leads to.
        (
            "inside",
            "runs:\n  using: composite\n  steps:\n    - shell: sh\n      run: echo inside\n",
        ),
    ] {
        std::fs::create_dir_all(dir.path().join(name)).unwrap();
        std::fs::write(dir.path().join(name).join("action.yml"), text).unwrap();
    }
    std::os::unix::fs::symlink("inside", dir.path().join("g")).unwrap();
    let ran = run_in(&compiled(workflow, "build"), dir.path());
    assert_eq!(
        ran.steps(),
        [
            "failure/success",
            "failure/success",
            "failure/success",
            "failure/success",
            "failure/success",
            "failure/success",
            "success/success",
            "success/success",
        ],
        "{}\n{}",
        ran.stdout,
        ran.stderr
    );
    for said in [
        "runs.steps[0] has no shell",
        "actions/setup-node@v4 was not fetched before the job started",
        "runs.steps[0].run holds an expression whose value an outsider may choose",
        "it uses ./d, which is more than 10 actions deep",
        "./e: action.yml leads out of the job's workspace",
        "./f: action.yml leads out of the job's workspace",
    ] {
        assert!(ran.stderr.contains(said), "{said} not in {}", ran.stderr);
    }
    let printed: Vec<&str> = ran
        .stdout
        .lines()
        .filter(|line| !line.starts_with("-- "))
        .collect();
    assert_eq!(
        printed,
        ["inside", "the job goes on"],
        "a link within the workspace is followed, and none out of it"
    );
}

const IN_CONTAINERS: &str = r#"
on: push
env:
  WORKFLOW_SAYS: from the workflow
jobs:
  build:
    runs-on: ubuntu-latest
    env:
      JOB_SAYS: from the job
    steps:
      - id: inside
        uses: docker://alpine:3.20
        with:
          entrypoint: /bin/sh
          args: -c "echo hi; echo there"
          who: rig
      - run: echo "said=${{ steps.inside.outputs.said }} env=$IMAGE_SET"
      - id: action
        uses: ./.github/actions/boxed
        with:
          who: rig
      - run: echo "the action said ${{ steps.action.outputs.said }}"
"#;

const BOXED: &str = r#"
inputs:
  who:
    default: world
runs:
  using: docker
  image: docker://alpine:3.20
  entrypoint: /bin/sh
  args: ["-c", "echo hello $0", "${{ inputs.who }}"]
  env:
    GREETING: hello ${{ inputs.who }}
"#;

/// A stand-in for the engine's results service: each step's container asked
/// for is kept, and "runs" as one that writes to its own runner's files
/// would, in the job's workspace (`dir`), where `/github/workspace` is.
fn stand_in_engine(
    dir: std::path::PathBuf,
) -> (String, std::sync::Arc<std::sync::Mutex<Vec<Value>>>) {
    use std::io::{BufRead, BufReader, Read, Write};
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let url = format!("http://{}/", listener.local_addr().unwrap());
    let asked = std::sync::Arc::new(std::sync::Mutex::new(Vec::<Value>::new()));
    let kept = asked.clone();
    std::thread::spawn(move || {
        for stream in listener.incoming() {
            let Ok(mut stream) = stream else { continue };
            let mut reader = BufReader::new(stream.try_clone().unwrap());
            let mut line = String::new();
            reader.read_line(&mut line).unwrap();
            let (method, path) = {
                let mut parts = line.split_whitespace();
                (
                    parts.next().unwrap_or("").to_string(),
                    parts.next().unwrap_or("").to_string(),
                )
            };
            let mut length = 0;
            loop {
                let mut header = String::new();
                reader.read_line(&mut header).unwrap();
                if header.trim().is_empty() {
                    break;
                }
                if let Some(v) = header.to_ascii_lowercase().strip_prefix("content-length:") {
                    length = v.trim().parse().unwrap();
                }
            }
            let mut body = vec![0; length];
            reader.read_exact(&mut body).unwrap();
            let (status, answer) = match method.as_str() {
                // An image to build: what it is built from is kept.
                "POST" if path.contains("/dagron/v1/steps/images") => {
                    let mut names: Vec<String> = tar::Archive::new(body.as_slice())
                        .entries()
                        .unwrap()
                        .map(|e| e.unwrap().path().unwrap().display().to_string())
                        .collect();
                    names.sort();
                    let n = {
                        let mut asked = kept.lock().unwrap();
                        asked.push(json!({ "built": path, "from": names }));
                        asked.len()
                    };
                    ("201 Created", format!(r#"{{"id":"b{n}"}}"#))
                }
                "POST" => {
                    let spec: Value = serde_json::from_slice(&body).unwrap();
                    let var = |name: &str| {
                        spec["env"]
                            .as_array()
                            .unwrap()
                            .iter()
                            .find(|p| p[0] == name)
                            .map(|p| p[1].as_str().unwrap().to_string())
                    };
                    let local =
                        |at: String| dir.join(at.strip_prefix("/github/workspace/").unwrap());
                    let mut out = std::fs::OpenOptions::new()
                        .append(true)
                        .open(local(var("GITHUB_OUTPUT").unwrap()))
                        .unwrap();
                    writeln!(out, "said=from-the-image").unwrap();
                    std::fs::write(local(var("GITHUB_ENV").unwrap()), "IMAGE_SET=yes\n").unwrap();
                    let n = {
                        let mut asked = kept.lock().unwrap();
                        asked.push(spec);
                        asked.len()
                    };
                    ("201 Created", format!(r#"{{"id":"s{n}"}}"#))
                }
                "GET" if path.contains("/dagron/v1/steps/b") => (
                    "200 OK",
                    r#"{"done":true,"image":"dagron-ci-action:feed","output":"built it\n"}"#
                        .to_string(),
                ),
                "GET" if path.contains("/dagron/v1/steps/s") => (
                    "200 OK",
                    r#"{"done":true,"exit_code":0,"timed_out":false,"output":"in a container\n"}"#
                        .to_string(),
                ),
                _ => ("404 Not Found", "{}".to_string()),
            };
            let _ = write!(
                stream,
                "HTTP/1.1 {status}\r\ncontent-type: application/json\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{answer}",
                answer.len()
            );
        }
    });
    (url, asked)
}

/// A step that uses an image, and an action of one, run in a container the
/// engine starts: asked for with what the step gives it, as GitHub's runner
/// gives a Docker action, and what the container wrote to its own runner's
/// files is the job's, as a script's is.
#[test]
fn a_step_in_an_image_of_its_own_is_asked_of_the_engine_and_hands_on_what_it_wrote() {
    let mut env = compiled(IN_CONTAINERS, "build");
    assert_eq!(env["CI_STEP_PROTOCOL"], "4");
    let dir = tempfile::tempdir().unwrap();
    let action = dir.path().join(".github/actions/boxed/action.yml");
    std::fs::create_dir_all(action.parent().unwrap()).unwrap();
    std::fs::write(&action, BOXED).unwrap();
    let (url, asked) = stand_in_engine(dir.path().to_path_buf());
    env.insert("ACTIONS_RESULTS_URL".into(), url);
    env.insert("ACTIONS_RUNTIME_TOKEN".into(), "a-token".into());
    let ran = run_in(&env, dir.path());
    assert_eq!(
        ran.steps(),
        [
            "success/success",
            "success/success",
            "success/success",
            "success/success"
        ],
        "{}{}",
        ran.stdout,
        ran.stderr
    );
    assert!(ran.stdout.contains("in a container\n"), "{}", ran.stdout);
    assert!(
        ran.stdout.contains("said=from-the-image env=yes"),
        "{}",
        ran.stdout
    );
    assert!(
        ran.stdout.contains("the action said from-the-image"),
        "{}",
        ran.stdout
    );

    let asked = asked.lock().unwrap();
    assert_eq!(asked.len(), 2);
    let var = |spec: &Value, name: &str| {
        spec["env"]
            .as_array()
            .unwrap()
            .iter()
            .find(|p| p[0] == name)
            .map(|p| p[1].as_str().unwrap().to_string())
    };
    // A step's `docker://`: its entrypoint and its args, split as a shell
    // splits words, and each other input of its `with:`.
    let step = &asked[0];
    assert_eq!(step["image"], "alpine:3.20");
    assert_eq!(step["entrypoint"], "/bin/sh");
    assert_eq!(step["args"], json!(["-c", "echo hi; echo there"]));
    assert_eq!(var(step, "INPUT_WHO").as_deref(), Some("rig"));
    assert_eq!(var(step, "INPUT_ARGS"), None);
    assert_eq!(
        var(step, "GITHUB_WORKSPACE").as_deref(),
        Some("/github/workspace")
    );
    assert!(var(step, "GITHUB_OUTPUT")
        .unwrap()
        .starts_with("/github/workspace/.dagron-ci-step-"));
    assert_eq!(var(step, "PATH"), None, "the image's own PATH is its own");
    assert_eq!(var(step, "GITHUB_SHA"), Some("c0ffee".repeat(6) + "c0ff"));
    // The workflow's and the job's `env:`, as a script step has them.
    assert_eq!(
        var(step, "WORKFLOW_SAYS").as_deref(),
        Some("from the workflow")
    );
    assert_eq!(var(step, "JOB_SAYS").as_deref(), Some("from the job"));
    // An action of `runs.using: docker`: its args each one argument, read
    // against its inputs, and its env likewise.
    let action = &asked[1];
    assert_eq!(action["args"], json!(["-c", "echo hello $0", "rig"]));
    assert_eq!(var(action, "GREETING").as_deref(), Some("hello rig"));
    assert_eq!(var(action, "INPUT_WHO").as_deref(), Some("rig"));
    // The steps' directories are gone after.
    let left: Vec<_> = std::fs::read_dir(dir.path())
        .unwrap()
        .filter_map(Result::ok)
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with(".dagron-ci-step-")
        })
        .collect();
    assert!(left.is_empty(), "{left:?}");
}

/// An action whose image is a Dockerfile of its directory has the engine
/// build it first, from the Dockerfile's directory, as GitHub's runner
/// builds it: the step runs in the image the engine names, and what the
/// build said is the step's.
#[test]
fn an_action_of_a_dockerfile_has_the_engine_build_its_image_first() {
    let workflow = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - id: built
        uses: ./.github/actions/built
      - run: echo "the action said ${{ steps.built.outputs.said }}"
"#;
    let mut env = compiled(workflow, "build");
    let dir = tempfile::tempdir().unwrap();
    let action = dir.path().join(".github/actions/built");
    std::fs::create_dir_all(action.join("image")).unwrap();
    std::fs::write(
        action.join("action.yml"),
        "runs:\n  using: docker\n  image: ./image/Dockerfile\n  args: [\"hi\"]\n",
    )
    .unwrap();
    std::fs::write(
        action.join("image/Dockerfile"),
        "FROM alpine:3.20\nCOPY run.sh /\n",
    )
    .unwrap();
    std::fs::write(action.join("image/run.sh"), "echo \"$1\"\n").unwrap();
    std::fs::write(action.join("README.md"), "not in the image\n").unwrap();
    let (url, asked) = stand_in_engine(dir.path().to_path_buf());
    env.insert("ACTIONS_RESULTS_URL".into(), url);
    env.insert("ACTIONS_RUNTIME_TOKEN".into(), "a-token".into());
    let ran = run_in(&env, dir.path());
    assert_eq!(
        ran.steps(),
        ["success/success", "success/success"],
        "{}{}",
        ran.stdout,
        ran.stderr
    );
    assert!(ran.stdout.contains("built it\n"), "{}", ran.stdout);
    assert!(
        ran.stdout.contains("the action said from-the-image"),
        "{}",
        ran.stdout
    );
    let asked = asked.lock().unwrap();
    assert_eq!(asked.len(), 2, "{asked:?}");
    assert!(
        asked[0]["built"]
            .as_str()
            .unwrap()
            .contains("dockerfile=Dockerfile"),
        "{asked:?}"
    );
    let from: Vec<&str> = asked[0]["from"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(Value::as_str)
        .collect();
    assert!(from.iter().any(|n| n.ends_with("Dockerfile")), "{from:?}");
    assert!(from.iter().any(|n| n.ends_with("run.sh")), "{from:?}");
    assert!(
        !from.iter().any(|n| n.ends_with("README.md")),
        "the Dockerfile's directory, not the action's: {from:?}"
    );
    assert_eq!(asked[1]["image"], "dagron-ci-action:feed");
    assert_eq!(asked[1]["args"], json!(["hi"]));
}

/// Where the job was given no results service, a step in an image of its
/// own fails, and says why.
#[test]
fn a_step_in_an_image_of_its_own_needs_the_engine_s_results_service() {
    let env = compiled(IN_CONTAINERS, "build");
    let dir = tempfile::tempdir().unwrap();
    let ran = run_in(&env, dir.path());
    assert_eq!(ran.steps()[0], "failure/failure", "{}", ran.stderr);
    assert!(
        ran.stdout
            .contains("which the engine's results service starts"),
        "{}",
        ran.stdout
    );
}

/// A job's and its workflow's `env:` that read a repository's secret are
/// read as the job starts: every step has the variable, in its script and
/// its `env` context, masked wherever it is printed.
#[test]
fn a_job_s_env_that_reads_a_secret_is_every_step_s() {
    let workflow = r#"
on: push
env:
  FROM_WORKFLOW: "w-${{ secrets.NPM_TOKEN }}"
jobs:
  build:
    runs-on: ubuntu-latest
    env:
      FROM_JOB: ${{ secrets.NPM_TOKEN }}
    steps:
      - run: |
          echo "job=[$FROM_JOB] workflow=[$FROM_WORKFLOW]"
          echo "tail=${FROM_JOB#npm_}"
      - env:
          SEEN: ${{ env.FROM_JOB }}
        run: echo "context=[$SEEN]"
"#;
    let mut env = compiled(workflow, "build");
    env.insert("CI_SECRET_NPM_TOKEN".into(), "npm_s3cret".into());
    let ran = run(&env);
    assert_eq!(ran.code, Some(0), "{}", ran.stderr);
    assert_eq!(
        ran.stdout.lines().collect::<Vec<_>>(),
        [
            "job=[***] workflow=[w-***]",
            // A piece of it is no mask's: the value reached the step.
            "tail=s3cret",
            "context=[***]",
        ],
        "{}",
        ran.stderr
    );
    // Not given the secret (a fork's run): the variable is empty.
    let ran = run(&compiled(workflow, "build"));
    assert!(
        ran.stdout.starts_with("job=[] workflow=[w-]\n"),
        "{}",
        ran.stdout
    );
}

/// A step's `::debug::` lines are printed only where `ACTIONS_STEP_DEBUG`
/// is `true` (here in the workflow's `env:`), which also sets
/// `RUNNER_DEBUG`, as on GitHub. `::group::` lines are printed as they
/// are, for whatever shows the log to fold.
#[test]
fn debug_lines_are_printed_where_step_debug_is_on() {
    let steps = r#"
    steps:
      - run: |
          echo "::group::Build"
          echo "::debug::the details"
          echo "debug=[$RUNNER_DEBUG]"
          echo "::endgroup::"
"#;
    let off = format!("on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n{steps}");
    let ran = run(&compiled(&off, "build"));
    assert_eq!(
        ran.stdout.lines().collect::<Vec<_>>(),
        ["::group::Build", "debug=[]", "::endgroup::"],
        "{}",
        ran.stderr
    );
    let on = format!(
        "on: push\nenv:\n  ACTIONS_STEP_DEBUG: true\njobs:\n  build:\n    runs-on: ubuntu-latest\n{steps}"
    );
    let ran = run(&compiled(&on, "build"));
    assert_eq!(
        ran.stdout.lines().collect::<Vec<_>>(),
        [
            "::group::Build",
            "::debug::the details",
            "debug=[1]",
            "::endgroup::"
        ],
        "{}",
        ran.stderr
    );
}

/// The run's numbers, who caused it and where the forge's API is: each a
/// `GITHUB_*` variable of the step's, from the `github` context.
#[test]
fn a_step_is_told_the_run_s_numbers_and_the_forge_s_api() {
    let workflow = r#"
on: push
jobs:
  build:
    runs-on: ubuntu-latest
    steps:
      - run: echo "$GITHUB_RUN_ATTEMPT|$GITHUB_API_URL|$GITHUB_GRAPHQL_URL|$GITHUB_HEAD_REF|${{ github.run_attempt }}"
"#;
    let ran = run(&compiled(workflow, "build"));
    assert_eq!(
        ran.stdout.lines().collect::<Vec<_>>(),
        ["1|https://api.github.com|https://api.github.com/graphql||1"],
        "{}",
        ran.stderr
    );
}
