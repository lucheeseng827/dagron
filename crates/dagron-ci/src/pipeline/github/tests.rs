use super::*;
use crate::pipeline::compile::{self, Outcome};

fn settings_of() -> Settings {
    settings(
        "github:\n  workflows: [ci.yml]\n  runs_on:\n    ubuntu-latest: docker.io/library/ubuntu:24.04\n    \
         ubuntu-24.04: docker.io/library/ubuntu:24.04\n  provided: [dtolnay/rust-toolchain, Swatinem/rust-cache]\n",
    )
    .unwrap()
}

fn push() -> Event {
    Event {
        kind: "push".into(),
        git_ref: "refs/heads/main".into(),
        sha: "a".repeat(40),
        trust: "trusted".into(),
        repo: "acme/widgets".into(),
        forge_host: "github.com".into(),
        ..Default::default()
    }
}

fn pull_request() -> Event {
    Event {
        kind: "pull_request".into(),
        git_ref: "refs/pull/7/head".into(),
        base_ref: Some("refs/heads/main".into()),
        base_sha: Some("b".repeat(40)),
        pr: Some(7),
        ..push()
    }
}

fn read(text: &str) -> Workflow {
    parse(text, &settings_of().provided).unwrap_or_else(|problems| panic!("{problems:#?}"))
}

fn lowered(text: &str, event: &Event) -> Lowered {
    read(text)
        .lower("ci", event, &settings_of())
        .unwrap_or_else(|problems| panic!("{problems:#?}"))
}

/// The problems of `text`, read and then lowered for a push.
fn problems(text: &str) -> Vec<String> {
    let found = match parse(text, &settings_of().provided) {
        Err(problems) => problems,
        Ok(workflow) => match workflow.lower("ci", &push(), &settings_of()) {
            Err(problems) => problems,
            Ok(l) => panic!("taken, but should not have been: {:?}", l.pipeline.tasks),
        },
    };
    found.iter().map(ToString::to_string).collect()
}

fn refused(text: &str, needle: &str) {
    let found = problems(text);
    assert!(
        found.iter().any(|p| p.contains(needle)),
        "{needle:?} not among {found:#?}"
    );
}

/// A workflow whose one job has `job` under it.
fn with_job(job: &str) -> String {
    let body: String = job.lines().map(|line| format!("    {line}\n")).collect();
    format!("on: push\njobs:\n  build:\n{body}")
}

const CI: &str = r#"
name: CI
on:
  push:
    branches: [main]
    paths-ignore: ["docs/**"]
  pull_request:
  workflow_dispatch:
    inputs:
      level: { type: choice, options: [debug, info], default: info }
  schedule:
    - cron: "0 3 * * *"
env:
  CARGO_TERM_COLOR: always
  COMMIT: ${{ github.sha }}
defaults:
  run:
    working-directory: crates
concurrency: ci-${{ github.ref }}
jobs:
  lint:
    runs-on: ubuntu-latest
    timeout-minutes: 10
    steps:
      - uses: actions/checkout@v4
        with: { persist-credentials: false }
      - uses: dtolnay/rust-toolchain@stable
      - name: Format
        run: cargo fmt --check
  test:
    needs: lint
    runs-on: ubuntu-latest
    strategy:
      matrix:
        crate: [core, api]
        channel: [stable]
        include:
          - crate: api
            features: postgres
    env:
      CRATE: ${{ matrix.crate }}
    services:
      db:
        image: postgres:16-alpine
        env: { POSTGRES_PASSWORD: pw }
        ports: ["5432:5432"]
        options: >-
          --health-cmd "pg_isready -h localhost"
          --health-interval 2s --health-retries 30
    steps:
      - uses: actions/checkout@v4
      - id: tested
        name: Test ${{ matrix.crate }}
        if: matrix.crate != 'skipme'
        working-directory: crates/${{ matrix.crate }}
        env:
          FEATURES: ${{ matrix.features }}
          PLAIN: "1"
        run: |
          cargo test -p ${{ matrix.crate }} --features "$FEATURES"
          echo "branch ${{ github.ref_name }} at ${{ github.sha }}"
          echo "ok=true" >> "$GITHUB_OUTPUT"
      - if: failure()
        shell: sh
        run: echo "failed on ${{ github.event.pull_request.title }}"
  report:
    needs: [lint, test]
    if: always()
    runs-on: ubuntu-latest
    continue-on-error: true
    steps:
      - run: echo done
  publish:
    needs: test
    if: github.event_name == 'push' && github.ref == 'refs/heads/main'
    container: { image: "docker.io/library/alpine:3.20", env: { TOKENLESS: "yes" } }
    steps:
      - run: echo publish
  cleanup:
    needs: publish
    if: failure()
    runs-on: [self-hosted, ubuntu-latest]
    steps:
      - run: echo cleanup
"#;

#[test]
fn a_workflow_is_read_into_the_pipeline_an_event_runs() {
    let workflow = read(CI);
    assert_eq!(workflow.name.as_deref(), Some("CI"));
    assert!(workflow.unheard.is_empty(), "{:?}", workflow.unheard);
    assert_eq!(workflow.on.schedule, ["0 3 * * *"]);
    let on = &workflow.on;
    assert!(on
        .push
        .as_ref()
        .unwrap()
        .branches
        .as_ref()
        .unwrap()
        .matches("main"));
    assert!(!on
        .push
        .as_ref()
        .unwrap()
        .paths
        .as_ref()
        .unwrap()
        .matches("docs/a.md"));
    assert!(on.pull_request.is_some() && on.merge_group.is_none());
    assert_eq!(
        on.dispatch.as_ref().unwrap().inputs["level"]
            .default
            .as_deref(),
        Some("info")
    );

    let Lowered { pipeline, skipped } = lowered(CI, &push());
    assert!(skipped.is_empty(), "{skipped:?}");
    assert_eq!(pipeline.name, "ci", "called by its file, which is one word");
    assert_eq!(
        pipeline.concurrency.as_ref().map(|c| c.key.as_str()),
        Some("ci-refs/heads/main")
    );
    let names: Vec<&str> = pipeline.tasks.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "lint",
            "test.core-stable",
            "test.api-stable-postgres",
            "report",
            "publish",
            "cleanup"
        ]
    );
    let task = |name: &str| pipeline.tasks.iter().find(|t| t.name == name).unwrap();
    let env = |t: &Task, name: &str| -> Option<String> {
        t.fields["env"]
            .as_array()
            .unwrap()
            .iter()
            .find_map(|e| (e["name"] == name).then(|| e["value"].as_str().unwrap().to_string()))
    };

    // A job with a label has the image the repository gives the label.
    let lint = task("lint");
    assert_eq!(
        lint.fields["docker_image"],
        "docker.io/library/ubuntu:24.04"
    );
    assert_eq!(lint.fields["timeout_secs"], 600);
    assert!(lint.depends_on.is_empty());
    assert_eq!(env(lint, "CARGO_TERM_COLOR").as_deref(), Some("always"));
    assert_eq!(env(lint, "COMMIT"), Some("a".repeat(40)));
    // The checkout is the workspace's own, and the toolchain's action is
    // one the repository says its image provides: one step is left.
    assert_eq!(lint.steps.len(), 1);
    assert_eq!(lint.steps[0].name.as_deref(), Some("Format"));
    assert_eq!(lint.steps[0].run, "cargo fmt --check");
    assert_eq!(
        lint.steps[0].shell.as_deref(),
        Some("auto"),
        "bash, where the image has one"
    );
    assert_eq!(lint.steps[0].working_directory.as_deref(), Some("crates"));

    // A matrix is a job for each combination, with what include adds.
    let api = task("test.api-stable-postgres");
    assert_eq!(api.depends_on, ["lint"]);
    assert_eq!(env(api, "CRATE").as_deref(), Some("api"));
    let context: serde_json::Value =
        serde_json::from_str(&env(api, "CI_CONTEXT").unwrap()).unwrap();
    assert_eq!(
        context["matrix"],
        json!({ "crate": "api", "channel": "stable", "features": "postgres" })
    );
    assert_eq!(context["github"]["job"], "test");
    assert_eq!(context["github"]["event_name"], "push");
    assert_eq!(context["strategy"]["job-total"], 2);
    assert_eq!(context["env"]["CRATE"], "api");
    let core: serde_json::Value =
        serde_json::from_str(&env(task("test.core-stable"), "CI_CONTEXT").unwrap()).unwrap();
    assert_eq!(
        core["matrix"],
        json!({ "crate": "core", "channel": "stable" })
    );
    assert_eq!(
        api.fields["services"],
        json!([{
            "name": "db", "image": "postgres:16-alpine",
            "env": [{ "name": "POSTGRES_PASSWORD", "value": "pw" }],
            "ready": "pg_isready -h localhost",
        }])
    );

    // A step is the step runner's to evaluate: its if, its directory, its
    // variables with expressions. Its script's expressions are pasted where
    // no outsider chooses their value, and are variables where one may.
    // Each is held as dagron hands it on unchanged: a `{{` of it written as
    // the parameter that holds one.
    let written = |text: &str| filled(text, &[(BRACES, "{{")]);
    let tested = &api.steps[0];
    assert_eq!(tested.id.as_deref(), Some("tested"));
    assert_eq!(
        tested.name.as_deref(),
        Some("Test ${{ ci_braces }} matrix.crate }}")
    );
    assert_eq!(
        written(tested.name.as_deref().unwrap()),
        "Test ${{ matrix.crate }}"
    );
    assert_eq!(
        tested.condition.as_deref(),
        Some("matrix.crate != 'skipme'")
    );
    assert_eq!(
        written(tested.working_directory.as_deref().unwrap()),
        "crates/${{ matrix.crate }}"
    );
    assert_eq!(tested.env.get("PLAIN").map(String::as_str), Some("1"));
    assert_eq!(
        written(&tested.late_env["FEATURES"]),
        "${{ matrix.features }}"
    );
    assert_eq!(
        written(&tested.run),
        "cargo test -p ${{ matrix.crate }} --features \"$FEATURES\"\n\
         echo \"branch ${__CI_E1} at ${{ github.sha }}\"\n\
         echo \"ok=true\" >> \"$GITHUB_OUTPUT\"\n"
    );
    assert_eq!(tested.exprs, ["github.ref_name"]);
    let failed = &api.steps[1];
    assert_eq!(failed.condition.as_deref(), Some("failure()"));
    assert_eq!(failed.shell.as_deref(), Some("sh"));
    assert_eq!(failed.run, "echo \"failed on ${__CI_E1}\"");
    assert_eq!(failed.exprs, ["github.event.pull_request.title"]);

    // always() runs whatever became of what it needs; failure() where one
    // of them failed.
    let report = task("report");
    assert_eq!(report.fields["trigger_rule"], "all_done");
    assert_eq!(report.fields["allow_failure"], true);
    assert_eq!(
        report.depends_on,
        ["lint", "test.core-stable", "test.api-stable-postgres"]
    );
    let publish = task("publish");
    assert_eq!(
        publish.fields["docker_image"],
        "docker.io/library/alpine:3.20"
    );
    assert!(publish.fields.get("trigger_rule").is_none());
    assert_eq!(env(publish, "TOKENLESS").as_deref(), Some("yes"));
    assert_eq!(task("cleanup").fields["trigger_rule"], "one_failed");
    assert_eq!(
        task("cleanup").fields["docker_image"],
        "docker.io/library/ubuntu:24.04"
    );
}

#[test]
fn a_job_whose_if_is_false_is_left_out_with_the_jobs_that_needed_it() {
    // For a pull request, `publish` is not for main: it is left out, and so
    // is `cleanup`, which runs after a failure of it and has nothing to
    // follow. `report` runs whatever became of the rest.
    let Lowered { pipeline, skipped } = lowered(CI, &pull_request());
    let names: Vec<&str> = pipeline.tasks.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(
        names,
        [
            "lint",
            "test.core-stable",
            "test.api-stable-postgres",
            "report"
        ]
    );
    assert_eq!(
        skipped,
        [
            ("publish".to_string(), "its if: is false".to_string()),
            (
                "cleanup".to_string(),
                "it runs after a failure, and no job it needs is run".to_string()
            ),
        ]
    );
    assert_eq!(
        pipeline.concurrency.unwrap().key,
        "ci-refs/pull/7/head",
        "a pull request's own line"
    );

    // A group whose newer run cancels is a key whose newer run takes the
    // older one's place. One that says no, or nothing, waits its turn.
    let job = "jobs: { a: { runs-on: ubuntu-latest, steps: [{ run: x }] } }";
    for (concurrency, cancels) in [
        ("{ group: x, cancel-in-progress: true }", true),
        ("{ group: x, cancel-in-progress: false }", false),
        ("{ group: x }", false),
        ("x", false),
    ] {
        let text = format!("on: push\nconcurrency: {concurrency}\n{job}\n");
        let Lowered { pipeline, .. } = lowered(&text, &push());
        let read = pipeline.concurrency.expect("a group");
        assert_eq!((read.key.as_str(), read.cancel_superseded), ("x", cancels), "{concurrency}");
    }

    let chain = "on: push\njobs:\n  a:\n    if: github.event_name == 'never'\n    runs-on: ubuntu-latest\n    steps: [{ run: a }]\n  \
                 b:\n    needs: a\n    runs-on: ubuntu-latest\n    steps: [{ run: b }]\n  \
                 c:\n    needs: b\n    if: always()\n    runs-on: ubuntu-latest\n    steps: [{ run: c }]\n  \
                 d:\n    needs: [b, c]\n    if: ${{ !cancelled() }}\n    runs-on: ubuntu-latest\n    steps: [{ run: d }]\n";
    let Lowered { pipeline, skipped } = lowered(chain, &push());
    assert_eq!(skipped[0].0, "a");
    assert_eq!(
        skipped[1],
        (
            "b".to_string(),
            "it needs a, which was left out".to_string()
        )
    );
    let c = &pipeline.tasks[0];
    assert_eq!(
        (c.name.as_str(), c.depends_on.len()),
        ("c", 0),
        "it runs with nothing before it"
    );
    let d = &pipeline.tasks[1];
    assert_eq!(d.depends_on, ["c"]);
    assert_eq!(d.fields["trigger_rule"], "all_done");
}

#[test]
fn the_compiled_workflow_is_one_dagron_takes() {
    for event in [push(), pull_request()] {
        let Lowered { pipeline, .. } = lowered(CI, &event);
        let compiled = match compile::compile(&pipeline, &event, &Default::default()) {
            Ok(Outcome::Run(compiled)) => compiled,
            // The push names no changed files, and the workflow has a paths filter.
            Ok(Outcome::NeedsChangedFiles) => {
                let listed = Event {
                    changed: compile::Changes::Listed(vec!["src/lib.rs".into()]),
                    ..event.clone()
                };
                match compile::compile(&pipeline, &listed, &Default::default()).unwrap() {
                    Outcome::Run(compiled) => compiled,
                    other => panic!("{other:?}"),
                }
            }
            other => panic!("{other:?}"),
        };
        let dag =
            dagron_core::dag::DagGraph::from_yaml_with_params(&compiled.yaml, &compiled.parameters)
                .unwrap_or_else(|e| panic!("{e:#}\n{}", compiled.yaml));
        let test = dag.task_spec("test.core-stable").unwrap();
        assert_eq!(test.services[0].name, "db");
        let var = |name: &str| {
            test.env
                .iter()
                .find(|e| e.name == name)
                .map(|e| e.value.as_str())
        };
        assert_eq!(
            var("CI_STEP_PROTOCOL"),
            Some("3"),
            "a GitHub step is the step runner's alone"
        );
        assert_eq!(var("CI_STEP_COUNT"), Some("2"));
        assert_eq!(var("CI_STEP_1_ID"), Some("tested"));
        assert_eq!(var("CI_STEP_1_SHELL"), Some("auto"));
        assert_eq!(var("CI_STEP_1_IF"), Some("matrix.crate != 'skipme'"));
        assert_eq!(var("CI_STEP_1_WORKDIR"), Some("crates/${{ matrix.crate }}"));
        assert_eq!(
            var("CI_STEP_1_ENVX_FEATURES"),
            Some("${{ matrix.features }}")
        );
        assert_eq!(var("CI_STEP_1_ENV_PLAIN"), Some("1"));
        assert_eq!(var("CI_STEP_1_EXPR_1"), Some("github.ref_name"));
        assert_eq!(var("CI_STEP_2_IF"), Some("failure()"));
        assert!(var("CI_CONTEXT").unwrap().contains("\"matrix\":{"));
        assert!(
            test.command[2].contains("--speaks 3") && test.command[2].contains("exit 125"),
            "no shell runs such steps in the step runner's place"
        );
        assert!(
            test.workspace.as_ref().unwrap().checkout.is_some(),
            "the event's commit"
        );
    }
}

/// A step that uses an action of another repository is handed on as it is
/// written, and the job's workspace fetches that repository, at that ref,
/// once however many steps use it: from github.com, or the operator's
/// mirror, where the operator allows it.
#[test]
fn an_action_of_another_repository_is_fetched_by_the_job_s_workspace() {
    let text = "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/checkout@v4\n      - uses: octo/hello@v1\n        with: { who: rig }\n      - uses: octo/hello/twice@v1\n      - uses: acme/lint@releases/v2\n      - run: make\n";
    let lowered = lowered(text, &push());
    let task = &lowered.pipeline.tasks[0];
    let uses: Vec<Option<&str>> = task.steps.iter().map(|s| s.uses.as_deref()).collect();
    assert_eq!(
        uses,
        [
            Some("octo/hello@v1"),
            Some("octo/hello/twice@v1"),
            Some("acme/lint@releases/v2"),
            None
        ]
    );
    assert_eq!(
        task.fields["workspace"]["actions"],
        json!([
            {"repository": "https://github.com/octo/hello.git", "ref": "v1", "path": "octo/hello/v1"},
            // The repository's own owner's, in a trusted run: fetched with
            // a credential, as it may be private.
            {"repository": "https://github.com/acme/lint.git", "ref": "releases/v2", "path": "acme/lint/releases%2Fv2", "credential": true},
        ])
    );
    // A run that is not trusted fetches it with none.
    let mut untrusted = push();
    untrusted.trust = "untrusted".into();
    let theirs = read(text).lower("ci", &untrusted, &settings_of()).unwrap();
    assert!(
        theirs.pipeline.tasks[0].fields["workspace"]["actions"]
            .as_array()
            .unwrap()
            .iter()
            .all(|a| a.get("credential").is_none()),
        "{:?}",
        theirs.pipeline.tasks[0].fields["workspace"]
    );
    // The checkout is still the event's: the workspace says nothing of it.
    assert!(task.fields["workspace"].get("checkout").is_none());
    // Any of them may be JavaScript: the job is given the Node.js it runs
    // on. A job that uses none is not.
    assert_eq!(task.fields["workspace"]["tools"], json!(["node"]));
    let plain = read("on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/checkout@v4\n      - run: make\n")
        .lower("ci", &push(), &settings_of())
        .unwrap();
    assert!(plain.pipeline.tasks[0].fields.get("workspace").is_none());

    // Listed, and pinned: from the operator's mirror. Not listed, or not
    // pinned: refused, by name.
    let sha = "4b4708e725f29891d22b4c96af9f0a403ff51c7c";
    let mut settings = settings_of();
    settings.actions =
        ActionSource::parse("https://git.example.internal/github/", "OCTO/*").unwrap();
    let pinned = text
        .replace("octo/hello@v1", &format!("octo/hello@{sha}"))
        .replace("octo/hello/twice@v1", &format!("octo/hello/twice@{sha}"))
        .replace("      - uses: acme/lint@releases/v2\n", "");
    let lowered = read(&pinned).lower("ci", &push(), &settings).unwrap();
    assert_eq!(
        lowered.pipeline.tasks[0].fields["workspace"]["actions"],
        json!([{
            "repository": "https://git.example.internal/github/octo/hello.git",
            "ref": sha,
            "path": format!("octo/hello/{sha}"),
        }])
    );
    let found: Vec<String> = read(text)
        .lower("ci", &push(), &settings)
        .unwrap_err()
        .iter()
        .map(ToString::to_string)
        .collect();
    assert!(
        found
            .iter()
            .any(|p| p.contains("octo/hello@v1: where the actions that may be used are listed")),
        "{found:#?}"
    );
    assert!(
        found
            .iter()
            .any(|p| p
                .contains("uses acme/lint, which is not among the actions this dagron-ci allows")),
        "{found:#?}"
    );
}

/// A job that may ask for an OIDC token (`id-token: write`, or
/// `write-all`) is given the engine's identity on a trusted run, where the
/// operator gives workflows one: for the forge's own audience first, as
/// GitHub's is for where none is asked, then those the repository names. A
/// job's own `permissions` are over the workflow's. An untrusted run's job,
/// and one that may not ask, is given none.
#[test]
fn a_job_that_may_ask_for_an_oidc_token_is_given_the_engine_s_identity() {
    let text = "on: [push, pull_request]\npermissions: { id-token: write, contents: read }\njobs:\n  deploy:\n    runs-on: ubuntu-latest\n    steps: [{ run: x }]\n  build:\n    runs-on: ubuntu-latest\n    permissions: { contents: read }\n    steps: [{ run: x }]\n  all:\n    runs-on: ubuntu-latest\n    permissions: write-all\n    steps: [{ run: x }]\n";
    let mut settings = settings_of();
    settings.oidc = true;
    settings.oidc_audiences = vec!["sts.amazonaws.com".into(), "https://github.com/acme".into()];
    let identities = |event: &Event, settings: &Settings| -> Vec<(String, Value)> {
        read(text)
            .lower("ci", event, settings)
            .unwrap()
            .pipeline
            .tasks
            .iter()
            .map(|t| {
                (
                    t.name.clone(),
                    t.fields.get("identity").cloned().unwrap_or(Value::Null),
                )
            })
            .collect()
    };
    let given = json!({"oidc": {"audience": ["https://github.com/acme", "sts.amazonaws.com"]}});
    assert_eq!(
        identities(&push(), &settings),
        [
            ("deploy".to_string(), given.clone()),
            ("build".to_string(), Value::Null),
            ("all".to_string(), given),
        ]
    );
    // A fork's pull request: none, and the run is made all the same.
    let fork = Event {
        trust: "untrusted".into(),
        ..pull_request()
    };
    assert!(identities(&fork, &settings)
        .iter()
        .all(|(_, i)| i.is_null()));
    // The operator gives workflows none.
    settings.oidc = false;
    assert!(identities(&push(), &settings)
        .iter()
        .all(|(_, i)| i.is_null()));

    for (permissions, why) in [
        (
            "permissions: write-some",
            "is read-all, write-all, or a mapping",
        ),
        (
            "permissions: [id-token]",
            "is read-all, write-all, or a mapping",
        ),
        ("permissions: { id-token: read }", "is write or none"),
    ] {
        refused(
            &format!("on: push\n{permissions}\njobs: {{ a: {{ runs-on: ubuntu-latest, steps: [{{ run: x }}] }} }}"),
            why,
        );
    }
}

/// A job's `GITHUB_TOKEN`, where the operator gives workflows one: what its
/// `permissions` grant, by a GitHub App's names, a job's own in place of
/// the workflow's; `contents: read` where nothing says, as GitHub's
/// restricted default; none where they grant nothing. What GitHub has and
/// an App does not gives the token nothing. The token service caps it by
/// the run's trust, so an untrusted run's job asks all the same.
#[test]
fn a_job_is_given_a_token_with_what_its_permissions_grant() {
    let text = "on: [push, pull_request]\npermissions: { contents: write, pull-requests: read, packages: write, id-token: write }\njobs:\n  wf:\n    runs-on: ubuntu-latest\n    steps: [{ run: x }]\n  own:\n    runs-on: ubuntu-latest\n    permissions: { statuses: write, contents: none }\n    steps: [{ run: x }]\n  nothing:\n    runs-on: ubuntu-latest\n    permissions: {}\n    steps: [{ run: x }]\n  all:\n    runs-on: ubuntu-latest\n    permissions: read-all\n    steps: [{ run: x }]\n";
    let mut settings = settings_of();
    settings.github_token = true;
    let tokens = |text: &str, event: &Event, settings: &Settings| -> Vec<(String, Value)> {
        read(text)
            .lower("ci", event, settings)
            .unwrap()
            .pipeline
            .tasks
            .iter()
            .map(|t| {
                (
                    t.name.clone(),
                    t.fields.get("forge_token").cloned().unwrap_or(Value::Null),
                )
            })
            .collect()
    };
    let got = tokens(text, &push(), &settings);
    assert_eq!(
        got[0],
        (
            "wf".to_string(),
            json!({"permissions": {"contents": "write", "pull_requests": "read"}})
        )
    );
    assert_eq!(
        got[1],
        (
            "own".to_string(),
            json!({"permissions": {"statuses": "write"}})
        )
    );
    assert_eq!(got[2], ("nothing".to_string(), Value::Null));
    assert_eq!(got[3].1["permissions"]["contents"], "read");
    assert_eq!(got[3].1["permissions"]["issues"], "read");
    assert!(got[3].1["permissions"].get("metadata").is_none());
    // An untrusted run's job asks all the same; the service gives it read.
    let fork = Event {
        trust: "untrusted".into(),
        ..pull_request()
    };
    assert_eq!(tokens(text, &fork, &settings)[0].1, got[0].1);
    // Nothing written: GitHub's restricted default.
    let plain = "on: push\njobs:\n  a:\n    runs-on: ubuntu-latest\n    steps: [{ run: x }]\n";
    assert_eq!(
        tokens(plain, &push(), &settings)[0].1,
        json!({"permissions": {"contents": "read"}})
    );
    // The operator gives none.
    settings.github_token = false;
    assert!(tokens(text, &push(), &settings)
        .iter()
        .all(|(_, t)| t.is_null()));

    refused(
        "on: push\npermissions: { contents: admin }\njobs: { a: { runs-on: ubuntu-latest, steps: [{ run: x }] } }",
        "is read, write or none",
    );
    // A name that is no permission is a mistake, and is said; one GitHub
    // has and an App does not is passed over.
    for typo in ["content", "pull_request"] {
        refused(
            &format!("on: push\npermissions: {{ {typo}: write }}\njobs: {{ a: {{ runs-on: ubuntu-latest, steps: [{{ run: x }}] }} }}"),
            "is not a permission GitHub gives a job",
        );
    }
    assert!(problems_of(
        "on: push\npermissions: { packages: write, attestations: write, repository-projects: read }\njobs: { a: { runs-on: ubuntu-latest, steps: [{ run: x }] } }"
    )
    .is_empty());
    // A step may read its token, and so may a job's own variables, which
    // the step runner reads as the job starts; not a container's, which
    // the engine reads.
    assert!(problems_of(
        "on: push\njobs:\n  a:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo hi\n        env: { T: \"${{ secrets.GITHUB_TOKEN }}\" }\n      - uses: ./x\n        with: { token: \"${{ secrets.github_token }}\" }\n"
    )
    .is_empty());
    assert!(problems_of(
        "on: push\nenv: { W: \"${{ secrets.GITHUB_TOKEN }}\" }\njobs:\n  a:\n    runs-on: ubuntu-latest\n    env: { T: \"${{ secrets.GITHUB_TOKEN }}\" }\n    steps: [{ run: x }]\n"
    )
    .is_empty());
    refused(
        "on: push\njobs:\n  a:\n    runs-on: ubuntu-latest\n    container: { image: alpine, env: { T: \"${{ secrets.GITHUB_TOKEN }}\" } }\n    steps: [{ run: x }]\n",
        "reads `secrets` where neither a step nor a job's `env:` does",
    );
    // A step reads a secret of the repository's by its name.
    assert!(problems_of(
        "on: push\njobs:\n  a:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo ${{ secrets.DEPLOY_KEY }}\n"
    )
    .is_empty());
    refused(
        "on: push\njobs:\n  a:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo ${{ toJSON(secrets) }}\n",
        "reads `secrets` whole",
    );
    refused(
        "on: push\njobs:\n  a:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo ${{ secrets.GITHUB_OTHER }}\n",
        "which is no secret's name",
    );
}

/// A job's and its workflow's `env:` that read a secret are read by the
/// step runner as the job starts: each is the task's `CI_JOB_ENVX_<NAME>`,
/// as written, and is in neither its variables nor its contexts as a value.
/// A name is the later level's, whichever kind each is. The secrets they
/// read are given the job as its steps' are.
#[test]
fn a_job_s_env_that_reads_a_secret_is_read_as_the_job_starts() {
    let text = "on: push\nenv:\n  W: \"${{ secrets.WORKFLOW_KEY }}\"\n  O: \"${{ secrets.OVERRIDDEN }}\"\n  P: plain\njobs:\n  deploy:\n    runs-on: ubuntu-latest\n    env:\n      J: \"x-${{ secrets.Job_Key }}\"\n      O: from-the-job\n      P: \"${{ secrets.LATER }}\"\n    steps: [{ run: ./deploy.sh }]\n";
    let lowered = lowered(text, &push());
    let task = &lowered.pipeline.tasks[0];
    let env = task.fields["env"].as_array().unwrap();
    let value = |name: &str| {
        env.iter()
            .find(|e| e["name"] == name)
            .map(|e| e["value"].as_str().unwrap_or_default().to_string())
    };
    // As written, with the braces dagron's own templates would read made
    // inert, as every other text of the workflow's is.
    assert!(value("CI_JOB_ENVX_W")
        .unwrap()
        .contains("secrets.WORKFLOW_KEY"));
    assert!(value("CI_JOB_ENVX_J").unwrap().contains("secrets.Job_Key"));
    assert!(value("CI_JOB_ENVX_P").unwrap().contains("secrets.LATER"));
    assert_eq!(value("CI_JOB_ENVX_O"), None, "the job's own O is the later");
    assert_eq!(value("O").as_deref(), Some("from-the-job"));
    assert_eq!(
        value("P"),
        None,
        "the job's P, which reads a secret, is the later"
    );
    assert_eq!(value("J"), None);
    let context: Value = serde_json::from_str(&value("CI_CONTEXT").unwrap()).unwrap();
    assert_eq!(context["env"], json!({ "O": "from-the-job" }));
    let refs: Vec<&str> = env
        .iter()
        .filter(|e| e.get("value_from").is_some())
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    assert_eq!(
        refs,
        [
            "CI_SECRET_ACTIONS_STEP_DEBUG",
            "CI_SECRET_JOB_KEY",
            "CI_SECRET_LATER",
            "CI_SECRET_WORKFLOW_KEY",
        ],
        "not OVERRIDDEN, whose variable the job's own O replaced"
    );
    // A variable named as dagron-ci's own is refused, so that no text an
    // outsider wrote is read again as one that reads a secret.
    refused(
        "on: push\njobs:\n  a:\n    runs-on: ubuntu-latest\n    env: { CI_JOB_ENVX_X: \"${{ github.head_ref }}\" }\n    steps: [{ run: x }]\n",
        "is dagron-ci's own",
    );
    refused(
        "on: push\nenv: { CI_SECRET_X: y }\njobs:\n  a:\n    runs-on: ubuntu-latest\n    steps: [{ run: x }]\n",
        "is dagron-ci's own",
    );
}

/// A schedule's event runs its workflow while the workflow has its cron,
/// which the workflow reads as `github.event.schedule`; a cron it no longer
/// has runs nothing. A cron that is not one is refused when the file is
/// read.
#[test]
fn a_schedule_runs_its_workflow_while_the_workflow_has_its_cron() {
    let text = "on:\n  schedule:\n    - cron: '0 3 * * *'\n    - cron: '*/30 * * * *'\njobs:\n  a:\n    runs-on: ubuntu-latest\n    steps:\n      - run: echo ${{ github.event.schedule }}\n";
    let schedule = |cron: &str| Event {
        kind: "schedule".into(),
        inputs: BTreeMap::from([("schedule".to_string(), cron.to_string())]),
        ..push()
    };
    let at = schedule("0 3 * * *");
    let pipeline = lowered(text, &at).pipeline;
    assert_eq!(pipeline.on.schedule, ["0 3 * * *", "*/30 * * * *"]);
    assert!(matches!(
        compile::compile(&pipeline, &at, &Default::default()),
        Ok(Outcome::Run(_))
    ));
    let gone = schedule("0 4 * * *");
    match compile::compile(&lowered(text, &gone).pipeline, &gone, &Default::default()) {
        Ok(Outcome::Skip(why)) => assert!(why.contains("no longer has"), "{why}"),
        _ => panic!("a cron the workflow no longer has runs nothing"),
    }
    let github = github_context(&at, "ci");
    assert_eq!(github.get("event_name").text(), "schedule");
    assert_eq!(github.get("event").get("schedule").text(), "0 3 * * *");
    refused(
        "on:\n  schedule:\n    - cron: '61 * * * *'\njobs:\n  a:\n    runs-on: ubuntu-latest\n    steps: [{ run: x }]\n",
        "its minute 61 is not between 0 and 59",
    );
    refused(
        "on:\n  schedule: daily\njobs:\n  a:\n    runs-on: ubuntu-latest\n    steps: [{ run: x }]\n",
        "is a list of `- cron:",
    );
}

/// The `github` context of a run dagron-ci gave its numbers to, of a pull
/// request: who caused it, its head branch, the run's numbers, and where
/// the forge's API is.
#[test]
fn the_github_context_says_who_the_run_and_the_api() {
    let event = Event {
        actor: Some("octocat".into()),
        head_ref: Some("feature/x".into()),
        run_numbers: Some((42, 7)),
        ..pull_request()
    };
    let github = github_context(&event, "ci");
    let field = |name: &str| github.get(name).text();
    assert_eq!(field("actor"), "octocat");
    assert_eq!(field("triggering_actor"), "octocat");
    assert_eq!(field("head_ref"), "feature/x");
    assert_eq!(field("run_id"), "42");
    assert_eq!(field("run_number"), "7");
    assert_eq!(field("run_attempt"), "1");
    assert_eq!(field("api_url"), "https://api.github.com");
    assert_eq!(field("graphql_url"), "https://api.github.com/graphql");
    assert_eq!(
        github.get("event").get("sender").get("login").text(),
        "octocat"
    );
    assert_eq!(
        github
            .get("event")
            .get("pull_request")
            .get("head")
            .get("ref")
            .text(),
        "feature/x"
    );
    // A push has no head branch, a server of GitHub's own has its API
    // under `/api/v3`, and Gitea's under `/api/v1`, with no GraphQL.
    let gitea = github_context(
        &Event {
            forge: "gitea".into(),
            forge_host: "git.example".into(),
            ..push()
        },
        "ci",
    );
    assert_eq!(gitea.get("api_url").text(), "https://git.example/api/v1");
    assert_eq!(gitea.get("graphql_url").text(), "");
    let push = github_context(
        &Event {
            forge_host: "ghe.example".into(),
            head_ref: Some("ignored".into()),
            ..push()
        },
        "ci",
    );
    assert_eq!(push.get("head_ref").text(), "");
    assert_eq!(push.get("api_url").text(), "https://ghe.example/api/v3");
    assert_eq!(push.get("run_id").text(), "", "no numbers given");
}

/// A secret of the repository's a job's steps read is given the job on a
/// trusted run, by the engine, from the repository's environment: a
/// reference for each name, read in capitals, and empty where the
/// environment has none. A fork's run is given none, as on GitHub, and a
/// job that reads none names no environment.
#[test]
fn a_repository_s_secrets_are_its_environment_s_on_a_trusted_run() {
    let text = "on: [push, pull_request]\njobs:\n  deploy:\n    runs-on: ubuntu-latest\n    steps:\n      - run: ./deploy.sh \"${{ secrets.Npm_Token }}\"\n        env: { KEY: \"${{ secrets.DEPLOY_KEY }}\" }\n      - uses: ./a\n        with: { token: \"${{ secrets.NPM_TOKEN }}\" }\n  lint:\n    runs-on: ubuntu-latest\n    steps: [{ run: make lint }]\n";
    let tasks = |event: &Event| -> Vec<(String, Value, Vec<Value>)> {
        read(text)
            .lower("ci", event, &settings_of())
            .unwrap()
            .pipeline
            .tasks
            .iter()
            .map(|t| {
                let refs: Vec<Value> = t.fields["env"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|e| e.get("value_from").is_some())
                    .cloned()
                    .collect();
                (
                    t.name.clone(),
                    t.fields.get("environment").cloned().unwrap_or(Value::Null),
                    refs,
                )
            })
            .collect()
    };
    let got = tasks(&push());
    assert_eq!(got[0].1, json!("github.com/acme/widgets"));
    assert_eq!(
        got[0].2,
        [
            json!({ "name": "CI_SECRET_ACTIONS_STEP_DEBUG", "value_from": { "secret": "ACTIONS_STEP_DEBUG", "optional": true } }),
            json!({ "name": "CI_SECRET_DEPLOY_KEY", "value_from": { "secret": "DEPLOY_KEY", "optional": true } }),
            json!({ "name": "CI_SECRET_NPM_TOKEN", "value_from": { "secret": "NPM_TOKEN", "optional": true } }),
        ],
        "one for each name, in capitals, and the one that turns on debug lines"
    );
    assert_eq!(
        (got[1].1.clone(), got[1].2.len()),
        (Value::Null, 0),
        "lint reads none"
    );
    // A fork's pull request: none, and no environment.
    let fork = Event {
        trust: "untrusted".into(),
        ..pull_request()
    };
    let got = tasks(&fork);
    assert_eq!((got[0].1.clone(), got[0].2.len()), (Value::Null, 0));
    assert_eq!(
        repository_environment("https://GitHub.com", "Acme/Widgets"),
        "github.com/acme/widgets"
    );
    assert_eq!(
        repository_environment("https://ghe.example/", "acme/widgets"),
        "ghe.example/acme/widgets",
        "another forge's is another's"
    );
    // The name the engine gives that repository's runs alone.
    assert_eq!(
        repository_environment("https://GitHub.com", "Acme/Widgets"),
        dagron_core::envpolicy::repository_environment("github.com", "Acme/Widgets")
    );
    assert!(dagron_core::envpolicy::is_repository_environment(
        &repository_environment("https://git.example:3000", "a-b/c_d.e")
    ));
}

/// The problems of `text`, read and then lowered for a push: none where it
/// is taken.
fn problems_of(text: &str) -> Vec<String> {
    match parse(text, &settings_of().provided)
        .and_then(|workflow| workflow.lower("ci", &push(), &settings_of()))
    {
        Ok(_) => Vec::new(),
        Err(found) => found.iter().map(ToString::to_string).collect(),
    }
}

/// `github.docker: true` gives each job of a trusted run a Docker daemon;
/// a run that is not trusted is refused one, so its jobs ask for none.
#[test]
fn a_repository_that_asks_has_a_docker_daemon_beside_each_trusted_job() {
    let text = "on: [push, pull_request]\njobs:\n  image:\n    runs-on: ubuntu-latest\n    steps: [{ run: docker build . }]\n";
    let daemons = |event: &Event, settings: &Settings| -> Vec<Value> {
        read(text)
            .lower("ci", event, settings)
            .unwrap()
            .pipeline
            .tasks
            .iter()
            .map(|t| {
                t.fields
                    .get("docker_daemon")
                    .cloned()
                    .unwrap_or(Value::Null)
            })
            .collect()
    };
    let mut asked = settings_of();
    assert_eq!(daemons(&push(), &asked), [Value::Null], "none unless asked");
    asked.docker = true;
    assert_eq!(daemons(&push(), &asked), [json!(true)]);
    let untrusted = Event {
        trust: "untrusted".into(),
        ..push()
    };
    assert_eq!(daemons(&untrusted, &asked), [Value::Null]);

    // A job that runs a daemon of its own keeps that one.
    let own = "on: push\njobs:\n  image:\n    runs-on: ubuntu-latest\n    services:\n      docker:\n        image: docker:dind\n    steps: [{ run: docker build . }]\n";
    let lowered = read(own).lower("ci", &push(), &asked).unwrap();
    assert!(lowered.pipeline.tasks[0]
        .fields
        .get("docker_daemon")
        .is_none());
    // One with as many services as a task may have has no room for it, and
    // says so at the job.
    let many: String = (0..crate::pipeline::MAX_SERVICES)
        .map(|i| format!("      s{i}:\n        image: x\n"))
        .collect();
    let full = format!("on: push\njobs:\n  image:\n    runs-on: ubuntu-latest\n    services:\n{many}    steps: [{{ run: x }}]\n");
    let found = read(&full).lower("ci", &push(), &asked).unwrap_err();
    assert!(
        found
            .iter()
            .any(|p| p.to_string().contains("jobs.image.services")
                && p.to_string().contains("github.docker")),
        "{found:?}"
    );

    let read = settings("github:\n  docker: true\n").unwrap();
    assert!(read.docker);
    assert!(!settings("github:\n  workflows: [ci.yml]\n").unwrap().docker);
    let err = settings("github:\n  docker: yes please\n").unwrap_err();
    assert!(
        err.iter().any(|p| p.to_string().contains("github.docker")),
        "{err:?}"
    );
}

/// The operator lists the repositories whose jobs may have a daemon
/// (`CI_GITHUB_DOCKER`): by name, or every one of an owner's.
#[test]
fn the_operator_lists_the_repositories_that_may_have_a_docker_daemon() {
    let repos = DockerRepos::parse(" Acme/*, other/Widgets ,").unwrap();
    assert!(repos.allows("acme/widgets"));
    assert!(repos.allows("ACME/gadgets"));
    assert!(repos.allows("other/widgets"));
    assert!(!repos.allows("other/gadgets"));
    assert!(!repos.allows("acmecorp/widgets"));
    assert!(!DockerRepos::parse("").unwrap().allows("acme/widgets"));
    for bad in ["acme", "acme/a b", "*/*", "acme/x/y"] {
        assert!(DockerRepos::parse(bad).is_err(), "{bad:?}");
    }
}

/// `hashFiles` is a step's, as on GitHub: a job's `runs-on` or `if:` that
/// calls it is refused when the file is read, where the job is named.
#[test]
fn hash_files_is_refused_outside_a_step() {
    for (job, at) in [
        (
            "    runs-on: ${{ hashFiles('x') }}\n    steps: [{ run: x }]\n",
            "jobs.build.runs-on",
        ),
        (
            "    if: hashFiles('x') != ''\n    runs-on: ubuntu-latest\n    steps: [{ run: x }]\n",
            "jobs.build.if",
        ),
    ] {
        let found = problems_of(&format!("on: push\njobs:\n  build:\n{job}"));
        assert!(
            found.iter().any(|p| p.contains(at)
                && p.contains("`hashFiles` is not a function that can be called here")),
            "{at}: {found:?}"
        );
    }
}

#[test]
fn the_audiences_a_repository_names_are_checked() {
    let ok = settings("github:\n  oidc_audiences: [sts.amazonaws.com, api://AzureADTokenExchange, sts.amazonaws.com]\n").unwrap();
    assert_eq!(
        ok.oidc_audiences,
        ["sts.amazonaws.com", "api://AzureADTokenExchange"],
        "each once"
    );
    for (text, why) in [
        (
            "github:\n  oidc_audiences: [\"a b\"]\n",
            "is not an audience",
        ),
        ("github:\n  oidc_audiences: [\"\"]\n", "is not an audience"),
        (
            "github:\n  oidc_audiences: [a, b, c, d, e, f, g, h]\n",
            "at most 7, the forge's own being one more",
        ),
    ] {
        let err = settings(text).unwrap_err();
        assert!(
            err.iter().any(|p| p.to_string().contains(why)),
            "{text}: {err:?}"
        );
    }
}

#[test]
fn the_operator_names_where_actions_come_from_and_which_may_be_used() {
    let none = ActionSource::parse("", "").unwrap();
    assert_eq!(none, ActionSource::default());
    assert_eq!(none.mirror(), "https://github.com");
    let set = ActionSource::parse(" http://mirror:8080/gh// ", " acme/*, Octo/Hello ,").unwrap();
    assert_eq!(set.mirror(), "http://mirror:8080/gh");
    assert_eq!(
        set.allow,
        Some(vec!["acme/*".to_string(), "octo/hello".to_string()])
    );
    for (mirror, allow, why) in [
        ("github.com", "", "CI_ACTIONS_MIRROR"),
        ("https://user:pw@github.com", "", "CI_ACTIONS_MIRROR"),
        ("https://github.com/?x=1", "", "CI_ACTIONS_MIRROR"),
        ("https://", "", "CI_ACTIONS_MIRROR"),
        ("https://github.com/{{ x }}", "", "CI_ACTIONS_MIRROR"),
        ("", "acme", "is not owner/* or owner/repo"),
        ("", "acme/*/x", "is not owner/* or owner/repo"),
        ("", "*/*", "is not owner/* or owner/repo"),
        ("", " , ", "names no action"),
    ] {
        let err = ActionSource::parse(mirror, allow).unwrap_err();
        assert!(err.contains(why), "{mirror:?} {allow:?}: {err}");
    }
}

/// An action's file is read from github.com's raw files with no mirror,
/// from a mirror's where the operator says where, and from nowhere where
/// a mirror is named and that is not said; the template's host is the
/// operator's.
#[test]
fn an_action_s_file_is_read_where_the_operator_says() {
    let action = expr::uses::remote("octo/tool/sub@v1").unwrap();
    let none = ActionSource::parse("", "").unwrap();
    assert_eq!(
        none.file_url(&action, "action.yml").as_deref(),
        Some("https://raw.githubusercontent.com/octo/tool/v1/sub/action.yml")
    );
    let mirror = ActionSource::parse("https://git.example/gh", "").unwrap();
    assert_eq!(mirror.file_url(&action, "action.yml"), None);
    let raw = mirror
        .with_raw(" https://git.example/{owner}/{repo}/raw/{ref}/{path} ")
        .unwrap();
    assert_eq!(
        raw.file_url(&action, "action.yaml").as_deref(),
        Some("https://git.example/octo/tool/raw/v1/sub/action.yaml")
    );
    let unset = ActionSource::parse("", "").unwrap().with_raw("  ").unwrap();
    assert_eq!(unset, ActionSource::default());
    for template in [
        "git.example/{owner}/{repo}/{ref}/{path}",
        "https://git.example/{owner}/{repo}/{ref}",
        "https://{owner}.example/{repo}/{ref}/{path}",
        "https:///{owner}/{repo}/{ref}/{path}",
        "https://u@git.example/{owner}/{repo}/{ref}/{path}",
        "https://git.example/{owner}/{repo}/{ref}/{path}?x=1",
        "https://git.example/{owner} /{repo}/{ref}/{path}",
    ] {
        let err = ActionSource::parse("", "")
            .unwrap()
            .with_raw(template)
            .unwrap_err();
        assert!(err.contains("CI_ACTIONS_MIRROR_RAW"), "{template}: {err}");
    }
}

/// A step that uses a local action is handed on as it is written, for the
/// step runner to read the action as the step starts: step protocol 4,
/// which a job with no such step is not handed on in.
#[test]
fn a_step_that_uses_a_local_action_is_handed_on_in_step_protocol_4() {
    let text = "on: push\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/checkout@v4\n      - id: lint\n        uses: ./.github/actions/lint\n        if: always()\n        env: { PLAIN: 1, LATE: \"${{ github.sha }}\" }\n        with:\n          level: 2\n          strict: true\n          target: \"${{ github.ref_name }}\"\n          braces: \"{{ ci_sha }}\"\n      - run: echo done\n";
    let lowered = lowered(text, &push());
    let task = &lowered.pipeline.tasks[0];
    let lint = &task.steps[0];
    assert_eq!(lint.uses.as_deref(), Some("./.github/actions/lint"));
    assert_eq!(lint.run, "");
    assert_eq!(
        (lint.shell.as_deref(), lint.working_directory.as_deref()),
        (None, None)
    );
    assert_eq!(lint.id.as_deref(), Some("lint"));
    assert_eq!(lint.condition.as_deref(), Some("always()"));
    assert_eq!(lint.env.get("PLAIN").map(String::as_str), Some("1"));
    // Each is as written, and a `{{` of the workflow's own is text to
    // dagron, as it is in any of a step's texts.
    assert_eq!(lint.late_env.get("LATE"), Some(&inert("${{ github.sha }}")));
    let with: Vec<(String, String)> = [
        ("braces", "{{ ci_sha }}"),
        ("level", "2"),
        ("strict", "true"),
        ("target", "${{ github.ref_name }}"),
    ]
    .into_iter()
    .map(|(name, value)| (name.to_string(), inert(value)))
    .collect();
    assert_eq!(lint.with, with);
    assert_eq!(compile::protocol(&task.steps), compile::STEP_PROTOCOL_4);
    assert_eq!(compile::step_name(lint), "Run ./.github/actions/lint");
    assert!(task.steps[1].uses.is_none());
}

#[test]
fn what_is_not_taken_is_refused_by_name_and_nothing_is_passed_over() {
    for (job, why) in [
        ("runs-on: ubuntu-latest\nsteps:\n  - uses: \"docker://\"", "names no image: docker:// and an image"),
        ("runs-on: ubuntu-latest\nsteps:\n  - uses: actions/setup-node", "is not an action GitHub would take"),
        ("runs-on: ubuntu-latest\nsteps:\n  - uses: actions/setup-node@v4 x", "is not an action GitHub would take"),
        ("runs-on: ubuntu-latest\nsteps:\n  - uses: o/r/../x@v1", "is not an action GitHub would take"),
        ("runs-on: ubuntu-latest\nsteps:\n  - uses: o/r@v1\n    shell: bash", "is a run step's, and this step uses an action"),
        ("runs-on: ubuntu-latest\nsteps:\n  - uses: actions/checkout@v4\n    with: { fetch-depth: -1 }\n  - run: x", "a checkout with `fetch-depth`"),
        ("runs-on: ubuntu-latest\nsteps:\n  - uses: actions/checkout@v4\n    with: { fetch-depth: all }\n  - run: x", "a checkout with `fetch-depth`"),
        ("runs-on: ubuntu-latest\nsteps:\n  - uses: actions/checkout@v4\n    with: { ref: other }\n  - run: x", "a checkout with `ref`"),
        ("runs-on: ubuntu-latest\nsteps:\n  - uses: actions/checkout@v4", "has no step that runs a script"),
        ("uses: ./.github/workflows/other.yml", "a job that calls a reusable workflow"),
        ("runs-on: ubuntu-latest\noutputs: { 1a: b }\nsteps: [{ run: x }]", "an output's name is"),
        ("runs-on: ubuntu-latest\noutputs: [a]\nsteps: [{ run: x }]", "maps the job's output names"),
        ("runs-on: ubuntu-latest\noutputs: { a: \"${{ secrets.T }}\" }\nsteps: [{ run: x }]", "reads `secrets`"),
        ("runs-on: ubuntu-latest\nenvironment: prod\nsteps: [{ run: x }]", "a job's `environment:`"),
        ("runs-on: ubuntu-latest\nif: needs.a.result == 'success'\nsteps: [{ run: x }]", "does not name a in its `needs:`"),
        ("runs-on: ubuntu-latest\nsteps: [{ run: \"echo ${{ needs.a.outputs.b }}\" }]", "does not name a in its `needs:`"),
        ("runs-on: ubuntu-latest\nenv: { T: \"${{ needs.a.result }}\" }\nsteps: [{ run: x }]", "anywhere but a job's `if:`, its steps and its `outputs:`"),
        ("runs-on: ubuntu-latest\nservices: { db: { image: postgres, env: { T: \"${{ secrets.TOKEN }}\" } } }\nsteps: [{ run: x }]", "reads `secrets` where neither a step nor a job's `env:` does"),
        ("runs-on: ubuntu-latest\nsteps:\n  - run: echo ${{ secrets }}", "reads `secrets` whole"),
        ("runs-on: ubuntu-latest\nstrategy: { matrix: \"m-${{ inputs.m }}\" }\nsteps: [{ run: x }]", "a matrix that is more than one expression, or text with one"),
        ("runs-on: ubuntu-latest\nstrategy: { matrix: \"${{ secrets.M }}\" }\nsteps: [{ run: x }]", "reads `secrets`"),
        ("runs-on: ubuntu-latest\nstrategy: { matrix: { a: [1] }, max-parallel: 2 }\nsteps: [{ run: x }]", "`max-parallel`"),
        ("runs-on: { group: big }\nsteps: [{ run: x }]", "a runner group"),
        ("runs-on: macos-latest\nsteps: [{ run: x }]", "macos-latest is no label .dagron/ci.yaml gives an image for"),
        ("runs-on: ubuntu-latest\ncontainer: { image: x, options: --privileged }\nsteps: [{ run: x }]", "a container's `options:`"),
        ("runs-on: ubuntu-latest\nservices: { db: { image: x, options: --privileged } }\nsteps: [{ run: x }]", "the container option `--privileged`"),
        ("runs-on: ubuntu-latest\nservices: { DB: { image: x } }\nsteps: [{ run: x }]", "a service's name is its host name here"),
        ("runs-on: ubuntu-latest\ncontainer: \"${{ github.head_ref }}\"\nsteps: [{ run: x }]", "reads `github`, which GitHub does not give here"),
        ("runs-on: ubuntu-latest\nsteps:\n  - run: x\n    shell: fish", "is not a shell this takes"),
        ("runs-on: ubuntu-latest\nsteps:\n  - run: print(\"${{ github.head_ref }}\")\n    shell: python", "in a script for a shell that is not sh or bash"),
        ("runs-on: ubuntu-latest\nsteps:\n  - run: x\n    if: \"${{ 1 +\"", "is not an expression GitHub would take"),
        ("runs-on: ubuntu-latest\nsteps:\n  - run: x\n    continue-on-error: \"${{ matrix.x }}\"", "a step's `continue-on-error` that is an expression"),
        ("runs-on: ubuntu-latest\nsteps:\n  - run: x\n    uses: a/b@v1", "a step is one or the other"),
        ("runs-on: ubuntu-latest\nsteps:\n  - uses: ./../outside", "is a local action: ./ and a directory of the repository, with no .. in it"),
        ("runs-on: ubuntu-latest\nsteps:\n  - uses: ./a/../../outside", "with no .. in it"),
        ("runs-on: ubuntu-latest\nsteps:\n  - uses: ./a\n    shell: bash", "is a run step's, and this step uses an action"),
        ("runs-on: ubuntu-latest\nsteps:\n  - uses: ./a\n    working-directory: x", "is a run step's, and this step uses an action"),
        ("runs-on: ubuntu-latest\nsteps:\n  - uses: ./a\n    with: [x]", "is a mapping of the action's inputs"),
        ("runs-on: ubuntu-latest\nsteps:\n  - uses: ./a\n    with: { x: [1] }", "is an input's value"),
        ("runs-on: ubuntu-latest\nsteps:\n  - uses: ./a\n    with: { x: \"${{ secrets.GITHUB_X }}\" }", "which is no secret's name"),
        ("runs-on: ubuntu-latest\nsteps:\n  - uses: ./a\n    with: { x: \"${{ 1 +\" }", "is not an expression GitHub would take"),
        ("runs-on: ubuntu-latest\nsteps:\n  - { run: x, id: same }\n  - { run: y, id: same }", "two steps have the id \"same\""),
        ("runs-on: ubuntu-latest\nneeds: nobody\nsteps: [{ run: x }]", "names no job of this workflow"),
        ("runs-on: ubuntu-latest\nstpes: [{ run: x }]", "is not a key this takes"),
        ("steps: [{ run: x }]", "runs-on: is required"),
    ] {
        refused(&with_job(job), why);
    }
    refused("on: push\nconcurrency: { group: x, cancel-in-progress: \"${{ github.ref != 'refs/heads/main' }}\" }\njobs: { a: { runs-on: ubuntu-latest, steps: [{ run: x }] } }", "cancel-in-progress: is true or false here");
    refused("on: { pull_request: { types: [labeled] } }\njobs: { a: { runs-on: ubuntu-latest, steps: [{ run: x }] } }", "the pull request activity `labeled`");
    refused("on: push\njobs:\n  a: { needs: b, runs-on: ubuntu-latest, steps: [{ run: x }] }\n  b: { needs: a, runs-on: ubuntu-latest, steps: [{ run: x }] }\n", "needs forms a cycle through a, b");
    refused(
        "jobs: { a: { runs-on: ubuntu-latest, steps: [{ run: x }] } }",
        "on: is required",
    );
    refused("on: push", "jobs: is required");
    // Every problem at once, each with where it is.
    let found = problems(&with_job(
        "runs-on: ubuntu-latest\nenvironment: prod\nsteps:\n  - uses: \"docker://\"\n  - run: \"${{ 1 +\"",
    ));
    assert!(found.len() >= 3, "{found:#?}");
    assert!(
        found
            .iter()
            .any(|p| p.starts_with("jobs.build.steps[0].uses: ")),
        "{found:#?}"
    );
}

#[test]
fn a_matrix_is_expanded_as_github_expands_one() {
    let of = |strategy: &str| -> Vec<(String, serde_json::Value)> {
        let text = with_job(&format!(
            "runs-on: ubuntu-latest\nstrategy:\n  matrix:\n{strategy}\nsteps: [{{ run: x }}]"
        ));
        lowered(&text, &push())
            .pipeline
            .tasks
            .iter()
            .map(|t| {
                let context = t.fields["env"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .find(|e| e["name"] == "CI_CONTEXT")
                    .unwrap()["value"]
                    .as_str()
                    .unwrap()
                    .to_string();
                let context: serde_json::Value = serde_json::from_str(&context).unwrap();
                (t.name.clone(), context["matrix"].clone())
            })
            .collect()
    };
    // GitHub's own example of include: it adds to what it agrees with, and
    // is a combination of its own where it fits none.
    let expanded = of("    fruit: [apple, pear]\n    animal: [cat, dog]\n    include:\n      - color: green\n      - color: pink\n        animal: cat\n      - fruit: apple\n        shape: circle\n      - fruit: banana\n      - fruit: banana\n        animal: cat");
    let matrices: Vec<&serde_json::Value> = expanded.iter().map(|(_, m)| m).collect();
    assert_eq!(
        matrices,
        [
            &json!({ "fruit": "apple", "animal": "cat", "color": "pink", "shape": "circle" }),
            &json!({ "fruit": "apple", "animal": "dog", "color": "green", "shape": "circle" }),
            &json!({ "fruit": "pear", "animal": "cat", "color": "pink" }),
            &json!({ "fruit": "pear", "animal": "dog", "color": "green" }),
            &json!({ "fruit": "banana" }),
            &json!({ "fruit": "banana", "animal": "cat" }),
        ]
    );
    let names: Vec<&str> = expanded.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names[4], "build.banana");
    assert_eq!(names[5], "build.banana-cat");

    let excluded = of("    os: [linux, mac]\n    version: [1, 2]\n    exclude:\n      - os: mac\n        version: 1");
    let names: Vec<&str> = excluded.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, ["build.linux-1", "build.linux-2", "build.mac-2"]);
    // Only include: each is a job.
    let only = of("    include:\n      - { name: a, image: x }\n      - { name: b, image: y }");
    assert_eq!(only.len(), 2);
    // Two that would read the same are told apart by their place.
    let same = of("    v: [\"a b\", \"a-b\"]");
    let names: Vec<&str> = same.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, ["build.a-b", "build.1"]);

    refused(
        &with_job("runs-on: ubuntu-latest\nstrategy:\n  matrix:\n    a: [1]\n    exclude: [{ a: 1 }]\nsteps: [{ run: x }]"),
        "leaves no combination to run",
    );
}

#[test]
fn a_repository_says_which_workflows_run_here_and_on_what() {
    let s = settings_of();
    assert!(s.runs("ci.yml") && !s.runs("release.yml"));
    assert_eq!(s.runs_on["ubuntu-latest"], "docker.io/library/ubuntu:24.04");
    assert!(s.provided.contains("dtolnay/rust-toolchain"));
    assert_eq!(settings("").unwrap(), Settings::default());
    assert_eq!(
        settings("other: 1").unwrap_err()[0].to_string(),
        "other: is not a key this takes (github, gitlab)"
    );
    assert_eq!(settings("github: {}").unwrap(), Settings::default());
    for (text, why) in [
        (
            "github: { workflows: [../ci.yml] }",
            "is not a file of .github/workflows",
        ),
        (
            "github: { workflows: [ci] }",
            "is not a file of .github/workflows",
        ),
        (
            "github: { runs_on: { ubuntu-latest: \"a b\" } }",
            "is the image a job with that label runs in",
        ),
        (
            "github: { runs_on: [x] }",
            "maps a runs-on label to an image",
        ),
        ("github: { nope: 1 }", "is not a key this takes"),
    ] {
        let err = settings(text).unwrap_err();
        assert!(
            err.iter().any(|p| p.to_string().contains(why)),
            "{text}: {err:?}"
        );
    }
}

#[test]
fn an_expression_is_pasted_only_where_no_outsider_chooses_its_value() {
    let is = |text: &str| dagron_ci_expr::parse(text).unwrap().pasted();
    for yes in [
        "matrix.crate",
        "github.sha",
        "github.event_name == 'push'",
        "format('{0}-{1}', matrix.os, github.run_id)",
        "runner.os",
        "job.status",
        "strategy.job-index",
        "'literal'",
    ] {
        assert!(is(yes), "{yes}");
    }
    for no in [
        "github.head_ref",
        "github.ref_name",
        "github.ref",
        "github.actor",
        "github.event.pull_request.title",
        "github.event.head_commit.message",
        "inputs.name",
        "env.BRANCH",
        "steps.x.outputs.y",
        "toJSON(github)",
        "github['head_ref']",
        "format('{0}', github.event.issue.title)",
    ] {
        assert!(!is(no), "{no}");
    }
}

#[test]
fn a_service_s_options_are_a_health_check_s_or_refused() {
    assert_eq!(
        health_command("--health-cmd \"pg_isready -U x\" --health-interval 10s --health-timeout=5s --health-retries 5"),
        Ok(Some("pg_isready -U x".to_string()))
    );
    assert_eq!(
        health_command("--health-cmd='redis-cli ping'"),
        Ok(Some("redis-cli ping".to_string()))
    );
    assert_eq!(health_command("--health-interval 1s"), Ok(None));
    assert_eq!(health_command(""), Ok(None));
    assert_eq!(
        health_command("--privileged"),
        Err("--privileged".to_string())
    );
    assert_eq!(
        health_command("--health-cmd x -v /:/host"),
        Err("-v".to_string())
    );
    assert_eq!(
        shell_words("a 'b c'  \"d e\" f''"),
        ["a", "b c", "d e", "f"]
    );
}

fn dispatch(inputs: &[(&str, &str)]) -> Event {
    Event {
        kind: "dispatch".into(),
        inputs: inputs
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect(),
        ..push()
    }
}

/// What dagron makes of a text: each `{{ name }}` that is a parameter's
/// filled in, once.
fn filled(text: &str, parameters: &[(&str, &str)]) -> String {
    let parameters = parameters
        .iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect();
    dagron_core::expand::substitute(text, &parameters)
}

/// The contexts the step runner is handed for `task`, as dagron hands them
/// on.
fn contexts(task: &Task) -> Value {
    let env = task.fields["env"].as_array().unwrap();
    let context = env
        .iter()
        .find(|e| e["name"] == "CI_CONTEXT")
        .and_then(|e| e["value"].as_str())
        .unwrap();
    serde_json::from_str(&filled(context, &[(BRACES, "{{")])).unwrap()
}

#[test]
fn an_input_nobody_set_reads_as_its_default() {
    let text = "on:\n  workflow_dispatch:\n    inputs:\n      who: { default: world }\n      where: {}\n\
                jobs:\n  greet:\n    if: inputs.who == 'world'\n    runs-on: ubuntu-latest\n    \
                steps: [{ run: 'echo ${{ inputs.who }}' }]\n  \
                other:\n    if: github.event.inputs.who != 'world'\n    runs-on: ubuntu-latest\n    \
                steps: [{ run: echo other }]\n";
    // Nobody set `who`: the job that asks for its default runs, in `inputs`
    // and in the event's own copy of them, and one with no default is empty.
    let Lowered { pipeline, skipped } = lowered(text, &dispatch(&[]));
    let names: Vec<&str> = pipeline.tasks.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(names, ["greet"]);
    assert_eq!(skipped[0].0, "other");
    let context = contexts(&pipeline.tasks[0]);
    assert_eq!(context["inputs"], json!({ "where": "", "who": "world" }));
    assert_eq!(context["github"]["event"]["inputs"], context["inputs"]);
    // The caller's value is the one read where there is one.
    let Lowered { pipeline, skipped } = lowered(text, &dispatch(&[("who", "rig")]));
    let names: Vec<&str> = pipeline.tasks.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(names, ["other"]);
    assert_eq!(skipped[0].0, "greet");
    assert_eq!(contexts(&pipeline.tasks[0])["inputs"]["who"], "rig");
    // A push's event says where the branch was before, and a new branch
    // was nowhere, as GitHub writes that.
    let pushed = Event {
        base_sha: Some("b".repeat(40)),
        ..push()
    };
    let Lowered { pipeline, .. } = lowered(text, &pushed);
    let event = &contexts(&pipeline.tasks[0])["github"]["event"];
    assert_eq!(event["before"], "b".repeat(40));
    assert_eq!(event["after"], pushed.sha.as_str());
    let Lowered { pipeline, .. } = lowered(text, &push());
    assert_eq!(
        contexts(&pipeline.tasks[0])["github"]["event"]["before"],
        "0".repeat(40)
    );
    // And no other event has inputs, whatever the workflow declares.
    let Lowered { pipeline, .. } = lowered(text, &push());
    assert_eq!(contexts(&pipeline.tasks[0])["inputs"], json!({}));
}

/// `github.event` is the forge's own payload, as its webhook delivered it,
/// with dagron-ci's fields over it: what a workflow reads of a pull
/// request's title, its labels or a push's commits is the delivery's, and
/// the commit and refs are what dagron-ci runs. With no payload kept, it
/// is dagron-ci's fields alone.
#[test]
fn github_event_is_the_forge_s_payload_under_dagron_ci_s_own_fields() {
    let text = "on: pull_request\njobs:\n  a:\n    runs-on: ubuntu-latest\n    \
                if: contains(github.event.pull_request.labels.*.name, 'ship')\n    steps:\n      \
                - run: echo \"${{ github.event.pull_request.title }}\"\n";
    let delivered = json!({
        "action": "synchronize",
        "number": 7,
        "pull_request": {
            "number": 7,
            "title": "Make it `rm -rf /` safe",
            "draft": false,
            "labels": [{"name": "ship"}, {"name": "ci"}],
            "user": {"login": "octocat"},
            "head": {"ref": "feature", "sha": "f".repeat(40), "repo": {"full_name": "fork/widgets"}},
            "base": {"ref": "main", "sha": "0".repeat(40)},
        },
        "repository": {"full_name": "acme/widgets", "default_branch": "main", "private": false},
    });
    let event = Event {
        payload: Some(delivered),
        ..pull_request()
    };
    let Lowered { pipeline, skipped } = lowered(text, &event);
    assert!(
        skipped.is_empty(),
        "the job's if: read the labels: {skipped:?}"
    );
    let context = contexts(&pipeline.tasks[0]);
    let got = &context["github"]["event"];
    assert_eq!(got["action"], "synchronize");
    assert_eq!(got["pull_request"]["title"], "Make it `rm -rf /` safe");
    assert_eq!(got["pull_request"]["user"]["login"], "octocat");
    assert_eq!(
        got["pull_request"]["head"]["repo"]["full_name"],
        "fork/widgets"
    );
    assert_eq!(got["repository"]["default_branch"], "main");
    // dagron-ci's own over the delivery's: the commit it runs.
    assert_eq!(got["pull_request"]["head"]["sha"], event.sha.as_str());
    // A title is an outsider's: it reaches the script as a variable.
    let script = pipeline.tasks[0].steps[0].run.clone();
    assert!(!script.contains("rm -rf"), "{script}");

    // One too large for a job's context, its `{{` written out, is left
    // out: dagron-ci's fields alone.
    let mut huge = event.payload.clone().unwrap();
    huge["pull_request"]["body"] = json!("{{".repeat(MAX_EVENT_PAYLOAD_BYTES / 8));
    let too_large = Event {
        payload: Some(huge),
        ..pull_request()
    };
    let Lowered { skipped, .. } = lowered(text, &too_large);
    assert_eq!(
        skipped.len(),
        1,
        "no payload, so no labels: the job is left out"
    );

    // Without a payload, dagron-ci's fields alone.
    let Lowered { pipeline, skipped } = lowered(text, &pull_request());
    assert_eq!(skipped.len(), 1, "no labels, so the job is left out");
    assert!(
        pipeline.tasks.is_empty()
            || contexts(&pipeline.tasks[0])["github"]["event"]["pull_request"]["title"].is_null()
    );
}

/// Where the operator serves dagron-ci's GitHub API door, a job of a
/// repository on github.com reaches GitHub's API there: `github.api_url`,
/// which is its `GITHUB_API_URL`. Elsewhere, and without one, GitHub's own.
#[test]
fn a_job_reaches_github_s_api_at_the_door_where_one_is_served() {
    let text = "on: push\njobs:\n  a:\n    runs-on: ubuntu-latest\n    steps:\n      - run: make\n";
    let mut settings = settings_of();
    let api = |settings: &Settings, event: &Event| {
        let lowered = read(text).lower("ci", event, settings).unwrap();
        contexts(&lowered.pipeline.tasks[0])["github"]["api_url"].clone()
    };
    assert_eq!(api(&settings, &push()), "https://api.github.com");
    settings.api_url = Some("https://ci.example/ci/v1/github-api".into());
    assert_eq!(
        api(&settings, &push()),
        "https://ci.example/ci/v1/github-api"
    );
    let elsewhere = Event {
        forge_host: "git.example".into(),
        ..push()
    };
    assert_eq!(api(&settings, &elsewhere), "https://git.example/api/v3");
}

#[test]
fn a_text_made_inert_is_handed_on_by_dagron_as_it_was_written() {
    let parameters = [
        (BRACES, "{{"),
        ("ci_ref", "REF"),
        ("version", "VERSION"),
        ("n", "3"),
    ];
    // What dagron fills, where nothing is done about it.
    assert_eq!(
        filled("{{ ci_ref }} {{version}} {{ n - 1 }}", &parameters),
        "REF VERSION 2"
    );
    for text in [
        "",
        "plain",
        "{{ ci_ref }}",
        "{{version}}",
        "{{ n - 1 }}",
        "{{ 1 + 1 }}",
        "{{{ ci_ref }}}",
        "{{{{ ci_ref }}}}",
        "{{{{{",
        "{{",
        "{",
        "a{b{{c}}d}",
        "}}{{",
        "{ {{ ci_ref }}",
        "${{ matrix.os }} and ${{ format('{{0}}', github.sha) }}",
        "{{ ci_braces }}",
        "{{ ci_braces }} ci_ref }}",
        "docker ps --format '{{.Names}}{{end}}'",
        "é{{ü}}{{ ci_ref }}",
    ] {
        assert_eq!(filled(&inert(text), &parameters), text, "{text}");
    }
}

#[test]
fn a_double_brace_in_a_workflow_file_is_text_and_so_is_one_a_caller_sends() {
    // What a native pipeline would have filled in by dagron, each where a
    // workflow file can have it: a script, a step's name and variables, a
    // job's variable, a service's. And a caller who sends as an input what
    // dagron would fill with the branch's name, on a branch named to end
    // the JSON it would land in.
    let text = r#"
on:
  workflow_dispatch:
    inputs:
      version:
        default: "1"
concurrency: ci-${{ github.ref }}
jobs:
  build:
    runs-on: ubuntu-latest
    env:
      SEEN: "${{ inputs.version }}"
      LITERAL: "{{ ci_ref }}"
    services:
      db:
        image: docker.io/library/postgres:16
        env:
          NOTE: "{{ ci_sha }}"
    steps:
      - name: "{{ ci_ref }}"
        env:
          PLAIN: "{{ version }}"
          LATE: "${{ github.sha }}-{{ ci_ref }}"
        run: |
          sed "s/{{ version }}/${{ inputs.version }}/" in > out
          docker ps --format '{{.Names}}{{end}}' # {{ ci_ref }} {{ 1 + 1 }}
          echo ${{ github.sha }}
      - run: echo "{{ version }} is the placeholder, and this line is long enough to be cut as a name"
"#;
    let sent = r#"{{ ci_ref }}","x":"{{ version }}"#;
    let event = Event {
        git_ref: r#"refs/heads/x{{ ci_sha }}"},"github":{"sha":"$(forged)"#.into(),
        ..dispatch(&[("version", sent)])
    };
    let Lowered { pipeline, .. } = lowered(text, &event);
    let Outcome::Run(compiled) = compile::compile(&pipeline, &event, &Default::default()).unwrap()
    else {
        panic!("a dispatch runs it");
    };
    let dag =
        dagron_core::dag::DagGraph::from_yaml_with_params(&compiled.yaml, &compiled.parameters)
            .unwrap_or_else(|e| panic!("{e:#}\n{}", compiled.yaml));
    let job = dag.task_spec("build").unwrap();
    let var = |name: &str| {
        job.env
            .iter()
            .find(|e| e.name == name)
            .map(|e| e.value.as_str())
            .unwrap_or_else(|| panic!("no {name}"))
    };
    let script = var("CI_STEP_1_RUN");
    for line in [
        r#"sed "s/{{ version }}/${__CI_E1}/" in > out"#,
        "docker ps --format '{{.Names}}{{end}}' # {{ ci_ref }} {{ 1 + 1 }}",
        "echo ${{ github.sha }}",
    ] {
        assert!(
            script.lines().any(|l| l == line),
            "{line}\nnot in\n{script}"
        );
    }
    assert_eq!(var("CI_STEP_1_EXPR_1"), "inputs.version");
    assert_eq!(var("CI_STEP_1_NAME"), "{{ ci_ref }}");
    assert_eq!(var("CI_STEP_1_ENV_PLAIN"), "{{ version }}");
    assert_eq!(var("CI_STEP_1_ENVX_LATE"), "${{ github.sha }}-{{ ci_ref }}");
    assert_eq!(var("LITERAL"), "{{ ci_ref }}");
    assert_eq!(var("SEEN"), sent);
    // A step with no name is called by the start of its script, cut where
    // it was written and not in the middle of what a brace is written as.
    assert_eq!(
        var("CI_STEP_2_NAME"),
        r#"echo "{{ version }} is the placeholder, and this line is lon…"#
    );
    assert_eq!(
        job.services[0]
            .env
            .iter()
            .find(|e| e.name == "NOTE")
            .unwrap()
            .value,
        "{{ ci_sha }}"
    );
    // The contexts are the JSON they were written as: the input is the text
    // that was sent, and the commit is the event's.
    let context: Value = serde_json::from_str(var("CI_CONTEXT")).unwrap();
    assert_eq!(context["inputs"]["version"], sent);
    assert_eq!(context["github"]["ref"], event.git_ref.as_str());
    assert_eq!(context["github"]["sha"], event.sha.as_str());
    assert_eq!(context["env"]["LITERAL"], "{{ ci_ref }}");
    // And a group named by the branch is not refused for how somebody named
    // a branch. dagron takes no key with a `{{` left in it, so a group's
    // braces are written another way, which keeps groups apart.
    assert_eq!(
        dag.spec.concurrency_key.as_deref(),
        Some(
            "github.com/acme/widgets:\
             ci-refs/heads/x%7B%7B ci_sha }}\"},\"github\":%7B\"sha\":\"$(forged)"
        )
    );
    assert_eq!(group_key("100%{{a}}"), "100%25%7B%7Ba}}");
    assert_ne!(group_key("{"), group_key("%7B"));
}

#[test]
fn a_job_hands_its_outputs_to_the_jobs_that_need_it() {
    let text = r#"
on: push
jobs:
  detect:
    runs-on: ubuntu-latest
    outputs:
      changed: ${{ steps.find.outputs.changed }}
      note: "{{ ci_ref }}"
    steps:
      - id: find
        run: echo "changed=true" >> "$GITHUB_OUTPUT"
  build:
    needs: detect
    if: needs.detect.outputs.changed == 'true'
    runs-on: ubuntu-latest
    strategy:
      matrix:
        part: [one, two]
    steps:
      - run: echo "building ${{ needs.detect.outputs.changed }}"
        env:
          CHANGED: ${{ needs.detect.outputs.changed }}
  never:
    if: github.event_name == 'release'
    runs-on: ubuntu-latest
    steps:
      - run: echo never
  after:
    needs: [build, never]
    if: always()
    runs-on: ubuntu-latest
    steps:
      - run: echo "${{ needs.build.result }} and ${{ needs.never.result }}"
"#;
    let event = push();
    let Lowered { pipeline, skipped } = lowered(text, &event);
    let names: Vec<&str> = pipeline.tasks.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(names, ["detect", "build.one", "build.two", "after"]);
    assert_eq!(skipped.len(), 1, "{skipped:?}");
    assert_eq!(skipped[0].0, "never");
    let task = |name: &str| pipeline.tasks.iter().find(|t| t.name == name).unwrap();
    let written = |text: &str| filled(text, &[(BRACES, "{{")]);

    // What a job hands on is the step runner's to read, as the job ends.
    let detect = task("detect").job.as_ref().unwrap();
    assert_eq!((detect.condition.as_ref(), detect.needs.len()), (None, 0));
    let outputs: Vec<(String, String)> = detect
        .outputs
        .iter()
        .map(|(name, value)| (name.clone(), written(value)))
        .collect();
    assert_eq!(
        outputs,
        [
            (
                "changed".to_string(),
                "${{ steps.find.outputs.changed }}".to_string()
            ),
            ("note".to_string(), "{{ ci_ref }}".to_string()),
        ]
    );

    // A job whose `if:` reads `needs` is started whatever became of the job
    // it needs, and decided by the step runner then. What it reads of the
    // other job in a script is a variable's value, never pasted.
    let build = task("build.one");
    assert_eq!(build.depends_on, ["detect"]);
    assert_eq!(build.fields["trigger_rule"], "all_done");
    let job = build.job.as_ref().unwrap();
    assert_eq!(
        job.condition.as_deref(),
        Some("needs.detect.outputs.changed == 'true'")
    );
    assert_eq!(
        job.needs,
        [("detect".to_string(), Some("detect".to_string()))]
    );
    assert_eq!(build.steps[0].run, "echo \"building ${__CI_E1}\"");
    assert_eq!(build.steps[0].exprs, ["needs.detect.outputs.changed"]);
    assert_eq!(
        written(&build.steps[0].late_env["CHANGED"]),
        "${{ needs.detect.outputs.changed }}"
    );

    // A job with a matrix is each of its tasks to the job that needs it,
    // and a job left out when the run was made is told as one with no task.
    let after = task("after");
    assert_eq!(after.depends_on, ["build.one", "build.two"]);
    assert_eq!(
        after.job.as_ref().unwrap().needs,
        [
            ("build".to_string(), Some("build.one".to_string())),
            ("build".to_string(), Some("build.two".to_string())),
            ("never".to_string(), None),
        ]
    );

    // And dagron takes what it compiles to: each needed task's status and
    // last line are the engine's to give, as the job starts.
    let Outcome::Run(compiled) = compile::compile(&pipeline, &event, &Default::default()).unwrap()
    else {
        panic!("a push runs it");
    };
    let dag =
        dagron_core::dag::DagGraph::from_yaml_with_params(&compiled.yaml, &compiled.parameters)
            .unwrap_or_else(|e| panic!("{e:#}\n{}", compiled.yaml));
    let var = |task: &str, name: &str| {
        dag.task_spec(task)
            .unwrap()
            .env
            .iter()
            .find(|e| e.name == name)
            .map(|e| e.value.clone())
    };
    let s = |text: &str| Some(text.to_string());
    for job in ["detect", "build.one", "after"] {
        assert_eq!(var(job, "CI_STEP_PROTOCOL"), s("3"), "{job}");
        assert!(dag.task_spec(job).unwrap().command[2].contains("--speaks 3"));
    }
    assert_eq!(var("detect", "CI_OUTPUT_COUNT"), s("2"));
    assert_eq!(var("detect", "CI_OUTPUT_1_NAME"), s("changed"));
    assert_eq!(
        var("detect", "CI_OUTPUT_1_VALUE"),
        s("${{ steps.find.outputs.changed }}")
    );
    assert_eq!(var("detect", "CI_OUTPUT_2_VALUE"), s("{{ ci_ref }}"));
    assert_eq!(var("detect", "CI_NEED_COUNT"), None);
    assert_eq!(
        var("build.one", "CI_JOB_IF"),
        s("needs.detect.outputs.changed == 'true'")
    );
    assert_eq!(var("build.one", "CI_NEED_COUNT"), s("1"));
    assert_eq!(var("build.one", "CI_NEED_1_JOB"), s("detect"));
    assert_eq!(
        var("build.one", "CI_NEED_1_STATUS"),
        None,
        "the engine's to give"
    );
    use dagron_core::dag::{EnvFrom, Read};
    let given = |name: &str, task: &str, read| EnvFrom {
        name: name.into(),
        task: task.into(),
        read,
    };
    assert_eq!(
        dag.task_spec("build.one").unwrap().env_from,
        [
            given("CI_NEED_1_STATUS", "detect", Read::Status),
            given("CI_NEED_1_SAID", "detect", Read::LastLine),
        ]
    );
    assert_eq!(var("after", "CI_JOB_IF"), s("always()"));
    assert_eq!(var("after", "CI_NEED_COUNT"), s("3"));
    assert_eq!(var("after", "CI_NEED_2_JOB"), s("build"));
    assert_eq!(var("after", "CI_NEED_3_JOB"), s("never"));
    assert_eq!(var("after", "CI_NEED_3_STATUS"), s("skipped"));
    assert_eq!(
        dag.task_spec("after").unwrap().env_from,
        [
            given("CI_NEED_1_STATUS", "build.one", Read::Status),
            given("CI_NEED_1_SAID", "build.one", Read::LastLine),
            given("CI_NEED_2_STATUS", "build.two", Read::Status),
            given("CI_NEED_2_SAID", "build.two", Read::LastLine),
        ]
    );
}

#[test]
fn a_checkout_step_says_how_much_history_the_job_s_workspace_has() {
    let text = r#"
on: push
jobs:
  alone:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
      - run: make
  one:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with: { fetch-depth: 1 }
      - run: make
  all:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with: { fetch-depth: 0 }
      - run: git merge-base HEAD origin/main
  some:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with: { fetch-depth: "50" }
      - uses: actions/checkout@v4
        with: { fetch-depth: 3 }
      - run: git log
  both:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v4
        with: { fetch-depth: 20 }
      - uses: actions/checkout@v4
        with: { fetch-depth: 0 }
      - run: git log
"#;
    let event = push();
    let Lowered { pipeline, .. } = lowered(text, &event);
    let Outcome::Run(compiled) = compile::compile(&pipeline, &event, &Default::default()).unwrap()
    else {
        panic!("a push runs it");
    };
    let dag =
        dagron_core::dag::DagGraph::from_yaml_with_params(&compiled.yaml, &compiled.parameters)
            .unwrap_or_else(|e| panic!("{e:#}\n{}", compiled.yaml));
    let depth = |job: &str| {
        let checkout = dag
            .task_spec(job)
            .unwrap()
            .workspace
            .clone()
            .unwrap()
            .checkout;
        let checkout = checkout.expect("every job's workspace starts with the commit");
        assert_eq!(checkout.commit, event.sha, "{job}");
        checkout.depth
    };
    // The commit alone where nothing is asked, or what is asked is that.
    assert_eq!(depth("alone"), None);
    assert_eq!(depth("one"), None);
    // All of it, and a number of commits. A job has one workspace: of two
    // checkouts it has the more history, and 0 is all of it.
    assert_eq!(depth("all"), Some(0));
    assert_eq!(depth("some"), Some(50));
    assert_eq!(depth("both"), Some(0));
}

#[test]
fn a_text_made_inert_twice_is_handed_on_by_both_of_dagron_s_fillings() {
    // The run's parameters, and then an instance's own: what a task that is
    // made as the run goes is filled with, in that order.
    let run = [(BRACES, "{{"), ("ci_ref", "REF"), ("item", "RUN-ITEM")];
    let instance = [
        ("item", "ITEM"),
        ("item.b", "{{"),
        ("item.label", "LABEL"),
        ("item.matrix", "MATRIX"),
        ("index", "7"),
    ];
    // What the second filling does to text that was written for one.
    assert_eq!(
        filled(&filled(&inert("{{ item }} {{ index }}"), &run), &instance),
        "ITEM 7"
    );
    for text in [
        "",
        "plain",
        "{{ item }}",
        "{{ index }} {{ item.label }} {{ item.matrix }} {{ item.b }}",
        "{{ ci_ref }} {{ ci_braces }}",
        "{{ index + 1 }} {{ 1 + 1 }}",
        "{{{ item }}}",
        "{{{{",
        "{",
        "}}{{",
        "${{ matrix.module }} and {{.Names}}{{end}}",
        "{{ ci_braces }} item.b }}",
        "é{{ü}}{{ item }}",
    ] {
        let once = filled(&inert_twice(text), &run);
        assert_eq!(filled(&once, &instance), text, "{text}");
    }
}

#[test]
fn a_matrix_an_earlier_job_computes_is_planned_as_the_run_goes() {
    let text = r#"
on: push
jobs:
  find:
    runs-on: ubuntu-latest
    outputs:
      list: ${{ steps.ls.outputs.list }}
    steps:
      - id: ls
        run: echo 'list=["core","api"]' >> "$GITHUB_OUTPUT"
  build:
    needs: find
    if: needs.find.outputs.list != '[]'
    runs-on: ubuntu-latest
    strategy:
      fail-fast: false
      matrix:
        os: [linux]
        module: ${{ fromJSON(needs.find.outputs.list) }}
    steps:
      - name: Build ${{ matrix.module }}
        working-directory: crates/${{ matrix.module }}
        run: |
          echo "building ${{ matrix.module }} on ${{ matrix.os }}"
          echo '{{ item }} {{ index }} {{ item.label }} {{ ci_ref }} {{.Names}}'
  after:
    needs: build
    runs-on: ubuntu-latest
    steps:
      - run: echo "build ${{ needs.build.result }}"
"#;
    let event = push();
    let Lowered { pipeline, skipped } = lowered(text, &event);
    assert!(skipped.is_empty(), "{skipped:?}");
    let names: Vec<&str> = pipeline.tasks.iter().map(|t| t.name.as_str()).collect();
    assert_eq!(names, ["find", "build.strategy.matrix", "build", "after"]);
    let Outcome::Run(compiled) = compile::compile(&pipeline, &event, &Default::default()).unwrap()
    else {
        panic!("a push runs it");
    };
    let dag =
        dagron_core::dag::DagGraph::from_yaml_with_params(&compiled.yaml, &compiled.parameters)
            .unwrap_or_else(|e| panic!("{e:#}\n{}", compiled.yaml));
    let var = |spec: &dagron_core::dag::TaskSpec, name: &str| {
        spec.env
            .iter()
            .find(|e| e.name == name)
            .map(|e| e.value.clone())
    };
    let s = |text: &str| Some(text.to_string());

    // The task that plans: started as the job would be, with what the job
    // it needs came to, and the matrix as it is written.
    let planner = dag.task_spec("build.strategy.matrix").unwrap();
    assert_eq!(planner.depends_on, ["find"]);
    assert!(planner.command[2].contains("exec \"$ci_exec\" matrix"));
    assert_eq!(
        planner.docker_image.as_deref(),
        Some("docker.io/library/ubuntu:24.04")
    );
    assert!(
        planner.workspace.as_ref().unwrap().checkout.is_none(),
        "the step runner is installed in a workspace, and it needs no commit"
    );
    assert_eq!(
        serde_json::from_str::<Value>(&var(planner, "CI_MATRIX_PLAN").unwrap()).unwrap(),
        json!({
            "axes": [["os", ["linux"]], ["module", { "expr": "fromJSON(needs.find.outputs.list)" }]],
            "include": [],
            "exclude": [],
            "fail_fast": false,
        })
    );
    assert_eq!(
        var(planner, "CI_JOB_IF"),
        s("needs.find.outputs.list != '[]'")
    );
    assert_eq!(var(planner, "CI_NEED_1_JOB"), s("find"));
    assert_eq!(planner.env_from.len(), 2);

    // The job: a task of each instance the planner prints, made by the
    // engine as the run goes.
    let build = dag.task_spec("build").unwrap();
    assert_eq!(
        build.with_output_of.as_deref(),
        Some("build.strategy.matrix")
    );
    assert_eq!(build.depends_on, ["build.strategy.matrix", "find"]);
    assert_eq!(build.trigger_rule.as_deref(), Some("all_done"));
    let printed = json!([
        { "label": "linux-core", "matrix": { "os": "linux", "module": "core" },
          "strategy": { "fail-fast": false, "job-index": 0, "job-total": 2 }, "b": "{{" },
        { "label": "linux-api", "matrix": { "os": "linux", "module": "api" },
          "strategy": { "fail-fast": false, "job-index": 1, "job-total": 2 }, "b": "{{" },
    ])
    .to_string();
    let made = dagron_core::expand::fanout_instances(build, &[("build.strategy.matrix", &printed)])
        .unwrap_or_else(|e| panic!("{e:#}"));
    let names: Vec<&str> = made.iter().map(|i| i.name.as_str()).collect();
    assert_eq!(names, ["build.linux-core", "build.linux-api"]);
    let api = &made[1].spec;
    // It is told which it is, and its text is what the workflow wrote: the
    // engine's own names in it are text, and the matrix is the runner's.
    assert_eq!(
        serde_json::from_str::<Value>(&var(api, "CI_MATRIX").unwrap()).unwrap(),
        json!({ "os": "linux", "module": "api" })
    );
    assert_eq!(
        serde_json::from_str::<Value>(&var(api, "CI_STRATEGY").unwrap()).unwrap()["job-index"],
        1
    );
    assert_eq!(var(api, "CI_STEP_PROTOCOL"), s("3"));
    assert_eq!(var(api, "CI_STEP_1_NAME"), s("Build ${{ matrix.module }}"));
    assert_eq!(
        var(api, "CI_STEP_1_WORKDIR"),
        s("crates/${{ matrix.module }}")
    );
    assert_eq!(
        var(api, "CI_STEP_1_RUN"),
        s(
            "echo \"building ${{ matrix.module }} on ${{ matrix.os }}\"\n\
           echo '{{ item }} {{ index }} {{ item.label }} {{ ci_ref }} {{.Names}}'\n"
        )
    );
    let context: Value = serde_json::from_str(&var(api, "CI_CONTEXT").unwrap()).unwrap();
    assert_eq!(context["github"]["sha"], event.sha.as_str());
    assert!(
        context["matrix"].is_null(),
        "it is told its matrix as it is made"
    );
    assert_eq!(var(api, "CI_JOB_IF"), s("needs.find.outputs.list != '[]'"));
    assert_eq!(api.env_from.len(), 2, "what `find` came to, as it starts");

    // The job after it needs the task its instances are made under.
    let after = dag.task_spec("after").unwrap();
    assert_eq!(after.depends_on, ["build"]);
    assert_eq!(var(after, "CI_NEED_1_JOB"), s("build"));

    // What is not taken of such a matrix, by name.
    let with = |matrix: &str, more: &str| {
        format!(
            "on: push\njobs:\n  find:\n    runs-on: ubuntu-latest\n    steps: [{{ run: x }}]\n  \
             build:\n    needs: find\n    runs-on: ubuntu-latest\n{more}    strategy:\n      matrix:\n{matrix}    \
             steps: [{{ run: y }}]\n"
        )
    };
    for (matrix, more, why) in [
        (
            "        m: \"a-${{ needs.find.outputs.x }}\"\n",
            "",
            "more than one expression, or text with one",
        ),
        (
            "        m: [\"${{ needs.find.outputs.x }}\"]\n",
            "",
            "inside a matrix's values or rows",
        ),
        (
            "        m: ${{ fromJSON(needs.other.outputs.x) }}\n",
            "",
            "does not name other in its `needs:`",
        ),
        (
            "        m: ${{ fromJSON(matrix.x) }}\n",
            "",
            "reads `matrix`",
        ),
        (
            "        m: ${{ fromJSON(needs.find.outputs.x) }}\n",
            "    outputs: { a: b }\n",
            "whose matrix an earlier job computes",
        ),
    ] {
        let found = match parse(&with(matrix, more), &settings_of().provided) {
            Err(problems) => problems,
            Ok(workflow) => workflow
                .lower("ci", &push(), &settings_of())
                .err()
                .unwrap_or_else(|| panic!("{matrix:?} was taken")),
        };
        let found: Vec<String> = found.iter().map(ToString::to_string).collect();
        assert!(
            found.iter().any(|p| p.contains(why)),
            "{why:?} not among {found:#?}"
        );
    }
}

#[test]
fn a_workflow_may_not_choose_the_ref_its_actions_cache_restores_from() {
    // At the workflow's level, a job's or a step's: the compiler writes it,
    // from the event, after them, and one chosen here would read another
    // branch's entries.
    for text in [
        "on: push\nenv:\n  DAGRON_ACTIONS_CACHE_RESTORE_FROM: refs/heads/release\njobs:\n  build:\n    runs-on: ubuntu-latest\n    steps:\n      - uses: actions/cache@v4\n        with: { path: x, key: k }\n",
        &with_job(
            "runs-on: ubuntu-latest\nenv:\n  DAGRON_ACTIONS_CACHE_RESTORE_FROM: refs/heads/release\nsteps:\n  - run: x",
        ),
        &with_job(
            "runs-on: ubuntu-latest\nsteps:\n  - run: x\n    env:\n      DAGRON_ACTIONS_CACHE_RESTORE_FROM: refs/heads/release",
        ),
    ] {
        refused(text, "is dagron-ci's own");
    }
}
