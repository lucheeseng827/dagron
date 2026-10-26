use super::*;

/// The plan's own example, less what v1 does not take yet (services,
/// workspace caches, workload identity).
const ENGINE: &str = r#"
name: engine-ci
on:
  pull_request: { branches: [main], paths: ["crates/**", "Cargo.*"] }
  push:         { branches: [main], paths: ["crates/**", "Cargo.*"] }
  merge_group:  {}
concurrency: { key: "{{ ci_group }}" }
task_defaults: { runner_class: linux-amd64, timeout_secs: 2700 }
environment: release
tasks:
  - name: lint
    docker_image: rust:1.86-bookworm
    steps:
      - run: cargo fmt --all -- --check
      - run: cargo clippy --workspace -- -D warnings
  - name: affected
    docker_image: rust:1.86-bookworm
    steps:
      - run: scripts/affected-crates.sh "$CI_BASE_SHA" "$CI_SHA"
  - name: test
    depends_on: [lint, affected]
    with_output_of: affected
    docker_image: rust:1.86-bookworm
    steps:
      - name: unit
        run: cargo test -p "$ITEM" -- --test-threads=1
        timeout_secs: 900
        env: { RUST_BACKTRACE: 1 }
      - run: scripts/flaky.sh
        continue_on_error: true
    report: { junit: target/junit.xml }
  - name: publish
    depends_on: [test]
    when: "{{ ci_event }} == push"
    steps:
      - run: scripts/push-image.sh
"#;

fn problems(text: &str) -> Vec<String> {
    match parse(text) {
        Ok(p) => panic!("parsed, but should not have: {p:?}"),
        Err(problems) => problems.iter().map(ToString::to_string).collect(),
    }
}

/// The one problem `text` has, which must mention `needle`.
fn refused(text: &str, needle: &str) {
    let found = problems(text);
    assert!(
        found.iter().any(|p| p.contains(needle)),
        "{needle:?} not among {found:#?}"
    );
}

/// A job may ask for a token for the repository it checks out, by a GitHub
/// App's names, each read or write; one that checks out nothing may not.
#[test]
fn a_job_asks_for_a_forge_token_for_the_repository_it_checks_out() {
    let p = parse(&with_tasks(
        "  - { name: pr, forge_token: { permissions: { contents: write, pull_requests: write } }, steps: [{ run: x }] }\n  \
         - { name: c, forge_token: { permissions: { contents: read } }, command: [\"true\"], workspace: {} }",
    ))
    .unwrap_or_else(|e| panic!("{e:#?}"));
    assert_eq!(
        p.tasks[0].fields["forge_token"],
        serde_json::json!({ "permissions": { "contents": "write", "pull_requests": "write" } })
    );
    for (task, says) in [
        ("forge_token: { permissions: {} }, steps: [{ run: x }]", "each of actions, checks, contents"),
        ("forge_token: { permissions: { contents: admin } }, steps: [{ run: x }]", "at read or write"),
        ("forge_token: { permissions: { secrets: read } }, steps: [{ run: x }]", "at read or write"),
        ("forge_token: { contents: write }, steps: [{ run: x }]", "is `{ permissions"),
        ("forge_token: { permissions: { contents: read } }, command: [\"true\"]", "checks out none"),
        (
            "forge_token: { permissions: { contents: read } }, steps: [{ run: x }], workspace: { checkout: false }",
            "checks out none",
        ),
        (
            "forge_token: { permissions: { contents: read } }, steps: [{ run: x }], workspace: null",
            "checks out none",
        ),
    ] {
        refused(&with_tasks(&format!("  - {{ name: a, {task} }}")), says);
    }
}

/// A task's `env:` is dagron's, a list of names and values, and a step's is
/// a map. A task's written as a map was left out of the workflow without a
/// word: the compiler adds the steps to a list, and to nothing else.
#[test]
fn a_task_s_env_is_a_list_as_dagron_reads_it() {
    refused(
        &with_tasks("  - { name: a, env: { FOO: bar }, steps: [{ run: x }] }"),
        "tasks[0] (a).env: is a list of names and values",
    );
    refused(
        "name: p\non: { push: }\ntask_defaults: { env: { FOO: bar } }\n\
         tasks:\n  - { name: a, steps: [{ run: x }] }\n",
        "task_defaults.env: is a list of names and values",
    );
    // task_defaults itself, written as anything but a mapping, would have
    // been left out whole.
    refused(
        "name: p\non: { push: }\ntask_defaults: [{ timeout_secs: 60 }]\n\
         tasks:\n  - { name: a, steps: [{ run: x }] }\n",
        "task_defaults: must be a mapping",
    );
    refused(
        &with_tasks("  - { name: a, env: [{ value: bar }], steps: [{ run: x }] }"),
        "tasks[0] (a).env[0]: is a name and a value",
    );
    refused(
        &with_tasks("  - { name: a, env: [FOO=bar], steps: [{ run: x }] }"),
        "tasks[0] (a).env[0]: is a name and a value",
    );
    // Each in its own form, both are taken; and no env at all is none.
    let both = with_tasks(
        "  - { name: a, env: [{ name: FOO, value: bar }], steps: [{ run: x, env: { BAR: baz } }] }",
    );
    assert!(parse(&both).is_ok(), "{:?}", parse(&both));
    let none = with_tasks("  - { name: a, env: null, steps: [{ run: x }] }");
    assert!(parse(&none).is_ok(), "{:?}", parse(&none));
}

/// A minimal pipeline with `tasks` spliced in.
fn with_tasks(tasks: &str) -> String {
    format!("name: p\non: {{ push: }}\nparameters: {{ version: \"1\" }}\ntasks:\n{tasks}")
}

#[test]
fn the_plans_example_reads_into_the_ir() {
    let p = parse(ENGINE).unwrap();
    assert_eq!(p.name, "engine-ci");
    let push = p.on.push.as_ref().unwrap();
    assert!(push.branches.as_ref().unwrap().matches("main"));
    assert!(!push.branches.as_ref().unwrap().matches("feature/x"));
    assert!(push.tags.is_none());
    assert!(push
        .paths
        .as_ref()
        .unwrap()
        .any(&["crates/dagron-ci/src/lib.rs"]));
    assert!(!push.paths.as_ref().unwrap().any(&["docs/README.md"]));
    assert!(p.on.pull_request.is_some() && p.on.merge_group.is_some());
    assert!(p.on.dispatch.is_none());
    assert_eq!(
        p.concurrency,
        Some(Concurrency {
            key: "{{ ci_group }}".into(),
            cancel_superseded: false
        })
    );
    assert_eq!(
        p.tasks.iter().map(|t| t.name.as_str()).collect::<Vec<_>>(),
        ["lint", "affected", "test", "publish"]
    );
    let test = &p.tasks[2];
    assert_eq!(test.depends_on, ["lint", "affected"]);
    assert_eq!(test.fields["with_output_of"], "affected");
    assert_eq!(test.steps[0].name.as_deref(), Some("unit"));
    assert_eq!(test.steps[0].timeout_secs, Some(900));
    assert_eq!(test.steps[0].env["RUST_BACKTRACE"], "1");
    assert!(test.steps[1].continue_on_error);
    assert_eq!(test.reports.junit, ["target/junit.xml"]);
    // dagron's keys pass through, as written; the pipeline's own do not.
    assert_eq!(p.spec["environment"], "release");
    assert_eq!(p.spec["task_defaults"]["timeout_secs"], 2700);
    assert!(!p.spec.contains_key("on") && !p.spec.contains_key("tasks"));
    assert!(!test.fields.contains_key("steps") && !test.fields.contains_key("depends_on"));
}

#[test]
fn on_is_a_key_not_a_boolean() {
    // YAML 1.1 reads a bare `on` as true; the pipeline format is YAML 1.2.
    let p =
        parse("name: p\non:\n  push:\ntasks:\n  - { name: a, steps: [{ run: make }] }\n").unwrap();
    assert!(p.on.push.is_some());
}

#[test]
fn what_the_file_as_a_whole_must_be() {
    refused("- a list", "a mapping with name, on and tasks");
    refused(": : :", "not YAML");
    let missing = problems("{}");
    for key in ["name", "on", "tasks"] {
        assert!(missing.iter().any(|p| p.starts_with(key)), "{missing:?}");
    }
    refused(
        &with_tasks("  []").replace("name: p", "name: \"a b\""),
        "name",
    );
    refused(&with_tasks("  []"), "needs at least one task");
    refused(
        &with_tasks("  - { name: a, steps: [{ run: x }] }").replace("tasks:", "taks: 1\ntasks:"),
        "did you mean \"tasks\"",
    );
    refused(
        &with_tasks("  - { name: a, steps: [{ run: x }] }")
            .replace("tasks:", "concurrency_key: k\ntasks:"),
        "concurrency: { key: … }",
    );
}

#[test]
fn concurrency_takes_a_key_and_needs_a_cap() {
    let with = |top: &str| {
        with_tasks("  - { name: a, steps: [{ run: x }] }")
            .replace("tasks:", &format!("{top}\ntasks:"))
    };
    assert!(parse(&with(
        "concurrency: { key: \"{{ ci_group }}\" }\nmax_active_runs: 2"
    ))
    .is_ok());
    // A newer run may take the place of the one before it: of one run, so
    // the cap is 1 or left out. What a full key does is said so, and no
    // other way.
    for taken in [
        "concurrency: { key: k, cancel_superseded: true }",
        "concurrency: { key: k, cancel_superseded: true }\nmax_active_runs: 1",
        "concurrency: { key: k, cancel_superseded: false }\nmax_active_runs: 3",
    ] {
        assert!(parse(&with(taken)).is_ok(), "{taken}");
    }
    refused(
        &with("concurrency: { key: k, cancel_superseded: true }\nmax_active_runs: 2"),
        "must be 1, or left out, beside concurrency.cancel_superseded",
    );
    refused(
        &with("concurrency: { key: k }\nwhen_full: queue"),
        "when_full: is dagron-ci's to say",
    );
    refused(
        &with("concurrency: { key: k }\nmax_active_runs: 0"),
        "at least 1 beside concurrency",
    );
    refused(&with("concurrency: { key: \" \" }"), "concurrency.key");
    assert!(
        parse(&with("max_active_runs: 0")).is_ok(),
        "no key, no cap to need"
    );
}

#[test]
fn on_takes_known_events_and_sound_filters() {
    let pipeline =
        |on: &str| format!("name: p\non: {on}\ntasks:\n  - {{ name: a, steps: [{{ run: x }}] }}\n");
    refused(&pipeline("{}"), "at least one event");
    refused(
        &pipeline("{ pull_requests: }"),
        "did you mean \"pull_request\"",
    );
    refused(
        &pipeline("{ push: { branch: [main] } }"),
        "did you mean \"branches\"",
    );
    refused(
        &pipeline("{ push: { branches: [main], branches_ignore: [x] } }"),
        "not both",
    );
    refused(&pipeline("{ push: { branches: [] } }"), "is empty");
    refused(
        &pipeline("{ push: { branches: [\"[main\"] } }"),
        "unclosed [",
    );
    refused(
        &pipeline("{ push: { branches: [\"!main\"] } }"),
        "nothing could match",
    );
    refused(
        &pipeline("{ push: { paths_ignore: [\"!x\"] } }"),
        "no ! patterns",
    );
    refused(
        &pipeline("{ merge_group: { paths: [x] } }"),
        "merge_group has no key",
    );
    refused(
        &pipeline("{ dispatch: { inputs: { ci_x: {} } } }"),
        "not starting ci_",
    );
    refused(
        &pipeline("{ dispatch: { inputs: { x: { required: maybe } } } }"),
        "true or false",
    );

    let p = parse(&pipeline(
        "{ push: { tags: [\"v*\"] }, dispatch: { inputs: { channel: { default: beta, required: true }, n: { default: 3 } } } }",
    ))
    .unwrap();
    let push = p.on.push.unwrap();
    assert!(push.branches.is_none() && push.tags.unwrap().matches("v1.2"));
    let inputs = p.on.dispatch.unwrap().inputs;
    assert_eq!(inputs["channel"].default.as_deref(), Some("beta"));
    assert!(inputs["channel"].required);
    assert_eq!(inputs["n"].default.as_deref(), Some("3"));
}

/// A task's steps travel in its environment: at most 1000 of them, carrying
/// at most 512 KiB between them, and compiled to a command, one argument,
/// under the kernel's 128 KiB.
#[test]
fn a_task_s_steps_fit_in_its_environment() {
    let task = |steps: &str| with_tasks(&format!("  - name: a\n    steps:\n{steps}"));
    let many = |n: usize, run: &str| task(&format!("      - run: {run}\n").repeat(n));
    assert!(parse(&many(MAX_STEPS, "x")).is_ok());
    refused(&many(MAX_STEPS + 1, "x"), "a task may have at most 1000");
    let big = "x".repeat(60 * 1024);
    assert!(parse(&many(8, &big)).is_ok(), "480 KiB");
    refused(
        &many(9, &big),
        "540 KiB of scripts and variables, over the 512 KiB",
    );
    // Each variable a step sets lengthens its line of the command.
    let vars = "        env: { A_LONG_VARIABLE_NAME_ONE: x, A_LONG_VARIABLE_NAME_TWO: y }\n";
    let with_vars = |n: usize| task(&format!("      - run: x\n{vars}").repeat(n));
    assert!(parse(&with_vars(300)).is_ok());
    refused(&with_vars(MAX_STEPS), "KiB command, over the 128 KiB");
}

#[test]
fn a_task_takes_dagron_keys_and_steps() {
    refused(
        &with_tasks("  - { name: a, stpes: [{ run: x }] }"),
        "did you mean \"steps\"",
    );
    // A task's reports are the pipeline's own key: read here, and no key
    // of dagron's task.
    let reported = with_tasks(
        "  - { name: a, steps: [{ run: x }], report: { junit: target/junit.xml, sarif: [a.sarif, lint/b.sarif] } }",
    );
    let read = parse(&reported).unwrap_or_else(|e| panic!("{e:?}"));
    assert_eq!(
        read.tasks[0].reports,
        Reports {
            junit: vec!["target/junit.xml".into()],
            sarif: vec!["a.sarif".into(), "lint/b.sarif".into()],
        }
    );
    assert!(
        !read.tasks[0].fields.contains_key("report"),
        "not dagron's to see"
    );
    for (report, why) in [
        ("{ junit: ../out.xml }", "is not a path in the workspace"),
        ("{ junit: /tmp/out.xml }", "is not a path in the workspace"),
        ("{ junit: 'a b.xml' }", "is not a path in the workspace"),
        ("{ junit: [] }", "names 1 to 8 files"),
        (
            "{ junit: [a, b, c, d, e, f, g, h, i] }",
            "names 1 to 8 files",
        ),
        ("{ tap: out.tap }", "a report has no key \"tap\""),
        ("junit.xml", "must be a mapping"),
        ("{ junit: 4 }", "must be a list of strings"),
    ] {
        refused(
            &with_tasks(&format!(
                "  - {{ name: a, steps: [{{ run: x }}], report: {report} }}"
            )),
            why,
        );
    }
    refused(
        &with_tasks("  - { name: a, command: [x], report: { junit: t.xml } }"),
        "a task with no steps writes no report",
    );
    // What the step runner is handed: each kind's paths, one to a line,
    // beside the steps. A task that names none is handed neither variable.
    let event = compile::Event {
        kind: "push".into(),
        git_ref: "refs/heads/main".into(),
        sha: "a".repeat(40),
        trust: "trusted".into(),
        repo: "acme/widgets".into(),
        forge_host: "github.com".into(),
        ..Default::default()
    };
    let compiled = |p: &Pipeline| match compile::compile(p, &event, &Default::default()) {
        Ok(compile::Outcome::Run(c)) => {
            // And dagron takes what it is handed: the key is not its to see.
            dagron_core::dag::DagGraph::from_yaml_with_params(&c.yaml, &c.parameters)
                .unwrap_or_else(|e| panic!("{e:#}"));
            c.yaml
        }
        other => panic!("not compiled: {other:?}"),
    };
    let yaml = compiled(&read);
    assert!(
        yaml.contains("- name: CI_REPORT_JUNIT\n    value: target/junit.xml\n"),
        "{yaml}"
    );
    assert!(
        yaml.contains(
            "- name: CI_REPORT_SARIF\n    value: |-\n      a.sarif\n      lint/b.sarif\n"
        ),
        "{yaml}"
    );
    let plain = parse(&with_tasks("  - { name: a, steps: [{ run: x }] }")).unwrap();
    assert!(!compiled(&plain).contains("CI_REPORT_"));
    // A job's own environment was one of these until dagron took one. It is
    // dagron's key now, and passes through.
    let own = with_tasks("  - { name: a, steps: [{ run: x }], environment: prod }");
    assert!(parse(&own).is_ok(), "{:?}", parse(&own).err());
    refused(
        &with_tasks("  - { name: a, steps: [{ run: x }], command: [make] }"),
        "steps or command, not both",
    );
    refused(
        &with_tasks("  - { name: a, steps: [] }"),
        "at least one step",
    );
    refused(
        &with_tasks("  - { name: a, steps: [{ name: x }] }"),
        "run: is required",
    );
    refused(
        &with_tasks("  - { name: a, steps: [{ run: x, rnu: y }] }"),
        "did you mean \"run\"",
    );
    refused(
        &with_tasks("  - { name: a, steps: [{ run: x, timeout_secs: 0 }] }"),
        "at least 1",
    );
    refused(
        &with_tasks("  - { name: a, steps: [{ run: x, env: { CI_SHA: y } }] }"),
        "set by dagron-ci",
    );
    refused(
        &with_tasks("  - { name: a, steps: [{ run: x, env: { 1x: y } }] }"),
        "not a variable name",
    );
    // A checkout is on or off, or how deep: it names no repository or
    // commit, which are the event's.
    for checkout in [
        "{ depth: -1 }",
        "{ depth: all }",
        "{ depth: 1, commit: abc }",
        "{ repository: \"https://example.com/x.git\" }",
        "{}",
        "deep",
    ] {
        refused(
            &with_tasks(&format!(
                "  - {{ name: a, steps: [{{ run: x }}], workspace: {{ checkout: {checkout} }} }}"
            )),
            "how much of its history comes with it",
        );
    }
    // Nor is one given from an earlier task: a step's script is a CI_
    // variable, and would be whatever that task printed last.
    refused(
        &with_tasks(
            "  - { name: a, steps: [{ run: x }] }\n  - name: b\n    depends_on: [a]\n    \
             steps: [{ run: y }]\n    \
             env_from: [{ name: CI_STEP_1_TIMEOUT_SECS, task: a, read: last_line }]",
        ),
        "CI_STEP_1_TIMEOUT_SECS: CI_ variables are set by dagron-ci",
    );
    let p = parse(&with_tasks(
        "  - { name: a, steps: [{ run: x }] }\n  - name: b\n    depends_on: [a]\n    \
         steps: [{ run: y }]\n    env_from: [{ name: FOUND, task: a, read: last_line }]",
    ))
    .unwrap();
    assert_eq!(p.tasks[1].fields["env_from"][0]["name"], "FOUND");
    refused(
        &with_tasks("  - { name: a, steps: [{ run: x }] }\n  - { name: a, steps: [{ run: y }] }"),
        "a second task",
    );
    // A task with neither is dagron's to judge: an approval, say.
    let p = parse(&with_tasks(
        "  - { name: gate, type: approval, approvers: [\"group:release\"] }",
    ))
    .unwrap();
    assert!(p.tasks[0].steps.is_empty());
    assert_eq!(p.tasks[0].fields["type"], "approval");
}

/// `workspace:` is dagron's, with a size, or null for none, and caches kept
/// between runs, checked as dagron checks them; where a cache restores from
/// is dagron-ci's to say.
#[test]
fn a_task_s_workspace_is_a_size_or_none() {
    let p = parse(&with_tasks(
        "  - { name: a, steps: [{ run: x }], workspace: { size: 20Gi } }\n  - { name: b, steps: [{ run: x }], workspace: null }\n  - { name: c, steps: [{ run: x }], workspace: {} }",
    ))
    .unwrap();
    assert_eq!(p.tasks[0].fields["workspace"]["size"], "20Gi");
    assert_eq!(p.tasks[1].fields["workspace"], Value::Null);
    let p = parse(&with_tasks(
        "  - { name: a, steps: [{ run: x }], workspace: { cache: [{ prefix: cargo, key_files: [Cargo.lock], paths: [/r] }] } }",
    ))
    .unwrap();
    assert_eq!(
        p.tasks[0].fields["workspace"]["cache"][0]["prefix"],
        "cargo"
    );
    for (cache, says) in [
        ("[target]", "is a mapping"),
        ("{ prefix: x }", "is a list of caches"),
        ("[{ prefix: \"a b\", paths: [/r] }]", "is the cache's name"),
        ("[{ prefix: x }]", "names no paths"),
        ("[{ paths: [/r] }]", "names no prefix"),
        ("[{ prefix: x, paths: [] }]", "1 to 16 directories"),
        ("[{ prefix: x, paths: [../up] }]", "none with '..'"),
        (
            "[{ prefix: x, paths: [/r], key_files: [/etc/passwd] }]",
            "files in the workspace",
        ),
        (
            "[{ prefix: x, paths: [/r], restore_from: main }]",
            "dagron-ci's to say",
        ),
        (
            "[{ prefix: x, paths: [/r] }, { prefix: x, paths: [/s] }]",
            "another cache of this job",
        ),
        (
            "[{ prefix: x, paths: [/r], keys: [a] }]",
            "a cache has no key \"keys\"",
        ),
    ] {
        refused(
            &with_tasks(&format!(
                "  - {{ name: a, steps: [{{ run: x }}], workspace: {{ cache: {cache} }} }}"
            )),
            says,
        );
    }
    for size in ["20GB", "0", "-1Gi", "1e3", "\"\""] {
        refused(
            &with_tasks(&format!(
                "  - {{ name: a, steps: [{{ run: x }}], workspace: {{ size: {size} }} }}"
            )),
            "is a size",
        );
    }
    refused(
        &with_tasks("  - { name: a, steps: [{ run: x }], workspace: { szie: 1Gi } }"),
        "did you mean \"size\"",
    );
    refused(
        &with_tasks("  - { name: a, steps: [{ run: x }], workspace: big }"),
        "or null for none",
    );
}

/// dagron-ci's size check and dagron's give one answer: what `validate`
/// passes, a submit is not refused for, and the other way round.
#[test]
fn a_size_is_what_dagron_takes_for_one() {
    for size in [
        "1",
        "512",
        "20Gi",
        "1.5Gi",
        "500M",
        "1k",
        "2Ei",
        ".5Ki",
        "1.",
        "0.1",
        "0",
        "0.0",
        "",
        ".",
        "Gi",
        "-1Gi",
        "1e3",
        "100m",
        "1.2.3",
        "1 Gi",
        "1gi",
        "1GB",
        "1KiB",
        "0.0000000000000000001",
        "999999999999999999999999Ei",
        "340282366920938463463374607431768211455",
    ] {
        let dagron = dagron_core::workspace::quantity_bytes(size).is_some_and(|b| b > 0);
        assert_eq!(super::is_size(size), dagron, "{size:?}");
    }
}

#[test]
fn tasks_form_a_graph() {
    refused(
        &with_tasks("  - { name: a, depends_on: [b], steps: [{ run: x }] }"),
        "names no task: \"b\"",
    );
    refused(
        &with_tasks("  - { name: a, depends_on: [a], steps: [{ run: x }] }"),
        "depends on itself",
    );
    refused(
        &with_tasks(
            "  - { name: a, depends_on: [c], steps: [{ run: x }] }\n  - { name: b, depends_on: [a], steps: [{ run: x }] }\n  - { name: c, depends_on: [b], steps: [{ run: x }] }",
        ),
        "cycle through a, b, c",
    );
    // A call expands into tasks this file does not name: dagron checks those.
    let calls = with_tasks("  - { name: a, depends_on: [build.compile], template: build }")
        .replace("tasks:", "templates: { build: { tasks: [] } }\ntasks:");
    assert!(parse(&calls).is_ok());
}

#[test]
fn a_command_names_nothing_an_outsider_chose() {
    refused(
        &with_tasks("  - { name: a, steps: [{ run: \"git checkout {{ ci_ref }}\" }] }"),
        "read $CI_REF",
    );
    refused(
        &with_tasks("  - { name: a, command: [sh, -c, \"echo {{ ci_ref }}\"] }"),
        "read $CI_REF",
    );
    refused(
        &with_tasks("  - { name: a, when: \"{{ ci_ref }} == main\", steps: [{ run: x }] }"),
        "read $CI_REF",
    );
    refused(
        &with_tasks("  - { name: a, steps: [{ run: \"echo {{ title }}\" }] }"),
        "{{ title }} is not a value",
    );
    // A dispatch input is its caller's text, even when it is also a parameter.
    let input = with_tasks("  - { name: a, steps: [{ run: \"deploy {{ version }}\" }] }").replace(
        "on: { push: }",
        "on: { dispatch: { inputs: { version: {} } } }",
    );
    refused(&input, "{{ version }} is not a value");
    // A task's output is dagron's to read in a when:, and it substitutes it
    // nowhere else: named in a script, it would reach the shell as written.
    refused(
        &with_tasks(concat!(
            "  - { name: a, steps: [{ run: x }] }\n",
            "  - { name: b, depends_on: [a], steps: [{ run: \"echo {{ tasks.a.output }}\" }] }",
        )),
        "tasks[1] (b).steps[0].run: a task's output is read in when:",
    );
    refused(
        &with_tasks(concat!(
            "  - { name: a, steps: [{ run: x }] }\n",
            "  - { name: b, depends_on: [a], command: [sh, -c, \"echo {{ tasks.a.output }}\"] }",
        )),
        "a task's output is read in when:",
    );

    // What dagron-ci or the pipeline itself decides is fine.
    let fine = with_tasks(concat!(
        "  - { name: a, steps: [{ run: \"build {{ ci_sha }} {{ ci_event }} {{ version }} {{ params.version }}\" }] }\n",
        "  - { name: b, depends_on: [a], with_items: [x, y], when: \"{{ tasks.a.output }} == ok\",\n",
        "      steps: [{ run: \"echo {{ item }} {{ index }}\", env: { REF: \"{{ ci_ref }}\" } }] }",
    ));
    assert!(parse(&fine).is_ok(), "{:?}", parse(&fine).err());

    // Not a name dagron would substitute: left alone, as dagron leaves it.
    let helm = with_tasks(
        "  - { name: a, steps: [{ run: \"helm template --set x='{{ .Values.y }}' {{ $z }}\" }] }",
    );
    assert!(parse(&helm).is_ok(), "{:?}", parse(&helm).err());
}

#[test]
fn every_problem_is_reported_at_once() {
    let found = problems(&with_tasks(
        "  - { name: a, stpes: [] }\n  - { name: b, depends_on: [z], steps: [{ run: \"{{ ci_ref }}\" }] }",
    ));
    assert!(found.len() >= 3, "{found:#?}");
    assert!(found.iter().any(|p| p.starts_with("tasks[0] (a).stpes")));
    assert!(found
        .iter()
        .any(|p| p.starts_with("tasks[1] (b).steps[0].run")));
}

#[test]
fn the_schema_names_what_the_parser_takes() {
    let schema: Value = serde_json::from_str(SCHEMA).expect("the schema is JSON");
    let keys = |v: &Value| -> BTreeSet<String> {
        v.as_object().expect("an object").keys().cloned().collect()
    };
    assert_eq!(
        keys(&schema["$defs"]["on"]["properties"]),
        TRIGGERS.iter().map(|s| s.to_string()).collect()
    );
    assert_eq!(
        keys(&schema["$defs"]["step"]["properties"]),
        STEP_KEYS.iter().map(|s| s.to_string()).collect()
    );
    let mut task: BTreeSet<String> = DAGRON_TASK_KEYS.iter().map(|s| s.to_string()).collect();
    task.insert("steps".into());
    task.extend(OWN_TASK_KEYS.iter().map(|s| s.to_string()));
    assert_eq!(keys(&schema["$defs"]["task"]["properties"]), task);
    assert_eq!(
        keys(&schema["$defs"]["task"]["properties"]["report"]["properties"]),
        REPORT_KINDS.iter().map(|s| s.to_string()).collect()
    );
    let mut top: BTreeSet<String> = DAGRON_SPEC_KEYS
        .iter()
        .filter(|k| !matches!(**k, "concurrency_key" | "when_full"))
        .map(|s| s.to_string())
        .collect();
    top.extend(["on".to_string(), "concurrency".to_string()]);
    assert_eq!(keys(&schema["properties"]), top);
}

#[test]
fn the_readme_example_is_a_valid_pipeline() {
    let readme = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/README.md"))
        .expect("the crate README");
    let start = readme
        .find("# .dagron/ci/engine.yaml\n")
        .expect("the README shows a pipeline");
    let block = &readme[start..];
    let block = &block[..block.find("```").expect("the example ends")];
    let p = parse(block).unwrap_or_else(|e| panic!("{e:#?}"));
    assert_eq!(p.tasks.len(), 2);
    assert!(p.on.push.unwrap().tags.unwrap().matches("v2.0.0"));
}

/// A pipeline whose one job has `services` as written: a YAML list, each
/// item on a line of its own.
fn with_services(services: &str) -> String {
    let items: String = services
        .lines()
        .map(|line| format!("      {line}\n"))
        .collect();
    with_tasks(&format!(
        "  - name: a\n    steps: [{{ run: x }}]\n    services:\n{items}"
    ))
}

/// A job's `services:` is dagron's key, and passes through as written. What
/// dagron would refuse of one is found here, and so is what a service's
/// image, arguments and `ready` may not name: each of them is run.
#[test]
fn a_job_s_services_pass_through_and_each_is_checked() {
    let whole = with_services(
        "- name: pg\n  image: \"postgres:{{ version }}-alpine\"\n  args: [postgres, -c, fsync=off]\n  \
         env: [{ name: POSTGRES_PASSWORD, value: pw }, { name: BRANCH, value: \"{{ ci_ref }}\" }]\n  \
         ready: pg_isready -h localhost -U postgres\n  ready_timeout_secs: 90\n  \
         resources: { requests: { cpu: 100m }, limits: { memory: 256Mi } }\n\
         - { name: cache, image: \"redis:7\", ready: [redis-cli, ping] }",
    );
    let p = parse(&whole).unwrap_or_else(|e| panic!("{e:#?}"));
    let services = p.tasks[0].fields["services"]
        .as_array()
        .expect("as written");
    assert_eq!(services.len(), 2);
    assert_eq!(services[0]["image"], "postgres:{{ version }}-alpine");
    assert_eq!(services[1]["ready"][1], "ping");

    for (services, why) in [
        ("- { name: Pg, image: x }", "is required: lowercase letters"),
        (
            "- { name: localhost, image: x }",
            "every container's name for itself",
        ),
        ("- { image: x }", "name: is required"),
        ("- { name: pg }", "image: is required"),
        (
            "- { name: pg, image: x }\n- { name: pg, image: y }",
            "a second service named \"pg\"",
        ),
        (
            "- { name: pg, image: x, ports: [5432] }",
            "has no key \"ports\"",
        ),
        (
            "- { name: pg, image: x, redy: \"true\" }",
            "did you mean \"ready\"",
        ),
        (
            "- { name: pg, image: x, ready: \" \" }",
            "exits 0 once the service is ready",
        ),
        (
            "- { name: pg, image: x, ready: { tcp: 5432 } }",
            "exits 0 once the service is ready",
        ),
        (
            "- { name: pg, image: x, ready_timeout_secs: 5 }",
            "the service has no ready",
        ),
        (
            "- { name: pg, image: x, ready: \"true\", ready_timeout_secs: 601 }",
            "from 1 to 600",
        ),
        (
            "- { name: pg, image: x, args: \"postgres -c x\" }",
            "is a list of strings",
        ),
        (
            "- { name: pg, image: x, env: { A: b } }",
            "is a list of names and values",
        ),
        (
            "- { name: pg, image: x, env: [{ name: A, value_from: { secret: S } }] }",
            "no secret is given to a service",
        ),
        (
            "- { name: pg, image: x, env: [{ name: A, value: [5] }] }",
            "its value is text",
        ),
        (
            "- { name: pg, image: x, resources: { gpu: { count: 1 } } }",
            "has no key \"gpu\"",
        ),
        // What is run may name nothing an outsider chose.
        (
            "- { name: pg, image: \"{{ ci_ref }}\" }",
            "a branch name is its pusher's text",
        ),
        (
            "- { name: pg, image: x, args: [\"--{{ ci_ref }}\"] }",
            "a branch name is its pusher's text",
        ),
        (
            "- { name: pg, image: x, ready: \"probe {{ ci_ref }}\" }",
            "a branch name is its pusher's text",
        ),
        (
            "- { name: pg, image: x, ready: [probe, \"{{ title }}\"] }",
            "is not a value this pipeline may put in a command",
        ),
    ] {
        refused(&with_services(services), why);
    }
    let many: String = (0..=MAX_SERVICES)
        .map(|i| format!("- {{ name: s{i}, image: x }}\n"))
        .collect();
    refused(&with_services(many.trim_end()), "a task may have 8");
    // Beside a command, and a task that runs none has nothing to be beside.
    refused(
        &with_tasks("  - { name: a, type: approval, services: [{ name: pg, image: x }] }"),
        "a task with type: approval runs none",
    );
}

/// `docker_daemon:` is checked as dagron checks it: true or false, on a
/// task that runs a command, and one more service.
#[test]
fn a_task_s_docker_daemon_is_checked_as_dagron_checks_it() {
    assert!(parse(&with_tasks(
        "  - { name: a, docker_daemon: true, steps: [{ run: docker build . }] }"
    ))
    .is_ok());
    refused(
        &with_tasks("  - { name: a, docker_daemon: yes please, steps: [{ run: x }] }"),
        "is true or false",
    );
    refused(
        &with_tasks("  - { name: a, docker_daemon: ~, steps: [{ run: x }] }"),
        "is true or false",
    );
    refused(
        &with_tasks("  - { name: a, type: approval, docker_daemon: true }"),
        "a task with type: approval runs none",
    );
    refused(
        &with_tasks(
            "  - { name: a, docker_daemon: true, services: [{ name: docker, image: x }], steps: [{ run: x }] }",
        ),
        "the task has one of its own",
    );
    let eight: Vec<String> = (0..MAX_SERVICES)
        .map(|i| format!("{{ name: s{i}, image: x }}"))
        .collect();
    refused(
        &with_tasks(&format!(
            "  - {{ name: a, docker_daemon: true, services: [{}], steps: [{{ run: x }}] }}",
            eight.join(", ")
        )),
        "the daemon is one more service",
    );
}

/// dagron-ci cannot link dagron-core, and keeps its own check of a service.
/// What it takes, dagron takes, as the compiled workflow hands it over; and
/// what dagron refuses, it refuses. It is the stricter of the two in the
/// cases marked so, where what was written is not what anyone meant.
#[test]
fn a_service_is_what_dagron_takes_for_one() {
    use dagron_core::service::{self, ServiceSpec};
    let event = compile::Event {
        kind: "push".into(),
        git_ref: "refs/heads/main".into(),
        sha: "a".repeat(40),
        trust: "trusted".into(),
        repo: "acme/widgets".into(),
        forge_host: "github.com".into(),
        ..Default::default()
    };
    let mut wrong = Vec::new();
    for (services, stricter) in [
        ("- { name: pg, image: \"postgres:16\" }", false),
        ("- { name: a-b1, image: x, ready: [check, now] }", false),
        ("- { name: pg, image: x, args: [a, b], env: [{ name: A, value: b }, { name: _B }], ready: \"true\", ready_timeout_secs: 600, resources: { requests: { cpu: 100m }, limits: { memory: 1Gi } } }", false),
        ("- { name: pg, image: x, ready: ~, ready_timeout_secs: ~, resources: ~ }", false),
        ("- { name: pg, image: x, resources: {} }", false),
        ("- { name: pg, image: x, args: [] }", false),
        // Text, as YAML reads a number written bare.
        ("- { name: pg, image: x, args: [--port, 5433] }", false),
        ("- { name: pg, image: x, env: [{ name: PORT, value: 5433 }, { name: ON, value: true }] }", false),
        ("- { name: pg, image: x, ready: [check, 5] }", false),
        ("- { name: pg, image: x, resources: { requests: { cpu: 1 } } }", false),
        ("- { name: PG, image: x }", false),
        ("- { name: 1pg, image: x }", false),
        ("- { name: pg-, image: x }", false),
        ("- { name: pg_1, image: x }", false),
        ("- { name: localhost, image: x }", false),
        ("- { name: abcdefghijklmnopqrstuvwxyzabcdefghijklmno, image: x }", false),
        ("- { image: x }", false),
        ("- { name: pg }", false),
        ("- { name: pg, image: \"\" }", false),
        ("- { name: pg, image: \"a b\" }", false),
        ("- { name: pg, image: 5 }", true),
        ("- { name: pg, image: x, ready: \" \" }", false),
        ("- { name: pg, image: x, ready: [] }", false),
        ("- { name: pg, image: x, ready: [\"\"] }", false),
        ("- { name: pg, image: x, ready: 5 }", false),
        ("- { name: pg, image: x, ready: { tcp: 5432 } }", false),
        ("- { name: pg, image: x, ready_timeout_secs: 5 }", false),
        ("- { name: pg, image: x, ready: \"true\", ready_timeout_secs: 0 }", false),
        ("- { name: pg, image: x, ready: \"true\", ready_timeout_secs: 601 }", false),
        ("- { name: pg, image: x, ready: \"true\", ready_timeout_secs: 1.5 }", false),
        ("- { name: pg, image: x, args: ~ }", false),
        ("- { name: pg, image: x, args: a }", false),
        ("- { name: pg, image: x, env: ~ }", false),
        ("- { name: pg, image: x, env: [{ name: 1X, value: y }] }", false),
        ("- { name: pg, image: x, env: [{ value: y }] }", false),
        ("- { name: pg, image: x, env: [{ name: X, value: ~ }] }", true),
        ("- { name: pg, image: x, env: [{ name: X, value: a }, { name: X, value: b }] }", false),
        ("- { name: pg, image: x, env: [{ name: X, value_from: { secret: S } }] }", false),
        // dagron drops a key it does not know in a variable, without a word.
        ("- { name: pg, image: x, env: [{ name: X, value: a, extra: b }] }", true),
        ("- { name: pg, image: x, ports: [5432] }", false),
        ("- { name: pg, image: x, resources: { gpu: { count: 1 } } }", false),
        ("- { name: pg, image: x, resources: { limits: ~ } }", false),
        ("- { name: pg, image: x }\n- { name: pg, image: y }", false),
        ("- pg", false),
    ] {
        let direct = serde_yaml::from_str::<Vec<ServiceSpec>>(services)
            .map_err(|e| e.to_string())
            .and_then(|read| service::validate(&read).map_err(|e| e.to_string()));
        match parse(&with_services(services)) {
            // As dagron is handed it: the compiled workflow.
            Ok(pipeline) => {
                let handed = match compile::compile(&pipeline, &event, &Default::default()) {
                    Ok(compile::Outcome::Run(c)) => {
                        dagron_core::dag::DagGraph::from_yaml_with_params(&c.yaml, &c.parameters)
                            .map(|_| ())
                            .map_err(|e| format!("{e:#}"))
                    }
                    other => Err(format!("not compiled: {other:?}")),
                };
                if handed.is_err() || stricter {
                    wrong.push(format!("{services}\n  taken here, and by dagron: {handed:?}"));
                }
            }
            Err(ours) => {
                if direct.is_ok() != stricter {
                    wrong.push(format!(
                        "{services}\n  refused here: {ours:?}\n  by dagron: {direct:?}"
                    ));
                }
            }
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    // A job whose services are written as nothing has none.
    let none = with_tasks("  - name: a\n    steps: [{ run: x }]\n    services: ~\n");
    assert!(parse(&none).is_err(), "dagron reads no list out of nothing");
    // And the two caps.
    assert_eq!(MAX_SERVICES, service::MAX_SERVICES);
    assert_eq!(DOCKER_SERVICE, dagron_core::docker_daemon::NAME);
    assert_eq!(MAX_READY_TIMEOUT_SECS, service::MAX_READY_TIMEOUT_SECS);
    assert!(is_service_name(&"a".repeat(service::MAX_NAME_LEN)));
    assert!(!is_service_name(&"a".repeat(service::MAX_NAME_LEN + 1)));
}

#[test]
fn the_schema_names_what_a_service_takes() {
    let schema: Value = serde_json::from_str(SCHEMA).expect("the schema is JSON");
    let keys: BTreeSet<String> = schema["$defs"]["service"]["properties"]
        .as_object()
        .expect("an object")
        .keys()
        .cloned()
        .collect();
    assert_eq!(keys, SERVICE_KEYS.iter().map(|s| s.to_string()).collect());
    assert_eq!(
        schema["$defs"]["task"]["properties"]["services"]["maxItems"],
        MAX_SERVICES
    );
    assert_eq!(
        schema["$defs"]["service"]["properties"]["ready_timeout_secs"]["maximum"],
        MAX_READY_TIMEOUT_SECS
    );
}

#[test]
fn a_pipeline_may_not_choose_the_ref_a_jobs_actions_cache_restores_from() {
    refused(
        &with_tasks(
            "  - name: a\n    steps: [{ run: x, env: { DAGRON_ACTIONS_CACHE_RESTORE_FROM: refs/heads/release } }]",
        ),
        "is dagron-ci's own",
    );
    refused(
        &with_tasks(
            "  - name: a\n    steps: [{ run: x }]\n    \
             env: [{ name: DAGRON_ACTIONS_CACHE_RESTORE_FROM, value: refs/heads/release }]",
        ),
        "DAGRON_ACTIONS_CACHE_RESTORE_FROM is dagron-ci's own",
    );
    refused(
        &with_tasks(
            "  - { name: a, steps: [{ run: x }] }\n  - name: b\n    depends_on: [a]\n    \
             steps: [{ run: y }]\n    \
             env_from: [{ name: DAGRON_ACTIONS_CACHE_RESTORE_FROM, task: a, read: last_line }]",
        ),
        "DAGRON_ACTIONS_CACHE_RESTORE_FROM is dagron-ci's own",
    );
}

/// `on.failed` names the pipelines whose failed runs run this one, as a list
/// or with the branches it runs for; it names some, each a pipeline's name,
/// and not its own.
#[test]
fn on_failed_names_the_pipelines_whose_failures_run_this_one() {
    let p = parse(
        "name: fix\non: { failed: [ci, lint] }\ntasks:\n  - { name: a, steps: [{ run: x }] }\n",
    )
    .unwrap();
    let failed = p.on.failed.unwrap();
    assert_eq!(failed.pipelines, ["ci", "lint"]);
    assert!(failed.branches.is_none());
    let p = parse(
        "name: fix\non: { push: {}, failed: { pipelines: ci, branches: [main] } }\n\
         tasks:\n  - { name: a, steps: [{ run: x }] }\n",
    )
    .unwrap();
    let failed = p.on.failed.unwrap();
    assert_eq!(failed.pipelines, ["ci"]);
    assert!(failed.branches.unwrap().matches("main"));
    assert!(p.on.push.is_some(), "beside the other events");
    let with = |on: &str| {
        format!("name: fix\non: {on}\ntasks:\n  - {{ name: a, steps: [{{ run: x }}] }}\n")
    };
    refused(&with("{ failed: [] }"), "on.failed: names no pipeline");
    refused(
        &with("{ failed: [\"a b\"] }"),
        "on.failed[0]: \"a b\" is no pipeline's name",
    );
    refused(
        &with("{ failed: { branches: [main] } }"),
        "on.failed.pipelines: is required",
    );
    refused(
        &with("{ failed: { pipelines: [ci], paths: [x] } }"),
        "on.failed.paths",
    );
    refused(&with("{ failed: 3 }"), "on.failed: must list the pipelines");
    refused(
        &with("{ failed: [fix] }"),
        "a pipeline does not run on its own failure",
    );
}
