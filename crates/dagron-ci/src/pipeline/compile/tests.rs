use std::collections::BTreeMap;

use super::*;
use crate::pipeline::parse;

const SHA: &str = "4b4708e725f29891d22b4c96af9f0a403ff51c7c";
const BASE: &str = "7a4235be307170bbd9b16543f6f4ce7d81682e4a";

const PIPELINE: &str = r#"
name: widgets-ci
on:
  push:         { branches: [main, "release/**"], tags: ["v*"] }
  pull_request: { branches: [main], paths: ["src/**", "Cargo.*"] }
  merge_group:  { branches: [main] }
  dispatch:     { inputs: { channel: { default: beta }, ticket: { required: true }, note: {} } }
concurrency: { key: "{{ ci_group }}" }
parameters: { profile: release }
task_defaults: { docker_image: "rust:1.86-bookworm", env: [{ name: CARGO_TERM_COLOR, value: always }] }
tasks:
  - name: lint
    steps:
      - run: cargo fmt --all -- --check
  - name: test
    depends_on: [lint]
    timeout_secs: 1800
    env: [{ name: RUST_LOG, value: info }]
    steps:
      - name: unit
        run: cargo test --profile {{ profile }} -- --test-threads=1
        timeout_secs: 900
        env: { RUST_BACKTRACE: "1", REF: "{{ ci_ref }}" }
      - run: scripts/flaky.sh
        continue_on_error: true
  - name: gate
    depends_on: [test]
    type: approval
    approvers: ["group:release"]
"#;

fn pipeline() -> Pipeline {
    parse(PIPELINE).unwrap_or_else(|e| panic!("{e:#?}"))
}

fn event(kind: &str, git_ref: &str) -> Event {
    Event {
        kind: kind.into(),
        git_ref: git_ref.into(),
        sha: SHA.into(),
        trust: "trusted".into(),
        repo: "acme/widgets".into(),
        forge_host: "github.com".into(),
        ..Event::default()
    }
}

fn pull_request(base: &str, changed: Option<&[&str]>) -> Event {
    Event {
        base_sha: Some(BASE.into()),
        base_ref: Some(base.into()),
        pr: Some(42),
        changed: match changed {
            Some(c) => Changes::Listed(c.iter().map(|s| s.to_string()).collect()),
            None => Changes::Unknown,
        },
        ..event("pull_request", "refs/pull/42/head")
    }
}

fn run(p: &Pipeline, ev: &Event) -> Compiled {
    match compile(p, ev, &Settings::default()) {
        Ok(Outcome::Run(compiled)) => compiled,
        other => panic!("expected a run, got {other:?}"),
    }
}

fn skipped(p: &Pipeline, ev: &Event) -> String {
    match compile(p, ev, &Settings::default()) {
        Ok(Outcome::Skip(why)) => why,
        other => panic!("expected a skip, got {other:?}"),
    }
}

#[test]
fn on_decides_by_kind_ref_base_and_paths() {
    let p = pipeline();
    run(&p, &event("push", "refs/heads/main"));
    run(&p, &event("push", "refs/heads/release/1.2"));
    assert!(skipped(&p, &event("push", "refs/heads/feature/x")).contains("on.push.branches"));
    run(&p, &event("tag", "refs/tags/v1.0.0"));
    assert!(skipped(&p, &event("tag", "refs/tags/nightly")).contains("on.push.tags"));

    run(
        &p,
        &pull_request("main", Some(&["src/lib.rs", "README.md"])),
    );
    // Skipped for its paths alone, which is said apart: everything else of
    // `on:` took the event, so its jobs are checks someone may be waiting
    // on, and they are named.
    match compile(
        &p,
        &pull_request("main", Some(&["README.md"])),
        &Settings::default(),
    ) {
        Ok(Outcome::Unchanged { why, jobs }) => {
            assert_eq!(why, "no changed file matches on.pull_request.paths");
            let named: Vec<&str> = p.tasks.iter().map(|t| t.name.as_str()).collect();
            assert_eq!(jobs, named);
            assert!(!jobs.is_empty());
        }
        other => panic!("expected a skip for its paths, got {other:?}"),
    }
    assert!(
        skipped(&p, &pull_request("dev", Some(&["src/lib.rs"]))).contains("base branch \"dev\"")
    );
    assert_eq!(
        compile(&p, &pull_request("main", None), &Settings::default()),
        Ok(Outcome::NeedsChangedFiles),
        "a paths filter never guesses"
    );
    // Asked for, and not to be had (a new branch, a change past what the
    // forge lists): the pipeline runs rather than skip a check.
    run(
        &p,
        &Event {
            changed: Changes::Unlisted("a new branch".into()),
            ..pull_request("main", None)
        },
    );
    let unknown_base = Event {
        base_ref: None,
        ..pull_request("main", Some(&["src/lib.rs"]))
    };
    assert!(skipped(&p, &unknown_base).contains("base branch is not known"));

    let group = Event {
        base_ref: Some("refs/heads/main".into()),
        ..event("merge_group", "refs/heads/gh-readonly-queue/main/pr-42-abc")
    };
    run(&p, &group);
    for kind in ["delete", "pull_request_closed"] {
        assert!(skipped(&p, &event(kind, "refs/heads/main")).contains("runs no pipeline"));
    }
}

#[test]
fn push_filters_read_branches_and_tags_as_github_does() {
    let only = |on: &str| {
        parse(&format!(
            "name: p\non: {{ push: {on} }}\ntasks:\n  - {{ name: a, steps: [{{ run: make }}] }}\n"
        ))
        .unwrap()
    };
    // Neither: every branch and tag. Tags only: no branch. Branches only: no tag.
    let any = only("");
    run(&any, &event("push", "refs/heads/anything"));
    run(&any, &event("tag", "refs/tags/anything"));
    let tags = only("{ tags: [\"v*\"] }");
    assert!(skipped(&tags, &event("push", "refs/heads/main")).contains("takes tags"));
    let branches = only("{ branches: [main] }");
    assert!(skipped(&branches, &event("tag", "refs/tags/v1")).contains("takes branches"));
    // A tag push reads no paths.
    let paths = only("{ paths: [\"src/**\"] }");
    run(&paths, &event("tag", "refs/tags/v1"));
    assert_eq!(
        compile(
            &paths,
            &event("push", "refs/heads/main"),
            &Settings::default()
        ),
        Ok(Outcome::NeedsChangedFiles)
    );
}

#[test]
fn a_dispatch_takes_the_inputs_it_declares() {
    let p = pipeline();
    let dispatch = |inputs: &[(&str, &str)]| Event {
        inputs: inputs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect(),
        ..event("dispatch", "refs/heads/main")
    };
    let c = run(&p, &dispatch(&[("ticket", "OPS-1")]));
    assert_eq!(c.parameters["ticket"], "OPS-1");
    assert_eq!(c.parameters["channel"], "beta", "its default");
    assert_eq!(c.parameters["note"], "", "declared, no default");
    assert_eq!(c.parameters["ci_event"], "dispatch");

    let refused = |inputs: &[(&str, &str)]| compile(&p, &dispatch(inputs), &Settings::default());
    assert!(refused(&[]).unwrap_err().contains("needs input \"ticket\""));
    assert!(refused(&[("ticket", "x"), ("tciket", "y")])
        .unwrap_err()
        .contains("takes no input \"tciket\""));
    // Inputs belong to dispatches alone.
    assert!(!run(&p, &event("push", "refs/heads/main"))
        .parameters
        .contains_key("ticket"));

    let no_dispatch =
        parse("name: p\non: { push: }\ntasks:\n  - { name: a, steps: [{ run: make }] }\n").unwrap();
    assert!(skipped(&no_dispatch, &dispatch(&[])).contains("does not take dispatch"));
}

#[test]
fn the_parameters_are_the_events_and_a_pull_request_groups_by_number() {
    let p = pipeline();
    let c = run(&p, &pull_request("main", Some(&["src/lib.rs"])));
    let expected: BTreeMap<String, String> = [
        ("ci_base_sha", BASE),
        ("ci_base_ref", "refs/heads/main"),
        ("ci_event", "pull_request"),
        ("ci_forge_host", "github.com"),
        ("ci_group", "pr-42"),
        ("ci_pr", "42"),
        ("ci_ref", "refs/pull/42/head"),
        ("ci_repo", "acme/widgets"),
        ("ci_sha", SHA),
        ("ci_trust", "trusted"),
    ]
    .iter()
    .map(|(k, v)| (k.to_string(), v.to_string()))
    .collect();
    assert_eq!(c.parameters, expected);
    assert_eq!(
        group(&event("push", "refs/heads/x;rm -rf /$(id)")),
        "refs/heads/x_rm_-rf_/__id_"
    );
}

#[test]
fn the_workflow_is_the_same_bytes_for_the_same_inputs_and_holds_no_event_text() {
    let p = pipeline();
    let a = run(&p, &event("push", "refs/heads/main"));
    assert_eq!(a, run(&p, &event("push", "refs/heads/main")), "pure");
    // Another commit, another branch: other parameters, the same workflow.
    let b = run(
        &p,
        &Event {
            sha: BASE.into(),
            ..event("push", "refs/heads/release/9")
        },
    );
    assert_eq!(a.yaml, b.yaml);
    assert_ne!(a.parameters, b.parameters);
    assert!(!a.yaml.contains(SHA) && !a.yaml.contains("refs/heads/main"));
}

/// The workflow a push compiles to, in full: what a reviewer reads in
/// `dagron-ci compile`, and what changes only on purpose. After such a
/// change, `DAGRON_CI_BLESS=1 cargo test -p dagron-ci golden` rewrites it.
#[test]
fn the_compiled_workflow_matches_its_golden_file() {
    let path = concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/src/pipeline/compile/golden_push.yaml"
    );
    let c = run(&pipeline(), &event("push", "refs/heads/main"));
    if std::env::var_os("DAGRON_CI_BLESS").is_some() {
        std::fs::write(path, &c.yaml).unwrap();
    }
    let expected = std::fs::read_to_string(path).unwrap();
    assert_eq!(
        c.yaml, expected,
        "the compiled workflow changed:\n{}",
        c.yaml
    );
}

/// Every compiled workflow is tagged `ci`, whatever its author wrote: a list
/// of workflows is narrowed to CI's by a tag no pipeline has to remember.
#[test]
fn every_workflow_is_tagged_ci_beside_its_own_tags() {
    let tagged = |tags: &str| {
        let p = parse(&format!(
            "name: p\non: {{ push: }}\n{tags}tasks:\n  - {{ name: a, steps: [{{ run: make }}] }}\n"
        ))
        .unwrap_or_else(|e| panic!("{e:#?}"));
        let c = run(&p, &event("push", "refs/heads/main"));
        let spec: serde_yaml::Value = serde_yaml::from_str(&c.yaml).unwrap();
        let tags: Vec<String> = serde_yaml::from_value(spec["tags"].clone()).unwrap();
        // What dagron itself reads, not only what the YAML says.
        let dag =
            dagron_core::dag::DagGraph::from_yaml_with_params(&c.yaml, &c.parameters).unwrap();
        assert_eq!(dag.spec.tags, tags);
        tags
    };
    assert_eq!(tagged(""), ["ci"], "none of its own");
    assert_eq!(
        tagged("tags: [team.build, nightly]\n"),
        ["team.build", "nightly", "ci"],
        "its own, then ci"
    );
    assert_eq!(
        tagged("tags: [ci, slow]\n"),
        ["ci", "slow"],
        "said already: not twice"
    );
    assert_eq!(tagged("tags: []\n"), ["ci"]);
    assert_eq!(tagged("tags:\n"), ["ci"], "a key with nothing under it");
}

#[test]
fn every_task_gets_a_timeout() {
    let p =
        parse("name: p\non: { push: }\ntasks:\n  - { name: a, steps: [{ run: make }] }\n").unwrap();
    let c = run(&p, &event("push", "refs/heads/main"));
    assert!(c.yaml.contains("  timeout_secs: 3600\n"), "{}", c.yaml);
    let c = compile(
        &p,
        &event("push", "refs/heads/main"),
        &Settings {
            default_timeout_secs: 600,
            ..Settings::default()
        },
    )
    .unwrap();
    let Outcome::Run(c) = c else { panic!() };
    assert!(c.yaml.contains("  timeout_secs: 600\n"));
    // The pipeline's own default stands.
    let own = parse("name: p\non: { push: }\ntask_defaults: { timeout_secs: 120 }\ntasks:\n  - { name: a, steps: [{ run: make }] }\n").unwrap();
    let c = run(&own, &event("push", "refs/heads/main"));
    assert!(c.yaml.contains("  timeout_secs: 120\n") && !c.yaml.contains("3600"));
}

#[test]
fn concurrency_scopes_one_active_run_per_key_unless_the_pipeline_says() {
    let p = pipeline();
    let c = run(&p, &event("push", "refs/heads/main"));
    assert!(
        c.yaml.contains(
            "concurrency_key: '{{ ci_forge_host }}/{{ ci_repo }}:{{ ci_group }}'\nmax_active_runs: 1\n"
        ),
        "{}",
        c.yaml
    );
    let own = parse("name: p\non: { push: }\nconcurrency: { key: k }\nmax_active_runs: 3\ntasks:\n  - { name: a, steps: [{ run: make }] }\n").unwrap();
    let c = run(&own, &event("push", "refs/heads/main"));
    assert!(c.yaml.contains("max_active_runs: 3") && !c.yaml.contains("max_active_runs: 1"));

    // A cap set alone is the repository's: dagron would count it across every
    // repository's pipeline of this name.
    let capped = |cap: &str| {
        let text = format!("name: p\non: {{ push: }}\nmax_active_runs: {cap}\ntasks:\n  - {{ name: a, steps: [{{ run: make }}] }}\n");
        run(&parse(&text).unwrap(), &event("push", "refs/heads/main")).yaml
    };
    assert!(capped("2").contains("concurrency_key: '{{ ci_forge_host }}/{{ ci_repo }}'\n"));
    assert!(
        !capped("0").contains("concurrency_key"),
        "0 sets no cap to scope"
    );
}

/// A run that arrives at a full key is made, and waits its turn in the
/// engine, where its commit can be told that it waits. Where the pipeline
/// says a newer run cancels, it takes the place of the run before it. A
/// pipeline with no cap says nothing of a full one.
#[test]
fn a_run_over_its_key_waits_in_the_engine_or_takes_the_older_one_s_place() {
    let of = |top: &str| {
        let text = format!("name: p\non: {{ push: }}\n{top}tasks:\n  - {{ name: a, steps: [{{ run: make }}] }}\n");
        run(&parse(&text).unwrap(), &event("push", "refs/heads/main")).yaml
    };
    assert!(of("concurrency: { key: k }\n").contains("max_active_runs: 1\nwhen_full: queue\n"));
    assert!(of("concurrency: { key: k, cancel_superseded: true }\n")
        .contains("max_active_runs: 1\nwhen_full: supersede\n"));
    assert!(of("concurrency: { key: k }\nmax_active_runs: 3\n").contains("when_full: queue\n"));
    assert!(of("max_active_runs: 2\n").contains("when_full: queue\n"));
    assert!(!of("").contains("when_full"));
    assert!(!of("max_active_runs: 0\n").contains("when_full"), "0 sets no cap to be full");
}

mod dagron {
    //! The compiled workflow, through dagron's own parse, expand and validate:
    //! the path dagron-api runs when the workflow is submitted.

    use super::*;
    use dagron_core::dag::DagGraph;

    fn expand(c: &Compiled) -> DagGraph {
        DagGraph::from_yaml_with_params(&c.yaml, &c.parameters)
            .unwrap_or_else(|e| panic!("dagron refuses the workflow: {e:#}\n{}", c.yaml))
    }

    fn task<'a>(g: &'a DagGraph, name: &str) -> &'a dagron_core::dag::TaskSpec {
        g.spec
            .tasks
            .iter()
            .find(|t| t.name == name)
            .unwrap_or_else(|| panic!("no task {name}"))
    }

    fn env<'a>(t: &'a dagron_core::dag::TaskSpec, name: &str) -> Option<&'a str> {
        t.env
            .iter()
            .find(|e| e.name == name)
            .map(|e| e.value.as_str())
    }

    #[test]
    fn dagron_takes_the_compiled_workflow() {
        let g = expand(&run(&pipeline(), &event("push", "refs/heads/main")));
        let test = task(&g, "test");
        assert_eq!(test.timeout_secs, Some(1800));
        assert_eq!(test.docker_image.as_deref(), Some("rust:1.86-bookworm"));
        assert_eq!(env(test, "CI_SHA"), Some(SHA));
        assert_eq!(env(test, "CARGO_TERM_COLOR"), Some("always"));
        assert_eq!(env(test, "RUST_LOG"), Some("info"));
        assert_eq!(
            env(test, "CI_STEP_1_RUN"),
            Some("cargo test --profile release -- --test-threads=1"),
            "the pipeline's own parameter, filled in"
        );
        assert_eq!(task(&g, "lint").timeout_secs, Some(3600));
        assert_eq!(task(&g, "gate").task_type.as_deref(), Some("approval"));
        assert!(task(&g, "gate").command.is_empty());
        assert_eq!(
            g.spec.concurrency_key.as_deref(),
            Some("github.com/acme/widgets:refs/heads/main")
        );
        // And what a full key does, as dagron reads it.
        assert_eq!(g.spec.when_full, Some(dagron_core::dag::WhenFull::Queue));
        let cancels = parse("name: p\non: { push: }\nconcurrency: { key: k, cancel_superseded: true }\ntasks:\n  - { name: a, steps: [{ run: make }] }\n").unwrap();
        let g = expand(&run(&cancels, &event("push", "refs/heads/main")));
        assert_eq!(g.spec.when_full, Some(dagron_core::dag::WhenFull::Supersede));
        assert_eq!(g.spec.max_active_runs, Some(1));
    }

    /// A job may name an environment of its own, for its secrets. It reaches
    /// dagron as that job's, beside the pipeline's, and a secret named by
    /// reference is still a reference there: dagron-ci resolves none.
    #[test]
    fn a_job_s_own_environment_reaches_dagron() {
        let p = parse(concat!(
            "name: release\non: { push: }\nenvironment: build\ntasks:\n",
            "  - { name: test, steps: [{ run: make test }] }\n",
            "  - name: publish\n",
            "    environment: registry\n",
            "    env: [{ name: TOKEN, value_from: { secret: registry_token } }]\n",
            "    steps: [{ run: make publish }]\n",
        ))
        .expect("a pipeline");
        let g = expand(&run(&p, &event("push", "refs/heads/main")));
        assert_eq!(g.spec.environment.as_deref(), Some("build"));
        assert_eq!(task(&g, "test").environment, None);
        let publish = task(&g, "publish");
        assert_eq!(publish.environment.as_deref(), Some("registry"));
        let token = publish
            .env
            .iter()
            .find(|e| e.name == "TOKEN")
            .expect("the job's own variable");
        assert_eq!(
            token.value_from.as_ref().map(|s| s.secret.as_str()),
            Some("registry_token")
        );
        assert_eq!(token.value, "");
        // What dagron asks a policy about: every environment the run names.
        let named: Vec<String> = dagron_core::envpolicy::named_by(&g).into_iter().collect();
        assert_eq!(named, ["build", "registry"]);
    }

    /// dagron counts a cap per workflow name and key. Two repositories on one
    /// cell, each with this pipeline, each with a pull request 42: their runs
    /// never wait on each other.
    #[test]
    fn repositories_never_share_a_concurrency_key() {
        let key = |repo: &str| {
            let ev = Event {
                repo: repo.into(),
                ..pull_request("main", Some(&["src/lib.rs"]))
            };
            expand(&run(&pipeline(), &ev)).spec.concurrency_key
        };
        assert_eq!(
            key("acme/widgets").as_deref(),
            Some("github.com/acme/widgets:pr-42")
        );
        assert_ne!(key("acme/widgets"), key("acme/gadgets"));
    }

    /// The plan's acceptance line: a branch named `x;rm -rf /` (and a pull
    /// request titled `$(curl evil)`, which no event carries at all) never
    /// reaches a shell unquoted. After dagron fills the workflow in, the
    /// branch name is in variables only, and every command is the compiler's.
    #[test]
    fn a_hostile_branch_name_never_reaches_a_command() {
        let hostile = "refs/heads/x;rm -rf /$(curl evil)";
        let c = run(&pipeline(), &event("push", "refs/heads/main"));
        let mut params = c.parameters.clone();
        params.insert("ci_ref".into(), hostile.into());
        let g = expand(&Compiled {
            yaml: c.yaml.clone(),
            parameters: params,
        });
        for t in &g.spec.tasks {
            for arg in &t.command {
                assert!(
                    !arg.contains("rm -rf") && !arg.contains("curl"),
                    "{}: {arg}",
                    t.name
                );
            }
        }
        let test = task(&g, "test");
        assert_eq!(env(test, "CI_REF"), Some(hostile));
        assert_eq!(
            env(test, "CI_STEP_1_ENV_REF"),
            Some(hostile),
            "a step's env: is a variable too"
        );
    }

    #[test]
    fn a_fan_out_and_a_dispatch_compile_to_workflows_dagron_takes() {
        let fan = parse(concat!(
            "name: matrix\non: { push: }\n",
            "tasks:\n",
            "  - { name: build, with_items: [amd64, arm64], steps: [{ run: \"make ARCH={{ item }}\" }] }\n",
            "  - { name: after, depends_on: [build], when: \"{{ ci_event }} == push\", steps: [{ run: echo done }] }\n",
        ))
        .unwrap();
        let g = expand(&run(&fan, &event("push", "refs/heads/main")));
        let runs: Vec<&str> = g
            .spec
            .tasks
            .iter()
            .filter_map(|t| env(t, "CI_STEP_1_RUN"))
            .collect();
        assert!(
            runs.contains(&"make ARCH=amd64") && runs.contains(&"make ARCH=arm64"),
            "{runs:?}"
        );

        let dispatch = Event {
            inputs: BTreeMap::from([("ticket".to_string(), "OPS-7".to_string())]),
            ..event("dispatch", "refs/heads/main")
        };
        expand(&run(&pipeline(), &dispatch));
    }

    /// A job has a workspace in dagron's terms: every task with steps gets
    /// one, of the size it states, or none where it says null; a task that
    /// is dagron's own (a command, an approval) gets none it did not ask for.
    /// Each starts with the event's commit checked out, the repository's own,
    /// unless it says `checkout: false`; `dagron-ci run`'s never do.
    #[test]
    fn every_job_runs_in_a_workspace_unless_it_says_not() {
        use dagron_core::workspace::{CheckoutSpec, WorkspaceSpec};
        let p = parse(concat!(
            "name: ws\non: { push: }\n",
            "tasks:\n",
            "  - { name: build, steps: [{ run: make }] }\n",
            "  - { name: big, steps: [{ run: make }], workspace: { size: 20Gi } }\n",
            "  - { name: empty, steps: [{ run: make }], workspace: { checkout: false } }\n",
            "  - { name: deep, steps: [{ run: make }], workspace: { checkout: { depth: 0 } } }\n",
            "  - { name: some, steps: [{ run: make }], workspace: { size: 1Gi, checkout: { depth: 30 } } }\n",
            "  - { name: bare, steps: [{ run: make }], workspace: null }\n",
            "  - { name: own, command: [make] }\n",
            "  - { name: owns, command: [make], workspace: { size: 1Gi } }\n",
            "  - { name: gate, type: approval }\n",
        ))
        .unwrap();
        let g = expand(&run(&p, &event("push", "refs/heads/main")));
        let commit = CheckoutSpec {
            repository: "https://github.com/acme/widgets.git".into(),
            commit: SHA.into(),
            depth: None,
            patched: false,
        };
        assert_eq!(
            task(&g, "build").workspace,
            Some(WorkspaceSpec {
                checkout: Some(commit.clone()),
                ..WorkspaceSpec::default()
            })
        );
        let big = task(&g, "big").workspace.as_ref().unwrap();
        assert_eq!(big.size.as_deref(), Some("20Gi"));
        assert_eq!(big.checkout.as_ref(), Some(&commit));
        assert_eq!(task(&g, "empty").workspace, Some(WorkspaceSpec::default()));
        // With its history, all of it or so many commits: the commit is
        // still the event's, and the repository its own.
        let with = |depth| {
            Some(CheckoutSpec {
                depth: Some(depth),
                ..commit.clone()
            })
        };
        assert_eq!(
            task(&g, "deep").workspace.as_ref().unwrap().checkout,
            with(0)
        );
        let some = task(&g, "some").workspace.as_ref().unwrap();
        assert_eq!(
            (some.size.as_deref(), &some.checkout),
            (Some("1Gi"), &with(30))
        );
        assert_eq!(task(&g, "bare").workspace, None);
        assert_eq!(task(&g, "own").workspace, None);
        assert_eq!(
            task(&g, "owns")
                .workspace
                .as_ref()
                .unwrap()
                .checkout
                .as_ref(),
            Some(&commit),
            "a command task's own workspace starts with the commit too"
        );
        assert_eq!(task(&g, "gate").workspace, None);

        // The workflow names the commit only as the run's parameter.
        let c = run(&p, &event("push", "refs/heads/main"));
        assert!(
            c.yaml
                .contains("repository: https://{{ ci_forge_host }}/{{ ci_repo }}.git")
                && c.yaml.contains("commit: '{{ ci_sha }}'")
                && !c.yaml.contains(SHA),
            "{}",
            c.yaml
        );

        // `dagron-ci run`'s tasks work in the tree it was run from.
        let Outcome::Run(local) = compile(
            &p,
            &event("push", "refs/heads/main"),
            &Settings {
                checkout: false,
                ..Settings::default()
            },
        )
        .unwrap() else {
            panic!("runs")
        };
        let g = expand(&local);
        assert_eq!(task(&g, "build").workspace, Some(WorkspaceSpec::default()));
        assert_eq!(task(&g, "big").workspace.as_ref().unwrap().checkout, None);
    }
}

mod driver {
    //! The step driver, run by a real `sh` with the variables dagron would
    //! give it, and with no dagron-ci-exec to hand the steps to: the
    //! fallback. dagron-ci-exec's conformance test runs both.

    use super::*;
    use std::process::Command;

    fn step(run: &str) -> Step {
        Step {
            run: run.into(),
            ..Step::default()
        }
    }

    /// What one run of the driver gave.
    struct Drove {
        code: i32,
        stdout: String,
        stderr: String,
    }

    /// Run the driver for `steps`, with the variables the compiler gives it.
    fn drive(steps: &[Step]) -> Drove {
        let mut cmd = Command::new("sh");
        cmd.arg("-c")
            .arg(driver(steps))
            .env_clear()
            .env("PATH", std::env::var("PATH").unwrap())
            .envs(step_variables(steps));
        let out = cmd.output().unwrap();
        Drove {
            code: out.status.code().unwrap_or(-1),
            stdout: String::from_utf8_lossy(&out.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&out.stderr).into_owned(),
        }
    }

    #[test]
    fn steps_run_in_order_and_a_failure_ends_the_task() {
        let out = drive(&[
            step("echo one; echo uno >&2"),
            step("echo two; exit 3"),
            step("echo never"),
        ]);
        assert_eq!(out.code, 3, "{}", out.stderr);
        assert_eq!(
            out.stdout, "one\ntwo\n",
            "the output is the steps' stdout, with no marker in it"
        );
        assert_eq!(
            out.stderr,
            "::dagron-ci::step::1::start::echo one; echo uno >&2\nuno\n\
             ::dagron-ci::step::1::end::0\n::dagron-ci::step::2::start::echo two; exit 3\n\
             ::dagron-ci::step::2::end::3\n"
        );
    }

    /// A step's stdout is the task's output: what `with_output_of` fans out
    /// over and `when:` reads. It is exactly what the step printed.
    #[test]
    fn the_output_is_what_the_steps_print() {
        let out = drive(&[step("echo '[\"a\",\"b\"]'")]);
        assert_eq!(out.code, 0);
        let items: serde_json::Value = serde_json::from_str(out.stdout.trim()).unwrap();
        assert_eq!(items, serde_json::json!(["a", "b"]));
    }

    #[test]
    fn a_step_that_may_fail_does_not_end_the_task() {
        let flaky = Step {
            continue_on_error: true,
            ..step("exit 7")
        };
        let out = drive(&[flaky, step("echo after")]);
        assert_eq!(out.code, 0, "{}", out.stderr);
        assert!(
            out.stderr
                .contains("::dagron-ci::step::1::end::7\n::dagron-ci::step::1::continued"),
            "{}",
            out.stderr
        );
        assert_eq!(out.stdout, "after\n");
    }

    #[test]
    fn set_e_holds_inside_a_step_and_its_env_is_its_own() {
        let first = Step {
            env: BTreeMap::from([(
                "GREETING".to_string(),
                "hi $(touch /tmp/x) `id`".to_string(),
            )]),
            ..step("printf '%s\\n' \"$GREETING\"; false; echo unreachable")
        };
        let out = drive(&[first, step("echo \"greeting=${GREETING:-unset}\"")]);
        assert_eq!(out.code, 1, "{}", out.stderr);
        assert_eq!(
            out.stdout, "hi $(touch /tmp/x) `id`\n",
            "a value is never parsed, and set -e ends the step"
        );

        let scoped = Step {
            env: BTreeMap::from([("GREETING".to_string(), "hi".to_string())]),
            ..step("true")
        };
        let out = drive(&[scoped, step("echo \"greeting=${GREETING:-unset}\"")]);
        assert_eq!(out.stdout, "greeting=unset\n");
    }

    #[test]
    fn a_step_past_its_timeout_is_stopped() {
        if Command::new("sh")
            .arg("-c")
            .arg("command -v timeout")
            .output()
            .map(|o| !o.status.success())
            .unwrap_or(true)
        {
            return eprintln!("no timeout(1) here - skipping");
        }
        let slow = Step {
            timeout_secs: Some(1),
            ..step("sleep 5")
        };
        let started = std::time::Instant::now();
        let out = drive(&[slow]);
        assert_eq!(out.code, 124, "timeout(1)'s code: {}", out.stderr);
        assert!(started.elapsed() < std::time::Duration::from_secs(4));
    }

    /// The protocol's variables, as the workflow lists them: what the
    /// fallback reads, and what dagron-ci-exec reads.
    #[test]
    fn the_steps_travel_as_protocol_1_variables() {
        let steps = [
            Step {
                name: Some("build".into()),
                timeout_secs: Some(600),
                env: BTreeMap::from([("PROFILE".to_string(), "release".to_string())]),
                ..step("make")
            },
            Step {
                continue_on_error: true,
                ..step("make lint\nmore")
            },
        ];
        let want = [
            ("CI_STEP_PROTOCOL", "1"),
            ("CI_STEP_COUNT", "2"),
            ("CI_STEP_1_NAME", "build"),
            ("CI_STEP_1_RUN", "make"),
            ("CI_STEP_1_TIMEOUT_SECS", "600"),
            ("CI_STEP_1_ENV_PROFILE", "release"),
            ("CI_STEP_2_NAME", "make lint"),
            ("CI_STEP_2_RUN", "make lint\nmore"),
            ("CI_STEP_2_CONTINUE_ON_ERROR", "true"),
        ];
        let want: Vec<(String, String)> = want
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        assert_eq!(step_variables(&steps), want);
    }
}

/// A `failed` event runs the pipeline whose `on.failed` names the failed
/// run's pipeline, for its branch, and gives it the run's facts; any other
/// event gives that pipeline empty ones, and a pipeline that waits on no
/// failure none at all.
#[test]
fn a_failed_event_runs_what_waits_on_its_pipeline_with_its_facts() {
    let fix = parse(
        "name: fix\non: { failed: { pipelines: [ci], branches: [main] }, dispatch: {} }\n\
         tasks:\n  - { name: agent, steps: [{ run: \"echo $CI_FAILED_JOB\" }] }\n",
    )
    .unwrap();
    let failed = |pipeline: &str, git_ref: &str| Event {
        inputs: BTreeMap::from([
            (FAILED_PIPELINE.to_string(), pipeline.to_string()),
            ("ci_failed_run".to_string(), "run-1".to_string()),
            ("ci_failed_job".to_string(), "test".to_string()),
        ]),
        ..event("failed", git_ref)
    };
    let settings = Settings::default();
    let Outcome::Run(run) = compile(&fix, &failed("ci", "refs/heads/main"), &settings).unwrap()
    else {
        panic!("ci's failure on main runs fix");
    };
    assert_eq!(run.parameters["ci_failed_run"], "run-1");
    assert_eq!(run.parameters["ci_failed_job"], "test");
    assert_eq!(run.parameters["ci_failed_disposition"], "");
    assert!(run.yaml.contains("name: CI_FAILED_RUN"), "{}", run.yaml);
    for (event, says) in [
        (
            failed("lint", "refs/heads/main"),
            "on.failed does not name \"lint\"",
        ),
        (
            failed("ci", "refs/heads/feature"),
            "branch \"feature\" is not in on.failed.branches",
        ),
    ] {
        match compile(&fix, &event, &settings).unwrap() {
            Outcome::Skip(why) => assert_eq!(why, says),
            other => panic!("{other:?}"),
        }
    }
    // A pull request's failure is of the branch it merges into, and so is a
    // merge group's.
    let pr = Event {
        base_ref: Some("main".into()),
        pr: Some(7),
        ..failed("ci", "refs/pull/7/head")
    };
    assert!(matches!(
        compile(&fix, &pr, &settings).unwrap(),
        Outcome::Run(_)
    ));
    let queued = Event {
        base_ref: Some("refs/heads/main".into()),
        ..failed(
            "ci",
            "refs/heads/gh-readonly-queue/main/pr-42-2222222222222222222222222222222222222222",
        )
    };
    assert!(matches!(
        compile(&fix, &queued, &settings).unwrap(),
        Outcome::Run(_)
    ));
    // Asked for otherwise, it is given empty facts.
    let Outcome::Run(run) =
        compile(&fix, &event("dispatch", "refs/heads/main"), &settings).unwrap()
    else {
        panic!("a dispatch runs fix");
    };
    assert_eq!(run.parameters["ci_failed_run"], "");
    // And a pipeline that waits on no failure is given none of them.
    match compile(&pipeline(), &failed("ci", "refs/heads/main"), &settings).unwrap() {
        Outcome::Skip(why) => assert_eq!(why, "on: does not take failed"),
        other => panic!("{other:?}"),
    }
    let Outcome::Run(push) =
        compile(&pipeline(), &event("push", "refs/heads/main"), &settings).unwrap()
    else {
        panic!("a push runs widgets-ci");
    };
    assert!(!push.parameters.contains_key("ci_failed_run"));
    assert!(!push.yaml.contains("CI_FAILED"));
}

/// The tasks dagron makes of `compiled`'s run, as dagron-api does.
fn made(compiled: &Compiled) -> Vec<String> {
    dagron_core::dag::DagGraph::from_yaml_with_params(&compiled.yaml, &compiled.parameters)
        .unwrap_or_else(|e| panic!("{e}\n{}", compiled.yaml))
        .spec
        .tasks
        .iter()
        .map(|task| task.name.clone())
        .collect()
}

/// The jobs a run's `when:` leaves out are the ones dagron made no task of:
/// a job that fans out is left out where it made no instance, one whose
/// `when:` reads another job's output is decided as it runs, and a job with
/// no `when:` is never one.
#[test]
fn the_jobs_a_when_leaves_out_are_those_dagron_makes_no_task_of() {
    let p = parse(
        r#"
name: release
on: { push: {} }
task_defaults: { docker_image: "alpine:3" }
tasks:
  - { name: build, steps: [{ run: make }] }
  - { name: notes, when: "{{ ci_event }} == push", steps: [{ run: make notes }] }
  - name: publish
    depends_on: [build]
    when: "{{ ci_event }} == tag"
    steps: [{ run: make publish }]
  - name: shards
    with_items: [a, b]
    when: "{{ ci_event }} == tag and {{ item }} != b"
    steps: [{ run: "make shard" }]
  - name: verify
    depends_on: [build]
    when: "{{ tasks.build.output }} == ok"
    steps: [{ run: make verify }]
"#,
    )
    .unwrap_or_else(|e| panic!("{e:#?}"));
    let push = run(&p, &event("push", "refs/heads/main"));
    assert!(can_leave_out(&push));
    assert_eq!(left_out(&push, &made(&push)), ["publish", "shards"]);
    let tag = run(&p, &event("tag", "refs/tags/v1"));
    let shards = made(&tag);
    assert_eq!(shards.iter().filter(|t| t.starts_with("shards")).count(), 1);
    assert_eq!(left_out(&tag, &made(&tag)), ["notes"]);

    // Every job left out: a run of no task, whose jobs are all said so.
    let p = parse(
        r#"
name: tags-only
on: { push: {} }
tasks:
  - { name: a, docker_image: "alpine:3", when: "{{ ci_event }} == tag", steps: [{ run: "true" }] }
  - { name: b, docker_image: "alpine:3", when: "{{ ci_event }} == tag", depends_on: [a], steps: [{ run: "true" }] }
"#,
    )
    .unwrap_or_else(|e| panic!("{e:#?}"));
    let push = run(&p, &event("push", "refs/heads/main"));
    assert!(made(&push).is_empty());
    assert_eq!(left_out(&push, &made(&push)), ["a", "b"]);
    let tag = run(&p, &event("tag", "refs/tags/v1"));
    assert!(left_out(&tag, &made(&tag)).is_empty());

    // A pipeline with no `when:` leaves nothing out, and its run's graph
    // need not be read.
    let push = run(&pipeline(), &event("push", "refs/heads/main"));
    assert!(!can_leave_out(&push));
    assert!(left_out(&push, &[]).is_empty());
}
