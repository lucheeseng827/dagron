use std::collections::BTreeMap;

use serde_json::json;

use super::*;
use crate::pipeline::compile::{self, Changes, Event, Outcome};

const SHA: &str = "4b4708e725f29891d22b4c96af9f0a403ff51c7c";
const BEFORE: &str = "1111111111111111111111111111111111111111";

fn push(branch: &str) -> Event {
    Event {
        kind: "push".into(),
        git_ref: format!("refs/heads/{branch}"),
        sha: SHA.into(),
        base_sha: Some(BEFORE.into()),
        trust: "trusted".into(),
        repo: "group/app".into(),
        forge: "gitlab".into(),
        forge_host: "gitlab.example".into(),
        default_branch: Some("main".into()),
        ..Event::default()
    }
}

fn tag(name: &str) -> Event {
    Event {
        kind: "tag".into(),
        git_ref: format!("refs/tags/{name}"),
        base_sha: None,
        ..push("main")
    }
}

fn merge_request() -> Event {
    Event {
        kind: "pull_request".into(),
        git_ref: "refs/merge-requests/12/head".into(),
        base_sha: Some(BEFORE.into()),
        base_ref: Some("main".into()),
        head_ref: Some("feature/x".into()),
        pr: Some(12),
        ..push("main")
    }
}

fn read(text: &str) -> Ci {
    parse(text, &BTreeMap::new()).unwrap_or_else(|p| panic!("{p:?}"))
}

fn problems(text: &str) -> String {
    match parse(text, &BTreeMap::new()) {
        Ok(_) => panic!("read: {text}"),
        Err(p) => p
            .iter()
            .map(|p| p.to_string())
            .collect::<Vec<_>>()
            .join("\n"),
    }
}

fn lowered(text: &str, event: &Event) -> Lowered {
    lowered_with(text, event, &Settings::default())
}

fn lowered_with(text: &str, event: &Event, settings: &Settings) -> Lowered {
    read(text)
        .lower("gitlab-ci", event, settings)
        .unwrap_or_else(|p| panic!("{p:?}"))
}

fn names(l: &Lowered) -> Vec<&str> {
    l.pipeline.tasks.iter().map(|t| t.name.as_str()).collect()
}

fn task<'a>(l: &'a Lowered, name: &str) -> &'a Task {
    l.pipeline
        .tasks
        .iter()
        .find(|t| t.name == name)
        .unwrap_or_else(|| panic!("no task {name}: {:?}", names(l)))
}

fn env<'a>(t: &'a Task, name: &str) -> Option<&'a str> {
    t.fields["env"]
        .as_array()
        .unwrap()
        .iter()
        .find(|e| e["name"] == name)
        .and_then(|e| e["value"].as_str())
}

const STAGES: &str = "
image: alpine:3.20
stages: [build, test, deploy]
build:
  stage: build
  script: [make]
  artifacts:
    paths: [out/]
    expire_in: 1 week
unit:
  stage: test
  script: [make test]
lint:
  stage: test
  script: [make lint]
ship:
  stage: deploy
  script: [make ship]
  rules:
    - if: $CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH
";

#[test]
fn a_job_waits_for_every_job_of_the_stages_before_its_own() {
    let l = lowered(STAGES, &push("main"));
    assert_eq!(names(&l), ["build", "unit", "lint", "ship"]);
    assert!(task(&l, "build").depends_on.is_empty());
    assert_eq!(task(&l, "unit").depends_on, ["build"]);
    assert_eq!(task(&l, "ship").depends_on, ["build", "unit", "lint"]);
    assert_eq!(
        task(&l, "build").fields["docker_image"],
        json!("alpine:3.20")
    );
    // The artifacts of the stages before it are fetched.
    assert_eq!(task(&l, "unit").hooks.download, ["build"]);
    let up = task(&l, "build").hooks.upload.clone().unwrap();
    assert_eq!((up.name.as_str(), up.expire_days), ("build", Some(7)));
    assert_eq!(up.paths, ["out/"]);
}

#[test]
fn rules_decide_which_jobs_an_event_has() {
    let l = lowered(STAGES, &push("feature"));
    assert_eq!(
        names(&l),
        ["build", "unit", "lint"],
        "ship is the default branch's"
    );
    // A merge request's pipeline has no job without rules.
    let l = lowered(STAGES, &merge_request());
    assert_eq!(
        l.passed_over.as_deref(),
        Some("no job of the pipeline is in it for this event")
    );
    let mr = "
image: alpine
test:
  script: [true]
  rules:
    - if: $CI_PIPELINE_SOURCE == 'merge_request_event'
    - if: $CI_COMMIT_TAG
      when: never
";
    let l = lowered(mr, &merge_request());
    assert_eq!(names(&l), ["test"]);
    assert_eq!(env(task(&l, "test"), "CI_MERGE_REQUEST_IID"), Some("12"));
    assert_eq!(
        env(task(&l, "test"), "CI_MERGE_REQUEST_SOURCE_BRANCH_NAME"),
        Some("feature/x")
    );
    assert!(lowered(mr, &push("main")).passed_over.is_some());
}

#[test]
fn workflow_rules_decide_whether_there_is_a_pipeline() {
    let text = "
workflow:
  rules:
    - if: $CI_COMMIT_TAG
      variables: {DEPLOY: 'yes'}
    - when: never
image: alpine
job:
  script: [echo $DEPLOY]
";
    assert_eq!(
        lowered(text, &push("main")).passed_over.as_deref(),
        Some("workflow:rules says never for this event")
    );
    let l = lowered(text, &tag("v1"));
    assert_eq!(env(task(&l, "job"), "DEPLOY"), Some("yes"));
    assert_eq!(env(task(&l, "job"), "CI_COMMIT_TAG"), Some("v1"));
}

#[test]
fn needs_waits_for_the_jobs_it_names_alone() {
    let text = "
image: alpine
stages: [build, test]
a: {stage: build, script: [a]}
b: {stage: build, script: [b], artifacts: {paths: [b.txt]}}
c:
  stage: test
  needs: [a, {job: b, artifacts: false}]
  script: [c]
d:
  stage: test
  needs: []
  script: [d]
";
    let l = lowered(text, &push("main"));
    assert_eq!(task(&l, "c").depends_on, ["a", "b"]);
    assert!(task(&l, "c").hooks.download.is_empty());
    assert!(task(&l, "d").depends_on.is_empty());
}

#[test]
fn a_need_its_rules_leave_out_is_refused_unless_optional() {
    let text = "
image: alpine
stages: [build, test]
docs:
  stage: build
  script: [d]
  rules: [{if: $CI_COMMIT_TAG}]
test:
  stage: test
  needs: [docs]
  script: [t]
";
    let p = read(text)
        .lower("gitlab-ci", &push("main"), &Settings::default())
        .unwrap_err();
    assert!(p[0].message.contains("optional: true"), "{p:?}");
    let optional = text.replace("needs: [docs]", "needs: [{job: docs, optional: true}]");
    assert!(task(&lowered(&optional, &push("main")), "test")
        .depends_on
        .is_empty());
}

#[test]
fn templates_references_and_defaults_are_resolved() {
    let text = "
default:
  image: ruby:3.3
  before_script: [bundle install]
.base: &base
  variables: {LEVEL: base, KEEP: kept}
  tags: [docker]
.setup:
  script: [setup.sh]
test:
  extends: .base
  variables: {LEVEL: test}
  script:
    - !reference [.setup, script]
    - rake test
lint:
  <<: *base
  script: [rubocop]
";
    let l = lowered(text, &push("main"));
    let test = task(&l, "test");
    assert_eq!(env(test, "LEVEL"), Some("test"));
    assert_eq!(env(test, "KEEP"), Some("kept"));
    let run = &test.steps[0].run;
    let at = |needle: &str| {
        run.find(needle)
            .unwrap_or_else(|| panic!("{needle}: {run}"))
    };
    assert!(at("bundle install") < at("setup.sh") && at("setup.sh") < at("rake test"));
    assert_eq!(test.fields["docker_image"], json!("ruby:3.3"));
    assert_eq!(env(task(&l, "lint"), "LEVEL"), Some("base"));
    assert_eq!(names(&l), ["test", "lint"], "templates are no jobs");
}

#[test]
fn a_local_include_is_merged_under_the_file() {
    let main = "
include:
  - local: /ci/common.yml
  - ci/more.yml
image: alpine
job:
  extends: .common
  script: [own]
";
    let included = BTreeMap::from([
        (
            "ci/common.yml".to_string(),
            Ok(".common:\n  variables: {FROM: common}\n  script: [theirs]\n".to_string()),
        ),
        (
            "ci/more.yml".to_string(),
            Ok("extra:\n  image: alpine\n  script: [extra]\n".to_string()),
        ),
    ]);
    assert_eq!(
        local_includes(main).unwrap(),
        ["ci/common.yml", "ci/more.yml"]
    );
    let ci = parse(main, &included).unwrap();
    let l = ci
        .lower("gitlab-ci", &push("main"), &Settings::default())
        .unwrap();
    assert_eq!(names(&l), ["extra", "job"]);
    let job = task(&l, "job");
    assert_eq!(env(job, "FROM"), Some("common"));
    assert!(job.steps[0].run.contains("own") && !job.steps[0].run.contains("theirs"));

    let missing = BTreeMap::new();
    let p = parse(main, &missing).unwrap_err();
    assert!(
        p.iter().any(|p| p.message.contains("ci/common.yml")),
        "{p:?}"
    );
    for remote in [
        "include: https://example.com/ci.yml\njob: {script: [x]}",
        "include: {project: a/b, file: ci.yml}\njob: {script: [x]}",
        "include: {template: Auto-DevOps.gitlab-ci.yml}\njob: {script: [x]}",
        "include: ../out.yml\njob: {script: [x]}",
    ] {
        assert!(parse(remote, &missing).is_err(), "{remote}");
    }
}

#[test]
fn parallel_makes_an_instance_of_each_and_needs_waits_for_all() {
    let text = "
image: alpine
stages: [test, report]
shard:
  stage: test
  parallel: 3
  script: [echo $CI_NODE_INDEX]
os:
  stage: test
  parallel:
    matrix:
      - OS: [linux, mac]
        ARCH: amd64
  script: [echo $OS]
report:
  stage: report
  needs: [shard]
  script: [r]
";
    let l = lowered(text, &push("main"));
    assert_eq!(
        names(&l),
        [
            "shard.1",
            "shard.2",
            "shard.3",
            "os.amd64-linux",
            "os.amd64-mac",
            "report"
        ]
    );
    assert_eq!(env(task(&l, "shard.2"), "CI_NODE_INDEX"), Some("2"));
    assert_eq!(env(task(&l, "shard.2"), "CI_NODE_TOTAL"), Some("3"));
    assert_eq!(env(task(&l, "os.amd64-mac"), "OS"), Some("mac"));
    assert_eq!(
        task(&l, "report").depends_on,
        ["shard.1", "shard.2", "shard.3"]
    );
}

#[test]
fn when_becomes_a_trigger_rule_and_manual_an_approval() {
    let text = "
image: alpine
stages: [build, deploy, cleanup]
build: {stage: build, script: [b]}
deploy:
  stage: deploy
  script: [d]
  when: manual
cleanup:
  stage: cleanup
  script: [c]
  when: always
notify:
  stage: cleanup
  script: [n]
  when: on_failure
";
    let l = lowered(text, &push("main"));
    let gate = task(&l, "deploy.approve");
    assert_eq!(gate.fields["type"], json!("approval"));
    assert_eq!(gate.depends_on, ["build"]);
    let deploy = task(&l, "deploy");
    assert_eq!(deploy.depends_on, ["deploy.approve"]);
    assert!(deploy.hooks.allow_failure);
    // A manual job that may fail is not waited for by the stages after it.
    assert_eq!(task(&l, "cleanup").depends_on, ["build"]);
    assert_eq!(
        task(&l, "cleanup").fields["trigger_rule"],
        json!("all_done")
    );
    assert_eq!(
        task(&l, "notify").fields["trigger_rule"],
        json!("one_failed")
    );
}

#[test]
fn an_on_failure_job_with_nothing_before_it_is_left_out_and_waited_for_by_none() {
    let text = "
image: alpine
stages: [first, second]
page:
  stage: first
  when: on_failure
  script: [page]
after:
  stage: second
  needs: [page]
  script: [after]
";
    let l = lowered(text, &push("main"));
    assert_eq!(names(&l), ["after"]);
    assert!(task(&l, "after").depends_on.is_empty());
}

#[test]
fn rules_changes_asks_for_the_changed_files_and_then_reads_them() {
    let text = "
image: alpine
docs:
  script: [mkdocs build]
  rules:
    - changes: [docs/**/*]
";
    let first = lowered(text, &push("main"));
    assert!(first.needs_changes);
    let mut listed = push("main");
    listed.changed = Changes::Listed(vec!["src/main.rs".into()]);
    let l = lowered(text, &listed);
    assert!(!l.needs_changes);
    assert!(l.passed_over.is_some());
    listed.changed = Changes::Listed(vec!["docs/guide/index.md".into()]);
    assert_eq!(names(&lowered(text, &listed)), ["docs"]);
    // A tag compares nothing: changes holds, as GitLab has it.
    let l = lowered(text, &tag("v1"));
    assert!(!l.needs_changes);
    assert_eq!(names(&l), ["docs"]);
}

#[test]
fn a_job_without_an_image_takes_its_tags_image_or_is_refused() {
    let text = "
job:
  tags: [docker, linux]
  script: [true]
";
    let settings = Settings {
        tags: BTreeMap::from([("linux".to_string(), "debian:12".to_string())]),
        ..Settings::default()
    };
    let l = lowered_with(text, &push("main"), &settings);
    assert_eq!(task(&l, "job").fields["docker_image"], json!("debian:12"));
    let p = read(text)
        .lower("gitlab-ci", &push("main"), &Settings::default())
        .unwrap_err();
    assert!(p[0].message.contains("gitlab: tags:"), "{p:?}");
}

#[test]
fn variables_expand_and_the_file_s_are_under_a_pipeline_s() {
    let text = "
image: alpine
variables:
  NAME: app
  IMAGE: registry.example/$NAME:$CI_COMMIT_SHORT_SHA
  LOOP: x$LOOP
  COST: $$5
job:
  variables: {NAME: job}
  script: [echo $IMAGE]
";
    let l = lowered(text, &push("main"));
    let job = task(&l, "job");
    // Expanded with the job's own NAME, as GitLab expands it.
    assert_eq!(env(job, "IMAGE"), Some("registry.example/job:4b4708e7"));
    assert_eq!(env(job, "NAME"), Some("job"));
    assert_eq!(env(job, "LOOP"), Some("x$LOOP"), "left for the shell");
    assert_eq!(env(job, "COST"), Some("$5"));
    assert_eq!(env(job, "CI_COMMIT_REF_SLUG"), Some("main"));
    let mut dispatch = push("main");
    dispatch.kind = "dispatch".into();
    dispatch.inputs = BTreeMap::from([("NAME".to_string(), "given".to_string())]);
    assert_eq!(
        env(task(&lowered(text, &dispatch), "job"), "NAME"),
        Some("given")
    );
    let own = read("image: alpine\njob:\n  script: [x]\n  variables: {CI_STEP_COUNT: '9'}\n")
        .lower("gitlab-ci", &push("main"), &Settings::default())
        .unwrap_err();
    assert!(own[0].message.contains("dagron's steps read"), "{own:?}");
}

#[test]
fn a_script_stops_at_its_first_failure_and_after_script_runs_whatever_became_of_it() {
    let text = "
image: alpine
job:
  before_script: [cd app]
  script:
    - make
    - echo 'it''s {{ not a parameter }}'
  after_script: [rm -rf tmp]
";
    let l = lowered(text, &push("main"));
    let job = task(&l, "job");
    let run = &job.steps[0].run;
    assert!(run.starts_with("set -e\n"), "{run}");
    assert!(run.contains("printf '$ %s\\n' 'cd app'"), "{run}");
    assert!(!run.contains("{{ not"), "a {{ is text: {run}");
    let after = &job.steps[1];
    assert_eq!(after.condition.as_deref(), Some("always()"));
    assert!(after.continue_on_error);
}

#[test]
fn what_is_not_taken_is_refused_with_what_it_is() {
    for (text, needle) in [
        ("job:\n  script: [x]\n  only: [main]\n", "rules:"),
        ("job:\n  script: [x]\n  trigger: other/project\n", "level 2"),
        ("job:\n  stage: nowhere\n  script: [x]\n", "stages"),
        ("job:\n  image: alpine\n", "every job has one"),
        (
            "job:\n  script: [x]\n  rules: [{if: '$A ='}]\n",
            "not an operator",
        ),
        (
            "job:\n  script: [x]\n  artifacts: {untracked: true}\n",
            "not taken yet",
        ),
        ("job:\n  script: [x]\n  extends: .missing\n", ".missing"),
        ("job:\n  script: [x]\n  retry: 5\n", "0, 1 or 2"),
        (
            ".a: {extends: .b}\n.b: {extends: .a}\njob: {extends: .a, script: [x]}\n",
            "circle",
        ),
        (
            "job:\n  script: [!reference [.none, script]]\n",
            "names nothing",
        ),
    ] {
        let p = problems(text);
        assert!(p.contains(needle), "{text}: {p}");
    }
}

#[test]
fn durations_read_as_gitlab_writes_them() {
    for (text, secs) in [
        ("3600", 3600),
        ("1h 30m", 5400),
        ("90 minutes", 5400),
        ("1 week", 604_800),
        ("2 days 4 hours", 187_200),
        ("1 hour and 30 minutes", 5400),
    ] {
        let parsed = duration_secs(text.replace(" and", ",").as_str());
        assert_eq!(parsed, Some(secs), "{text}");
    }
    assert_eq!(duration_secs("soon"), None);
}

#[test]
fn a_lowered_pipeline_compiles() {
    let l = lowered(STAGES, &push("main"));
    let out = compile::compile(&l.pipeline, &push("main"), &compile::Settings::default()).unwrap();
    let Outcome::Run(compiled) = out else {
        panic!("{out:?}")
    };
    assert!(
        compiled.yaml.contains("CI_ARTIFACT_UPLOAD_PATHS"),
        "{}",
        compiled.yaml
    );
    assert!(
        compiled.yaml.contains("CI_ARTIFACT_DOWNLOAD"),
        "{}",
        compiled.yaml
    );
    dagron_core::dag::DagGraph::from_yaml_with_params(&compiled.yaml, &compiled.parameters)
        .unwrap_or_else(|e| panic!("{e}: {}", compiled.yaml));
}

#[test]
fn the_settings_name_the_file_and_map_tags() {
    let s =
        settings(&json!({"pipeline": "/.gitlab-ci.yml", "tags": {"docker": "alpine:3"}})).unwrap();
    assert_eq!(s.pipeline.as_deref(), Some(".gitlab-ci.yml"));
    assert_eq!(s.tags["docker"], "alpine:3");
    assert!(settings(&json!({"pipeline": "../x.yml"})).is_err());
    assert!(settings(&json!({"other": 1})).is_err());
}

#[test]
fn names_a_task_cannot_have_are_made_ones_it_can() {
    assert_eq!(task_name("build:linux arm"), "build-linux-arm");
    assert_eq!(task_name("::"), "job");
    assert_eq!(
        service_name("docker.io/library/postgres"),
        "docker-io-library-postgres"
    );
    assert_eq!(service_name("postgres"), "postgres");
    assert_eq!(service_name("9db"), "s9db");
    assert_eq!(slug("Feature/ABC_1"), "feature-abc-1");
    assert_eq!(
        service_alias("registry.example:5000/team/db:16"),
        "registry.example:5000-team-db"
    );
}

#[test]
fn a_rule_without_when_takes_the_job_s_and_one_that_makes_a_job_manual_blocks() {
    let text = "
image: alpine
stages: [deploy, verify]
deploy:
  stage: deploy
  when: manual
  rules: [{if: $CI_COMMIT_BRANCH == $CI_DEFAULT_BRANCH}]
  script: [./deploy]
release:
  stage: deploy
  rules: [{if: $CI_COMMIT_BRANCH, when: manual}]
  script: [./release]
verify:
  stage: verify
  script: [./verify]
";
    let l = lowered(text, &push("main"));
    // The job's own `when: manual` holds where its rule says none.
    assert_eq!(task(&l, "deploy").depends_on, ["deploy.approve"]);
    assert!(
        task(&l, "deploy").hooks.allow_failure,
        "a job-level manual may fail"
    );
    assert_eq!(
        task(&l, "deploy.approve").fields["allow_failure"],
        json!(true)
    );
    // A rule's manual may not fail: the stage after waits for it.
    assert!(!task(&l, "release").hooks.allow_failure);
    assert_eq!(task(&l, "verify").depends_on, ["release"]);
}

#[test]
fn a_job_that_may_fail_is_the_step_runner_s_to_end_and_an_on_failure_one_skipped_stops_nothing() {
    let text = "
image: alpine
stages: [test, notify, deploy]
lint:
  stage: test
  allow_failure: true
  script: [exit 1]
page:
  stage: notify
  when: on_failure
  script: [./page]
deploy:
  stage: deploy
  script: [./deploy]
";
    let l = lowered(text, &push("main"));
    let lint = task(&l, "lint");
    assert!(lint.hooks.allow_failure);
    assert!(
        !lint.fields.contains_key("allow_failure"),
        "not dagron's: it skips what follows"
    );
    // `page` is skipped where nothing failed, and `deploy` runs all the same.
    assert_eq!(
        task(&l, "deploy").fields["trigger_rule"],
        json!("none_failed")
    );
    let out = compile::compile(&l.pipeline, &push("main"), &compile::Settings::default()).unwrap();
    let Outcome::Run(compiled) = out else {
        panic!("{out:?}")
    };
    assert!(
        compiled.yaml.contains("CI_JOB_ALLOW_FAILURE"),
        "{}",
        compiled.yaml
    );
    assert!(compiled.yaml.contains("--speaks 5"), "{}", compiled.yaml);
}

#[test]
fn a_reference_reads_a_job_as_its_templates_made_it() {
    let text = "
image: alpine
.base: {script: [echo hi]}
a: {extends: .base}
b: {script: [!reference [a, script], echo after]}
";
    let l = lowered(text, &push("main"));
    let run = &task(&l, "b").steps[0].run;
    assert!(
        run.contains("echo hi") && run.contains("echo after"),
        "{run}"
    );
}

#[test]
fn what_a_small_file_could_grow_into_is_bounded() {
    // Each variable names the one before it a hundred times.
    let mut text = String::from("image: alpine\nvariables:\n  V0: x\n");
    for i in 1..10 {
        text.push_str(&format!(
            "  V{i}: \"{}\"\n",
            format!("$V{}", i - 1).repeat(100)
        ));
    }
    text.push_str("job:\n  script: [echo $V9]\n");
    let p = read(&text)
        .lower("gitlab-ci", &push("main"), &Settings::default())
        .unwrap_err();
    assert!(
        p.iter().any(|p| p.message.contains("expands to more than")),
        "{p:?}"
    );

    // Each template level references the one below it a hundred times.
    let mut text = String::from("image: alpine\n.l0: {s: [x]}\n");
    for i in 1..10 {
        let refs = (0..100)
            .map(|_| format!("!reference [.l{}, s]", i - 1))
            .collect::<Vec<_>>()
            .join(", ");
        text.push_str(&format!(".l{i}: {{s: [{refs}]}}\n"));
    }
    text.push_str("job:\n  script: [!reference [.l9, s]]\n");
    let p = problems(&text);
    assert!(p.contains("more than"), "{p}");
}

#[test]
fn a_variable_expand_false_keeps_and_ci_project_dir_is_the_job_s_directory() {
    let text = "
image: alpine
variables:
  RAW: {value: 'a$CI_COMMIT_SHA', expand: false}
  PIP_CACHE_DIR: $CI_PROJECT_DIR/.cache/pip
job:
  script: [pip install .]
";
    let l = lowered(text, &push("main"));
    let job = task(&l, "job");
    assert_eq!(env(job, "RAW"), Some("a$CI_COMMIT_SHA"));
    let run = &job.steps[0].run;
    assert!(
        run.contains("export PIP_CACHE_DIR=\"$CI_PROJECT_DIR\"'/.cache/pip'"),
        "{run}"
    );
    assert!(
        run.find("export CI_PROJECT_DIR").unwrap() < run.find("export PIP_CACHE_DIR").unwrap(),
        "{run}"
    );
}

#[test]
fn a_dependency_that_is_no_job_and_an_input_that_is_gitlab_s_are_refused() {
    let text = "
image: alpine
stages: [build, test]
build: {stage: build, script: [make]}
test: {stage: test, dependencies: [biuld], script: [make test]}
";
    let p = read(text)
        .lower("gitlab-ci", &push("main"), &Settings::default())
        .unwrap_err();
    assert!(p[0].message.contains("\"biuld\" is no job"), "{p:?}");
    let mut dispatch = push("feature");
    dispatch.kind = "dispatch".into();
    dispatch.inputs = BTreeMap::from([("CI_COMMIT_BRANCH".to_string(), "main".to_string())]);
    let p = read("image: alpine\njob: {script: [x]}\n")
        .lower("gitlab-ci", &dispatch, &Settings::default())
        .unwrap_err();
    assert!(p[0].message.contains("GitLab's own"), "{p:?}");
}

#[test]
fn the_settings_keep_a_gitlab_pipeline_out_of_dagron_s_own_places() {
    assert!(settings(&json!({"pipeline": ".dagron/ci/build.yaml"})).is_err());
    assert!(settings(&json!({"pipeline": "ci/my build.yml"})).is_err());
    assert!(settings_in("github: {workflows: [x]}\nother: 1\n")
        .unwrap()
        .pipeline
        .is_none());
    assert_eq!(
        settings_in("gitlab: {pipeline: ci/main.yml}\n")
            .unwrap()
            .pipeline
            .as_deref(),
        Some("ci/main.yml")
    );
}
