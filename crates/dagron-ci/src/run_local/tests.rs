//! `dagron-ci run`, piece by piece: its flags, what it reads of a checkout,
//! the event and the changed files it makes of that, the engine it starts,
//! and how it prints what the engine's database says. The whole of it,
//! through the binaries and a real engine, is `tests/run.rs`.

use super::*;

const SHA: &str = "4b4708e725f29891d22b4c96af9f0a403ff51c7c";

fn options_of(args: &str) -> Result<Options<'static>, String> {
    // Leaked: a test's arguments live as long as the test.
    let args: &'static str = Box::leak(args.to_string().into_boxed_str());
    let args: Vec<&'static str> = args.split_whitespace().collect();
    options(Box::leak(args.into_boxed_slice()))
}

#[test]
fn flags_and_files_come_in_any_order() {
    let o = options_of(&format!(
        "ci.yaml --executor local --ref refs/heads/dev lint.yml --keep --engine /opt/dagron --sha {SHA}"
    ))
    .unwrap();
    assert_eq!(o.files, ["ci.yaml", "lint.yml"]);
    assert_eq!(o.executor, Executor::Local);
    assert_eq!(o.engine, Some("/opt/dagron"));
    assert!(o.keep && !o.help && !o.docker_daemon);
    assert!(options_of("--docker-daemon").unwrap().docker_daemon);
    assert_eq!(o.flags.git_ref.as_deref(), Some("refs/heads/dev"));
    assert_eq!(o.flags.sha.as_deref(), Some(SHA));

    let o = options_of("").unwrap();
    assert_eq!(
        (o.executor, o.files.len(), o.keep),
        (Executor::Docker, 0, false)
    );
    assert!(options_of("--help").unwrap().help);
    for bad in [
        "--executor kubernetes",
        "--event delete",
        "--json",
        "--pr seven",
        "--repo github.com/acme",
        "--input novalue",
    ] {
        assert!(options_of(bad).is_err(), "{bad:?}");
    }
}

#[test]
fn an_origin_names_its_forge_and_repository() {
    let named = |host: &str, name: &str| Some((host.to_string(), name.to_string()));
    assert_eq!(
        forge_of("https://github.com/Acme/Widgets.git"),
        named("github.com", "acme/widgets")
    );
    assert_eq!(
        forge_of("git@github.com:acme/widgets.git"),
        named("github.com", "acme/widgets")
    );
    assert_eq!(
        forge_of("github.com:acme/widgets"),
        named("github.com", "acme/widgets")
    );
    assert_eq!(
        forge_of("ssh://git@gitlab.example:2222/group/sub/project.git"),
        named("gitlab.example", "group/sub/project"),
        "an SSH port is SSH's, not the forge's"
    );
    assert_eq!(
        forge_of("https://ci:t0ken@ghe.example:8443/acme/widgets/"),
        named("ghe.example:8443", "acme/widgets"),
        "a web port is kept; a credential never"
    );
    for none in [
        "/srv/git/widgets.git",
        "file:///srv/git/widgets.git",
        "../widgets",
        "github.com",
        "https://github.com/acme",
        "C:\\src\\widgets",
        "ftp://example.com/acme/widgets",
    ] {
        assert_eq!(forge_of(none), None, "{none}");
    }
}

fn on_a_branch() -> Checkout {
    Checkout {
        root: "/src/widgets".into(),
        git: true,
        head: Some(SHA.into()),
        branch: Some("refs/heads/feature/x".into()),
        origin: Some(("github.com".into(), "acme/widgets".into())),
    }
}

#[test]
fn the_event_is_a_push_of_the_branch_head_is_on() {
    let e = event(&EventFlags::default(), &on_a_branch()).unwrap();
    assert_eq!(
        (
            e.kind.as_str(),
            e.git_ref.as_str(),
            e.sha.as_str(),
            e.trust.as_str()
        ),
        ("push", "refs/heads/feature/x", SHA, "trusted")
    );
    assert_eq!(
        (e.forge_host.as_str(), e.repo.as_str()),
        ("github.com", "acme/widgets")
    );
    assert_eq!(
        e.changed,
        Changes::Unknown,
        "looked up only when a filter asks"
    );

    // What the flags say wins.
    let o = options_of(
        "--event pull_request --ref refs/pull/7/head --base-ref main --pr 7 \
         --repo gitlab.example/g/w --changed a.rs --changed b.rs",
    )
    .unwrap();
    let e = event(&o.flags, &on_a_branch()).unwrap();
    assert_eq!(
        (e.kind.as_str(), e.git_ref.as_str()),
        ("pull_request", "refs/pull/7/head")
    );
    assert_eq!((e.base_ref.as_deref(), e.pr), (Some("main"), Some(7)));
    assert_eq!(
        (e.forge_host.as_str(), e.repo.as_str()),
        ("gitlab.example", "g/w")
    );
    assert_eq!(
        e.changed,
        Changes::Listed(vec!["a.rs".into(), "b.rs".into()])
    );
}

#[test]
fn an_event_without_a_ref_to_be_had_is_refused() {
    let detached = Checkout {
        branch: None,
        ..on_a_branch()
    };
    let why = event(&EventFlags::default(), &detached).unwrap_err();
    assert!(
        why.contains("not on a branch") && why.contains("--ref"),
        "{why}"
    );
    let o = options_of("--ref refs/heads/main").unwrap();
    assert_eq!(
        event(&o.flags, &detached).unwrap().git_ref,
        "refs/heads/main"
    );

    let why = event(&options_of("--event tag").unwrap().flags, &on_a_branch()).unwrap_err();
    assert!(why.contains("refs/tags/"), "{why}");

    // Outside git: a ref must be named, and the commit is all zeros.
    let elsewhere = Checkout {
        root: "/tmp/x".into(),
        ..Checkout::default()
    };
    assert!(event(&EventFlags::default(), &elsewhere)
        .unwrap_err()
        .contains("not a git checkout"));
    let e = event(
        &options_of("--ref refs/heads/main").unwrap().flags,
        &elsewhere,
    )
    .unwrap();
    assert_eq!(e.sha, "0".repeat(40));
    assert_eq!(
        (e.forge_host.as_str(), e.repo.as_str()),
        ("localhost", "local/checkout")
    );

    assert!(event(&options_of("--sha abc123").unwrap().flags, &on_a_branch()).is_err());
}

/// A git repository in a directory of its own, with what a test does to it.
struct Repo {
    dir: tempfile::TempDir,
}

impl Repo {
    /// On `main`, one commit holding `README` and a `.gitignore`.
    fn new() -> Repo {
        let repo = Repo {
            dir: tempfile::tempdir().unwrap(),
        };
        repo.git(&["init", "-q", "-b", "main"]);
        repo.write("README", "hello\n");
        repo.write(".gitignore", "target/\n");
        repo.commit("first");
        repo
    }

    fn path(&self) -> &Path {
        self.dir.path()
    }

    fn git(&self, args: &[&str]) -> String {
        let out = Command::new("git")
            .args([
                "-c",
                "user.email=t@example.com",
                "-c",
                "user.name=t",
                "-c",
                "commit.gpgsign=false",
            ])
            .args(args)
            .current_dir(self.path())
            .output()
            .unwrap();
        assert!(
            out.status.success(),
            "git {args:?}: {}",
            String::from_utf8_lossy(&out.stderr)
        );
        String::from_utf8(out.stdout).unwrap().trim().to_string()
    }

    fn write(&self, path: &str, text: &str) {
        let path = self.path().join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }

    fn commit(&self, message: &str) {
        self.git(&["add", "-A"]);
        self.git(&["commit", "-q", "-m", message]);
    }

    /// An `origin` this repository has pushed `main` to, as its upstream.
    fn with_origin(self) -> (Repo, tempfile::TempDir) {
        let origin = tempfile::tempdir().unwrap();
        let bare = origin.path().join("widgets.git");
        self.git(&["init", "-q", "--bare", bare.to_str().unwrap()]);
        self.git(&["remote", "add", "origin", bare.to_str().unwrap()]);
        self.git(&["push", "-q", "-u", "origin", "main"]);
        (self, origin)
    }
}

#[test]
fn a_checkout_is_read_from_git() {
    let repo = Repo::new();
    repo.git(&["remote", "add", "origin", "git@github.com:acme/widgets.git"]);
    let co = checkout(repo.path());
    assert!(co.git);
    assert_eq!(
        std::fs::canonicalize(&co.root).unwrap(),
        std::fs::canonicalize(repo.path()).unwrap()
    );
    assert_eq!(co.head, Some(repo.git(&["rev-parse", "HEAD"])));
    assert_eq!(co.branch.as_deref(), Some("refs/heads/main"));
    assert_eq!(
        co.origin,
        Some(("github.com".into(), "acme/widgets".into()))
    );

    // From a directory inside it, the same checkout.
    repo.write("src/lib.rs", "");
    assert_eq!(checkout(&repo.path().join("src")).root, co.root);

    repo.git(&["checkout", "-q", "--detach"]);
    assert_eq!(checkout(repo.path()).branch, None);

    let plain = tempfile::tempdir().unwrap();
    let co = checkout(plain.path());
    assert!(!co.git && co.head.is_none());
    assert_eq!(co.root, plain.path());
}

#[test]
fn the_changed_files_run_from_the_upstream_to_the_working_tree() {
    let (repo, _origin) = Repo::new().with_origin();
    repo.write("gone.txt", "to be deleted");
    repo.commit("pushed");
    repo.git(&["push", "-q"]);
    assert_eq!(
        changed_files(&checkout(repo.path()), &EventFlags::default()),
        Changes::Listed(vec![]),
        "nothing changed since the push"
    );
    repo.write("src/committed.rs", "1");
    repo.commit("not pushed yet");
    repo.write("docs/staged.md", "2");
    repo.git(&["add", "docs/staged.md"]);
    repo.write("README", "changed, not staged\n");
    repo.write("new.txt", "untracked");
    repo.write("target/debug/ignored", "ignored");
    repo.git(&["rm", "-q", "gone.txt"]);
    assert_eq!(
        changed_files(&checkout(repo.path()), &EventFlags::default()),
        Changes::Listed(
            [
                "README",
                "docs/staged.md",
                "gone.txt",
                "new.txt",
                "src/committed.rs"
            ]
            .map(String::from)
            .to_vec()
        )
    );
}

#[test]
fn a_base_names_what_the_working_tree_is_compared_with() {
    let (repo, _origin) = Repo::new().with_origin();
    repo.git(&["checkout", "-q", "-b", "feature"]);
    repo.write("src/feature.rs", "1");
    repo.commit("feature");
    let co = checkout(repo.path());

    let none = changed_files(&co, &EventFlags::default());
    assert!(
        matches!(&none, Changes::Unlisted(why) if why.contains("no upstream")),
        "a new branch has nothing to compare with: {none:?}"
    );
    let base = options_of("--base-ref main").unwrap().flags;
    assert_eq!(
        changed_files(&co, &base),
        Changes::Listed(vec!["src/feature.rs".into()])
    );
    let base = options_of("--base-ref refs/heads/main").unwrap().flags;
    assert_eq!(
        changed_files(&co, &base),
        Changes::Listed(vec!["src/feature.rs".into()])
    );
    let main = repo.git(&["rev-parse", "main"]);
    let base = options_of(&format!("--base-sha {main}")).unwrap().flags;
    assert_eq!(
        changed_files(&co, &base),
        Changes::Listed(vec!["src/feature.rs".into()])
    );

    for (flags, said) in [
        (format!("--base-sha {SHA}"), "is not a commit here"),
        ("--base-ref nope".to_string(), "is not a branch here"),
    ] {
        let got = changed_files(&co, &options_of(&flags).unwrap().flags);
        assert!(
            matches!(&got, Changes::Unlisted(why) if why.contains(said)),
            "{flags}: {got:?}"
        );
    }

    repo.git(&["checkout", "-q", "--orphan", "unrelated"]);
    repo.commit("no shared history");
    let got = changed_files(
        &checkout(repo.path()),
        &options_of("--base-ref main").unwrap().flags,
    );
    assert!(
        matches!(&got, Changes::Unlisted(why) if why.contains("share no history")),
        "{got:?}"
    );
}

#[test]
fn without_git_or_a_commit_the_changed_files_are_not_to_be_had() {
    let plain = tempfile::tempdir().unwrap();
    assert!(matches!(
        changed_files(&checkout(plain.path()), &EventFlags::default()),
        Changes::Unlisted(_)
    ));
    let fresh = tempfile::tempdir().unwrap();
    let out = Command::new("git")
        .args(["init", "-q"])
        .current_dir(fresh.path())
        .status()
        .unwrap();
    assert!(out.success());
    let got = changed_files(&checkout(fresh.path()), &EventFlags::default());
    assert!(
        matches!(&got, Changes::Unlisted(why) if why.contains("no commit")),
        "{got:?}"
    );
}

#[test]
fn pipelines_are_the_checkout_s_unless_named() {
    let repo = Repo::new();
    let co = checkout(repo.path());
    let why = pipeline_files(&co, repo.path(), &[], &[]).unwrap_err();
    assert!(why.contains("no pipeline files"), "{why}");
    assert!(why.contains("lists no workflow file"), "{why}");
    repo.write(".dagron/ci/b.yml", "");
    repo.write(".dagron/ci/a.yaml", "");
    repo.write(".dagron/ci/notes.txt", "");
    // Read from the checkout wherever this process is; shown from where it is.
    let files = pipeline_files(&co, &co.root, &[], &[]).unwrap();
    assert_eq!(
        files,
        [
            PathBuf::from(".dagron/ci/a.yaml"),
            PathBuf::from(".dagron/ci/b.yml")
        ]
    );
    let files = pipeline_files(&co, Path::new("/elsewhere"), &[], &[]).unwrap();
    assert_eq!(files[0], co.root.join(".dagron/ci/a.yaml"));
    assert_eq!(
        pipeline_files(&co, repo.path(), &["x.yaml"], &[]).unwrap(),
        [PathBuf::from("x.yaml")]
    );
    // And the workflow files the repository's settings list, after them.
    let listed = vec!["build.yml".to_string()];
    let files = pipeline_files(&co, &co.root, &[], &listed).unwrap();
    assert_eq!(files[2], PathBuf::from(".github/workflows/build.yml"));
}

#[test]
fn a_file_of_github_workflows_is_a_workflow_file() {
    assert!(is_workflow(Path::new(".github/workflows/build.yml")));
    assert!(is_workflow(Path::new("/r/.github/workflows/build.yml")));
    assert!(!is_workflow(Path::new(".dagron/ci/build.yml")));
    assert!(!is_workflow(Path::new(".github/workflows/sub/build.yml")));
}

fn compiled_push(pipeline: &str) -> compile::Compiled {
    let pipeline = crate::pipeline::parse(pipeline).unwrap();
    let event = event(
        &EventFlags::default(),
        &Checkout {
            branch: Some("refs/heads/main".into()),
            ..on_a_branch()
        },
    )
    .unwrap();
    match compile::compile(&pipeline, &event, &Settings::default()) {
        Ok(Outcome::Run(c)) => c,
        other => panic!("{other:?}"),
    }
}

const PIPELINE: &str = "name: ci
on: { push: { branches: [main] } }
concurrency: { key: \"{{ ci_group }}\" }
parameters: { profile: release }
task_defaults: { docker_image: \"rust:1.86\" }
tasks:
  - name: build
    steps: [{ run: cargo build }]
  - name: docs
    docker_image: \"node:20\"
    steps: [{ run: npm run docs }]
";

#[test]
fn the_run_s_parameters_become_the_file_s_defaults() {
    let (yaml, doc) = with_parameters(&compiled_push(PIPELINE)).unwrap();
    assert_eq!(doc["parameters"]["ci_sha"], SHA);
    assert_eq!(doc["parameters"]["ci_repo"], "acme/widgets");
    assert_eq!(
        doc["parameters"]["profile"], "release",
        "a pipeline's own default stays"
    );

    // dagron reads it as a run with those parameters: the concurrency key,
    // which may not name a parameter that has no value, resolves.
    let graph = dagron_core::dag::DagGraph::from_yaml(&yaml).unwrap();
    assert_eq!(
        graph.spec.concurrency_key.as_deref(),
        Some("github.com/acme/widgets:refs/heads/main")
    );
    assert_eq!(graph.parameters["ci_ref"], "refs/heads/main");
}

#[test]
fn the_images_a_run_needs_are_its_tasks() {
    let (_, doc) = with_parameters(&compiled_push(PIPELINE)).unwrap();
    assert_eq!(
        images(&doc),
        BTreeSet::from(["node:20".to_string(), "rust:1.86".to_string()])
    );
    let doc: Yaml = serde_yaml::from_str(
        "tasks:\n  - { name: a, command: [make] }\n  - { name: gate, type: approval }\n",
    )
    .unwrap();
    assert_eq!(images(&doc), BTreeSet::from([DEFAULT_IMAGE.to_string()]));
}

/// An image a task's items fill in is one image for each item, and is
/// pulled as each: it used to be pulled as it is written, braces and all,
/// which no registry has. One that names something else is the executor's
/// to pull, when the run has said what.
#[test]
fn an_image_a_task_s_items_fill_in_is_one_for_each_item() {
    let doc: Yaml = serde_yaml::from_str(
        "tasks:\n  - name: plain\n    command: [make]\n    docker_image: \"node:{{ item }}-slim\"\n    with_items: [18, \"20\"]\n  - name: keyed\n    command: [make]\n    docker_image: \"{{ item.image }}\"\n    with_items:\n      - { v: 1, image: \"python:3.12\" }\n      - { v: 2, image: \"python:3.13\" }\n      - { v: 3 }\n  - name: later\n    command: [make]\n    docker_image: \"registry.example/{{ ci_repo }}:{{ item }}\"\n    with_items: [a]\n  - name: nowhere\n    command: [make]\n    docker_image: \"node:{{ item }}\"\n",
    )
    .unwrap();
    assert_eq!(
        images(&doc),
        BTreeSet::from([
            "node:18-slim".to_string(),
            "node:20-slim".to_string(),
            "python:3.12".to_string(),
            "python:3.13".to_string(),
        ])
    );
    let item: Yaml = serde_yaml::from_str("{ a: 1, b-c: x, d: [1] }").unwrap();
    for (text, filled) in [
        ("{{item.a}}/{{ item.b-c }}", "1/x"),
        ("{{ item }}", "{{ item }}"),
        ("{{ item.d }} {{ item.none }} {{ items }}", "{{ item.d }} {{ item.none }} {{ items }}"),
        ("a {{ item.a", "a {{ item.a"),
    ] {
        assert_eq!(of_item(text, &item), filled, "{text}");
    }
}

#[test]
fn the_working_tree_is_mounted_where_it_is() {
    let root = Path::new("/src/widgets");
    assert_eq!(
        docker_mounts(root, None).unwrap(),
        "/src/widgets:/src/widgets"
    );
    let mounts = docker_mounts(root, Some(Path::new("/opt/bin/dagron-ci-exec"))).unwrap();
    assert_eq!(
        mounts,
        "/src/widgets:/src/widgets,/opt/bin/dagron-ci-exec:/dagron/bin/dagron-ci-exec:ro"
    );
    // What the engine reads: the same two mounts, the second read-only.
    let parsed = dagron_executor::docker_executor::parse_mounts(&mounts).unwrap();
    assert_eq!(parsed.len(), 2);
    assert!(!parsed[0].read_only && parsed[1].read_only);
    assert_eq!(parsed[1].target, EXEC_IN_TASK);

    for bad in ["/src/a,b", "/src/a:b"] {
        let why = docker_mounts(Path::new(bad), None).unwrap_err();
        assert!(why.contains("--executor local"), "{why}");
    }
}

fn envs(cmd: &Command) -> BTreeMap<String, Option<String>> {
    cmd.get_envs()
        .map(|(k, v)| {
            (
                k.to_string_lossy().into_owned(),
                v.map(|v| v.to_string_lossy().into_owned()),
            )
        })
        .collect()
}

#[test]
fn the_engine_runs_without_its_own_settings() {
    let dir = tempfile::tempdir().unwrap();
    let settings = ["API_ADDR", "GITHUB_TOKEN", "EXECUTOR", "RUNNER_CLASSES"].map(String::from);
    let plan = Plan {
        engine: Path::new("/opt/dagron"),
        settings: &settings,
        executor: Executor::Docker,
        root: Path::new("/src/widgets"),
        exec: Some(Path::new("/opt/dagron-ci-exec")),
        keep: false,
        docker_daemon: true,
    };
    let cmd = engine_command(&plan, dir.path()).unwrap();
    let env = envs(&cmd);
    for gone in [
        "API_ADDR",
        "GITHUB_TOKEN",
        "RUNNER_CLASSES",
        "DAGRON_CONFIG",
    ] {
        assert_eq!(env.get(gone), Some(&None), "{gone} is removed");
    }
    let set = |k: &str| env.get(k).cloned().flatten();
    assert_eq!(
        set("EXECUTOR").as_deref(),
        Some("docker"),
        "removed, then the run's own"
    );
    assert_eq!(set("SOURCE").as_deref(), Some("file"));
    assert_eq!(set("DOCKER_IMAGE").as_deref(), Some(DEFAULT_IMAGE));
    assert_eq!(
        set(dagron_executor::docker_executor::ENV_MOUNTS).as_deref(),
        Some("/src/widgets:/src/widgets,/opt/dagron-ci-exec:/dagron/bin/dagron-ci-exec:ro")
    );
    assert_eq!(
        set(dagron_executor::docker_executor::ENV_WORKDIR).as_deref(),
        Some("/src/widgets")
    );
    assert_eq!(
        set(dagron_core::docker_daemon::ENV).as_deref(),
        Some("dind"),
        "a job that asks has a daemon, with --docker-daemon"
    );
    assert_eq!(cmd.get_current_dir(), Some(Path::new("/src/widgets")));
    assert_eq!(cmd.get_program(), "/opt/dagron");
    // Without it, the engine offers none.
    let plan = Plan {
        docker_daemon: false,
        ..plan
    };
    let env = envs(&engine_command(&plan, dir.path()).unwrap());
    assert_eq!(
        env.get(dagron_core::docker_daemon::ENV).cloned().flatten(),
        None,
        "not set"
    );
}

/// A job that asks for a Docker daemon, which is privileged on this
/// machine's Docker host, has one only where the run was told it may: the
/// run stops before it starts, and says which job asked and how.
#[test]
fn a_job_has_a_docker_daemon_only_where_the_run_was_told_it_may() {
    let doc: Yaml = serde_yaml::from_str(
        "tasks:\n  - { name: lint, command: [x] }\n  - { name: image, command: [x], docker_daemon: true }\n",
    )
    .unwrap();
    let plan = |executor, docker_daemon| Plan {
        engine: Path::new("/opt/dagron"),
        settings: &[],
        executor,
        root: Path::new("/src/widgets"),
        exec: None,
        keep: false,
        docker_daemon,
    };
    assert_eq!(daemon_refusal(&doc, &plan(Executor::Docker, true)), None);
    let why = daemon_refusal(&doc, &plan(Executor::Docker, false)).unwrap();
    assert!(
        why.contains("\"image\"") && why.contains("--docker-daemon"),
        "{why}"
    );
    let why = daemon_refusal(&doc, &plan(Executor::Local, true)).unwrap();
    assert!(why.contains("--executor docker"), "{why}");
    let none: Yaml = serde_yaml::from_str("tasks:\n  - { name: lint, command: [x] }\n").unwrap();
    assert_eq!(daemon_refusal(&none, &plan(Executor::Docker, false)), None);
}

#[cfg(unix)]
#[test]
fn a_local_task_finds_dagron_ci_exec_first_on_its_path() {
    let dir = tempfile::tempdir().unwrap();
    let plan = Plan {
        engine: Path::new("/opt/dagron"),
        settings: &[],
        executor: Executor::Local,
        root: Path::new("/src/widgets"),
        exec: Some(Path::new("/opt/bin/dagron-ci-exec")),
        keep: false,
        docker_daemon: true,
    };
    let cmd = engine_command(&plan, dir.path()).unwrap();
    let env = envs(&cmd);
    let path = env["PATH"].clone().unwrap();
    let first = std::env::split_paths(&path).next().unwrap();
    assert_eq!(first, dir.path().join("bin"));
    let held: Vec<_> = std::fs::read_dir(&first)
        .unwrap()
        .map(|e| e.unwrap().file_name())
        .collect();
    assert_eq!(
        held,
        ["dagron-ci-exec"],
        "nothing beside it to shadow a task's programs"
    );
    assert_eq!(
        std::fs::read_link(first.join("dagron-ci-exec")).unwrap(),
        Path::new("/opt/bin/dagron-ci-exec")
    );
    assert!(!env.contains_key(dagron_executor::docker_executor::ENV_MOUNTS));
    assert!(!env.contains_key(dagron_core::docker_daemon::ENV));
    assert_eq!(env["EXECUTOR"].as_deref(), Some("local"));
    assert_eq!(
        env[dagron_executor::workspace::ENV_LOCAL_WORKDIR].as_deref(),
        Some("/src/widgets"),
        "every task in the tree, a job's workspace too"
    );
}

/// A script at `dir/name`, executable, and run once before it is returned:
/// a file just written can be "busy" to exec(2) while a fork elsewhere in the
/// test process still holds it open.
#[cfg(unix)]
fn script(dir: &Path, name: &str, body: &str) -> PathBuf {
    use std::os::unix::fs::PermissionsExt;
    let path = dir.join(name);
    std::fs::write(&path, format!("#!/bin/sh\n{body}\n")).unwrap();
    std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o755)).unwrap();
    for _ in 0..100 {
        match Command::new(&path).arg("probe").output() {
            Err(e) if e.raw_os_error() == Some(libc::ETXTBSY) => {
                std::thread::sleep(Duration::from_millis(20))
            }
            _ => break,
        }
    }
    path
}

#[cfg(unix)]
#[test]
fn the_engine_lists_its_own_settings() {
    let dir = tempfile::tempdir().unwrap();
    let engine = script(
        dir.path(),
        "engine",
        "[ \"$1 $2\" = 'config --json' ] || exit 9\n\
         echo 'INFO logging initialized'\n\
         echo '{\"knobs\": [{\"name\": \"API_ADDR\", \"value\": \"-\"}, {\"name\": \"EXECUTOR\"}]}'",
    );
    assert_eq!(engine_settings(&engine).unwrap(), ["API_ADDR", "EXECUTOR"]);

    for (n, body) in [
        "exit 0",
        "exit 1",
        "echo '{\"knobs\": []}'",
        "echo 'usage: something else'",
        "echo '{\"knobs\": [{\"name\": \"A\"}]}'; exit 2",
    ]
    .iter()
    .enumerate()
    {
        let not_one = script(dir.path(), &format!("other{n}"), body);
        let why = engine_settings(&not_one).unwrap_err();
        assert!(why.contains("is not a dagron engine"), "{body}: {why}");
    }
}

#[test]
fn a_program_is_the_one_given_else_beside_this_one_else_on_path() {
    assert_eq!(find_program("dagron", Some("/nonexistent/dagron")), None);
    // A bare name is looked up on PATH; `sh` is on every PATH a test has.
    assert!(find_program("sh", Some("sh")).is_some_and(|p| p.ends_with("sh")));
    assert!(find_program("sh", None).is_some_and(|p| p.ends_with("sh")));
    assert_eq!(find_program("no-such-program-anywhere", None), None);
}

#[test]
fn markers_read_as_what_they_say() {
    let r = |line: &str| render("test", line);
    assert_eq!(
        r("::dagron-ci::step::2::start::cargo test").as_deref(),
        Some("[test] == step 2: cargo test")
    );
    assert_eq!(
        r("::dagron-ci::step::2::end::0").as_deref(),
        Some("[test] == step 2 ok")
    );
    assert_eq!(
        r("::dagron-ci::step::2::end::101").as_deref(),
        Some("[test] == step 2 failed, exit 101")
    );
    assert_eq!(
        r("::dagron-ci::step::3::continued").as_deref(),
        Some("[test] == step 3 may fail (continue_on_error): the task goes on")
    );
    assert_eq!(
        r("::dagron-ci::warning::no timeout(1) in this image").as_deref(),
        Some("[test] warning: no timeout(1) in this image")
    );
    assert_eq!(
        r("::dagron-ci::cancelled::15").as_deref(),
        Some("[test] cancelled by signal 15")
    );
    // What the task's reports held: the counts, a place, a failure by name.
    assert_eq!(
        r(r#"::dagron-ci::report::{"tests":4,"failures":2,"errors":0,"skipped":1,"findings":2,"problems":["a.xml could not be read: no such file"]}"#).as_deref(),
        Some("[test] report: 4 tests, 2 failed, 0 errors, 1 skipped, 2 findings; a.xml could not be read: no such file")
    );
    assert_eq!(
        r(r#"::dagron-ci::annotation::{"path":"src/lib.rs","start_line":7,"end_line":9,"level":"failure","title":"t.bad","message":"left != right\nat line 7"}"#).as_deref(),
        Some("[test] src/lib.rs:7: failure: t.bad: left != right")
    );
    assert_eq!(
        r(r#"::dagron-ci::failed::{"name":"t.worse","message":""}"#).as_deref(),
        Some("[test] failed: t.worse")
    );
    assert_eq!(r("::dagron-ci::failed::not json"), None);
    // The result is for a machine; a marker this does not know, for a newer reader.
    assert_eq!(r("::dagron-ci::result::{\"protocol\":1}"), None);
    assert_eq!(r("::dagron-ci::summary::x"), None);
    assert_eq!(r("::dagron-ci::step::4"), None);
    assert_eq!(r("::dagron-ci::step::4::paused"), None);
    // Anything else is the task's own, a carriage return aside.
    assert_eq!(
        r("compiling 3 crates\r").as_deref(),
        Some("[test] compiling 3 crates")
    );
    assert_eq!(
        r("echo ::dagron-ci::step::1::end::0").as_deref(),
        Some("[test] echo ::dagron-ci::step::1::end::0")
    );
}

fn task(id: &str, name: &str, status: &str, attempt: i64, log: Option<&str>) -> TaskRow {
    TaskRow {
        id: id.into(),
        name: name.into(),
        status: status.into(),
        attempt,
        log: log.map(String::from),
        output: None,
    }
}

fn snap(tasks: Vec<TaskRow>) -> Snapshot {
    Snapshot {
        run: Some(("run".into(), "running".into())),
        tasks,
    }
}

#[test]
fn a_task_s_log_is_printed_once_a_line_at_a_time() {
    let mut p = Printer::default();
    let t0 = Instant::now();
    assert!(p
        .update(
            &snap(vec![task("1", "build", "pending", 0, None)]),
            t0,
            false
        )
        .is_empty());

    let log = "::dagron-ci::step::1::start::compile\nhalf";
    let lines = p.update(
        &snap(vec![task("1", "build", "running", 1, Some(log))]),
        t0,
        false,
    );
    assert_eq!(
        lines,
        ["[build] started", "[build] == step 1: compile"],
        "half a line waits"
    );

    let log = "::dagron-ci::step::1::start::compile\nhalf a line\n::dagron-ci::step::1::end::0\n";
    let row = task("1", "build", "running", 1, Some(log));
    let lines = p.update(&snap(vec![row.clone()]), t0, false);
    assert_eq!(lines, ["[build] half a line", "[build] == step 1 ok"]);
    assert!(
        p.update(&snap(vec![row]), t0, false).is_empty(),
        "nothing new, nothing printed"
    );

    let log = format!("{log}no newline at the end");
    let done = task("1", "build", "succeeded", 1, Some(&log));
    let lines = p.update(
        &snap(vec![done.clone()]),
        t0 + Duration::from_millis(2500),
        false,
    );
    assert_eq!(
        lines,
        ["[build] no newline at the end", "[build] succeeded in 2.5s"]
    );
    assert!(
        p.update(&snap(vec![done]), t0, true).is_empty(),
        "and never again"
    );
}

#[test]
fn a_task_that_ran_between_two_looks_or_never_ran() {
    let mut p = Printer::default();
    let now = Instant::now();
    let lines = p.update(
        &snap(vec![
            task("1", "lint", "pending", 0, None),
            task("2", "test", "pending", 0, None),
        ]),
        now,
        false,
    );
    assert!(lines.is_empty());
    let lines = p.update(
        &snap(vec![
            task(
                "1",
                "lint",
                "failed",
                1,
                Some("::dagron-ci::step::1::end::3\n"),
            ),
            task("2", "test", "skipped", 0, None),
            task("3", "deploy", "pending", 0, None),
        ]),
        now,
        false,
    );
    assert_eq!(
        lines,
        [
            "[lint] started",
            "[lint] == step 1 failed, exit 3",
            "[lint] failed",
            "[test] skipped"
        ]
    );
    let lines = p.update(
        &snap(vec![task("3", "deploy", "pending", 0, None)]),
        now,
        true,
    );
    assert_eq!(lines, ["[deploy] did not finish (pending)"]);
}

#[test]
fn a_retry_starts_its_log_over() {
    let mut p = Printer::default();
    let now = Instant::now();
    let lines = p.update(
        &snap(vec![task("1", "flaky", "running", 1, Some("first try\n"))]),
        now,
        false,
    );
    assert_eq!(lines, ["[flaky] started", "[flaky] first try"]);
    // The next attempt claimed, its log not yet begun: the old one is not
    // printed twice.
    let lines = p.update(
        &snap(vec![task("1", "flaky", "running", 2, Some("first try\n"))]),
        now,
        false,
    );
    assert_eq!(lines, ["[flaky] retrying, attempt 2"]);
    let lines = p.update(
        &snap(vec![task("1", "flaky", "running", 2, Some("second\n"))]),
        now,
        false,
    );
    assert_eq!(lines, ["[flaky] second"]);
    // Rewritten whole at the same length, as redaction does: printed again.
    let lines = p.update(
        &snap(vec![task("1", "flaky", "running", 2, Some("******\n"))]),
        now,
        false,
    );
    assert_eq!(lines, ["[flaky] ******"]);
}

#[test]
fn an_executor_that_does_not_stream_is_read_when_the_task_ends() {
    let mut p = Printer::default();
    let now = Instant::now();
    let mut row = task("1", "build", "running", 1, None);
    row.output = Some("partial".into());
    assert_eq!(
        p.update(&snap(vec![row.clone()]), now, false),
        ["[build] started"]
    );
    row.status = "failed".into();
    row.output = Some("No such image: rust:9\n".into());
    assert_eq!(
        p.update(&snap(vec![row]), now, false),
        ["[build] No such image: rust:9", "[build] failed in 0.0s"]
    );
}
