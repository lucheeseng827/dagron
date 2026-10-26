//! `dagron-ci run` through the shipped binaries: a checkout's pipelines on a
//! real `dagron` engine, the one `cargo build` puts beside `dagron-ci`.
//!
//! Without that engine these tests are skipped, saying so, except under CI,
//! which builds the workspace before it tests it and so must have one. The
//! Docker test is skipped, likewise, where no Docker daemon answers.

#![cfg(unix)]

use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::time::{Duration, Instant};

const BIN: &str = env!("CARGO_BIN_EXE_dagron-ci");

/// How long a run may take before the test gives up on it.
const RUN_LIMIT: Duration = Duration::from_secs(120);

/// The engine beside dagron-ci, or `None` (and the test skipped) without one.
fn engine() -> Option<PathBuf> {
    let engine = Path::new(BIN).with_file_name("dagron");
    if engine.is_file() {
        return Some(engine);
    }
    assert!(
        std::env::var_os("CI").is_none(),
        "no dagron engine at {}: CI builds the workspace before it tests",
        engine.display()
    );
    eprintln!("no dagron engine beside dagron-ci (cargo build --bin dagron) - skipping");
    None
}

/// A git repository in a directory of its own, on `main`, with one commit.
struct Repo {
    dir: tempfile::TempDir,
}

impl Repo {
    fn new(pipelines: &[(&str, &str)]) -> Repo {
        let repo = Repo {
            dir: tempfile::tempdir().unwrap(),
        };
        repo.git(&["init", "-q", "-b", "main"]);
        repo.write("README", "hello\n");
        for (name, text) in pipelines {
            repo.write(&format!(".dagron/ci/{name}"), text);
        }
        repo.commit("first");
        repo
    }

    /// Where git says the working tree is: the path a task sees.
    fn root(&self) -> PathBuf {
        PathBuf::from(self.git(&["rev-parse", "--show-toplevel"]))
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
            .current_dir(self.dir.path())
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
        let path = self.dir.path().join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, text).unwrap();
    }

    fn commit(&self, message: &str) {
        self.git(&["add", "-A"]);
        self.git(&["commit", "-q", "-m", message]);
    }

    fn read(&self, path: &str) -> String {
        std::fs::read_to_string(self.dir.path().join(path)).unwrap_or_default()
    }
}

/// A `dagron-ci run` a test started. Dropped while it still runs, as when
/// the test fails, it is stopped as a user would stop it, with SIGTERM, which
/// stops its engine and every task too, and killed if that does not do: a
/// failed test leaves nothing running, and nothing holding its output open.
struct Started(std::process::Child);

impl Started {
    fn id(&self) -> u32 {
        self.0.id()
    }

    /// Its exit status, once it has one within `limit`.
    fn wait(&mut self, limit: Duration) -> Option<std::process::ExitStatus> {
        let started = Instant::now();
        loop {
            if let Some(status) = self.0.try_wait().unwrap() {
                return Some(status);
            }
            if started.elapsed() > limit {
                return None;
            }
            std::thread::sleep(Duration::from_millis(50));
        }
    }
}

impl Drop for Started {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(None)) {
            unsafe {
                libc::kill(self.0.id() as libc::pid_t, libc::SIGTERM);
            }
            if self.wait(Duration::from_secs(30)).is_none() {
                let _ = self.0.kill();
                let _ = self.0.wait();
            }
        }
    }
}

struct Ran {
    code: Option<i32>,
    stdout: String,
    stderr: String,
}

impl Ran {
    fn says(&self, text: &str) -> bool {
        self.stdout.contains(text)
    }

    /// Whether `lines` are all said, each after the one before, not
    /// necessarily next to it. A task's log keeps the order of what each of
    /// its pipes said, but the local executor reads stdout and stderr side by
    /// side, so lines from the two fall among each other as they arrive: an
    /// order is asserted only among lines from one of them.
    fn says_in_order(&self, lines: &[&str]) -> bool {
        let mut from = 0;
        lines
            .iter()
            .all(|line| match self.stdout[from..].find(line) {
                Some(at) => {
                    from += at + line.len();
                    true
                }
                None => false,
            })
    }
}

/// `dagron-ci run ARGS` in `repo`, with `env` on top of this environment.
fn run(repo: &Repo, args: &[&str], env: &[(&str, &str)]) -> Ran {
    let mut child = Started(
        Command::new(BIN)
            .arg("run")
            .args(args)
            .current_dir(repo.dir.path())
            .envs(env.iter().copied())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .unwrap(),
    );
    let out = std::thread::spawn({
        let stdout = child.0.stdout.take().unwrap();
        move || std::io::read_to_string(stdout).unwrap()
    });
    let err = std::thread::spawn({
        let stderr = child.0.stderr.take().unwrap();
        move || std::io::read_to_string(stderr).unwrap()
    });
    let status = child
        .wait(RUN_LIMIT)
        .unwrap_or_else(|| panic!("dagron-ci run {args:?} took over {RUN_LIMIT:?}"));
    let ran = Ran {
        code: status.code(),
        stdout: out.join().unwrap(),
        stderr: err.join().unwrap(),
    };
    eprintln!(
        "--- dagron-ci run {args:?}: {:?}\n{}{}",
        ran.code, ran.stdout, ran.stderr
    );
    ran
}

/// The directory a failed run's files were kept in, as the run says.
fn kept(ran: &Ran) -> PathBuf {
    let line = ran
        .stdout
        .lines()
        .find_map(|l| l.strip_prefix("the run's files are in "))
        .expect("the run says where its files are");
    PathBuf::from(line.split_once(':').unwrap().0)
}

/// Whether `pid` is a process that has not ended (a zombie has).
fn alive(pid: u32) -> bool {
    std::fs::read_to_string(format!("/proc/{pid}/stat"))
        .ok()
        .and_then(|stat| {
            stat.rsplit_once(") ")
                .map(|(_, rest)| !rest.starts_with('Z'))
        })
        .unwrap_or(false)
}

/// Whether any process has `needle` in its command line.
fn any_process_naming(needle: &str) -> bool {
    std::fs::read_dir("/proc").unwrap().flatten().any(|e| {
        std::fs::read(e.path().join("cmdline"))
            .map(|c| String::from_utf8_lossy(&c).contains(needle))
            .unwrap_or(false)
    })
}

const CI: &str = "name: ci
on: { push: { branches: [main] } }
tasks:
  - name: build
    steps:
      - name: env
        run: echo \"$CI_REPO $CI_REF $CI_SHA ${GITHUB_TOKEN:-scrubbed} ${DAGRON_INSTALLATION:-scrubbed} ${DAGRON_NOT_A_SETTING:-scrubbed} $KEPT\"; echo to-stderr >&2
      - run: pwd > where.txt; echo made > made.txt
  - name: test
    depends_on: [build]
    steps:
      - run: cat made.txt
";

const RELEASE: &str = "name: release
on: { push: { tags: [\"v*\"] } }
tasks:
  - name: publish
    steps: [{ run: exit 1 }]
";

#[test]
fn a_checkout_s_pipelines_run_on_the_local_executor() {
    if engine().is_none() {
        return;
    }
    let repo = Repo::new(&[("ci.yaml", CI), ("release.yaml", RELEASE)]);
    repo.git(&["remote", "add", "origin", "git@github.com:acme/widgets.git"]);
    let sha = repo.git(&["rev-parse", "HEAD"]);
    let ran = run(
        &repo,
        &["--executor", "local", "--keep"],
        &[
            ("KEPT", "from-the-shell"),
            // The engine's own settings, which would each keep this run from
            // ending (a resident API, a class no task is in, a configuration
            // that is not there) or reach past it (a forge token), are not
            // the engine's here, nor a task's.
            ("API_ADDR", "127.0.0.1:1"),
            ("RUNNER_CLASSES", "elsewhere"),
            ("DAGRON_CONFIG", "/nonexistent/dagron.yaml"),
            ("GITHUB_TOKEN", "secret-token"),
            ("DAGRON_INSTALLATION", "someone-else"),
            // dagron's namespace, whether or not the engine lists the name.
            ("DAGRON_NOT_A_SETTING", "leaked"),
        ],
    );
    assert_eq!(ran.code, Some(0), "{}", ran.stderr);
    assert!(
        ran.says(&format!(
            "dagron-ci run: push refs/heads/main at {} (github.com/acme/widgets), on local",
            &sha[..12]
        )),
        "the event is the checkout's"
    );
    assert!(ran.says(".dagron/ci/release.yaml: release does not run: on.push takes tags"));
    assert!(ran.says(&format!(
        "[build] acme/widgets refs/heads/main {sha} scrubbed scrubbed scrubbed from-the-shell\n"
    )));
    // The markers and the step's own stderr share a pipe, so keep its order.
    assert!(ran.says_in_order(&[
        "[build] started\n",
        "[build] == step 1: env\n",
        "[build] to-stderr\n",
        "[build] == step 1 ok\n",
    ]));
    assert!(ran.says("[test] made\n"));
    assert!(ran.says("\nci: succeeded in "));
    // The tasks ran in the working tree.
    assert_eq!(PathBuf::from(repo.read("where.txt").trim()), repo.root());
    // The engine never heard of another installation: its sweep stayed off.
    let dir = kept(&ran);
    let log = std::fs::read_to_string(dir.join("engine.log")).unwrap();
    assert!(
        log.contains("fleet sweep OFF") && !log.contains("someone-else"),
        "{log}"
    );
    let _ = std::fs::remove_dir_all(dir);

    // A pipeline named runs alone; without --keep, a run that succeeded
    // leaves nothing behind.
    let ran = run(&repo, &["--executor", "local", ".dagron/ci/ci.yaml"], &[]);
    assert_eq!(ran.code, Some(0), "{}", ran.stderr);
    assert!(!ran.says("release"));
    assert!(!ran.says("the run's files are in"));
}

#[test]
fn a_failing_task_fails_the_run_and_keeps_its_files() {
    if engine().is_none() {
        return;
    }
    let repo = Repo::new(&[(
        "ci.yaml",
        "name: ci
on: { push: {} }
tasks:
  - name: test
    steps:
      - run: echo before
      - name: suite
        run: exit 3
      - run: echo never
  - name: deploy
    depends_on: [test]
    steps: [{ run: echo never }]
",
    )]);
    let ran = run(&repo, &["--executor", "local"], &[]);
    assert_eq!(ran.code, Some(1));
    assert!(ran.says("[test] before\n"));
    assert!(ran.says_in_order(&[
        "[test] == step 1: echo before\n",
        "[test] == step 1 ok\n",
        "[test] == step 2: suite\n",
        "[test] == step 2 failed, exit 3\n",
    ]));
    assert!(!ran.says("never"));
    assert!(ran.says("ci: failed in ") && ran.says("(test failed)"));

    let dir = kept(&ran);
    for file in ["workflow.yaml", "run.db", "engine.log"] {
        assert!(dir.join(file).is_file(), "{file} is kept");
    }
    // The steps ran under dagron-ci-exec, which was beside dagron-ci: its
    // result is the last of the task's `::dagron-ci::` lines, which all go
    // to stderr (a stdout line may still arrive after it).
    let log = tokio::runtime::Runtime::new().unwrap().block_on(async {
        let db = sqlx::SqlitePool::connect(&format!("sqlite://{}", dir.join("run.db").display()))
            .await
            .unwrap();
        let log: String = sqlx::query_scalar("SELECT log FROM task_runs WHERE name = 'test'")
            .fetch_one(&db)
            .await
            .unwrap();
        db.close().await;
        log
    });
    assert!(
        log.lines()
            .rfind(|line| line.starts_with("::dagron-ci::"))
            .is_some_and(|line| line.starts_with("::dagron-ci::result::")),
        "{log}"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn nothing_runs_for_an_event_no_pipeline_takes() {
    if engine().is_none() {
        return;
    }
    let repo = Repo::new(&[("ci.yaml", CI)]);
    let ran = run(
        &repo,
        &["--executor", "local", "--ref", "refs/heads/dev"],
        &[],
    );
    assert_eq!(ran.code, Some(3));
    assert!(ran.says("ci does not run: branch \"dev\" is not in on.push.branches"));
    assert!(ran.says("no pipeline runs for push refs/heads/dev"));
}

/// A pipeline whose every job its `when:` leaves out of the event runs, and
/// ends at once with nothing to have run, where the run it made was never
/// ended and `dagron-ci run` waited until it was stopped.
#[test]
fn a_run_that_leaves_out_every_job_ends_at_once() {
    if engine().is_none() {
        return;
    }
    let tags_only = "name: tags-only\non:\n  push: {}\ntasks:\n  \
        - name: release\n    when: \"{{ ci_event }} == tag\"\n    steps:\n      - run: echo released\n";
    let repo = Repo::new(&[("tags-only.yaml", tags_only)]);
    let report = repo.dir.path().join("report.json");
    let started = Instant::now();
    let ran = run(
        &repo,
        &["--executor", "local", "--report", report.to_str().unwrap()],
        &[],
    );
    assert_eq!(ran.code, Some(0), "{}", ran.stderr);
    assert!(ran.says(", with no job: each one's `when:` leaves it out of this event"));
    assert!(!ran.says("released"), "the job was left out");
    assert!(
        started.elapsed() < Duration::from_secs(30),
        "it ended at once"
    );
    // The report says the job was skipped, as the commit would.
    let report: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&report).unwrap()).unwrap();
    assert_eq!(report["pipelines"][0]["result"], "succeeded", "{report:#}");
    assert_eq!(
        report["pipelines"][0]["jobs"],
        serde_json::json!({ "release": "skipped" }),
        "{report:#}"
    );

    // The event the job is for runs it.
    let ran = run(
        &repo,
        &[
            "--executor",
            "local",
            "--event",
            "tag",
            "--ref",
            "refs/tags/v1",
        ],
        &[],
    );
    assert_eq!(ran.code, Some(0), "{}", ran.stderr);
    assert!(ran.says("[release] released\n"));
}

#[test]
fn a_paths_filter_reads_what_the_working_tree_changed() {
    if engine().is_none() {
        return;
    }
    let repo = Repo::new(&[(
        "ci.yaml",
        "name: ci
on: { push: { paths: [\"src/**\"] } }
tasks:
  - name: build
    steps: [{ run: echo built }]
",
    )]);
    let origin = tempfile::tempdir().unwrap();
    let bare = origin.path().join("widgets.git");
    repo.git(&["init", "-q", "--bare", bare.to_str().unwrap()]);
    repo.git(&["remote", "add", "origin", bare.to_str().unwrap()]);
    repo.git(&["push", "-q", "-u", "origin", "main"]);

    repo.write("docs/guide.md", "words");
    let ran = run(&repo, &["--executor", "local"], &[]);
    assert_eq!(ran.code, Some(3));
    assert!(ran.says("ci does not run: no changed file matches on.push.paths"));

    repo.write("src/lib.rs", "fn main() {}");
    let ran = run(&repo, &["--executor", "local"], &[]);
    assert_eq!(ran.code, Some(0));
    assert!(ran.says("[build] built"));

    // Named files win over the working tree's.
    let ran = run(
        &repo,
        &["--executor", "local", "--changed", "docs/guide.md"],
        &[],
    );
    assert_eq!(ran.code, Some(3));

    // A branch with no upstream has nothing to compare with: the filter lets
    // the pipeline run, and the run says why.
    repo.git(&["checkout", "-q", "-b", "fresh"]);
    let ran = run(&repo, &["--executor", "local"], &[]);
    assert_eq!(ran.code, Some(0));
    assert!(ran.says("the changed files are not known") && ran.says("no upstream"));
}

#[test]
fn ctrl_c_stops_the_run_and_everything_it_started() {
    if engine().is_none() {
        return;
    }
    let repo = Repo::new(&[(
        "ci.yaml",
        "name: ci
on: { push: {} }
tasks:
  - name: slow
    steps: [{ run: echo $$ > step.pid; exec sleep 300 }]
  - name: after
    depends_on: [slow]
    steps: [{ run: echo never }]
",
    )]);
    let mut child = Started(
        Command::new(BIN)
            .args(["run", "--executor", "local"])
            .current_dir(repo.dir.path())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let mut lines = BufReader::new(child.0.stdout.take().unwrap()).lines();
    let mut seen = String::new();
    for line in lines.by_ref() {
        let line = line.unwrap();
        seen.push_str(&line);
        seen.push('\n');
        if line.starts_with("[slow] == step 1") {
            break;
        }
    }
    let started = Instant::now();
    while repo.read("step.pid").trim().is_empty() {
        assert!(
            started.elapsed() < RUN_LIMIT,
            "the step never started:\n{seen}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    let step: u32 = repo.read("step.pid").trim().parse().unwrap();
    assert!(alive(step));

    // A terminal's ctrl-c reaches this process alone: the engine has a
    // process group of its own, which dagron-ci stops.
    unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGINT);
    }
    // Well inside the 20 s grace a stopped engine has: the engine and the
    // step are asked to stop, and do.
    let status = child
        .wait(Duration::from_secs(10))
        .expect("dagron-ci did not stop");
    for line in lines {
        seen.push_str(&line.unwrap());
        seen.push('\n');
    }
    assert_eq!(status.code(), Some(130), "{seen}");
    assert!(seen.contains("stopping the run (interrupted)"), "{seen}");
    assert!(seen.contains("[after] did not finish (pending)"), "{seen}");
    assert!(seen.contains("ci: stopped (interrupted)"), "{seen}");
    assert!(!seen.contains("never"));

    let ran = Ran {
        code: status.code(),
        stdout: seen,
        stderr: String::new(),
    };
    let dir = kept(&ran);
    let started = Instant::now();
    while alive(step) || any_process_naming(dir.to_str().unwrap()) {
        assert!(
            started.elapsed() < Duration::from_secs(10),
            "the step ({step}) or the engine outlived the run"
        );
        std::thread::sleep(Duration::from_millis(50));
    }
    let _ = std::fs::remove_dir_all(dir);
}

/// Whether a Docker daemon answers; a test that needs one is skipped, saying
/// so, where none does.
fn docker_answers() -> bool {
    let up = Command::new("docker")
        .args(["version", "--format", "{{.Server.Version}}"])
        .output()
        .map(|o| o.status.success())
        .unwrap_or(false);
    if !up {
        eprintln!("no Docker daemon answers - skipping");
    }
    up
}

#[test]
fn on_docker_the_working_tree_is_mounted_where_it_is() {
    if engine().is_none() || !docker_answers() {
        return;
    }
    let repo = Repo::new(&[(
        "ci.yaml",
        "name: ci
on: { push: {} }
tasks:
  - name: build
    docker_image: alpine:3.20
    steps:
      - run: pwd; echo from-the-container > out.txt; echo \"sha $CI_SHA\"
  - name: check
    depends_on: [build]
    docker_image: alpine:3.20
    steps: [{ run: cat out.txt }]
",
    )]);
    let sha = repo.git(&["rev-parse", "HEAD"]);
    let ran = run(&repo, &[], &[]);
    assert_eq!(ran.code, Some(0), "{}", ran.stderr);
    assert!(ran.says(", on docker, with "));
    assert!(
        ran.says(&format!("[build] {}\n", repo.root().display())),
        "the task starts in the tree"
    );
    assert!(ran.says(&format!("[build] sha {sha}\n")));
    assert!(
        ran.says("[check] from-the-container\n"),
        "a task sees what an earlier one wrote"
    );
    assert_eq!(
        repo.read("out.txt"),
        "from-the-container\n",
        "and so does the checkout"
    );
}

#[test]
fn what_is_not_an_engine_is_refused() {
    let repo = Repo::new(&[("ci.yaml", CI)]);
    let ran = run(
        &repo,
        &["--executor", "local", "--engine", "/nonexistent/dagron"],
        &[],
    );
    assert_eq!(ran.code, Some(1));
    assert!(
        ran.stderr
            .contains("no dagron engine at /nonexistent/dagron"),
        "{}",
        ran.stderr
    );

    let ran = run(&repo, &["--executor", "local", "--engine", "true"], &[]);
    assert_eq!(ran.code, Some(1));
    assert!(
        ran.stderr.contains("is not a dagron engine"),
        "{}",
        ran.stderr
    );
}

#[test]
fn a_request_it_cannot_run_is_a_usage_error() {
    let repo = Repo::new(&[("ci.yaml", CI)]);
    let ran = run(&repo, &["--executor", "kubernetes"], &[]);
    assert_eq!(ran.code, Some(2));
    assert!(
        ran.stderr.contains("--executor is docker or local"),
        "{}",
        ran.stderr
    );

    repo.git(&["checkout", "-q", "--detach"]);
    let ran = run(&repo, &[], &[]);
    assert_eq!(ran.code, Some(2));
    assert!(ran.stderr.contains("--ref"), "{}", ran.stderr);

    let empty = Repo::new(&[]);
    let ran = run(&empty, &[], &[]);
    assert_eq!(ran.code, Some(2));
    assert!(
        ran.stderr.contains("no pipeline files in .dagron/ci"),
        "{}",
        ran.stderr
    );

    let ran = run(&empty, &["--help"], &[]);
    assert_eq!(ran.code, Some(0));
    assert!(
        ran.stdout.starts_with("usage: dagron-ci run [FILE]..."),
        "{}",
        ran.stdout
    );
}

#[test]
fn ctrl_c_on_docker_removes_the_run_s_containers() {
    if engine().is_none() || !docker_answers() {
        return;
    }
    let repo = Repo::new(&[(
        "ci.yaml",
        "name: ci
on: { push: {} }
tasks:
  - name: slow
    docker_image: alpine:3.20
    steps: [{ run: touch running; sleep 300 }]
",
    )]);
    let mut child = Started(
        Command::new(BIN)
            .arg("run")
            .current_dir(repo.dir.path())
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap(),
    );
    let mut lines = BufReader::new(child.0.stdout.take().unwrap()).lines();
    let mut seen = String::new();
    for line in lines.by_ref() {
        let line = line.unwrap();
        seen.push_str(&line);
        seen.push('\n');
        if line == "[slow] started" {
            break;
        }
    }
    assert!(seen.contains("[slow] started"), "{seen}");
    // Started is the engine's claim; the container comes after. The step's
    // file in the mounted tree says the container is running.
    let started = Instant::now();
    while !repo.dir.path().join("running").exists() {
        assert!(
            started.elapsed() < RUN_LIMIT,
            "the container never started:\n{seen}"
        );
        std::thread::sleep(Duration::from_millis(20));
    }
    unsafe {
        libc::kill(child.id() as libc::pid_t, libc::SIGINT);
    }
    let status = child
        .wait(Duration::from_secs(30))
        .expect("dagron-ci did not stop");
    for line in lines {
        seen.push_str(&line.unwrap());
        seen.push('\n');
    }
    assert_eq!(status.code(), Some(130), "{seen}");

    // The engine stopped mid-task, leaving the task's container running:
    // dagron-ci removes it, by the run's label.
    let dir = kept(&Ran {
        code: status.code(),
        stdout: seen,
        stderr: String::new(),
    });
    let run_id = tokio::runtime::Runtime::new().unwrap().block_on(async {
        let db = sqlx::SqlitePool::connect(&format!("sqlite://{}", dir.join("run.db").display()))
            .await
            .unwrap();
        let id: String = sqlx::query_scalar("SELECT id FROM workflow_runs")
            .fetch_one(&db)
            .await
            .unwrap();
        db.close().await;
        id
    });
    let out = Command::new("docker")
        .args([
            "ps",
            "--all",
            "--quiet",
            "--filter",
            &format!("label=dagron.dev/run-id={run_id}"),
        ])
        .output()
        .unwrap();
    assert!(out.status.success());
    assert_eq!(
        String::from_utf8_lossy(&out.stdout).trim(),
        "",
        "a container outlived the run"
    );
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn a_workflow_the_engine_refuses_says_why() {
    if engine().is_none() {
        return;
    }
    // A dagron key passes through as written; the engine is the one to judge it.
    let repo = Repo::new(&[(
        "ci.yaml",
        "name: ci
on: { push: {} }
run_timeout_secs: 0
tasks:
  - name: build
    steps: [{ run: echo never }]
  - name: publish
    when: \"{{ ci_event }} == tag\"
    steps: [{ run: echo never }]
",
    )]);
    let report = repo.dir.path().join("report.json");
    let ran = run(
        &repo,
        &["--executor", "local", "--report", report.to_str().unwrap()],
        &[],
    );
    assert_eq!(ran.code, Some(1));
    assert!(
        ran.says("ci: did not run: the engine refused the workflow: ")
            && ran.says("invalid run_timeout_secs=0"),
        "{}",
        ran.stdout
    );
    assert!(!ran.says("never"));
    // No run was made, so no `when:` was decided: no job is said skipped.
    let report: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&report).unwrap()).unwrap();
    assert_eq!(
        report["pipelines"][0]["result"], "did not run",
        "{report:#}"
    );
    assert_eq!(
        report["pipelines"][0]["jobs"],
        serde_json::json!({}),
        "{report:#}"
    );
    let _ = std::fs::remove_dir_all(kept(&ran));
}
