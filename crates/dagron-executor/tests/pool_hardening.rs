//! What a hardened local pool actually refuses, against real subprocesses.
//!
//! `pool.rs` unit-tests the policy for every input. What it cannot reach is the
//! part that decides whether the feature exists at all: that the environment
//! variables are spelled the way an operator will spell them, that the
//! `OnceLock` reads them, that **both** spawn paths honour them — a rule that
//! held on the buffered path and not the streaming one would be a hole shaped
//! like "whether a live-log sink happened to be wired", and the streaming path
//! is the one a real task takes — and that a cleared variable is actually gone
//! from the child rather than merely absent from a struct.
//!
//! Its own test binary on purpose: the policy is cached process-wide on first
//! use, so one process can hold exactly one policy. This file holds the
//! hardened one; the unhardened behaviour is what every other test in the crate
//! already runs under.
//!
//! The environment is the build pool's, from `ee/compose.image-build.yaml`:
//! a datastore DSN, the key that decrypts stored task secrets, an image-signing
//! key, and a daemon socket. Each of those was read by a task before this.

use std::sync::OnceLock;

use dagron_core::dag::EnvVar;
use dagron_executor::executor::{run_command, ExecContext, Executor, LocalExecutor, LogChunk, LogSink};
use dagron_executor::redact::Redactor;

fn ev(name: &str, value: &str) -> EnvVar {
    EnvVar { name: name.into(), value: value.into(), value_from: None }
}

/// The permitted program: a real file, since the allowlist matches files.
/// Stands in for `dagron-build` — it prints its own environment, which is what
/// the isolation assertions read.
struct Pool {
    allowed: std::path::PathBuf,
}

fn pool() -> &'static Pool {
    static P: OnceLock<Pool> = OnceLock::new();
    P.get_or_init(|| {
        let dir = std::env::temp_dir().join(format!("dagron-pool-e2e-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let allowed = dir.join("dagron-build");
        std::fs::write(&allowed, "#!/bin/sh\nenv\n").unwrap();
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            std::fs::set_permissions(&allowed, std::fs::Permissions::from_mode(0o755)).unwrap();
        }

        // The pool's own environment.
        std::env::set_var("DATABASE_URL", "postgres://dagron:dagron@postgres:5432/workflow");
        std::env::set_var("DAGRON_ENV_SECRET_KEY", "dev-insecure-env-secret-key-change-me");
        std::env::set_var("DAGRON_BUILD_ATTEST_KEY", "1".repeat(64));
        std::env::set_var("DOCKER_HOST", "unix:///run/podman/podman.sock");
        std::env::set_var("DAGRON_BUILD_TIMEOUT_SECS", "840");

        // The hardening, set before anything reads the policy.
        std::env::set_var("DAGRON_LOCAL_COMMAND_ALLOWLIST", allowed.display().to_string());
        std::env::set_var(
            "DAGRON_LOCAL_ENV_PASSTHROUGH",
            "DOCKER_HOST,DAGRON_BUILD_ATTEST_KEY,DAGRON_BUILD_TIMEOUT_SECS",
        );
        Pool { allowed }
    })
}

/// Run through the path a real task takes. The engine always wires a log sink
/// (`log_tx: Some(...)`), so production is the streaming arm of
/// `LocalExecutor::execute` — testing only `run_command` would test the arm no
/// task uses.
async fn streamed(command: &[&str], env: &[EnvVar]) -> anyhow::Result<String> {
    let (tx, _rx) = tokio::sync::mpsc::unbounded_channel::<LogChunk>();
    let sink = LogSink::new(tx, "t1".into(), 1, Redactor::from_task_env(env));
    let mut ctx = ExecContext::new(
        command.iter().map(|s| s.to_string()).collect(),
        Some(20),
        None,
    );
    ctx.env = env.to_vec();
    ctx.log_sink = Some(sink);
    LocalExecutor.execute(&ctx).await.map(|o| o.output)
}

/// The escalation that motivated all of this: `sh -c` on the build pool. It ran
/// as the pool's user with the pool's environment; now it does not run.
#[tokio::test]
async fn an_arbitrary_command_is_refused_on_both_spawn_paths() {
    pool();
    for command in [vec!["sh", "-c", "env"], vec!["id"], vec!["/bin/sh"]] {
        let err = streamed(&command, &[])
            .await
            .expect_err("streaming path must refuse {command:?}");
        assert!(
            err.to_string().contains("DAGRON_LOCAL_COMMAND_ALLOWLIST"),
            "the refusal must name the knob that caused it: {err}"
        );

        let owned: Vec<String> = command.iter().map(|s| s.to_string()).collect();
        let err = run_command(&owned, Some(20), &[])
            .await
            .expect_err("buffered path must refuse it too");
        assert!(err.to_string().contains("DAGRON_LOCAL_COMMAND_ALLOWLIST"), "{err}");
    }
}

/// The permitted program still runs, and the refusal is not a blanket one.
#[tokio::test]
async fn the_permitted_program_still_runs() {
    let p = pool();
    let out = streamed(&[&p.allowed.display().to_string()], &[])
        .await
        .expect("the allowlisted program must run");
    assert!(out.contains("PATH="), "it ran and printed its environment:\n{out}");
}

/// A decoy of the same name reached through `PATH` — the bypass that defeats a
/// comparison on `command[0]`. Note this also proves the child does no lookup
/// of its own: the task cannot set `PATH` at all (below), and the spawn is by
/// canonical path.
#[tokio::test]
async fn a_planted_program_of_the_permitted_name_is_refused() {
    let p = pool();
    let evil = p.allowed.parent().unwrap().join("evil");
    std::fs::create_dir_all(&evil).unwrap();
    let decoy = evil.join("dagron-build");
    std::fs::write(&decoy, "#!/bin/sh\necho decoy\n").unwrap();
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(&decoy, std::fs::Permissions::from_mode(0o755)).unwrap();
    }
    let err = streamed(&[&decoy.display().to_string()], &[])
        .await
        .expect_err("a different file of the same name is a different program");
    assert!(err.to_string().contains("refused"), "{err}");
}

/// The pool's secrets stop at the boundary. Measured by reading the child's own
/// environment, not by inspecting a struct: `env_clear` either happened in the
/// spawned process or it did not.
#[tokio::test]
async fn the_pools_own_secrets_do_not_reach_a_task() {
    let p = pool();
    let out = streamed(&[&p.allowed.display().to_string()], &[ev("MY_OWN", "fine")])
        .await
        .expect("the allowlisted program runs");

    // Gone. The DSN is a write to any run's state; the env secret key decrypts
    // every stored task secret in the datastore.
    for gone in ["DATABASE_URL", "DAGRON_ENV_SECRET_KEY"] {
        assert!(
            !out.contains(&format!("{gone}=")),
            "{gone} reached the task:\n{out}"
        );
    }
    // Present, because the operator named them: the builder needs its daemon
    // and its signing key.
    for kept in ["DOCKER_HOST=unix:///run/podman/podman.sock", "DAGRON_BUILD_TIMEOUT_SECS=840"] {
        assert!(out.contains(kept), "{kept} should have come through:\n{out}");
    }
    assert!(out.contains("DAGRON_BUILD_ATTEST_KEY="), "the signing key is named:\n{out}");
    // The task's own env still arrives, and the baseline is there so the
    // program can find anything at all.
    assert!(out.contains("MY_OWN=fine"), "{out}");
    assert!(out.contains("PATH="), "{out}");
}

/// A task may not replace what the pool owns. Refused rather than silently
/// losing: a task that ran under a configuration its author did not write is
/// worse than a task that did not run.
#[tokio::test]
async fn a_task_cannot_replace_a_pool_owned_variable() {
    let p = pool();
    let allowed = p.allowed.display().to_string();
    for name in ["DAGRON_BUILD_ATTEST_KEY", "DOCKER_HOST", "PATH"] {
        match streamed(&[&allowed], &[ev(name, "mine")]).await {
            Ok(out) => panic!("setting {name} was not refused; the task ran:\n{out}"),
            Err(e) => assert!(
                e.to_string().contains(name) && e.to_string().contains("may not replace"),
                "the refusal must name the variable: {e}"
            ),
        }
    }
    // And a name the operator did not claim is still the task's to set.
    let out = streamed(&[&allowed], &[ev("DAGRON_BUILD_RECIPE", "name: x")])
        .await
        .expect("an unclaimed name is the task's own");
    assert!(out.contains("DAGRON_BUILD_RECIPE=name: x"), "{out}");
}

/// The failure a refused task records is classified as a configuration fault,
/// so it fails once instead of burning its retry budget on an error that is
/// deterministic. The classifier reads the message the executor produced —
/// this pins the two together, since they live in different crates.
#[tokio::test]
async fn a_refusal_reads_as_a_config_fault_to_the_retry_path() {
    pool();
    let err = run_command(&["sh".into()], Some(20), &[]).await.unwrap_err();
    let class = dagron_core::fault::classify_text(&err.to_string())
        .unwrap_or_else(|| panic!("the refusal must classify: {err}"))
        .class;
    assert_eq!(class, dagron_core::fault::FaultClass::Config);
    assert!(
        !dagron_core::models::should_retry_failed_with_class(1, 5, false, true, Some(class), None),
        "a refused task must not be retried"
    );
}
