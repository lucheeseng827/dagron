//! Environment integration for the engine's run-creation and dispatch paths.
//!
//! A spec that declares `environment: <name>` gets two things:
//! * its variables merged into the templating scope as `{{ env.NAME }}` keys
//!   at run creation ([`template_params`]);
//! * its **secrets** resolvable at dispatch via `value_from: {secret: NAME}` —
//!   the DB store (AES-256-GCM, `DAGRON_ENV_SECRET_KEY`) is consulted first,
//!   then the classic process-env / secrets-dir tiers ([`resolve_secrets`]),
//!   so existing SOPS/External-Secrets deployments keep working unchanged.
//!
//! Both are the environment's policy's to allow (`dagron_core::envpolicy`):
//! which runs it admits, by trust class, ref and pipeline. And a task may
//! name an environment of its own for its secrets.

use std::collections::BTreeMap;

use anyhow::Result;
use serde::Deserialize;

use dagron_core::envpolicy;
use dagron_core::secrets;
pub(crate) use dagron_core::secrets::{admit, is_refusal, secret_source};

use crate::dag;
use crate::db;

/// Minimal peek at a spec YAML for its `environment:` line and its name — full
/// parsing (and its errors) stay with the caller's own `DagGraph::from_yaml*`.
#[derive(Default, Deserialize)]
struct EnvPeek {
    #[serde(default)]
    name: Option<String>,
    #[serde(default)]
    environment: Option<String>,
}

/// Marker error: the spec names an environment that does not exist. Permanent
/// (unlike a transient DB failure while resolving one) — callers that must
/// decide between "give up" and "retry next tick" downcast for this.
#[derive(Debug)]
pub(crate) struct UnknownEnvironment(pub(crate) String);

impl std::fmt::Display for UnknownEnvironment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "environment '{}' not found", self.0)
    }
}

impl std::error::Error for UnknownEnvironment {}

/// Template keys (`env.NAME` → value) for the spec's declared environment.
/// Empty map when no environment is declared; a **declared but unknown**
/// environment is a hard error — a spec pinned to `prod` must never silently
/// run without prod's variables.
///
/// The environment's policy is asked first, so that a run which may not use
/// it is not made, and its variables are put into nothing that is stored.
/// Every caller is the engine making a run of its own: a schedule's fire, a
/// backfill's, a dataset's, a call on its API. Those are the installation's
/// own (`trusted`), and for no ref.
pub(crate) async fn template_params(
    pool: &db::Pool,
    yaml: &str,
) -> Result<BTreeMap<String, String>> {
    let peek: EnvPeek = serde_yaml::from_str(yaml).unwrap_or_default();
    let mut out = BTreeMap::new();
    if let Some(name) = peek.environment {
        let facts = envpolicy::RunFacts::of(None, peek.name.as_deref().unwrap_or_default());
        admit(pool, &name, None, &facts).await?;
        let vars = db::environment_vars(pool, &name)
            .await?
            .ok_or_else(|| anyhow::Error::new(UnknownEnvironment(name.clone())))?;
        for (k, v) in vars {
            out.insert(format!("env.{k}"), v);
        }
    }
    Ok(out)
}

/// Resolve every `value_from: {secret: NAME}` env var for a task about to
/// dispatch. Tier order: the environment's secret store (decrypted with
/// `DAGRON_ENV_SECRET_KEY`), then `DAGRON_SECRET_<NAME>` / the secrets dir.
/// A secret found nowhere is a hard error — the task must not run with an
/// empty credential. `value_from` stays set on resolved vars so the output
/// redactor keeps masking their values.
///
/// Which stores are read is [`secret_source`]'s to say
/// ([`dagron_core::secrets::resolve`]), and this engine's own store is its
/// process environment and secrets directory.
pub(crate) async fn resolve_secrets(
    pool: &db::Pool,
    run_id: &str,
    task_environment: Option<&str>,
    env: &mut [dag::EnvVar],
) -> Result<()> {
    secrets::resolve(
        pool,
        run_id,
        task_environment,
        env,
        &dagron_executor::secrets::lookup,
    )
    .await
}

/// Whether a task of `run_id` may resolve secrets at all, and from its
/// environment where it names one ([`secret_source`]). For a task whose
/// secrets are resolved somewhere a refusal cannot end it: a deferred job's
/// headers, which the sweep that polls it resolves.
pub(crate) async fn admit_secrets(
    pool: &db::Pool,
    run_id: &str,
    task_environment: Option<&str>,
) -> Result<()> {
    secret_source(pool, run_id, task_environment)
        .await
        .map(|_| ())
}

/// What a run may read, on a real database: its environment's secrets by that
/// environment's policy, and the engine's own store by its class.
#[cfg(all(test, feature = "sqlite"))]
mod policy_tests {
    use super::*;
    use crate::models::{trigger_kind, RunTrigger};

    async fn pool() -> db::Pool {
        let path = std::env::temp_dir().join(format!("m54_env_policy_{}.db", uuid::Uuid::new_v4()));
        db::init_pool(path.to_str().unwrap()).await.unwrap()
    }

    /// An environment, with a policy where one is given as its three lists.
    async fn environment(
        pool: &db::Pool,
        name: &str,
        policy: Option<(&str, Option<&str>, Option<&str>)>,
    ) -> String {
        let id = uuid::Uuid::new_v4().to_string();
        sqlx::query(
            "INSERT INTO environments (id, name, variables, created_at, updated_at)
             VALUES (?, ?, '{\"REGION\":\"eu\"}', 't', 't')",
        )
        .bind(&id)
        .bind(name)
        .execute(pool)
        .await
        .unwrap();
        if let Some((trust, refs, pipelines)) = policy {
            sqlx::query(
                "INSERT INTO environment_policies
                        (environment_id, allowed_trust, allowed_refs, allowed_pipelines, updated_at)
                 VALUES (?, ?, ?, ?, 't')",
            )
            .bind(&id)
            .bind(trust)
            .bind(refs)
            .bind(pipelines)
            .execute(pool)
            .await
            .unwrap();
        }
        id
    }

    /// A CI service's record of a run: its class, and the ref it was for.
    fn ci(trust: Option<&str>, git_ref: &str) -> RunTrigger {
        RunTrigger {
            kind: trigger_kind::CI.to_string(),
            principal: "ci:svc@example.com".to_string(),
            actor: Some("octocat".to_string()),
            trust: trust.map(str::to_string),
            forge_host: Some("github.com".to_string()),
            repo: Some("acme/widgets".to_string()),
            git_ref: Some(git_ref.to_string()),
            ..Default::default()
        }
    }

    /// A run of the workflow `name`, naming `environment` where one is given.
    async fn run(
        pool: &db::Pool,
        name: &str,
        environment: Option<&str>,
        trigger: Option<&RunTrigger>,
    ) -> String {
        let environment = environment
            .map(|e| format!("environment: {e}\n"))
            .unwrap_or_default();
        let yaml =
            format!("name: {name}\n{environment}tasks:\n  - name: a\n    command: [\"true\"]\n");
        let dag = dag::DagGraph::from_yaml(&yaml).unwrap();
        match trigger {
            Some(t) => db::create_run_triggered(pool, &dag, &yaml, t)
                .await
                .unwrap(),
            None => db::create_run(pool, &dag, &yaml).await.unwrap(),
        }
    }

    async fn why_not(pool: &db::Pool, run: &str, task_environment: Option<&str>) -> String {
        let e = secret_source(pool, run, task_environment)
            .await
            .err()
            .expect("the run is refused its secrets");
        assert!(is_refusal(&e), "not a refusal by policy: {e:#}");
        format!("{e}")
    }

    /// The rule the untrusted class rests on: a run that is not `trusted`
    /// reads no environment nobody opened to it, and nothing of the engine's
    /// own store, whatever its workflow names.
    #[tokio::test]
    async fn a_run_that_is_not_trusted_reads_nothing_it_was_not_given() {
        let pool = pool().await;
        environment(&pool, "prod", None).await;
        environment(
            &pool,
            "previews",
            Some((r#"["trusted","untrusted"]"#, None, None)),
        )
        .await;
        let fork = ci(Some("untrusted"), "refs/pull/8/head");

        // No environment: the engine's own store would be the only one read.
        let bare = run(&pool, "ci", None, Some(&fork)).await;
        let why = why_not(&pool, &bare, None).await;
        assert!(
            why.contains("this run is untrusted") && why.contains("trusted runs alone"),
            "{why}"
        );

        // An environment with no policy admits trusted runs alone.
        let reaching = run(&pool, "ci", Some("prod"), Some(&fork)).await;
        let why = why_not(&pool, &reaching, None).await;
        assert_eq!(
            why,
            "environment 'prod' may not be used by this run: its policy admits trusted runs, \
             and this one is untrusted"
        );
        // Naming it on the task instead changes nothing.
        let why = why_not(&pool, &bare, Some("prod")).await;
        assert!(
            why.starts_with("environment 'prod' may not be used"),
            "{why}"
        );

        // One an admin opened to the class is read, and the engine's own
        // store still is not.
        let given = run(&pool, "ci", Some("previews"), Some(&fork)).await;
        let source = secret_source(&pool, &given, None).await.unwrap();
        assert_eq!(source.environment.as_deref(), Some("previews"));
        assert!(!source.host_store);

        // A record that names no class is not taken for trusted, and an
        // agent's run is a class of its own.
        for record in [
            ci(None, "refs/heads/main"),
            ci(Some("agent"), "refs/heads/main"),
        ] {
            let r = run(&pool, "ci", Some("prod"), Some(&record)).await;
            let why = why_not(&pool, &r, None).await;
            assert!(why.contains("admits trusted runs"), "{why}");
        }
    }

    #[tokio::test]
    async fn a_trusted_run_is_held_to_the_refs_and_pipelines_its_environment_names() {
        let pool = pool().await;
        environment(
            &pool,
            "release",
            Some((
                r#"["trusted"]"#,
                Some(r#"["refs/heads/main","refs/tags/v*"]"#),
                Some(r#"["publish"]"#),
            )),
        )
        .await;
        environment(&pool, "build", None).await;

        let main = run(
            &pool,
            "publish",
            Some("release"),
            Some(&ci(Some("trusted"), "refs/heads/main")),
        )
        .await;
        let source = secret_source(&pool, &main, None).await.unwrap();
        assert_eq!(source.environment.as_deref(), Some("release"));
        assert!(
            source.host_store,
            "a trusted run reads the engine's own store too"
        );
        let tag = run(
            &pool,
            "publish",
            Some("release"),
            Some(&ci(Some("trusted"), "refs/tags/v1.2.0")),
        )
        .await;
        assert!(secret_source(&pool, &tag, None).await.is_ok());

        // A member's pull request is trusted, and is not for main.
        let pull = run(
            &pool,
            "publish",
            Some("release"),
            Some(&ci(Some("trusted"), "refs/pull/7/head")),
        )
        .await;
        let why = why_not(&pool, &pull, None).await;
        assert!(
            why.contains("and this run is for refs/pull/7/head"),
            "{why}"
        );

        // Another pipeline at main.
        let other = run(
            &pool,
            "ci",
            Some("release"),
            Some(&ci(Some("trusted"), "refs/heads/main")),
        )
        .await;
        let why = why_not(&pool, &other, None).await;
        assert!(
            why.contains("admits the pipelines publish, and this one is ci"),
            "{why}"
        );

        // A person's own run, and one with no record at all, are trusted and
        // for no ref: a policy that names refs does not admit them.
        let manual = RunTrigger {
            kind: trigger_kind::MANUAL.to_string(),
            principal: "user:dev@example.com".to_string(),
            ..Default::default()
        };
        for record in [Some(&manual), None] {
            let r = run(&pool, "publish", Some("release"), record).await;
            let why = why_not(&pool, &r, None).await;
            assert!(why.contains("this run was made for no ref"), "{why}");
            // The same run, of an environment with no policy, is admitted.
            let r = run(&pool, "publish", Some("build"), record).await;
            assert!(secret_source(&pool, &r, None).await.unwrap().host_store);
        }

        // A task's own environment is the one asked, in place of the run's.
        let built = run(
            &pool,
            "publish",
            Some("build"),
            Some(&ci(Some("trusted"), "refs/pull/7/head")),
        )
        .await;
        assert_eq!(
            secret_source(&pool, &built, None)
                .await
                .unwrap()
                .environment
                .as_deref(),
            Some("build")
        );
        let why = why_not(&pool, &built, Some("release")).await;
        assert!(
            why.starts_with("environment 'release' may not be used"),
            "{why}"
        );
    }

    /// A stored policy that cannot be read is an error. It is never read as
    /// "no policy", which would open what someone wrote a row to close.
    #[tokio::test]
    async fn a_policy_that_does_not_read_admits_nobody() {
        let pool = pool().await;
        environment(&pool, "closed", Some(("not json", None, None))).await;
        let r = run(&pool, "ci", Some("closed"), None).await;
        let e = secret_source(&pool, &r, None)
            .await
            .err()
            .expect("no admission");
        assert!(
            !is_refusal(&e),
            "an unreadable policy is not a verdict of one"
        );
        assert!(
            format!("{e:#}").contains("its policy's allowed_trust is not a list"),
            "{e:#}"
        );
    }

    /// Through `resolve_secrets`, the function the dispatch loop calls: what a
    /// refused task holds afterwards is an empty value, and a run that is not
    /// trusted does not fall through to the engine's own store even for a
    /// secret that is there.
    #[tokio::test]
    async fn a_refused_task_holds_no_value_and_the_engine_s_own_store_is_a_trusted_run_s() {
        let pool = pool().await;
        // In the engine's own store, under a name no other test uses.
        let name = format!("policy_{}", uuid::Uuid::new_v4().simple());
        std::env::set_var(
            format!("DAGRON_SECRET_{}", name.to_uppercase()),
            "from-the-host",
        );
        std::env::set_var(dagron_crypto::KEY_ENV, "a passphrase for the policy tests");
        let key = dagron_crypto::load_key().unwrap();
        let previews = environment(
            &pool,
            "previews",
            Some((r#"["trusted","untrusted"]"#, None, None)),
        )
        .await;
        sqlx::query("INSERT INTO environment_secrets (environment_id, name, ciphertext, updated_at) VALUES (?, 'preview_token', ?, 't')")
            .bind(&previews)
            .bind(dagron_crypto::encrypt(&key, "from-the-environment").unwrap())
            .execute(&pool)
            .await
            .unwrap();
        let var = |secret: &str| dag::EnvVar {
            name: "X".to_string(),
            value: String::new(),
            value_from: Some(dag::SecretRef {
                secret: secret.to_string(),
                optional: false,
            }),
        };

        // A trusted run reads the engine's own store, with no environment.
        let own = run(&pool, "ci", None, None).await;
        let mut env = [var(&name)];
        resolve_secrets(&pool, &own, None, &mut env).await.unwrap();
        assert_eq!(env[0].value, "from-the-host");

        let fork = ci(Some("untrusted"), "refs/pull/8/head");
        // An untrusted run with no environment: refused, and nothing is put in.
        let bare = run(&pool, "ci", None, Some(&fork)).await;
        let mut env = [var(&name)];
        let e = resolve_secrets(&pool, &bare, None, &mut env)
            .await
            .unwrap_err();
        assert!(is_refusal(&e), "{e:#}");
        assert_eq!(env[0].value, "");

        // With an environment that admits it: that environment's secret is
        // read, and one it does not hold is not looked for on the host.
        let given = run(&pool, "ci", Some("previews"), Some(&fork)).await;
        let mut env = [var("preview_token")];
        resolve_secrets(&pool, &given, None, &mut env)
            .await
            .unwrap();
        assert_eq!(env[0].value, "from-the-environment");
        let mut env = [var(&name)];
        let e = resolve_secrets(&pool, &given, None, &mut env)
            .await
            .unwrap_err();
        assert!(
            format!("{e}").contains(
                "is not in environment 'previews', and a run that is untrusted reads nothing"
            ),
            "{e}"
        );
        assert_eq!(env[0].value, "");

        // A task with no secret to resolve is not asked anything: a policy
        // governs secrets, and such a task reads none.
        let mut plain = [dag::EnvVar {
            name: "A".into(),
            value: "b".into(),
            value_from: None,
        }];
        resolve_secrets(&pool, &bare, None, &mut plain)
            .await
            .unwrap();
        assert_eq!(plain[0].value, "b");
        // But a task that will resolve some later (a deferred job's headers)
        // is asked at dispatch all the same.
        assert!(is_refusal(
            &admit_secrets(&pool, &bare, None).await.unwrap_err()
        ));
        assert!(admit_secrets(&pool, &given, None).await.is_ok());
    }

    /// An optional reference (a workflow file's `secrets.NAME`) is its
    /// environment's alone: read where the environment holds it, empty where
    /// it does not, and never looked for in the engine's own store, though
    /// the run is trusted and the store holds the name. A repository's
    /// environment is that repository's runs' alone: not another's, nor one
    /// the engine made, whatever the task names.
    #[tokio::test]
    async fn an_optional_secret_is_its_environment_s_and_empty_where_it_has_none() {
        let pool = pool().await;
        let name = format!("optional_{}", uuid::Uuid::new_v4().simple());
        std::env::set_var(
            format!("DAGRON_SECRET_{}", name.to_uppercase()),
            "from-the-host",
        );
        std::env::set_var(dagron_crypto::KEY_ENV, "a passphrase for the policy tests");
        let key = dagron_crypto::load_key().unwrap();
        let repo = environment(&pool, "github.com/acme/widgets", None).await;
        sqlx::query("INSERT INTO environment_secrets (environment_id, name, ciphertext, updated_at) VALUES (?, 'NPM_TOKEN', ?, 't')")
            .bind(&repo)
            .bind(dagron_crypto::encrypt(&key, "the repository's").unwrap())
            .execute(&pool)
            .await
            .unwrap();
        let var = |secret: &str| dag::EnvVar {
            name: "X".to_string(),
            value: "left over".to_string(),
            value_from: Some(dag::SecretRef {
                secret: secret.to_string(),
                optional: true,
            }),
        };
        let own = run(
            &pool,
            "ci",
            None,
            Some(&ci(Some("trusted"), "refs/heads/main")),
        )
        .await;
        let mut env = [var("NPM_TOKEN"), var(&name)];
        resolve_secrets(&pool, &own, Some("github.com/acme/widgets"), &mut env)
            .await
            .unwrap();
        assert_eq!(env[0].value, "the repository's");
        assert_eq!(
            env[1].value, "",
            "not the engine's own, though it has the name"
        );
        // Another repository's run, the same repository on another forge,
        // and a run the engine made: none reads it.
        let other = RunTrigger {
            repo: Some("acme/other".to_string()),
            ..ci(Some("trusted"), "refs/heads/main")
        };
        let elsewhere = RunTrigger {
            forge_host: Some("git.example".to_string()),
            ..ci(Some("trusted"), "refs/heads/main")
        };
        for trigger in [Some(&other), Some(&elsewhere), None] {
            let theirs = run(&pool, "ci", None, trigger).await;
            let why = why_not(&pool, &theirs, Some("github.com/acme/widgets")).await;
            assert!(why.contains("it is a repository's"), "{why}");
        }
        // Not optional, the same miss is the engine's own store's, as before.
        let mut env = [dag::EnvVar {
            value_from: Some(dag::SecretRef {
                secret: name.clone(),
                optional: false,
            }),
            ..var(&name)
        }];
        resolve_secrets(&pool, &own, Some("github.com/acme/widgets"), &mut env)
            .await
            .unwrap();
        assert_eq!(env[0].value, "from-the-host");
    }

    /// A run the engine makes itself (a schedule's fire, a call on its API)
    /// is asked the policy before its environment's variables are put in.
    #[tokio::test]
    async fn the_engine_s_own_run_is_given_no_variables_of_an_environment_that_does_not_admit_it() {
        let pool = pool().await;
        environment(&pool, "build", None).await;
        environment(
            &pool,
            "release",
            Some((r#"["trusted"]"#, Some(r#"["refs/heads/main"]"#), None)),
        )
        .await;
        environment(
            &pool,
            "nightly",
            Some((r#"["trusted"]"#, None, Some(r#"["nightly-*"]"#))),
        )
        .await;
        let spec = |name: &str, environment: &str| {
            format!("name: {name}\nenvironment: {environment}\ntasks: []\n")
        };

        let vars = template_params(&pool, &spec("any", "build")).await.unwrap();
        assert_eq!(vars.get("env.REGION").map(String::as_str), Some("eu"));
        // No ref, so not admitted by a policy that names refs.
        let e = template_params(&pool, &spec("any", "release"))
            .await
            .unwrap_err();
        assert!(is_refusal(&e), "{e:#}");
        // By pipeline: the spec's own name.
        assert!(template_params(&pool, &spec("nightly-eu", "nightly"))
            .await
            .is_ok());
        let e = template_params(&pool, &spec("adhoc", "nightly"))
            .await
            .unwrap_err();
        assert!(format!("{e}").contains("and this one is adhoc"), "{e}");
        // An environment that is not there is still that, and not a refusal.
        let e = template_params(&pool, &spec("any", "gone"))
            .await
            .unwrap_err();
        assert!(
            e.downcast_ref::<UnknownEnvironment>().is_some() && !is_refusal(&e),
            "{e:#}"
        );
    }

    /// A protected environment puts a gate of its
    /// approvers before an agent's task that uses it, as the run is made,
    /// and before no trusted run's; and an agent's task holds an identity
    /// only where the policy says so.
    #[tokio::test]
    async fn an_agent_s_run_waits_for_a_protected_environment_and_holds_an_identity_by_policy() {
        let pool = pool().await;
        let env = environment(&pool, "prod", Some(("[\"trusted\",\"agent\"]", None, None))).await;
        sqlx::query("UPDATE environment_policies SET approvers = '[\"group:release\"]' WHERE environment_id = ?")
            .bind(&env)
            .execute(&pool)
            .await
            .unwrap();
        let mut agent = ci(Some("agent"), "refs/heads/main");
        agent.actor = Some("agent:fixer".into());
        let tasks = |run: String| {
            let pool = pool.clone();
            async move {
                sqlx::query_as::<_, (String, Option<String>)>(
                    "SELECT name, input FROM task_runs WHERE run_id = ? ORDER BY name",
                )
                .bind(run)
                .fetch_all(&pool)
                .await
                .unwrap()
            }
        };
        let gated = tasks(run(&pool, "ci", Some("prod"), Some(&agent)).await).await;
        let names: Vec<&str> = gated.iter().map(|(n, _)| n.as_str()).collect();
        assert_eq!(names, ["a", "a.environment"]);
        let gate: dag::TaskSpec = serde_json::from_str(gated[1].1.as_deref().unwrap()).unwrap();
        assert!(gate.is_approval());
        assert_eq!(gate.approvers, ["group:release"]);
        let task: dag::TaskSpec = serde_json::from_str(gated[0].1.as_deref().unwrap()).unwrap();
        assert_eq!(task.depends_on, ["a.environment"]);
        // A trusted run of the environment is not asked.
        let trusted = tasks(
            run(
                &pool,
                "ci",
                Some("prod"),
                Some(&ci(Some("trusted"), "refs/heads/main")),
            )
            .await,
        )
        .await;
        assert_eq!(trusted.len(), 1);

        // An identity, by the policy.
        let refused = dagron_core::secrets::admit_identity(&pool, Some("prod"), "agent")
            .await
            .unwrap_err();
        assert!(
            format!("{refused:#}").contains("allowed_identity"),
            "{refused:#}"
        );
        sqlx::query("UPDATE environment_policies SET allowed_identity = '[\"agent\"]' WHERE environment_id = ?")
            .bind(&env)
            .execute(&pool)
            .await
            .unwrap();
        dagron_core::secrets::admit_identity(&pool, Some("prod"), "agent")
            .await
            .unwrap();
        assert!(
            dagron_core::secrets::admit_identity(&pool, Some("prod"), "untrusted")
                .await
                .is_err()
        );
        dagron_core::secrets::admit_identity(&pool, None, "trusted")
            .await
            .unwrap();
    }
}
