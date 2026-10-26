//! A claimed task, as a remote runner is given it ([`dagron_core::runner::Job`]).
//!
//! What a task is given that needs the database is given here: its run's
//! identity, the variables it reads of earlier tasks (`env_from`), a retry's
//! checkpoint, its secrets, by its environment's policy, and the request
//! credential of a task that asks for an identity. What depends on
//! the host that runs it is the runner's to do as an engine does: the
//! isolation floor, and a Docker daemon beside the task.
//!
//! A task this protocol cannot carry is refused, and failed with the reason,
//! as an engine fails a task it cannot run: never run without what it asked
//! for.

use dagron_core::dag::{self, EnvVar, TaskSpec};
use dagron_core::models::TaskRun;
use dagron_core::runner::{ArtifactApi, Identity, Job, Secrets};
use dagron_core::{confine, db, envpolicy, oidc, secrets};

/// A claimed task, made ready for a runner, or why it cannot be.
pub(crate) enum Prepared {
    Job(Box<Job>),
    /// Why the task does not run on a remote runner: it is failed with this.
    Refused(String),
    /// Why it does not run yet: a token budget over it has no room while
    /// reservations are out. It is handed back, to wait.
    #[cfg(feature = "enterprise")]
    Wait(String),
}

/// What a session's claimed `task` is given, for a session whose secrets are
/// resolved as `secrets` says, by a dagron-api that is an OIDC issuer where
/// `issuer` says. An error is the database's: the claim is left to its lease.
pub(crate) async fn prepare(
    pool: &db::Pool,
    task: &TaskRun,
    secrets_mode: Secrets,
    issuer: bool,
) -> anyhow::Result<Prepared> {
    let refused = |why: String| Ok(Prepared::Refused(why));
    let Some(input) = task.input.as_deref() else {
        return refused("the task has no stored spec".into());
    };
    let spec: TaskSpec = match serde_json::from_str(input) {
        Ok(spec) => spec,
        Err(e) => return refused(format!("unparseable task spec: {e}")),
    };
    // What the run is held to is its record's, written with the run. A run
    // that is not trusted is held by an engine to a floor of its own, which a
    // runner is not given in protocol 0.
    let trigger = db::run_trigger(pool, &task.run_id).await?;
    let class = envpolicy::class_of(trigger.as_ref()).to_string();
    if confine::confined(&class) {
        return refused(format!(
            "this run is {class}, and a remote runner runs no work that is not trusted: \
             give this task a runner class an engine serves"
        ));
    }
    if let Some(why) = not_carried(&spec) {
        return refused(why);
    }

    let mut env: Vec<EnvVar> = spec.env.clone();
    // The pair a deferred submit names its remote job with, as the engine
    // gives it: the same on a resubmit after a crash.
    if spec.defer.is_some() {
        env.push(var("DAGRON_TASK_ID", &task.id));
        env.push(var(
            "DAGRON_EXTERNAL_EPOCH",
            &task.external_epoch.to_string(),
        ));
    }
    for (name, value) in [
        ("DAGRON_RUN_ID", task.run_id.as_str()),
        ("DAGRON_TASK", task.name.as_str()),
        ("DAGRON_TASK_ID", task.id.as_str()),
    ] {
        env.push(var(name, value));
    }
    // A retry is handed the last checkpoint its task reported.
    if task.attempt > 0 {
        if let Some((uri, marker)) = db::task_checkpoint(pool, &task.id).await? {
            env.push(var("DAGRON_RESUME_FROM", &uri));
            if let Some(marker) = marker {
                env.push(var("DAGRON_RESUME_MARKER", &marker));
            }
        }
    }
    if !spec.env_from.is_empty() {
        match db::given_from_tasks(pool, &task.run_id, &spec.env_from).await? {
            Ok(given) => env.extend(given),
            Err(why) => return refused(format!("env_from: {why}")),
        }
    }

    // Secrets: who may read which store is the run's environment's policy,
    // asked as the engine asks it. Under `server`, dagron-api reads the
    // environment's store and sends the values; it has no store of its own.
    // Under `home`, it sends nothing but each secret's name, for the runner
    // to resolve from its own store, which stands in for both.
    let task_environment = spec.environment.as_deref();
    let names_a_secret = env.iter().any(|e| e.value_from.is_some())
        || spec
            .defer
            .as_ref()
            .and_then(|d| d.http.as_ref())
            .is_some_and(|h| h.headers.iter().any(|e| e.value_from.is_some()));
    if names_a_secret {
        let resolved = match secrets_mode {
            Secrets::Server => {
                secrets::resolve(
                    pool,
                    &task.run_id,
                    task_environment,
                    &mut env,
                    &no_store_here,
                )
                .await
            }
            Secrets::Home => secrets::secret_source(pool, &task.run_id, task_environment)
                .await
                .map(|_| ()),
        };
        if let Err(e) = resolved {
            return refused(format!("secret resolution failed: {e:#}"));
        }
    }

    // An identity: a request credential for this attempt, minted here
    // as an engine mints it, for the runner to give the task with where to
    // trade it for tokens. A token names the task's environment, so a run
    // that may not use that environment is refused one, as it would be the
    // environment's secrets.
    let identity = match &spec.identity {
        None => None,
        Some(_) if !issuer => {
            return refused(
                "identity: this dagron-api is no OIDC issuer (DAGRON_OIDC_ISSUER is unset)".into(),
            )
        }
        Some(identity) => {
            if let Err(e) = secrets::secret_source(pool, &task.run_id, task_environment).await {
                return refused(format!(
                    "identity: its environment is not this run's to use: {e}"
                ));
            }
            let token = oidc::request_credential(
                pool,
                task,
                identity,
                spec.environment.clone(),
                spec.timeout_secs,
            )
            .await?;
            Some(Identity {
                token,
                audience: identity.first_audience().to_string(),
            })
        }
    };

    // Its run's artifacts over dagron-api's API: a credential for this
    // attempt, as an engine mints it.
    let artifact_api = if spec.artifact_api {
        Some(ArtifactApi {
            token: dagron_core::artifact_api::credential(pool, task, spec.timeout_secs).await?,
        })
    } else {
        None
    };

    // Its reservation of the token budgets over it, last, as an engine
    // makes it; what it spent is kept when its result is applied.
    #[cfg(feature = "enterprise")]
    if let Some(tokens) = spec.tokens {
        let fence = task.version.saturating_add(1);
        match dagron_core::tokens::reserve(pool, &task.id, &task.run_id, fence, tokens).await? {
            Ok(()) => {}
            Err(no) if no.for_good() => return refused(no.reason()),
            Err(no) => return Ok(Prepared::Wait(no.reason())),
        }
    }

    Ok(Prepared::Job(Box::new(Job {
        task_id: task.id.clone(),
        run_id: task.run_id.clone(),
        name: task.name.clone(),
        fence: task.version.saturating_add(1),
        attempt: task.attempt,
        command: spec.command,
        timeout_secs: spec.timeout_secs,
        docker_image: spec.docker_image,
        image_pull_policy: spec.image_pull_policy,
        env,
        resources: spec.resources,
        service_account: spec.service_account,
        isolation: spec.isolation,
        workspace: spec.workspace,
        services: spec.services,
        docker_daemon: spec.docker_daemon,
        identity,
        artifact_api,
    })))
}

/// What of a task protocol 0 does not carry to a runner, if anything.
fn not_carried(spec: &TaskSpec) -> Option<String> {
    let what = match spec.task_type.as_deref() {
        Some("workflow") => Some("a `workflow` task, which an engine starts a run for"),
        Some("wait") => Some("a `wait` task, which an engine parks"),
        _ if spec.gang_member.is_some() => {
            Some("a gang's member, which an engine claims with its gang")
        }
        _ if spec.forge_token.is_some() => {
            Some("a task with a `forge_token`, which an engine mints")
        }
        // A runner's host is not known to be a dedicated pool
        // (`dagron_core::privileged`), which an engine checks it is.
        _ if spec.privileged => Some("a privileged task, which a dedicated pool of engines runs"),
        _ => None,
    }?;
    Some(format!(
        "{what}, does not run on a remote runner (protocol 0): give it a runner class an \
         engine serves"
    ))
}

/// The host store under `server`: dagron-api has none, so a secret the task's
/// environment does not hold is not there.
fn no_store_here(name: &str) -> anyhow::Result<String> {
    anyhow::bail!(
        "dagron-api reads no store of its own for a remote runner's task: put '{name}' in the \
         run's environment, or have the runner resolve its secrets itself \
         (DAGRON_RUNNER_SECRETS=home)"
    )
}

fn var(name: &str, value: &str) -> EnvVar {
    dag::EnvVar {
        name: name.to_string(),
        value: value.to_string(),
        value_from: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn spec(json: serde_json::Value) -> TaskSpec {
        serde_json::from_value(json).unwrap()
    }

    #[test]
    fn what_an_engine_alone_can_do_is_not_carried_and_says_so() {
        let plain = spec(serde_json::json!({"name": "build", "command": ["make"]}));
        assert!(not_carried(&plain).is_none());
        let child = spec(serde_json::json!({"name": "c", "type": "workflow", "workflow": "x"}));
        let why = not_carried(&child).unwrap();
        assert!(
            why.contains("`workflow` task") && why.contains("runner class"),
            "{why}"
        );
        let wait = spec(serde_json::json!({"name": "w", "type": "wait", "wait": {"for": "5s"}}));
        assert!(not_carried(&wait).unwrap().contains("`wait` task"));
        // An identity is carried: dagron-api mints its request credential.
        let identity = spec(serde_json::json!({
            "name": "deploy", "command": ["true"],
            "identity": {"oidc": {"audience": "sts.amazonaws.com"}}
        }));
        assert!(not_carried(&identity).is_none());
        let privileged = spec(serde_json::json!({"name": "k3s", "command": ["true"], "privileged": true}));
        assert!(not_carried(&privileged).unwrap().contains("privileged task"));
    }

    #[test]
    fn a_secret_dagron_api_does_not_hold_names_where_it_can_be_put() {
        let e = no_store_here("deploy_key").unwrap_err().to_string();
        assert!(
            e.contains("'deploy_key'") && e.contains("DAGRON_RUNNER_SECRETS=home"),
            "{e}"
        );
    }
}
