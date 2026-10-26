//! The host a runner runs its tasks on: its executor, made as the engine
//! makes its own from the same settings, and what that executor can hold a
//! task to.
//!
//! A task is made ready here as an engine makes one ready for its own
//! executor, for what depends on the host rather than the database: raised
//! to this host's isolation floor (`DAGRON_TASK_ISOLATION_FLOOR`) and refused
//! where the executor cannot hold it; given the Docker daemon it asks for
//! where this host runs one (`DAGRON_DOCKER_DAEMON`); and, where secrets stay
//! home, given its secrets from this host's own store.

use std::sync::Arc;

use anyhow::{bail, Context, Result};
use dagron_core::dag::{self, EnvVar};
use dagron_core::isolation::{Enforcer, ExecutorKind, IsolationSpec};
use dagron_core::runner::{Job, Secrets};
use dagron_executor::executor::{ExecContext, Executor, LocalExecutor, Stop, TaskIdentity};

pub struct Host {
    pub kind: ExecutorKind,
    pub executor: Arc<dyn Executor>,
    /// What this host's executor can hold a task to.
    pub enforcer: Enforcer,
    /// The isolation no task run here may go below.
    pub floor: Option<IsolationSpec>,
    /// The image of the Docker daemon a task that asks for one is given:
    /// `None` where this host runs none.
    pub docker_daemon: Option<String>,
    /// Which installation the workloads it makes are labelled as.
    pub installation: Option<String>,
    pub secrets: Secrets,
    /// dagron-api's `/oidc/token`, as a task reaches it: given to a task that
    /// asks for an identity.
    pub oidc_request_url: String,
    /// dagron-api's `/task/v1/artifacts`, as a task reaches it: given to a
    /// task that asks for its run's artifacts.
    pub artifact_api_url: String,
    /// The runner's metrics, which its Docker executor counts its image
    /// pulls in too.
    pub metrics: Arc<dagron_core::metrics::Metrics>,
}

impl Host {
    /// This host's executor of `kind`, made from the engine's settings for it.
    pub async fn from_env(
        kind: ExecutorKind,
        secrets: Secrets,
        oidc_request_url: String,
        artifact_api_url: String,
    ) -> Result<Host> {
        let env = |name: &str| std::env::var(name).unwrap_or_default();
        let metrics = Arc::new(dagron_core::metrics::Metrics::new());
        #[cfg(feature = "kubernetes")]
        let network_policy = {
            use dagron_executor::kube_executor as kube;
            kube::parse_network_policy(&env(kube::ENV_NETWORK_POLICY))
                .map_err(|e| anyhow::anyhow!("invalid {}: {e}", kube::ENV_NETWORK_POLICY))?
        };
        #[cfg(not(feature = "kubernetes"))]
        let network_policy = false;
        // The allowlists the operator has made: NetworkPolicies on
        // Kubernetes, internal networks on Docker.
        let allowlists = {
            use dagron_core::isolation::{parse_allowlists, ENV_NETWORK_ALLOWLISTS};
            parse_allowlists(&env(ENV_NETWORK_ALLOWLISTS))
                .map_err(|e| anyhow::anyhow!("invalid {ENV_NETWORK_ALLOWLISTS}: {e}"))?
        };
        let enforcer = Enforcer {
            kind,
            network_policy,
            allowlists,
        };

        let floor = match env("DAGRON_TASK_ISOLATION_FLOOR").trim() {
            "" => None,
            raw => {
                let floor = IsolationSpec::parse_floor(raw)
                    .map_err(|e| anyhow::anyhow!("invalid DAGRON_TASK_ISOLATION_FLOOR: {e}"))?;
                floor.require_enforceable_by(&enforcer).map_err(|e| {
                    anyhow::anyhow!("DAGRON_TASK_ISOLATION_FLOOR is not enforceable here: {e}")
                })?;
                Some(floor)
            }
        };

        let docker_daemon = {
            use dagron_core::docker_daemon::{offered, refusal, ENV, ENV_IMAGE};
            let image = offered(
                std::env::var(ENV).ok().as_deref(),
                std::env::var(ENV_IMAGE).ok().as_deref(),
            )?;
            if let Some(image) = &image {
                if let Some(why) = refusal(kind, Some(image)) {
                    bail!("{ENV} is set, and this runner cannot run a daemon: {why}");
                }
            }
            image
        };
        let daemon_runtime = dagron_core::docker_daemon::runtime(
            std::env::var(dagron_core::docker_daemon::ENV_RUNTIME)
                .ok()
                .as_deref(),
            docker_daemon.is_some(),
        )?;

        let executor: Arc<dyn Executor> = match kind {
            ExecutorKind::Local => {
                use dagron_executor::workspace::{parse_local_workdir, ENV_LOCAL_WORKDIR};
                let workdir = parse_local_workdir(&env(ENV_LOCAL_WORKDIR))
                    .map_err(|e| anyhow::anyhow!("invalid {ENV_LOCAL_WORKDIR}: {e}"))?;
                Arc::new(LocalExecutor { workdir })
            }
            ExecutorKind::Docker => {
                use dagron_executor::docker_executor as docker;
                use dagron_executor::workspace::{parse_image, ENV_CI_EXEC_IMAGE};
                let image =
                    std::env::var("DOCKER_IMAGE").unwrap_or_else(|_| "alpine:latest".into());
                let mounts = docker::parse_mounts(&env(docker::ENV_MOUNTS))
                    .map_err(invalid(docker::ENV_MOUNTS))?;
                let workdir = docker::parse_workdir(&env(docker::ENV_WORKDIR))
                    .map_err(invalid(docker::ENV_WORKDIR))?;
                let ci_exec_image =
                    parse_image(&env(ENV_CI_EXEC_IMAGE)).map_err(invalid(ENV_CI_EXEC_IMAGE))?;
                let checkout_image =
                    parse_image(&env(dagron_executor::checkout::ENV_CHECKOUT_IMAGE))
                        .map_err(invalid(dagron_executor::checkout::ENV_CHECKOUT_IMAGE))?;
                let log_driver = docker::parse_log_driver(&env(docker::ENV_LOG_DRIVER))
                    .map_err(invalid(docker::ENV_LOG_DRIVER))?;
                let network = docker::parse_network(&env(docker::ENV_NETWORK))
                    .map_err(invalid(docker::ENV_NETWORK))?;
                let build_actions = docker::parse_build_actions(&env(docker::ENV_BUILD_ACTIONS))
                    .map_err(invalid(docker::ENV_BUILD_ACTIONS))?;
                Arc::new(
                    docker::DockerExecutor::connect(image)
                        .await
                        .context("connecting to the Docker daemon")?
                        .with_metrics(Arc::clone(&metrics))
                        .with_log_driver(log_driver)
                        .await?
                        .with_network(network)
                        .await?
                        .with_network_allowlists(enforcer.allowlists.clone())
                        .await?
                        .with_daemon_runtime(daemon_runtime)
                        .await?
                        .with_task_mounts(mounts, workdir)
                        .with_build_actions(build_actions)
                        .with_ci_exec_image(ci_exec_image)
                        .await?
                        .with_checkout_image(checkout_image)
                        .await?,
                )
            }
            ExecutorKind::Kubernetes => {
                #[cfg(feature = "kubernetes")]
                {
                    dagron_executor::install_crypto_provider();
                    let image = std::env::var("K8S_IMAGE")
                        .or_else(|_| std::env::var("DOCKER_IMAGE"))
                        .unwrap_or_else(|_| "alpine:latest".to_string());
                    let namespace =
                        std::env::var("K8S_NAMESPACE").unwrap_or_else(|_| "default".to_string());
                    Arc::new(
                        dagron_executor::kube_executor::KubeExecutor::connect(image, namespace)
                            .await
                            .context("connecting to the Kubernetes API")?,
                    )
                }
                #[cfg(not(feature = "kubernetes"))]
                {
                    bail!("DAGRON_RUNNER_EXECUTOR=k8s needs a runner built with `--features kubernetes`")
                }
            }
        };
        let installation = std::env::var(dagron_executor::executor::DAGRON_INSTALLATION)
            .ok()
            .map(|v| v.trim().to_string())
            .filter(|v| !v.is_empty());
        Ok(Host {
            kind,
            executor,
            enforcer,
            floor,
            docker_daemon,
            installation,
            secrets,
            oidc_request_url,
            artifact_api_url,
            metrics,
        })
    }

    /// `job` as this host's executor runs it, or why it does not run here:
    /// the task is then failed with that, and nothing runs.
    pub fn prepare(&self, job: Job) -> Result<ExecContext, String> {
        prepare(self, job, &dagron_executor::secrets::lookup)
    }
}

/// What a setting that cannot be read is said as.
fn invalid<E: std::fmt::Display>(name: &'static str) -> impl Fn(E) -> anyhow::Error {
    move |e| anyhow::anyhow!("invalid {name}: {e}")
}

/// [`Host::prepare`], reading this host's secrets with `lookup`.
pub(crate) fn prepare(
    host: &Host,
    job: Job,
    lookup: &dyn Fn(&str) -> Result<String>,
) -> Result<ExecContext, String> {
    let mut env: Vec<EnvVar> = job.env;
    // Secrets stay home: each is named alone, and read from this
    // host's own store. One the task may go without is empty where the store
    // has none; any other is a task that does not run.
    if host.secrets == Secrets::Home {
        for var in env.iter_mut() {
            let Some(secret) = var.value_from.as_ref() else {
                continue;
            };
            match lookup(&secret.secret) {
                Ok(value) => var.value = value,
                Err(_) if secret.optional => var.value = String::new(),
                Err(e) => {
                    return Err(format!(
                        "secret '{}' is not in this runner's store: {e:#}",
                        secret.secret
                    ))
                }
            }
        }
    }

    // An identity: the request credential dagron-api minted for this
    // attempt, and where the task trades it for tokens, as an engine gives
    // them. The token is masked in the task's log by its name.
    if let Some(identity) = job.identity {
        for (name, value) in [
            ("DAGRON_OIDC_REQUEST_URL", host.oidc_request_url.clone()),
            ("DAGRON_OIDC_REQUEST_TOKEN", identity.token),
            ("DAGRON_OIDC_AUDIENCE", identity.audience),
        ] {
            env.retain(|v| v.name != name);
            env.push(dag::EnvVar {
                name: name.into(),
                value,
                value_from: None,
            });
        }
    }

    // Its run's artifacts: the credential dagron-api minted for this
    // attempt, and where the task sends it, as an engine gives them.
    if let Some(api) = job.artifact_api {
        for (name, value) in [
            (
                dagron_core::artifact_api::ENV_API,
                host.artifact_api_url.clone(),
            ),
            (dagron_core::artifact_api::ENV_TOKEN, api.token),
        ] {
            env.retain(|v| v.name != name);
            env.push(dag::EnvVar {
                name: name.into(),
                value,
                value_from: None,
            });
        }
    }

    // The task's isolation, raised to this host's floor, and refused where
    // its executor cannot hold it: a task never runs believing itself
    // sandboxed when it is not.
    let isolation = match (job.isolation, &host.floor) {
        (None, None) => None,
        (declared, floor) => {
            let declared = declared.unwrap_or_default();
            let effective = match floor {
                Some(floor) => declared.apply_floor(floor).0,
                None => declared,
            };
            if effective.is_empty() {
                None
            } else {
                Some(effective)
            }
        }
    };
    if let Some(iso) = &isolation {
        iso.require_enforceable_by(&host.enforcer)
            .map_err(|e| e.to_string())?;
    }

    // A Docker daemon beside the task, first among its services, where this
    // host runs one; refused where it runs none.
    let mut services = job.services;
    if job.docker_daemon {
        use dagron_core::docker_daemon::{refusal, service, ENV_HOST, HOST};
        if let Some(why) = refusal(host.kind, host.docker_daemon.as_deref()) {
            return Err(why);
        }
        let image = host
            .docker_daemon
            .as_deref()
            .unwrap_or(dagron_core::docker_daemon::DEFAULT_IMAGE);
        services.insert(0, service(image));
        if !env.iter().any(|e| e.name == ENV_HOST) {
            env.push(dag::EnvVar {
                name: ENV_HOST.into(),
                value: HOST.into(),
                value_from: None,
            });
        }
    }

    Ok(ExecContext {
        command: job.command,
        timeout_secs: job.timeout_secs,
        docker_image: job.docker_image,
        image_pull_policy: job.image_pull_policy,
        env,
        resources: job.resources,
        service_account: job.service_account,
        isolation,
        workspace: job.workspace,
        services,
        // dagron-api gives a runner no privileged task in protocol 0.
        privileged: false,
        // Wired for each attempt by the worker.
        log_sink: None,
        identity: Some(TaskIdentity {
            task_id: job.task_id,
            run_id: job.run_id,
            attempt: job.attempt,
            fence: job.fence,
            installation: host.installation.clone(),
        }),
        // Protocol 0 carries no credential for a checkout or a forge: a
        // private repository's checkout is fetched without one.
        checkout_credential: None,
        checkout_patch: None,
        action_credentials: Vec::new(),
        forge_token: None,
        // dagron-api gives a runner no task of a run that is not trusted.
        confinement: None,
        stop: Stop::default(),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use dagron_core::dag::SecretRef;

    fn host(kind: ExecutorKind, secrets: Secrets) -> Host {
        Host {
            kind,
            executor: Arc::new(LocalExecutor::default()),
            enforcer: Enforcer::from(kind),
            floor: None,
            docker_daemon: None,
            installation: Some("runner-test".into()),
            secrets,
            oidc_request_url: "https://dagron.example/oidc/token".into(),
            artifact_api_url: "https://dagron.example/task/v1/artifacts".into(),
            metrics: Arc::default(),
        }
    }

    fn job(env: Vec<EnvVar>) -> Job {
        serde_json::from_value(serde_json::json!({
            "task_id": "t1", "run_id": "r1", "name": "build", "fence": 9, "attempt": 2,
            "command": ["make"], "docker_image": "rust:1",
        }))
        .map(|mut j: Job| {
            j.env = env;
            j
        })
        .unwrap()
    }

    fn secret(name: &str, secret: &str, optional: bool) -> EnvVar {
        EnvVar {
            name: name.into(),
            value: String::new(),
            value_from: Some(SecretRef {
                secret: secret.into(),
                optional,
            }),
        }
    }

    /// Why `prepared` was refused (an `ExecContext` has no `Debug` to unwrap
    /// one with).
    fn refused(prepared: Result<ExecContext, String>) -> String {
        match prepared {
            Err(why) => why,
            Ok(_) => panic!("expected a refusal"),
        }
    }

    fn store(name: &str) -> Result<String> {
        match name {
            "deploy_key" => Ok("s3cret".into()),
            other => bail!("no {other}"),
        }
    }

    #[test]
    fn a_job_is_the_context_the_executor_runs_with_its_claim_s_identity() {
        let ctx = prepare(
            &host(ExecutorKind::Docker, Secrets::Server),
            job(vec![]),
            &store,
        )
        .unwrap();
        assert_eq!(ctx.command, ["make"]);
        assert_eq!(ctx.docker_image.as_deref(), Some("rust:1"));
        let id = ctx.identity.unwrap();
        assert_eq!((id.task_id.as_str(), id.fence, id.attempt), ("t1", 9, 2));
        assert_eq!(id.installation.as_deref(), Some("runner-test"));
        assert!(ctx.isolation.is_none() && ctx.confinement.is_none());
    }

    #[test]
    fn secrets_stay_home_and_are_read_from_this_host_s_store() {
        let home = host(ExecutorKind::Docker, Secrets::Home);
        let ctx = prepare(
            &home,
            job(vec![
                secret("KEY", "deploy_key", false),
                secret("MAYBE", "absent", true),
            ]),
            &store,
        )
        .unwrap();
        assert_eq!(ctx.env[0].value, "s3cret");
        // Still marked a secret, so that the worker masks it.
        assert!(ctx.env[0].value_from.is_some());
        assert_eq!(ctx.env[1].value, "");
        let e = refused(prepare(
            &home,
            job(vec![secret("KEY", "absent", false)]),
            &store,
        ));
        assert!(
            e.contains("'absent'") && e.contains("this runner's store"),
            "{e}"
        );
        // Under `server` dagron-api filled them in, and nothing is read here.
        let server = host(ExecutorKind::Docker, Secrets::Server);
        let mut sent = secret("KEY", "absent", false);
        sent.value = "from-api".into();
        let ctx = prepare(&server, job(vec![sent]), &store).unwrap();
        assert_eq!(ctx.env[0].value, "from-api");
    }

    /// A task that asks for an identity is given its request credential with
    /// where this runner's tasks reach dagron-api's issuer, in place of any
    /// variable of those names the task set itself.
    #[test]
    fn an_identity_is_given_as_an_engine_gives_it() {
        let mut asks = job(vec![EnvVar {
            name: "DAGRON_OIDC_REQUEST_URL".into(),
            value: "https://elsewhere.example".into(),
            value_from: None,
        }]);
        asks.identity = Some(dagron_core::runner::Identity {
            token: "dgo_abc".into(),
            audience: "sts.amazonaws.com".into(),
        });
        let ctx = prepare(&host(ExecutorKind::Docker, Secrets::Server), asks, &store).unwrap();
        let get = |name: &str| {
            let found: Vec<&EnvVar> = ctx.env.iter().filter(|v| v.name == name).collect();
            assert_eq!(found.len(), 1, "{name} once");
            found[0].value.clone()
        };
        assert_eq!(
            get("DAGRON_OIDC_REQUEST_URL"),
            "https://dagron.example/oidc/token"
        );
        assert_eq!(get("DAGRON_OIDC_REQUEST_TOKEN"), "dgo_abc");
        assert_eq!(get("DAGRON_OIDC_AUDIENCE"), "sts.amazonaws.com");
        // A task that asks for none is given none.
        let ctx = prepare(
            &host(ExecutorKind::Docker, Secrets::Server),
            job(vec![]),
            &store,
        )
        .unwrap();
        assert!(!ctx.env.iter().any(|v| v.name.starts_with("DAGRON_OIDC_")));
    }

    /// A task that asks for its run's artifacts is given the credential and
    /// where this runner's tasks reach dagron-api's door, in place of any
    /// variable of those names it set itself.
    #[test]
    fn the_artifact_api_is_given_as_an_engine_gives_it() {
        let mut asks = job(vec![EnvVar {
            name: "DAGRON_ARTIFACTS_API".into(),
            value: "https://elsewhere.example".into(),
            value_from: None,
        }]);
        asks.artifact_api = Some(dagron_core::runner::ArtifactApi {
            token: "dga_abc".into(),
        });
        let ctx = prepare(&host(ExecutorKind::Docker, Secrets::Server), asks, &store).unwrap();
        let get = |name: &str| {
            let found: Vec<&EnvVar> = ctx.env.iter().filter(|v| v.name == name).collect();
            assert_eq!(found.len(), 1, "{name} once");
            found[0].value.clone()
        };
        assert_eq!(
            get("DAGRON_ARTIFACTS_API"),
            "https://dagron.example/task/v1/artifacts"
        );
        assert_eq!(get("DAGRON_ARTIFACTS_TOKEN"), "dga_abc");
        let ctx = prepare(
            &host(ExecutorKind::Docker, Secrets::Server),
            job(vec![]),
            &store,
        )
        .unwrap();
        assert!(!ctx
            .env
            .iter()
            .any(|v| v.name.starts_with("DAGRON_ARTIFACTS_")));
    }

    #[test]
    fn isolation_this_executor_cannot_hold_is_refused_and_the_floor_is_raised_to() {
        let mut asks = job(vec![]);
        asks.isolation = Some(IsolationSpec::parse_floor("runtime_class=gvisor").unwrap());
        let e = refused(prepare(
            &host(ExecutorKind::Docker, Secrets::Server),
            asks,
            &store,
        ));
        assert!(!e.is_empty());
        let mut floored = host(ExecutorKind::Kubernetes, Secrets::Server);
        floored.floor = Some(IsolationSpec::parse_floor("read_only_root_fs=true").unwrap());
        let ctx = prepare(&floored, job(vec![]), &store).unwrap();
        assert!(
            ctx.isolation.is_some(),
            "the floor applies to a task that asked for nothing"
        );
    }

    #[test]
    fn a_docker_daemon_is_given_where_this_host_runs_one_and_refused_where_not() {
        let mut asks = job(vec![]);
        asks.docker_daemon = true;
        assert!(prepare(
            &host(ExecutorKind::Docker, Secrets::Server),
            asks.clone(),
            &store
        )
        .is_err());
        let mut runs_one = host(ExecutorKind::Docker, Secrets::Server);
        runs_one.docker_daemon = Some("docker:27-dind".into());
        let ctx = prepare(&runs_one, asks, &store).unwrap();
        assert_eq!(ctx.services.len(), 1);
        assert!(ctx.env.iter().any(|e| e.name == "DOCKER_HOST"));
    }
}
