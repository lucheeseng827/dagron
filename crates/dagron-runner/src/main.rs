//! `dagron-runner [run | register | scale]`. See the crate's README.

use anyhow::{bail, Context, Result};
use dagron_runner::client::Client;
use dagron_runner::config::Config;
use dagron_runner::host::Host;
use dagron_runner::runner::{self, Runner};

const USAGE: &str = "\
usage: dagron-runner [run | register | scale | --version]

  run        take and run tasks (the default): DAGRON_RUNNER_URL, and a
             credential (DAGRON_RUNNER_TOKEN_FILE) or a join token for the
             first start (DAGRON_RUNNER_JOIN_TOKEN_FILE)
  register   exchange the join token for a credential, keep it, and exit
  scale      size runner Deployments by the work there is for their labels
             (DAGRON_SCALE_TARGETS; built with --features kubernetes)
";

#[tokio::main]
async fn main() {
    if dagron_logging::version_requested("dagron-runner") {
        return;
    }
    dagron_logging::init("runner");
    let command = std::env::args().nth(1).unwrap_or_else(|| "run".to_string());
    let outcome = match command.as_str() {
        "run" => run().await,
        "register" => register().await,
        "scale" => scale().await,
        "help" | "-h" | "--help" => {
            print!("{USAGE}");
            Ok(())
        }
        other => {
            eprint!("dagron-runner: unknown command '{other}'\n\n{USAGE}");
            std::process::exit(2);
        }
    };
    dagron_logging::shutdown();
    if let Err(e) = outcome {
        eprintln!("dagron-runner: {e:#}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
    let cfg = Config::from_env()?;
    let client = Client::new(&cfg.url)?;
    let token = runner::credential(&cfg, &client).await?;
    let host = Host::from_env(
        cfg.executor,
        cfg.secrets,
        cfg.oidc_request_url.clone(),
        cfg.artifact_api_url.clone(),
    )
    .await?;
    tracing::info!(
        url = %cfg.url, executor = ?cfg.executor, capacity = cfg.capacity,
        labels = ?cfg.labels, secrets = cfg.secrets.as_str(),
        version = dagron_logging::VERSION, "dagron-runner starting"
    );
    let runner = Runner::new(cfg, client.with_token(token), host).await?;
    runner.run(runner::shutdown_on_signals()).await
}

async fn register() -> Result<()> {
    let cfg = Config::from_env()?;
    let client = Client::new(&cfg.url)?;
    let Some(join_file) = &cfg.join_token_file else {
        bail!(
            "{} is not set: the join token to register with",
            dagron_runner::config::ENV_JOIN_TOKEN_FILE
        );
    };
    let join_token = tokio::fs::read_to_string(join_file)
        .await
        .with_context(|| format!("reading the join token at {}", join_file.display()))?;
    let registered = runner::register(&cfg, &client, join_token.trim()).await?;
    runner::store_credential(&cfg.token_file, &registered.token).await?;
    println!(
        "registered runner:{} for {} — its credential is in {}",
        registered.runner_id,
        registered.labels.join(","),
        cfg.token_file.display()
    );
    Ok(())
}

async fn scale() -> Result<()> {
    let settings = dagron_runner::scale::Settings::from_env()?;
    let url = std::env::var(dagron_runner::config::ENV_URL)
        .with_context(|| format!("{} is not set", dagron_runner::config::ENV_URL))?;
    let url = dagron_runner::config::check_url(
        &url,
        std::env::var(dagron_runner::config::ENV_INSECURE)
            .ok()
            .as_deref()
            == Some("allow"),
    )?;
    let token_file = std::env::var(dagron_runner::config::ENV_TOKEN_FILE).with_context(|| {
        format!(
            "{} is not set: a runner credential that serves the labels",
            dagron_runner::config::ENV_TOKEN_FILE
        )
    })?;
    let token = tokio::fs::read_to_string(&token_file)
        .await
        .with_context(|| format!("reading the credential at {token_file}"))?;
    let client = Client::new(&url)?.with_token(token.trim());
    #[cfg(feature = "kubernetes")]
    {
        dagron_executor::install_crypto_provider();
        let kube = kube::Client::try_default()
            .await
            .context("connecting to the Kubernetes API")?;
        let namespace = std::env::var("K8S_NAMESPACE").unwrap_or_else(|_| "default".to_string());
        let targets = settings
            .targets
            .iter()
            .map(|(label, name)| {
                dagron_runner::scale::Target::new(
                    label.clone(),
                    Box::new(dagron_runner::scale::Deployment::new(
                        kube.clone(),
                        name,
                        &namespace,
                    )),
                    settings.down_after,
                )
            })
            .collect();
        tracing::info!(targets = ?settings.targets, policy = ?settings.policy, "scaling runners");
        dagron_runner::scale::control_loop(
            client,
            targets,
            settings.policy,
            settings.every,
            runner::shutdown_on_signals(),
        )
        .await
    }
    #[cfg(not(feature = "kubernetes"))]
    {
        let _ = (client, settings);
        bail!("`dagron-runner scale` sizes Kubernetes Deployments: build it with `--features kubernetes`")
    }
}
