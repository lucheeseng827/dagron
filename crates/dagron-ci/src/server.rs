//! The HTTP surface (probes, metrics, the webhook receivers), and the event
//! workers and poll sweepers that run beside it.
//!
//! `/healthz`, `/readyz` and `/metrics` answer without authentication, as an
//! orchestrator expects. The receivers authenticate each request by its
//! signature. The run API (`api.rs`) has dagron-api identify each caller. The
//! checkout credentials ([`checkout`]) are for engines holding their secret.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Bytes;
use axum::extract::{DefaultBodyLimit, State};
use axum::http::{header, HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use serde_json::json;

use tokio::sync::{watch, Notify};

use crate::admission::Admission;
use crate::api;
use crate::checkout::{self, Checkouts};
use crate::config::Config;
use crate::db::{Db, Recorded};
use crate::dispatch::Dispatcher;
use crate::forge::Forge;
use crate::github::{self, Parsed};
use crate::gitlab;
use crate::metrics::{self, Metrics, MetricsSource, Outcome};
use crate::poll::{self, Poller};
use crate::problem::problem;
use crate::report::{self, Reporter};
use crate::rerun::{RerunFrom, Reruns};
use crate::worker;
use crate::Seams;

/// GitHub caps a webhook payload at 25 MB; a body over that is not GitHub's.
pub const MAX_WEBHOOK_BYTES: usize = 25 * 1024 * 1024;

/// How long `/readyz` and `/metrics` wait on the database before they answer
/// without it, so a slow pool never holds a probe past the kubelet's patience.
const PROBE_BUDGET: Duration = Duration::from_secs(1);

/// How long dagron-api may take to say who a run API caller is.
const IDENTIFY_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct AppState {
    pub(crate) db: Db,
    github_secret: Option<Arc<[u8]>>,
    /// CI_GITLAB_WEBHOOK_SECRET_FILE's token, or `None`: POST /hooks/gitlab
    /// answers 404.
    gitlab_secret: Option<Arc<[u8]>>,
    pub(crate) metrics: Arc<Metrics>,
    extra_metrics: Arc<[Arc<dyn MetricsSource>]>,
    /// Rung once per queued event, by a receiver, the run API or a poll
    /// sweep, so an idle worker starts on it at once.
    pub(crate) wake: Arc<Notify>,
    /// Each cell's dagron-api, which identifies the run API's callers.
    pub(crate) cells: Arc<BTreeMap<String, String>>,
    /// Reads forges: the poll sweep's refs and pull requests, and the commit
    /// a dispatched ref points at.
    pub(crate) forge: Arc<Forge>,
    /// Asks dagron-api who a run API caller is.
    pub(crate) http: crate::config::Clients,
    /// What a followed log is read with: no bound on the whole
    /// of it, which lasts as long as its run, and one on a silence, which a
    /// stream's keep-alives never let last.
    pub(crate) follow_http: crate::config::Clients,
    /// Set once dagron-ci is to stop: a followed log ends then, and its
    /// reader goes on from its cursor with whichever server answers next.
    pub(crate) stop: watch::Receiver<bool>,
    /// Mints each checkout's credential; `None` when neither
    /// CI_CHECKOUT_TOKEN_SECRET_FILE nor CI_CHECKOUT_TOKEN_SECRETS_DIR is set.
    pub(crate) checkouts: Option<Arc<Checkouts>>,
    /// Asks a cell's dagron-api whether a run is running again, and for a
    /// rerun. `None` without a cell and dagron-ci's own token for it
    /// (CI_CELLS, CI_API_TOKEN_FILE): a record then never re-opens.
    pub(crate) reruns: Option<Arc<Reruns>>,
    /// The build's limit on runs in flight, which a rerun asks like a new run.
    pub(crate) admission: Option<Arc<dyn Admission>>,
    /// Where the build keeps the acts dagron-ci records (`Seams::audit`).
    pub(crate) audit: Option<Arc<dyn crate::Audit>>,
    /// The App whose check runs dagron-ci posts, where it posts them
    /// (CI_GITHUB_CHECKS=on): a "Re-run" of one of them is taken.
    checks_app: Option<u64>,
    /// The labels of GitHub Actions' own jobs this dagron-ci serves
    /// (CI_GITHUB_FLEET_FILE). `None`: a job's delivery is dropped.
    fleet: Option<Arc<crate::fleet::Fleet>>,
    /// The same, of GitLab CI's own jobs, by tag (CI_GITLAB_FLEET_FILE).
    gitlab_fleet: Option<Arc<crate::fleet::Fleet>>,
    /// Whether an agent's runs are run (CI_AGENT_RUNS=untrusted): an agent
    /// then reruns and cancels its own, and is refused both otherwise.
    pub(crate) agent_runs: bool,
    /// The GitHub API door jobs reach (CI_GITHUB_API_FOR_JOBS), where it
    /// is served.
    pub(crate) door: Option<Arc<crate::github_door::Door>>,
    /// Where a payload too large for its event's row is kept
    /// (CI_OBJECT_STORE_URL). `None`: such a payload is not kept.
    pub(crate) payloads: Option<crate::payloads::Payloads>,
    /// Signs the links a patched checkout fetches its bundle by, at the URL
    /// jobs reach dagron-ci at (CI_URL_FOR_JOBS). `None`: no bundle is taken.
    pub(crate) bundle_links: Option<Arc<crate::bundles::Links>>,
    /// How many bundles are sent to checkouts at once
    /// ([`crate::bundles::SENT_AT_ONCE`]): each holds its compressed bundle
    /// in memory while it is sent.
    pub(crate) bundle_sends: Arc<tokio::sync::Semaphore>,
}

/// Why a delivery was not recorded.
enum NotKept {
    /// The event queue's database.
    Db(sqlx::Error),
    /// CI_OBJECT_STORE_URL, which a payload over the row's limit goes to
    /// before its event is recorded.
    Store(anyhow::Error),
}

impl AppState {
    /// Record a webhook's delivery ([`Db::record_delivery`]): its payload in
    /// its event's row where it fits, else in CI_OBJECT_STORE_URL first,
    /// where there is one, and its event naming it. A payload kept for an
    /// event that was not recorded (a redelivery, a write that failed) is
    /// deleted again.
    async fn record_delivery(
        &self,
        repo: &crate::db::Repo,
        delivery_id: &str,
        event: &crate::event::NewEvent,
        body: &[u8],
    ) -> Result<Recorded, NotKept> {
        let Some(payloads) = self
            .payloads
            .as_ref()
            .filter(|_| body.len() > crate::db::PAYLOAD_INLINE_MAX)
        else {
            return self
                .db
                .record_delivery(repo, delivery_id, event, body)
                .await
                .map_err(NotKept::Db);
        };
        let name = payloads.put(body).await.map_err(NotKept::Store)?;
        let recorded = self
            .db
            .record_delivery_stored(repo, delivery_id, event, &name)
            .await;
        if !matches!(recorded, Ok(Recorded::Event(_))) {
            if let Err(e) = payloads.delete(&name).await {
                tracing::warn!(payload = %name, error = %format!("{e:#}"), "a payload kept for an event that was not recorded was not deleted");
            }
        }
        recorded.map_err(NotKept::Db)
    }
}

impl AppState {
    pub fn new(db: Db, config: &Config, seams: Seams) -> AppState {
        let metrics = Arc::new(Metrics::default());
        AppState {
            db,
            github_secret: config.github_webhook_secret.as_deref().map(Arc::from),
            gitlab_secret: config.gitlab_webhook_secret.as_deref().map(Arc::from),
            checks_app: config
                .github_app
                .as_ref()
                .filter(|_| config.github_checks)
                .map(|app| app.id()),
            fleet: config.fleet.clone().map(Arc::new),
            gitlab_fleet: config.gitlab_fleet.clone().map(Arc::new),
            agent_runs: config.agent_runs,
            payloads: None,
            bundle_links: None,
            bundle_sends: Arc::new(tokio::sync::Semaphore::new(crate::bundles::SENT_AT_ONCE)),
            door: config.github_api_for_jobs.as_ref().and_then(|public| {
                // Its links are signed with what no job holds: the engines'
                // secret for the token service, which the door needs anyway,
                // since the tokens it reads are that service's.
                let mut secret = Vec::new();
                if let Some(s) = &config.checkout_token_secret {
                    secret.extend_from_slice(s.as_bytes());
                }
                for (cell, s) in &config.checkout_cell_secrets {
                    secret.extend_from_slice(cell.as_bytes());
                    secret.extend_from_slice(s.as_bytes());
                }
                (!secret.is_empty()).then(|| {
                    Arc::new(crate::github_door::Door::new(
                        public.clone(),
                        config
                            .github_api
                            .clone()
                            .unwrap_or_else(|| "https://api.github.com".to_string()),
                        config.api_token.clone(),
                        &secret,
                    ))
                })
            }),
            reruns: match &config.api_token {
                Some(token) if !config.cells.is_empty() => Some(Arc::new(Reruns::new(
                    config.cells.clone(),
                    token.clone(),
                    metrics.clone(),
                ))),
                _ => None,
            },
            admission: seams.admission.clone(),
            audit: seams.audit.clone(),
            metrics,
            extra_metrics: seams.metrics.into(),
            wake: Arc::new(Notify::new()),
            cells: Arc::new(config.cells.clone()),
            forge: Arc::new(
                Forge::new(config.github_token.clone())
                    .with_github_api(config.github_api.clone())
                    .with_gitlab_tokens(&config.gitlab_tokens)
                    .with_app(config.github_app.clone())
                    .with_checks(config.github_checks)
                    .with_pr_comment(config.github_pr_comment)
                    .with_mr_notes(config.gitlab_mr_notes)
                    .with_event_lease(config.event_lease)
                    .with_flake_issues(config.flake_issues.is_some()),
            ),
            http: crate::config::Clients::new(|| {
                reqwest::Client::builder()
                    .timeout(IDENTIFY_TIMEOUT)
                    .user_agent(concat!("dagron-ci/", env!("CARGO_PKG_VERSION")))
            }),
            follow_http: crate::config::Clients::new(|| {
                reqwest::Client::builder()
                    .connect_timeout(Duration::from_secs(15))
                    .read_timeout(Duration::from_secs(120))
                    .user_agent(concat!("dagron-ci/", env!("CARGO_PKG_VERSION")))
            }),
            stop: watch::channel(false).1,
            checkouts: (config.checkout_token_secret.is_some()
                || !config.checkout_cell_secrets.is_empty())
            .then(|| {
                Arc::new(
                    Checkouts::new(
                        config.checkout_token_secret.clone(),
                        config.github_app.clone(),
                        config.github_api.clone(),
                    )
                    .with_cell_secrets(&config.checkout_cell_secrets)
                    .with_gitlab_tokens(&config.gitlab_tokens),
                )
            }),
        }
    }

    /// Post check runs as the App `app`, as CI_GITHUB_CHECKS=on does with
    /// the App configured: a test has no key to configure one with.
    #[cfg(test)]
    pub(crate) fn with_checks_app(mut self, app: Option<u64>) -> AppState {
        self.checks_app = app;
        self
    }

    /// Serve the jobs `fleet` names, as CI_GITHUB_FLEET_FILE does with an
    /// App configured: a test has no key to configure one with.
    #[cfg(test)]
    pub(crate) fn with_fleet(mut self, fleet: crate::fleet::Fleet) -> AppState {
        self.fleet = Some(Arc::new(fleet));
        self
    }

    /// Serve the GitLab jobs `fleet` names, as CI_GITLAB_FLEET_FILE does.
    #[cfg(test)]
    pub(crate) fn with_gitlab_fleet(mut self, fleet: crate::fleet::Fleet) -> AppState {
        self.gitlab_fleet = Some(Arc::new(fleet));
        self
    }

    /// Read every forge at `origin` instead: a test's stub.
    #[cfg(test)]
    pub(crate) fn with_forge_origin(mut self, origin: &str) -> AppState {
        self.forge = Arc::new(Forge::new(None).with_origin(origin));
        self
    }
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/metrics", get(metrics_text))
        .route(
            "/hooks/github",
            post(github_hook).layer(DefaultBodyLimit::max(MAX_WEBHOOK_BYTES)),
        )
        .route(
            "/hooks/gitlab",
            post(gitlab_hook).layer(DefaultBodyLimit::max(MAX_WEBHOOK_BYTES)),
        )
        .route(
            "/ci/v1/runs",
            post(api::create_run).layer(DefaultBodyLimit::max(api::MAX_BODY)),
        )
        .route(
            "/ci/v1/bundles",
            post(crate::bundles::upload).layer(DefaultBodyLimit::max(crate::bundles::MAX_INLINE)),
        )
        .route(crate::bundles::CONTENT_PATH, get(crate::bundles::content))
        .route("/ci/v1/me", get(api::me))
        .route("/ci/v1/runs/{id}", get(api::get_run))
        .route("/ci/v1/runs/{id}/rerun", post(api::rerun_run))
        .route("/ci/v1/runs/{id}/cancel", post(api::cancel_run))
        .route("/ci/v1/runs/{id}/logs", get(api::run_logs))
        .route("/ci/v1/runs/{id}/statuses", get(api::run_statuses))
        .route("/ci/v1/repos", get(api::repos::list))
        .route("/ci/v1/fleet/usage", get(api::fleet::usage))
        .route(
            "/ci/v1/repos/{*repo}",
            get(api::repos::get)
                .put(api::repos::put)
                .delete(api::repos::delete)
                .layer(DefaultBodyLimit::max(api::repos::MAX_BODY)),
        )
        .route(
            &format!("{}/{{*rest}}", crate::github_door::PATH),
            axum::routing::any(crate::github_door::door)
                .layer(DefaultBodyLimit::max(crate::github_door::MAX_BODY)),
        )
        .route(
            checkout::PATH,
            post(checkout::mint)
                .delete(checkout::revoke)
                .layer(DefaultBodyLimit::max(checkout::MAX_BODY)),
        )
        .with_state(state)
}

/// Connect, migrate, start the poll sweepers, and the event workers when a
/// cell is configured, and serve until SIGTERM or ctrl-c. Then stop accepting,
/// drop any sweep in hand, let each worker finish the event in hand, and close
/// the pool.
pub async fn serve(config: Config, seams: Seams) -> anyhow::Result<()> {
    config.log_effective();
    let db = Db::connect(&config).await?;
    let admission = seams.admission.clone();
    let run_meter = seams.run_meter.clone();
    let governance = seams.governance.clone();
    let audit = seams.audit.clone();
    let mut state = AppState::new(db.clone(), &config, seams);
    // The links a patched checkout fetches its bundle by: signed with what
    // no job holds, the engines' secret for the credential door, which a
    // patched checkout asks anyway.
    state.bundle_links = config.url_for_jobs.as_ref().and_then(|public| {
        let mut secret = Vec::new();
        if let Some(s) = &config.checkout_token_secret {
            secret.extend_from_slice(s.as_bytes());
        }
        for (cell, s) in &config.checkout_cell_secrets {
            secret.extend_from_slice(cell.as_bytes());
            secret.extend_from_slice(s.as_bytes());
        }
        (!secret.is_empty()).then(|| Arc::new(crate::bundles::Links::new(public, &secret)))
    });
    state.payloads = match &config.object_store_url {
        Some(url) => Some(crate::payloads::Payloads::from_url(url)?),
        None => None,
    };
    let listener = tokio::net::TcpListener::bind(config.listen)
        .await
        .map_err(|e| anyhow::anyhow!("cannot listen on CI_LISTEN={}: {e}", config.listen))?;

    let (stop_tx, stop) = watch::channel(false);
    state.stop = stop.clone();
    #[cfg(feature = "otel")]
    spawn_otlp_metrics(state.clone(), stop.clone());
    // Each day's partition of the event queue made ahead, and the days past
    // CI_RECORD_DAYS dropped, where that is set.
    let retention = crate::retention::spawn(
        Arc::new(crate::retention::Retention::new(
            db.clone(),
            config.record_days,
            config.segment_days,
            state.payloads.clone(),
            state.metrics.clone(),
        )),
        stop.clone(),
    );
    // Beside the workers: the runs they submit are what it reports on.
    let reporter = match &config.api_token {
        Some(token) if config.event_workers > 0 && !config.cells.is_empty() => {
            let reporter = Reporter::new(
                db.clone(),
                config.cells.clone(),
                token.clone(),
                state.forge.clone(),
                config.run_url.clone(),
                state.metrics.clone(),
            );
            let reporter = match &run_meter {
                Some(meter) => reporter.with_meter(meter.clone()),
                None => reporter,
            };
            let reporter = match config.flake_issues {
                Some((file_after, days)) => reporter.with_flakes(file_after, days),
                None => reporter,
            };
            // A run moved through dagron-api is seen running by its sweep.
            let reporter = match &state.reruns {
                Some(reruns) => reporter.with_reruns(reruns.clone()),
                None => reporter,
            };
            Some(report::spawn(Arc::new(reporter), stop.clone()))
        }
        _ => None,
    };
    let mut workers = match &config.api_token {
        Some(token) if config.event_workers > 0 && !config.cells.is_empty() => {
            state.metrics.set_workers(config.event_workers);
            let dispatcher = Dispatcher::new(
                db.clone(),
                config.cells.clone(),
                token.clone(),
                state.forge.clone(),
                crate::pipeline::compile::Settings {
                    default_timeout_secs: config.default_timeout_secs,
                    ..Default::default()
                },
            )
            .with_metrics(state.metrics.clone())
            .with_max_attempts(config.event_max_attempts)
            .with_forks(config.forks.clone())
            .with_agent_runs(config.agent_runs)
            .with_github_workflows(config.github_workflows)
            .with_gitlab_pipelines(config.gitlab_pipelines)
            .with_actions(config.actions.clone())
            .with_github_oidc(config.github_oidc)
            .with_github_token(config.github_job_token)
            .with_github_docker(config.github_docker.clone())
            .with_github_api_for_jobs(state.door.as_ref().map(|door| door.public.clone()))
            .with_payloads(state.payloads.clone());
            let dispatcher = match admission {
                Some(admission) => dispatcher.with_admission(admission),
                None => dispatcher,
            };
            let dispatcher = match run_meter {
                Some(meter) => dispatcher.with_run_meter(meter),
                None => dispatcher,
            };
            let dispatcher = match governance {
                Some(governance) => dispatcher.with_governance(governance),
                None => dispatcher,
            };
            let dispatcher = match audit {
                Some(audit) => dispatcher.with_audit(audit),
                None => dispatcher,
            };
            let settings = worker::Settings {
                workers: config.event_workers,
                lease: config.event_lease,
                max_attempts: config.event_max_attempts,
            };
            Some(worker::spawn(
                db.clone(),
                Arc::new(dispatcher),
                settings,
                state.metrics.clone(),
                state.wake.clone(),
                stop.clone(),
            ))
        }
        _ => None,
    };
    // Always started: a repository may set its own interval when CI_POLL_SECS
    // is 0. An idle sweeper costs one query every few seconds.
    let poller = Poller::new(
        db.clone(),
        state.forge.clone(),
        config.poll_secs,
        state.metrics.clone(),
    );
    let mut sweepers = poll::spawn(Arc::new(poller), state.wake.clone(), stop.clone());
    // Where workflow files are read, their schedules fire.
    let mut schedulers = if config.github_workflows {
        let scheduler = crate::schedule::Scheduler::new(db.clone(), state.forge.clone());
        crate::schedule::spawn(Arc::new(scheduler), state.wake.clone(), stop.clone())
    } else {
        tokio::task::JoinSet::new()
    };
    // The fleet's looker: it needs the cells, dagron-ci's token for them,
    // and the App the label map was refused without. One looks over either
    // forge's repositories, each by its own forge's map.
    let fleets = (config.fleet.is_some() || config.gitlab_fleet.is_some()).then_some(());
    let lookout = match (fleets, &config.api_token, &state.reruns) {
        (Some(()), Some(token), Some(reruns)) if !config.cells.is_empty() => {
            let github = config.fleet.clone().unwrap_or_default();
            let gitlab = config.gitlab_fleet.clone().unwrap_or_default();
            let labels =
                |fleet: &crate::fleet::Fleet| fleet.labels().collect::<Vec<_>>().join(", ");
            if config.fleet.is_some() {
                tracing::info!(
                    labels = %labels(&github),
                    environment = %config.fleet_environment,
                    "fleet mode: jobs of GitHub Actions' own that ask for these labels are served"
                );
            }
            if config.gitlab_fleet.is_some() {
                tracing::info!(
                    labels = %labels(&gitlab),
                    environment = %config.fleet_environment,
                    "fleet mode: jobs of GitLab CI's own that name these tags are served"
                );
            }
            let lookout = crate::fleet::worker::Lookout::new(
                db.clone(),
                github,
                gitlab,
                state.forge.clone(),
                config.cells.clone(),
                token.clone(),
                reruns.clone(),
                config.fleet_environment.clone(),
                config.fleet_untrusted_environment.clone(),
            );
            Some(crate::fleet::worker::spawn(Arc::new(lookout), stop.clone()))
        }
        // The configuration is refused without them when it is read. One
        // made otherwise is refused here: served, it would take a job's
        // delivery and keep it, with nothing to start it a runner.
        (Some(()), ..) => anyhow::bail!(
            "CI_GITHUB_FLEET_FILE and CI_GITLAB_FLEET_FILE need a cell to start each runner in, \
             and dagron-ci's token for it (CI_CELLS, CI_API_TOKEN_FILE)"
        ),
        (None, ..) => None,
    };
    tokio::spawn(async move {
        shutdown_signal().await;
        let _ = stop_tx.send(true);
    });

    tracing::info!(listen = %config.listen, workers = config.event_workers, "dagron-ci serving");
    let mut stopping = stop.clone();
    axum::serve(listener, router(state))
        .with_graceful_shutdown(async move {
            while !*stopping.borrow() {
                if stopping.changed().await.is_err() {
                    break;
                }
            }
        })
        .await?;
    // A pass cut off at stop is made again at the next start: each of its
    // steps is a transaction of its own.
    retention.abort();
    // A sweeper drops its sweep on stop, uncommitted: the repository is due
    // again once its claim lapses. This only waits for them to notice.
    if tokio::time::timeout(Duration::from_secs(5), async {
        while sweepers.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        sweepers.abort_all();
    }
    // A scheduler drops its look on stop, as a sweeper does.
    if tokio::time::timeout(Duration::from_secs(5), async {
        while schedulers.join_next().await.is_some() {}
    })
    .await
    .is_err()
    {
        schedulers.abort_all();
    }
    // A look in hand is short; one cut off leaves its run to its lease.
    if let Some(mut reporter) = reporter {
        if tokio::time::timeout(Duration::from_secs(15), &mut reporter)
            .await
            .is_err()
        {
            tracing::warn!(
                "the commit-status reporter is still busy; leaving its run to its lease"
            );
            reporter.abort();
        }
    }
    // A look in hand is short too; one cut off leaves its fleet to its lease.
    if let Some(mut lookout) = lookout {
        if tokio::time::timeout(Duration::from_secs(15), &mut lookout)
            .await
            .is_err()
        {
            lookout.abort();
        }
    }
    if let Some(mut set) = workers.take() {
        // A worker still busy after a lease is abandoned: its claim lapses and
        // another replica takes the event over, so nothing is lost.
        let drained = tokio::time::timeout(config.event_lease, async {
            while set.join_next().await.is_some() {}
        })
        .await;
        if drained.is_err() {
            tracing::warn!("event workers still busy after a lease; leaving their events to lapse");
            set.abort_all();
        }
    }
    db.close().await;
    tracing::info!("dagron-ci stopped");
    Ok(())
}

/// Resolves on SIGTERM (a Kubernetes stop) or ctrl-c (local use).
async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };
    #[cfg(unix)]
    let term = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut sig) => {
                sig.recv().await;
            }
            Err(_) => std::future::pending().await,
        }
    };
    #[cfg(not(unix))]
    let term = std::future::pending::<()>();
    tokio::select! {
        _ = ctrl_c => {}
        _ = term => {}
    }
    tracing::info!("shutdown signal received; draining");
}

/// Liveness: the process answers. No database.
async fn healthz() -> &'static str {
    "ok"
}

/// Readiness: one bounded round trip to the database. Without it no delivery
/// can be recorded, so a replica that cannot reach it should get no traffic.
async fn readyz(State(st): State<AppState>) -> Response {
    match tokio::time::timeout(PROBE_BUDGET, st.db.ping()).await {
        Ok(Ok(())) => (StatusCode::OK, "ready").into_response(),
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "readiness: the database query failed");
            (StatusCode::SERVICE_UNAVAILABLE, "database unreachable").into_response()
        }
        Err(_) => {
            tracing::warn!("readiness: the database did not answer within 1 s");
            (StatusCode::SERVICE_UNAVAILABLE, "database timed out").into_response()
        }
    }
}

async fn metrics_text(State(st): State<AppState>) -> Response {
    let text = exposition(&st).await;
    ([(header::CONTENT_TYPE, metrics::CONTENT_TYPE)], text).into_response()
}

/// What `GET /metrics` serves: the counters, and the backlog and the fleet's
/// gauges where the database answers in time.
async fn exposition(st: &AppState) -> String {
    let backlog = match tokio::time::timeout(PROBE_BUDGET, st.db.backlog()).await {
        Ok(Ok(b)) => Some(b),
        Ok(Err(e)) => {
            tracing::warn!(error = %e, "metrics: the backlog query failed");
            None
        }
        Err(_) => {
            tracing::warn!("metrics: the backlog query did not answer within 1 s");
            None
        }
    };
    let mut text = st.metrics.render(backlog, &st.extra_metrics);
    // Fleet mode's, where it serves anything: left out, as the backlog is,
    // where the database did not answer in time.
    if st.fleet.is_some() || st.gitlab_fleet.is_some() {
        match tokio::time::timeout(PROBE_BUDGET, st.db.fleet_gauges()).await {
            Ok(Ok(gauges)) => {
                let labels: Vec<&str> = st
                    .fleet
                    .iter()
                    .chain(st.gitlab_fleet.iter())
                    .flat_map(|fleet| fleet.labels())
                    .collect();
                text.push_str(&metrics::fleet(&labels, &gauges));
            }
            Ok(Err(e)) => tracing::warn!(error = %e, "metrics: the fleet's query failed"),
            Err(_) => tracing::warn!("metrics: the fleet's query did not answer within 1 s"),
        }
    }
    text
}

/// Send what `GET /metrics` serves to an OTLP collector each
/// `OTEL_METRIC_EXPORT_INTERVAL`, where the environment names one
/// ([`dagron_logging::otlp_metrics`]), as the engine sends its own. Each
/// export costs what a scrape does.
#[cfg(feature = "otel")]
fn spawn_otlp_metrics(state: AppState, mut stop: watch::Receiver<bool>) {
    let Some(export) = dagron_logging::otlp_metrics::MetricsExport::from_env(
        dagron_logging::service().unwrap_or("dagron-ci"),
    ) else {
        return;
    };
    tracing::info!(
        every_ms = export.interval().as_millis() as u64,
        "dagron-ci's metrics are sent to the OTLP collector"
    );
    tokio::spawn(async move {
        let mut tick = tokio::time::interval(export.interval());
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = tick.tick() => export.send(exposition(&state).await),
                _ = stop.changed() => return,
            }
        }
    });
}

/// `POST /hooks/github`. 202 once the event is committed, or once a delivery
/// is known to need nothing; 401 for a bad signature; 400 for a verified
/// request that cannot be read; 404 for a repository this dagron-ci does not
/// serve. Only a 202 for a new event writes anything.
async fn github_hook(State(st): State<AppState>, headers: HeaderMap, body: Bytes) -> Response {
    let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    let refuse = |outcome: Outcome, status: StatusCode, title: &str, detail: String| {
        st.metrics.github_delivery(outcome);
        problem(status, title, detail)
    };

    let Some(secret) = st.github_secret.as_deref() else {
        return refuse(
            Outcome::NotConfigured,
            StatusCode::NOT_FOUND,
            "receiver not configured",
            "this dagron-ci has no GitHub webhook secret (CI_GITHUB_WEBHOOK_SECRET_FILE)".into(),
        );
    };
    // Before anything reads the body.
    if let Err(e) = github::verify_signature(secret, header(github::SIGNATURE_HEADER), &body) {
        tracing::warn!(error = %e, "refused a GitHub delivery");
        crate::audit::record(st.audit.as_ref(), || {
            crate::audit::Entry::new(crate::audit::kind::DELIVERY_REFUSED)
                .with("forge", "github")
                .with(
                    "delivery",
                    header(github::DELIVERY_HEADER).unwrap_or_default(),
                )
                .with("why", e.to_string())
        });
        return refuse(
            Outcome::Unauthorized,
            StatusCode::UNAUTHORIZED,
            "signature refused",
            e.to_string(),
        );
    }

    let Some(event_type) = header(github::EVENT_HEADER) else {
        return refuse(
            Outcome::BadRequest,
            StatusCode::BAD_REQUEST,
            "no event type",
            "the X-GitHub-Event header is missing".into(),
        );
    };
    let Some(delivery_id) = header(github::DELIVERY_HEADER)
        .map(str::trim)
        .filter(|d| !d.is_empty() && d.len() <= 255 && !d.contains(char::is_control))
    else {
        return refuse(
            Outcome::BadRequest,
            StatusCode::BAD_REQUEST,
            "no delivery id",
            "the X-GitHub-Delivery header is missing or unusable".into(),
        );
    };
    if header(header::CONTENT_TYPE.as_str())
        .is_some_and(|ct| ct.starts_with("application/x-www-form-urlencoded"))
    {
        return refuse(
            Outcome::BadRequest,
            StatusCode::BAD_REQUEST,
            "form-encoded payload",
            "set the webhook's content type to application/json".into(),
        );
    }
    if event_type == "ping" {
        // Sent once when the hook is created: the secret works.
        st.metrics.github_delivery(Outcome::Ignored);
        return accepted(json!({ "pong": true }));
    }

    let (forge_host, full_name, event) = match github::parse(event_type, &body) {
        Ok(Parsed::Event {
            forge_host,
            full_name,
            event,
        }) => (forge_host, full_name, event),
        Ok(Parsed::Rerequested(asked)) => return rerequested(&st, delivery_id, asked).await,
        Ok(Parsed::Job(job)) => return fleet_job(&st, job).await,
        Ok(Parsed::Ignored(why)) => {
            st.metrics.github_delivery(Outcome::Ignored);
            return accepted(json!({ "ignored": why }));
        }
        // GitLab's alone: GitHub's deliveries make none.
        Ok(Parsed::Train(_)) => {
            st.metrics.github_delivery(Outcome::Ignored);
            return accepted(json!({ "ignored": "nothing in it leads to a run" }));
        }
        Err(e) => {
            return refuse(
                Outcome::BadRequest,
                StatusCode::BAD_REQUEST,
                "unreadable payload",
                e.to_string(),
            )
        }
    };

    let repo = match st.db.find_repo(&forge_host, &full_name).await {
        Ok(Some(repo)) => repo,
        Ok(None) => {
            return refuse(
                Outcome::UnknownRepo,
                StatusCode::NOT_FOUND,
                "repository not connected",
                format!(
                "{forge_host}/{full_name} is not served here; connect it with `dagron-ci repo add`"
            ),
            )
        }
        Err(e) => return database_error(&st, e),
    };
    // Its forge runs a fleet-mode repository's pipelines: nothing of it is
    // queued here. Its jobs' deliveries are the fleet's, above.
    if !lets_through(&repo, &event) {
        st.metrics.github_delivery(Outcome::Ignored);
        return accepted(json!({ "ignored": crate::db::FLEET_MODE }));
    }
    match st.record_delivery(&repo, delivery_id, &event, &body).await {
        Ok(Recorded::Event(id)) => {
            st.metrics.github_delivery(Outcome::Accepted);
            st.wake.notify_one();
            tracing::info!(
                event_id = %id,
                delivery_id,
                repo = %full_name,
                kind = event.kind.as_str(),
                trust = event.trust.as_str(),
                "event queued"
            );
            accepted(json!({ "event_id": id }))
        }
        Ok(Recorded::Duplicate) => {
            st.metrics.github_delivery(Outcome::Duplicate);
            accepted(json!({ "duplicate": true }))
        }
        Err(NotKept::Db(e)) => database_error(&st, e),
        Err(NotKept::Store(e)) => {
            st.metrics.github_delivery(Outcome::Error);
            payload_not_kept(e)
        }
    }
}

/// `POST /hooks/gitlab`, as [`github_hook`] answers: 202 once the event is
/// committed, or once a delivery is known to need nothing; 401 for a token
/// that is not the secret; 400 for a delivery that cannot be read; 404 for a
/// project this dagron-ci does not serve as a GitLab one. Only a 202 for a
/// new event writes anything.
async fn gitlab_hook(
    State(st): State<AppState>,
    headers: HeaderMap,
    body: axum::body::Body,
) -> Response {
    let header = |name: &str| headers.get(name).and_then(|v| v.to_str().ok());
    let refuse = |outcome: Outcome, status: StatusCode, title: &str, detail: String| {
        st.metrics.gitlab_delivery(outcome);
        problem(status, title, detail)
    };
    let Some(secret) = st.gitlab_secret.as_deref() else {
        return refuse(
            Outcome::NotConfigured,
            StatusCode::NOT_FOUND,
            "receiver not configured",
            "this dagron-ci has no GitLab webhook secret (CI_GITLAB_WEBHOOK_SECRET_FILE)".into(),
        );
    };
    // Before anything reads the body.
    if !gitlab::token_matches(secret, header(gitlab::TOKEN_HEADER)) {
        tracing::warn!("refused a GitLab delivery: X-Gitlab-Token is not the webhook's secret");
        crate::audit::record(st.audit.as_ref(), || {
            crate::audit::Entry::new(crate::audit::kind::DELIVERY_REFUSED)
                .with("forge", "gitlab")
                .with(
                    "why",
                    "X-Gitlab-Token is missing, or is not the webhook's secret token",
                )
        });
        return refuse(
            Outcome::Unauthorized,
            StatusCode::UNAUTHORIZED,
            "token refused",
            "X-Gitlab-Token is missing, or is not the webhook's secret token".into(),
        );
    }
    // Read only once the token is the secret's: GitLab signs nothing, so the
    // body says nothing of who sent it.
    let body = match axum::body::to_bytes(body, MAX_WEBHOOK_BYTES).await {
        Ok(body) => body,
        Err(_) => {
            return refuse(
                Outcome::BadRequest,
                StatusCode::PAYLOAD_TOO_LARGE,
                "payload too large",
                format!(
                    "a delivery is read to {} MiB, and this one is over it, or was cut short",
                    MAX_WEBHOOK_BYTES >> 20
                ),
            )
        }
    };
    let Some(event_type) = header(gitlab::EVENT_HEADER) else {
        return refuse(
            Outcome::BadRequest,
            StatusCode::BAD_REQUEST,
            "no event type",
            "the X-Gitlab-Event header is missing".into(),
        );
    };
    let Some(delivery_id) = gitlab::DELIVERY_HEADERS
        .iter()
        .find_map(|name| header(name))
        .map(str::trim)
        .filter(|d| !d.is_empty() && d.len() <= 255 && !d.contains(char::is_control))
    else {
        return refuse(
            Outcome::BadRequest,
            StatusCode::BAD_REQUEST,
            "no delivery id",
            "the webhook-id, Idempotency-Key and X-Gitlab-Event-UUID headers are missing, or \
             the first of them is unusable"
                .into(),
        );
    };
    let (forge_host, full_name, event, train) = match gitlab::parse(event_type, &body) {
        Ok(Parsed::Event {
            forge_host,
            full_name,
            event,
        }) => (forge_host, full_name, event, None),
        Ok(Parsed::Train(train)) => (
            train.forge_host,
            train.full_name,
            train.event,
            Some(train.pipeline),
        ),
        Ok(Parsed::Ignored(why)) => {
            st.metrics.gitlab_delivery(Outcome::Ignored);
            return accepted(json!({ "ignored": why }));
        }
        Ok(Parsed::Job(job)) => return gitlab_fleet_job(&st, job).await,
        Ok(_) => {
            st.metrics.gitlab_delivery(Outcome::Ignored);
            return accepted(json!({ "ignored": "nothing in it leads to a run" }));
        }
        Err(e) => {
            return refuse(
                Outcome::BadRequest,
                StatusCode::BAD_REQUEST,
                "unreadable payload",
                e.to_string(),
            )
        }
    };
    let not_recorded = |e: sqlx::Error| {
        tracing::error!(error = %e, "could not record a GitLab delivery");
        refuse(
            Outcome::Error,
            StatusCode::INTERNAL_SERVER_ERROR,
            "not recorded",
            "the event queue is unavailable; the delivery can be retried".into(),
        )
    };
    // A repository of the same name on another forge is not this project:
    // GitLab's secret says nothing of it.
    let repo = match st.db.find_repo(&forge_host, &full_name).await {
        Ok(Some(repo)) if repo.forge == "gitlab" => repo,
        Ok(_) => {
            return refuse(
                Outcome::UnknownRepo,
                StatusCode::NOT_FOUND,
                "repository not connected",
                format!(
                    "{forge_host}/{full_name} is not served here as a GitLab project; connect it \
                     with `dagron-ci repo add --forge gitlab`"
                ),
            )
        }
        Err(e) => return not_recorded(e),
    };
    if !lets_through(&repo, &event) {
        st.metrics.gitlab_delivery(Outcome::Ignored);
        return accepted(json!({ "ignored": crate::db::FLEET_MODE }));
    }
    if let Some(pipeline) = &train {
        // A merge train's pipeline is told of at each step it takes (created,
        // pending, running): its checks are asked for once, until the train
        // lets go of it, and it is let go of once.
        let asked = match st
            .db
            .group_asked(&repo.id, &event.git_ref, &event.sha)
            .await
        {
            Ok(asked) => asked,
            Err(e) => return not_recorded(e),
        };
        let done = match event.kind {
            crate::event::Kind::MergeGroup if asked => {
                Some("the merge train's checks of this commit were asked for already")
            }
            crate::event::Kind::MergeGroupDestroyed if !asked => {
                Some("no checks of this commit are asked for of the merge train")
            }
            _ => None,
        };
        if let Some(why) = done {
            st.metrics.gitlab_delivery(Outcome::Ignored);
            return accepted(json!({ "ignored": why }));
        }
        // Only now is GitLab asked which of the merge request's pipelines
        // this is: the hook names the request's source branch for each. Not
        // for longer than GitLab waits on the delivery itself.
        let asked = tokio::time::timeout(TRAIN_LOOKUP, st.forge.gitlab_pipeline(&repo, pipeline))
            .await
            .unwrap_or_else(|_| {
                Err(crate::forge::ForgeError::Failed(format!(
                    "no answer in {}s",
                    TRAIN_LOOKUP.as_secs()
                )))
            });
        match asked {
            Ok(Some(found)) if found.git_ref.as_deref() == Some(event.git_ref.as_str()) => {
                // GitLab tells a pipeline's steps in no set order: one told
                // late, of a pipeline that has ended since, asks for nothing.
                if let Some(status) = found.status.filter(|status| {
                    event.kind == crate::event::Kind::MergeGroup && !gitlab::waits(status)
                }) {
                    st.metrics.gitlab_delivery(Outcome::Ignored);
                    return accepted(json!({
                        "ignored": format!("pipeline {pipeline} is {status} now")
                    }));
                }
            }
            Ok(found) => {
                st.metrics.gitlab_delivery(Outcome::Ignored);
                let at = found.and_then(|found| found.git_ref).map_or_else(
                    || {
                        "no ref GitLab gives (a private project's pipelines are read with \
                         CI_GITLAB_TOKENS_DIR's token for its host)"
                            .to_string()
                    },
                    |at| format!("at {at}"),
                );
                return accepted(json!({
                    "ignored": format!("pipeline {pipeline} is {at}, not of its merge train")
                }));
            }
            Err(e) => {
                let (crate::forge::ForgeError::RateLimited { why, .. }
                | crate::forge::ForgeError::Failed(why)) = e;
                tracing::warn!(
                    repo = %full_name, pipeline = %pipeline, error = %why,
                    "GitLab could not be asked whether a pipeline is its merge train's"
                );
                return refuse(
                    Outcome::Error,
                    StatusCode::BAD_GATEWAY,
                    "GitLab not asked",
                    format!(
                        "whether pipeline {pipeline} is its merge train's could not be read: \
                         {why}; the delivery can be retried, and the pipeline's next step is \
                         told again"
                    ),
                );
            }
        }
    }
    match st.record_delivery(&repo, delivery_id, &event, &body).await {
        Ok(Recorded::Event(id)) => {
            st.metrics.gitlab_delivery(Outcome::Accepted);
            st.wake.notify_one();
            tracing::info!(
                event_id = %id,
                delivery_id,
                repo = %full_name,
                kind = event.kind.as_str(),
                trust = event.trust.as_str(),
                "event queued"
            );
            accepted(json!({ "event_id": id }))
        }
        Ok(Recorded::Duplicate) => {
            st.metrics.gitlab_delivery(Outcome::Duplicate);
            accepted(json!({ "duplicate": true }))
        }
        Err(NotKept::Db(e)) => not_recorded(e),
        Err(NotKept::Store(e)) => {
            st.metrics.gitlab_delivery(Outcome::Error);
            payload_not_kept(e)
        }
    }
}

/// A job of GitHub Actions' own, as its delivery says it stands. Where it
/// asks for a label the fleet serves, it is written down, and the
/// repository's fleet is looked over: a runner is started for a job that
/// waits ([`crate::fleet::worker`]). Nothing is started here, and a
/// delivery is answered whether or not a runner can be.
///
/// A job that asks for no label of the fleet's, or for one besides, is
/// another runner's: its delivery is dropped, and nothing is kept of it.
/// Unless GitHub gave it to a runner of the fleet's all the same, as it
/// gives a job that asks for its own labels alone (`self-hosted`) to any
/// self-hosted runner: it is then kept as a job of that runner's label, so
/// that it is told apart, and counted, as the label's own jobs are.
async fn fleet_job(st: &AppState, job: github::Job) -> Response {
    let ignored = |why: &str| {
        st.metrics.github_delivery(Outcome::Ignored);
        accepted(json!({ "ignored": why }))
    };
    let Some(fleet) = &st.fleet else {
        return ignored(
            "this dagron-ci serves no job of GitHub Actions' own (CI_GITHUB_FLEET_FILE)",
        );
    };
    let class = match fleet.serves(&job.labels) {
        Some((class, _)) => class.to_string(),
        None => {
            let ours = job
                .runner_name
                .as_deref()
                .filter(|name| crate::fleet::worker::is_runner_name(name));
            let class = match ours {
                Some(name) => match st.db.fleet_runner_class(name).await {
                    Ok(class) => class,
                    Err(e) => return database_error(st, e),
                },
                None => None,
            };
            match class {
                Some(class) => class,
                None => {
                    return ignored(
                        "the job asks for no label this fleet serves, or for one besides",
                    )
                }
            }
        }
    };
    let class = class.as_str();
    let repo = match st.db.find_repo(&job.forge_host, &job.full_name).await {
        Ok(Some(repo)) => repo,
        Ok(None) => {
            // An organization's runner may be given a job of a repository
            // that is not connected here: what the delivery says of the
            // runner is the fleet's all the same, and nothing of the job is.
            if let Err(e) = st.db.fleet_runner_heard(&job).await {
                return database_error(st, e);
            }
            st.metrics.github_delivery(Outcome::UnknownRepo);
            return problem(
                StatusCode::NOT_FOUND,
                "repository not connected",
                format!(
                    "{}/{} is not served here; connect it with `dagron-ci repo add`",
                    job.forge_host, job.full_name
                ),
            );
        }
        Err(e) => return database_error(st, e),
    };
    if let Err(e) = st.db.fleet_job(&repo, &job, class).await {
        return database_error(st, e);
    }
    st.metrics.github_delivery(Outcome::Accepted);
    let state = match job.action {
        github::JobAction::Queued => "queued",
        github::JobAction::InProgress => "running",
        github::JobAction::Completed => "done",
    };
    tracing::info!(
        repo = %job.full_name, job = job.id, class, state,
        runner = job.runner_name.as_deref().unwrap_or("-"),
        "fleet: a job's delivery was taken"
    );
    accepted(json!({ "fleet_job": job.id.to_string(), "class": class, "state": state }))
}

/// A job of GitLab CI's own, as its Job Hook says it stands. A job's
/// delivery names none of its tags, so what waits is not written down here:
/// the project's fleet is made due for a look, which asks GitLab for the
/// jobs that wait and their tags ([`crate::fleet::worker`]). What became of
/// a job the fleet keeps, or of one a runner of the fleet's took, is
/// written as GitHub's is.
async fn gitlab_fleet_job(st: &AppState, job: github::Job) -> Response {
    let ignored = |why: &str| {
        st.metrics.gitlab_delivery(Outcome::Ignored);
        accepted(json!({ "ignored": why }))
    };
    // GitLab's, and counted as GitLab's: [`database_error`] is GitHub's.
    let not_recorded = |e: sqlx::Error| {
        tracing::error!(error = %e, "could not record a GitLab delivery");
        st.metrics.gitlab_delivery(Outcome::Error);
        problem(
            StatusCode::INTERNAL_SERVER_ERROR,
            "not recorded",
            "the event queue is unavailable; the delivery can be retried",
        )
    };
    if st.gitlab_fleet.is_none() {
        return ignored("this dagron-ci serves no job of GitLab CI's own (CI_GITLAB_FLEET_FILE)");
    }
    let repo = match st.db.find_repo(&job.forge_host, &job.full_name).await {
        Ok(Some(repo)) if crate::forge::gitlab::is_gitlab(&repo) => repo,
        Ok(_) => {
            // A group's runner may be given a job of a project that is not
            // connected here.
            if let Err(e) = st.db.fleet_runner_heard(&job).await {
                return not_recorded(e);
            }
            st.metrics.gitlab_delivery(Outcome::UnknownRepo);
            return problem(
                StatusCode::NOT_FOUND,
                "repository not connected",
                format!(
                    "{}/{} is not served here; connect it with `dagron-ci repo add --forge gitlab`",
                    job.forge_host, job.full_name
                ),
            );
        }
        Err(e) => return not_recorded(e),
    };
    let id = job.id.to_string();
    let class = match st.db.fleet_job_class(&job.forge_host, &id).await {
        Ok(Some(class)) => Some(class),
        Ok(None) => match job
            .runner_name
            .as_deref()
            .filter(|name| crate::fleet::worker::is_runner_name(name))
        {
            Some(name) => match st.db.fleet_runner_class(name).await {
                Ok(class) => class,
                Err(e) => return not_recorded(e),
            },
            None => None,
        },
        Err(e) => return not_recorded(e),
    };
    let written = match (&class, job.action) {
        (Some(class), github::JobAction::InProgress | github::JobAction::Completed) => {
            st.db.fleet_job(&repo, &job, class).await
        }
        _ => st.db.fleet_due(&repo.id).await,
    };
    if let Err(e) = written {
        return not_recorded(e);
    }
    st.metrics.gitlab_delivery(Outcome::Accepted);
    let state = match job.action {
        github::JobAction::Queued => "queued",
        github::JobAction::InProgress => "running",
        github::JobAction::Completed => "done",
    };
    tracing::info!(
        repo = %job.full_name, job = job.id, state,
        class = class.as_deref().unwrap_or("-"),
        runner = job.runner_name.as_deref().unwrap_or("-"),
        "fleet: a GitLab job's delivery was taken"
    );
    accepted(json!({ "fleet_job": id, "class": class, "state": state }))
}

/// How long a check's "Re-run" may take to ask the run's cell: GitHub waits
/// ten seconds for a delivery's answer.
const RERUN_BUDGET: Duration = Duration::from_secs(8);

/// A check's "Re-run": the run the check run is of is run again from its
/// failed jobs, as [`api::rerun_run`] runs one, and its record re-opens, so
/// that its check runs follow it.
///
/// And "Re-run all checks", which GitHub sends as a check suite's: every
/// run of the commit that dagron-ci reported as check runs is run again in
/// the same way. From its failed jobs, each: a run that succeeded is left
/// as it is, and the answer says so. dagron has no rerun of everything.
///
/// **Whose authority.** GitHub's, for who may ask: it offers "Re-run" to
/// someone who can write to the repository, and the delivery's signature
/// says it is GitHub that sends this. dagron-ci's own, for the asking: the
/// rerun is made with its token for the run's cell, and dagron-api's record
/// of it names dagron-ci. Who asked is in dagron-ci's log.
///
/// What is run again is the run as it was submitted: the same commit, the
/// same pipeline, the same trust. A re-run widens nothing.
async fn rerequested(st: &AppState, delivery_id: &str, asked: github::Rerequested) -> Response {
    let ignored = |why: String| {
        st.metrics.github_delivery(Outcome::Ignored);
        accepted(json!({ "ignored": why }))
    };
    let Some(app) = st.checks_app else {
        return ignored("check runs are not on here (CI_GITHUB_CHECKS)".into());
    };
    let what = match &asked.of {
        github::Of::Run(_) => "a check's re-run",
        github::Of::Commit(_) => "a re-run of all checks",
    };
    // The check run, or the suite, is believed to be dagron-ci's only
    // where the delivery says whose it is.
    if asked.app_id != Some(app) {
        return ignored("a check run of another App".into());
    }
    // An agent's runs are refused until that class has its floor, and so is
    // what one asks to have run again.
    if asked.bot {
        return ignored("a re-run a bot asked for is not taken".into());
    }
    let repo = match st.db.find_repo(&asked.forge_host, &asked.full_name).await {
        Ok(Some(repo)) => repo,
        Ok(None) => {
            st.metrics.github_delivery(Outcome::UnknownRepo);
            return problem(
                StatusCode::NOT_FOUND,
                "repository not connected",
                format!(
                    "{}/{} is not served here; connect it with `dagron-ci repo add`",
                    asked.forge_host, asked.full_name
                ),
            );
        }
        Err(e) => return database_error(st, e),
    };
    if !repo.mode.runs_pipelines() {
        return ignored(crate::db::FLEET_MODE.into());
    }
    let runs: Vec<String> = match &asked.of {
        // The check run names its run. It is believed only for a run of
        // this repository: the name is the App's to write, and dagron-ci
        // wrote it.
        github::Of::Run(id) => {
            let run = match crate::rerun::is_run_id(id) {
                true => match st.db.run_commit(id).await {
                    Ok(run) => run.filter(|run| run.repo.id == repo.id),
                    Err(e) => return database_error(st, e),
                },
                false => None,
            };
            if run.is_none() {
                return ignored(
                    "the check run is of no run dagron-ci has for this repository".into(),
                );
            }
            vec![id.clone()]
        }
        // A suite names a commit. Its runs are those of this repository at
        // that commit that were reported as check runs: a run reported as
        // statuses is of no suite.
        github::Of::Commit(sha) => match st.db.checked_runs_at(&repo.id, sha).await {
            Ok(runs) if runs.is_empty() => {
                return ignored(
                    "dagron-ci has posted no check run for this commit of this repository".into(),
                )
            }
            Ok(runs) => runs,
            Err(e) => return database_error(st, e),
        },
    };
    let Some(reruns) = &st.reruns else {
        return ignored(
            "this dagron-ci has no token for its cells' dagron-api (CI_API_TOKEN_FILE)".into(),
        );
    };
    // Each run with its event, narrowed to that run.
    let mut found = Vec::with_capacity(runs.len());
    for id in &runs {
        match api::named(st, id).await {
            Ok(Some(named)) => found.push(named.event),
            Ok(None) => {}
            Err(e) => return database_error(st, e),
        }
    }
    if found.is_empty() {
        return ignored("the run's event is no longer on record".into());
    }
    // Taken once, as an event's delivery is: the same one sent again, or a
    // capture replayed, reruns nothing, however the run has ended since.
    match st.db.take_delivery(&repo.forge_host, delivery_id).await {
        Ok(true) => {}
        Ok(false) => {
            st.metrics.github_delivery(Outcome::Duplicate);
            return accepted(json!({ "duplicate": true }));
        }
        Err(e) => return database_error(st, e),
    }
    // Where nothing could be done for it, the delivery is not kept: it is
    // answered as one that may be sent again, and is then taken.
    let untaken = || async {
        if let Err(e) = st.db.forget_delivery(&repo.forge_host, delivery_id).await {
            tracing::warn!(delivery_id, error = %e, "a re-run's delivery could not be forgotten: sent again, it is a duplicate");
        }
    };
    // One budget for all of them: GitHub waits ten seconds for an answer,
    // however many runs the commit has. What has been done is kept apart
    // from the asking, so that it is still known where the asking is cut
    // short: a run that is running again is, whatever became of the rest.
    let done = std::sync::Mutex::new(api::Rerun {
        again: Vec::new(),
        left: Vec::new(),
        unavailable: None,
        denied: None,
    });
    let asking = async {
        for event in &found {
            // From the failed jobs, for "Re-run all checks" too: what
            // succeeded is kept.
            let one =
                api::rerun_runs(st, reruns, event, reruns.own_token(), RerunFrom::Failed).await?;
            let mut all = done.lock().unwrap_or_else(|e| e.into_inner());
            all.again.extend(one.again);
            all.left.extend(one.left);
            all.unavailable = all.unavailable.take().or(one.unavailable);
            // A run the cell refused dagron-ci's token for is left with
            // why, as one of an event's own runs is once another of them is
            // running again: a rerun of one run does not hide it. The first
            // is kept for where nothing was rerun at all.
            if let Some((code, run)) = one.denied {
                let pipeline = event
                    .runs
                    .iter()
                    .find(|r| r.id == run)
                    .map(|r| r.pipeline.clone());
                all.left.push(json!({
                    "id": run,
                    "pipeline": pipeline,
                    "why": "dagron-api did not let dagron-ci's own token rerun it",
                }));
                all.denied.get_or_insert((code, run));
            }
        }
        Ok::<_, sqlx::Error>(())
    };
    let ended = tokio::time::timeout(RERUN_BUDGET, asking).await;
    let mut rerun = done.into_inner().unwrap_or_else(|e| e.into_inner());
    let cut_short = match ended {
        Ok(Ok(())) => None,
        // Cut short with nothing done: the delivery is not kept, and may be
        // sent again.
        Ok(Err(e)) if rerun.again.is_empty() => {
            untaken().await;
            return database_error(st, e);
        }
        Err(_) if rerun.again.is_empty() => {
            untaken().await;
            st.metrics.github_delivery(Outcome::Error);
            return problem(
                StatusCode::SERVICE_UNAVAILABLE,
                "dagron-api did not answer in time",
                "the run's cell was asked for a rerun and has not answered; the delivery can \
                 be sent again",
            );
        }
        // Cut short with a run running again: the delivery stays taken, as
        // one that reran something is, so that sent again it reruns nothing
        // whatever that run has done since. The runs it did not get to are
        // left, with why.
        Ok(Err(e)) => {
            tracing::error!(
                delivery_id, of = %asked.of.as_str(), error = %e,
                "{what}: the database failed with some of the runs running again"
            );
            Some("dagron-ci's database failed before it was asked for")
        }
        Err(_) => Some("its cell had not answered in time, or was not asked"),
    };
    if let Some(why) = cut_short {
        let said: std::collections::BTreeSet<String> = rerun
            .again
            .iter()
            .chain(&rerun.left)
            .filter_map(|run| run["id"].as_str())
            .map(str::to_string)
            .collect();
        for id in runs.iter().filter(|id| !said.contains(*id)) {
            rerun.left.push(json!({ "id": id, "why": why }));
        }
        tracing::warn!(
            delivery_id, of = %asked.of.as_str(), %why,
            "{what}: cut short; the runs it got to are running again, and the rest are left"
        );
    }
    let sender = asked.sender.as_deref().unwrap_or("someone");
    if !rerun.again.is_empty() {
        st.metrics.github_delivery(Outcome::Accepted);
        // Some run again and some do not: what was done is what is
        // answered, with the others left and why. A cell that refuses
        // dagron-ci's token, or does not answer, is the operator's to see
        // as much as where nothing was rerun, so the log says it as loudly.
        if let Some((code, run)) = &rerun.denied {
            tracing::warn!(
                delivery_id, of = %asked.of.as_str(), code, run_id = %run,
                "{what}: dagron-api did not let dagron-ci's own token rerun a run; the others are running again"
            );
        }
        if let Some(reason) = &rerun.unavailable {
            tracing::warn!(
                delivery_id, of = %asked.of.as_str(), %reason,
                "{what}: a run's cell could not be asked; the others are running again"
            );
        }
        tracing::info!(
            delivery_id, of = %asked.of.as_str(), repo = %asked.full_name, by = %sender,
            again = rerun.again.len(), left = rerun.left.len(),
            "{what}: running again from the failed jobs"
        );
        return accepted(json!({ "rerun": rerun.again, "left": rerun.left }));
    }
    let why = rerun
        .left
        .iter()
        .filter_map(|run| run["why"].as_str())
        .collect::<Vec<_>>()
        .join("; ");
    if let Some(reason) = rerun.unavailable {
        untaken().await;
        st.metrics.github_delivery(Outcome::Error);
        tracing::warn!(delivery_id, of = %asked.of.as_str(), %reason, "{what} could not be asked for");
        return problem(
            StatusCode::SERVICE_UNAVAILABLE,
            "dagron-api could not be asked",
            "nothing was rerun; the delivery can be sent again",
        );
    }
    if let Some((code, _)) = rerun.denied {
        untaken().await;
        st.metrics.github_delivery(Outcome::Error);
        tracing::warn!(
            delivery_id, of = %asked.of.as_str(), code,
            "{what}: dagron-api did not let dagron-ci's own token rerun the run"
        );
        return problem(
            StatusCode::BAD_GATEWAY,
            "dagron-api refused the rerun",
            "dagron-ci's token for the run's cell may not rerun it (CI_API_TOKEN_FILE)",
        );
    }
    tracing::info!(
        delivery_id, of = %asked.of.as_str(), repo = %asked.full_name, by = %sender, %why,
        "{what}: nothing was rerun"
    );
    ignored(format!("nothing was rerun: {why}"))
}

fn accepted(body: serde_json::Value) -> Response {
    (StatusCode::ACCEPTED, Json(body)).into_response()
}

/// The longest a GitLab delivery waits on GitLab's answer to which pipeline
/// it is of: well within the 10 seconds GitLab gives a webhook, past which
/// the delivery counts as failed, and enough of them disable the hook.
const TRAIN_LOOKUP: Duration = Duration::from_secs(5);

/// Whether `repo`'s mode lets `event` be queued. Its forge runs a fleet-mode
/// repository's pipelines, so nothing of one is, but a merge group let go
/// of: its runs from before the repository was put in that mode are
/// cancelled still, which a dispatch does before it looks at the mode.
fn lets_through(repo: &crate::db::Repo, event: &crate::event::NewEvent) -> bool {
    repo.mode.runs_pipelines() || event.kind == crate::event::Kind::MergeGroupDestroyed
}

/// 500, so the forge redelivers. Nothing was committed: the delivery row and
/// its event share one transaction.
/// A payload over its row's limit that the store did not take: nothing is
/// recorded, and the forge delivers it again.
fn payload_not_kept(e: anyhow::Error) -> Response {
    tracing::error!(error = %format!("{e:#}"), "could not keep a delivery's payload in CI_OBJECT_STORE_URL");
    problem(
        StatusCode::SERVICE_UNAVAILABLE,
        "not recorded",
        "the delivery's payload could not be kept; the delivery can be retried",
    )
}

fn database_error(st: &AppState, e: sqlx::Error) -> Response {
    tracing::error!(error = %e, "could not record a GitHub delivery");
    st.metrics.github_delivery(Outcome::Error);
    problem(
        StatusCode::INTERNAL_SERVER_ERROR,
        "not recorded",
        "the event queue is unavailable; the delivery can be retried",
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::tests::{drop_schema, postgres, sqlite};
    use crate::github::sign;
    use crate::github::tests::{push_body, HEAD};
    use axum::body::Body;
    use axum::http::Request;
    use tower::ServiceExt;

    const SECRET: &[u8] = b"s3cret";

    fn state(db: Db, secret: Option<&[u8]>) -> AppState {
        AppState {
            db,
            github_secret: secret.map(Arc::from),
            gitlab_secret: secret.map(Arc::from),
            metrics: Arc::new(Metrics::default()),
            extra_metrics: Arc::from(Vec::new()),
            wake: Arc::new(Notify::new()),
            cells: Arc::default(),
            forge: Arc::new(Forge::new(None)),
            http: crate::config::Clients::new(reqwest::Client::builder),
            follow_http: crate::config::Clients::new(reqwest::Client::builder),
            stop: watch::channel(false).1,
            checkouts: None,
            reruns: None,
            admission: None,
            audit: None,
            checks_app: None,
            fleet: None,
            gitlab_fleet: None,
            agent_runs: false,
            door: None,
            payloads: None,
            bundle_links: None,
            bundle_sends: Arc::new(tokio::sync::Semaphore::new(crate::bundles::SENT_AT_ONCE)),
        }
    }

    fn delivery(event: &str, id: &str, body: &[u8], signature: Option<String>) -> Request<Body> {
        let mut req = Request::post("/hooks/github")
            .header("content-type", "application/json")
            .header(github::EVENT_HEADER, event)
            .header(github::DELIVERY_HEADER, id);
        if let Some(sig) = signature {
            req = req.header(github::SIGNATURE_HEADER, sig);
        }
        req.body(Body::from(body.to_vec())).unwrap()
    }

    fn signed(event: &str, id: &str, body: &[u8]) -> Request<Body> {
        delivery(event, id, body, Some(sign(SECRET, body)))
    }

    async fn call(app: &Router, req: Request<Body>) -> (StatusCode, String) {
        let res = app.clone().oneshot(req).await.unwrap();
        let status = res.status();
        let body = axum::body::to_bytes(res.into_body(), 1 << 20)
            .await
            .unwrap();
        (status, String::from_utf8(body.to_vec()).unwrap())
    }

    async fn get(app: &Router, path: &str) -> (StatusCode, String) {
        call(app, Request::get(path).body(Body::empty()).unwrap()).await
    }

    fn gitlab_delivery(event: &str, id: &str, body: &[u8], token: Option<&str>) -> Request<Body> {
        let mut req = Request::post("/hooks/gitlab")
            .header("content-type", "application/json")
            .header(gitlab::EVENT_HEADER, event)
            .header("webhook-id", id)
            .header("x-gitlab-event-uuid", "shared-by-recursive-webhooks");
        if let Some(token) = token {
            req = req.header(gitlab::TOKEN_HEADER, token);
        }
        req.body(Body::from(body.to_vec())).unwrap()
    }

    /// A GitLab job's Job Hook makes its project due for a look where it
    /// waits, and moves a job the fleet keeps on; a project nobody connected
    /// is refused, and without a GitLab fleet the delivery is dropped.
    #[tokio::test]
    async fn a_gitlab_job_s_hook_makes_its_project_due_for_a_look() {
        let (db, _dir) = sqlite().await;
        let repo = db
            .add_repo("gitlab", "gitlab.example", "group/app", None, "default")
            .await
            .unwrap()
            .unwrap();
        let token = std::str::from_utf8(SECRET).unwrap();
        let hook = |status: &str, runner: serde_json::Value, project: &str| {
            json!({
                "object_kind": "build", "build_id": 501, "build_status": status,
                "build_name": "test", "build_stage": "test", "pipeline_id": 77, "sha": HEAD,
                "runner": runner,
                "project": {
                    "id": 42, "web_url": format!("https://gitlab.example/{project}"),
                    "path_with_namespace": project,
                },
            })
            .to_string()
            .into_bytes()
        };
        let off = router(state(db.clone(), Some(SECRET)));
        let (status, text) = call(
            &off,
            gitlab_delivery(
                "Job Hook",
                "j-1",
                &hook("pending", json!(null), "group/app"),
                Some(token),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{text}");
        assert!(text.contains("CI_GITLAB_FLEET_FILE"), "{text}");

        let fleet =
            crate::fleet::Fleet::parse_gitlab("labels:\n  linux:\n    image: runner:1\n").unwrap();
        let app = router(state(db.clone(), Some(SECRET)).with_gitlab_fleet(fleet));
        let (status, text) = call(
            &app,
            gitlab_delivery(
                "Job Hook",
                "j-2",
                &hook("pending", json!(null), "group/app"),
                Some(token),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{text}");
        assert!(
            db.claim_fleet("test", Duration::from_secs(60))
                .await
                .unwrap()
                .is_some(),
            "its project is due for a look"
        );
        let (status, _) = call(
            &app,
            gitlab_delivery(
                "Job Hook",
                "j-3",
                &hook("pending", json!(null), "group/other"),
                Some(token),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND);

        // A job the fleet keeps, taken by its runner.
        let queued = crate::github::Job {
            forge_host: "gitlab.example".into(),
            full_name: "group/app".into(),
            action: crate::github::JobAction::Queued,
            id: 501,
            run_id: 77,
            run_attempt: 1,
            name: "test".into(),
            workflow: None,
            labels: vec!["linux".into()],
            head_sha: HEAD.into(),
            runner_id: None,
            runner_name: None,
            conclusion: None,
        };
        db.fleet_job(&repo, &queued, "linux").await.unwrap();
        assert!(db
            .fleet_want(
                &repo,
                "linux",
                "dagron-0a1b2c3d4e5f",
                None,
                crate::fleet::store::TRUSTED,
                crate::fleet::store::Quota::default(),
            )
            .await
            .unwrap());
        let runner = json!({ "id": 900, "description": "dagron-0a1b2c3d4e5f" });
        let (status, text) = call(
            &app,
            gitlab_delivery(
                "Job Hook",
                "j-4",
                &hook("running", runner, "group/app"),
                Some(token),
            ),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{text}");
        assert!(text.contains(r#""class":"linux""#), "{text}");
        let view = db.fleet_view(&repo.id).await.unwrap();
        assert_eq!(
            (
                view.runners[0].state.as_str(),
                view.runners[0].job_id.as_deref()
            ),
            ("busy", Some("501"))
        );
        assert!(view.queued.is_empty());
    }

    /// A GitLab project's push is queued with the webhook's secret token,
    /// once for its delivery, and for a project connected as GitLab's alone.
    #[tokio::test]
    async fn a_gitlab_delivery_is_queued_with_the_webhook_s_token() {
        let (db, _dir) = sqlite().await;
        let push = json!({
            "object_kind": "push", "ref": "refs/heads/main",
            "before": "0".repeat(40), "after": HEAD, "checkout_sha": HEAD,
            "user_username": "dev",
            "project": {
                "id": 7, "web_url": "https://gitlab.example/group/app",
                "path_with_namespace": "group/app",
            },
        })
        .to_string()
        .into_bytes();
        let token = std::str::from_utf8(SECRET).unwrap();

        let off = router(state(db.clone(), None));
        let (status, text) = call(
            &off,
            gitlab_delivery("Push Hook", "u-1", &push, Some(token)),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{text}");

        let app = router(state(db.clone(), Some(SECRET)));
        for wrong in [None, Some("s3cre"), Some("not it")] {
            let (status, text) =
                call(&app, gitlab_delivery("Push Hook", "u-1", &push, wrong)).await;
            assert_eq!(status, StatusCode::UNAUTHORIZED, "{wrong:?}: {text}");
        }

        let (status, text) = call(
            &app,
            gitlab_delivery("Push Hook", "u-1", &push, Some(token)),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "not connected: {text}");
        // The same name on another forge is not this project.
        db.add_repo("github", "gitlab.example", "group/app", None, "default")
            .await
            .unwrap()
            .unwrap();
        let (status, text) = call(
            &app,
            gitlab_delivery("Push Hook", "u-1", &push, Some(token)),
        )
        .await;
        assert_eq!(status, StatusCode::NOT_FOUND, "another forge's: {text}");

        let (db, _dir) = sqlite().await;
        db.add_repo("gitlab", "gitlab.example", "group/app", None, "default")
            .await
            .unwrap()
            .unwrap();
        let (origin, gl) = crate::forge::gitlab::tests::stub_gitlab().await;
        let app = router(state(db.clone(), Some(SECRET)).with_forge_origin(&origin));
        let (status, text) = call(
            &app,
            gitlab_delivery("Push Hook", "u-1", &push, Some(token)),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{text}");
        assert!(text.contains("event_id"), "{text}");
        let (status, text) = call(
            &app,
            gitlab_delivery("Push Hook", "u-1", &push, Some(token)),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{text}");
        assert!(text.contains("duplicate"), "{text}");

        let (status, text) = call(
            &app,
            gitlab_delivery("Note Hook", "u-2", b"{}", Some(token)),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{text}");
        assert!(text.contains("ignored"), "{text}");
        let (status, text) =
            call(&app, gitlab_delivery("Push Hook", "u-3", b"{", Some(token))).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{text}");
        let large = vec![b' '; MAX_WEBHOOK_BYTES + 1];
        let (status, text) = call(
            &app,
            gitlab_delivery("Push Hook", "u-4", &large, Some(token)),
        )
        .await;
        assert_eq!(status, StatusCode::PAYLOAD_TOO_LARGE, "{text}");
        let (status, _) = call(&app, gitlab_delivery("Push Hook", "u-4", &large, None)).await;
        assert_eq!(
            status,
            StatusCode::UNAUTHORIZED,
            "the token before the body"
        );

        let ev = db
            .claim_event("w", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
        assert_eq!((ev.kind.as_str(), ev.sha.as_str()), ("push", HEAD));
        assert!(
            db.claim_event("w", Duration::from_secs(60))
                .await
                .unwrap()
                .is_none(),
            "one event for the two deliveries"
        );

        let (_, metrics) = get(&app, "/metrics").await;
        assert!(
            metrics.contains(
                "dagron_ci_webhook_deliveries_total{forge=\"gitlab\",outcome=\"accepted\"} 1"
            ),
            "{metrics}"
        );

        // A merge request's pipeline is told of by its source branch, and
        // GitLab is asked, once the project is served, whether it is its
        // merge train's. One that is asks for its checks once, however many
        // steps of it are told; cancelled, the train let go of it.
        let pipeline = |id: u64, status: &str| {
            json!({
                "object_kind": "pipeline",
                "object_attributes": {
                    "id": id, "ref": "feature-x", "sha": HEAD,
                    "status": status, "source": "merge_request_event",
                },
                "merge_request": {
                    "iid": 4, "source_branch": "feature-x", "target_branch": "main",
                },
                "user": { "username": "mae" },
                "project": {
                    "id": 7, "web_url": "https://gitlab.example/group/app",
                    "path_with_namespace": "group/app",
                },
            })
            .to_string()
            .into_bytes()
        };
        let at = |id: u64| format!("/projects/group%2Fapp/pipelines/{id}");
        let answer = |id: u64, status: u16, body: serde_json::Value| {
            gl.lock()
                .unwrap()
                .answers
                .insert(at(id), (status, Vec::new(), body.to_string()));
        };
        let asked = |id: u64| {
            gl.lock()
                .unwrap()
                .seen
                .iter()
                .filter(|(_, path, _, _)| *path == at(id))
                .count()
        };
        answer(
            31,
            200,
            json!({ "id": 31, "ref": "refs/merge-requests/4/train", "status": "running" }),
        );
        answer(
            34,
            200,
            json!({ "id": 34, "ref": "refs/merge-requests/4/train", "status": "canceled" }),
        );
        answer(
            32,
            200,
            json!({ "id": 32, "ref": "refs/merge-requests/4/head" }),
        );
        answer(33, 500, json!({ "message": "500 Internal Server Error" }));
        let hook = |id: &str, pipeline_id: u64, status: &str| {
            gitlab_delivery(
                "Pipeline Hook",
                id,
                &pipeline(pipeline_id, status),
                Some(token),
            )
        };

        // The request's own pipeline, and one GitLab does not give, are not
        // the train's; one GitLab could not be asked of is said again.
        let (status, text) = call(&app, hook("p-0", 32, "running")).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{text}");
        assert!(text.contains("at refs/merge-requests/4/head"), "{text}");
        let (status, text) = call(&app, hook("p-00", 30, "running")).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{text}");
        assert!(text.contains("CI_GITLAB_TOKENS_DIR"), "{text}");
        let (status, text) = call(&app, hook("p-000", 33, "running")).await;
        assert_eq!(status, StatusCode::BAD_GATEWAY, "{text}");
        assert!(text.contains("can be retried"), "{text}");
        // Nor is a train's step told after the train cancelled it.
        let (status, text) = call(&app, hook("p-00000", 34, "running")).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{text}");
        assert!(text.contains("is canceled now"), "{text}");
        // Nor is one cancelled before anything asked for its checks: GitLab
        // is not asked.
        let (status, text) = call(&app, hook("p-0000", 32, "canceled")).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{text}");
        assert!(text.contains("no checks of this commit"), "{text}");
        assert_eq!(asked(32), 1);

        let (status, text) = call(&app, hook("p-1", 31, "pending")).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{text}");
        assert!(text.contains("event_id"), "{text}");
        let (status, text) = call(&app, hook("p-2", 31, "running")).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{text}");
        assert!(text.contains("asked for already"), "{text}");
        assert_eq!(asked(31), 1, "asked once its checks were asked for");
        let (status, text) = call(&app, hook("p-3", 31, "canceling")).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{text}");
        assert!(text.contains("event_id"), "{text}");
        let (status, text) = call(&app, hook("p-3b", 31, "canceled")).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{text}");
        assert!(
            text.contains("no checks of this commit"),
            "let go of once: {text}"
        );
        // Let go of and put on the train again at the same commit, it is
        // asked anew: later than it was let go of, which SQLite tells to
        // the millisecond.
        tokio::time::sleep(Duration::from_millis(5)).await;
        let (status, text) = call(&app, hook("p-4", 31, "created")).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{text}");
        assert!(text.contains("event_id"), "{text}");
        for kind in ["merge_group", "merge_group_destroyed", "merge_group"] {
            let ev = db
                .claim_event("w", Duration::from_secs(60))
                .await
                .unwrap()
                .unwrap();
            assert_eq!(
                (ev.kind.as_str(), ev.git_ref.as_str(), ev.trust.as_str()),
                (kind, "refs/merge-requests/4/train", "trusted")
            );
        }
        assert!(db
            .claim_event("w", Duration::from_secs(60))
            .await
            .unwrap()
            .is_none());

        // In fleet mode its forge runs its pipelines: a push queues nothing,
        // but a merge group let go of is, so that its runs from before are
        // cancelled.
        db.change_repo(
            "gitlab.example",
            "group/app",
            None,
            None,
            None,
            Some(crate::db::Mode::Fleet),
            "ops",
        )
        .await
        .unwrap();
        let (status, text) = call(
            &app,
            gitlab_delivery("Push Hook", "u-5", &push, Some(token)),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{text}");
        assert!(text.contains("fleet mode"), "{text}");
        let (status, text) = call(&app, hook("p-5", 31, "canceled")).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{text}");
        assert!(text.contains("event_id"), "{text}");
        let ev = db
            .claim_event("w", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(ev.kind.as_str(), "merge_group_destroyed");
        assert!(db
            .claim_event("w", Duration::from_secs(60))
            .await
            .unwrap()
            .is_none());
    }

    /// A GitLab that does not say in time which pipeline a delivery is of
    /// is answered 502 before GitLab gives up on the delivery itself.
    #[tokio::test]
    async fn a_merge_train_gitlab_does_not_answer_for_is_502_in_time() {
        let (db, _dir) = sqlite().await;
        db.add_repo("gitlab", "gitlab.example", "group/app", None, "default")
            .await
            .unwrap()
            .unwrap();
        // Takes the connection, and says nothing.
        let silent = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let origin = format!("http://{}", silent.local_addr().unwrap());
        tokio::spawn(async move {
            let mut held = Vec::new();
            while let Ok((conn, _)) = silent.accept().await {
                held.push(conn);
            }
        });
        let app = router(state(db.clone(), Some(SECRET)).with_forge_origin(&origin));
        let body = json!({
            "object_kind": "pipeline",
            "object_attributes": { "id": 31, "ref": "feature-x", "sha": HEAD, "status": "running" },
            "merge_request": { "iid": 4, "target_branch": "main" },
            "project": {
                "id": 7, "web_url": "https://gitlab.example/group/app",
                "path_with_namespace": "group/app",
            },
        })
        .to_string()
        .into_bytes();
        let token = std::str::from_utf8(SECRET).unwrap();
        let began = std::time::Instant::now();
        let (status, text) = call(
            &app,
            gitlab_delivery("Pipeline Hook", "p-1", &body, Some(token)),
        )
        .await;
        assert_eq!(status, StatusCode::BAD_GATEWAY, "{text}");
        assert!(text.contains("no answer in 5s"), "{text}");
        assert!(
            began.elapsed() < Duration::from_secs(9),
            "{:?}",
            began.elapsed()
        );
    }

    /// A job of GitHub Actions' own is written down for the fleet where it
    /// asks for one of the fleet's labels, and dropped where it does not.
    #[tokio::test]
    async fn a_workflow_job_is_taken_for_the_fleet_where_it_asks_for_its_label() {
        let (db, _dir) = sqlite().await;
        let repo = db
            .add_repo("github", "github.com", "acme/widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        let job = |id: u64, action: &str, labels: &[&str], repo: &str| {
            json!({
                "action": action,
                "workflow_job": {
                    "id": id, "run_id": 77, "run_attempt": 1, "name": "build",
                    "workflow_name": "CI", "head_sha": HEAD, "labels": labels,
                    "runner_name": null, "conclusion": null,
                },
                "repository": { "id": 7, "full_name": repo, "html_url": format!("https://github.com/{repo}") },
            })
            .to_string()
            .into_bytes()
        };
        let ours = ["self-hosted", "linux", "x64", "Dagron"];

        // With no fleet, a job's delivery is dropped as it always was.
        let off = router(state(db.clone(), Some(SECRET)));
        let body = job(1, "queued", &ours, "acme/widgets");
        let (status, text) = call(&off, signed("workflow_job", "d-1", &body)).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{text}");
        assert!(text.contains("CI_GITHUB_FLEET_FILE"), "{text}");

        let fleet =
            crate::fleet::Fleet::parse("labels:\n  dagron:\n    image: runner:1\n").unwrap();
        let app = router(state(db.clone(), Some(SECRET)).with_fleet(fleet));
        let (status, text) = call(&app, signed("workflow_job", "d-2", &body)).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{text}");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&text).unwrap(),
            json!({ "fleet_job": "1", "class": "dagron", "state": "queued" })
        );
        // Another runner's: hosted by GitHub, or asking for a label besides.
        for theirs in [&["ubuntu-latest"][..], &["self-hosted", "dagron", "gpu"]] {
            let body = job(2, "queued", theirs, "acme/widgets");
            let (status, text) = call(&app, signed("workflow_job", "d-3", &body)).await;
            assert_eq!(status, StatusCode::ACCEPTED, "{text}");
            assert!(text.contains("ignored"), "{theirs:?}: {text}");
        }
        // A repository nobody connected is refused, as any delivery of it is.
        let body = job(3, "queued", &ours, "acme/elsewhere");
        let (status, _) = call(&app, signed("workflow_job", "d-4", &body)).await;
        assert_eq!(status, StatusCode::NOT_FOUND);
        // And an unsigned one says nothing of anything.
        let body = job(4, "queued", &ours, "acme/widgets");
        let unsigned = delivery("workflow_job", "d-5", &body, None);
        assert_eq!(call(&app, unsigned).await.0, StatusCode::UNAUTHORIZED);

        let view = db.fleet_view(&repo.id).await.unwrap();
        assert_eq!(
            view.queued
                .get("dagron")
                .map(crate::fleet::store::Waiting::all),
            Some(1),
            "the one job, once"
        );
        let look = db
            .claim_fleet("test", Duration::from_secs(60))
            .await
            .unwrap();
        assert!(
            look.is_some(),
            "and its repository's fleet is due for a look"
        );

        // A job that asks for `self-hosted` alone is another runner's, but
        // GitHub may give it to the fleet's: it is then kept as a job of
        // that runner's label.
        assert!(db
            .fleet_want(
                &repo,
                "dagron",
                "dagron-0a1b2c3d4e5f",
                None,
                crate::fleet::store::TRUSTED,
                crate::fleet::store::Quota::default(),
            )
            .await
            .unwrap());
        let taken = |id: u64, runner: &str| {
            let mut body: serde_json::Value =
                serde_json::from_slice(&job(id, "in_progress", &["self-hosted"], "acme/widgets"))
                    .unwrap();
            body["workflow_job"]["runner_name"] = json!(runner);
            body.to_string().into_bytes()
        };
        let (status, text) = call(
            &app,
            signed("workflow_job", "d-6", &taken(5, "dagron-0a1b2c3d4e5f")),
        )
        .await;
        assert_eq!(status, StatusCode::ACCEPTED, "{text}");
        assert!(text.contains(r#""class":"dagron""#), "{text}");
        let view = db.fleet_view(&repo.id).await.unwrap();
        assert_eq!(
            (
                view.runners[0].state.as_str(),
                view.runners[0].job_id.as_deref()
            ),
            ("busy", Some("5"))
        );
        // Another runner's runner is no business of the fleet's.
        let (_, text) = call(
            &app,
            signed("workflow_job", "d-7", &taken(6, "GitHub Actions 3")),
        )
        .await;
        assert!(text.contains("ignored"), "{text}");
    }

    /// The acceptance list for the GitHub receiver, against either database.
    async fn the_receiver_contract(db: Db) {
        db.add_repo("github", "github.com", "acme/widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        let app = router(state(db.clone(), Some(SECRET)));
        let body = push_body("refs/heads/main");

        // A signed push is queued once; its replay writes nothing.
        let (status, text) = call(&app, signed("push", "d-1", &body)).await;
        assert_eq!(status, StatusCode::ACCEPTED, "{text}");
        assert!(text.contains("event_id"), "{text}");
        let (status, text) = call(&app, signed("push", "d-1", &body)).await;
        assert_eq!(
            (status, text.as_str()),
            (StatusCode::ACCEPTED, "{\"duplicate\":true}")
        );
        assert_eq!(db.backlog().await.unwrap().pending, 1);

        // A bad or missing signature is 401 and writes nothing.
        let forged = delivery("push", "d-2", &body, Some(sign(b"guess", &body)));
        assert_eq!(call(&app, forged).await.0, StatusCode::UNAUTHORIZED);
        assert_eq!(
            call(&app, delivery("push", "d-3", &body, None)).await.0,
            StatusCode::UNAUTHORIZED
        );
        assert_eq!(db.backlog().await.unwrap().pending, 1);

        // A repository nobody connected is refused and writes nothing.
        let stranger = String::from_utf8(body.clone())
            .unwrap()
            .replace("Acme/Widgets", "acme/other");
        let (status, text) = call(&app, signed("push", "d-4", stranger.as_bytes())).await;
        assert_eq!(status, StatusCode::NOT_FOUND, "{text}");
        assert!(text.contains("acme/other"), "{text}");

        // Ping and unhandled events are acknowledged and dropped.
        let (status, text) = call(&app, signed("ping", "d-5", b"{\"zen\":\"z\"}")).await;
        assert_eq!(
            (status, text.as_str()),
            (StatusCode::ACCEPTED, "{\"pong\":true}")
        );
        assert_eq!(
            call(&app, signed("issues", "d-6", b"{}")).await.0,
            StatusCode::ACCEPTED
        );

        // Verified but unreadable is 400.
        assert_eq!(
            call(&app, signed("push", "d-7", b"{}")).await.0,
            StatusCode::BAD_REQUEST
        );
        let mut no_delivery = signed("push", "x", &body);
        no_delivery.headers_mut().remove(github::DELIVERY_HEADER);
        assert_eq!(call(&app, no_delivery).await.0, StatusCode::BAD_REQUEST);
        let mut form = signed("push", "d-8", &body);
        form.headers_mut().insert(
            "content-type",
            "application/x-www-form-urlencoded".parse().unwrap(),
        );
        assert_eq!(call(&app, form).await.0, StatusCode::BAD_REQUEST);

        assert_eq!(
            db.backlog().await.unwrap().pending,
            1,
            "only the first push was queued"
        );

        let (_, text) = get(&app, "/metrics").await;
        for line in [
            "outcome=\"accepted\"} 1",
            "outcome=\"duplicate\"} 1",
            "outcome=\"unauthorized\"} 2",
            "outcome=\"unknown_repo\"} 1",
            "outcome=\"ignored\"} 2",
            "outcome=\"bad_request\"} 3",
            "dagron_ci_events_pending 1",
        ] {
            assert!(text.contains(line), "{line} in:\n{text}");
        }
    }

    #[tokio::test]
    async fn the_receiver_contract_on_sqlite() {
        let (db, _dir) = sqlite().await;
        the_receiver_contract(db).await;
    }

    #[tokio::test]
    async fn the_receiver_contract_on_postgres() {
        let Some((db, schema, admin)) = postgres().await else {
            return;
        };
        the_receiver_contract(db.clone()).await;
        db.close().await;
        drop_schema(&admin, &schema).await;
    }

    #[tokio::test]
    async fn the_queued_event_is_what_the_payload_said() {
        let (db, _dir) = sqlite().await;
        let repo = db
            .add_repo("github", "github.com", "acme/widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        let app = router(state(db.clone(), Some(SECRET)));
        call(&app, signed("push", "d-1", &push_body("refs/tags/v1.2.3"))).await;
        let Db::Sqlite(pool) = &db else {
            unreachable!()
        };
        let row: (
            String,
            String,
            String,
            String,
            String,
            String,
            Option<String>,
        ) = sqlx::query_as(
            "SELECT kind, ref, sha, trust, source, commit_key, delivery_id FROM ci_events",
        )
        .fetch_one(pool)
        .await
        .unwrap();
        assert_eq!(
            row,
            (
                "tag".into(),
                "refs/tags/v1.2.3".into(),
                HEAD.into(),
                "trusted".into(),
                "webhook".into(),
                format!("ci:{}:tag:refs/tags/v1.2.3:{HEAD}", repo.id),
                Some("d-1".into()),
            )
        );
    }

    /// A payload over its row's limit is kept in CI_OBJECT_STORE_URL, its
    /// event naming it; a redelivery leaves nothing more there. Without a
    /// store it is not kept, and its event is; and with one that does not
    /// take it, nothing is recorded, and the forge is asked to send it again.
    #[tokio::test]
    async fn a_large_payload_is_kept_in_the_store_and_named_by_its_event() {
        let (db, _dir) = sqlite().await;
        db.add_repo("github", "github.com", "acme/widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        let mut body: serde_json::Value =
            serde_json::from_slice(&push_body("refs/heads/main")).unwrap();
        body["padding"] = serde_json::json!("x".repeat(crate::db::PAYLOAD_INLINE_MAX));
        let big = body.to_string().into_bytes();
        let store = tempfile::tempdir().unwrap();
        let files = |dir: &std::path::Path| -> Vec<std::path::PathBuf> {
            let mut out = Vec::new();
            let mut todo = vec![dir.to_path_buf()];
            while let Some(d) = todo.pop() {
                for entry in std::fs::read_dir(&d).into_iter().flatten().flatten() {
                    let p = entry.path();
                    if p.is_dir() {
                        todo.push(p)
                    } else {
                        out.push(p)
                    }
                }
            }
            out
        };
        let mut st = state(db.clone(), Some(SECRET));
        st.payloads = Some(
            crate::payloads::Payloads::from_url(&format!("file://{}", store.path().display()))
                .unwrap(),
        );
        let app = router(st);
        assert_eq!(
            call(&app, signed("push", "d-1", &big)).await.0,
            StatusCode::ACCEPTED
        );
        let Db::Sqlite(pool) = &db else {
            unreachable!()
        };
        let row: (Option<String>, Option<String>) =
            sqlx::query_as("SELECT payload, payload_ref FROM ci_events WHERE delivery_id = 'd-1'")
                .fetch_one(pool)
                .await
                .unwrap();
        let name = row.1.clone().expect("its event names the kept payload");
        assert_eq!(row.0, None, "not in the row");
        assert_eq!(std::fs::read(store.path().join(&name)).unwrap(), big);
        assert_eq!(files(store.path()).len(), 1);
        // Sent again: a duplicate, and the store keeps one.
        assert_eq!(
            call(&app, signed("push", "d-1", &big)).await.0,
            StatusCode::ACCEPTED
        );
        assert_eq!(
            files(store.path()).len(),
            1,
            "the redelivery's copy was taken out again"
        );
        assert_eq!(
            db.event_payload(
                &sqlx::query_scalar::<_, String>(
                    "SELECT id FROM ci_events WHERE delivery_id = 'd-1'"
                )
                .fetch_one(pool)
                .await
                .unwrap()
            )
            .await
            .unwrap(),
            Some(crate::db::EventPayload::Stored(name))
        );

        // No store: the event without its payload, as before.
        let app = router(state(db.clone(), Some(SECRET)));
        assert_eq!(
            call(&app, signed("push", "d-2", &big)).await.0,
            StatusCode::ACCEPTED
        );
        let row: (Option<String>, Option<String>) =
            sqlx::query_as("SELECT payload, payload_ref FROM ci_events WHERE delivery_id = 'd-2'")
                .fetch_one(pool)
                .await
                .unwrap();
        assert_eq!(row, (None, None));

        // A store that takes nothing: nothing recorded, and asked again.
        let not_a_dir = store.path().join("a-file");
        std::fs::write(&not_a_dir, b"").unwrap();
        let mut st = state(db.clone(), Some(SECRET));
        st.payloads = Some(
            crate::payloads::Payloads::from_url(&format!("file://{}", not_a_dir.display()))
                .unwrap(),
        );
        let app = router(st);
        assert_eq!(
            call(&app, signed("push", "d-3", &big)).await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        let rows: i64 =
            sqlx::query_scalar("SELECT count(*) FROM ci_events WHERE delivery_id = 'd-3'")
                .fetch_one(pool)
                .await
                .unwrap();
        assert_eq!(rows, 0);
        // And its delivery is not taken for a duplicate when it comes again.
        let rows: i64 =
            sqlx::query_scalar("SELECT count(*) FROM ci_deliveries WHERE delivery_id = 'd-3'")
                .fetch_one(pool)
                .await
                .unwrap();
        assert_eq!(rows, 0);
    }

    #[tokio::test]
    async fn without_a_secret_the_receiver_is_not_there() {
        let (db, _dir) = sqlite().await;
        let app = router(state(db, None));
        let body = push_body("refs/heads/main");
        assert_eq!(
            call(&app, signed("push", "d-1", &body)).await.0,
            StatusCode::NOT_FOUND
        );
    }

    #[tokio::test]
    async fn probes_answer() {
        let (db, _dir) = sqlite().await;
        let app = router(state(db.clone(), None));
        assert_eq!(get(&app, "/healthz").await, (StatusCode::OK, "ok".into()));
        assert_eq!(get(&app, "/readyz").await, (StatusCode::OK, "ready".into()));
        let (status, text) = get(&app, "/metrics").await;
        assert_eq!(status, StatusCode::OK);
        assert!(text.contains("dagron_ci_build_info{version=\""), "{text}");

        db.close().await;
        assert_eq!(
            get(&app, "/readyz").await.0,
            StatusCode::SERVICE_UNAVAILABLE
        );
        let (status, text) = get(&app, "/metrics").await;
        assert_eq!(
            status,
            StatusCode::OK,
            "metrics still answer without the database"
        );
        assert!(!text.contains("dagron_ci_events_pending"), "{text}");
    }

    struct Licensed;
    impl MetricsSource for Licensed {
        fn render(&self, out: &mut String) {
            out.push_str("# TYPE seam_metric gauge\nseam_metric 1\n");
        }
    }

    #[tokio::test]
    async fn a_seam_s_metrics_are_served() {
        let (db, _dir) = sqlite().await;
        let config =
            Config::from_lookup(|name| (name == "CI_DATABASE_URL").then(|| "sqlite://x".into()))
                .unwrap();
        let app = router(AppState::new(
            db,
            &config,
            Seams {
                metrics: vec![Arc::new(Licensed)],
                ..Seams::default()
            },
        ));
        assert!(get(&app, "/metrics").await.1.contains("seam_metric 1"));
    }
}
