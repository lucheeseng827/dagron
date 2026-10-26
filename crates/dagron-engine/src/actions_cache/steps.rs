//! **A step in an image of its own** (a workflow's Docker action): the job's
//! step runner asks for it here, with the job's own credential, and the job's
//! executor runs it beside the job, with the job's workspace and network
//! ([`Executor::run_step_container`]).
//!
//! - `POST /dagron/v1/steps`: the image, its entrypoint, its arguments, its
//!   variables and how long it may run; the answer is the run's id.
//! - `GET /dagron/v1/steps/{id}?wait=<secs>`: whether it has ended, waiting
//!   for that up to 30 seconds; once it has, its exit code and its output.
//! - `DELETE /dagron/v1/steps/{id}`: the step runner is done with it.
//! - `POST /dagron/v1/steps/images?dockerfile=<path>&timeout_secs=<secs>`,
//!   a tar of a Dockerfile's directory: its image built
//!   ([`Executor::build_step_image`]), for a Docker action whose image is
//!   one; asked after as a step's run is, and once it has ended, the
//!   image's name and what the build said. Named for a digest of what it is
//!   built from, here, so that no job chooses which image another's is.
//!
//! A run that is not trusted names an image of those its tasks may name
//! (`DAGRON_UNTRUSTED_IMAGES`), as its tasks' own images are, and has none
//! built; the container is held as its task is, by the executor.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::extract::{Path, Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::{IntoResponse, Response};
use axum::Json;
use dagron_executor::executor::{
    StepBuild, StepBuilt, StepContainer, StepOutcome, BUILT_STEP_IMAGE,
};
use serde::Deserialize;
use serde_json::json;
use sha2::{Digest, Sha256};
use tokio::sync::watch;

use super::ActionsCache;

/// The most steps of one task running at once.
const MAX_RUNNING_PER_TASK: usize = 2;
/// The longest a poll waits for a step to end.
const MAX_WAIT_SECS: u64 = 30;
/// The longest a step may be asked to run.
const MAX_STEP_SECS: u64 = 24 * 3600;
/// How much a step may be given.
const MAX_ARGS: usize = 256;
const MAX_ENV: usize = 1024;
const MAX_TEXT: usize = 128 << 10;
/// The most a step's image may be built from.
pub(super) const MAX_CONTEXT_BYTES: usize = 64 << 20;
/// A run whose end nobody asked after is dropped after this.
const KEPT_FOR_AT_MOST: Duration = Duration::from_secs(24 * 3600);

/// How a step's run ended: its container's outcome, or why it did not run.
#[derive(Debug, Clone)]
enum Ended {
    Ran(StepOutcome),
    Built(StepBuilt),
    Failed(String),
}

/// A step's run, while the step runner has not had its end.
pub(super) struct Running {
    task: String,
    ended: watch::Receiver<Option<Ended>>,
    at: Instant,
    /// The step runner let go of it: no one may ask after it, and it counts
    /// as running until its container has ended.
    let_go: bool,
}

impl Running {
    fn has_ended(&self) -> bool {
        self.ended.borrow().is_some()
    }
}

pub(super) type Runs = std::sync::Mutex<HashMap<String, Running>>;

#[derive(Deserialize)]
pub(super) struct Asked {
    image: String,
    #[serde(default)]
    entrypoint: Option<String>,
    #[serde(default)]
    args: Vec<String>,
    #[serde(default)]
    env: Vec<(String, String)>,
    #[serde(default)]
    timeout_secs: Option<u64>,
}

#[derive(Deserialize)]
pub(super) struct Building {
    #[serde(default)]
    dockerfile: Option<String>,
    #[serde(default)]
    timeout_secs: Option<u64>,
}

#[derive(Deserialize)]
pub(super) struct Wait {
    #[serde(default)]
    wait: Option<u64>,
}

fn refused(status: StatusCode, why: impl Into<String>) -> Response {
    (status, Json(json!({ "error": why.into() }))).into_response()
}

/// A name or a text no job could mean as more than one thing.
fn plain(text: &str) -> bool {
    text.len() <= MAX_TEXT && !text.contains('\0')
}

fn variable(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 256
        && !name.starts_with(|c: char| c.is_ascii_digit())
        && name.chars().all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// Start a step's container: its run's id.
pub(super) async fn start(
    State(svc): State<Arc<ActionsCache>>,
    headers: HeaderMap,
    Json(asked): Json<Asked>,
) -> Response {
    let Some(claims) = svc.claims(&headers) else {
        return refused(
            StatusCode::UNAUTHORIZED,
            "no credential this engine gave a job, or one that has lapsed",
        );
    };
    let Some(executor) = svc.executor.clone() else {
        return refused(
            StatusCode::SERVICE_UNAVAILABLE,
            "this engine runs no step in an image of its own",
        );
    };
    let image = asked.image.trim();
    if image.is_empty()
        || image.len() > 512
        || image.chars().any(|c| c.is_whitespace() || c.is_control())
    {
        return refused(StatusCode::BAD_REQUEST, "the step names no image");
    }
    if asked.args.len() > MAX_ARGS
        || asked.env.len() > MAX_ENV
        || !asked.args.iter().all(|a| plain(a))
        || !asked.entrypoint.as_deref().is_none_or(plain)
    {
        return refused(
            StatusCode::BAD_REQUEST,
            format!("a step is given at most {MAX_ARGS} arguments and {MAX_ENV} variables, each of text"),
        );
    }
    if let Some((name, _)) = asked
        .env
        .iter()
        .find(|(name, value)| !variable(name) || !plain(value))
    {
        return refused(
            StatusCode::BAD_REQUEST,
            format!("{name:?} is no variable a step may be given"),
        );
    }
    // A run that is not trusted names an image its tasks may name.
    if !claims.w
        && !svc.images.is_empty()
        && !svc
            .images
            .iter()
            .any(|pattern| dagron_core::envpolicy::matches(pattern, image))
    {
        return refused(
            StatusCode::FORBIDDEN,
            format!(
                "this run is not trusted, and its step names the image '{image}', which is none of \
                 those a run that is not trusted may name ({})",
                dagron_core::confine::ENV_IMAGES
            ),
        );
    }
    let secs = time_of(&claims, asked.timeout_secs);
    let Some((id, tell)) = begun(&svc, &claims.t) else {
        return too_many();
    };
    let step = StepContainer {
        image: image.to_string(),
        entrypoint: asked.entrypoint.filter(|e| !e.is_empty()),
        args: asked.args,
        env: asked.env,
        timeout: Duration::from_secs(secs),
    };
    let task = claims.t.clone();
    tracing::info!(task = %task, image = %step.image, "a step's container is asked for");
    tokio::spawn(async move {
        let ended = match executor.run_step_container(&task, step).await {
            Ok(outcome) => Ended::Ran(outcome),
            Err(e) => Ended::Failed(format!("{e:#}")),
        };
        let _ = tell.send(Some(ended));
    });
    (StatusCode::CREATED, Json(json!({ "id": id }))).into_response()
}

/// How long a step's run may take: what it asks, to the most a step may,
/// and no longer than its credential lasts.
fn time_of(claims: &super::Claims, asked: Option<u64>) -> u64 {
    let left = u64::try_from(claims.exp - chrono::Utc::now().timestamp()).unwrap_or(0);
    asked
        .unwrap_or(MAX_STEP_SECS)
        .clamp(1, MAX_STEP_SECS)
        .min(left.max(1))
}

/// A run of one of `task`'s steps, begun where the task has room for it:
/// its id, and where its end is told. None where it has none.
fn begun(svc: &ActionsCache, task: &str) -> Option<(String, watch::Sender<Option<Ended>>)> {
    let id = uuid::Uuid::new_v4().simple().to_string();
    let (tell, ended) = watch::channel(None);
    let mut runs = svc.steps.lock().unwrap();
    // One let go of is gone once it has ended; one that has not ended
    // counts, let go of or not: a job may not start more by letting go.
    runs.retain(|_, r| !(r.let_go && r.has_ended()));
    let running = runs
        .values()
        .filter(|r| r.task == task && !r.has_ended())
        .count();
    if running >= MAX_RUNNING_PER_TASK {
        return None;
    }
    runs.insert(
        id.clone(),
        Running {
            task: task.to_string(),
            ended,
            at: Instant::now(),
            let_go: false,
        },
    );
    Some((id, tell))
}

fn too_many() -> Response {
    refused(
        StatusCode::TOO_MANY_REQUESTS,
        "this job has as many steps' containers running as it may",
    )
}

/// A Dockerfile's path in what its image is built from: relative, and in
/// it.
fn dockerfile_path(path: &str) -> bool {
    !path.is_empty()
        && path.len() <= 512
        && !path.starts_with('/')
        && !path.contains(['\\', '\0'])
        && !path.chars().any(char::is_control)
        && !path.split('/').any(|part| part == "..")
}

/// The name of the image built from `context` with `dockerfile`: a digest
/// of both.
pub(super) fn built_name(dockerfile: &str, context: &[u8]) -> String {
    let mut digest = Sha256::new();
    digest.update(dockerfile.as_bytes());
    digest.update([0]);
    digest.update(context);
    let tag: String = digest.finalize()[..16]
        .iter()
        .map(|b| format!("{b:02x}"))
        .collect();
    format!("{BUILT_STEP_IMAGE}:{tag}")
}

/// Build a step's image from a tar of its Dockerfile's directory: its
/// run's id. Only for a trusted run.
pub(super) async fn build(
    State(svc): State<Arc<ActionsCache>>,
    headers: HeaderMap,
    Query(q): Query<Building>,
    context: axum::body::Bytes,
) -> Response {
    let Some(claims) = svc.claims(&headers) else {
        return refused(
            StatusCode::UNAUTHORIZED,
            "no credential this engine gave a job, or one that has lapsed",
        );
    };
    let Some(executor) = svc.executor.clone() else {
        return refused(
            StatusCode::SERVICE_UNAVAILABLE,
            "this engine runs no step in an image of its own",
        );
    };
    // A build runs what its Dockerfile says, where no task's confinement
    // holds it: a run that is not trusted names an image a registry has.
    if !claims.w {
        return refused(
            StatusCode::FORBIDDEN,
            "this run is not trusted, and an action whose image is built from a Dockerfile is \
             run only for one that is: one that is not trusted names an image a registry has \
             (docker://image)",
        );
    }
    let dockerfile = q.dockerfile.as_deref().unwrap_or("Dockerfile").trim();
    if !dockerfile_path(dockerfile) {
        return refused(
            StatusCode::BAD_REQUEST,
            "the Dockerfile is named by a path of what its image is built from",
        );
    }
    if context.is_empty() {
        return refused(StatusCode::BAD_REQUEST, "there is nothing to build from");
    }
    let secs = time_of(&claims, q.timeout_secs);
    let Some((id, tell)) = begun(&svc, &claims.t) else {
        return too_many();
    };
    let build = StepBuild {
        image: built_name(dockerfile, &context),
        context: context.to_vec(),
        dockerfile: dockerfile.to_string(),
        timeout: Duration::from_secs(secs),
    };
    let task = claims.t.clone();
    tracing::info!(task = %task, image = %build.image, "a step's image is asked to be built");
    tokio::spawn(async move {
        let ended = match executor.build_step_image(&task, build).await {
            Ok(built) => Ended::Built(built),
            Err(e) => Ended::Failed(format!("{e:#}")),
        };
        let _ = tell.send(Some(ended));
    });
    (StatusCode::CREATED, Json(json!({ "id": id }))).into_response()
}

/// Whether a step's run has ended, waiting for it a while; its end, once.
pub(super) async fn poll(
    State(svc): State<Arc<ActionsCache>>,
    Path(id): Path<String>,
    Query(q): Query<Wait>,
    headers: HeaderMap,
) -> Response {
    let Some(claims) = svc.claims(&headers) else {
        return refused(
            StatusCode::UNAUTHORIZED,
            "no credential this engine gave a job, or one that has lapsed",
        );
    };
    let mut ended = {
        let runs = svc.steps.lock().unwrap();
        match runs.get(&id).filter(|r| r.task == claims.t && !r.let_go) {
            Some(run) => run.ended.clone(),
            None => return refused(StatusCode::NOT_FOUND, "no such step of this job's"),
        }
    };
    let wait = Duration::from_secs(q.wait.unwrap_or(0).min(MAX_WAIT_SECS));
    let _ = tokio::time::timeout(wait, ended.wait_for(Option::is_some)).await;
    let end = ended.borrow().clone();
    let Some(end) = end else {
        return Json(json!({ "done": false })).into_response();
    };
    svc.steps.lock().unwrap().remove(&id);
    Json(match end {
        Ended::Ran(outcome) => json!({
            "done": true,
            "exit_code": outcome.exit_code,
            "timed_out": outcome.timed_out,
            "output": outcome.output,
        }),
        Ended::Built(built) => json!({
            "done": true,
            "image": built.image,
            "output": built.output,
        }),
        Ended::Failed(why) => json!({ "done": true, "error": why }),
    })
    .into_response()
}

/// The step runner is done with a step's run, ended or not. One that has
/// not ended runs on until its time is up or its task ends (the executor
/// takes back all a task lent when it ends), and counts against the job's
/// steps running at once until then.
pub(super) async fn stop(
    State(svc): State<Arc<ActionsCache>>,
    Path(id): Path<String>,
    headers: HeaderMap,
) -> Response {
    let Some(claims) = svc.claims(&headers) else {
        return refused(
            StatusCode::UNAUTHORIZED,
            "no credential this engine gave a job, or one that has lapsed",
        );
    };
    let mut runs = svc.steps.lock().unwrap();
    match runs.get_mut(&id) {
        Some(run) if run.task == claims.t && !run.let_go => {
            if run.has_ended() {
                runs.remove(&id);
            } else {
                run.let_go = true;
            }
            StatusCode::NO_CONTENT.into_response()
        }
        _ => refused(StatusCode::NOT_FOUND, "no such step of this job's"),
    }
}

/// Drop the runs whose end nobody asked after, and those let go of that
/// have ended.
pub(super) fn drop_stale(runs: &Runs) {
    runs.lock()
        .unwrap()
        .retain(|_, run| run.at.elapsed() <= KEPT_FOR_AT_MOST && !(run.let_go && run.has_ended()));
}
