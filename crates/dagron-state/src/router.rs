//! The `state` HTTP surface, as a self-contained component.
//!
//! This module is the reason the crate is shaped the way it is. It owns its own
//! [`Router`] and depends on its host through exactly one seam — [`PlanSubmitter`].
//! It does not know about `AppState`, sqlx, dagron's identity provider, or any
//! dagron crate. Mounting it is one `merge`; unmounting it is deleting that line;
//! standing it up as its own service is implementing [`PlanSubmitter`] over HTTP
//! and moving the directory. See this crate's README.
//!
//! ## Routes
//!
//! Paths are **mount-relative**: the component does not decide where it lives, so
//! a host can nest it wherever it wants and a future standalone binary can serve
//! it at the root. dagron mounts it at [`MOUNT_PREFIX`].
//!
//! | Route | Mounted in dagron as | What it does |
//! |---|---|---|
//! | `GET /contract` | `/api/state/contract` | The wire contract revision this build reads |
//! | `POST /plans` | `/api/state/plans` | Compile a plan → workflow YAML. Submits nothing. |
//! | `POST /plans/explain` | `/api/state/plans/explain` | Why each model rebuilds: summary, rows, markdown, Mermaid |
//! | `POST /plans/submit` | `/api/state/plans/submit` | Compile, then submit through the host |
//!
//! The compile-only route is the explainability surface: it answers "what would
//! this SQL change rebuild, and as what run graph" without side effects, which is
//! the same question `dagron-plan` answers for workflow changes.
//!
//! **Authentication is the host's job.** This router adds none, because it has no
//! way to know what a credential means here. `POST /plans/submit` creates runs, so
//! a host MUST layer its own auth over this router — dagron-api does exactly that
//! at the mount — and a standalone deployment must not expose it open.

use std::future::Future;
use std::sync::Arc;

use axum::extract::State;
use axum::http::StatusCode;
use axum::routing::{get, post};
use axum::{Json, Router};
use serde::Serialize;

use crate::compile::{compile, CompileError, PlanEnvelope};
use crate::explain::{explain, Explanation};
use crate::wire::WIRE_CONTRACT_VERSION;

/// The one thing this component needs from whoever hosts it: a way to turn
/// workflow YAML into a run.
///
/// dagron-api implements it over its own submit path. A future standalone
/// `dagron-state` binary implements it over `POST /api/runs`. Nothing else in this
/// crate knows which one it got.
pub trait PlanSubmitter: Send + Sync + 'static {
    /// Submit workflow YAML, returning the created run's id.
    fn submit(&self, yaml: String) -> impl Future<Output = Result<String, SubmitError>> + Send;
}

/// Why a submit did not produce a run.
#[derive(Clone, Debug)]
pub enum SubmitError {
    /// The host parsed the spec and refused it (→ 400).
    Rejected(String),
    /// The host could not be reached or is not ready (→ 503).
    Unavailable(String),
    /// Anything else (→ 500).
    Internal(String),
}

impl SubmitError {
    fn into_response(self) -> (StatusCode, String) {
        match self {
            Self::Rejected(m) => (StatusCode::BAD_REQUEST, m),
            Self::Unavailable(m) => (StatusCode::SERVICE_UNAVAILABLE, m),
            Self::Internal(m) => (StatusCode::INTERNAL_SERVER_ERROR, m),
        }
    }
}

/// What this build speaks, for a client that wants to check before sending.
#[derive(Debug, Serialize)]
pub struct ContractView {
    pub component: &'static str,
    pub wire_contract: &'static str,
}

/// A compiled plan, not submitted.
#[derive(Debug, Serialize)]
pub struct CompiledView {
    /// The workflow YAML `POST /api/runs` accepts.
    pub yaml: String,
    /// How many models the plan would rebuild.
    pub model_count: usize,
    /// Model names in plan (topological) order — the explain line.
    pub models: Vec<String>,
}

/// A compiled plan that became a run.
#[derive(Debug, Serialize)]
pub struct SubmittedView {
    pub run_id: String,
    pub model_count: usize,
    pub models: Vec<String>,
}

/// Where dagron mounts this component. Exported so the host and the docs cannot
/// drift apart on the prefix.
pub const MOUNT_PREFIX: &str = "/api/state";

/// Build the component's router, with mount-relative paths.
///
/// State is already applied, so the result is a plain `Router` a host can nest as
/// a service without sharing or matching state types:
///
/// ```ignore
/// let app = app.nest_service(
///     dagron_state::MOUNT_PREFIX,
///     dagron_state::router(Arc::new(my_submitter))
///         .layer(/* the host's auth */),
/// );
/// ```
pub fn router<S: PlanSubmitter>(submitter: Arc<S>) -> Router {
    Router::new()
        .route("/contract", get(contract))
        .route("/plans", post(plans::<S>))
        .route("/plans/explain", post(explain_plan::<S>))
        .route("/plans/submit", post(submit::<S>))
        .with_state(submitter)
}

async fn contract() -> Json<ContractView> {
    Json(ContractView { component: "dagron-state", wire_contract: WIRE_CONTRACT_VERSION })
}

/// `POST /api/state/plans` — compile only.
async fn plans<S: PlanSubmitter>(
    State(_submitter): State<Arc<S>>,
    Json(envelope): Json<PlanEnvelope>,
) -> Result<Json<CompiledView>, (StatusCode, String)> {
    let (yaml, models) = compile_to_yaml(&envelope)?;
    Ok(Json(CompiledView { yaml, model_count: models.len(), models }))
}

/// `POST /plans/explain` — why each model rebuilds, as rows + markdown + Mermaid.
///
/// Read-only, like `/plans`: this is the surface a reviewer hits from a pull
/// request, so it must never have a side effect.
async fn explain_plan<S: PlanSubmitter>(
    State(_submitter): State<Arc<S>>,
    Json(envelope): Json<PlanEnvelope>,
) -> Result<Json<Explanation>, (StatusCode, String)> {
    let spec = compile(&envelope).map_err(compile_status)?;
    Ok(Json(explain(&envelope, &spec)))
}

/// `POST /api/state/plans/submit` — compile, then hand the YAML to the host.
async fn submit<S: PlanSubmitter>(
    State(submitter): State<Arc<S>>,
    Json(envelope): Json<PlanEnvelope>,
) -> Result<(StatusCode, Json<SubmittedView>), (StatusCode, String)> {
    let (yaml, models) = compile_to_yaml(&envelope)?;
    let run_id = submitter.submit(yaml).await.map_err(SubmitError::into_response)?;
    Ok((
        StatusCode::CREATED,
        Json(SubmittedView { run_id, model_count: models.len(), models }),
    ))
}

/// Shared by both routes so they cannot disagree about what a plan compiles to.
/// One mapping from a compile refusal to a status, shared by every route so they
/// cannot disagree about what an empty plan means.
///
/// An empty plan is the planner's *good* outcome — nothing to rebuild — so it is a
/// 422, not a 400: the request was well-formed, there is just no run to make of it.
/// A caller planning on every commit hits this constantly and should not have to
/// treat it as a client error.
fn compile_status(e: CompileError) -> (StatusCode, String) {
    let status = match e {
        CompileError::EmptyPlan => StatusCode::UNPROCESSABLE_ENTITY,
        CompileError::PlannerFailed { .. } | CompileError::NoCommand => StatusCode::BAD_REQUEST,
    };
    (status, e.to_string())
}

fn compile_to_yaml(envelope: &PlanEnvelope) -> Result<(String, Vec<String>), (StatusCode, String)> {
    let spec = compile(envelope).map_err(compile_status)?;
    let models = spec
        .tasks
        .iter()
        .map(|t| {
            t.input
                .as_ref()
                .and_then(|i| i.get("model"))
                .and_then(|m| m.as_str())
                .unwrap_or(t.name.as_str())
                .to_string()
        })
        .collect();
    let yaml = spec
        .to_yaml()
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, format!("failed to render YAML: {e}")))?;
    Ok((yaml, models))
}
