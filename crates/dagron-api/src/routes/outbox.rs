//! The engine's outbox, handed to dagron-ci.
//!
//! dagron-ci's reporter follows each run it started by looking at it, and
//! used to look at every running run every few seconds whether anything had
//! changed or not. Here it is told instead: a task of the run beginning or
//! ending, the run ending, the run running again, as the engine's
//! transactional outbox has them, through a sink of its own
//! ([`dagron_core::db::CI_SINK`]'s types), claimed and acknowledged over
//! HTTP, as dagron-ci reaches a cell by its dagron-api alone.
//!
//! - `POST /api/outbox/ci/claim` `{limit?, lease_secs?, wait_secs?}`: up to
//!   `limit` events (200 by default, at most 500), leased for `lease_secs`
//!   (60; 10 to 300), in the order they were written; with `wait_secs` (at
//!   most 30), held until there are some or it is up.
//! - `POST /api/outbox/ci/ack` `{ids}`: those events are done with; one not
//!   acknowledged is handed out again once its lease is up.
//!
//! Only a service (a user in `ci:trigger`) may claim, and each has a sink of
//! its own, named by who it is: two dagron-ci services on one cell, each
//! with its own identity, are each handed every event. Two deployments that
//! share one identity share its sink too, and split its events between them:
//! each is to have an identity of its own. A caller is handed the events of
//! the runs it started (`run_triggers.principal`) and no other's, which are
//! acknowledged for it as they are claimed.
//!
//! A claim waits on the cell's task changes, and looks again at most twice a
//! second ([`WAKE_EVERY`]). Task events older than a day are deleted, a
//! batch at a time, by the claims of the hour ([`prune`]).

use axum::extract::State;
use axum::http::StatusCode;
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::auth::AuthUser;
use crate::state::AppState;

type ApiError = (StatusCode, String);

/// Events one claim hands out at most.
const MAX_LIMIT: i64 = 500;
/// The longest a claim is held for events to come.
const MAX_WAIT_SECS: u64 = 30;
/// The least time between two looks of a waiting claim at the outbox, however
/// often the cell's tasks change.
const WAKE_EVERY: std::time::Duration = std::time::Duration::from_millis(500);
/// How often this process deletes old task events, and how many at a time.
const PRUNE_EVERY_SECS: i64 = 3600;
const PRUNE_ROWS: i64 = 5000;
/// When this process last deleted old task events, in Unix seconds.
static PRUNED_AT: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

#[derive(Debug, Default, Deserialize)]
pub struct ClaimBody {
    pub limit: Option<i64>,
    pub lease_secs: Option<i64>,
    pub wait_secs: Option<u64>,
}

/// One event of the outbox, as a claim hands it out.
#[derive(Debug, Serialize)]
pub struct Notice {
    /// What it is acknowledged by.
    pub id: String,
    pub run_id: String,
    /// `task.started`, `task.finished`, `run.completed`, `run.rerun`,
    /// `run.auto_rerun`.
    #[serde(rename = "type")]
    pub kind: String,
    pub payload: Value,
    /// When the engine wrote it, RFC 3339.
    pub created_at: String,
}

#[derive(Debug, Serialize)]
pub struct Claimed {
    pub events: Vec<Notice>,
}

#[derive(Debug, Deserialize)]
pub struct AckBody {
    pub ids: Vec<String>,
}

/// The caller's sink and the principal its runs' trigger records name: a
/// service's alone.
fn sink_of(claims: &crate::auth::SessionClaims) -> Result<(String, String), ApiError> {
    if !crate::auth::may_set_trigger(claims) {
        return Err((
            StatusCode::FORBIDDEN,
            format!(
                "the outbox is handed to a service, a user in {}",
                crate::auth::TRIGGER_GROUP
            ),
        ));
    }
    let who = crate::routes::control::caller_identity(claims);
    Ok((format!("ci/{who}"), format!("ci:{who}")))
}

/// `POST /api/outbox/ci/claim`.
pub async fn claim(
    AuthUser(claims): AuthUser,
    State(state): State<AppState>,
    body: Option<Json<ClaimBody>>,
) -> Result<Json<Claimed>, ApiError> {
    let (name, principal) = sink_of(&claims)?;
    let body = body.map(|Json(b)| b).unwrap_or_default();
    let limit = body.limit.unwrap_or(200).clamp(1, MAX_LIMIT);
    let lease = body.lease_secs.unwrap_or(60).clamp(10, 300);
    let wait = std::time::Duration::from_secs(body.wait_secs.unwrap_or(0).min(MAX_WAIT_SECS));
    let sink = dagron_core::db::OutboxSink {
        name: &name,
        ..dagron_core::db::CI_SINK
    };
    prune(&state);
    let deadline = tokio::time::Instant::now() + wait;
    // Subscribed before the first claim, so that a change landing between
    // the claim and the wait is not missed.
    let mut changes = state.tx.subscribe();
    loop {
        let rows =
            dagron_core::db::claim_outbox_batch_for_uplink(&state.write_pool, &sink, limit, lease)
                .await
                .map_err(internal)?;
        if !rows.is_empty() {
            let runs: Vec<String> = rows.iter().map(|r| r.event.run_id.clone()).collect();
            let mine: std::collections::HashSet<String> = sqlx::query_scalar(
                "SELECT run_id FROM run_triggers WHERE principal = $1 AND run_id = ANY($2)",
            )
            .bind(&principal)
            .bind(&runs)
            .fetch_all(&state.read_pool)
            .await
            .map_err(|e| internal(e.into()))?
            .into_iter()
            .collect();
            let (events, others): (Vec<_>, Vec<_>) = rows
                .into_iter()
                .partition(|r| mine.contains(&r.event.run_id));
            // Another's runs are none of this caller's: done with, for it.
            let others: Vec<String> = others.into_iter().map(|r| r.event.id).collect();
            if !others.is_empty() {
                acknowledge(&state, &name, &others).await?;
            }
            if !events.is_empty() {
                return Ok(Json(Claimed {
                    events: events
                        .into_iter()
                        .map(|r| Notice {
                            payload: serde_json::from_str(&r.event.payload)
                                .unwrap_or(Value::String(r.event.payload.clone())),
                            id: r.event.id,
                            run_id: r.event.run_id,
                            kind: r.event.event_type,
                            created_at: r.created_at,
                        })
                        .collect(),
                }));
            }
            // A batch of others' alone: look again, within the wait.
            if tokio::time::Instant::now() >= deadline {
                return Ok(Json(Claimed { events: Vec::new() }));
            }
            continue;
        }
        if tokio::time::Instant::now() >= deadline {
            return Ok(Json(Claimed { events: Vec::new() }));
        }
        // Until a task changes anywhere, the stream hiccups, a second goes
        // by (the net for a change this process was not told of), or the
        // wait is up.
        let recheck = std::cmp::min(
            deadline,
            tokio::time::Instant::now() + std::time::Duration::from_secs(1),
        );
        let looked = tokio::time::Instant::now();
        tokio::select! {
            _ = changes.recv() => {}
            _ = tokio::time::sleep_until(recheck) => {}
        }
        // At most twice a second, however many changes come.
        tokio::time::sleep_until(std::cmp::min(deadline, looked + WAKE_EVERY)).await;
    }
}

/// Delete the task events older than a day, in the background, at most once
/// an hour in this process.
fn prune(state: &AppState) {
    use std::sync::atomic::Ordering;
    let now = chrono::Utc::now();
    let last = PRUNED_AT.load(Ordering::Relaxed);
    if now.timestamp() - last < PRUNE_EVERY_SECS
        || PRUNED_AT
            .compare_exchange(last, now.timestamp(), Ordering::Relaxed, Ordering::Relaxed)
            .is_err()
    {
        return;
    }
    let pool = state.write_pool.clone();
    let before = (now - chrono::TimeDelta::seconds(dagron_core::db::OUTBOX_TASK_EVENTS_KEPT_SECS))
        .to_rfc3339();
    tokio::spawn(async move {
        let mut gone = 0;
        // A few batches an hour: a backlog goes over some hours, not in one
        // statement that holds the outbox.
        for _ in 0..10 {
            match dagron_core::db::prune_task_events(&pool, &before, PRUNE_ROWS).await {
                Ok(0) => break,
                Ok(n) => gone += n,
                Err(e) => {
                    tracing::warn!(error = %e, "old task events could not be deleted");
                    break;
                }
            }
        }
        if gone > 0 {
            tracing::info!(gone, "task events older than a day deleted from the outbox");
        }
    });
}

/// `POST /api/outbox/ci/ack`.
pub async fn ack(
    AuthUser(claims): AuthUser,
    State(state): State<AppState>,
    Json(body): Json<AckBody>,
) -> Result<StatusCode, ApiError> {
    let (name, _) = sink_of(&claims)?;
    if body.ids.len() > 10 * MAX_LIMIT as usize {
        return Err((
            StatusCode::BAD_REQUEST,
            format!("at most {} ids at once", 10 * MAX_LIMIT),
        ));
    }
    acknowledge(&state, &name, &body.ids).await?;
    Ok(StatusCode::NO_CONTENT)
}

/// Mark `ids` delivered to `sink`, in one statement.
async fn acknowledge(state: &AppState, sink: &str, ids: &[String]) -> Result<(), ApiError> {
    sqlx::query(
        "UPDATE outbox_deliveries SET status = 'delivered', delivered_at = $1
         WHERE sink = $2 AND outbox_id = ANY($3) AND status = 'pending'",
    )
    .bind(chrono::Utc::now().to_rfc3339())
    .bind(sink)
    .bind(ids)
    .execute(&state.write_pool)
    .await
    .map_err(|e| internal(e.into()))?;
    Ok(())
}

fn internal(e: anyhow::Error) -> ApiError {
    tracing::error!(error = %e, "the outbox could not be read");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "the outbox could not be read".to_string(),
    )
}

/// The outbox handed to a service, against the engine's own schema: the
/// trigger writes a task's start and end once a service claims, it is
/// handed its own runs' events alone and in order, and an acknowledged one
/// is not handed again.
#[cfg(test)]
mod outbox_live_tests {
    use super::*;
    use crate::auth::SessionClaims;
    use crate::routes::control::{submit_run, SubmitBody};
    use crate::routes::ops::redrive_tests::test_state;

    fn service(who: &str) -> AuthUser {
        AuthUser(SessionClaims {
            sub: format!("sub-{who}"),
            email: format!("{who}@example.com"),
            name: String::new(),
            groups: vec![crate::auth::TRIGGER_GROUP.to_string()],
            exp: 0,
        })
    }

    /// A run `who` submits, with a trigger record of its own, of one task.
    async fn submit(state: &AppState, who: &str) -> String {
        let name = format!("outbox-{}", uuid::Uuid::new_v4());
        let body: SubmitBody = serde_json::from_value(serde_json::json!({
            "yaml": format!("name: {name}\ntags: [ci]\ntasks:\n  - name: build\n    command: [\"true\"]\n"),
            "trigger": { "kind": "ci", "forge_host": "github.com", "repo": "acme/widgets", "sha": "a1", "event_id": name },
        }))
        .unwrap();
        let (_, Json(r)) = submit_run(
            service(who),
            State(state.clone()),
            axum::http::HeaderMap::new(),
            Json(body),
        )
        .await
        .unwrap();
        r.run_id
    }

    /// Move the run's one task to `status`, as the engine would.
    async fn task_to(state: &AppState, run: &str, status: &str) {
        sqlx::query("UPDATE task_runs SET status = $1, attempt = attempt + 1 WHERE run_id = $2")
            .bind(status)
            .bind(run)
            .execute(&state.write_pool)
            .await
            .unwrap();
    }

    async fn claimed(state: &AppState, who: &str, wait: u64) -> Vec<Notice> {
        let body = ClaimBody {
            limit: Some(500),
            lease_secs: Some(60),
            wait_secs: Some(wait),
        };
        let Json(c) = claim(service(who), State(state.clone()), Some(Json(body)))
            .await
            .unwrap();
        c.events
    }

    #[tokio::test]
    async fn a_service_is_handed_its_own_runs_changes_in_order_until_it_acknowledges_them() {
        let Some(state) = test_state("outbox-live").await else {
            return;
        };
        let me = format!("ci-{}", uuid::Uuid::new_v4().simple());
        let other = format!("ci-{}", uuid::Uuid::new_v4().simple());
        // The first claim names the sink: from here on, its events are written.
        assert!(claimed(&state, &me, 0).await.is_empty());
        claimed(&state, &other, 0).await;

        let mine = submit(&state, &me).await;
        let theirs = submit(&state, &other).await;
        task_to(&state, &mine, "running").await;
        task_to(&state, &theirs, "running").await;
        task_to(&state, &mine, "succeeded").await;

        let got = claimed(&state, &me, 5).await;
        let kinds: Vec<(&str, &str)> = got
            .iter()
            .map(|n| (n.run_id.as_str(), n.kind.as_str()))
            .collect();
        assert_eq!(
            kinds,
            [
                (mine.as_str(), "task.started"),
                (mine.as_str(), "task.finished")
            ],
            "its own, in order"
        );
        assert_eq!(got[1].payload["name"], "build");
        assert_eq!(got[1].payload["status"], "succeeded");
        assert!(claimed(&state, &me, 0).await.is_empty(), "leased");

        // The other service is handed its own, and none of mine.
        let theirs_got = claimed(&state, &other, 5).await;
        assert!(
            theirs_got.iter().all(|n| n.run_id == theirs),
            "{theirs_got:?}"
        );
        assert_eq!(theirs_got.len(), 1);

        let ids: Vec<String> = got.iter().map(|n| n.id.clone()).collect();
        assert_eq!(
            ack(service(&me), State(state.clone()), Json(AckBody { ids }))
                .await
                .unwrap(),
            StatusCode::NO_CONTENT
        );
        let pending: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM outbox_deliveries WHERE sink = $1 AND status = 'pending'
               AND outbox_id IN (SELECT id FROM event_outbox WHERE run_id = $2)",
        )
        .bind(format!("ci/{me}@example.com"))
        .bind(&mine)
        .fetch_one(&state.write_pool)
        .await
        .unwrap();
        assert_eq!(pending, 0, "acknowledged");

        // A person may not claim.
        let person = AuthUser(SessionClaims {
            sub: "p".into(),
            email: "p@example.com".into(),
            name: String::new(),
            groups: vec!["admin".into()],
            exp: 0,
        });
        let refused = claim(person, State(state.clone()), None).await.unwrap_err();
        assert_eq!(refused.0, StatusCode::FORBIDDEN);
    }
}
