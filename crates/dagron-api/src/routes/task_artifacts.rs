//! A task's door into its run's artifacts, for a task
//! that does not see the store's directory: `GET` and `PUT
//! /task/v1/artifacts/{task}/{name}`, with the credential of its attempt
//! (`Authorization: Bearer $DAGRON_ARTIFACTS_TOKEN`), which the engine, or
//! dagron-api for a remote runner, minted as it dispatched the task
//! ([`dagron_core::artifact_api`]). No session.
//!
//! - A credential is taken while its task runs and it has not expired: `401`
//!   for none, one that is no task's or one past its time, `403` once its
//!   task is not running.
//! - `GET` reads an artifact of the credential's run, of any of its tasks.
//! - `PUT` writes one under the credential's own task's name, and no other
//!   (`403`): no task writes what another's approval binds, nor the digests
//!   an approval publishes under its gate's.
//!
//! What is written is what a person's `PUT /api/runs/{run_id}/artifacts/…`
//! writes, in the same store and index, with the default retention, and
//! within the same body limit (`DAGRON_ARTIFACT_MAX_BYTES`).

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::Response;

use dagron_artifact::ArtifactKey;
use dagron_core::artifact_api::{self, Holder};

use crate::routes::artifacts;
use crate::state::AppState;

type Refusal = (StatusCode, String);

/// The holder of the request's credential, while it may act; or why not.
async fn holder(state: &AppState, headers: &HeaderMap) -> Result<Holder, Refusal> {
    let refused = |code: StatusCode, why: &str| (code, why.to_string());
    let credential = headers
        .get(axum::http::header::AUTHORIZATION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| {
            v.strip_prefix("Bearer ")
                .or_else(|| v.strip_prefix("bearer "))
        })
        .map(str::trim)
        .filter(|v| !v.is_empty())
        .ok_or_else(|| {
            refused(
                StatusCode::UNAUTHORIZED,
                "send the task's credential: Authorization: Bearer $DAGRON_ARTIFACTS_TOKEN",
            )
        })?;
    let found = artifact_api::holder(&state.read_pool, credential)
        .await
        .map_err(|e| {
            tracing::error!(error = %format!("{e:#}"), "a task's artifact credential could not be read");
            refused(StatusCode::INTERNAL_SERVER_ERROR, "internal error")
        })?;
    let Some((holder, status)) = found else {
        return Err(refused(
            StatusCode::UNAUTHORIZED,
            "not the artifact credential of a task",
        ));
    };
    match artifact_api::refusal(&holder, status.as_deref(), chrono::Utc::now()) {
        None => Ok(holder),
        Some(why @ "its task is not running") => Err(refused(StatusCode::FORBIDDEN, why)),
        Some(why) => Err(refused(StatusCode::UNAUTHORIZED, why)),
    }
}

/// `GET /task/v1/artifacts/{task}/{name}`: an artifact of the credential's run.
pub async fn get(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((task, name)): Path<(String, String)>,
) -> Result<Response, Refusal> {
    let holder = holder(&state, &headers).await?;
    artifacts::get_body(&state, ArtifactKey::new(holder.run_id, task, name)).await
}

/// `PUT /task/v1/artifacts/{task}/{name}`: one of the credential's own task.
pub async fn put(
    State(state): State<AppState>,
    headers: HeaderMap,
    Path((task, name)): Path<(String, String)>,
    body: Body,
) -> Result<(StatusCode, String), Refusal> {
    let holder = holder(&state, &headers).await?;
    if task != holder.task_name {
        return Err((
            StatusCode::FORBIDDEN,
            format!(
                "a task writes its own artifacts alone: this credential is task '{}''s, not '{task}''s",
                holder.task_name
            ),
        ));
    }
    tracing::info!(
        run_id = %holder.run_id, task = %holder.task_name, attempt = holder.attempt, %name,
        "a task writes an artifact"
    );
    artifacts::put_body(
        &state,
        ArtifactKey::new(holder.run_id, task, name),
        None,
        body,
    )
    .await
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use axum::body::to_bytes;
    use axum::http::HeaderValue;
    use axum::Json;
    use dagron_artifact::LocalFsStore;
    use sha2::{Digest, Sha256};

    use super::*;
    use crate::auth::{AuthUser, SessionClaims};
    use crate::routes::control;
    use crate::routes::ops::redrive_tests::test_state;

    fn bearer(token: &str) -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert(
            axum::http::header::AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {token}")).unwrap(),
        );
        headers
    }

    async fn task(state: &AppState, run: &str, name: &str) -> dagron_core::models::TaskRun {
        sqlx::query_as("SELECT * FROM task_runs WHERE run_id = $1 AND name = $2")
            .bind(run)
            .bind(name)
            .fetch_one(&state.write_pool)
            .await
            .unwrap()
    }

    async fn set_status(state: &AppState, task: &str, status: &str) {
        sqlx::query("UPDATE task_runs SET status = $1 WHERE id = $2")
            .bind(status)
            .bind(task)
            .execute(&state.write_pool)
            .await
            .unwrap();
    }

    async fn read(
        state: &AppState,
        token: &str,
        task: &str,
        name: &str,
    ) -> Result<Vec<u8>, Refusal> {
        let got = get(
            State(state.clone()),
            bearer(token),
            Path((task.to_string(), name.to_string())),
        )
        .await?;
        Ok(to_bytes(got.into_body(), usize::MAX)
            .await
            .unwrap()
            .to_vec())
    }

    async fn write(
        state: &AppState,
        token: &str,
        task: &str,
        name: &str,
        bytes: &'static [u8],
    ) -> Result<StatusCode, Refusal> {
        put(
            State(state.clone()),
            bearer(token),
            Path((task.to_string(), name.to_string())),
            Body::from(bytes),
        )
        .await
        .map(|(code, _)| code)
    }

    /// The fix loop's bytes, from containers that see no directory of the
    /// store's: `fix` writes its patch through the door, a person approves
    /// those bytes, and `open-pr` reads the digests the approval published
    /// and the patch they name. Each only while it runs, and `fix` alone
    /// writes `fix`'s.
    #[tokio::test]
    async fn a_task_writes_its_own_artifacts_and_reads_its_run_s_while_it_runs() {
        let Some(mut state) = test_state("task-artifacts").await else {
            return;
        };
        let dir =
            std::env::temp_dir().join(format!("dagron-task-artifacts-{}", uuid::Uuid::new_v4()));
        state.artifact_store = Some(std::sync::Arc::new(LocalFsStore::new(&dir)));
        let yaml = format!(
            "name: door-{}\ntasks:\n  - {{ name: fix, command: [\"true\"], artifact_api: true }}\n  \
             - {{ name: review, type: approval, approvers: [\"ops@example.com\"], binds: [\"fix/patch.diff\"], depends_on: [fix] }}\n  \
             - {{ name: open-pr, command: [\"true\"], artifact_api: true, depends_on: [review] }}\n",
            uuid::Uuid::new_v4()
        );
        let run = control::submit_yaml_as(&state, &yaml, &yaml, &BTreeMap::new(), None)
            .await
            .unwrap();

        let fix = task(&state, &run, "fix").await;
        let fix_token = dagron_core::artifact_api::credential(&state.write_pool, &fix, Some(60))
            .await
            .unwrap();
        // Not yet running: refused.
        let e = write(&state, &fix_token, "fix", "patch.diff", b"x")
            .await
            .unwrap_err();
        assert_eq!(e.0, StatusCode::FORBIDDEN, "{e:?}");
        set_status(&state, &fix.id, "running").await;

        let patch: &'static [u8] = b"diff --git a/x b/x\n+fixed\n";
        assert_eq!(
            write(&state, &fix_token, "fix", "patch.diff", patch)
                .await
                .unwrap(),
            StatusCode::CREATED
        );
        assert_eq!(
            read(&state, &fix_token, "fix", "patch.diff").await.unwrap(),
            patch
        );
        // Its own task's alone; and the gate's digests above all.
        for other in ["review", "open-pr"] {
            let e = write(&state, &fix_token, other, "approved.sha256", b"0")
                .await
                .unwrap_err();
            assert_eq!(e.0, StatusCode::FORBIDDEN, "{e:?}");
            assert!(e.1.contains("writes its own artifacts alone"), "{}", e.1);
        }
        assert_eq!(
            read(&state, &fix_token, "fix", "absent")
                .await
                .unwrap_err()
                .0,
            StatusCode::NOT_FOUND
        );
        for bad in ["", "dga_nobody", "dgo_another_kind"] {
            let mut headers = bearer(bad);
            if bad.is_empty() {
                headers.clear();
            }
            let e = get(
                State(state.clone()),
                headers,
                Path(("fix".into(), "patch.diff".into())),
            )
            .await
            .unwrap_err();
            assert_eq!(e.0, StatusCode::UNAUTHORIZED, "{bad}: {e:?}");
        }
        set_status(&state, &fix.id, "succeeded").await;
        assert_eq!(
            read(&state, &fix_token, "fix", "patch.diff")
                .await
                .unwrap_err()
                .0,
            StatusCode::FORBIDDEN,
            "its task is over"
        );

        // A person approves the bytes the task wrote.
        let review = task(&state, &run, "review").await;
        set_status(&state, &review.id, "awaiting_approval").await;
        let digest = format!("{:x}", Sha256::digest(patch));
        let ops = AuthUser(SessionClaims {
            sub: "sub-ops".into(),
            email: "ops@example.com".into(),
            name: String::new(),
            groups: vec![],
            exp: 0,
        });
        let _approved = control::approve_task(
            ops,
            State(state.clone()),
            Path((run.clone(), review.id.clone())),
            Some(Json(control::DecisionBody {
                comment: None,
                digests: [("fix/patch.diff".to_string(), digest.clone())].into(),
            })),
        )
        .await
        .unwrap();

        // `open-pr` reads what was approved, and the patch it names.
        let open = task(&state, &run, "open-pr").await;
        set_status(&state, &open.id, "running").await;
        let open_token = dagron_core::artifact_api::credential(&state.write_pool, &open, None)
            .await
            .unwrap();
        let approved = String::from_utf8(
            read(&state, &open_token, "review", "approved.sha256")
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(approved, format!("{digest}  fix/patch.diff\n"));
        assert_eq!(
            read(&state, &open_token, "fix", "patch.diff")
                .await
                .unwrap(),
            patch
        );
        let _ = std::fs::remove_dir_all(dir);
    }
}
