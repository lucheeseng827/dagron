//! A run's **workflow artifacts**: what a GitHub workflow's jobs uploaded
//! with `actions/upload-artifact`, which the engine's results service keeps
//! (`workflow_artifacts`, its bytes in the cache store, `DAGRON_CACHE_URL`),
//! and not dagron's own artifacts (`/api/runs/{run_id}/artifacts`).
//!
//! - `GET /api/runs/{run_id}/workflow-artifacts`: those complete, by name,
//!   with their size, SHA-256 and when they expire.
//! - `GET /api/runs/{run_id}/workflow-artifacts/{name}`: one's bytes, the
//!   zip its job uploaded, streamed from the store as GitHub serves one.
//! - `GET /api/runs/{run_id}/workflow-artifacts/{name}/provenance`: its SLSA
//!   v1 provenance, signed by the engine as the artifact was finalized:
//!   a DSSE envelope, which `cosign verify-blob-attestation --type
//!   slsaprovenance1` checks against the zip with the operator's public key.
//!   Listed as `provenance: true` where there is one.
//! - `GET /api/workflow-artifacts/{id}`: one complete artifact by the number
//!   its job knows it by, with its run: what dagron-ci's GitHub API door
//!   asks, as GitHub names an artifact by that number alone.
//!
//! Each needs what reading the run does. The bytes are read from the store
//! the engine writes them to, which dagron-api is given the same way
//! (`DAGRON_CACHE_URL`, and the store's own variables); without it, a
//! listing is still had and a download says why there is none.

use axum::body::Body;
use axum::extract::{Path, State};
use axum::http::{header, StatusCode};
use axum::response::Response;
use axum::Json;
use serde_json::json;
use sqlx::Row;

use crate::auth::AuthUser;
use crate::state::AppState;

/// Where the engine keeps a workflow artifact's bytes, under the cache
/// store's own root: `workflow-artifacts/<run>/<number>.zip`.
const OBJECTS: &str = "workflow-artifacts";

type Refusal = (StatusCode, String);

fn internal(what: &str, e: impl std::fmt::Display) -> Refusal {
    tracing::error!(error = %e, "{what}");
    (
        StatusCode::INTERNAL_SERVER_ERROR,
        "internal error".to_string(),
    )
}

/// One complete artifact of the run, as the listing says it.
fn listed(row: &sqlx::postgres::PgRow) -> Result<serde_json::Value, sqlx::Error> {
    Ok(json!({
        "id": row.try_get::<i64, _>("artifact_id")?,
        "name": row.try_get::<String, _>("name")?,
        "size_bytes": row.try_get::<Option<i64>, _>("size_bytes")?,
        "sha256": row.try_get::<Option<String>, _>("sha256")?,
        "created_at": row.try_get::<String, _>("created_at")?,
        "expires_at": row.try_get::<String, _>("expires_at")?,
        "provenance": row.try_get::<bool, _>("signed")?,
    }))
}

/// `GET /api/runs/{run_id}/workflow-artifacts`: the run's complete workflow
/// artifacts, by name.
pub async fn list(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(run_id): Path<String>,
) -> Result<Json<serde_json::Value>, Refusal> {
    let rows = sqlx::query(
        "SELECT artifact_id, name, size_bytes, sha256, created_at, expires_at,
                provenance IS NOT NULL AS signed
           FROM workflow_artifacts
          WHERE run_id = $1 AND state = 'ready'
          ORDER BY name",
    )
    .bind(&run_id)
    .fetch_all(&state.read_pool)
    .await
    .map_err(|e| internal("workflow artifacts: list failed", e))?;
    let artifacts = rows
        .iter()
        .map(listed)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|e| internal("workflow artifacts: a row could not be read", e))?;
    Ok(Json(json!({ "run_id": run_id, "artifacts": artifacts })))
}

/// `GET /api/workflow-artifacts/{id}`: the complete artifact numbered `id`,
/// with its run.
pub async fn by_number(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path(id): Path<i64>,
) -> Result<Json<serde_json::Value>, Refusal> {
    let row = sqlx::query(
        "SELECT run_id, artifact_id, name, size_bytes, sha256, created_at, expires_at,
                provenance IS NOT NULL AS signed
           FROM workflow_artifacts
          WHERE artifact_id = $1 AND state = 'ready'",
    )
    .bind(id)
    .fetch_optional(&state.read_pool)
    .await
    .map_err(|e| internal("workflow artifacts: lookup failed", e))?
    .ok_or((StatusCode::NOT_FOUND, format!("no workflow artifact {id}")))?;
    let mut artifact =
        listed(&row).map_err(|e| internal("workflow artifacts: a row could not be read", e))?;
    artifact["run_id"] = json!(row
        .try_get::<String, _>("run_id")
        .map_err(|e| internal("workflow artifacts: a row could not be read", e))?);
    Ok(Json(artifact))
}

/// The run's complete artifact of `name`: its number and where its bytes
/// are, as the engine keyed them (`<run>/<number>`).
pub async fn artifact_of(
    state: &AppState,
    run_id: &str,
    name: &str,
) -> Result<Option<(i64, String, Option<i64>)>, Refusal> {
    let row = sqlx::query(
        "SELECT artifact_id, object_key, size_bytes
           FROM workflow_artifacts
          WHERE run_id = $1 AND name = $2 AND state = 'ready'",
    )
    .bind(run_id)
    .bind(name)
    .fetch_optional(&state.read_pool)
    .await
    .map_err(|e| internal("workflow artifacts: lookup failed", e))?;
    row.map(|row| {
        Ok((
            row.try_get("artifact_id")?,
            row.try_get("object_key")?,
            row.try_get("size_bytes")?,
        ))
    })
    .transpose()
    .map_err(|e: sqlx::Error| internal("workflow artifacts: a row could not be read", e))
}

/// `name`, as a file's name: what is not plain is `_`.
fn file_name(name: &str) -> String {
    name.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect()
}

/// `GET /api/runs/{run_id}/workflow-artifacts/{name}/provenance`: the
/// artifact's signed provenance, the DSSE envelope the engine kept with it,
/// as `cosign verify-blob-attestation --signature` reads one. Not found where
/// the artifact is none, or was finalized by an engine given no key.
pub async fn provenance(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path((run_id, name)): Path<(String, String)>,
) -> Result<Response, Refusal> {
    let kept: Option<Option<String>> = sqlx::query_scalar(
        "SELECT provenance
           FROM workflow_artifacts
          WHERE run_id = $1 AND name = $2 AND state = 'ready'",
    )
    .bind(&run_id)
    .bind(&name)
    .fetch_optional(&state.read_pool)
    .await
    .map_err(|e| internal("workflow artifacts: lookup failed", e))?;
    let envelope = match kept {
        None => {
            return Err((
                StatusCode::NOT_FOUND,
                format!("run {run_id} has no workflow artifact named {name:?}"),
            ))
        }
        Some(None) => {
            return Err((
                StatusCode::NOT_FOUND,
                format!(
                    "the workflow artifact {name:?} of run {run_id} was kept without provenance: \
                     its engine was given no key to sign with (DAGRON_PROVENANCE_KEY)"
                ),
            ))
        }
        Some(Some(envelope)) => envelope,
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/json")
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{}.intoto.json\"", file_name(&name)),
        )
        .body(Body::from(envelope))
        .map_err(|e| internal("workflow artifacts: the answer could not be made", e))
}

/// `GET /api/runs/{run_id}/workflow-artifacts/{name}`: the artifact's zip,
/// streamed from the cache store.
pub async fn download(
    _auth: AuthUser,
    State(state): State<AppState>,
    Path((run_id, name)): Path<(String, String)>,
) -> Result<Response, Refusal> {
    let Some((_, object_key, size)) = artifact_of(&state, &run_id, &name).await? else {
        return Err((
            StatusCode::NOT_FOUND,
            format!("run {run_id} has no workflow artifact named {name:?}"),
        ));
    };
    let body = bytes_of(&object_key).await?;
    let file = file_name(&name);
    let mut response = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/zip")
        .header(
            header::CONTENT_DISPOSITION,
            format!("attachment; filename=\"{file}.zip\""),
        );
    if let Some(size) = size {
        response = response.header(header::CONTENT_LENGTH, size);
    }
    response
        .body(body)
        .map_err(|e| internal("workflow artifacts: the answer could not be made", e))
}

/// The bytes of the artifact the engine keyed `object_key` (`<run>/<number>`),
/// streamed from the cache store.
pub async fn bytes_of(object_key: &str) -> Result<Body, Refusal> {
    let Some((run, number)) = object_key.split_once('/') else {
        return Err(internal(
            "workflow artifacts: an object key names no run",
            object_key,
        ));
    };
    let url = std::env::var("DAGRON_CACHE_URL")
        .ok()
        .filter(|u| !u.trim().is_empty())
        .ok_or((
            StatusCode::SERVICE_UNAVAILABLE,
            "a workflow artifact's bytes are in the cache store, and this dagron-api is given \
             none (DAGRON_CACHE_URL, as the engine's)"
                .to_string(),
        ))?;
    stream(&url, run, number).await
}

#[cfg(feature = "archive-cloud")]
async fn stream(url: &str, run: &str, number: &str) -> Result<Body, Refusal> {
    use dagron_artifact::sanitize_component;
    use futures::TryStreamExt;
    use object_store::ObjectStore;

    let (store, prefix) = crate::objstore::from_url(url)
        .map_err(|e| internal("workflow artifacts: the cache store could not be opened", e))?;
    let path = prefix
        .child(OBJECTS)
        .child(sanitize_component(run))
        .child(sanitize_component(&format!("{number}.zip")));
    let got = store.get(&path).await.map_err(|e| match e {
        object_store::Error::NotFound { .. } => (
            StatusCode::NOT_FOUND,
            "the artifact's bytes are no longer in the store".to_string(),
        ),
        e => internal("workflow artifacts: the store would not give the bytes", e),
    })?;
    Ok(Body::from_stream(
        got.into_stream().map_err(std::io::Error::other),
    ))
}

#[cfg(not(feature = "archive-cloud"))]
async fn stream(_url: &str, _run: &str, _number: &str) -> Result<Body, Refusal> {
    let _ = OBJECTS;
    Err((
        StatusCode::SERVICE_UNAVAILABLE,
        "this dagron-api was built without a cloud store (archive-s3, archive-gcs or \
         archive-azure), and a workflow artifact's bytes are in one"
            .to_string(),
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::auth::SessionClaims;
    use crate::routes::ops::redrive_tests::test_state;

    fn user() -> AuthUser {
        AuthUser(SessionClaims {
            sub: "sub-wa".to_string(),
            email: "wa@example.com".to_string(),
            name: String::new(),
            groups: vec![],
            exp: 0,
        })
    }

    /// A run's complete workflow artifacts are listed by name, and one
    /// still being uploaded is not; one the run has not is not found.
    #[tokio::test]
    async fn a_run_s_workflow_artifacts_are_listed_complete_by_name() {
        let Some(state) = test_state("workflow-artifacts").await else {
            return;
        };
        let run = format!("run-wa-{}", uuid::Uuid::new_v4());
        let number = |n: i64| n + (uuid::Uuid::new_v4().as_u128() as i64 & 0x0fff_ffff) * 16;
        for (name, state_of, n) in [
            ("built", "ready", 1),
            ("logs", "ready", 2),
            ("half", "pending", 3),
        ] {
            sqlx::query(
                "INSERT INTO workflow_artifacts
                   (id, artifact_id, run_id, task_id, name, object_key, size_bytes, sha256,
                    state, created_at, expires_at, provenance)
                 VALUES ($1, $2, $3, 't', $4, $5, 12, 'ab', $6,
                         '2026-10-17T00:00:00Z', '2026-10-19T00:00:00Z', $7)",
            )
            .bind(uuid::Uuid::new_v4().to_string())
            .bind(number(n))
            .bind(&run)
            .bind(name)
            .bind(format!("{run}/{n}"))
            .bind(state_of)
            .bind((name == "built").then_some(r#"{"payloadType":"application/vnd.in-toto+json"}"#))
            .execute(&state.read_pool)
            .await
            .unwrap();
        }
        let Json(listed) = list(user(), State(state.clone()), Path(run.clone()))
            .await
            .unwrap();
        let names: Vec<&str> = listed["artifacts"]
            .as_array()
            .unwrap()
            .iter()
            .map(|a| a["name"].as_str().unwrap())
            .collect();
        assert_eq!(names, ["built", "logs"], "{listed}");
        assert_eq!(listed["artifacts"][0]["size_bytes"], 12);
        assert_eq!(listed["artifacts"][0]["expires_at"], "2026-10-19T00:00:00Z");
        // Its provenance, where the engine signed one, and not where not.
        assert_eq!(listed["artifacts"][0]["provenance"], true);
        assert_eq!(listed["artifacts"][1]["provenance"], false);
        let signed = provenance(
            user(),
            State(state.clone()),
            Path((run.clone(), "built".to_string())),
        )
        .await
        .unwrap();
        assert_eq!(signed.status(), StatusCode::OK);
        assert_eq!(
            signed.headers()[header::CONTENT_DISPOSITION],
            "attachment; filename=\"built.intoto.json\""
        );
        let body = axum::body::to_bytes(signed.into_body(), 1 << 16)
            .await
            .unwrap();
        assert_eq!(
            &body[..],
            br#"{"payloadType":"application/vnd.in-toto+json"}"#
        );
        for (name, says) in [
            ("logs", "without provenance"),
            ("half", "no workflow artifact"),
        ] {
            let none = provenance(
                user(),
                State(state.clone()),
                Path((run.clone(), name.into())),
            )
            .await
            .unwrap_err();
            assert_eq!(none.0, StatusCode::NOT_FOUND);
            assert!(none.1.contains(says), "{name}: {}", none.1);
        }

        let missing = download(
            user(),
            State(state.clone()),
            Path((run.clone(), "half".to_string())),
        )
        .await
        .unwrap_err();
        assert_eq!(
            missing.0,
            StatusCode::NOT_FOUND,
            "one still uploading is none"
        );
        sqlx::query("DELETE FROM workflow_artifacts WHERE run_id = $1")
            .bind(&run)
            .execute(&state.read_pool)
            .await
            .unwrap();
    }
}
