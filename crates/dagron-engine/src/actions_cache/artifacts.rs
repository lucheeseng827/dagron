//! **GitHub's artifact service**, as `actions/upload-artifact` and
//! `actions/download-artifact` v4 speak it, on the results service's
//! listener and credential.
//!
//! An artifact is its run's, by its name: any job of the run lists it and
//! downloads it, and no job of another run sees it. A run that is not
//! trusted uploads too, as a fork's workflow does on GitHub: what it uploads
//! is its own run's alone.
//!
//! - **Upload** (`CreateArtifact`, the zip put in blocks as Azure's Blob SDK
//!   puts it, `FinalizeArtifact`): kept in the engine's directory until it is
//!   finalized, then put in the store, with its SHA-256, which must be the
//!   one the job says where it says one. A name already in the run is
//!   refused, as GitHub refuses it: `overwrite: true` deletes it first.
//! - **List** (`ListArtifacts`) and **download** (`GetSignedArtifactURL`): a
//!   URL the store signs, so the bytes go from the store to the job. The
//!   digest is listed with it, which the download checks.
//! - **Delete** (`DeleteArtifact`).
//! - **Provenance**: where the engine is given a key, each artifact is
//!   made its run's with its SLSA v1 provenance, signed ([`super::provenance`]).
//!
//! The index is `workflow_artifacts`. An artifact expires when the job asks
//! (`retention-days`), or at the operator's most
//! (`DAGRON_ACTIONS_ARTIFACT_RETENTION_DAYS`, 90 days unset), and the sweep
//! deletes it then, with its bytes; an upload never finalized goes a day on.

use std::time::Instant;

use anyhow::{anyhow, bail, Context, Result};
use axum::http::StatusCode;
use chrono::{DateTime, SecondsFormat, TimeDelta, Utc};
use dagron_artifact::{ArtifactKey, Transfer};
use dagron_core::db;
use dagron_core::models::WorkflowArtifact;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use tokio::io::AsyncReadExt;

use super::{field, number_of, plain, text, ActionsCache, Answer, Claims, Open, Refusal, What};
use super::{DOWNLOAD_SECS, MAX_OPEN_PER_TASK};

pub const ENV_RETENTION_DAYS: &str = "DAGRON_ACTIONS_ARTIFACT_RETENTION_DAYS";
pub const ENV_MAX_BYTES: &str = "DAGRON_ACTIONS_ARTIFACT_MAX_BYTES";

/// Where in the store a run's artifacts are.
const OBJECTS: &str = "workflow-artifacts";
/// The longest an artifact may be kept, in days.
const MAX_RETENTION_DAYS: u32 = 3650;
/// How long an upload's row lasts before it is finalized.
const PENDING: TimeDelta = TimeDelta::days(1);
/// What a sweep deletes at most, each time it runs.
const SWEEP_AT_MOST: i64 = 500;
/// What GitHub refuses in an artifact's name.
const FORBIDDEN: &[char] = &['"', ':', '<', '>', '|', '*', '?', '\\', '/', '\r', '\n'];

/// How long artifacts are kept, and how large one may be.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ArtifactSettings {
    pub retention_days: u32,
    pub max_bytes: u64,
}

impl Default for ArtifactSettings {
    fn default() -> Self {
        Self {
            retention_days: 90,
            max_bytes: 5 << 30,
        }
    }
}

/// The settings, as the environment has them.
pub fn artifact_settings(
    retention_days: Option<&str>,
    max_bytes: Option<&str>,
) -> Result<ArtifactSettings> {
    let mut s = ArtifactSettings::default();
    if let Some(v) = retention_days.map(str::trim).filter(|v| !v.is_empty()) {
        s.retention_days = v
            .parse()
            .ok()
            .filter(|d| (1..=MAX_RETENTION_DAYS).contains(d))
            .ok_or_else(|| {
                anyhow!(
                    "{ENV_RETENTION_DAYS}={v:?} is a number of days from 1 to {MAX_RETENTION_DAYS}"
                )
            })?;
    }
    if let Some(v) = max_bytes.map(str::trim).filter(|v| !v.is_empty()) {
        s.max_bytes = v
            .parse()
            .ok()
            .filter(|b| *b > 0)
            .ok_or_else(|| anyhow!("{ENV_MAX_BYTES}={v:?} is a number of bytes"))?;
    }
    Ok(s)
}

fn said(status: StatusCode, code: &'static str, msg: impl Into<String>) -> Refusal {
    Refusal::Said(status, code, msg.into())
}

/// A name already in the run: refused, as GitHub refuses it.
fn taken(name: &str) -> Refusal {
    said(
        StatusCode::CONFLICT,
        "already_exists",
        format!("an artifact named {name:?} is in this run already"),
    )
}

/// The run a request is for: the credential's, which a request that names
/// another may not reach.
fn run_of<'a>(claims: &'a Claims, body: &Value) -> Result<&'a str, Refusal> {
    let asked = text(body, "workflow_run_backend_id", "workflowRunBackendId");
    if !asked.is_empty() && asked != claims.u {
        return Err(said(
            StatusCode::FORBIDDEN,
            "permission_denied",
            "a job reaches its own run's artifacts alone",
        ));
    }
    Ok(&claims.u)
}

fn named(body: &Value) -> Result<String, Refusal> {
    let name = text(body, "name", "name");
    if !plain(&name) || name.contains(FORBIDDEN) {
        return Err(said(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            "an artifact's name is 1 to 512 characters, none of \" : < > | * ? \\ / or a line's end",
        ));
    }
    Ok(name)
}

/// An artifact's object in the store, from the key its row keeps:
/// `<run>/<id>`.
fn object(object_key: &str) -> Result<ArtifactKey> {
    let Some((run, id)) = object_key.split_once('/') else {
        bail!("an artifact's object key {object_key:?} names no run");
    };
    Ok(ArtifactKey::new(OBJECTS, run, format!("{id}.zip")))
}

/// A time as protobuf's JSON writes one, which the actions read no other way.
fn timestamp(rfc3339: &str) -> String {
    DateTime::parse_from_rfc3339(rfc3339)
        .map(|t| {
            t.with_timezone(&Utc)
                .to_rfc3339_opts(SecondsFormat::Millis, true)
        })
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_string())
}

/// When an artifact the job asked to keep until `asked` expires: then, and no
/// later than the operator's most.
fn expiry(
    settings: &ArtifactSettings,
    asked: Option<&Value>,
    now: DateTime<Utc>,
) -> Result<DateTime<Utc>, Refusal> {
    let most = now + TimeDelta::days(i64::from(settings.retention_days));
    let Some(asked) = asked.and_then(Value::as_str).filter(|a| !a.is_empty()) else {
        return Ok(most);
    };
    let asked = DateTime::parse_from_rfc3339(asked)
        .map_err(|_| {
            said(
                StatusCode::BAD_REQUEST,
                "invalid_argument",
                "expires_at is no time",
            )
        })?
        .with_timezone(&Utc);
    if asked <= now {
        return Err(said(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            "expires_at has passed",
        ));
    }
    Ok(asked.min(most))
}

/// Whether `earlier` was uploaded by the job asking, in an attempt of it
/// before the one that runs now: before this attempt was queued. Such an
/// artifact is the job's to replace. One this attempt uploaded is not (an
/// artifact of version 4 is uploaded once), nor is another job's, nor one
/// whose times cannot be read.
async fn uploaded_before_this_attempt(
    svc: &ActionsCache,
    claims: &Claims,
    earlier: &WorkflowArtifact,
) -> Result<bool, Refusal> {
    if earlier.task_id != claims.t {
        return Ok(false);
    }
    let Some(queued) = db::task_queued_at(&svc.pool, &claims.t).await? else {
        return Ok(false);
    };
    let read = |at: &str| DateTime::parse_from_rfc3339(at).ok();
    Ok(match (read(&earlier.created_at), read(&queued)) {
        (Some(uploaded), Some(queued)) => uploaded < queued,
        _ => false,
    })
}

/// Reserve an upload: the URL the job puts its zip to.
pub(super) async fn create(svc: &ActionsCache, claims: &Claims, body: &Value) -> Answer {
    let run = run_of(claims, body)?.to_string();
    let name = named(body)?;
    let version = field(body, "version", "version").and_then(Value::as_i64);
    if version != Some(4) {
        return Err(said(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            "this service keeps artifacts of version 4, as upload-artifact v4 makes them",
        ));
    }
    let now = Utc::now();
    let expires_at =
        expiry(&svc.artifacts, field(body, "expires_at", "expiresAt"), now)?.to_rfc3339();
    let had = db::workflow_artifacts_of_run(&svc.pool, &run).await?;
    if let Some(earlier) = had.iter().find(|a| a.name == name) {
        if !uploaded_before_this_attempt(svc, claims, earlier).await? {
            return Err(taken(&name));
        }
        // This job's own, from an attempt before a rerun, a clear or a retry
        // queued this one: replaced, as `overwrite: true` would have the job
        // replace it. Else a job that uploads would fail at its upload
        // whenever its run is run again, which is the same engine run.
        svc.caches
            .store()
            .delete(&object(&earlier.object_key)?)
            .await
            .context("an earlier attempt's artifact could not be deleted")?;
        db::workflow_artifact_drop(&svc.pool, &earlier.id).await?;
    }
    let uploading = |o: &Open| matches!(&o.what, What::Artifact { run: r, name: n, .. } if *r == run && *n == name);
    // An upload of the name that an attempt left open, cut short or failed
    // as it was finalized, is not finished: the attempt now asking takes
    // its place, as it would a finished one's that it deleted. Else every
    // attempt of the job is refused the name until the open one lapses.
    let superseded: Vec<String> = svc
        .open
        .lock()
        .unwrap()
        .values()
        .filter(|o| uploading(o))
        .map(|o| o.id.clone())
        .collect();
    for id in &superseded {
        drop_upload(svc, id, None).await;
    }
    let id = uuid::Uuid::new_v4().simple().to_string();
    let artifact_id = number_of(&id);
    let object_key = format!("{run}/{id}");
    {
        let mut open = svc.open.lock().unwrap();
        if open.values().filter(|o| o.task == claims.t).count() >= MAX_OPEN_PER_TASK {
            return Err(said(
                StatusCode::TOO_MANY_REQUESTS,
                "resource_exhausted",
                "this job has too many uploads open",
            ));
        }
        if open.values().any(uploading) {
            return Err(taken(&name));
        }
        open.insert(
            id.clone(),
            Open {
                id: id.clone(),
                task: claims.t.clone(),
                at: Instant::now(),
                max: svc.artifacts.max_bytes,
                what: What::Artifact {
                    run: run.clone(),
                    name: name.clone(),
                    artifact_id,
                    object_key: object_key.clone(),
                    expires_at,
                },
            },
        );
    }
    let begun = db::workflow_artifact_begin(
        &svc.pool,
        &WorkflowArtifact {
            id: id.clone(),
            artifact_id,
            run_id: run,
            task_id: claims.t.clone(),
            name,
            object_key,
            size_bytes: None,
            sha256: None,
            created_at: now.to_rfc3339(),
            expires_at: (now + PENDING).to_rfc3339(),
        },
    )
    .await;
    if let Err(e) = begun {
        svc.open.lock().unwrap().remove(&id);
        return Err(e.into());
    }
    Ok(json!({ "ok": true, "signed_upload_url": svc.upload_url(&id, claims) }))
}

/// The SHA-256 of a file, as `sha256:<hex>`.
async fn digest_of(path: &std::path::Path) -> Result<String> {
    let mut file = tokio::fs::File::open(path).await?;
    let mut sha = Sha256::new();
    let mut buf = vec![0u8; 1 << 16];
    loop {
        let n = file.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        sha.update(&buf[..n]);
    }
    Ok(format!("sha256:{}", super::hex(&sha.finalize())))
}

/// Finalize an upload: its zip put in the store, and the artifact its run's.
pub(super) async fn finalize(svc: &ActionsCache, claims: &Claims, body: &Value) -> Answer {
    let run = run_of(claims, body)?.to_string();
    let name = named(body)?;
    let size: u64 = match field(body, "size", "size") {
        Some(Value::String(s)) => s.parse().unwrap_or(u64::MAX),
        Some(Value::Number(n)) => n.as_u64().unwrap_or(u64::MAX),
        _ => u64::MAX,
    };
    let said_hash = field(body, "hash", "hash")
        .and_then(Value::as_str)
        .filter(|h| !h.is_empty())
        .map(str::to_string);
    let found = svc
        .open
        .lock()
        .unwrap()
        .values()
        .find_map(|o| match &o.what {
            What::Artifact {
                run: r,
                name: n,
                artifact_id,
                object_key,
                expires_at,
            } if o.task == claims.t && *r == run && *n == name => Some((
                o.id.clone(),
                *artifact_id,
                object_key.clone(),
                expires_at.clone(),
            )),
            _ => None,
        });
    let Some((id, artifact_id, object_key, expires_at)) = found else {
        return Err(said(
            StatusCode::NOT_FOUND,
            "not_found",
            "no upload of this artifact is open for this job",
        ));
    };
    let whole = svc.dir.join(&id).join("whole");
    let had = tokio::fs::metadata(&whole).await.map(|m| m.len()).ok();
    if had != Some(size) || size > svc.artifacts.max_bytes {
        drop_upload(svc, &id, None).await;
        return Err(said(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            "the upload is not the size the job says, or is larger than an artifact may be",
        ));
    }
    let digest = digest_of(&whole).await?;
    if said_hash.as_deref().is_some_and(|h| h != digest) {
        drop_upload(svc, &id, None).await;
        return Err(said(
            StatusCode::BAD_REQUEST,
            "invalid_argument",
            "the upload is not what the job says it hashed",
        ));
    }
    let key = object(&object_key)?;
    let file = tokio::fs::File::open(&whole)
        .await
        .map_err(anyhow::Error::from)?;
    if let Err(e) = svc.caches.store().put_stream(&key, Box::new(file)).await {
        // Not left open to block the name: the job's next attempt uploads
        // it again.
        drop_upload(svc, &id, Some(&key)).await;
        return Err(e
            .context("the artifact could not be put in the store")
            .into());
    }
    // Its provenance, where the engine signs one: an artifact whose
    // provenance could not be signed is not made one, and the job is told.
    let provenance = match &svc.provenance {
        None => None,
        Some(p) => match p.sign(&svc.pool, &run, &claims.t, &name, &digest).await {
            Ok(signed) => Some(signed),
            Err(e) => {
                drop_upload(svc, &id, Some(&key)).await;
                return Err(e
                    .context("the artifact's provenance could not be signed")
                    .into());
            }
        },
    };
    let ready = db::workflow_artifact_ready(
        &svc.pool,
        &id,
        i64::try_from(size).unwrap_or(i64::MAX),
        &digest,
        &expires_at,
        provenance.as_deref(),
    )
    .await?;
    if !ready {
        drop_upload(svc, &id, Some(&key)).await;
        return Err(taken(&name));
    }
    svc.forget(&id).await;
    tracing::info!(task = %claims.t, run = %run, name = %name, size, signed = provenance.is_some(), "a workflow artifact was uploaded");
    Ok(json!({ "ok": true, "artifact_id": artifact_id.to_string() }))
}

/// An upload that will not be an artifact: its row, its files, and what of
/// it is in the store.
/// What is in the store goes first: a row whose bytes could not be deleted
/// is kept, still pending, and the sweep deletes them when it expires.
async fn drop_upload(svc: &ActionsCache, id: &str, stored: Option<&ArtifactKey>) {
    svc.forget(id).await;
    if let Some(key) = stored {
        if let Err(e) = svc.caches.store().delete(key).await {
            tracing::warn!(error = %format!("{e:#}"), "an upload's bytes were not deleted: the sweep will");
            return;
        }
    }
    let _ = db::workflow_artifact_drop(&svc.pool, id).await;
}

/// The run's artifacts, by a name or a number where the job asks for one.
pub(super) async fn list(svc: &ActionsCache, claims: &Claims, body: &Value) -> Answer {
    let run = run_of(claims, body)?;
    // A wrapper's JSON is its value; an object of one `value` is read too.
    let wrapped = |proto: &str, json: &str| {
        field(body, proto, json).map(|v| v.get("value").unwrap_or(v).clone())
    };
    let name = wrapped("name_filter", "nameFilter");
    let name = name.as_ref().and_then(Value::as_str);
    let number = wrapped("id_filter", "idFilter").and_then(|v| match v {
        Value::String(s) => s.parse::<i64>().ok(),
        Value::Number(n) => n.as_i64(),
        _ => None,
    });
    let artifacts: Vec<Value> = db::workflow_artifacts_of_run(&svc.pool, run)
        .await?
        .into_iter()
        .filter(|a| name.is_none_or(|n| a.name == n))
        .filter(|a| number.is_none_or(|n| a.artifact_id == n))
        .map(|a| {
            let mut listed = json!({
                "workflow_run_backend_id": a.run_id,
                "workflow_job_run_backend_id": a.task_id,
                "database_id": a.artifact_id.to_string(),
                "name": a.name,
                "size": a.size_bytes.unwrap_or(0).to_string(),
                "created_at": timestamp(&a.created_at),
            });
            if let Some(sha) = a.sha256 {
                listed["digest"] = json!(sha);
            }
            listed
        })
        .collect();
    Ok(json!({ "artifacts": artifacts }))
}

/// The run's artifact of a name.
async fn by_name(
    svc: &ActionsCache,
    claims: &Claims,
    body: &Value,
) -> Result<WorkflowArtifact, Refusal> {
    let run = run_of(claims, body)?;
    let name = named(body)?;
    db::workflow_artifacts_of_run(&svc.pool, run)
        .await?
        .into_iter()
        .find(|a| a.name == name)
        .ok_or_else(|| {
            said(
                StatusCode::NOT_FOUND,
                "not_found",
                format!("no artifact named {name:?} is in this run"),
            )
        })
}

/// A URL the store signed for the run's artifact of a name.
pub(super) async fn signed_url(svc: &ActionsCache, claims: &Claims, body: &Value) -> Answer {
    let artifact = by_name(svc, claims, body).await?;
    let signed = svc
        .caches
        .sign(&object(&artifact.object_key)?, Transfer::Get, DOWNLOAD_SECS)
        .await?;
    Ok(json!({ "signed_url": signed.url }))
}

/// Delete the run's artifact of a name, and its bytes. The bytes go first:
/// where the store does not delete them, the row is kept, the artifact is
/// still listed, and the job is told it failed, so that it can ask again.
pub(super) async fn delete(svc: &ActionsCache, claims: &Claims, body: &Value) -> Answer {
    let artifact = by_name(svc, claims, body).await?;
    svc.caches
        .store()
        .delete(&object(&artifact.object_key)?)
        .await
        .context("a workflow artifact's bytes could not be deleted")?;
    db::workflow_artifact_drop(&svc.pool, &artifact.id).await?;
    Ok(json!({ "ok": true, "artifact_id": artifact.artifact_id.to_string() }))
}

/// Delete what has expired, with its bytes: how many went. The bytes go
/// first, and the row only once they have: one the store would not delete
/// is kept, and the next sweep tries it again.
pub(super) async fn sweep(svc: &ActionsCache) -> Result<usize> {
    let now = Utc::now().to_rfc3339();
    let mut gone = 0;
    for (id, object_key) in db::workflow_artifacts_expired(&svc.pool, &now, SWEEP_AT_MOST).await? {
        if let Err(e) = svc.caches.store().delete(&object(&object_key)?).await {
            tracing::warn!(
                error = %format!("{e:#}"),
                "an expired workflow artifact's bytes were not deleted: the next sweep tries again"
            );
            continue;
        }
        if db::workflow_artifact_drop(&svc.pool, &id).await?.is_some() {
            gone += 1;
        }
    }
    Ok(gone)
}
