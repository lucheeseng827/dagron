//! A task's way into its run's artifacts over dagron-api's API, for a task
//! that does not see the store's directory: one in a container, a pod, or on
//! a remote runner.
//!
//! A task that asks for it (`artifact_api: true`) is given, where it is
//! dispatched, [`ENV_API`], dagron-api's [`PATH`] as the task reaches it, and
//! [`ENV_TOKEN`], a credential of its attempt's own. With it, while the task
//! runs:
//!
//! - `GET {api}/{task}/{name}` reads an artifact of the task's run, of any of
//!   its tasks;
//! - `PUT {api}/{task}/{name}` writes one, under the task's own name alone, so
//!   that no task writes what another's approval binds.
//!
//! The bytes are dagron-api's store's, the one an approval's `binds` is
//! checked against, so the engine and dagron-api need share no directory.
//!
//! The credential is random, and only its SHA-256 is kept
//! (`artifact_api_tokens`), in place of the one an earlier attempt of the task
//! had. It is good until the task's timeout and fifteen minutes, or six hours
//! for a task with none, and dagron-api takes it only while its task runs.

use anyhow::Result;

/// The URL a task is given: dagron-api's [`PATH`], as the task reaches it.
pub const ENV_API: &str = "DAGRON_ARTIFACTS_API";
/// The credential a task is given, sent as a bearer token.
pub const ENV_TOKEN: &str = "DAGRON_ARTIFACTS_TOKEN";
/// Where dagron-api serves it.
pub const PATH: &str = "/task/v1/artifacts";
/// What every credential starts with.
pub const TOKEN_PREFIX: &str = "dga_";

/// Whose a credential is: an attempt of a task, of a run.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Holder {
    pub run_id: String,
    pub task_id: String,
    pub task_name: String,
    pub attempt: i64,
    pub expires_at: String,
}

/// A credential for the attempt that is dispatched of `task`, the claimed
/// row, whose attempt is the one before it. Minted where the task is
/// dispatched: by the engine that claimed it, or by dagron-api for a remote
/// runner's session.
pub async fn credential(
    pool: &crate::db::Pool,
    task: &crate::models::TaskRun,
    timeout_secs: Option<u64>,
) -> Result<String> {
    let token = format!(
        "{TOKEN_PREFIX}{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let secs = timeout_secs
        .map_or(6 * 3600, |t| t.saturating_add(900))
        .min(7 * 24 * 3600);
    let now = chrono::Utc::now();
    let expires_at = now + chrono::Duration::seconds(i64::try_from(secs).unwrap_or(i64::MAX));
    let now = now.to_rfc3339();
    let mut tx = pool.begin().await?;
    sqlx::query("DELETE FROM artifact_api_tokens WHERE task_id = $1 OR expires_at < $2")
        .bind(&task.id)
        .bind(&now)
        .execute(&mut *tx)
        .await?;
    sqlx::query(
        "INSERT INTO artifact_api_tokens
           (token_sha256, run_id, task_id, task_name, attempt, expires_at, created_at)
         VALUES ($1, $2, $3, $4, $5, $6, $7)",
    )
    .bind(dagron_crypto::bundle::sha256_hex(token.as_bytes()))
    .bind(&task.run_id)
    .bind(&task.id)
    .bind(&task.name)
    .bind(task.attempt.saturating_add(1))
    .bind(expires_at.to_rfc3339())
    .bind(&now)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(token)
}

/// Whose `token` is, with its task's status now (`None` when the task's row
/// is gone); `None` for a token that is no task's.
pub async fn holder(
    pool: &crate::db::Pool,
    token: &str,
) -> Result<Option<(Holder, Option<String>)>> {
    if !token.starts_with(TOKEN_PREFIX) {
        return Ok(None);
    }
    #[allow(clippy::type_complexity)]
    let row: Option<(String, String, String, i64, String, Option<String>)> = sqlx::query_as(
        "SELECT a.run_id, a.task_id, a.task_name, a.attempt, a.expires_at, t.status
           FROM artifact_api_tokens a LEFT JOIN task_runs t ON t.id = a.task_id
          WHERE a.token_sha256 = $1",
    )
    .bind(dagron_crypto::bundle::sha256_hex(token.as_bytes()))
    .fetch_optional(pool)
    .await?;
    Ok(row.map(
        |(run_id, task_id, task_name, attempt, expires_at, status)| {
            (
                Holder {
                    run_id,
                    task_id,
                    task_name,
                    attempt,
                    expires_at,
                },
                status,
            )
        },
    ))
}

/// Why a holder may not do what it asks now, or `None`: its credential is
/// past its time, or its task is not running.
pub fn refusal(
    holder: &Holder,
    status: Option<&str>,
    now: chrono::DateTime<chrono::Utc>,
) -> Option<&'static str> {
    let expired = chrono::DateTime::parse_from_rfc3339(&holder.expires_at)
        .map_or(true, |at| at.with_timezone(&chrono::Utc) <= now);
    if expired {
        Some("the credential is past its time")
    } else if status != Some("running") {
        Some("its task is not running")
    } else {
        None
    }
}

#[cfg(all(test, feature = "sqlite"))]
mod tests {
    use super::*;
    use crate::dag::DagGraph;

    async fn pool() -> crate::db::Pool {
        let path =
            std::env::temp_dir().join(format!("m54_artifact_api_{}.db", uuid::Uuid::new_v4()));
        crate::db::init_pool(path.to_str().unwrap()).await.unwrap()
    }

    /// A credential names its task's attempt, while the task runs; the next
    /// attempt's is in its place, and the first is no one's.
    #[tokio::test]
    async fn a_credential_is_its_attempt_s_and_its_task_s_while_it_runs() {
        let pool = pool().await;
        let yaml = "name: a\ntasks:\n  - { name: fix, command: [\"true\"], artifact_api: true }\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        assert!(dag.spec.tasks[0].artifact_api);
        let run = crate::db::create_run(&pool, &dag, yaml).await.unwrap();
        let mut task: crate::models::TaskRun =
            sqlx::query_as("SELECT * FROM task_runs WHERE run_id = $1")
                .bind(&run)
                .fetch_one(&pool)
                .await
                .unwrap();
        let first = credential(&pool, &task, Some(60)).await.unwrap();
        assert!(first.starts_with(TOKEN_PREFIX) && first.len() > 60);
        let (who, status) = holder(&pool, &first).await.unwrap().unwrap();
        assert_eq!(
            (who.run_id.as_str(), who.task_name.as_str(), who.attempt),
            (run.as_str(), "fix", 1)
        );
        let now = chrono::Utc::now();
        assert_eq!(
            refusal(&who, status.as_deref(), now),
            Some("its task is not running")
        );
        assert_eq!(refusal(&who, Some("running"), now), None);
        assert_eq!(
            refusal(&who, Some("running"), now + chrono::Duration::seconds(961)),
            Some("the credential is past its time")
        );

        task.attempt = 1;
        let second = credential(&pool, &task, None).await.unwrap();
        assert_eq!(
            holder(&pool, &first).await.unwrap(),
            None,
            "the next attempt's is in its place"
        );
        assert_eq!(holder(&pool, &second).await.unwrap().unwrap().0.attempt, 2);
        assert_eq!(holder(&pool, "dgo_not_this_kind").await.unwrap(), None);
    }
}
