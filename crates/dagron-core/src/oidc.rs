//! **A task's identity**: `identity:{ oidc: { audience } }`
//! asks for OpenID Connect tokens that name the run, for a cloud's federation
//! to trade for credentials, so the task holds no secret of the cloud's.
//!
//! The engine gives such a task a request credential of its own
//! (`DAGRON_OIDC_REQUEST_TOKEN`, good while the task runs) and where to use it
//! (`DAGRON_OIDC_REQUEST_URL`). dagron-api's issuer (`DAGRON_OIDC_ISSUER`) signs
//! a token for one of the task's audiences, short-lived, whose claims carry
//! the repository, ref, commit, pipeline, run, environment and trust class of
//! the run's record, so that a trust policy can say which runs may assume a
//! role. A run that is not trusted is given none: its task is refused
//! ([`crate::confine::task_refusal`]).

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

/// At most this many audiences a task.
pub const MAX_AUDIENCES: usize = 8;

/// `identity:` on a task.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct TaskIdentity {
    /// OpenID Connect tokens for the audiences named.
    pub oidc: OidcIdentity,
}

/// `identity.oidc`: the audiences a token may be asked for, one or a list
/// (`sts.amazonaws.com`, a GCP workload identity provider's URL). The first
/// is the one a request that names none gets.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct OidcIdentity {
    #[serde(deserialize_with = "one_or_many")]
    pub audience: Vec<String>,
}

fn one_or_many<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> std::result::Result<Vec<String>, D::Error> {
    #[derive(Deserialize)]
    #[serde(untagged)]
    enum OneOrMany {
        One(String),
        Many(Vec<String>),
    }
    Ok(match OneOrMany::deserialize(d)? {
        OneOrMany::One(one) => vec![one],
        OneOrMany::Many(many) => many,
    })
}

impl TaskIdentity {
    pub fn validate(&self) -> Result<()> {
        let audiences = &self.oidc.audience;
        if audiences.is_empty() {
            bail!("identity.oidc.audience names no audience");
        }
        if audiences.len() > MAX_AUDIENCES {
            bail!(
                "identity.oidc.audience names {} audiences; at most {MAX_AUDIENCES}",
                audiences.len()
            );
        }
        for audience in audiences {
            if audience.is_empty()
                || audience.len() > 256
                || audience.contains("{{")
                || audience
                    .chars()
                    .any(|c| c.is_whitespace() || c.is_control())
            {
                bail!("identity.oidc.audience {audience:?} is not an audience: up to 256 characters, none a space");
            }
        }
        Ok(())
    }

    /// The audience a request that names none gets.
    pub fn first_audience(&self) -> &str {
        self.oidc
            .audience
            .first()
            .map(String::as_str)
            .unwrap_or_default()
    }
}

/// A task's request credential for its identity: random, of which only the
/// SHA-256 is kept, in place of the one an earlier attempt of the task had.
/// Good until the task's timeout and fifteen minutes, or six hours for a task
/// with none; dagron-api signs nothing for it once the task is not running.
/// `task` is the claimed row, whose attempt is the one before this.
///
/// Minted where the task is dispatched: by the engine that claimed it, or by
/// dagron-api for a remote runner's session.
pub async fn request_credential(
    pool: &crate::db::Pool,
    task: &crate::models::TaskRun,
    identity: &TaskIdentity,
    environment: Option<String>,
    timeout_secs: Option<u64>,
) -> Result<String> {
    let token = format!(
        "dgo_{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    let secs = timeout_secs
        .map_or(6 * 3600, |t| t.saturating_add(900))
        .min(7 * 24 * 3600);
    let expires_at =
        chrono::Utc::now() + chrono::Duration::seconds(i64::try_from(secs).unwrap_or(i64::MAX));
    crate::db::oidc_request_offered(
        pool,
        &crate::models::OidcRequest {
            token_sha256: dagron_crypto::bundle::sha256_hex(token.as_bytes()),
            run_id: task.run_id.clone(),
            task_id: task.id.clone(),
            task_name: task.name.clone(),
            attempt: task.attempt.saturating_add(1),
            audiences: identity.oidc.audience.clone(),
            environment,
            expires_at: expires_at.to_rfc3339(),
        },
    )
    .await?;
    Ok(token)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_identity_names_one_audience_or_several() {
        let one: TaskIdentity =
            serde_json::from_value(serde_json::json!({"oidc": {"audience": "sts.amazonaws.com"}}))
                .unwrap();
        assert_eq!(one.oidc.audience, vec!["sts.amazonaws.com"]);
        assert!(one.validate().is_ok());
        assert_eq!(one.first_audience(), "sts.amazonaws.com");
        let many: TaskIdentity = serde_json::from_value(
            serde_json::json!({"oidc": {"audience": ["a", "https://iam.googleapis.com/x"]}}),
        )
        .unwrap();
        assert_eq!(many.oidc.audience.len(), 2);
        for (v, why) in [
            (
                serde_json::json!({"oidc": {"audience": []}}),
                "names no audience",
            ),
            (
                serde_json::json!({"oidc": {"audience": "a b"}}),
                "is not an audience",
            ),
            (
                serde_json::json!({"oidc": {"audience": ""}}),
                "is not an audience",
            ),
        ] {
            let id: TaskIdentity = serde_json::from_value(v).unwrap();
            assert!(id.validate().unwrap_err().to_string().contains(why));
        }
        let err = serde_json::from_value::<TaskIdentity>(
            serde_json::json!({"oidc": {"audience": "a", "scope": "x"}}),
        )
        .unwrap_err()
        .to_string();
        assert!(err.contains("unknown field `scope`"), "{err}");
        assert!(serde_json::from_value::<TaskIdentity>(serde_json::json!({"aws": {}})).is_err());
    }
}
