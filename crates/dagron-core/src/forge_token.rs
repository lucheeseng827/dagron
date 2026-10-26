//! **A forge token**: a credential a task is given to call its repository's
//! forge with, as GitHub gives a workflow's job its `GITHUB_TOKEN`.
//!
//! ```yaml
//! forge_token: { permissions: { contents: read, pull_requests: write } }
//! ```
//!
//! The engine asks the operator's token service (`DAGRON_CHECKOUT_TOKEN_URL`,
//! the one a checkout's credential comes from) for it as each attempt
//! starts, for the repository of the task's checkout, with these
//! permissions. The service decides what it gives: it may give fewer, as
//! dagron-ci gives a run that is not trusted read alone, or none. The task
//! has it as `GITHUB_TOKEN`, masked in its log, and it is revoked once the
//! attempt is over.

use std::collections::BTreeMap;

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};

/// The permissions a forge token may be asked for: a GitHub App's names for
/// a repository's, which a workflow's `permissions:` names with `-` for `_`.
pub const PERMISSIONS: &[&str] = &[
    "actions",
    "checks",
    "contents",
    "deployments",
    "discussions",
    "issues",
    "metadata",
    "pages",
    "pull_requests",
    "security_events",
    "statuses",
];

/// `forge_token:` on a task.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ForgeTokenSpec {
    /// Each permission of [`PERMISSIONS`] asked for: `read` or `write`.
    pub permissions: BTreeMap<String, String>,
}

impl ForgeTokenSpec {
    pub fn validate(&self) -> Result<()> {
        if self.permissions.is_empty() {
            bail!("forge_token names no permissions: name what the token may do, contents: read");
        }
        for (name, level) in &self.permissions {
            if !PERMISSIONS.contains(&name.as_str()) {
                bail!(
                    "forge_token.permissions names {name:?}, and the permissions are {}",
                    PERMISSIONS.join(", ")
                );
            }
            if level != "read" && level != "write" {
                bail!("forge_token.permissions.{name} is {level:?}: read or write");
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_forge_token_names_permissions_a_forge_has_at_read_or_write() {
        let spec = |v: serde_json::Value| -> ForgeTokenSpec { serde_json::from_value(v).unwrap() };
        assert!(spec(
            serde_json::json!({"permissions": {"contents": "read", "pull_requests": "write"}})
        )
        .validate()
        .is_ok());
        for (v, why) in [
            (
                serde_json::json!({"permissions": {}}),
                "names no permissions",
            ),
            (
                serde_json::json!({"permissions": {"administration": "write"}}),
                "and the permissions are",
            ),
            (
                serde_json::json!({"permissions": {"contents": "admin"}}),
                "read or write",
            ),
            (
                serde_json::json!({"permissions": {"pull-requests": "read"}}),
                "and the permissions are",
            ),
        ] {
            let err = spec(v.clone()).validate().unwrap_err().to_string();
            assert!(err.contains(why), "{v}: {err}");
        }
        assert!(
            serde_json::from_value::<ForgeTokenSpec>(serde_json::json!({"scopes": []})).is_err()
        );
    }
}
