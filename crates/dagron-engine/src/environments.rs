//! Environment integration for the engine's run-creation and dispatch paths.
//!
//! A spec that declares `environment: <name>` gets two things:
//! * its variables merged into the templating scope as `{{ env.NAME }}` keys
//!   at run creation ([`template_params`]);
//! * its **secrets** resolvable at dispatch via `value_from: {secret: NAME}` —
//!   the DB store (AES-256-GCM, `DAGRON_ENV_SECRET_KEY`) is consulted first,
//!   then the classic process-env / secrets-dir tiers ([`resolve_secrets`]),
//!   so existing SOPS/External-Secrets deployments keep working unchanged.

use std::collections::BTreeMap;

use anyhow::{Context, Result};
use serde::Deserialize;

use crate::dag;
use crate::db;

/// Minimal peek at a spec YAML for its `environment:` line — full parsing (and
/// its errors) stay with the caller's own `DagGraph::from_yaml*`.
#[derive(Default, Deserialize)]
struct EnvPeek {
    #[serde(default)]
    environment: Option<String>,
}

/// Marker error: the spec names an environment that does not exist. Permanent
/// (unlike a transient DB failure while resolving one) — callers that must
/// decide between "give up" and "retry next tick" downcast for this.
#[derive(Debug)]
pub(crate) struct UnknownEnvironment(pub(crate) String);

impl std::fmt::Display for UnknownEnvironment {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "environment '{}' not found", self.0)
    }
}

impl std::error::Error for UnknownEnvironment {}

/// Template keys (`env.NAME` → value) for the spec's declared environment.
/// Empty map when no environment is declared; a **declared but unknown**
/// environment is a hard error — a spec pinned to `prod` must never silently
/// run without prod's variables.
pub(crate) async fn template_params(
    pool: &db::Pool,
    yaml: &str,
) -> Result<BTreeMap<String, String>> {
    let peek: EnvPeek = serde_yaml::from_str(yaml).unwrap_or_default();
    let mut out = BTreeMap::new();
    if let Some(name) = peek.environment {
        let vars = db::environment_vars(pool, &name)
            .await?
            .ok_or_else(|| anyhow::Error::new(UnknownEnvironment(name.clone())))?;
        for (k, v) in vars {
            out.insert(format!("env.{k}"), v);
        }
    }
    Ok(out)
}

/// Resolve every `value_from: {secret: NAME}` env var for a task about to
/// dispatch. Tier order: the run's environment secret store (decrypted with
/// `DAGRON_ENV_SECRET_KEY`), then `DAGRON_SECRET_<NAME>` / the secrets dir.
/// A secret found nowhere is a hard error — the task must not run with an
/// empty credential. `value_from` stays set on resolved vars so the output
/// redactor keeps masking their values.
pub(crate) async fn resolve_secrets(
    pool: &db::Pool,
    run_id: &str,
    env: &mut [dag::EnvVar],
) -> Result<()> {
    if !env.iter().any(|e| e.value_from.is_some()) {
        return Ok(());
    }
    // Propagate a DB failure here — swallowing it would silently skip the DB
    // secret tier and fail later with a misleading "secret not found".
    let env_name = db::run_environment(pool, run_id)
        .await
        .context("loading the run's environment for secret resolution")?;
    // Key errors only matter if a store lookup is actually attempted; a
    // missing key simply skips the DB tier (classic deployments have no key).
    let key = if env_name.is_some() { dagron_crypto::load_key().ok() } else { None };

    for var in env.iter_mut() {
        let Some(secret_ref) = var.value_from.clone() else { continue };
        let mut resolved: Option<String> = None;
        if let (Some(env_name), Some(key)) = (&env_name, &key) {
            if let Some(ciphertext) = db::environment_secret(pool, env_name, &secret_ref.secret).await? {
                resolved = Some(dagron_crypto::decrypt(key, &ciphertext).with_context(|| {
                    format!(
                        "decrypting secret '{}' from environment '{env_name}' \
                         (mismatched {}?)",
                        secret_ref.secret,
                        dagron_crypto::KEY_ENV
                    )
                })?);
            }
        }
        var.value = match resolved {
            Some(v) => v,
            None => dagron_executor::secrets::lookup(&secret_ref.secret).with_context(|| {
                match &env_name {
                    Some(n) => format!(
                        "secret '{}' is neither in environment '{n}' nor the process env/secrets dir",
                        secret_ref.secret
                    ),
                    None => format!("secret '{}' not found", secret_ref.secret),
                }
            })?,
        };
    }
    Ok(())
}
