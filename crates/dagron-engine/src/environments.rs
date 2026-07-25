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
    // Two decryptors, built once: the legacy single key (v1) and/or an envelope
    // KEK provider (v2, BYOK/KMS). Per-secret decrypt dispatches on the stored
    // ciphertext version. A classic deployment with neither simply skips the DB
    // secret tier.
    let legacy_key = if env_name.is_some() { dagron_crypto::load_key().ok() } else { None };
    // Keep the provider *construction* Result: a provider that is configured but
    // built wrong (bad KEK, missing wrap/unwrap command, …) must surface THAT
    // reason on a v2 secret — not the misleading "no provider configured".
    let provider_result = if env_name.is_some() {
        dagron_crypto::provider_from_env()
    } else {
        Ok(None)
    };
    let provider_build_err: Option<String> =
        provider_result.as_ref().err().map(|e| format!("{e:#}"));
    let provider: Option<&dyn dagron_crypto::KeyProvider> =
        provider_result.as_ref().ok().and_then(|o| o.as_deref());
    // Consult the DB secret tier when *any* decryptor is configured — including a
    // provider that failed to build, so a v2 secret reports the precise reason
    // rather than falling through to a misleading "secret not found".
    let can_decrypt = legacy_key.is_some() || provider.is_some() || provider_build_err.is_some();

    for var in env.iter_mut() {
        let Some(secret_ref) = var.value_from.clone() else { continue };
        let mut resolved: Option<String> = None;
        if let Some(env_name) = &env_name {
            if can_decrypt {
                if let Some(ciphertext) = db::environment_secret(pool, env_name, &secret_ref.secret).await? {
                    resolved = Some(decrypt_secret_value(
                        &ciphertext,
                        legacy_key.as_ref(),
                        provider,
                        provider_build_err.as_deref(),
                        &secret_ref.secret,
                        env_name,
                    )?);
                }
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

/// Decrypt one stored environment-secret ciphertext, dispatching on its version:
/// `v2:` = envelope (needs the KEK `provider`), `v1:` = legacy single key. Errors
/// (never falls back) when the required decryptor is absent or the version is
/// unknown — a task must not run with an unresolved credential.
fn decrypt_secret_value(
    ciphertext: &str,
    legacy_key: Option<&[u8; 32]>,
    provider: Option<&dyn dagron_crypto::KeyProvider>,
    provider_build_err: Option<&str>,
    secret_name: &str,
    env_name: &str,
) -> Result<String> {
    match dagron_crypto::version_of(ciphertext) {
        Some("v2") => {
            let p = match provider {
                Some(p) => p,
                // A provider that was configured but failed to build reports the
                // real reason; a genuinely-absent provider gets the config hint.
                None => match provider_build_err {
                    Some(build_err) => anyhow::bail!(
                        "secret '{secret_name}' in environment '{env_name}' is envelope-encrypted \
                         (v2) but the KEK provider ({}) failed to initialize: {build_err}",
                        dagron_crypto::PROVIDER_ENV
                    ),
                    None => anyhow::bail!(
                        "secret '{secret_name}' in environment '{env_name}' is envelope-encrypted \
                         (v2) but no KEK provider is configured (set {})",
                        dagron_crypto::PROVIDER_ENV
                    ),
                },
            };
            dagron_crypto::decrypt_envelope(p, ciphertext).with_context(|| {
                format!("decrypting envelope secret '{secret_name}' from environment '{env_name}'")
            })
        }
        Some("v1") => {
            let k = legacy_key.with_context(|| {
                format!(
                    "secret '{secret_name}' is v1-encrypted but {} is not set",
                    dagron_crypto::KEY_ENV
                )
            })?;
            dagron_crypto::decrypt(k, ciphertext).with_context(|| {
                format!(
                    "decrypting secret '{secret_name}' from environment '{env_name}' (mismatched {}?)",
                    dagron_crypto::KEY_ENV
                )
            })
        }
        _ => anyhow::bail!("secret '{secret_name}' has an unrecognized ciphertext version"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use dagron_crypto::{encrypt, encrypt_envelope, LocalKekProvider};

    #[test]
    fn v1_secret_round_trips_via_legacy_key() {
        let key = [7u8; 32];
        let ct = encrypt(&key, "hunter2").unwrap();
        let got = decrypt_secret_value(&ct, Some(&key), None, None, "DB_PASSWORD", "prod").unwrap();
        assert_eq!(got, "hunter2");
    }

    #[test]
    fn v2_secret_round_trips_via_kek_provider() {
        let p = LocalKekProvider::new([9u8; 32]);
        let ct = encrypt_envelope(&p, "s3cret").unwrap();
        let got = decrypt_secret_value(&ct, None, Some(&p), None, "API_KEY", "prod").unwrap();
        assert_eq!(got, "s3cret");
    }

    #[test]
    fn v2_secret_without_a_provider_is_an_error_not_a_fallback() {
        let p = LocalKekProvider::new([9u8; 32]);
        let ct = encrypt_envelope(&p, "s3cret").unwrap();
        // Only the legacy key present → must error, never silently skip.
        let err = decrypt_secret_value(&ct, Some(&[0u8; 32]), None, None, "API_KEY", "prod").unwrap_err();
        assert!(err.to_string().contains("no KEK provider"));
    }

    #[test]
    fn v1_secret_without_the_legacy_key_is_an_error() {
        let ct = encrypt(&[1u8; 32], "x").unwrap();
        let p = LocalKekProvider::new([9u8; 32]);
        let err = decrypt_secret_value(&ct, None, Some(&p), None, "X", "prod").unwrap_err();
        assert!(err.to_string().contains("not set"));
    }

    #[test]
    fn unknown_version_is_rejected() {
        assert!(decrypt_secret_value("garbage", Some(&[0u8; 32]), None, None, "X", "prod").is_err());
    }

    #[test]
    fn v2_secret_with_a_broken_provider_surfaces_the_build_error() {
        // Provider was configured but failed to build → the v2 error must name the
        // real reason, not the misleading "no KEK provider is configured".
        let p = LocalKekProvider::new([9u8; 32]);
        let ct = encrypt_envelope(&p, "s3cret").unwrap();
        let err = decrypt_secret_value(
            &ct,
            None,
            None,
            Some("DAGRON_ENV_KEK is not set"),
            "API_KEY",
            "prod",
        )
        .unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("failed to initialize"), "got: {msg}");
        assert!(msg.contains("DAGRON_ENV_KEK is not set"), "got: {msg}");
    }
}
