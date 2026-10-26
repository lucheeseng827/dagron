//! Which stores a task may read its secrets from, and reading them: an
//! environment's own store, by the policy stored for it
//! ([`crate::envpolicy`]), and the host's store, by the run's trust class.
//!
//! Shared by the two that hand a task its secrets: the engine, as it
//! dispatches a task to its own executor, and dagron-api, as it hands one to a
//! remote runner. One copy, so the two cannot come to disagree about
//! who may read what. What the host's store is is the caller's to say
//! ([`HostStore`]): the engine's is its process environment and secrets
//! directory; dagron-api reads none of its own.

use anyhow::{Context, Result};

use crate::dag;
use crate::db;
use crate::envpolicy;

/// The host's own store, as the caller has one: a secret by its name, or an
/// error saying why it is not there. Read for a `trusted` run, for what its
/// environment does not hold.
pub type HostStore<'a> = dyn Fn(&str) -> Result<String> + Send + Sync + 'a;

/// Marker error: a run that is not `trusted` asked for a secret and named no
/// environment that admits it. The engine's own store (`DAGRON_SECRET_<NAME>`,
/// the secrets directory) belongs to no environment, so no policy speaks for
/// it: it is the installation's, and a run of another class reads none of it.
#[derive(Debug)]
pub struct HostStoreRefused {
    trust: String,
}

impl std::fmt::Display for HostStoreRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "this run is {}, and names no environment for its secrets: the engine's own \
             store is read by trusted runs alone",
            self.trust
        )
    }
}

impl std::error::Error for HostStoreRefused {}

/// Whether an error is a refusal by policy: the environment's
/// ([`envpolicy::EnvironmentRefused`]) or the engine's own store's
/// ([`HostStoreRefused`]). Permanent, like [`UnknownEnvironment`]: asking
/// again gets the same answer until an operator changes the policy.
pub fn is_refusal(e: &anyhow::Error) -> bool {
    e.downcast_ref::<envpolicy::EnvironmentRefused>().is_some()
        || e.downcast_ref::<HostStoreRefused>().is_some()
}

/// Whether `environment` admits a run of these facts, by the policy stored
/// for it ([`envpolicy::admits`]). A refusal is an
/// [`envpolicy::EnvironmentRefused`]; a policy that cannot be loaded or read
/// is an error of its own, and never an admission.
pub async fn admit(
    pool: &db::Pool,
    environment: &str,
    trigger: Option<&crate::models::RunTrigger>,
    facts: &envpolicy::RunFacts,
) -> Result<()> {
    envpolicy::repository_admits(environment, trigger).map_err(anyhow::Error::new)?;
    let policy = db::environment_policy(pool, environment)
        .await
        .with_context(|| format!("loading the policy of environment '{environment}'"))?;
    envpolicy::admits(environment, policy.as_ref(), facts).map_err(anyhow::Error::new)
}

/// Whether a task of a run of `class` may hold an identity in `environment`,
/// by the policy stored for it ([`envpolicy::identity_admits`]). A trusted
/// run's may, and its policy is not read; a policy that cannot be loaded is
/// an error, and never an admission.
pub async fn admit_identity(pool: &db::Pool, environment: Option<&str>, class: &str) -> Result<()> {
    if class == envpolicy::TRUSTED {
        return Ok(());
    }
    let policy = match environment {
        Some(name) => db::environment_policy(pool, name)
            .await
            .with_context(|| format!("loading the policy of environment '{name}'"))?,
        None => None,
    };
    envpolicy::identity_admits(environment, policy.as_ref(), class).map_err(anyhow::Error::msg)
}

/// Where one task of a run may resolve its secrets from.
pub struct SecretSource {
    /// The environment whose store is read first: the task's own
    /// (`environment:` on the task), else the run's. Its policy admitted the
    /// run, or this would not have been returned.
    pub environment: Option<String>,
    /// Whether the engine's own store is read for what the environment does
    /// not hold. A `trusted` run's alone.
    pub host_store: bool,
    /// The run's trust class, for the words of an error.
    pub trust: String,
}

/// What a task of `run_id` may resolve its secrets from, or why it may
/// resolve none ([`is_refusal`]). Asked before any secret is read.
///
/// The run's facts are its own record's: the class and the ref a CI service
/// wrote when it made the run, and never anything the workflow says of
/// itself. An environment is asked its policy with them. The engine's own
/// store has no policy, and is read by `trusted` runs alone.
pub async fn secret_source(
    pool: &db::Pool,
    run_id: &str,
    task_environment: Option<&str>,
) -> Result<SecretSource> {
    let trigger = db::run_trigger(pool, run_id)
        .await
        .context("loading what started the run, for its secrets")?;
    let pipeline = db::workflow_name_for_run(pool, run_id)
        .await
        .context("loading the run's workflow name, for its secrets")?
        .unwrap_or_default();
    let facts = envpolicy::RunFacts::of(trigger.as_ref(), &pipeline);
    // Propagate a DB failure here — swallowing it would silently skip the DB
    // secret tier and fail later with a misleading "secret not found".
    let environment = match task_environment {
        Some(name) => Some(name.to_string()),
        None => db::run_environment(pool, run_id)
            .await
            .context("loading the run's environment for secret resolution")?,
    };
    if let Some(name) = &environment {
        admit(pool, name, trigger.as_ref(), &facts).await?;
    }
    let host_store = facts.trust == envpolicy::TRUSTED;
    if environment.is_none() && !host_store {
        return Err(anyhow::Error::new(HostStoreRefused { trust: facts.trust }));
    }
    Ok(SecretSource {
        environment,
        host_store,
        trust: facts.trust,
    })
}

/// Resolve every `value_from: {secret: NAME}` env var for a task about to
/// dispatch. Tier order: the environment's secret store (decrypted with
/// `DAGRON_ENV_SECRET_KEY`), then `DAGRON_SECRET_<NAME>` / the secrets dir.
/// A secret found nowhere is a hard error — the task must not run with an
/// empty credential. `value_from` stays set on resolved vars so the output
/// redactor keeps masking their values.
///
/// Which stores are read is [`secret_source`]'s to say, and it is asked
/// first: the environment is the task's own (`task_environment`) or the
/// run's, and only where its policy admits the run; the second tier is a
/// `trusted` run's alone.
pub async fn resolve(
    pool: &db::Pool,
    run_id: &str,
    task_environment: Option<&str>,
    env: &mut [dag::EnvVar],
    host_store: &HostStore<'_>,
) -> Result<()> {
    if !env.iter().any(|e| e.value_from.is_some()) {
        return Ok(());
    }
    let source = secret_source(pool, run_id, task_environment).await?;
    let env_name = source.environment;
    // Two decryptors, built once: the legacy single key (v1) and/or an envelope
    // KEK provider (v2, BYOK/KMS). Per-secret decrypt dispatches on the stored
    // ciphertext version. A classic deployment with neither simply skips the DB
    // secret tier.
    let legacy_key = if env_name.is_some() {
        dagron_crypto::load_key().ok()
    } else {
        None
    };
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
        let Some(secret_ref) = var.value_from.clone() else {
            continue;
        };
        let mut resolved: Option<String> = None;
        if let Some(env_name) = &env_name {
            if can_decrypt {
                if let Some(ciphertext) =
                    db::environment_secret(pool, env_name, &secret_ref.secret).await?
                {
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
            // Its environment's alone, and empty where it has none: never
            // the engine's own store, whatever the run.
            None if secret_ref.optional => String::new(),
            // The engine's own store, for a trusted run.
            None if source.host_store => {
                host_store(&secret_ref.secret).with_context(|| {
                    match &env_name {
                        Some(n) => format!(
                            "secret '{}' is neither in environment '{n}' nor the process env/secrets dir",
                            secret_ref.secret
                        ),
                        None => format!("secret '{}' not found", secret_ref.secret),
                    }
                })?
            }
            // A run of another class has what its environment holds, and
            // nothing else. (It has an environment: `secret_source` refused
            // it otherwise.)
            None => anyhow::bail!(
                "secret '{}' is not in environment '{}', and a run that is {} reads nothing \
                 from the engine's own store",
                secret_ref.secret,
                env_name.as_deref().unwrap_or_default(),
                source.trust
            ),
        };
    }
    Ok(())
}

/// Decrypt one stored environment-secret ciphertext, dispatching on its version:
/// `v2:` = envelope (needs the KEK `provider`), `v1:` = legacy single key. Errors
/// (never falls back) when the required decryptor is absent or the version is
/// unknown — a task must not run with an unresolved credential.
pub fn decrypt_secret_value(
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
        let err =
            decrypt_secret_value(&ct, Some(&[0u8; 32]), None, None, "API_KEY", "prod").unwrap_err();
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
        assert!(
            decrypt_secret_value("garbage", Some(&[0u8; 32]), None, None, "X", "prod").is_err()
        );
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
