//! Secret resolution for env `value_from` references (fast-win #9).
//!
//! A task can pull an env var's value from an external secret instead of storing
//! it inline, so a credential never lands in the workflow spec or the datastore:
//!
//! ```yaml
//! env:
//!   - name: DB_PASSWORD
//!     value_from: { secret: prod-db-password }
//! ```
//!
//! At dispatch the engine calls [`resolve`], which reads each secret from:
//!   1. `DAGRON_SECRET_<NAME>` in the engine process environment (NAME uppercased,
//!      non-alphanumerics → `_`), or
//!   2. a file `<DAGRON_SECRETS_DIR>/<NAME>` — the SOPS / External-Secrets-Operator
//!      / Kubernetes-secret mount convention.
//!
//! A missing secret is an error: the caller fails the task rather than run it
//! with an empty credential. Resolved values never persist — only the reference
//! is stored on the task spec — and are always masked in output (the redactor
//! honors the `value_from` marker regardless of the var's name; see
//! [`crate::redact`]).

use anyhow::{Context, Result};

use dagron_core::dag::EnvVar;

/// Resolve every `value_from` reference in `env` into a concrete value, leaving
/// literal vars untouched. Returns a new list with all refs resolved (and
/// `value_from` cleared). Errors if any referenced secret cannot be found.
pub fn resolve(env: &[EnvVar]) -> Result<Vec<EnvVar>> {
    let mut out = Vec::with_capacity(env.len());
    for e in env {
        match &e.value_from {
            None => out.push(e.clone()),
            Some(r) => {
                let value = lookup(&r.secret).with_context(|| {
                    format!("env '{}' value_from secret '{}'", e.name, r.secret)
                })?;
                // Keep `value_from` set so the redactor knows this value is a
                // secret and masks it regardless of the var's name; the resolved
                // value lives only in the in-memory ExecContext, never persisted.
                out.push(EnvVar {
                    name: e.name.clone(),
                    value,
                    value_from: e.value_from.clone(),
                });
            }
        }
    }
    Ok(out)
}

/// Look up one secret by name from the process env or the secrets dir.
fn lookup(name: &str) -> Result<String> {
    let env_key = format!(
        "DAGRON_SECRET_{}",
        name.to_uppercase()
            .replace(|c: char| !c.is_ascii_alphanumeric(), "_")
    );
    if let Ok(v) = std::env::var(&env_key) {
        return Ok(v);
    }
    if let Ok(dir) = std::env::var("DAGRON_SECRETS_DIR") {
        // A secret name is a single path segment — never let it escape the dir.
        if name.is_empty() || name.contains('/') || name.contains('\\') || name.contains("..") {
            anyhow::bail!("invalid secret name '{name}' (must be a single path segment)");
        }
        let path = std::path::Path::new(&dir).join(name);
        if path.is_file() {
            let raw = std::fs::read_to_string(&path)
                .with_context(|| format!("reading secret file {}", path.display()))?;
            // Trim a single trailing newline (the common `echo secret > file` case)
            // without stripping meaningful whitespace inside the value.
            return Ok(raw
                .strip_suffix('\n')
                .map(|s| s.strip_suffix('\r').unwrap_or(s))
                .unwrap_or(&raw)
                .to_string());
        }
    }
    anyhow::bail!("secret '{name}' not found (set {env_key}, or place it in $DAGRON_SECRETS_DIR)")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ev(name: &str, secret: Option<&str>, value: &str) -> EnvVar {
        EnvVar {
            name: name.to_string(),
            value: value.to_string(),
            value_from: secret.map(|s| dagron_core::dag::SecretRef {
                secret: s.to_string(),
            }),
        }
    }

    #[test]
    fn resolves_from_process_env_and_passes_literals_through() {
        std::env::set_var("DAGRON_SECRET_PROD_DB_PASSWORD", "hunter2");
        let env = vec![
            ev("REGION", None, "us-east-1"),
            ev("DB_PASSWORD", Some("prod-db-password"), ""),
        ];
        let out = resolve(&env).unwrap();
        assert_eq!(out[0].value, "us-east-1"); // literal untouched
        assert_eq!(out[1].value, "hunter2"); // resolved
        assert!(
            out[1].value_from.is_some(),
            "ref kept so the redactor always masks it"
        );
        std::env::remove_var("DAGRON_SECRET_PROD_DB_PASSWORD");
    }

    #[test]
    fn resolves_from_secrets_dir_file() {
        let dir = std::env::temp_dir().join(format!("dagron-secrets-{}", uuid_like()));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("api-key"), "sk-abc123\n").unwrap();
        std::env::set_var("DAGRON_SECRETS_DIR", &dir);
        let out = resolve(&[ev("API_KEY", Some("api-key"), "")]).unwrap();
        assert_eq!(out[0].value, "sk-abc123"); // trailing newline trimmed
        std::env::remove_var("DAGRON_SECRETS_DIR");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn missing_secret_and_traversal_are_errors() {
        let err = resolve(&[ev("X", Some("nope-not-set"), "")])
            .unwrap_err()
            .to_string();
        assert!(
            err.contains("value_from secret 'nope-not-set'"),
            "got: {err}"
        );

        std::env::set_var("DAGRON_SECRETS_DIR", "/tmp");
        assert!(resolve(&[ev("X", Some("../etc/passwd"), "")]).is_err());
        std::env::remove_var("DAGRON_SECRETS_DIR");
    }

    // Avoid a uuid dep in the executor's test just for a temp name.
    fn uuid_like() -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        format!(
            "{}",
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        )
    }
}
