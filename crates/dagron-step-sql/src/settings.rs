//! How to reach one store, read from the environment under one prefix.
//!
//! The step reaches one store, under `SQL_`. The parity check reaches two, under
//! `PARITY_SOURCE_` and `PARITY_TARGET_`, with the same variables after the prefix.
//! So the transports take a [`Settings`] rather than reading `SQL_*` themselves:
//! one set of rules for what a connection needs, applied the same way to every
//! side.

use std::fmt;

use anyhow::{bail, Context, Result};

use crate::{reject_inline_credential, Engine, KNOWN_ENGINES};

/// The statement deadline when `…TIMEOUT_SECS` is not set.
pub const DEFAULT_TIMEOUT_SECS: u64 = 300;

/// One store's connection, as the environment gave it.
///
/// `Debug` never prints the password, so a `Settings` can be logged.
#[derive(Clone)]
pub struct Settings {
    /// `SQL_`, `PARITY_SOURCE_` or `PARITY_TARGET_`. Every message that names a
    /// variable names it with this prefix, so an error points at the variable the
    /// operator set.
    pub prefix: &'static str,
    pub engine: Engine,
    pub dsn: String,
    pub user: Option<String>,
    pub password: Option<String>,
    /// Snowflake only: what kind of token `password` is.
    pub token_type: Option<String>,
    pub timeout_secs: u64,
}

impl Settings {
    /// Read `{prefix}ENGINE`, `{prefix}DSN`, `{prefix}USER`, `{prefix}PASSWORD`,
    /// `{prefix}TOKEN_TYPE` and `{prefix}TIMEOUT_SECS` through `get`.
    ///
    /// The DSN is checked for an inline credential here, once for every side:
    /// nothing masks a DSN, so a password inside one would be printed wherever the
    /// DSN is.
    pub fn from_env(prefix: &'static str, get: impl Fn(&str) -> Option<String>) -> Result<Settings> {
        let var = |name: &str| get(&format!("{prefix}{name}")).filter(|s| !s.trim().is_empty());
        let Some(raw_engine) = var("ENGINE") else {
            bail!("{prefix}ENGINE is required ({KNOWN_ENGINES})");
        };
        let engine = Engine::parse(&raw_engine).with_context(|| format!("reading {prefix}ENGINE"))?;
        let dsn = var("DSN").with_context(|| {
            format!("{prefix}DSN is required (e.g. http://clickhouse:8123/?database=analytics)")
        })?;
        reject_inline_credential(&dsn).with_context(|| format!("reading {prefix}DSN"))?;
        let timeout_secs = match var("TIMEOUT_SECS") {
            None => DEFAULT_TIMEOUT_SECS,
            Some(raw) => raw.trim().parse().with_context(|| {
                format!("{prefix}TIMEOUT_SECS must be a non-negative integer, got '{raw}'")
            })?,
        };
        Ok(Settings {
            prefix,
            engine,
            dsn,
            user: var("USER"),
            // Not trimmed here: a password may hold spaces. The token-based
            // transports trim theirs, because a token never does.
            password: get(&format!("{prefix}PASSWORD")).filter(|s| !s.is_empty()),
            token_type: var("TOKEN_TYPE"),
            timeout_secs,
        })
    }

    /// The full name of one of this side's variables, for messages.
    pub fn var(&self, name: &str) -> String {
        format!("{}{name}", self.prefix)
    }
}

impl fmt::Debug for Settings {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Settings")
            .field("prefix", &self.prefix)
            .field("engine", &self.engine)
            .field("dsn", &self.dsn)
            .field("user", &self.user)
            .field("password", &self.password.as_ref().map(|_| "<set>"))
            .field("token_type", &self.token_type)
            .field("timeout_secs", &self.timeout_secs)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn env(pairs: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let map: BTreeMap<String, String> =
            pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        move |k| map.get(k).cloned()
    }

    #[test]
    fn every_side_reads_the_same_variables_under_its_own_prefix() {
        let get = env(&[
            ("PARITY_SOURCE_ENGINE", "postgres"),
            ("PARITY_SOURCE_DSN", "postgresql://etl@old/db"),
            ("PARITY_SOURCE_PASSWORD", "pw with spaces"),
            ("PARITY_SOURCE_TIMEOUT_SECS", "60"),
            ("SQL_ENGINE", "clickhouse"),
        ]);
        let s = Settings::from_env("PARITY_SOURCE_", &get).unwrap();
        assert_eq!(s.engine, Engine::Postgres);
        assert_eq!(s.password.as_deref(), Some("pw with spaces"), "a password keeps its spaces");
        assert_eq!(s.timeout_secs, 60);
        assert_eq!(s.var("PASSWORD"), "PARITY_SOURCE_PASSWORD");
        assert!(!format!("{s:?}").contains("pw with"), "Debug never prints the password");

        let e = Settings::from_env("PARITY_TARGET_", &get).unwrap_err().to_string();
        assert!(e.contains("PARITY_TARGET_ENGINE is required"), "names the prefixed variable: {e}");
    }

    #[test]
    fn an_inline_credential_is_refused_on_every_side() {
        let get = env(&[("PARITY_TARGET_ENGINE", "postgres"), ("PARITY_TARGET_DSN", "postgresql://u:pw@h/db")]);
        let e = format!("{:#}", Settings::from_env("PARITY_TARGET_", &get).unwrap_err());
        assert!(e.contains("reading PARITY_TARGET_DSN") && e.contains("inline password"), "{e}");
        let get = env(&[("SQL_ENGINE", "postgres"), ("SQL_DSN", "postgresql://u@h/db"), ("SQL_TIMEOUT_SECS", "soon")]);
        assert!(Settings::from_env("SQL_", &get).unwrap_err().to_string().contains("SQL_TIMEOUT_SECS"));
    }
}
