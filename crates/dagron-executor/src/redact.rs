//! Secret masking for task output (fast-win #8).
//!
//! A task's environment routinely carries credentials (a `*_TOKEN`,
//! `*_PASSWORD`, `DATABASE_URL`, …). If the task echoes one — or a library
//! prints it in a stack trace — it would otherwise land verbatim in the stored
//! task output and the UI. [`Redactor`] masks known secret values with `***`
//! before the output is persisted or logged.
//!
//! What counts as a secret:
//!   * any task env var whose **name** matches a sensitive pattern
//!     (`TOKEN`/`PASSWORD`/`SECRET`/`KEY`/… — case-insensitive substring),
//!     overridable via `DAGRON_SENSITIVE_ENV_PATTERNS` (comma-separated; empty
//!     disables name-based masking);
//!   * the value of any engine-process env var explicitly listed in
//!     `DAGRON_REDACT_ENV` (e.g. `DATABASE_URL,MY_APP_SECRET`).
//!
//! Only values of at least [`MIN_LEN`] characters are masked, so a
//! `DEBUG=1`-style flag named `..._KEY` isn't turned into noise.

use std::borrow::Cow;

use dagron_core::dag::EnvVar;

/// Shortest value that gets masked — avoids redacting trivial flags like `1`.
const MIN_LEN: usize = 4;

/// Default sensitive name substrings (uppercased) when
/// `DAGRON_SENSITIVE_ENV_PATTERNS` is unset.
const DEFAULT_PATTERNS: &[&str] = &[
    "SECRET",
    "TOKEN",
    "PASSWORD",
    "PASSWD",
    "PWD",
    "CREDENTIAL",
    "APIKEY",
    "ACCESS_KEY",
    "PRIVATE_KEY",
];

/// A set of secret literals to mask out of task output. Cheap to build
/// per-dispatch; a no-op (borrows its input) when nothing sensitive is present.
#[derive(Debug, Default, Clone)]
pub struct Redactor {
    /// Secret values, de-duplicated and sorted longest-first so a value that
    /// contains another is masked before its substring.
    values: Vec<String>,
}

impl Redactor {
    /// Build a redactor for one task from its declared `env` plus the process
    /// env vars named in `DAGRON_REDACT_ENV`.
    pub fn from_task_env(task_env: &[EnvVar]) -> Self {
        Self::build(
            task_env,
            |k| std::env::var(k).ok(),
            std::env::var("DAGRON_SENSITIVE_ENV_PATTERNS").ok(),
            std::env::var("DAGRON_REDACT_ENV").ok(),
        )
    }

    /// Testable core: `lookup` resolves a process env var, and the two config
    /// strings are the raw `DAGRON_SENSITIVE_ENV_PATTERNS` / `DAGRON_REDACT_ENV`.
    fn build(
        task_env: &[EnvVar],
        lookup: impl Fn(&str) -> Option<String>,
        patterns_cfg: Option<String>,
        redact_env_cfg: Option<String>,
    ) -> Self {
        let patterns: Vec<String> = match patterns_cfg {
            // Explicitly set (even empty) overrides the defaults; empty ⇒ no
            // name-based masking.
            Some(raw) => raw
                .split(',')
                .map(|p| p.trim().to_uppercase())
                .filter(|p| !p.is_empty())
                .collect(),
            None => DEFAULT_PATTERNS.iter().map(|s| s.to_string()).collect(),
        };

        let mut values: Vec<String> = Vec::new();
        // 1. Task env vars whose name looks sensitive, and any var resolved from a
        //    secret (`value_from`) — explicitly a secret, so masked whatever its
        //    name.
        for e in task_env {
            let name = e.name.to_uppercase();
            if e.value_from.is_some() || patterns.iter().any(|p| name.contains(p.as_str())) {
                values.push(e.value.clone());
            }
        }
        // 2. Explicitly-named process env values (always masked).
        if let Some(raw) = redact_env_cfg {
            for key in raw.split(',').map(str::trim).filter(|k| !k.is_empty()) {
                if let Some(v) = lookup(key) {
                    values.push(v);
                }
            }
        }

        values.retain(|v| v.chars().count() >= MIN_LEN);
        values.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a.cmp(b)));
        values.dedup();
        Self { values }
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Replace every known secret value in `s` with `***`. Borrows `s` unchanged
    /// when nothing matches (the common case), so it is free on secret-less
    /// output.
    pub fn redact<'a>(&self, s: &'a str) -> Cow<'a, str> {
        if self.values.is_empty() || !self.values.iter().any(|v| s.contains(v.as_str())) {
            return Cow::Borrowed(s);
        }
        let mut out = s.to_string();
        for v in &self.values {
            out = out.replace(v.as_str(), "***");
        }
        Cow::Owned(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn env(pairs: &[(&str, &str)]) -> Vec<EnvVar> {
        pairs
            .iter()
            .map(|(n, v)| EnvVar {
                name: n.to_string(),
                value: v.to_string(),
                value_from: None,
            })
            .collect()
    }

    #[test]
    fn masks_sensitively_named_task_env() {
        let r = Redactor::build(
            &env(&[
                ("API_TOKEN", "sk-abcdef123456"),
                ("REGION", "us-east-1"),
                ("DB_PASSWORD", "hunter2!!"),
            ]),
            |_| None,
            None,
            None,
        );
        let out = r.redact("connected with sk-abcdef123456 to us-east-1 pw=hunter2!!");
        assert_eq!(out, "connected with *** to us-east-1 pw=***");
    }

    #[test]
    fn short_values_and_nonsensitive_names_are_left_alone() {
        let r = Redactor::build(
            &env(&[("APP_KEY", "1"), ("REGION", "us-east-1")]),
            |_| None,
            None,
            None,
        );
        // "1" is below MIN_LEN; REGION isn't a sensitive name.
        assert!(r.is_empty());
        assert_eq!(r.redact("region us-east-1 key 1"), "region us-east-1 key 1");
    }

    #[test]
    fn custom_patterns_override_defaults() {
        // Only WITCHCRAFT is sensitive now; TOKEN no longer is.
        let r = Redactor::build(
            &env(&[
                ("MY_TOKEN", "tok-longvalue"),
                ("WITCHCRAFT_VAL", "abracadabra"),
            ]),
            |_| None,
            Some("WITCHCRAFT".to_string()),
            None,
        );
        assert_eq!(r.redact("tok-longvalue abracadabra"), "tok-longvalue ***");
    }

    #[test]
    fn empty_patterns_disable_name_masking_but_redact_env_still_works() {
        let r = Redactor::build(
            &env(&[("API_TOKEN", "tok-longvalue")]),
            |k| (k == "DATABASE_URL").then(|| "postgres://u:p@host/db".to_string()),
            Some(String::new()), // disable name-based masking
            Some("DATABASE_URL".to_string()),
        );
        assert_eq!(
            r.redact("url postgres://u:p@host/db token tok-longvalue"),
            "url *** token tok-longvalue"
        );
    }

    #[test]
    fn value_from_secrets_are_masked_regardless_of_name() {
        // A resolved secret ref is masked even though "DEPLOY_KEY" isn't a
        // default sensitive-name pattern.
        let e = EnvVar {
            name: "DEPLOY_KEY".to_string(),
            value: "file-secret-xyz".to_string(),
            value_from: Some(dagron_core::dag::SecretRef { secret: "deploy-key".to_string() }),
        };
        let r = Redactor::build(&[e], |_| None, None, None);
        assert_eq!(r.redact("key=file-secret-xyz"), "key=***");
    }

    #[test]
    fn overlapping_values_mask_longest_first() {
        // A secret that is a superstring of another must not leave a dangling tail.
        let r = Redactor::build(
            &env(&[("A_TOKEN", "abcdef"), ("B_TOKEN", "abcdefghij")]),
            |_| None,
            None,
            None,
        );
        assert_eq!(r.redact("x abcdefghij y"), "x *** y");
    }
}
