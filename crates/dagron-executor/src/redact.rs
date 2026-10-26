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
//!
//! A value of several lines (a PEM key, a JSON credential) is masked whole.
//! A task's log is masked as it is written, a piece at a time, and a piece
//! is a line, so a key printed line by line would be masked only once the
//! whole log is: the log's redactor masks each of its lines that could be
//! secret on its own as well ([`Redactor::with_lines`]). Its output, which a
//! later task may read, is masked with the whole values alone, so that no
//! line of its that happens to be a line of a secret (`name: kubernetes`)
//! is changed.
//!
//! But the value of a line of such a value whose key names a secret, as a
//! sensitive env var's name does (`token: …`, `password = …`,
//! `AWS_SECRET_ACCESS_KEY=…`), is a secret of its own, which a task that
//! reads it out of the whole may print alone: it is masked on its own, in a
//! log and in an output alike ([`named_value`]).

use std::borrow::Cow;

use dagron_core::dag::EnvVar;

/// Shortest value that gets masked — avoids redacting trivial flags like `1`.
const MIN_LEN: usize = 4;

/// The shortest a line of a value of several lines is, or its value where
/// it is a `key: value`, to be masked on its own: a line of one that is
/// shorter (`}`, `  ],`, `kind: Config`) is its shape, and the line of many
/// other texts.
const MIN_LINE_LEN: usize = 8;

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
        // 3. In a value of several lines, the value of each line whose key
        //    names a secret as an env var's name would.
        let named: Vec<String> = values
            .iter()
            .filter(|v| v.contains('\n'))
            .flat_map(|v| v.lines().filter_map(|line| named_value(line, &patterns)))
            .map(str::to_string)
            .collect();
        values.extend(named);

        Self {
            values: masked(values),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// The length of the longest value this masks, in bytes.
    pub fn longest(&self) -> usize {
        self.values.first().map_or(0, String::len)
    }

    /// How far into `text` a value this masks could reach that began
    /// before it: the longest beginning of `text` that is an end of one,
    /// shorter than it.
    pub fn reach(&self, text: &str) -> usize {
        let text = text.as_bytes();
        let mut most = 0;
        for v in &self.values {
            let v = v.as_bytes();
            let head = &text[..text.len().min(v.len() - 1)];
            // How much of each beginning of `head` is also its own end, then
            // `v` read against it: what of `head` it ends on.
            let mut back = vec![0; head.len()];
            for i in 1..head.len() {
                let mut k = back[i - 1];
                while k > 0 && head[i] != head[k] {
                    k = back[k - 1];
                }
                if head[i] == head[k] {
                    k += 1;
                }
                back[i] = k;
            }
            let mut k = 0;
            for &b in v {
                while k > 0 && (k == head.len() || head[k] != b) {
                    k = back[k - 1];
                }
                if k < head.len() && head[k] == b {
                    k += 1;
                }
            }
            most = most.max(k);
        }
        most
    }

    /// Where `text` can be cut, at `at` or past it, without cutting in two
    /// a value this masks: `at`, or the end of the last of them that spans
    /// it, and so on from there, one being able to end inside another.
    pub fn clear_cut(&self, text: &str, at: usize) -> usize {
        let floor = |at: usize| {
            let mut at = at.min(text.len());
            while !text.is_char_boundary(at) {
                at -= 1;
            }
            at
        };
        let mut at = floor(at);
        loop {
            let before = at;
            for v in &self.values {
                // Each as far as it goes first: one that overlaps itself
                // ends inside the next of it, in a run of it, and the others
                // are then looked for once, not at each step.
                loop {
                    // One that ends between these began before `at`, so
                    // spans it, and the last ends latest.
                    let from = floor(at.saturating_sub(v.len() - 1));
                    let till = floor(at + v.len() - 1);
                    match text[from..till].rfind(v.as_str()) {
                        Some(start) if from + start + v.len() > at => at = from + start + v.len(),
                        _ => break,
                    }
                }
            }
            if at == before {
                return at;
            }
        }
    }

    /// This redactor, masking `extra` as well: a secret the task's env does
    /// not hold, such as the credential its checkout was fetched with.
    pub fn with_values<I, S>(mut self, extra: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.values.extend(extra.into_iter().map(Into::into));
        self.values = masked(std::mem::take(&mut self.values));
        self
    }

    /// This redactor, masking each line of each value of several lines that
    /// could be secret ([`secret_line`]) on its own as well, without the
    /// space around it: a task's log's, which is masked a line at a time as
    /// it is written.
    pub fn with_lines(mut self) -> Self {
        let lines: Vec<String> = self
            .values
            .iter()
            .filter(|v| v.contains('\n'))
            .flat_map(|v| v.lines().map(str::trim).filter(|line| secret_line(line)))
            .map(str::to_string)
            .collect();
        self.values.extend(lines);
        self.values = masked(std::mem::take(&mut self.values));
        self
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

/// The values a redactor masks, of `values`: each, none shorter than
/// [`MIN_LEN`]; longest first, so that a value that holds another is masked
/// before it; and each once.
fn masked(mut values: Vec<String>) -> Vec<String> {
    values.retain(|v| v.chars().count() >= MIN_LEN);
    values.sort_by(|a, b| b.len().cmp(&a.len()).then_with(|| a.cmp(b)));
    values.dedup();
    values
}

/// Whether a line of a value of several lines (its space taken off) could
/// be secret, and is masked on its own: one of at least [`MIN_LINE_LEN`]
/// characters, or, where it is a `key: value` (or `key = value`), whose
/// value is, its quotes and a last comma taken off. Not a key with no value
/// (`clusters:`), an INI file's section (`[default]`), nor a PEM key's
/// armour alone on its line (`-----BEGIN …-----`), which is every key's.
fn secret_line(line: &str) -> bool {
    let armour =
        line.starts_with("-----") && line.ends_with("-----") && line.matches("-----").count() == 2;
    let section = line.starts_with('[') && line.ends_with(']');
    let bare_key = line.ends_with(':') && !line.contains(": ");
    if armour || section || bare_key {
        return false;
    }
    let value = match line.split_once(": ").or_else(|| line.split_once(" = ")) {
        Some((key, value)) if !key.trim_start_matches("- ").contains(char::is_whitespace) => value,
        _ => line,
    };
    let value = value
        .trim()
        .trim_end_matches(',')
        .trim_matches(|c| c == '"' || c == '\'');
    value.chars().count() >= MIN_LINE_LEN
}

/// Keys of a file's lines that hold a secret though no sensitive env var's
/// name would be theirs: a kubeconfig's `client-key-data`, a Docker
/// config's `auth`, a key's `passphrase`. A key is one of these, or ends in
/// `_` and one of them, not merely holds one (`author`).
const SECRET_KEYS: &[&str] = &["KEY_DATA", "AUTH", "PASSPHRASE", "IDENTITYTOKEN"];

/// The value of a line of a value of several lines whose key names a secret,
/// as a sensitive env var's name does (it holds one of `patterns`), or is
/// one of [`SECRET_KEYS`], where it is of [`MIN_LINE_LEN`] characters or
/// more: `token: …`, `"private_key_id": "…",`, `password = …`,
/// `AWS_SECRET_ACCESS_KEY=…`, `client-key-data: …`, `"auth": "…"`. None,
/// with no patterns: masking by name is off. The key is the line's up to
/// its first `: `, ` = ` or `=`, of no space, its quotes, a list's `- ` and
/// an `export ` taken off, a `-` in it read as `_`; the value has its
/// quotes and a last comma taken off.
fn named_value<'a>(line: &'a str, patterns: &[String]) -> Option<&'a str> {
    let line = line.trim();
    let line = line.strip_prefix("- ").unwrap_or(line);
    let line = line.strip_prefix("export ").unwrap_or(line);
    let (at, len) = [": ", " = ", "="]
        .iter()
        .filter_map(|sep| line.find(sep).map(|at| (at, sep.len())))
        .min()?;
    let key = line[..at].trim_matches(|c| c == '"' || c == '\'');
    if key.is_empty() || key.contains(char::is_whitespace) {
        return None;
    }
    let name = key.to_uppercase().replace('-', "_");
    let secret_key = SECRET_KEYS
        .iter()
        .any(|k| name == *k || name.ends_with(&format!("_{k}")));
    if patterns.is_empty() || !(secret_key || patterns.iter().any(|p| name.contains(p.as_str()))) {
        return None;
    }
    let value = line[at + len..]
        .trim()
        .trim_end_matches(',')
        .trim_matches(|c| c == '"' || c == '\'');
    (value.chars().count() >= MIN_LINE_LEN).then_some(value)
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

    /// A secret of several lines is masked whole, and, in a log, each of its
    /// lines that could be secret on its own: a log masked a line at a time,
    /// as it is written, shows none of those, and one masked whole shows the
    /// secret as one mask. An output is masked with the whole value alone.
    #[test]
    fn each_line_of_a_secret_of_several_is_masked() {
        let key = "-----BEGIN KEY-----\r\nMIIBOgIBAAJBAKj34\nGkxF0d==\nab==\n-----END KEY-----";
        let output = Redactor::build(&env(&[("TLS_PRIVATE_KEY", key)]), |_| None, None, None);
        let r = output.clone().with_lines();
        let log = format!("before\n{}\nafter\n", key.replace("\r\n", "\n"));
        let by_line: String = log
            .split_inclusive('\n')
            .map(|line| r.redact(line))
            .collect();
        assert_eq!(
            by_line, "before\n-----BEGIN KEY-----\n***\n***\nab==\n-----END KEY-----\nafter\n",
            "a line under the shortest is not masked on its own, nor the armour"
        );
        assert_eq!(r.redact(&format!("x{key}y")), "x***y");
        // An output's redactor masks the whole value alone.
        assert_eq!(output.redact("GkxF0d==\n"), "GkxF0d==\n");
        assert_eq!(output.redact(&format!("x{key}y")), "x***y");
        // A redactor given more values does too.
        let r = Redactor::default().with_values([key]).with_lines();
        assert_eq!(r.redact("GkxF0d==\n"), "***\n");
        // Wherever a line is, whatever is around it.
        assert_eq!(r.redact("    MIIBOgIBAAJBAKj34,\n"), "    ***,\n");
    }

    /// A line of a secret that is its shape, not what is secret in it, is
    /// not masked on its own: so what else a task prints with that shape,
    /// its output a later task reads, is as it was.
    #[test]
    fn a_line_that_is_a_secret_s_shape_is_not_masked_on_its_own() {
        let credential = "{\n    \"type\": \"external_account\",\n    \"credential_source\": {\n        \"file\": \"/var/run/token\"\n        \n    },\n    \"private_key\": \"k3yk3yk3yk3y\"\n}";
        let kubeconfig = "apiVersion: v1\nkind: Config\nclusters:\n- name: default\n  cluster:\n    server: https://10.0.0.1:6443";
        let r = Redactor::build(
            &env(&[("GCP_CREDENTIAL", credential), ("KUBE_TOKEN", kubeconfig)]),
            |_| None,
            None,
            None,
        )
        .with_lines();
        let output =
            "{\n    \"items\": [\n        {\n            \"name\": \"a\"\n        }\n    ]\n}\n\
                      apiVersion: v1\nkind: Config\n    name: default-token-x\n";
        assert_eq!(r.redact(output), output, "nothing of its shape is masked");
        // What could be secret in it is.
        assert_eq!(r.redact("\"private_key\": \"k3yk3yk3yk3y\""), "***");
        assert_eq!(r.redact("  server: https://10.0.0.1:6443\n"), "  ***\n");
    }

    /// Which lines of a secret could be secret: by their value where they
    /// have a key, else by their length; never the shape around them.
    #[test]
    fn a_line_could_be_secret_by_its_value() {
        for line in [
            "passphrase: correct horse battery staple",
            "client-key-data: LS0tLS1CRUdJTiBSU0E=",
            "\"private_key_id\": \"0a1b2c3d4e5f\",",
            "token = 0a1b2c3d4e5f6a7b",
            "MIIEvQIBADANBgkqhkiG9w0BAQEFAASC",
            "-----BEGIN KEY----- MIIBOgIBAAJBAKj34 -----END KEY-----",
        ] {
            assert!(secret_line(line), "{line}");
        }
        for line in [
            "",
            "}",
            "kind: Config",
            "- name: default",
            "clusters:",
            "\"credential_source\": {",
            "[default]",
            "-----BEGIN RSA PRIVATE KEY-----",
            "password: S3cr3t!",
        ] {
            assert!(!secret_line(line), "{line}");
        }
    }

    /// The value of a line of a secret of several lines whose key names a
    /// secret is masked on its own, in a log and in an output: a task that
    /// reads it out of the whole and prints it alone shows it no more than
    /// it would the whole. One whose key does not, or that is short, is not.
    #[test]
    fn a_value_whose_key_names_a_secret_is_masked_on_its_own() {
        let kubeconfig = "users:\n- name: kubernetes-admin\n  user:\n    token: abcdefghijkl\n";
        let dotenv = "DB_HOST=db.internal.example\nexport AWS_SECRET_ACCESS_KEY=wJalrXUtnFEMIK7MDENG\nAPI_TOKEN = \"tok_0123456789\"\n";
        let json = "{\n  \"type\": \"service_account\",\n  \"private_key_id\": \"0a1b2c3d4e5f\",\n  \"client_secret\": \"short\"\n}";
        let output = Redactor::build(
            &env(&[
                ("KUBECONFIG_SECRET", kubeconfig),
                ("DOTENV_SECRET", dotenv),
                ("GCP_CREDENTIAL", json),
            ]),
            |_| None,
            None,
            None,
        );
        let log = output.clone().with_lines();
        for r in [&output, &log] {
            for value in [
                "abcdefghijkl",
                "wJalrXUtnFEMIK7MDENG",
                "tok_0123456789",
                "0a1b2c3d4e5f",
            ] {
                assert_eq!(r.redact(&format!("it is {value}\n")), "it is ***\n");
            }
            for value in [
                "kubernetes-admin",
                "db.internal.example",
                "service_account",
                "short",
            ] {
                assert_eq!(
                    r.redact(&format!("it is {value}\n")),
                    format!("it is {value}\n")
                );
            }
        }
        // An output's line keeps its key; a log's is masked whole, as before.
        assert_eq!(
            output.redact("    token: abcdefghijkl\n"),
            "    token: ***\n"
        );
        assert_eq!(log.redact("    token: abcdefghijkl\n"), "    ***\n");
        // With no name masked by its name, none is masked by its key.
        let secret = EnvVar {
            name: "KUBECONFIG".to_string(),
            value: kubeconfig.to_string(),
            value_from: Some(dagron_core::dag::SecretRef {
                secret: "kubeconfig".to_string(),
                optional: false,
            }),
        };
        let r = Redactor::build(&[secret], |_| None, Some(String::new()), None);
        assert_eq!(r.redact("abcdefghijkl"), "abcdefghijkl");
    }

    /// Where a text can be cut clear of the secrets in it: where it is
    /// asked, or past the one that would be cut in two, and past one that
    /// one ends in; never inside a character.
    #[test]
    fn a_text_is_cut_clear_of_its_secrets() {
        let r = Redactor::default().with_values(["s3cr3t-token-value", "value-and-more"]);
        // The first secret is at 2..20, and the second at 15..29.
        let text = "a s3cr3t-token-value-and-more b é";
        assert_eq!(r.clear_cut(text, 1), 1);
        assert_eq!(r.clear_cut(text, 2), 2, "at its start, none is cut");
        assert_eq!(r.clear_cut(text, 8), 29, "past it, and the one it ends in");
        assert_eq!(r.clear_cut(text, 20), 29);
        assert_eq!(r.clear_cut(text, 29), 29);
        assert_eq!(r.clear_cut(text, text.len() - 1), text.len() - 2);
        assert_eq!(r.clear_cut(text, 1000), text.len());
        assert_eq!(Redactor::default().clear_cut(text, 8), 8);
        // A run of one that overlaps itself, beside a long one, is cut past
        // whole.
        let r = Redactor::default().with_values(["xxxxxxxx".to_string(), "y".repeat(64 * 1024)]);
        let run = format!("{}z", "x".repeat(256 * 1024));
        assert_eq!(r.clear_cut(&run, 7), run.len() - 1);
        assert_eq!(r.clear_cut(&run, 0), 0);
    }

    /// How far a secret begun before a text could reach into it: the most of
    /// its beginning that is an end of one, never the whole of one.
    #[test]
    fn how_far_a_secret_begun_before_reaches_in() {
        let r = Redactor::default().with_values(["s3cr3t-token-value", "abcabcab"]);
        assert_eq!(r.reach("token-value and on"), 11);
        assert_eq!(r.reach("e and on"), 1);
        assert_eq!(
            r.reach("s3cr3t-token-value and on"),
            0,
            "a whole one is not"
        );
        assert_eq!(r.reach("cabcab and"), 6);
        assert_eq!(r.reach("abcab"), 5, "where it is its own end too");
        assert_eq!(r.reach("abcabcab"), 5, "but not the whole of it");
        assert_eq!(r.reach("plain"), 0);
        assert_eq!(r.reach(""), 0);
        assert_eq!(Redactor::default().reach("token-value"), 0);
        // The cut past it, where what follows it is a whole one.
        assert_eq!(r.clear_cut("abcabcab", r.reach("abcabcab")), 8);
    }

    /// Which lines' values are masked by their key, and what of them.
    #[test]
    fn a_named_value_is_the_line_s_after_its_key() {
        let patterns: Vec<String> = DEFAULT_PATTERNS.iter().map(|s| s.to_string()).collect();
        for (line, value) in [
            ("token: abcdefghijkl", "abcdefghijkl"),
            ("  - access-key: \"AKIAABCDEFGH\",", "AKIAABCDEFGH"),
            ("password = 'correct horse'", "correct horse"),
            ("export GITHUB_TOKEN=ghp_0123456789", "ghp_0123456789"),
            ("db_password=a=b=c=d=e", "a=b=c=d=e"),
            ("\"private_key\": \"k3yk3yk3yk3y\"", "k3yk3yk3yk3y"),
            (
                "client-key-data: LS0tLS1CRUdJTiBSU0E=",
                "LS0tLS1CRUdJTiBSU0E=",
            ),
            ("\"auth\": \"dXNlcjpwYXNzd29yZA==\"", "dXNlcjpwYXNzd29yZA=="),
            ("_auth=dXNlcjpwYXNzd29yZA==", "dXNlcjpwYXNzd29yZA=="),
            ("passphrase: correct horse battery", "correct horse battery"),
            (
                "\"identitytoken\": \"eyJhbGciOiJIUzI1\"",
                "eyJhbGciOiJIUzI1",
            ),
        ] {
            assert_eq!(named_value(line, &patterns), Some(value), "{line}");
            assert_eq!(named_value(line, &[]), None, "masking by name is off");
        }
        for line in [
            "",
            "token:",
            "token: short",
            "name: kubernetes-admin",
            "[secret_section]",
            "my token = abcdefghijkl",
            "https://user:password@example.com/repository",
            "-----BEGIN PRIVATE KEY-----",
            "author: someone of a long name",
            "authority: https://login.example.com",
            "client-certificate-data: LS0tLS1CRUdJTiBDRVJU",
        ] {
            assert_eq!(named_value(line, &patterns), None, "{line}");
        }
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
            value_from: Some(dagron_core::dag::SecretRef {
                secret: "deploy-key".to_string(),
                optional: false,
            }),
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
