//! `dagron-step-sql parity` against a live Postgres: the checksum shown to catch a
//! dropped, a duplicated and a changed row, and to pass what a copy leaves equal.
//!
//! Needs a server: `TEST_DATABASE_URL` (what `scripts/ci-test.sh` starts in CI)
//! or `DAGRON_STEP_SQL_PG_URL`, e.g. `postgresql://postgres@127.0.0.1:5432/postgres`.
//! A password in the URL is passed as `SQL_PASSWORD` / `PARITY_*_PASSWORD`, as a task
//! would pass it.
//! Without one each test says so and passes, like the gateway's live tests. Every
//! test works in two schemas of its own and drops them. Never point it at a
//! database you care about: it writes.
//!
//! Both sides are Postgres here, which proves the comparison and the Postgres
//! canonical forms. The other stores' forms are rendered by the same code and
//! pinned by the unit tests in `src/parity.rs`, but they are not executed here.

#![cfg(feature = "postgres")]

use std::process::Output;

fn url() -> Option<String> {
    let url = std::env::var("DAGRON_STEP_SQL_PG_URL")
        .or_else(|_| std::env::var("TEST_DATABASE_URL"))
        .ok()
        .filter(|u| !u.trim().is_empty());
    if url.is_none() {
        eprintln!("skipping: set TEST_DATABASE_URL or DAGRON_STEP_SQL_PG_URL to run the live parity tests");
    }
    url
}

/// The URL without its password, and the password, decoded. The step refuses a DSN
/// that carries one (CI's does), and splices `SQL_PASSWORD` back in itself.
fn split_password(url: &str) -> (String, Option<String>) {
    let Some((scheme, rest)) = url.split_once("://") else { return (url.into(), None) };
    let Some((userinfo, host)) = rest.split_once('@') else { return (url.into(), None) };
    let Some((user, pw)) = userinfo.split_once(':') else { return (url.into(), None) };
    let mut raw = Vec::with_capacity(pw.len());
    let mut bytes = pw.bytes();
    while let Some(b) = bytes.next() {
        let hex = |b: Option<u8>| (b? as char).to_digit(16);
        match b {
            b'%' => {
                let mut ahead = bytes.clone();
                match (hex(ahead.next()), hex(ahead.next())) {
                    (Some(h), Some(l)) => {
                        raw.push((h * 16 + l) as u8);
                        bytes = ahead;
                    }
                    _ => raw.push(b),
                }
            }
            _ => raw.push(b),
        }
    }
    (format!("{scheme}://{user}@{host}"), Some(String::from_utf8_lossy(&raw).into_owned()))
}

/// A pair of schemas, `<name>_src` and `<name>_dst`, dropped on drop.
struct Pair {
    url: String,
    password: Option<String>,
    src: String,
    dst: String,
}

impl Pair {
    fn new(url: &str, name: &str) -> Pair {
        let base = format!("parity_{name}_{}", std::process::id());
        let (url, password) = split_password(url);
        let pair = Pair { url, password, src: format!("{base}_src"), dst: format!("{base}_dst") };
        pair.sql(&format!(
            "DROP SCHEMA IF EXISTS {s} CASCADE; DROP SCHEMA IF EXISTS {d} CASCADE; CREATE SCHEMA {s}; CREATE SCHEMA {d};",
            s = pair.src,
            d = pair.dst
        ));
        pair
    }

    /// Run a script through the step itself, so the test needs no client.
    fn sql(&self, script: &str) {
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_dagron-step-sql"))
            .env_clear()
            .env("LOG_LEVEL", "warn")
            .env("SQL_ENGINE", "postgres")
            .env("SQL_DSN", &self.url)
            .envs(self.password.iter().map(|p| ("SQL_PASSWORD", p)))
            .env("SQL_MODE", "script")
            .env("SQL_STATEMENT", script)
            .output()
            .unwrap();
        assert!(out.status.success(), "setup failed: {}\n{script}", String::from_utf8_lossy(&out.stderr));
    }

    /// The events table on both sides, the target a copy of the source.
    fn seeded(url: &str, name: &str) -> Pair {
        let pair = Pair::new(url, name);
        pair.sql(&format!(
            "CREATE TABLE {s}.events (dt date, user_id bigint, n integer, amount numeric(10,2), ts timestamp, \
               tstz timestamptz, ok boolean, score double precision, note text);
             INSERT INTO {s}.events VALUES
               ('2026-06-20', 1, 3, 1.50, '2026-06-20 12:00:00.123456', '2026-06-20 12:00:00+02', true, 0.1, 'a|b'),
               ('2026-06-20', 2, NULL, NULL, NULL, NULL, NULL, NULL, NULL),
               ('2026-06-20', 3, 7, 10.00, '2026-06-20 00:00:00', '2026-06-20 00:00:00+00', false, 1e20, 'ünïcode'),
               ('2026-06-21', 4, 1, 2.25, '2026-06-21 01:02:03', '2026-06-21 01:02:03+00', true, -2.5, 'other day');
             CREATE TABLE {d}.events AS SELECT * FROM {s}.events;",
            s = pair.src,
            d = pair.dst
        ));
        pair
    }

    fn parity(&self, extra: &[(&str, &str)]) -> Output {
        let pred = "dt IN (DATE '2026-06-20')";
        std::process::Command::new(env!("CARGO_BIN_EXE_dagron-step-sql"))
            .arg("parity")
            .env_clear()
            .env("LOG_LEVEL", "warn")
            .envs([
                ("PARITY_SOURCE_ENGINE", "postgres"),
                ("PARITY_SOURCE_DSN", self.url.as_str()),
                ("PARITY_SOURCE_SCHEMA", self.src.as_str()),
                ("PARITY_SOURCE_TABLE", "events"),
                ("PARITY_SOURCE_WHERE", pred),
                ("PARITY_TARGET_ENGINE", "postgres"),
                ("PARITY_TARGET_DSN", self.url.as_str()),
                ("PARITY_TARGET_SCHEMA", self.dst.as_str()),
                ("PARITY_TARGET_TABLE", "events"),
                ("PARITY_TARGET_WHERE", pred),
            ])
            .envs(self.password.iter().flat_map(|p| [("PARITY_SOURCE_PASSWORD", p), ("PARITY_TARGET_PASSWORD", p)]))
            .envs(extra.iter().copied())
            .output()
            .unwrap()
    }
}

impl Drop for Pair {
    fn drop(&mut self) {
        let _ = std::process::Command::new(env!("CARGO_BIN_EXE_dagron-step-sql"))
            .env_clear()
            .env("SQL_ENGINE", "postgres")
            .env("SQL_DSN", &self.url)
            .envs(self.password.iter().map(|p| ("SQL_PASSWORD", p)))
            .env("SQL_MODE", "script")
            .env("SQL_STATEMENT", format!("DROP SCHEMA IF EXISTS {} CASCADE; DROP SCHEMA IF EXISTS {} CASCADE;", self.src, self.dst))
            .output();
    }
}

fn stderr(out: &Output) -> String {
    String::from_utf8_lossy(&out.stderr).into_owned()
}

fn assert_equal(out: &Output) {
    assert_eq!(out.status.code(), Some(0), "expected equal: {}", stderr(out));
    let stdout = String::from_utf8_lossy(&out.stdout);
    assert!(stdout.lines().any(|l| l.starts_with("equal: 3 row(s)")), "{stdout}");
}

fn assert_different(out: &Output, needle: &str) {
    assert_eq!(out.status.code(), Some(2), "expected different (exit 2): {}", stderr(out));
    assert!(stderr(out).contains(needle), "expected {needle:?} in: {}", stderr(out));
}

#[test]
fn a_faithful_copy_is_shown_equal() {
    let Some(url) = url() else { return };
    let pair = Pair::seeded(&url, "equal");
    assert_equal(&pair.parity(&[]));
}

#[test]
fn a_changed_value_is_caught() {
    let Some(url) = url() else { return };
    let pair = Pair::seeded(&url, "changed");
    pair.sql(&format!("UPDATE {}.events SET amount = 1.51 WHERE user_id = 1", pair.dst));
    assert_different(&pair.parity(&[]), "checksums differ");
}

#[test]
fn a_duplicated_row_is_caught() {
    let Some(url) = url() else { return };
    let pair = Pair::seeded(&url, "duplicated");
    pair.sql(&format!("INSERT INTO {d}.events SELECT * FROM {d}.events WHERE user_id = 3", d = pair.dst));
    assert_different(&pair.parity(&[]), "the source has 3 row(s) and the target 4");
}

#[test]
fn a_dropped_row_is_caught() {
    let Some(url) = url() else { return };
    let pair = Pair::seeded(&url, "dropped");
    pair.sql(&format!("DELETE FROM {}.events WHERE user_id = 2", pair.dst));
    assert_different(&pair.parity(&[]), "the source has 3 row(s) and the target 2");
}

/// The swap that keeps both counts: one row's NULL became an empty string.
#[test]
fn null_is_not_the_empty_string() {
    let Some(url) = url() else { return };
    let pair = Pair::seeded(&url, "null");
    pair.sql(&format!("UPDATE {}.events SET note = '' WHERE user_id = 2", pair.dst));
    assert_different(&pair.parity(&[]), "checksums differ");
}

/// Rows outside the compared partition are not compared.
#[test]
fn only_the_partition_named_is_compared() {
    let Some(url) = url() else { return };
    let pair = Pair::seeded(&url, "scoped");
    pair.sql(&format!("UPDATE {}.events SET note = 'changed' WHERE dt = '2026-06-21'", pair.dst));
    assert_equal(&pair.parity(&[]));
    let whole = pair.parity(&[("PARITY_SOURCE_WHERE", ""), ("PARITY_TARGET_WHERE", "")]);
    assert_eq!(whole.status.code(), Some(2), "the whole relation does differ: {}", stderr(&whole));
}

/// A target created with its columns in another order, and wider types, holds
/// the same values: compared by name, at the larger scale.
#[test]
fn column_order_and_a_wider_type_do_not_matter() {
    let Some(url) = url() else { return };
    let pair = Pair::seeded(&url, "reordered");
    pair.sql(&format!(
        "DROP TABLE {d}.events;
         CREATE TABLE {d}.events (note text, tstz timestamptz, ts timestamp, score double precision, ok boolean,
           amount numeric(12,4), n bigint, user_id integer, dt date);
         INSERT INTO {d}.events (note, tstz, ts, score, ok, amount, n, user_id, dt)
           SELECT note, tstz, ts, score, ok, amount, n, user_id, dt FROM {s}.events;",
        s = pair.src,
        d = pair.dst
    ));
    assert_equal(&pair.parity(&[]));
}

#[test]
fn a_missing_column_is_a_difference_and_is_named() {
    let Some(url) = url() else { return };
    let pair = Pair::seeded(&url, "missing");
    pair.sql(&format!("ALTER TABLE {}.events DROP COLUMN score", pair.dst));
    assert_different(&pair.parity(&[]), "`score` is on the source only");
}

#[test]
fn a_type_it_cannot_compare_is_refused_not_guessed() {
    let Some(url) = url() else { return };
    let pair = Pair::seeded(&url, "jsonb");
    pair.sql(&format!(
        "ALTER TABLE {s}.events ADD COLUMN doc jsonb; ALTER TABLE {d}.events ADD COLUMN doc jsonb;",
        s = pair.src,
        d = pair.dst
    ));
    let out = pair.parity(&[]);
    assert_eq!(out.status.code(), Some(1), "not compared is not 'different': {}", stderr(&out));
    assert!(stderr(&out).contains("doc (jsonb)"), "{}", stderr(&out));
}

#[test]
fn the_verdict_is_recorded_in_the_artifact() {
    let Some(url) = url() else { return };
    let pair = Pair::seeded(&url, "report");
    pair.sql(&format!("DELETE FROM {}.events WHERE user_id = 2", pair.dst));
    let dir = std::env::temp_dir().join(format!("dagron-step-sql-parity-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let out = pair.parity(&[("DAGRON_ARTIFACTS", dir.to_str().unwrap())]);
    assert_eq!(out.status.code(), Some(2));
    let report: serde_json::Value = serde_json::from_str(&std::fs::read_to_string(dir.join("parity.json")).unwrap()).unwrap();
    assert_eq!(report["verdict"], "different");
    assert_eq!(report["reason"], "row_count");
    assert_eq!(report["source"]["row_count"], "3");
    assert_eq!(report["target"]["row_count"], "2");
    let compared: Vec<&str> = report["columns"].as_array().unwrap().iter().map(|c| c["compared_as"].as_str().unwrap()).collect();
    assert!(compared.contains(&"exact at 2 place(s)") && compared.contains(&"rounded to 6 place(s)"), "{compared:?}");
    std::fs::remove_dir_all(&dir).ok();
}
