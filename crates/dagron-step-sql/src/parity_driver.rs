//! `dagron-step-sql parity`: is one relation's partition the same on two stores?
//!
//! Each side is read from the environment under its own prefix, with the same
//! variables the step reads under `SQL_`: `PARITY_SOURCE_ENGINE`, `…_DSN`,
//! `…_PASSWORD`, `…_USER`, `…_TOKEN_TYPE`, `…_TIMEOUT_SECS`, and `PARITY_TARGET_…`
//! likewise. Then what to compare: `…_TABLE` (required), `…_SCHEMA`, `…_RELATION`
//! (the relation as that side's SQL writes it; composed from the two when unset)
//! and `…_WHERE` (the rows, as a predicate in that side's dialect).
//!
//! Both sides run through the step's own transports, so every rule they keep holds
//! here too: credentials masked by name, the deadline, a bounded read.
//!
//! The exit status is the verdict:
//!
//! | Exit | Meaning |
//! |---|---|
//! | 0 | **shown equal**: same columns, same row count, same checksum. One line on stdout says so |
//! | 2 | **shown different**: counts, checksums or columns differ. stderr says which |
//! | 1 | **not compared**: a side could not be read, or holds a type that cannot be compared honestly |
//!
//! With `$DAGRON_ARTIFACTS` set, `parity.json` records both sides' numbers, the
//! columns and how each was compared, whatever the verdict. It is the record a
//! cut-over is decided on.

use anyhow::{Context, Result};
use dagron_step_sql::parity::{
    align, checksum_sql, columns, discovery_sql, Fingerprint, Pair, Side, Which, DEFAULT_FLOAT_DIGITS,
    MAX_PLACES,
};
use dagron_step_sql::{Budget, Mode, Settings};
use serde_json::{json, Map, Value};

use crate::RowSink;

/// Exit status for "shown different", distinct from 1, "could not compare".
const DIFFERENT: i32 = 2;

pub async fn main() -> Result<()> {
    let get = |k: &str| std::env::var(k).ok();
    let float_digits = match get("PARITY_FLOAT_DIGITS").filter(|s| !s.trim().is_empty()) {
        None => DEFAULT_FLOAT_DIGITS,
        Some(raw) => {
            let d: u32 = raw.trim().parse().with_context(|| format!("PARITY_FLOAT_DIGITS must be an integer, got '{raw}'"))?;
            anyhow::ensure!(d <= MAX_PLACES, "PARITY_FLOAT_DIGITS is at most {MAX_PLACES}, got {d}");
            d
        }
    };
    let (source_conn, source) = side("PARITY_SOURCE_", &get)?;
    let (target_conn, target) = side("PARITY_TARGET_", &get)?;

    // Discovery, then the checksum, each on both sides at once.
    let (source_discovery, target_discovery) = (discovery_sql(&source), discovery_sql(&target));
    let (source_rows, target_rows) = tokio::try_join!(
        query(&source_conn, &source_discovery),
        query(&target_conn, &target_discovery),
    )
    .context("reading each side's columns")?;
    let source_cols = columns(&source, &source_rows).context("reading the source's columns")?;
    let target_cols = columns(&target, &target_rows).context("reading the target's columns")?;

    let pairs = match align(&source_cols, &target_cols, float_digits) {
        Ok(pairs) => pairs,
        Err(problems) => {
            let report = json!({
                "verdict": "different",
                "reason": "columns",
                "problems": problems,
                "source": describe(&source, None),
                "target": describe(&target, None),
            });
            write_report(&report)?;
            different(&format!(
                "the two relations do not have the same columns, so they are not shown equal:\n  - {}",
                problems.join("\n  - ")
            ));
        }
    };

    let source_sql = checksum_sql(&source, &pairs, Which::Source);
    let target_sql = checksum_sql(&target, &pairs, Which::Target);
    tracing::debug!(source = %source_sql, target = %target_sql, "checksum queries");
    let (s, t) = tokio::try_join!(query(&source_conn, &source_sql), query(&target_conn, &target_sql))
        .context("fingerprinting each side")?;
    let s = Fingerprint::from_rows(&s).context("reading the source's fingerprint")?;
    let t = Fingerprint::from_rows(&t).context("reading the target's fingerprint")?;

    let equal = s == t;
    let report = json!({
        "verdict": if equal { "equal" } else { "different" },
        "reason": if equal { Value::Null } else if s.row_count != t.row_count { "row_count".into() } else { "checksum".into() },
        "source": describe(&source, Some(&s)),
        "target": describe(&target, Some(&t)),
        "columns": pairs.iter().map(column_report).collect::<Vec<_>>(),
        "float_digits": float_digits,
    });
    write_report(&report)?;
    if !equal {
        let why = if s.row_count != t.row_count {
            format!("the source has {} row(s) and the target {}", s.row_count, t.row_count)
        } else {
            format!(
                "both have {} row(s), but their checksums differ (source {}, target {}): at least one \
                 value is not the same",
                s.row_count, s.checksum, t.checksum
            )
        };
        different(&format!("{} and {} are not shown equal: {why}.", source.relation, target.relation));
    }
    println!("equal: {} row(s), checksum {}", s.row_count, s.checksum);
    Ok(())
}

/// One side: how to reach it, and what to compare there.
fn side(prefix: &'static str, get: &impl Fn(&str) -> Option<String>) -> Result<(Settings, Side)> {
    let conn = Settings::from_env(prefix, get)?;
    let var = |name: &str| get(&format!("{prefix}{name}")).map(|v| v.trim().to_string()).filter(|v| !v.is_empty());
    let table = var("TABLE").with_context(|| format!("{prefix}TABLE is required: the table to compare"))?;
    let side = Side::new(conn.engine, var("SCHEMA"), table, var("RELATION"), var("WHERE"))
        .with_context(|| format!("reading the {prefix}… side"))?;
    Ok((conn, side))
}

/// Run one query and hand back its rows, through the step's own transports.
///
/// The budget is generous and still a budget: discovery returns a row per column,
/// the checksum one row, and a result past these limits is not a parity query's.
async fn query(conn: &Settings, sql: &str) -> Result<Vec<Map<String, Value>>> {
    let mut sink = RowSink::memory();
    crate::run(conn, sql, Mode::Rows, Some(&mut sink), Budget::new(10_000, 8 * 1024 * 1024))
        .await
        .with_context(|| format!("querying {:?} ({})", conn.engine, conn.var("DSN")))?;
    sink.into_lines()
        .iter()
        .map(|line| match serde_json::from_str::<Value>(line) {
            Ok(Value::Object(row)) => Ok(row),
            _ => anyhow::bail!("a result row is not a JSON object: {line}"),
        })
        .collect()
}

fn describe(side: &Side, fp: Option<&Fingerprint>) -> Value {
    json!({
        "engine": format!("{:?}", side.engine).to_ascii_lowercase(),
        "relation": side.relation,
        "where": side.predicate,
        "row_count": fp.map(|f| f.row_count.clone()),
        "checksum": fp.map(|f| f.checksum.clone()),
    })
}

fn column_report(p: &Pair) -> Value {
    json!({
        "source": p.source.name,
        "source_type": p.source.data_type,
        "target": p.target.name,
        "target_type": p.target.data_type,
        "compared_as": p.compare.to_string(),
    })
}

fn write_report(report: &Value) -> Result<()> {
    let Some(dir) = std::env::var("DAGRON_ARTIFACTS").ok().filter(|d| !d.trim().is_empty()) else {
        return Ok(());
    };
    let path = std::path::Path::new(dir.trim()).join("parity.json");
    let body = serde_json::to_string_pretty(report).expect("a JSON value serializes");
    std::fs::write(&path, body).with_context(|| format!("writing {}", path.display()))
}

/// The verdict "shown different": said on stderr, and exit 2, so a run can tell it
/// from a check that could not be made.
fn different(message: &str) -> ! {
    eprintln!("parity: {message}");
    std::process::exit(DIFFERENT);
}
