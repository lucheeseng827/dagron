//! The parity check: is this partition the same on both stores?
//!
//! A warehouse move is done when every partition is *shown* equal on the old store
//! and the new one. This module is the pure half of showing it. It writes the SQL
//! each side runs and says what the answers mean. The binary's `parity` subcommand
//! runs it (`dagron-step-sql parity`).
//!
//! ## What is compared
//!
//! Two numbers per side, computed where the data is, so nothing but them crosses
//! the network:
//!
//! * **the row count**, which catches a dropped row and a duplicated one;
//! * **an order-independent checksum**: the sum, over the rows, of 60 bits of the
//!   MD5 of each row's canonical text. A sum does not care what order the rows come
//!   in, and unlike an XOR it counts a duplicated row twice. Any changed value
//!   changes the row's text and so, short of a 2⁻⁶⁰ collision, the sum.
//!
//! ## Canonical text, the hard part
//!
//! Two stores print the same value differently. Postgres prints `1.50`, BigQuery
//! `1.5`; a timestamp has as many spellings as there are warehouses. So a value is
//! never compared as the store prints it. Each side's columns are read from its
//! `information_schema`, each column is put in a **family**, and each family is
//! rendered in a form every store spells the same way:
//!
//! | Family | Canonical text |
//! |---|---|
//! | exact numeric (integers, decimals) | the value × 10^scale, as an integer: `1.50` at scale 2 is `150` |
//! | float | the same, rounded to `PARITY_FLOAT_DIGITS` places (default 6) |
//! | boolean | `1` or `0` |
//! | date | days since 1970-01-01 |
//! | timestamp | microseconds since 1970-01-01 00:00:00 UTC |
//! | text | the text itself |
//!
//! Integers are the one textual form every store agrees on, which is why most
//! families end in one. Each cell is written `length:text`, or `N` for NULL, so no
//! value can run into the next, NULL never equals the empty string, and a row's
//! text decodes one way only. The columns are taken in the order of their
//! lower-cased names, not their position, so a target whose columns were created in
//! another order still compares.
//!
//! ## Where it is strict
//!
//! * The two sides must have the **same columns** (by name, ignoring case), each of
//!   a family the other's can be compared with. A missing column is a difference,
//!   not something to skip.
//! * A type outside the families (JSON, arrays, structs, binary, time of day,
//!   intervals) is **refused**, never cast to text and hoped for. A comparison it
//!   cannot make honestly, it does not make.
//!
//! ## Where it is not exact, by design
//!
//! * Floats are compared at `PARITY_FLOAT_DIGITS` places, and stores round a tie
//!   (exactly …5 at the last place) differently. A float copied through Parquet is
//!   bit-for-bit the same, so a copy compares equal; a change below that place is
//!   not seen.
//! * An exact numeric is compared at the larger of the two scales, at most 18
//!   places. A Postgres `numeric` with no declared scale has none to read, so it is
//!   compared like a float.
//! * A timestamp with no time zone is compared as if it were UTC on both sides. A
//!   column that is zoned on one side and not on the other compares instants against
//!   wall-clock readings, and differs unless the readings were UTC.

use std::collections::BTreeMap;
use std::fmt;

use anyhow::{bail, Result};
use serde_json::{Map, Value};

use crate::Engine;

/// The most decimal places a numeric is compared at. The canonical form is the
/// value × 10^places in a 38-digit decimal, so every place taken here is a digit of
/// integer part given up.
pub const MAX_PLACES: u32 = 18;
/// `PARITY_FLOAT_DIGITS` when unset.
pub const DEFAULT_FLOAT_DIGITS: u32 = 6;

/// The stores a parity check speaks: the five dialects Freshet renders for.
fn supported(engine: Engine) -> Result<()> {
    match engine {
        Engine::Postgres | Engine::Redshift | Engine::Snowflake | Engine::BigQuery | Engine::Databricks => Ok(()),
        other => bail!(
            "the parity check runs on postgres, redshift, snowflake, bigquery and databricks, the \
             warehouses Freshet plans for; {other:?} is not one of them. Its canonical forms are \
             written per store, and none is written for this one."
        ),
    }
}

/// One side of a comparison: which relation, which rows of it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Side {
    pub engine: Engine,
    /// The relation as this side's SQL writes it (`analytics.events`). Composed
    /// from `schema` and `table` when not given.
    pub relation: String,
    /// The schema, as the move named it: `analytics`, or two parts where the store
    /// nests (`db.schema` on Snowflake, `catalog.schema` on Databricks,
    /// `project.dataset` on BigQuery). `None` is the session's current schema.
    pub schema: Option<String>,
    pub table: String,
    /// The rows compared, as a SQL predicate in this side's dialect, such as
    /// `dt IN (DATE '2026-06-20')`. `None` compares the whole relation.
    pub predicate: Option<String>,
}

impl Side {
    pub fn new(engine: Engine, schema: Option<String>, table: String, relation: Option<String>, predicate: Option<String>) -> Result<Side> {
        supported(engine)?;
        for name in schema.iter().flat_map(|s| s.split('.')).chain(std::iter::once(table.as_str())) {
            if name.is_empty() || name.chars().any(|c| matches!(c, '\'' | '"' | '`' | '\\') || c.is_control()) {
                bail!(
                    "'{name}' cannot name a relation here: it is empty or holds a quote, a backslash \
                     or a control character, and it goes into a string literal on every store"
                );
            }
        }
        if engine == Engine::BigQuery && schema.is_none() {
            bail!(
                "a BigQuery side needs its dataset (`dataset` or `project.dataset`): BigQuery's \
                 INFORMATION_SCHEMA is read per dataset, so there is no current one to fall back on"
            );
        }
        let relation = match relation.filter(|r| !r.trim().is_empty()) {
            Some(r) => r,
            None => {
                let mut parts: Vec<String> = schema.iter().flat_map(|s| s.split('.')).map(|p| ident(engine, p)).collect();
                parts.push(ident(engine, &table));
                parts.join(".")
            }
        };
        Ok(Side { engine, relation, schema, table, predicate: predicate.filter(|p| !p.trim().is_empty()) })
    }
}

/// A plain identifier: what a store folds rather than takes literally.
fn is_plain(name: &str) -> bool {
    let mut chars = name.chars();
    chars.next().is_some_and(|c| c.is_ascii_alphabetic() || c == '_')
        && chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
}

/// An identifier as this store's SQL writes it: bare when plain, quoted otherwise.
/// The same rule Freshet renders by, so a composed relation resolves the way the
/// move's own statements do.
fn ident(engine: Engine, name: &str) -> String {
    if is_plain(name) {
        return name.to_string();
    }
    match engine {
        Engine::BigQuery | Engine::Databricks => format!("`{name}`"),
        _ => format!("\"{name}\""),
    }
}

/// A column exactly as the store keeps it, always quoted. A stored name is the
/// truth here, and quoting it is the one way to refer to it that no store folds:
/// bare, a Postgres column created as `"Mixed"` would be read as `mixed`, and one
/// named `user` would be a syntax error.
fn quoted(engine: Engine, name: &str) -> String {
    match engine {
        Engine::BigQuery | Engine::Databricks => format!("`{name}`"),
        _ => format!("\"{name}\""),
    }
}

/// The name a store keeps in `information_schema` for an identifier written the way
/// [`ident`] writes it. A plain name is folded, lower-case on Postgres and Redshift,
/// upper-case on Snowflake. Databricks folds every name to lower case, and BigQuery
/// keeps table names exactly as written.
fn stored(engine: Engine, name: &str) -> String {
    match engine {
        Engine::Postgres | Engine::Redshift if is_plain(name) => name.to_ascii_lowercase(),
        Engine::Snowflake if is_plain(name) => name.to_ascii_uppercase(),
        Engine::Databricks => name.to_ascii_lowercase(),
        _ => name.to_string(),
    }
}

/// The SQL that lists `side`'s columns, one row per column, every value as text so
/// any transport can read it.
pub fn discovery_sql(side: &Side) -> String {
    let e = side.engine;
    let lit = |s: &str| format!("'{}'", stored(e, s));
    let parts: Vec<&str> = side.schema.as_deref().map(|s| s.split('.').collect()).unwrap_or_default();
    // (where information_schema lives, the schema to match)
    let (catalog, schema) = match parts.as_slice() {
        [] => (None, None),
        [s] => (None, Some(*s)),
        [c, s, ..] => (Some(*c), Some(*s)),
    };
    let current = match e {
        Engine::Snowflake => "CURRENT_SCHEMA()",
        _ => "current_schema()",
    };
    let schema_match = schema.map(lit).unwrap_or_else(|| current.to_string());
    match e {
        Engine::Postgres | Engine::Redshift => {
            let text = if e == Engine::Postgres { "text" } else { "varchar" };
            format!(
                "SELECT CAST(table_schema AS {text}) AS table_schema, CAST(table_name AS {text}) AS table_name, \
                 CAST(column_name AS {text}) AS column_name, CAST(data_type AS {text}) AS data_type, \
                 CAST(numeric_scale AS {text}) AS numeric_scale\nFROM information_schema.columns\n\
                 WHERE table_schema = {schema_match} AND table_name = {}\nORDER BY ordinal_position",
                lit(&side.table)
            )
        }
        Engine::Snowflake => {
            let view = match catalog {
                Some(db) => format!("{}.information_schema.columns", ident(e, db)),
                None => "information_schema.columns".into(),
            };
            format!(
                "SELECT table_schema, table_name, column_name, data_type, \
                 TO_VARCHAR(numeric_scale) AS numeric_scale\nFROM {view}\n\
                 WHERE table_schema = {schema_match} AND table_name = {}\nORDER BY ordinal_position",
                lit(&side.table)
            )
        }
        Engine::BigQuery => {
            let mut view: Vec<String> = parts.iter().map(|p| ident(e, p)).collect();
            view.push("INFORMATION_SCHEMA".into());
            view.push("COLUMNS".into());
            format!(
                "SELECT table_schema, table_name, column_name, data_type, CAST(NULL AS STRING) AS numeric_scale\n\
                 FROM {}\nWHERE table_name = {}\nORDER BY ordinal_position",
                view.join("."),
                lit(&side.table)
            )
        }
        Engine::Databricks => {
            let view = match catalog {
                Some(c) => format!("{}.information_schema.columns", ident(e, c)),
                None => "information_schema.columns".into(),
            };
            format!(
                "SELECT table_schema, table_name, column_name, data_type, \
                 CAST(numeric_scale AS STRING) AS numeric_scale\nFROM {view}\n\
                 WHERE table_schema = {schema_match} AND table_name = {}\nORDER BY ordinal_position",
                lit(&side.table)
            )
        }
        _ => unreachable!("refused in Side::new"),
    }
}

/// How a column's values are compared, which decides its canonical text.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Family {
    /// Integers and decimals. `None` is a scale the store does not declare.
    Exact { scale: Option<u32> },
    Float,
    Boolean,
    Date,
    /// `zoned`: an instant (`timestamptz`, BigQuery `TIMESTAMP`) rather than a
    /// wall-clock reading. BigQuery needs to know which to convert.
    Timestamp { zoned: bool },
    Text,
}

impl fmt::Display for Family {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Family::Exact { scale: Some(s) } => write!(f, "exact numeric (scale {s})"),
            Family::Exact { scale: None } => f.write_str("exact numeric (no declared scale)"),
            Family::Float => f.write_str("float"),
            Family::Boolean => f.write_str("boolean"),
            Family::Date => f.write_str("date"),
            Family::Timestamp { .. } => f.write_str("timestamp"),
            Family::Text => f.write_str("text"),
        }
    }
}

/// One column, as one side has it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Column {
    /// As the store keeps it, which is how this side's SQL refers to it.
    pub name: String,
    pub data_type: String,
    pub family: Family,
}

/// Put a store's type name in its family, or refuse it.
pub fn classify(engine: Engine, data_type: &str, scale: Option<u32>) -> Option<Family> {
    let t = data_type.trim().to_ascii_lowercase();
    // `NUMERIC(10, 2)` and `STRING(10)` carry their parameters in the name.
    let base = t.split('(').next().unwrap_or("").trim();
    let paren_scale = || {
        t.split_once('(')
            .and_then(|(_, rest)| rest.trim_end_matches(')').split(',').nth(1))
            .and_then(|s| s.trim().parse::<u32>().ok())
    };
    Some(match (engine, base) {
        (_, "smallint" | "integer" | "int" | "bigint" | "tinyint" | "int2" | "int4" | "int8" | "int64") => {
            Family::Exact { scale: Some(0) }
        }
        (Engine::BigQuery, "numeric") => Family::Exact { scale: Some(paren_scale().unwrap_or(9)) },
        (Engine::BigQuery, "bignumeric") => Family::Exact { scale: Some(paren_scale().unwrap_or(38)) },
        (_, "numeric" | "decimal" | "number") => Family::Exact { scale: scale.or_else(paren_scale) },
        (_, "real" | "double precision" | "double" | "float" | "float4" | "float8" | "float64") => Family::Float,
        (_, "boolean" | "bool") => Family::Boolean,
        (_, "date") => Family::Date,
        (Engine::BigQuery, "timestamp") => Family::Timestamp { zoned: true },
        (Engine::BigQuery, "datetime") => Family::Timestamp { zoned: false },
        (_, "timestamp with time zone" | "timestamptz" | "timestamp_tz" | "timestamp_ltz") => {
            Family::Timestamp { zoned: true }
        }
        (_, "timestamp without time zone" | "timestamp" | "timestamp_ntz") => Family::Timestamp { zoned: false },
        (_, "text" | "character varying" | "varchar" | "character" | "char" | "string" | "bpchar" | "name" | "uuid" | "citext") => {
            Family::Text
        }
        _ => return None,
    })
}

/// Read the rows [`discovery_sql`] returned into columns.
///
/// Refuses what cannot be compared honestly: no columns (no such relation, as far
/// as this side can see), columns of more than one relation (a name that matched
/// twice), two columns whose names differ only in case, and a type outside the
/// families.
pub fn columns(side: &Side, rows: &[Map<String, Value>]) -> Result<Vec<Column>> {
    let text = |row: &Map<String, Value>, key: &str| -> Option<String> {
        // Stores disagree on the case of a result's column names.
        row.iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(key))
            .and_then(|(_, v)| match v {
                Value::String(s) => Some(s.clone()),
                Value::Null => None,
                other => Some(other.to_string()),
            })
    };
    if rows.is_empty() {
        bail!(
            "{:?} shows no columns for {}: the relation does not exist where this side looked, or \
             this side cannot see it",
            side.engine,
            side.relation
        );
    }
    let mut relations: Vec<String> = rows
        .iter()
        .map(|r| format!("{}.{}", text(r, "table_schema").unwrap_or_default(), text(r, "table_name").unwrap_or_default()))
        .collect();
    relations.sort();
    relations.dedup();
    if relations.len() > 1 {
        bail!(
            "{} matched more than one relation ({}). Name the schema, or the table as the store \
             spells it.",
            side.relation,
            relations.join(", ")
        );
    }
    let mut seen: BTreeMap<String, String> = BTreeMap::new();
    let mut unsupported = Vec::new();
    let mut out = Vec::new();
    for row in rows {
        let name = text(row, "column_name").unwrap_or_default();
        let data_type = text(row, "data_type").unwrap_or_default();
        let scale = text(row, "numeric_scale").and_then(|s| s.trim().parse::<u32>().ok());
        if name.is_empty() || name.chars().any(|c| matches!(c, '"' | '`') || c.is_control()) {
            bail!("{} has a column named {name:?}, which cannot be quoted on every store", side.relation);
        }
        if let Some(other) = seen.insert(name.to_ascii_lowercase(), name.clone()) {
            bail!("{} has two columns whose names differ only in case ({other}, {name}), which cannot be paired with the other side's", side.relation);
        }
        match classify(side.engine, &data_type, scale) {
            Some(family) => out.push(Column { name, data_type, family }),
            None => unsupported.push(format!("{name} ({data_type})")),
        }
    }
    if !unsupported.is_empty() {
        bail!(
            "{} has columns whose type the parity check cannot compare honestly: {}. Only exact \
             and float numerics, booleans, dates, timestamps and text are compared; anything else \
             is refused rather than cast to text and hoped for.",
            side.relation,
            unsupported.join(", ")
        );
    }
    Ok(out)
}

/// How a pair of columns, one per side, is compared.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CompareAs {
    /// × 10^places, as an integer. `round` when the value may carry more places
    /// than that (a float, or a numeric with no declared scale).
    Scaled { places: u32, round: bool },
    Boolean,
    Date,
    Timestamp,
    Text,
}

impl fmt::Display for CompareAs {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            CompareAs::Scaled { places, round: false } => write!(f, "exact at {places} place(s)"),
            CompareAs::Scaled { places, round: true } => write!(f, "rounded to {places} place(s)"),
            CompareAs::Boolean => f.write_str("boolean"),
            CompareAs::Date => f.write_str("days since 1970-01-01"),
            CompareAs::Timestamp => f.write_str("microseconds since 1970-01-01 UTC"),
            CompareAs::Text => f.write_str("text"),
        }
    }
}

/// One column of the comparison: its name on each side, and how it is compared.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Pair {
    pub source: Column,
    pub target: Column,
    pub compare: CompareAs,
}

/// Pair the two sides' columns by name, ignoring case, in the order of their
/// lower-cased names. `Err` lists every difference: a column only one side has, and
/// a column whose families cannot be compared. Either means the relations are not
/// the same shape, so they are not shown equal.
pub fn align(source: &[Column], target: &[Column], float_digits: u32) -> Result<Vec<Pair>, Vec<String>> {
    let by_name = |cols: &[Column]| -> BTreeMap<String, Column> {
        cols.iter().map(|c| (c.name.to_ascii_lowercase(), c.clone())).collect()
    };
    let (s, t) = (by_name(source), by_name(target));
    let float_digits = float_digits.min(MAX_PLACES);
    let mut problems = Vec::new();
    let mut pairs = Vec::new();
    for (key, sc) in &s {
        let Some(tc) = t.get(key) else {
            problems.push(format!("`{}` is on the source only", sc.name));
            continue;
        };
        let compare = match (sc.family, tc.family) {
            (Family::Exact { scale: Some(a) }, Family::Exact { scale: Some(b) }) => {
                let places = a.max(b);
                CompareAs::Scaled { places: places.min(MAX_PLACES), round: places > MAX_PLACES }
            }
            (Family::Exact { .. } | Family::Float, Family::Exact { .. } | Family::Float) => {
                CompareAs::Scaled { places: float_digits, round: true }
            }
            (Family::Boolean, Family::Boolean) => CompareAs::Boolean,
            (Family::Date, Family::Date) => CompareAs::Date,
            (Family::Timestamp { .. }, Family::Timestamp { .. }) => CompareAs::Timestamp,
            (Family::Text, Family::Text) => CompareAs::Text,
            (a, b) => {
                problems.push(format!("`{}` is {a} on the source and {b} on the target", sc.name));
                continue;
            }
        };
        pairs.push(Pair { source: sc.clone(), target: tc.clone(), compare });
    }
    for (key, tc) in &t {
        if !s.contains_key(key) {
            problems.push(format!("`{}` is on the target only", tc.name));
        }
    }
    if problems.is_empty() {
        Ok(pairs)
    } else {
        Err(problems)
    }
}

/// The type a store's text is, for `CAST(… AS …)`.
fn text_type(e: Engine) -> &'static str {
    match e {
        Engine::Postgres => "TEXT",
        Engine::BigQuery | Engine::Databricks => "STRING",
        _ => "VARCHAR",
    }
}

/// A decimal wide enough to sum 60-bit integers over any partition.
fn wide_decimal(e: Engine) -> &'static str {
    match e {
        Engine::Postgres => "NUMERIC(38,0)",
        Engine::Snowflake => "NUMBER(38,0)",
        Engine::BigQuery => "BIGNUMERIC",
        _ => "DECIMAL(38,0)",
    }
}

/// `v`'s canonical text, by [`CompareAs`], in this store's SQL. NULL stays NULL;
/// [`cell`] encodes it.
fn canonical(e: Engine, v: &str, compare: CompareAs, family: Family) -> String {
    let t = text_type(e);
    match compare {
        CompareAs::Scaled { places, round: _ } => {
            // Cast to a decimal of exactly `places` places (which rounds a value
            // carrying more), then shift the point away: an integer is the one
            // numeric text every store prints alike. BigQuery's NUMERIC cannot hold
            // 38 digits at a small scale, its BIGNUMERIC can.
            let dec = |p: u32| match e {
                Engine::BigQuery => format!("BIGNUMERIC(38, {p})"),
                Engine::Snowflake => format!("NUMBER(38, {p})"),
                _ => format!("DECIMAL(38, {p})"),
            };
            if places == 0 {
                return format!("CAST(CAST({v} AS {}) AS {t})", dec(0));
            }
            let pow = format!("1{}", "0".repeat(places as usize));
            format!("CAST(CAST(CAST({v} AS {}) * {pow} AS {}) AS {t})", dec(places), dec(0))
        }
        CompareAs::Boolean => format!("CASE WHEN {v} THEN '1' WHEN NOT {v} THEN '0' END"),
        CompareAs::Date => match e {
            Engine::Postgres => format!("CAST({v} - DATE '1970-01-01' AS {t})"),
            Engine::Redshift | Engine::Snowflake => format!("CAST(DATEDIFF(day, DATE '1970-01-01', {v}) AS {t})"),
            Engine::BigQuery => format!("CAST(UNIX_DATE({v}) AS {t})"),
            _ => format!("CAST(unix_date({v}) AS {t})"),
        },
        CompareAs::Timestamp => match e {
            Engine::Postgres => {
                format!("CAST(CAST(EXTRACT(EPOCH FROM {v}) * 1000000 AS NUMERIC(38,0)) AS {t})")
            }
            Engine::Redshift => {
                format!("CAST(DATEDIFF(microsecond, TIMESTAMP '1970-01-01 00:00:00', {v}) AS {t})")
            }
            Engine::Snowflake => format!("CAST(DATE_PART(epoch_microsecond, {v}) AS {t})"),
            Engine::BigQuery => match family {
                Family::Timestamp { zoned: false } => format!("CAST(UNIX_MICROS(TIMESTAMP({v})) AS {t})"),
                _ => format!("CAST(UNIX_MICROS({v}) AS {t})"),
            },
            _ => format!("CAST(unix_micros(CAST({v} AS TIMESTAMP)) AS {t})"),
        },
        CompareAs::Text => match e {
            // Postgres `char(n)` drops its padding on the way to text, and `uuid`
            // and `name` need the cast to be text at all.
            Engine::Postgres => format!("CAST({v} AS {t})"),
            _ => v.to_string(),
        },
    }
}

/// One cell: `length:text`, or `N` for NULL. Self-delimiting, so a row's text has
/// exactly one reading, and NULL is not the empty string (`0:`).
fn cell(e: Engine, expr: &str) -> String {
    format!("COALESCE(CAST(LENGTH({expr}) AS {}) || ':' || {expr}, 'N')", text_type(e))
}

/// 60 bits of the row text's MD5, as an integer: the first 15 hex digits. Fits a
/// signed 64-bit integer everywhere.
fn row_hash(e: Engine, row: &str) -> String {
    match e {
        Engine::Postgres => format!("CAST(CAST('x' || SUBSTR(MD5({row}), 1, 15) AS BIT(60)) AS BIGINT)"),
        Engine::Redshift => format!("STRTOL(SUBSTRING(MD5({row}), 1, 15), 16)"),
        Engine::Snowflake => format!("TO_NUMBER(SUBSTR(MD5({row}), 1, 15), 'XXXXXXXXXXXXXXX')"),
        Engine::BigQuery => format!("CAST(CONCAT('0x', SUBSTR(TO_HEX(MD5({row})), 1, 15)) AS INT64)"),
        _ => format!("CAST(conv(substr(md5({row}), 1, 15), 16, 10) AS BIGINT)"),
    }
}

/// Which side of a [`Pair`] a query is for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Which {
    Source,
    Target,
}

/// The one-row query that fingerprints `side`: `row_count` and `checksum`, both as
/// text, because a sum this wide is a type some transports cannot decode.
pub fn checksum_sql(side: &Side, pairs: &[Pair], which: Which) -> String {
    let e = side.engine;
    let cells: Vec<String> = pairs
        .iter()
        .map(|p| {
            let col = match which {
                Which::Source => &p.source,
                Which::Target => &p.target,
            };
            let expr = canonical(e, &format!("freshet_rows.{}", quoted(e, &col.name)), p.compare, col.family);
            cell(e, &expr)
        })
        .collect();
    let row = cells.join("\n      || ");
    let filter = side.predicate.as_ref().map(|p| format!("\n  WHERE {p}")).unwrap_or_default();
    let t = text_type(e);
    format!(
        "SELECT CAST(COUNT(*) AS {t}) AS row_count,\n       CAST(COALESCE(SUM(CAST(freshet_h AS {w})), 0) AS {t}) AS checksum\n\
         FROM (\n  SELECT {h} AS freshet_h\n  FROM {rel} AS freshet_rows{filter}\n) AS freshet_parity",
        w = wide_decimal(e),
        h = row_hash(e, &format!("\n         {row}\n      ")),
        rel = side.relation,
    )
}

/// One side's answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Fingerprint {
    pub row_count: String,
    pub checksum: String,
}

impl Fingerprint {
    /// Read the one row [`checksum_sql`] returns. The numbers are compared as
    /// integers, not text: a store may print `0` where another prints `0.0`… it
    /// should not, given the casts, but a difference in spelling is not a
    /// difference in data.
    pub fn from_rows(rows: &[Map<String, Value>]) -> Result<Fingerprint> {
        let [row] = rows else { bail!("the checksum query returned {} rows, not one", rows.len()) };
        let get = |key: &str| -> Result<String> {
            let v = row
                .iter()
                .find(|(k, _)| k.eq_ignore_ascii_case(key))
                .map(|(_, v)| v)
                .ok_or_else(|| anyhow::anyhow!("the checksum query returned no `{key}`"))?;
            let raw = match v {
                Value::String(s) => s.trim().to_string(),
                Value::Number(n) => n.to_string(),
                other => bail!("`{key}` came back as {other}, not a number"),
            };
            // Normalise `123.0`, `0123`: an integer, digits only.
            let digits = raw.split('.').next().unwrap_or("").trim_start_matches('+');
            let (sign, body) = digits.strip_prefix('-').map(|b| ("-", b)).unwrap_or(("", digits));
            if body.is_empty() || !body.bytes().all(|b| b.is_ascii_digit()) || raw.split('.').nth(1).is_some_and(|f| f.bytes().any(|b| b != b'0')) {
                bail!("`{key}` came back as '{raw}', not an integer");
            }
            let body = body.trim_start_matches('0');
            Ok(if body.is_empty() { "0".into() } else { format!("{sign}{body}") })
        };
        Ok(Fingerprint { row_count: get("row_count")?, checksum: get("checksum")? })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn side(engine: Engine, schema: Option<&str>, table: &str) -> Side {
        Side::new(engine, schema.map(Into::into), table.into(), None, Some("dt IN (DATE '2026-06-20')".into())).unwrap()
    }

    fn col(name: &str, family: Family) -> Column {
        Column { name: name.into(), data_type: String::new(), family }
    }

    fn row(pairs: &[(&str, &str)]) -> Map<String, Value> {
        pairs.iter().map(|(k, v)| (k.to_string(), json!(v))).collect()
    }

    #[test]
    fn a_relation_is_composed_the_way_freshet_renders_one() {
        assert_eq!(side(Engine::Postgres, Some("analytics"), "events").relation, "analytics.events");
        assert_eq!(side(Engine::Snowflake, Some("ANALYTICS.MARTS"), "events").relation, "ANALYTICS.MARTS.events");
        assert_eq!(side(Engine::Postgres, None, "daily-events").relation, "\"daily-events\"");
        assert_eq!(side(Engine::Databricks, Some("main.marts"), "daily-events").relation, "main.marts.`daily-events`");
        let given = Side::new(Engine::Postgres, None, "events".into(), Some("public.events".into()), None).unwrap();
        assert_eq!(given.relation, "public.events", "a relation the move rendered is used as it is");
    }

    #[test]
    fn a_side_refuses_what_it_cannot_put_in_a_literal_or_look_up() {
        let e = Side::new(Engine::Postgres, None, "ev'ents".into(), None, None).unwrap_err().to_string();
        assert!(e.contains("quote"), "{e}");
        let e = Side::new(Engine::BigQuery, None, "events".into(), None, None).unwrap_err().to_string();
        assert!(e.contains("dataset"), "{e}");
        let e = Side::new(Engine::ClickHouse, None, "events".into(), None, None).unwrap_err().to_string();
        assert!(e.contains("Freshet plans for"), "{e}");
    }

    /// A plain name is looked up as the store folds it; anything else as written.
    #[test]
    fn discovery_looks_a_name_up_the_way_the_store_keeps_it() {
        let pg = discovery_sql(&side(Engine::Postgres, None, "Events"));
        assert!(pg.contains("table_schema = current_schema() AND table_name = 'events'"), "{pg}");
        assert!(pg.contains("CAST(numeric_scale AS text)"), "every value as text: {pg}");
        let sf = discovery_sql(&side(Engine::Snowflake, Some("ANALYTICS.marts"), "events"));
        assert!(sf.contains("FROM ANALYTICS.information_schema.columns"), "{sf}");
        assert!(sf.contains("table_schema = 'MARTS' AND table_name = 'EVENTS'"), "{sf}");
        let quoted = discovery_sql(&side(Engine::Snowflake, None, "Mixed Case"));
        assert!(quoted.contains("table_name = 'Mixed Case'"), "a quoted name keeps its case: {quoted}");
        let bq = discovery_sql(&side(Engine::BigQuery, Some("proj.analytics"), "Events"));
        assert!(bq.contains("FROM proj.analytics.INFORMATION_SCHEMA.COLUMNS"), "{bq}");
        assert!(bq.contains("table_name = 'Events'"), "BigQuery table names are case-sensitive: {bq}");
        let dbx = discovery_sql(&side(Engine::Databricks, Some("main.marts"), "Events"));
        assert!(dbx.contains("FROM main.information_schema.columns") && dbx.contains("table_name = 'events'"), "{dbx}");
    }

    #[test]
    fn types_fall_into_families_or_are_refused() {
        use Family::*;
        let cases = [
            (Engine::Postgres, "bigint", None, Some(Exact { scale: Some(0) })),
            (Engine::Postgres, "numeric", Some(2), Some(Exact { scale: Some(2) })),
            (Engine::Postgres, "numeric", None, Some(Exact { scale: None })),
            (Engine::Postgres, "double precision", None, Some(Float)),
            (Engine::Postgres, "timestamp with time zone", None, Some(Timestamp { zoned: true })),
            (Engine::Postgres, "character varying", None, Some(Text)),
            (Engine::Postgres, "jsonb", None, None),
            (Engine::Snowflake, "NUMBER", Some(0), Some(Exact { scale: Some(0) })),
            (Engine::Snowflake, "TIMESTAMP_NTZ", None, Some(Timestamp { zoned: false })),
            (Engine::Snowflake, "VARIANT", None, None),
            (Engine::BigQuery, "NUMERIC(10, 2)", None, Some(Exact { scale: Some(2) })),
            (Engine::BigQuery, "NUMERIC", None, Some(Exact { scale: Some(9) })),
            (Engine::BigQuery, "BIGNUMERIC", None, Some(Exact { scale: Some(38) })),
            (Engine::BigQuery, "TIMESTAMP", None, Some(Timestamp { zoned: true })),
            (Engine::BigQuery, "DATETIME", None, Some(Timestamp { zoned: false })),
            (Engine::BigQuery, "STRING(10)", None, Some(Text)),
            (Engine::BigQuery, "STRUCT<a INT64>", None, None),
            (Engine::Databricks, "DECIMAL", Some(4), Some(Exact { scale: Some(4) })),
            (Engine::Databricks, "TIMESTAMP_NTZ", None, Some(Timestamp { zoned: false })),
            (Engine::Databricks, "ARRAY", None, None),
            (Engine::Redshift, "timestamp without time zone", None, Some(Timestamp { zoned: false })),
        ];
        for (engine, ty, scale, want) in cases {
            assert_eq!(classify(engine, ty, scale), want, "{engine:?} {ty}");
        }
    }

    #[test]
    fn discovered_columns_are_read_and_the_ambiguous_refused() {
        let s = side(Engine::Postgres, Some("src"), "events");
        let rows = vec![
            row(&[("table_schema", "src"), ("table_name", "events"), ("column_name", "dt"), ("data_type", "date"), ("numeric_scale", "")]),
            row(&[("TABLE_SCHEMA", "src"), ("TABLE_NAME", "events"), ("COLUMN_NAME", "amount"), ("DATA_TYPE", "numeric"), ("NUMERIC_SCALE", "2")]),
        ];
        let cols = columns(&s, &rows).unwrap();
        assert_eq!(cols[1].family, Family::Exact { scale: Some(2) }, "result keys are matched ignoring case");

        assert!(columns(&s, &[]).unwrap_err().to_string().contains("no columns"));
        let mut two = rows.clone();
        two.push(row(&[("table_schema", "other"), ("table_name", "events"), ("column_name", "x"), ("data_type", "text")]));
        assert!(columns(&s, &two).unwrap_err().to_string().contains("more than one relation"));
        let mut odd = rows.clone();
        odd.push(row(&[("table_schema", "src"), ("table_name", "events"), ("column_name", "doc"), ("data_type", "jsonb")]));
        let e = columns(&s, &odd).unwrap_err().to_string();
        assert!(e.contains("doc (jsonb)") && e.contains("refused"), "{e}");
    }

    #[test]
    fn columns_pair_by_name_and_every_difference_is_listed() {
        let src = [col("DT", Family::Date), col("amount", Family::Exact { scale: Some(2) }), col("gone", Family::Text)];
        let dst = [col("dt", Family::Date), col("AMOUNT", Family::Exact { scale: Some(4) }), col("new", Family::Text)];
        let problems = align(&src, &dst, 6).unwrap_err();
        assert_eq!(problems, ["`gone` is on the source only", "`new` is on the target only"]);

        let pairs = align(&src[..2], &dst[..2], 6).unwrap();
        assert_eq!(pairs[0].source.name, "amount", "in the order of lower-cased names");
        assert_eq!(pairs[0].target.name, "AMOUNT", "each side keeps its own spelling");
        assert_eq!(pairs[0].compare, CompareAs::Scaled { places: 4, round: false }, "the larger scale");

        let mixed = align(&[col("x", Family::Exact { scale: None })], &[col("x", Family::Float)], 6).unwrap();
        assert_eq!(mixed[0].compare, CompareAs::Scaled { places: 6, round: true });
        let clash = align(&[col("x", Family::Date)], &[col("x", Family::Text)], 6).unwrap_err();
        assert_eq!(clash, ["`x` is date on the source and text on the target"]);
        let wide = align(&[col("x", Family::Exact { scale: Some(38) })], &[col("x", Family::Exact { scale: Some(38) })], 6).unwrap();
        assert_eq!(wide[0].compare, CompareAs::Scaled { places: MAX_PLACES, round: true });
    }

    /// The query the live test runs on Postgres, pinned so a change to it is a
    /// change someone reviewed.
    #[test]
    fn the_postgres_checksum_is_the_form_shown_to_work() {
        let s = side(Engine::Postgres, Some("src"), "events");
        let pairs = align(
            &[col("amount", Family::Exact { scale: Some(2) }), col("dt", Family::Date), col("note", Family::Text)],
            &[col("amount", Family::Exact { scale: Some(2) }), col("dt", Family::Date), col("note", Family::Text)],
            6,
        )
        .unwrap();
        let sql = checksum_sql(&s, &pairs, Which::Source);
        assert!(sql.contains(r#"COALESCE(CAST(LENGTH(CAST(CAST(CAST(freshet_rows."amount" AS DECIMAL(38, 2)) * 100 AS DECIMAL(38, 0)) AS TEXT)) AS TEXT) || ':' || "#), "{sql}");
        assert!(sql.contains(r#"CAST(freshet_rows."dt" - DATE '1970-01-01' AS TEXT)"#), "{sql}");
        assert!(sql.contains("CAST('x' || SUBSTR(MD5("), "{sql}");
        assert!(sql.contains("FROM src.events AS freshet_rows\n  WHERE dt IN (DATE '2026-06-20')"), "{sql}");
        assert!(sql.contains("SUM(CAST(freshet_h AS NUMERIC(38,0)))"), "{sql}");
    }

    #[test]
    fn every_store_gets_its_own_spelling_of_the_same_canonical_forms() {
        let pairs = align(
            &[col("ok", Family::Boolean), col("ts", Family::Timestamp { zoned: false }), col("dt", Family::Date)],
            &[col("ok", Family::Boolean), col("ts", Family::Timestamp { zoned: false }), col("dt", Family::Date)],
            6,
        )
        .unwrap();
        let expect = [
            (Engine::Redshift, Some("s"), ["STRTOL(SUBSTRING(MD5(", "DATEDIFF(microsecond, TIMESTAMP '1970-01-01 00:00:00'", "DATEDIFF(day, DATE '1970-01-01'"]),
            (Engine::Snowflake, Some("s"), ["TO_NUMBER(SUBSTR(MD5(", "DATE_PART(epoch_microsecond", "NUMBER(38,0)"]),
            (Engine::BigQuery, Some("ds"), ["CAST(CONCAT('0x', SUBSTR(TO_HEX(MD5(", "UNIX_MICROS(TIMESTAMP(", "UNIX_DATE("]),
            (Engine::Databricks, Some("s"), ["conv(substr(md5(", "unix_micros(CAST(", "unix_date("]),
        ];
        for (engine, schema, needles) in expect {
            let sql = checksum_sql(&side(engine, schema, "events"), &pairs, Which::Target);
            for n in needles {
                assert!(sql.contains(n), "{engine:?} lacks {n}:\n{sql}");
            }
            let ok = if matches!(engine, Engine::BigQuery | Engine::Databricks) { "`ok`" } else { "\"ok\"" };
            assert!(sql.contains(&format!("CASE WHEN freshet_rows.{ok} THEN '1' WHEN NOT freshet_rows.{ok} THEN '0' END")), "{engine:?}:\n{sql}");
        }
    }

    #[test]
    fn a_fingerprint_is_read_as_integers_whatever_the_spelling() {
        let fp = Fingerprint::from_rows(&[row(&[("ROW_COUNT", "3"), ("CHECKSUM", "0042")])]).unwrap();
        assert_eq!(fp, Fingerprint { row_count: "3".into(), checksum: "42".into() });
        let fp = Fingerprint::from_rows(&[serde_json::Map::from_iter([("row_count".to_string(), json!(0)), ("checksum".to_string(), json!("0.0"))])]).unwrap();
        assert_eq!(fp, Fingerprint { row_count: "0".into(), checksum: "0".into() });
        assert!(Fingerprint::from_rows(&[row(&[("row_count", "1.5"), ("checksum", "1")])]).is_err());
        assert!(Fingerprint::from_rows(&[]).is_err());
    }
}
