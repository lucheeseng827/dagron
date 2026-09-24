//! Reading a verdict out of a vendor's JSON status response.
//!
//! A deferred task's poller gets back a document it did not design, and has to
//! answer one question about it: is this job done, and did it work? That is a
//! *predicate over a path*, and this module is the smallest thing that
//! expresses one.
//!
//! ## Why not a JSONPath crate
//!
//! `jsonpath-rust` is already in `Cargo.lock`, which makes it look free. It is
//! not: it arrives only through `kube`, behind the **non-default** `kubernetes`
//! feature, so a default build does not compile it. Depending on it here would
//! pull it — and `pest`, its parser generator — into every build, including the
//! armv7 release leg, to evaluate expressions that are a dotted path and a
//! comparison. The whole grammar below is under 200 lines and has no
//! dependencies beyond `serde_json`, which is already a direct dependency.
//!
//! ## The grammar
//!
//! ```text
//! predicate := path "==" value
//!            | path "!=" value
//!            | path "in" "[" value ("," value)* "]"
//!
//! path      := segment ("." segment)*
//! segment   := key | index          # `status.phase`, `items.0.state`
//! value     := bare | "quoted"      # COMPLETED, "not started", 3, true, null
//! ```
//!
//! Deliberately not expressible: wildcards, filters, recursive descent,
//! arithmetic, boolean connectives. Each is a request that shows up eventually,
//! and each is a step toward a query language nobody asked this project to
//! maintain. A response that needs one of them needs a step binary, which is a
//! seam that already exists.
//!
//! ## Why it parses at validation
//!
//! [`Predicate::parse`] runs when the DAG is submitted, not when the poll
//! happens. A typo in `succeed_when` is otherwise a workflow that submits
//! cleanly, starts a six-hour job, and *then* discovers it cannot read the
//! answer — the most expensive possible moment to learn about a typo.

use serde_json::Value;

/// A resolved path into a JSON document.
///
/// Segments are matched against object keys first and array indices second, so
/// `items.0.state` works without the author saying which is which — a vendor
/// response that puts a list where you expected a map is a normal kind of
/// surprise, and one that should not need a different syntax.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Path(Vec<String>);

impl Path {
    /// Parse a dotted path. Empty segments are rejected: `a..b` and a trailing
    /// dot are typos, and silently treating them as `a.b` would make the path
    /// that runs differ from the path that was written.
    pub fn parse(raw: &str) -> Result<Self, String> {
        let raw = raw.trim();
        if raw.is_empty() {
            return Err("path is empty".into());
        }
        let segs: Vec<String> = raw.split('.').map(|s| s.trim().to_string()).collect();
        if let Some(i) = segs.iter().position(|s| s.is_empty()) {
            return Err(format!(
                "path '{raw}' has an empty segment at position {} — an empty segment is a \
                 typo (`a..b`, or a trailing dot), not a wildcard",
                i + 1
            ));
        }
        Ok(Path(segs))
    }

    /// Follow the path. `None` means "not present", which every caller must
    /// treat as *undecided* rather than false — a vendor that has not yet
    /// written `status.phase` is not a vendor reporting failure.
    pub fn get<'a>(&self, doc: &'a Value) -> Option<&'a Value> {
        let mut cur = doc;
        for seg in &self.0 {
            cur = match cur {
                Value::Object(map) => map.get(seg)?,
                Value::Array(items) => items.get(seg.parse::<usize>().ok()?)?,
                _ => return None,
            };
        }
        Some(cur)
    }
}

impl std::fmt::Display for Path {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.0.join("."))
    }
}

/// One comparison against a path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Predicate {
    Eq { path: Path, value: String },
    Ne { path: Path, value: String },
    In { path: Path, values: Vec<String> },
}

impl Predicate {
    /// Parse one predicate, or say precisely what is wrong with it.
    pub fn parse(raw: &str) -> Result<Self, String> {
        let raw = raw.trim();
        if raw.is_empty() {
            return Err("predicate is empty".into());
        }
        // `in` before the comparisons: `a in [x]` contains no `==`, but
        // checking `==` first would still be wrong the day someone writes a
        // value containing the word "in".
        if let Some((lhs, rhs)) = split_once_kw(raw, "in") {
            let rhs = rhs.trim();
            let inner = rhs
                .strip_prefix('[')
                .and_then(|r| r.strip_suffix(']'))
                .ok_or_else(|| {
                    format!("`in` needs a bracketed list, e.g. `{lhs} in [A, B]` — got `{rhs}`")
                })?;
            let values: Vec<String> = inner
                .split(',')
                .map(|v| unquote(v.trim()))
                .filter(|v| !v.is_empty())
                .collect();
            if values.is_empty() {
                return Err(format!(
                    "`in []` matches nothing, so the predicate can never hold — list at least \
                     one value (in `{raw}`)"
                ));
            }
            return Ok(Predicate::In { path: Path::parse(lhs)?, values });
        }
        for (op, build) in [
            ("==", (|p, v| Predicate::Eq { path: p, value: v }) as fn(Path, String) -> Predicate),
            ("!=", (|p, v| Predicate::Ne { path: p, value: v }) as fn(Path, String) -> Predicate),
        ] {
            if let Some((lhs, rhs)) = raw.split_once(op) {
                let value = unquote(rhs.trim());
                if value.is_empty() {
                    return Err(format!("`{op}` has no right-hand value in `{raw}`"));
                }
                return Ok(build(Path::parse(lhs)?, value));
            }
        }
        Err(format!(
            "cannot parse predicate `{raw}` — expected `path == VALUE`, `path != VALUE`, or \
             `path in [A, B]` (paths are dotted, e.g. `status.applicationState.state`)"
        ))
    }

    /// The path this predicate reads, for diagnostics.
    pub fn path(&self) -> &Path {
        match self {
            Predicate::Eq { path, .. } | Predicate::Ne { path, .. } | Predicate::In { path, .. } => path,
        }
    }

    /// Evaluate against a document.
    ///
    /// **A missing path is `false` for every form, `!=` included.** That
    /// asymmetry is deliberate: `phase != RUNNING` reads naturally as "has it
    /// stopped running", and answering `true` for a document that has no
    /// `phase` yet would resolve a task on a response the vendor has not
    /// finished writing. Undecided is the safe answer for a poller, because
    /// the next poll costs seconds and a wrong verdict costs the job.
    pub fn eval(&self, doc: &Value) -> bool {
        let Some(found) = self.path().get(doc) else { return false };
        let actual = scalar(found);
        match self {
            Predicate::Eq { value, .. } => actual.as_deref() == Some(value.as_str()),
            Predicate::Ne { value, .. } => {
                actual.as_deref().is_some_and(|a| a != value.as_str())
            }
            Predicate::In { values, .. } => {
                actual.as_deref().is_some_and(|a| values.iter().any(|v| v == a))
            }
        }
    }
}

impl std::fmt::Display for Predicate {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Predicate::Eq { path, value } => write!(f, "{path} == {value}"),
            Predicate::Ne { path, value } => write!(f, "{path} != {value}"),
            Predicate::In { path, values } => write!(f, "{path} in [{}]", values.join(", ")),
        }
    }
}

/// A JSON value as the string a predicate compares against.
///
/// Numbers and booleans render the way the author would have typed them, so
/// `retries == 3` and `done == true` work without quoting rules. Objects and
/// arrays return `None` — a predicate that lands on one is comparing against a
/// subtree, which is a mistake rather than a comparison that should silently
/// be false against every value.
fn scalar(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        Value::Number(n) => Some(n.to_string()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Null => Some("null".into()),
        Value::Object(_) | Value::Array(_) => None,
    }
}

/// Strip one layer of matching quotes. Quoting exists so a value may contain a
/// space or a comma; an unquoted value is taken verbatim.
fn unquote(s: &str) -> String {
    let s = s.trim();
    for q in ['"', '\''] {
        if s.len() >= 2 && s.starts_with(q) && s.ends_with(q) {
            return s[1..s.len() - 1].to_string();
        }
    }
    s.to_string()
}

/// Split on a bare keyword (` in `), not on the substring — so a path segment
/// or value spelled `pending`, `min` or `finished` does not split the
/// predicate in the middle of a word.
fn split_once_kw<'a>(raw: &'a str, kw: &str) -> Option<(&'a str, &'a str)> {
    let bytes = raw.as_bytes();
    let mut from = 0;
    while let Some(rel) = raw[from..].find(kw) {
        let at = from + rel;
        let end = at + kw.len();
        let before_ok = at > 0 && bytes[at - 1].is_ascii_whitespace();
        let after_ok = end < bytes.len() && bytes[end].is_ascii_whitespace();
        if before_ok && after_ok {
            return Some((&raw[..at], &raw[end..]));
        }
        from = end;
    }
    None
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn paths_walk_objects_and_arrays() {
        let doc = json!({
            "status": {"applicationState": {"state": "COMPLETED"}},
            "items": [{"state": "a"}, {"state": "b"}],
            "n": 3, "ok": true, "nothing": null
        });
        let g = |p: &str| Path::parse(p).unwrap().get(&doc).cloned();
        assert_eq!(g("status.applicationState.state"), Some(json!("COMPLETED")));
        assert_eq!(g("items.1.state"), Some(json!("b")), "an index is just a segment");
        assert_eq!(g("n"), Some(json!(3)));
        assert_eq!(g("nothing"), Some(json!(null)), "present-and-null differs from absent");
        assert_eq!(g("status.missing"), None);
        assert_eq!(g("items.9.state"), None, "out of range is absent, not an error");
        assert_eq!(g("n.deeper"), None, "descending into a scalar is absent");
    }

    #[test]
    fn an_empty_segment_is_a_typo_not_a_wildcard() {
        for bad in ["a..b", "a.", ".a", "  "] {
            assert!(Path::parse(bad).is_err(), "{bad} must be refused");
        }
        assert!(Path::parse("a..b").unwrap_err().contains("empty segment"));
    }

    #[test]
    fn predicates_parse_and_evaluate() {
        let doc = json!({"status": {"phase": "FAILED", "retries": 3, "done": true}});
        let p = |s: &str| Predicate::parse(s).unwrap();

        assert!(p("status.phase == FAILED").eval(&doc));
        assert!(!p("status.phase == COMPLETED").eval(&doc));
        assert!(p("status.phase != COMPLETED").eval(&doc));
        assert!(p("status.phase in [FAILED, SUBMISSION_FAILED]").eval(&doc));
        assert!(!p("status.phase in [COMPLETED]").eval(&doc));

        // Numbers and booleans compare as written, without quoting rules.
        assert!(p("status.retries == 3").eval(&doc));
        assert!(p("status.done == true").eval(&doc));

        // Quotes carry a value containing a space or a comma.
        let spaced = json!({"s": "not started"});
        assert!(p("s == \"not started\"").eval(&spaced));
        assert!(p("s in ['not started', other]").eval(&spaced));
    }

    /// The asymmetry that matters: a path the vendor has not written yet is
    /// undecided, so EVERY form is false — `!=` included. Answering true there
    /// would resolve a task on a half-written response.
    #[test]
    fn a_missing_path_is_false_for_every_form_including_ne() {
        let doc = json!({"status": {}});
        for pred in [
            "status.phase == COMPLETED",
            "status.phase != COMPLETED",
            "status.phase in [COMPLETED, FAILED]",
        ] {
            assert!(!Predicate::parse(pred).unwrap().eval(&doc), "{pred} must be false");
        }
        // …and the same for a path that lands on a subtree rather than a scalar.
        let sub = json!({"status": {"phase": {"nested": 1}}});
        assert!(!Predicate::parse("status.phase != COMPLETED").unwrap().eval(&sub));
    }

    /// ` in ` splits on the keyword, not the substring — otherwise a path or
    /// value containing those two letters tears the predicate in half.
    #[test]
    fn the_in_keyword_does_not_split_inside_a_word() {
        let doc = json!({"pipeline": {"finished": "min"}});
        let p = Predicate::parse("pipeline.finished == min").unwrap();
        assert!(p.eval(&doc), "`min` and `finished` must not be read as the `in` operator");
        assert!(matches!(p, Predicate::Eq { .. }));

        let listy = Predicate::parse("pipeline.finished in [min, max]").unwrap();
        assert!(matches!(listy, Predicate::In { .. }));
        assert!(listy.eval(&doc));
    }

    #[test]
    fn unparseable_predicates_say_what_was_expected() {
        let err = |s: &str| Predicate::parse(s).unwrap_err();
        assert!(err("status.phase").contains("expected `path == VALUE`"));
        assert!(err("status.phase == ").contains("no right-hand value"));
        assert!(err("status.phase in COMPLETED").contains("bracketed list"));
        assert!(err("status.phase in []").contains("can never hold"));
        assert!(err("").contains("empty"));
    }

    #[test]
    fn display_round_trips_through_parse() {
        for s in [
            "status.phase == FAILED",
            "status.phase != COMPLETED",
            "status.phase in [A, B]",
        ] {
            let p = Predicate::parse(s).unwrap();
            assert_eq!(p.to_string(), s, "Display is what a diagnostic prints");
            assert_eq!(Predicate::parse(&p.to_string()).unwrap(), p);
        }
    }
}
