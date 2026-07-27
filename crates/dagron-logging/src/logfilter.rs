//! Stored-log **filter engine**: turn a run's captured task output into an
//! explicitly-scoped, adjustable view.
//!
//! This is the read side of logging, and deliberately separate from the
//! `tracing` bootstrap in [`crate`]: that one decides what a *process* emits,
//! this one decides what a *human* sees when they open a workflow's logs. Task
//! output is stored verbatim (`task_runs.output`), so "show me only the errors
//! from the two extract tasks, minus the healthcheck noise" is a query over
//! that text — not something the emitting side can be reconfigured to answer
//! after the fact.
//!
//! It lives here, rather than in `dagron-api`, because more than one process
//! needs the *same* grammar: the management API and the engine's ops API both
//! apply it, and anything that wants to *store* a filter for later replay parses
//! it here first. A filter that round-trips through storage must mean exactly
//! what it meant when it was typed, so there can only be one parser.
//!
//! ## Grammar
//!
//! A filter is a set of independent predicates, all of which must hold for a
//! line to be kept:
//!
//! | Field | Query param | Meaning |
//! |-------|-------------|---------|
//! | [`contains`](LogFilter::contains) | `q` (repeatable) | line contains **every** term |
//! | [`exclude`](LogFilter::exclude)   | `exclude` (repeatable) | line contains **none** of these terms |
//! | [`pattern`](LogFilter::pattern)   | `regex` | line matches this regular expression |
//! | [`levels`](LogFilter::levels)     | `level` (repeatable/CSV) | line's inferred level is one of these |
//! | [`case_sensitive`](LogFilter::case_sensitive) | `case=1` | term/regex matching is case-sensitive (default: insensitive) |
//! | [`context`](LogFilter::context)   | `context=N` | also keep N unmatched lines either side of a match |
//! | [`limit`](LogFilter::limit)       | `limit=N` | cap the returned lines (0 = the built-in cap) |
//! | [`tail`](LogFilter::tail)         | `tail=1` | when capping, keep the **last** N lines rather than the first |
//!
//! An empty filter ([`LogFilter::is_noop`]) keeps everything, so "no filter" and
//! "a filter that matches everything" are the same code path.

use std::fmt;

use regex::{Regex, RegexBuilder};

/// Hard cap on returned lines when the caller doesn't ask for one. A log view is
/// a screenful with a scrollback, not a bulk export — an unbounded default would
/// let one 500MB task output decide the response size.
pub const DEFAULT_LIMIT: usize = 2_000;

/// Absolute cap: a caller-supplied `limit` is clamped to this. Bulk retrieval is
/// what the raw (unfiltered) output endpoint is for.
pub const MAX_LIMIT: usize = 50_000;

/// Longest accepted regex source. A pathological pattern is a CPU risk even with
/// the linear-time `regex` engine (compilation itself is not free), and no honest
/// log filter needs more than this.
pub const MAX_PATTERN_LEN: usize = 512;

/// The severity a stored log line appears to carry.
///
/// Task output is whatever the command printed — there is no structured level to
/// read, so it is *inferred* from the line's text ([`Level::infer`]). That makes
/// level filtering a best-effort convenience, not a guarantee: a line that never
/// says "error" cannot be found by asking for errors.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Level {
    /// No recognizable level token — the common case for ordinary output.
    Plain,
    Trace,
    Debug,
    Info,
    Warn,
    Error,
}

impl Level {
    /// The wire/query name (`"error"`, `"plain"`, …).
    pub fn as_str(self) -> &'static str {
        match self {
            Level::Plain => "plain",
            Level::Trace => "trace",
            Level::Debug => "debug",
            Level::Info => "info",
            Level::Warn => "warn",
            Level::Error => "error",
        }
    }

    /// Parse a level name. Accepts the common aliases a user is likely to type
    /// (`warning`, `err`, `fatal`, `critical`) so a filter isn't rejected over
    /// spelling.
    pub fn parse(s: &str) -> Option<Level> {
        let s = s.trim();
        // Compare ASCII-case-insensitively without allocating a lowercase copy.
        let eq = |name: &str| s.eq_ignore_ascii_case(name);
        if eq("error") || eq("err") || eq("fatal") || eq("critical") || eq("crit") {
            Some(Level::Error)
        } else if eq("warn") || eq("warning") {
            Some(Level::Warn)
        } else if eq("info") || eq("notice") {
            Some(Level::Info)
        } else if eq("debug") {
            Some(Level::Debug)
        } else if eq("trace") {
            Some(Level::Trace)
        } else if eq("plain") || eq("none") {
            Some(Level::Plain)
        } else {
            None
        }
    }

    /// Infer a line's level from its text.
    ///
    /// Only the head of the line is scanned: a level token belongs to a line's
    /// prefix (`2026-07-26T10:00:00Z ERROR ...`, `[warn]`, `ERROR:`), and
    /// scanning the whole line would classify `echo "no errors found"` as an
    /// error — the single most annoying false positive a log filter can have.
    /// The highest-severity token in that window wins, so `WARN: recovering from
    /// error` reads as a warning rather than jumping to error on a later word.
    pub fn infer(line: &str) -> Level {
        /// Bytes of the line prefix examined for a level token. Generous enough
        /// to clear an RFC3339 timestamp + a logger name, short enough that a
        /// sentence's contents don't reach it.
        const HEAD: usize = 64;
        let head = &line[..line.floor_char_boundary_compat(HEAD)];
        let mut best = Level::Plain;
        for &(tok, level) in TOKENS {
            if level > best && contains_token(head, tok) {
                best = level;
            }
        }
        best
    }
}

/// Split a leading RFC3339 timestamp off a log line.
///
/// Returns `(Some(stamp), message)` when the line begins with one followed by a
/// separator, else `(None, line)`. This is how a real per-line time reaches the
/// read side without a schema change: whatever already prints one — a task that
/// logs structured output, or Docker/Kubernetes with their `timestamps` option
/// — gets it parsed out instead of left buried in the text.
///
/// Splitting matters for more than display. Everything after it (level
/// inference, `q=`, `exclude=`, `regex=`) sees the **message**, so an anchored
/// pattern like `^ERROR` keeps working the moment a stamp appears in front of
/// it. Leaving the prefix in the text would silently change what those filters
/// mean.
///
/// Hand-rolled rather than pulled from a date crate: this only needs to
/// recognize the shape, never to interpret it, and dagron-logging is linked into
/// every binary in the stack.
pub fn split_timestamp(line: &str) -> (Option<&str>, &str) {
    let b = line.as_bytes();
    let digits = |at: usize, n: usize| at + n <= b.len() && b[at..at + n].iter().all(u8::is_ascii_digit);

    // YYYY-MM-DD
    if !(digits(0, 4)
        && b.get(4) == Some(&b'-')
        && digits(5, 2)
        && b.get(7) == Some(&b'-')
        && digits(8, 2))
    {
        return (None, line);
    }
    // RFC3339 allows 'T' or a space as the date/time separator; Docker and the
    // kubelet both emit 'T'.
    match b.get(10) {
        Some(&c) if c == b'T' || c == b't' || c == b' ' => {}
        _ => return (None, line),
    }
    // HH:MM:SS
    if !(digits(11, 2)
        && b.get(13) == Some(&b':')
        && digits(14, 2)
        && b.get(16) == Some(&b':')
        && digits(17, 2))
    {
        return (None, line);
    }
    let mut i = 19;
    // Optional fractional seconds — Docker emits nanoseconds.
    if b.get(i) == Some(&b'.') {
        let start = i + 1;
        let mut j = start;
        while j < b.len() && b[j].is_ascii_digit() {
            j += 1;
        }
        if j == start {
            return (None, line); // a '.' with no digits is not a timestamp
        }
        i = j;
    }
    // Offset: Z, or ±HH:MM.
    match b.get(i) {
        Some(&c) if c == b'Z' || c == b'z' => i += 1,
        Some(&c) if c == b'+' || c == b'-' => {
            if !(digits(i + 1, 2) && b.get(i + 3) == Some(&b':') && digits(i + 4, 2)) {
                return (None, line);
            }
            i += 6;
        }
        _ => return (None, line),
    }
    // A stamp has to be followed by a separator, else "2026-01-01T00:00:00Zed"
    // would read as a timestamped line.
    match b.get(i) {
        None => (Some(&line[..i]), ""),
        Some(&b' ') | Some(&b'\t') => (Some(&line[..i]), line[i..].trim_start()),
        _ => (None, line),
    }
}

/// Level tokens recognized in a line's head, longest-first per level so a
/// substring alias can't shadow the token that actually appears.
const TOKENS: &[(&str, Level)] = &[
    ("error", Level::Error),
    ("fatal", Level::Error),
    ("critical", Level::Error),
    ("panic", Level::Error),
    ("warning", Level::Warn),
    ("warn", Level::Warn),
    ("info", Level::Info),
    ("debug", Level::Debug),
    ("trace", Level::Trace),
];

/// Case-insensitive search for `needle` in `hay` **as a whole word**, so
/// `traceparent=…` isn't read as a `trace` line and `errors_total` isn't an
/// error. Word boundaries are "not an ASCII alphanumeric or `_`".
fn contains_token(hay: &str, needle: &str) -> bool {
    let bytes = hay.as_bytes();
    let n = needle.as_bytes();
    if n.is_empty() || bytes.len() < n.len() {
        return false;
    }
    let is_word = |b: u8| b.is_ascii_alphanumeric() || b == b'_';
    for start in 0..=bytes.len() - n.len() {
        if !bytes[start..start + n.len()].eq_ignore_ascii_case(n) {
            continue;
        }
        let before_ok = start == 0 || !is_word(bytes[start - 1]);
        let after_ok = start + n.len() == bytes.len() || !is_word(bytes[start + n.len()]);
        if before_ok && after_ok {
            return true;
        }
    }
    false
}

/// `str::floor_char_boundary` is still unstable; this is the same clamp so a
/// prefix slice never splits a multibyte character.
trait FloorCharBoundary {
    fn floor_char_boundary_compat(&self, index: usize) -> usize;
}

impl FloorCharBoundary for str {
    fn floor_char_boundary_compat(&self, index: usize) -> usize {
        if index >= self.len() {
            return self.len();
        }
        let mut i = index;
        while i > 0 && !self.is_char_boundary(i) {
            i -= 1;
        }
        i
    }
}

/// Why a filter couldn't be built. Callers map this to a 400 — every variant is
/// a caller mistake, never a server fault.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FilterError {
    /// The regex didn't compile (message from the `regex` crate).
    BadPattern(String),
    /// The regex source exceeded [`MAX_PATTERN_LEN`].
    PatternTooLong(usize),
    /// An unrecognized `level=` value.
    UnknownLevel(String),
    /// A numeric parameter wasn't a number.
    NotANumber { param: &'static str, value: String },
    /// The saved-view query string wasn't parseable as `k=v&k=v`.
    MalformedQuery(String),
}

impl fmt::Display for FilterError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            FilterError::BadPattern(m) => write!(f, "invalid regex: {m}"),
            FilterError::PatternTooLong(n) => {
                write!(f, "regex too long ({n} bytes, max {MAX_PATTERN_LEN})")
            }
            FilterError::UnknownLevel(v) => write!(
                f,
                "unknown level {v:?} (expected error/warn/info/debug/trace/plain)"
            ),
            FilterError::NotANumber { param, value } => {
                write!(f, "{param} must be a number, got {value:?}")
            }
            FilterError::MalformedQuery(m) => write!(f, "malformed filter query: {m}"),
        }
    }
}

impl std::error::Error for FilterError {}

/// A parsed, compiled log filter. Build one with [`LogFilter::builder`] or
/// [`LogFilter::parse_query`], then [`apply`](LogFilter::apply) it to raw output.
#[derive(Debug, Clone, Default)]
pub struct LogFilter {
    /// Terms a line must contain (AND). Stored lowercased when
    /// [`case_sensitive`](Self::case_sensitive) is false.
    contains: Vec<String>,
    /// Terms a line must not contain (NOR). Same casing rule as `contains`.
    exclude: Vec<String>,
    /// Compiled `regex=` pattern, if any.
    pattern: Option<Regex>,
    /// Kept levels. Empty = every level.
    levels: Vec<Level>,
    case_sensitive: bool,
    context: usize,
    limit: usize,
    tail: bool,
}

/// Fluent builder for a [`LogFilter`]. Every setter is optional; the default is
/// a no-op filter.
#[derive(Debug, Clone, Default)]
pub struct LogFilterBuilder {
    contains: Vec<String>,
    exclude: Vec<String>,
    pattern: Option<String>,
    levels: Vec<Level>,
    case_sensitive: bool,
    context: usize,
    limit: usize,
    tail: bool,
}

impl LogFilterBuilder {
    /// Add a required substring.
    pub fn contains(mut self, term: impl Into<String>) -> Self {
        let t = term.into();
        if !t.is_empty() {
            self.contains.push(t);
        }
        self
    }
    /// Add a forbidden substring.
    pub fn exclude(mut self, term: impl Into<String>) -> Self {
        let t = term.into();
        if !t.is_empty() {
            self.exclude.push(t);
        }
        self
    }
    /// Set the regex source (compiled by [`build`](Self::build)).
    pub fn pattern(mut self, re: impl Into<String>) -> Self {
        let p = re.into();
        self.pattern = (!p.is_empty()).then_some(p);
        self
    }
    /// Keep only these levels (empty = all).
    pub fn levels(mut self, levels: impl IntoIterator<Item = Level>) -> Self {
        self.levels.extend(levels);
        self
    }
    /// Make term/regex matching case-sensitive.
    pub fn case_sensitive(mut self, on: bool) -> Self {
        self.case_sensitive = on;
        self
    }
    /// Keep `n` unmatched lines either side of each match.
    pub fn context(mut self, n: usize) -> Self {
        self.context = n;
        self
    }
    /// Cap the returned lines (0 = [`DEFAULT_LIMIT`]).
    pub fn limit(mut self, n: usize) -> Self {
        self.limit = n;
        self
    }
    /// When capping, keep the last lines instead of the first.
    pub fn tail(mut self, on: bool) -> Self {
        self.tail = on;
        self
    }

    /// Compile into a [`LogFilter`]. The only fallible step is the regex.
    pub fn build(self) -> Result<LogFilter, FilterError> {
        let pattern = match self.pattern {
            Some(src) => {
                if src.len() > MAX_PATTERN_LEN {
                    return Err(FilterError::PatternTooLong(src.len()));
                }
                Some(
                    RegexBuilder::new(&src)
                        .case_insensitive(!self.case_sensitive)
                        // Bound compiled-program size: the default is already
                        // generous, but a filter is untrusted input on a shared
                        // API, so keep the ceiling explicit and modest.
                        .size_limit(1 << 20)
                        .build()
                        .map_err(|e| FilterError::BadPattern(e.to_string()))?,
                )
            }
            None => None,
        };
        let fold = |v: Vec<String>| {
            if self.case_sensitive {
                v
            } else {
                v.into_iter().map(|s| s.to_lowercase()).collect()
            }
        };
        let mut levels = self.levels;
        levels.sort_unstable();
        levels.dedup();
        Ok(LogFilter {
            contains: fold(self.contains),
            exclude: fold(self.exclude),
            pattern,
            levels,
            case_sensitive: self.case_sensitive,
            context: self.context,
            limit: self.limit.clamp(0, MAX_LIMIT),
            tail: self.tail,
        })
    }
}

impl LogFilter {
    /// Start building a filter.
    pub fn builder() -> LogFilterBuilder {
        LogFilterBuilder::default()
    }

    /// True when the filter selects nothing out — every line is kept and only
    /// the line cap applies. Callers use this to keep the unfiltered response
    /// shape byte-for-byte identical to what it was before filtering existed.
    pub fn is_noop(&self) -> bool {
        self.contains.is_empty()
            && self.exclude.is_empty()
            && self.pattern.is_none()
            && self.levels.is_empty()
    }

    /// The effective line cap ([`DEFAULT_LIMIT`] when unset).
    pub fn effective_limit(&self) -> usize {
        if self.limit == 0 {
            DEFAULT_LIMIT
        } else {
            self.limit
        }
    }

    /// Whether the cap keeps the tail rather than the head.
    pub fn is_tail(&self) -> bool {
        self.tail
    }

    /// Parse a `k=v&k=v` filter query (a URL query string *without* the leading
    /// `?`). Unknown keys are ignored so the same string can carry unrelated
    /// params (`offset=`, `task=`) — this parses the filter grammar only.
    ///
    /// This is also the function a *stored* filter is validated with before it
    /// is written down, so accepting something here is a promise that the HTTP
    /// surfaces will accept it too.
    pub fn parse_query(query: &str) -> Result<LogFilter, FilterError> {
        Self::from_pairs(&parse_pairs(query))
    }

    /// Same as [`parse_query`](Self::parse_query) for a caller that already
    /// split the query with [`parse_pairs`] — the API reads its own scoping
    /// params (`task`, `status`, `offset`) out of the same pass.
    pub fn from_pairs(pairs: &[(String, String)]) -> Result<LogFilter, FilterError> {
        let mut b = LogFilter::builder();
        // Case-sensitivity must be known before terms are folded, and it can
        // appear after them in the string, so resolve it in a first pass.
        if let Some((_, v)) = pairs.iter().find(|(k, _)| k == "case") {
            b = b.case_sensitive(truthy(v));
        }
        for (k, v) in pairs {
            match k.as_str() {
                "q" | "contains" => b = b.contains(v.clone()),
                "exclude" | "not" => b = b.exclude(v.clone()),
                "regex" => b = b.pattern(v.clone()),
                "level" | "levels" => {
                    // Accept both repeated params and a CSV value.
                    for part in v.split(',').filter(|p| !p.trim().is_empty()) {
                        let lv = Level::parse(part)
                            .ok_or_else(|| FilterError::UnknownLevel(part.trim().to_string()))?;
                        b = b.levels([lv]);
                    }
                }
                "context" => b = b.context(parse_usize("context", v)?),
                "limit" => b = b.limit(parse_usize("limit", v)?),
                "tail" => b = b.tail(truthy(v)),
                _ => {}
            }
        }
        b.build()
    }

    /// Apply the filter to raw task output.
    ///
    /// Line numbers in the result are 1-based positions in the *unfiltered*
    /// text, so a filtered view still says where each line came from — without
    /// that, "line 12" in a filtered pane is a different line from "line 12" in
    /// the raw pane, which is worse than no numbers at all.
    pub fn apply(&self, output: &str) -> FilterResult {
        // `lines()` on "" yields nothing, which is the right answer (no output
        // is zero lines, not one empty line).
        let raw: Vec<&str> = output.lines().collect();
        let total = raw.len();

        // Split the stamp off first, so everything after this point — level
        // inference and every predicate — sees the message alone.
        let split: Vec<(Option<&str>, &str)> = raw.iter().map(|l| split_timestamp(l)).collect();
        let all: Vec<&str> = split.iter().map(|(_, msg)| *msg).collect();

        // Classify once: level inference is the expensive per-line step and the
        // context pass would otherwise redo it for neighbours.
        let levels: Vec<Level> = all.iter().map(|l| Level::infer(l)).collect();
        let matched_idx: Vec<usize> = (0..total).filter(|&i| self.matches(all[i], levels[i])).collect();
        let matched = matched_idx.len();

        // Expand matches by `context` lines either side, then keep the union in
        // original order. Neighbours are marked `matched = false` so the UI can
        // dim them — context you can't distinguish from a hit is misleading.
        let keep: Vec<usize> = if self.context == 0 || matched == 0 {
            matched_idx.clone()
        } else {
            let mut flags = vec![false; total];
            for &i in &matched_idx {
                let lo = i.saturating_sub(self.context);
                let hi = (i + self.context).min(total.saturating_sub(1));
                flags[lo..=hi].iter_mut().for_each(|f| *f = true);
            }
            (0..total).filter(|&i| flags[i]).collect()
        };

        let limit = self.effective_limit();
        let truncated = keep.len() > limit;
        let window: &[usize] = if !truncated {
            &keep
        } else if self.tail {
            &keep[keep.len() - limit..]
        } else {
            &keep[..limit]
        };

        let is_match = |i: usize| matched_idx.binary_search(&i).is_ok();
        let lines = window
            .iter()
            .map(|&i| LogLine {
                n: i + 1,
                level: levels[i],
                ts: split[i].0.map(str::to_string),
                text: all[i].to_string(),
                matched: is_match(i),
            })
            .collect();

        FilterResult { lines, total, matched, truncated }
    }

    /// Does one line survive the filter? `level` is passed in because the caller
    /// has already inferred it.
    fn matches(&self, line: &str, level: Level) -> bool {
        if !self.levels.is_empty() && !self.levels.contains(&level) {
            return false;
        }
        if self.contains.is_empty() && self.exclude.is_empty() && self.pattern.is_none() {
            return true;
        }
        // One lowercase copy per line, reused by every term — folding inside the
        // term loop would re-lowercase the line N times.
        let hay = if self.case_sensitive {
            None
        } else {
            Some(line.to_lowercase())
        };
        let hay: &str = hay.as_deref().unwrap_or(line);
        if !self.contains.iter().all(|t| hay.contains(t.as_str())) {
            return false;
        }
        if self.exclude.iter().any(|t| hay.contains(t.as_str())) {
            return false;
        }
        // The regex carries its own case-insensitivity flag, so it always runs
        // against the original line.
        if let Some(re) = &self.pattern {
            if !re.is_match(line) {
                return false;
            }
        }
        true
    }
}

/// One line of a filtered log view.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LogLine {
    /// 1-based line number in the **unfiltered** output.
    pub n: usize,
    /// Level inferred from the line's head.
    pub level: Level,
    /// The RFC3339 stamp the line carried, if any ([`split_timestamp`]). `None`
    /// is the common case — task output is whatever the command printed, and
    /// most commands print no time at all. A reader that wants a time for every
    /// line falls back to the task's own start, which the log endpoints return
    /// alongside these lines.
    pub ts: Option<String>,
    /// The line text with any leading timestamp removed, and no trailing
    /// newline. Filters match against this, not against the stamp.
    pub text: String,
    /// True for a line the filter actually matched; false for a neighbour kept
    /// only because of `context`.
    pub matched: bool,
}

/// The outcome of applying a [`LogFilter`].
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct FilterResult {
    /// The kept lines, in original order, capped by the filter's limit.
    pub lines: Vec<LogLine>,
    /// Lines in the unfiltered output.
    pub total: usize,
    /// Lines the filter matched, **before** the limit — so the UI can say
    /// "showing 2000 of 18432 matches" instead of silently lying.
    pub matched: usize,
    /// True when the limit dropped lines that would otherwise have been kept.
    pub truncated: bool,
}

impl FilterResult {
    /// Re-join the kept lines into a plain text block.
    pub fn to_text(&self) -> String {
        let mut s = String::new();
        for l in &self.lines {
            // Re-attach the stamp: this is the *text* view of the result, and a
            // filtered log that quietly lost its timestamps would be worse than
            // one that never carried them.
            if let Some(ts) = &l.ts {
                s.push_str(ts);
                s.push(' ');
            }
            s.push_str(&l.text);
            s.push('\n');
        }
        s
    }
}

/// Every query key the filter grammar understands. Anything else in a query
/// string belongs to the endpoint, not the filter.
pub const FILTER_KEYS: &[&str] = &[
    "q", "contains", "exclude", "not", "regex", "level", "levels", "case", "context", "limit",
    "tail",
];

/// Split a `k=v&k=v` query string into percent-decoded pairs, preserving order
/// and repeats. Shared so the filter grammar and an endpoint's own params are
/// read the same way — a `q` the API sees must be the `q` the filter sees.
pub fn parse_pairs(query: &str) -> Vec<(String, String)> {
    query
        .trim_start_matches('?')
        .split('&')
        .filter(|s| !s.is_empty())
        .map(|kv| {
            let (k, v) = kv.split_once('=').unwrap_or((kv, ""));
            (percent_decode(k), percent_decode(v))
        })
        .collect()
}

/// Does this query string ask for any filtering at all?
///
/// Callers use this to keep the unfiltered path byte-identical: with no filter
/// keys present there is no reason to classify lines or reshape the response.
pub fn query_has_filter(query: &str) -> bool {
    pairs_have_filter(&parse_pairs(query))
}

/// [`query_has_filter`] for already-split pairs.
pub fn pairs_have_filter(pairs: &[(String, String)]) -> bool {
    pairs.iter().any(|(k, _)| FILTER_KEYS.contains(&k.as_str()))
}

/// `1`/`true`/`yes`/`on` (and a bare `?tail` with no value) are true.
fn truthy(v: &str) -> bool {
    matches!(v.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on" | "")
}

fn parse_usize(param: &'static str, v: &str) -> Result<usize, FilterError> {
    v.trim()
        .parse::<usize>()
        .map_err(|_| FilterError::NotANumber { param, value: v.to_string() })
}

/// Minimal `application/x-www-form-urlencoded` decode (`%XX` + `+`). Saved views
/// store an already-encoded query string, so parsing one has to undo that;
/// pulling in a URL crate for two escapes isn't worth the dependency.
fn percent_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out: Vec<u8> = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => {
                out.push(b' ');
                i += 1;
            }
            b'%' if i + 2 < bytes.len() => {
                // Decode from the raw bytes, not a `&s[..]` slice: the two bytes
                // after a `%` can be UTF-8 continuation bytes, and slicing the
                // str there would panic on a non-boundary.
                let decoded = std::str::from_utf8(&bytes[i + 1..i + 3])
                    .ok()
                    .and_then(|h| u8::from_str_radix(h, 16).ok());
                match decoded {
                    Some(b) => {
                        out.push(b);
                        i += 3;
                    }
                    // Not a valid escape — keep the '%' literally rather than
                    // dropping input the user typed.
                    None => {
                        out.push(b'%');
                        i += 1;
                    }
                }
            }
            b => {
                out.push(b);
                i += 1;
            }
        }
    }
    String::from_utf8_lossy(&out).into_owned()
}

#[cfg(test)]
mod tests {
    use super::*;

    const SAMPLE: &str = "2026-07-26T10:00:00Z INFO  starting extract\n\
         2026-07-26T10:00:01Z DEBUG opened connection\n\
         rows=1200 exported\n\
         2026-07-26T10:00:02Z WARN  retrying page 3\n\
         2026-07-26T10:00:03Z ERROR upstream timeout\n\
         done";

    #[test]
    fn noop_filter_keeps_everything() {
        let f = LogFilter::builder().build().unwrap();
        assert!(f.is_noop());
        let r = f.apply(SAMPLE);
        assert_eq!(r.total, 6);
        assert_eq!(r.matched, 6);
        assert_eq!(r.lines.len(), 6);
        assert!(!r.truncated);
    }

    #[test]
    fn empty_output_is_zero_lines() {
        let r = LogFilter::builder().build().unwrap().apply("");
        assert_eq!(r.total, 0);
        assert!(r.lines.is_empty());
    }

    #[test]
    fn contains_terms_are_anded() {
        let f = LogFilter::builder().contains("retrying").contains("page").build().unwrap();
        let r = f.apply(SAMPLE);
        assert_eq!(r.matched, 1);
        assert_eq!(r.lines[0].n, 4, "line numbers are positions in the raw output");
        // Both terms present but on different lines must not match.
        let f = LogFilter::builder().contains("retrying").contains("timeout").build().unwrap();
        assert_eq!(f.apply(SAMPLE).matched, 0);
    }

    #[test]
    fn matching_is_case_insensitive_by_default() {
        let f = LogFilter::builder().contains("UPSTREAM").build().unwrap();
        assert_eq!(f.apply(SAMPLE).matched, 1);
        let f = LogFilter::builder().contains("UPSTREAM").case_sensitive(true).build().unwrap();
        assert_eq!(f.apply(SAMPLE).matched, 0);
    }

    #[test]
    fn exclude_removes_lines() {
        let f = LogFilter::builder().exclude("connection").build().unwrap();
        let r = f.apply(SAMPLE);
        assert_eq!(r.matched, 5, "every line but the one naming a connection");
    }

    /// The deliberate consequence of splitting the stamp off: predicates see the
    /// message, so a term that only appears in the timestamp no longer matches
    /// anything. This used to exclude the four stamped lines.
    ///
    /// It is a trade, not an oversight. Matching the raw line would mean an
    /// anchored pattern silently stops working the moment a stamp appears in
    /// front of it (see `anchored_patterns_survive_a_timestamp`), which is the
    /// worse failure because it is invisible. Filtering by time belongs on the
    /// parsed `ts`, as a `since=`/`until=` pair, not on substring luck.
    #[test]
    fn filters_see_the_message_not_the_timestamp() {
        let f = LogFilter::builder().exclude("2026-07-26").build().unwrap();
        assert_eq!(f.apply(SAMPLE).matched, 6, "the date is no longer in the text");
        let f = LogFilter::builder().contains("2026-07-26").build().unwrap();
        assert_eq!(f.apply(SAMPLE).matched, 0);
    }

    /// The reason the split exists.
    #[test]
    fn anchored_patterns_survive_a_timestamp() {
        let f = LogFilter::builder().pattern("^INFO").build().unwrap();
        let r = f.apply(SAMPLE);
        assert_eq!(r.matched, 1, "^ still anchors the message, not the stamp");
        assert_eq!(r.lines[0].text, "INFO  starting extract");
        assert_eq!(r.lines[0].ts.as_deref(), Some("2026-07-26T10:00:00Z"));
    }

    #[test]
    fn stamped_lines_carry_ts_and_unstamped_ones_do_not() {
        let r = LogFilter::builder().build().unwrap().apply(SAMPLE);
        let stamped: Vec<_> = r.lines.iter().map(|l| l.ts.is_some()).collect();
        assert_eq!(stamped, [true, true, false, true, true, false]);
        // The stamp is removed from the text, never duplicated into it.
        assert_eq!(r.lines[2].text, "rows=1200 exported");
        assert!(r.lines.iter().all(|l| !l.text.starts_with("2026-")));
    }

    /// A filtered view is still a log: dropping the times on the way out would
    /// be worse than never parsing them.
    #[test]
    fn to_text_reattaches_the_timestamp() {
        let r = LogFilter::builder().pattern("^ERROR").build().unwrap().apply(SAMPLE);
        assert_eq!(r.to_text(), "2026-07-26T10:00:03Z ERROR upstream timeout\n");
    }

    #[test]
    fn split_timestamp_accepts_the_real_shapes() {
        // Docker / kubelet: nanosecond precision, Z.
        assert_eq!(
            split_timestamp("2026-07-26T10:00:00.123456789Z hello"),
            (Some("2026-07-26T10:00:00.123456789Z"), "hello")
        );
        // Whole seconds, and a numeric offset.
        assert_eq!(
            split_timestamp("2026-07-26T10:00:00Z hello"),
            (Some("2026-07-26T10:00:00Z"), "hello")
        );
        assert_eq!(
            split_timestamp("2026-07-26T10:00:00+08:00 hello"),
            (Some("2026-07-26T10:00:00+08:00"), "hello")
        );
        // RFC3339 permits a space separator.
        assert_eq!(
            split_timestamp("2026-07-26 10:00:00Z hello"),
            (Some("2026-07-26 10:00:00Z"), "hello")
        );
        // A stamp on its own line has an empty message.
        assert_eq!(
            split_timestamp("2026-07-26T10:00:00Z"),
            (Some("2026-07-26T10:00:00Z"), "")
        );
    }

    #[test]
    fn split_timestamp_rejects_near_misses() {
        // Ordinary output that merely starts with digits.
        for line in [
            "rows=1200 exported",
            "2026-07-26 no time of day",
            "2026-07-26T10:00:00 no offset",
            "2026-07-26T10:00:00.Z empty fraction",
            "2026-07-26T10:00:00+8:00 short offset",
            // No separator: this is one token, not a stamp plus a message.
            "2026-07-26T10:00:00Zed",
            "10:00:00Z leading time only",
        ] {
            assert_eq!(split_timestamp(line), (None, line), "should not split {line:?}");
        }
    }

    #[test]
    fn regex_filters_and_reports_bad_patterns() {
        let f = LogFilter::builder().pattern(r"rows=\d+").build().unwrap();
        assert_eq!(f.apply(SAMPLE).matched, 1);
        let err = LogFilter::builder().pattern("(unclosed").build().unwrap_err();
        assert!(matches!(err, FilterError::BadPattern(_)));
        let long = "a".repeat(MAX_PATTERN_LEN + 1);
        assert!(matches!(
            LogFilter::builder().pattern(long).build().unwrap_err(),
            FilterError::PatternTooLong(_)
        ));
    }

    #[test]
    fn level_inference_reads_the_head_only() {
        assert_eq!(Level::infer("2026-07-26T10:00:03Z ERROR upstream timeout"), Level::Error);
        assert_eq!(Level::infer("[warn] disk almost full"), Level::Warn);
        assert_eq!(Level::infer("rows=1200 exported"), Level::Plain);
        // The false positive that matters: output that merely talks about errors.
        assert_eq!(
            Level::infer("checked 4000 records and found no problems whatsoever, zero error rows"),
            Level::Plain,
        );
        // Word boundaries, not substrings.
        assert_eq!(Level::infer("traceparent=00-abc-def-01"), Level::Plain);
        assert_eq!(Level::infer("errors_total 0"), Level::Plain);
        // Highest severity in the head wins, not the last token seen.
        assert_eq!(Level::infer("WARN recovering from error"), Level::Error);
    }

    #[test]
    fn level_filter_selects_inferred_levels() {
        let f = LogFilter::builder().levels([Level::Error, Level::Warn]).build().unwrap();
        let r = f.apply(SAMPLE);
        assert_eq!(r.matched, 2);
        assert_eq!(r.lines.iter().map(|l| l.n).collect::<Vec<_>>(), vec![4, 5]);
    }

    #[test]
    fn context_keeps_neighbours_marked_unmatched() {
        let f = LogFilter::builder().levels([Level::Error]).context(1).build().unwrap();
        let r = f.apply(SAMPLE);
        assert_eq!(r.matched, 1, "context lines don't inflate the match count");
        assert_eq!(r.lines.iter().map(|l| l.n).collect::<Vec<_>>(), vec![4, 5, 6]);
        assert_eq!(r.lines.iter().map(|l| l.matched).collect::<Vec<_>>(), vec![false, true, false]);
    }

    #[test]
    fn context_at_the_edges_does_not_overflow() {
        // A match on the first line with context must not underflow to a huge
        // index, and one on the last must not run past the end.
        let f = LogFilter::builder().contains("starting").context(3).build().unwrap();
        assert_eq!(f.apply(SAMPLE).lines.iter().map(|l| l.n).collect::<Vec<_>>(), vec![1, 2, 3, 4]);
        let f = LogFilter::builder().contains("done").context(3).build().unwrap();
        assert_eq!(f.apply(SAMPLE).lines.iter().map(|l| l.n).collect::<Vec<_>>(), vec![3, 4, 5, 6]);
    }

    #[test]
    fn limit_truncates_head_or_tail() {
        let f = LogFilter::builder().limit(2).build().unwrap();
        let r = f.apply(SAMPLE);
        assert!(r.truncated);
        assert_eq!(r.matched, 6, "matched counts pre-limit so the UI can say 2 of 6");
        assert_eq!(r.lines.iter().map(|l| l.n).collect::<Vec<_>>(), vec![1, 2]);
        let f = LogFilter::builder().limit(2).tail(true).build().unwrap();
        assert_eq!(f.apply(SAMPLE).lines.iter().map(|l| l.n).collect::<Vec<_>>(), vec![5, 6]);
    }

    #[test]
    fn limit_is_clamped_to_max() {
        let f = LogFilter::builder().limit(MAX_LIMIT * 10).build().unwrap();
        assert_eq!(f.effective_limit(), MAX_LIMIT);
    }

    #[test]
    fn parse_query_round_trips_the_grammar() {
        let f = LogFilter::parse_query("q=timeout&level=error,warn&context=1&limit=10&tail=1")
            .unwrap();
        assert!(!f.is_noop());
        assert!(f.is_tail());
        assert_eq!(f.effective_limit(), 10);
        let r = f.apply(SAMPLE);
        assert_eq!(r.matched, 1);
    }

    #[test]
    fn parse_query_ignores_unrelated_params() {
        // `offset`/`task` belong to the endpoint, not the filter grammar.
        let f = LogFilter::parse_query("offset=120&task=extract").unwrap();
        assert!(f.is_noop());
        assert!(!query_has_filter("offset=120&task=extract"));
        assert!(query_has_filter("offset=120&level=error"));
        // A cap with no predicate still counts as filtering — otherwise
        // `?limit=10` would silently return everything.
        assert!(query_has_filter("limit=10"));
    }

    #[test]
    fn parse_pairs_keeps_order_and_repeats() {
        let p = parse_pairs("?q=a&q=b&exclude=c");
        assert_eq!(
            p,
            vec![
                ("q".to_string(), "a".to_string()),
                ("q".to_string(), "b".to_string()),
                ("exclude".to_string(), "c".to_string()),
            ]
        );
    }

    #[test]
    fn parse_query_percent_decodes_values() {
        let f = LogFilter::parse_query("q=disk%20full").unwrap();
        assert_eq!(f.apply("disk full\nok").matched, 1);
        let f = LogFilter::parse_query("regex=rows%3D%5Cd%2B").unwrap();
        assert_eq!(f.apply(SAMPLE).matched, 1);
    }

    #[test]
    fn parse_query_applies_case_flag_regardless_of_order() {
        // `case` after the term must still take effect — the terms are folded
        // with the flag, so a naive single pass would get this wrong.
        let f = LogFilter::parse_query("q=UPSTREAM&case=1").unwrap();
        assert_eq!(f.apply(SAMPLE).matched, 0);
        let f = LogFilter::parse_query("case=1&q=UPSTREAM").unwrap();
        assert_eq!(f.apply(SAMPLE).matched, 0);
    }

    #[test]
    fn parse_query_rejects_bad_input() {
        assert!(matches!(
            LogFilter::parse_query("level=loud").unwrap_err(),
            FilterError::UnknownLevel(_)
        ));
        assert!(matches!(
            LogFilter::parse_query("limit=lots").unwrap_err(),
            FilterError::NotANumber { param: "limit", .. }
        ));
        assert!(matches!(
            LogFilter::parse_query("regex=%5B").unwrap_err(),
            FilterError::BadPattern(_)
        ));
    }

    #[test]
    fn level_aliases_parse() {
        assert_eq!(Level::parse("WARNING"), Some(Level::Warn));
        assert_eq!(Level::parse("fatal"), Some(Level::Error));
        assert_eq!(Level::parse("plain"), Some(Level::Plain));
        assert_eq!(Level::parse("shout"), None);
    }

    #[test]
    fn head_slice_never_splits_a_multibyte_char() {
        // The 64-byte head window must clamp to a char boundary rather than
        // panicking, and the clamp must not change what's inferred.
        //
        // Both alignments are covered: `é` is 2 bytes, so 40 of them put byte 64
        // exactly on a boundary, while a 1-byte prefix shifts it to land *inside*
        // a character — the case that panics if the clamp is missing.
        let aligned = format!("{}ERROR tail", "é".repeat(40));
        let straddling = format!("x{}ERROR tail", "é".repeat(40));

        // In both, "ERROR" sits past byte 64, so it is outside the head window
        // and must not be read as a level — that window is the whole reason
        // `echo "no errors found"` isn't an error line.
        assert_eq!(Level::infer(&aligned), Level::Plain);
        assert_eq!(Level::infer(&straddling), Level::Plain);

        // And the same token *inside* the window is still found, so the clamp
        // narrows the slice without breaking inference.
        assert_eq!(Level::infer(&format!("é ERROR {}", "é".repeat(40))), Level::Error);
    }

    #[test]
    fn to_text_rejoins_kept_lines() {
        let f = LogFilter::builder().levels([Level::Error]).build().unwrap();
        assert_eq!(f.apply(SAMPLE).to_text(), "2026-07-26T10:00:03Z ERROR upstream timeout\n");
    }
}
