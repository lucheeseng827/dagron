//! What a job says of its tests when it is over: its JUnit and SARIF reports
//! read where the job wrote them, and said on the job's own stream in a few
//! short lines, for whoever reports the job to read.
//!
//! A report is a file in the job's workspace, and nothing carries a file out
//! of a job but its stream. So the reading is done here, where the file is,
//! and what is said is bounded: counts, the failures that name a place in
//! the repository as annotations ([`ANNOTATIONS_MAX`] of them, which is what
//! a forge takes in one request), and the failures by name
//! ([`FAILED_MAX`]). Each is a line of its own, short enough for any reader
//! of lines, and how many were left out is said.
//!
//! These lines are the job's word about itself, on a stream its steps write
//! to as well. A reader takes them for that: a job can say anything of its
//! own tests, as it could by writing any report. How the job ended is not
//! theirs to say.
//!
//! **JUnit** is read by a scanner of this module's own. The crate links
//! nothing but what it must, and a report needs little of XML: elements,
//! their attributes, their text. It is not a validating parser, and takes a
//! file that is not well formed as far as it can be read.

use std::path::{Component, Path, PathBuf};

use serde_json::{json, Value};

/// The annotations said of one job: what a forge takes in one request.
pub const ANNOTATIONS_MAX: usize = 50;
/// The failures said by name.
pub const FAILED_MAX: usize = 50;
/// How much of a report file is read. One larger is read to here, and said
/// to have been cut.
pub const FILE_MAX: u64 = 32 * 1024 * 1024;
/// The longest path, title and message said, in characters.
const PATH_MAX: usize = 240;
const TITLE_MAX: usize = 160;
const MESSAGE_MAX: usize = 400;
/// How much of what the steps wrote to their summaries is said, in bytes,
/// and in lines of how many.
pub const SUMMARY_MAX: usize = 24 * 1024;
const SUMMARY_LINE: usize = 3 * 1024;

/// The reports a job was told it writes (`CI_REPORT_JUNIT`,
/// `CI_REPORT_SARIF`): paths in its workspace.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Reports {
    pub junit: Vec<PathBuf>,
    pub sarif: Vec<PathBuf>,
}

impl Reports {
    pub fn is_empty(&self) -> bool {
        self.junit.is_empty() && self.sarif.is_empty()
    }
}

/// A failure, or a finding, at a place in the repository.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Annotation {
    pub path: String,
    pub start_line: u64,
    pub end_line: u64,
    /// `failure`, `warning` or `notice`: a forge's three.
    pub level: &'static str,
    pub title: String,
    pub message: String,
}

/// What the reports held.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Read {
    pub tests: u64,
    pub failures: u64,
    pub errors: u64,
    pub skipped: u64,
    /// SARIF results.
    pub findings: u64,
    pub annotations: Vec<Annotation>,
    /// Every test that failed, by name, with what it said.
    pub failed: Vec<(String, String)>,
    /// How many annotations and failures there were beyond those kept.
    pub more_annotations: u64,
    pub more_failed: u64,
    /// A report that could not be read, and why.
    pub problems: Vec<String>,
}

fn clamp(s: &str, max: usize) -> String {
    let s = s.trim();
    if s.chars().count() <= max {
        return s.to_string();
    }
    let mut out: String = s.chars().take(max.saturating_sub(1)).collect();
    out.push('…');
    out
}

/// A report's path for a place, as a path in the repository: relative to
/// the workspace, with no way out of it. `None` for one that is elsewhere.
fn in_repository(raw: &str, workspace: &Path) -> Option<String> {
    let raw = raw.trim();
    let raw = raw.strip_prefix("file://").unwrap_or(raw);
    if raw.is_empty() {
        return None;
    }
    let path = Path::new(raw);
    let relative = if path.is_absolute() {
        path.strip_prefix(workspace).ok()?.to_path_buf()
    } else {
        path.to_path_buf()
    };
    let mut parts = Vec::new();
    for part in relative.components() {
        match part {
            Component::Normal(p) => parts.push(p.to_str()?.to_string()),
            Component::CurDir => {}
            _ => return None,
        }
    }
    if parts.is_empty() {
        return None;
    }
    let joined = parts.join("/");
    (joined.chars().count() <= PATH_MAX).then_some(joined)
}

impl Read {
    fn annotate(&mut self, a: Annotation) {
        if self.annotations.len() < ANNOTATIONS_MAX {
            self.annotations.push(a);
        } else {
            self.more_annotations += 1;
        }
    }

    fn fail(&mut self, name: &str, message: &str) {
        if self.failed.len() < FAILED_MAX {
            self.failed
                .push((clamp(name, TITLE_MAX), clamp(message, MESSAGE_MAX)));
        } else {
            self.more_failed += 1;
        }
    }

    /// The lines this is said in, each whole and short: the counts first.
    pub fn lines(&self) -> Vec<String> {
        let mut out = vec![format!(
            "::dagron-ci::report::{}",
            json!({
                "tests": self.tests,
                "failures": self.failures,
                "errors": self.errors,
                "skipped": self.skipped,
                "findings": self.findings,
                "more_annotations": self.more_annotations,
                "more_failed": self.more_failed,
                "problems": self.problems,
            })
        )];
        for a in &self.annotations {
            out.push(format!(
                "::dagron-ci::annotation::{}",
                json!({
                    "path": a.path,
                    "start_line": a.start_line,
                    "end_line": a.end_line,
                    "level": a.level,
                    "title": a.title,
                    "message": a.message,
                })
            ));
        }
        for (name, message) in &self.failed {
            out.push(format!(
                "::dagron-ci::failed::{}",
                json!({ "name": name, "message": message })
            ));
        }
        out
    }
}

/// Read the job's reports, each where the job wrote it. `None` where the job
/// was told of none: nothing is said then.
pub fn read(reports: &Reports, workspace: &Path) -> Option<Read> {
    read_masked(reports, workspace, &|text: &str| text.to_string())
}

/// [`read`], with each report's text through `mask` before it is read:
/// what the job's steps had masked is `***` in what is said of it, and is
/// so before anything of it is cut short.
pub fn read_masked(
    reports: &Reports,
    workspace: &Path,
    mask: &dyn Fn(&str) -> String,
) -> Option<Read> {
    if reports.is_empty() {
        return None;
    }
    let mut read = Read::default();
    type Take<'a> = &'a dyn Fn(&str, &Path, &mut Read) -> Result<(), String>;
    let mut each = |paths: &[PathBuf], kind: &str, take: Take| {
        for path in paths {
            let at = if path.is_absolute() {
                path.clone()
            } else {
                workspace.join(path)
            };
            match file(&at) {
                Ok((text, cut)) => {
                    if cut {
                        read.problems.push(format!(
                            "{} is larger than is read ({} bytes): its end was left out",
                            path.display(),
                            FILE_MAX
                        ));
                    }
                    if let Err(why) = take(&text, workspace, &mut read) {
                        read.problems
                            .push(format!("{} is not a {kind} report: {why}", path.display()));
                    }
                }
                Err(why) => read
                    .problems
                    .push(format!("{} could not be read: {why}", path.display())),
            }
        }
    };
    // Each field masked as it is decoded, before it is cut short: the
    // report's own text is read as it is, so that no mask breaks its syntax.
    each(&reports.junit, "JUnit", &|t, w, r| {
        junit_masked(t, w, r, mask)
    });
    each(&reports.sarif, "SARIF", &|t, w, r| {
        sarif_masked(t, w, r, mask)
    });
    read.problems = read
        .problems
        .iter()
        .take(8)
        .map(|p| clamp(p, MESSAGE_MAX))
        .collect();
    // And once more, whole, what was said: a value cut by a field's bound
    // is masked where the whole of it was.
    for a in &mut read.annotations {
        a.path = mask(&a.path);
        a.title = mask(&a.title);
        a.message = mask(&a.message);
    }
    for (name, said) in &mut read.failed {
        *name = mask(name);
        *said = mask(said);
    }
    for problem in &mut read.problems {
        *problem = mask(problem);
    }
    Some(read)
}

/// Open `path` where it names a file. What is none (a directory, a device,
/// a pipe that nobody writes to) is said to be none: a pipe's opening waits
/// for a writer, and what a job says of itself is not what it waits for.
///
/// It is opened without waiting, and its kind is asked of what was opened,
/// so that it is the kind of what is then read, whatever a step puts at the
/// path meanwhile. A link is followed: a report may be one, and what it
/// leads to is held to the same.
pub(crate) fn open_file(path: &Path) -> Result<std::fs::File, String> {
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NONBLOCK);
    }
    let file = options.open(path).map_err(|e| e.to_string())?;
    if !file.metadata().map_err(|e| e.to_string())?.is_file() {
        return Err("it is not a file".into());
    }
    Ok(file)
}

/// A report file's text, to [`FILE_MAX`], and whether it was longer.
fn file(path: &Path) -> Result<(String, bool), String> {
    use std::io::Read as _;
    let f = open_file(path)?;
    let mut bytes = Vec::new();
    f.take(FILE_MAX + 1)
        .read_to_end(&mut bytes)
        .map_err(|e| e.to_string())?;
    let cut = bytes.len() as u64 > FILE_MAX;
    bytes.truncate(FILE_MAX as usize);
    Ok((String::from_utf8_lossy(&bytes).into_owned(), cut))
}

// ── XML, as much as a report needs ───────────────────────────────────────────

#[derive(Debug, PartialEq, Eq)]
enum Token {
    Start {
        name: String,
        attrs: Vec<(String, String)>,
        empty: bool,
    },
    End(String),
    Text(String),
}

struct Xml<'a> {
    s: &'a str,
    i: usize,
}

fn unescape(s: &str) -> String {
    if !s.contains('&') {
        return s.to_string();
    }
    let mut out = String::with_capacity(s.len());
    let mut rest = s;
    while let Some(at) = rest.find('&') {
        out.push_str(&rest[..at]);
        rest = &rest[at..];
        let Some(end) = rest.find(';').filter(|end| *end <= 10) else {
            out.push('&');
            rest = &rest[1..];
            continue;
        };
        let name = &rest[1..end];
        let ch = match name {
            "lt" => Some('<'),
            "gt" => Some('>'),
            "amp" => Some('&'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            _ => name
                .strip_prefix("#x")
                .or_else(|| name.strip_prefix("#X"))
                .map(|hex| u32::from_str_radix(hex, 16))
                .or_else(|| name.strip_prefix('#').map(|dec| dec.parse::<u32>()))
                .and_then(|n| n.ok())
                .and_then(char::from_u32),
        };
        match ch {
            Some(ch) => {
                out.push(ch);
                rest = &rest[end + 1..];
            }
            None => {
                out.push('&');
                rest = &rest[1..];
            }
        }
    }
    out.push_str(rest);
    out
}

impl<'a> Xml<'a> {
    fn new(s: &'a str) -> Self {
        Xml { s, i: 0 }
    }

    /// The next element, end or text. `None` at the end, and where what is
    /// left cannot be read as any of them.
    fn next(&mut self) -> Option<Token> {
        loop {
            let rest = &self.s[self.i..];
            if rest.is_empty() {
                return None;
            }
            if !rest.starts_with('<') {
                let end = rest.find('<').unwrap_or(rest.len());
                self.i += end;
                let text = &rest[..end];
                if text.trim().is_empty() {
                    continue;
                }
                return Some(Token::Text(unescape(text)));
            }
            if let Some(body) = rest.strip_prefix("<![CDATA[") {
                let end = body.find("]]>")?;
                self.i += "<![CDATA[".len() + end + "]]>".len();
                return Some(Token::Text(body[..end].to_string()));
            }
            if rest.starts_with("<!--") {
                self.i += rest.find("-->")? + 3;
                continue;
            }
            if rest.starts_with("<?") {
                self.i += rest.find("?>")? + 2;
                continue;
            }
            if rest.starts_with("<!") {
                self.i += rest.find('>')? + 1;
                continue;
            }
            if let Some(body) = rest.strip_prefix("</") {
                let end = body.find('>')?;
                self.i += 2 + end + 1;
                return Some(Token::End(body[..end].trim().to_string()));
            }
            // A start tag: its name, then its attributes, to the `>` that is
            // not inside a quoted value.
            let bytes = rest.as_bytes();
            let mut at = 1;
            while at < bytes.len()
                && !bytes[at].is_ascii_whitespace()
                && bytes[at] != b'>'
                && bytes[at] != b'/'
            {
                at += 1;
            }
            let name = rest[1..at].to_string();
            if name.is_empty() {
                return None;
            }
            let mut attrs = Vec::new();
            let mut empty = false;
            loop {
                while at < bytes.len() && bytes[at].is_ascii_whitespace() {
                    at += 1;
                }
                match bytes.get(at) {
                    None => return None,
                    Some(b'>') => {
                        at += 1;
                        break;
                    }
                    Some(b'/') => {
                        empty = true;
                        at += 1;
                        continue;
                    }
                    Some(_) => {}
                }
                let key_at = at;
                while at < bytes.len()
                    && bytes[at] != b'='
                    && bytes[at] != b'>'
                    && !bytes[at].is_ascii_whitespace()
                {
                    at += 1;
                }
                let key = rest[key_at..at].to_string();
                while at < bytes.len() && bytes[at].is_ascii_whitespace() {
                    at += 1;
                }
                if bytes.get(at) != Some(&b'=') {
                    // An attribute with no value: kept as empty.
                    attrs.push((key, String::new()));
                    continue;
                }
                at += 1;
                while at < bytes.len() && bytes[at].is_ascii_whitespace() {
                    at += 1;
                }
                let quote = *bytes.get(at)?;
                if quote != b'"' && quote != b'\'' {
                    return None;
                }
                let value_at = at + 1;
                let close = rest[value_at..].find(quote as char)?;
                attrs.push((key, unescape(&rest[value_at..value_at + close])));
                at = value_at + close + 1;
            }
            self.i += at;
            return Some(Token::Start { name, attrs, empty });
        }
    }
}

fn attr<'a>(attrs: &'a [(String, String)], name: &str) -> Option<&'a str> {
    attrs
        .iter()
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.as_str())
        .filter(|v| !v.trim().is_empty())
}

/// Read a JUnit report: its test cases, and of each that failed, its name,
/// what it said, and where it is if the report says.
pub fn junit(text: &str, workspace: &Path, read: &mut Read) -> Result<(), String> {
    junit_masked(text, workspace, read, &|t: &str| t.to_string())
}

/// [`junit`], with each field through `mask` as it is decoded.
fn junit_masked(
    text: &str,
    workspace: &Path,
    read: &mut Read,
    mask: &dyn Fn(&str) -> String,
) -> Result<(), String> {
    let mut xml = Xml::new(text);
    let mut seen_suite = false;
    // The test case being read: its name, its place, and what failed it.
    struct Case {
        name: String,
        path: Option<String>,
        line: Option<u64>,
        failed: Option<(&'static str, String)>,
        skipped: bool,
        within: Option<String>,
    }
    let mut case: Option<Case> = None;
    let end = |case: Case, read: &mut Read| {
        read.tests += 1;
        if case.skipped {
            read.skipped += 1;
        }
        let Some((kind, said)) = case.failed else {
            return;
        };
        match kind {
            "error" => read.errors += 1,
            _ => read.failures += 1,
        }
        // The first lines of what it said: the rest is its trace.
        let said = said
            .lines()
            .map(str::trim)
            .filter(|l| !l.is_empty())
            .take(6)
            .collect::<Vec<_>>()
            .join("\n");
        let said = if said.is_empty() {
            format!("the test {kind}ed")
        } else {
            mask(&said)
        };
        let name = mask(&case.name);
        if let Some(path) = &case.path {
            let line = case.line.filter(|l| *l > 0).unwrap_or(1);
            read.annotate(Annotation {
                path: mask(path),
                start_line: line,
                end_line: line,
                level: "failure",
                title: clamp(&name, TITLE_MAX),
                message: clamp(&said, MESSAGE_MAX),
            });
        }
        read.fail(&name, &said);
    };
    // pytest counts a test's line from 0, and names its suite for itself
    // unless it is told another name: its lines are read as it counts them.
    let mut from_zero = false;
    while let Some(token) = xml.next() {
        match token {
            Token::Start { name, attrs, empty } => match name.as_str() {
                "testsuites" => seen_suite = true,
                "testsuite" => {
                    seen_suite = true;
                    from_zero = attr(&attrs, "name") == Some("pytest");
                }
                "testcase" => {
                    if let Some(open) = case.take() {
                        end(open, read);
                    }
                    let own = attr(&attrs, "name").unwrap_or("a test");
                    let name = match attr(&attrs, "classname") {
                        Some(class) => format!("{class}.{own}"),
                        None => own.to_string(),
                    };
                    let this = Case {
                        name,
                        path: attr(&attrs, "file").and_then(|f| in_repository(f, workspace)),
                        line: attr(&attrs, "line")
                            .and_then(|l| l.trim().parse::<u64>().ok())
                            .map(|line| line + u64::from(from_zero)),
                        failed: None,
                        skipped: false,
                        within: None,
                    };
                    if empty {
                        end(this, read);
                    } else {
                        case = Some(this);
                    }
                }
                "failure" | "error" => {
                    if let Some(case) = case.as_mut() {
                        let kind = if name == "error" { "error" } else { "failure" };
                        if case.failed.is_none() {
                            let said = attr(&attrs, "message")
                                .or_else(|| attr(&attrs, "type"))
                                .unwrap_or_default();
                            case.failed = Some((kind, said.to_string()));
                        }
                        case.within = (!empty).then(|| name.clone());
                    }
                }
                "skipped" => {
                    if let Some(case) = case.as_mut() {
                        case.skipped = true;
                    }
                }
                _ => {}
            },
            Token::Text(text) => {
                // What a failure holds between its tags is used where its
                // message said nothing.
                if let Some(case) = case.as_mut() {
                    if case.within.is_some() {
                        if let Some((_, said)) = case.failed.as_mut() {
                            if said.trim().is_empty() {
                                *said = text;
                            }
                        }
                    }
                }
            }
            Token::End(name) => match name.as_str() {
                "testcase" => {
                    if let Some(open) = case.take() {
                        end(open, read);
                    }
                }
                "failure" | "error" => {
                    if let Some(case) = case.as_mut() {
                        case.within = None;
                    }
                }
                _ => {}
            },
        }
    }
    if let Some(open) = case.take() {
        end(open, read);
    }
    if !seen_suite {
        return Err("it holds no test suite".into());
    }
    Ok(())
}

/// Read a SARIF log: each result of each run, at the first place it names.
pub fn sarif(text: &str, workspace: &Path, read: &mut Read) -> Result<(), String> {
    sarif_masked(text, workspace, read, &|t: &str| t.to_string())
}

/// [`sarif`], with each field through `mask` as it is decoded.
fn sarif_masked(
    text: &str,
    workspace: &Path,
    read: &mut Read,
    mask: &dyn Fn(&str) -> String,
) -> Result<(), String> {
    let log: Value = serde_json::from_str(text).map_err(|e| e.to_string())?;
    let runs = log["runs"].as_array().ok_or("it holds no runs")?;
    for run in runs {
        for result in run["results"].as_array().into_iter().flatten() {
            read.findings += 1;
            let level = match result["level"].as_str() {
                Some("error") => "failure",
                Some("note") | Some("none") => "notice",
                _ => "warning",
            };
            let rule = mask(result["ruleId"].as_str().unwrap_or("a finding"));
            let message = result["message"]["text"]
                .as_str()
                .map(mask)
                .unwrap_or_else(|| rule.clone());
            let place = &result["locations"][0]["physicalLocation"];
            let path = place["artifactLocation"]["uri"]
                .as_str()
                .and_then(|uri| in_repository(uri, workspace))
                .map(|path| mask(&path));
            let Some(path) = path else {
                read.fail(&rule, &message);
                continue;
            };
            let start = place["region"]["startLine"]
                .as_u64()
                .filter(|l| *l > 0)
                .unwrap_or(1);
            let end = place["region"]["endLine"]
                .as_u64()
                .filter(|l| *l >= start)
                .unwrap_or(start);
            read.annotate(Annotation {
                path,
                start_line: start,
                end_line: end,
                level,
                title: clamp(&rule, TITLE_MAX),
                message: clamp(&message, MESSAGE_MAX),
            });
        }
    }
    Ok(())
}

/// What the steps wrote to their summaries, as the lines it is said in:
/// each a JSON string, to be put back end to end. Cut at [`SUMMARY_MAX`],
/// and said to have been.
pub fn summary_lines(text: &str) -> Vec<String> {
    let text = text.trim_end();
    if text.trim().is_empty() {
        return Vec::new();
    }
    let mut kept = text;
    let mut cut = false;
    if kept.len() > SUMMARY_MAX {
        let mut end = SUMMARY_MAX;
        while !kept.is_char_boundary(end) {
            end -= 1;
        }
        kept = &kept[..end];
        cut = true;
    }
    let mut out = Vec::new();
    let mut rest = kept;
    while !rest.is_empty() {
        let mut end = rest.len().min(SUMMARY_LINE);
        while !rest.is_char_boundary(end) {
            end -= 1;
        }
        out.push(format!(
            "::dagron-ci::summary::{}",
            Value::String(rest[..end].to_string())
        ));
        rest = &rest[end..];
    }
    if cut {
        out.push(format!(
            "::dagron-ci::summary::{}",
            Value::String(format!(
                "\n\n_The summary is longer than is kept ({SUMMARY_MAX} bytes): its end was left out._"
            ))
        ));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ws() -> PathBuf {
        PathBuf::from("/work/repo")
    }

    #[test]
    fn xml_is_read_as_far_as_a_report_needs() {
        let mut xml = Xml::new(
            "<?xml version=\"1.0\"?>\n<!-- made by a tool -->\n<a x=\"1 &lt; 2\" y='it&apos;s'>\n  hi &amp; bye\n  <b/>\n  <![CDATA[<raw & kept>]]>\n</a>",
        );
        assert_eq!(
            xml.next(),
            Some(Token::Start {
                name: "a".into(),
                attrs: vec![("x".into(), "1 < 2".into()), ("y".into(), "it's".into())],
                empty: false
            })
        );
        assert_eq!(xml.next(), Some(Token::Text("\n  hi & bye\n  ".into())));
        assert_eq!(
            xml.next(),
            Some(Token::Start {
                name: "b".into(),
                attrs: vec![],
                empty: true
            })
        );
        assert_eq!(xml.next(), Some(Token::Text("<raw & kept>".into())));
        assert_eq!(xml.next(), Some(Token::End("a".into())));
        assert_eq!(xml.next(), None);
        // A `>` inside a quoted value does not end the tag, and a number
        // stands for its character.
        let mut xml = Xml::new("<t m=\"a > b &#65;&#x42;\"/>");
        assert_eq!(
            xml.next(),
            Some(Token::Start {
                name: "t".into(),
                attrs: vec![("m".into(), "a > b AB".into())],
                empty: true
            })
        );
        // What cannot be read ends the reading, and loses nothing before it.
        let mut xml = Xml::new("<ok/><broken attr=");
        assert!(matches!(xml.next(), Some(Token::Start { .. })));
        assert_eq!(xml.next(), None);
    }

    #[test]
    fn a_junit_report_is_counted_and_its_failures_are_placed_where_it_says() {
        // As pytest writes one: a place on each test case.
        let pytest = r#"<?xml version="1.0" encoding="utf-8"?>
<testsuites><testsuite name="pytest" errors="0" failures="1" skipped="1" tests="4">
  <testcase classname="tests.test_math" name="test_adds" file="tests/test_math.py" line="3" time="0.001"/>
  <testcase classname="tests.test_math" name="test_divides" file="/work/repo/tests/test_math.py" line="9" time="0.002">
    <failure message="assert 1 == 2">def test_divides():
&gt;       assert 1 == 2
E       assert 1 == 2

tests/test_math.py:10: AssertionError</failure>
  </testcase>
  <testcase classname="tests.test_io" name="test_slow" file="tests/test_io.py" line="1"><skipped message="slow"/></testcase>
  <testcase classname="tests.test_io" name="test_reads" file="../outside.py" line="4"><error type="OSError">boom
at the second line</error></testcase>
</testsuite></testsuites>"#;
        let mut read = Read::default();
        junit(pytest, &ws(), &mut read).unwrap();
        assert_eq!(
            (read.tests, read.failures, read.errors, read.skipped),
            (4, 1, 1, 1)
        );
        // The one with a place in the repository is an annotation there. An
        // absolute path inside the workspace is taken as its path within it,
        // and pytest's line, which it counts from 0, as the line it is.
        assert_eq!(
            read.annotations,
            [Annotation {
                path: "tests/test_math.py".into(),
                start_line: 10,
                end_line: 10,
                level: "failure",
                title: "tests.test_math.test_divides".into(),
                message: "assert 1 == 2".into(),
            }]
        );
        // Both are said by name. One whose place is outside the repository
        // is that only, and a failure with no message says its first lines.
        assert_eq!(
            read.failed,
            [
                (
                    "tests.test_math.test_divides".to_string(),
                    "assert 1 == 2".to_string()
                ),
                (
                    "tests.test_io.test_reads".to_string(),
                    "OSError".to_string()
                ),
            ]
        );

        // As a tool with no places writes one, and no counts on its suite.
        let bare = "<testsuite><testcase name=\"a\"/><testcase name=\"b\"><failure>it broke\n  at x</failure></testcase></testsuite>";
        let mut read = Read::default();
        junit(bare, &ws(), &mut read).unwrap();
        assert_eq!((read.tests, read.failures), (2, 1));
        assert!(read.annotations.is_empty(), "no place, so no annotation");
        assert_eq!(
            read.failed,
            [("b".to_string(), "it broke\nat x".to_string())]
        );

        // What is not a report says so.
        let mut read = Read::default();
        assert!(junit("<html><body/></html>", &ws(), &mut read).is_err());
        assert!(junit("not xml at all", &ws(), &mut read).is_err());
    }

    #[test]
    fn a_sarif_log_s_results_are_placed_and_levelled() {
        let log = json!({
            "version": "2.1.0",
            "runs": [{ "results": [
                { "ruleId": "no-unused", "level": "error", "message": { "text": "x is never read" },
                  "locations": [{ "physicalLocation": {
                      "artifactLocation": { "uri": "src/lib.rs" },
                      "region": { "startLine": 12, "endLine": 14 } } }] },
                { "ruleId": "style", "message": { "text": "prefer a let" },
                  "locations": [{ "physicalLocation": { "artifactLocation": { "uri": "file:///work/repo/src/main.rs" } } }] },
                { "ruleId": "info", "level": "note", "message": { "text": "fyi" },
                  "locations": [{ "physicalLocation": { "artifactLocation": { "uri": "/etc/passwd" } } }] },
            ] }]
        })
        .to_string();
        let mut read = Read::default();
        sarif(&log, &ws(), &mut read).unwrap();
        assert_eq!(read.findings, 3);
        assert_eq!(
            read.annotations
                .iter()
                .map(|a| (
                    a.path.as_str(),
                    a.start_line,
                    a.end_line,
                    a.level,
                    a.title.as_str()
                ))
                .collect::<Vec<_>>(),
            [
                ("src/lib.rs", 12, 14, "failure", "no-unused"),
                ("src/main.rs", 1, 1, "warning", "style"),
            ]
        );
        // One at a place outside the repository is said by its rule alone.
        assert_eq!(read.failed, [("info".to_string(), "fyi".to_string())]);
        let mut read = Read::default();
        assert!(sarif("{}", &ws(), &mut read).is_err());
        assert!(sarif("<xml/>", &ws(), &mut read).is_err());
    }

    #[test]
    fn what_is_said_is_bounded_and_says_what_it_left_out() {
        let mut cases = String::from("<testsuite>");
        for n in 0..(ANNOTATIONS_MAX + 7) {
            cases.push_str(&format!(
                "<testcase name=\"t{n}\" file=\"src/t.rs\" line=\"{}\"><failure message=\"{}\"/></testcase>",
                n + 1,
                "x".repeat(2000)
            ));
        }
        cases.push_str("</testsuite>");
        let mut read = Read::default();
        junit(&cases, &ws(), &mut read).unwrap();
        assert_eq!(read.annotations.len(), ANNOTATIONS_MAX);
        assert_eq!((read.more_annotations, read.more_failed), (7, 7));
        let lines = read.lines();
        assert_eq!(lines.len(), 1 + ANNOTATIONS_MAX + FAILED_MAX);
        assert!(lines[0].starts_with("::dagron-ci::report::{"));
        let counts: Value =
            serde_json::from_str(lines[0].strip_prefix("::dagron-ci::report::").unwrap()).unwrap();
        assert_eq!(
            (
                counts["tests"].as_u64(),
                counts["more_annotations"].as_u64()
            ),
            (Some(57), Some(7))
        );
        // Each line is short, whatever a test said.
        assert!(
            lines.iter().all(|l| l.len() < 2048 && !l.contains('\n')),
            "a line is too long"
        );

        // A job told of no report says nothing; one whose report is not
        // there says so.
        assert_eq!(read_none(), None);
        let asked = Reports {
            junit: vec![PathBuf::from("no/such.xml")],
            sarif: vec![],
        };
        let said = super::read(&asked, &std::env::temp_dir()).unwrap();
        assert_eq!(said.tests, 0);
        assert!(
            said.problems[0].starts_with("no/such.xml could not be read"),
            "{:?}",
            said.problems
        );
    }

    /// A report's path that names no file is said, and is not opened: a pipe
    /// that nobody writes to would be waited on for as long as the job may
    /// run.
    /// A masked value is masked however a report wrote it: escaped, as
    /// JUnit writes `&` in a failure's message, it is decoded first; and a
    /// mask that is a word of the report's own syntax breaks no report.
    #[test]
    fn a_masked_value_is_masked_however_a_report_wrote_it() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(
            dir.path().join("junit.xml"),
            "<testsuite><testcase name=\"pa&amp;ss\"><failure message=\"said pa&amp;ss\"/></testcase></testsuite>",
        )
        .unwrap();
        let asked = Reports {
            junit: vec![PathBuf::from("junit.xml")],
            sarif: Vec::new(),
        };
        let mask = |text: &str| text.replace("pa&ss", "***");
        let read = read_masked(&asked, dir.path(), &mask).unwrap();
        assert_eq!(read.failed, [("***".to_string(), "said ***".to_string())]);
        let syntax = |text: &str| text.replace("testcase", "***").replace("pa&ss", "***");
        let read = read_masked(&asked, dir.path(), &syntax).unwrap();
        assert!(read.problems.is_empty(), "{:?}", read.problems);
        assert_eq!((read.tests, read.failures), (1, 1));
    }

    #[cfg(unix)]
    #[test]
    fn what_is_no_file_is_not_opened() {
        use std::os::unix::ffi::OsStrExt;
        let dir = std::env::temp_dir().join(format!("dagron-ci-report-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(dir.join("a-directory")).unwrap();
        let pipe = std::ffi::CString::new(dir.join("pipe.xml").as_os_str().as_bytes()).unwrap();
        // SAFETY: `pipe` is a NUL-terminated path that outlives the call.
        assert_eq!(unsafe { libc::mkfifo(pipe.as_ptr(), 0o600) }, 0);
        let asked = Reports {
            junit: vec![PathBuf::from("pipe.xml")],
            sarif: vec![PathBuf::from("a-directory")],
        };
        let (tx, rx) = std::sync::mpsc::channel();
        let workspace = dir.clone();
        std::thread::spawn(move || {
            let _ = tx.send(super::read(&asked, &workspace));
        });
        let said = rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("a pipe is not waited on")
            .unwrap();
        assert_eq!(
            said.problems,
            [
                "pipe.xml could not be read: it is not a file",
                "a-directory could not be read: it is not a file",
            ]
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn read_none() -> Option<Read> {
        super::read(&Reports::default(), &std::env::temp_dir())
    }

    #[test]
    fn a_summary_is_said_in_short_lines_and_put_back_whole() {
        assert!(summary_lines("  \n").is_empty());
        let text = format!("## Results\n\n{}\nétoile ✓\n", "| a | b |\n".repeat(900));
        let lines = summary_lines(&text);
        assert!(lines.len() > 1);
        assert!(lines
            .iter()
            .all(|l| l.len() < 2 * SUMMARY_LINE + 64 && !l.contains('\n')));
        let back: String = lines
            .iter()
            .map(|l| {
                serde_json::from_str::<String>(l.strip_prefix("::dagron-ci::summary::").unwrap())
                    .unwrap()
            })
            .collect();
        assert_eq!(back, text.trim_end());
        // One longer than is kept is cut, and says so.
        let long = "é".repeat(SUMMARY_MAX);
        let back: String = summary_lines(&long)
            .iter()
            .map(|l| {
                serde_json::from_str::<String>(l.strip_prefix("::dagron-ci::summary::").unwrap())
                    .unwrap()
            })
            .collect();
        assert!(
            back.len() < long.len() && back.ends_with("its end was left out._"),
            "{}",
            &back[back.len() - 80..]
        );
    }
}
