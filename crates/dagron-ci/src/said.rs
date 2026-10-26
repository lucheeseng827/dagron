//! What a job said of itself as it ended: its tests' report and its steps'
//! summaries, in the lines dagron-ci-exec writes them in on the job's own
//! stream (`::dagron-ci::report::`, `::annotation::`, `::failed::`,
//! `::summary::`), read back for the job's check run.
//!
//! **These lines are the job's word, and no more.** A job's steps write to
//! the same stream, so any of them can be written by the job's own script.
//! They are taken for what a job may say of itself: where its tests failed,
//! and what it wants read beside its check. How the job ended is the
//! engine's to say and is never read from here. And they are held to bounds
//! here whatever wrote them: so many annotations, so much text, and a place
//! only inside the repository.

use serde_json::Value;

use crate::forge::Annotation;

/// What every line of dagron-ci-exec's own begins with.
pub(crate) const PREFIX: &str = "::dagron-ci::";
/// The annotations kept of one job: what GitHub takes in one request.
pub(crate) const ANNOTATIONS_MAX: usize = 50;
/// The failures listed by name.
const FAILED_MAX: usize = 50;
/// How much of the steps' summaries is kept, in bytes.
const SUMMARY_MAX: usize = 32 * 1024;
const PATH_MAX: usize = 240;
const TITLE_MAX: usize = 160;
const MESSAGE_MAX: usize = 400;

/// The counts a report's first line gives.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Counts {
    pub tests: u64,
    pub failures: u64,
    pub errors: u64,
    pub skipped: u64,
    pub findings: u64,
    pub more_annotations: u64,
    pub more_failed: u64,
    /// A report the job could not read, and why.
    pub problems: Vec<String>,
}

/// What one job said.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Said {
    pub counts: Option<Counts>,
    pub annotations: Vec<Annotation>,
    /// Each test that failed, by name, with what it said.
    pub failed: Vec<(String, String)>,
    /// What its steps wrote to their summaries: Markdown.
    pub summary: String,
}

fn clamp(text: &str, max: usize) -> String {
    let text = text.trim();
    if text.chars().count() <= max {
        return text.to_string();
    }
    let mut out: String = text.chars().take(max.saturating_sub(1)).collect();
    out.push('…');
    out
}

/// Whether `path` is a file's path inside a repository: relative, with no
/// way out.
fn in_repository(path: &str) -> bool {
    !path.is_empty()
        && path.chars().count() <= PATH_MAX
        && !path.starts_with('/')
        && !path.contains('\\')
        && !path.contains(['\n', '\r', '\0'])
        && path
            .split('/')
            .all(|part| !part.is_empty() && part != "." && part != "..")
}

fn annotation(v: &Value) -> Option<Annotation> {
    let path = v["path"].as_str().filter(|p| in_repository(p))?;
    let start = v["start_line"].as_u64().filter(|l| *l >= 1)?;
    let end = v["end_line"]
        .as_u64()
        .filter(|l| *l >= start)
        .unwrap_or(start);
    let level = match v["level"].as_str() {
        Some("failure") => "failure",
        Some("warning") => "warning",
        _ => "notice",
    };
    let message = clamp(v["message"].as_str().unwrap_or_default(), MESSAGE_MAX);
    Some(Annotation {
        path: path.to_string(),
        start_line: start,
        end_line: end,
        level,
        title: clamp(v["title"].as_str().unwrap_or_default(), TITLE_MAX),
        // GitHub refuses an annotation that says nothing.
        message: if message.is_empty() {
            "(no message)".to_string()
        } else {
            message
        },
    })
}

/// Read what a job said from the lines of its log, in their order. A line
/// that is not one of these, or cannot be read as one, is passed over.
pub(crate) fn read<'a>(lines: impl IntoIterator<Item = &'a str>) -> Said {
    let mut said = Said::default();
    let mut more_annotations = 0u64;
    let mut more_failed = 0u64;
    for line in lines {
        let Some(rest) = line.trim_end().strip_prefix(PREFIX) else {
            continue;
        };
        let Some((kind, json)) = rest.split_once("::") else {
            continue;
        };
        match kind {
            "report" => {
                let Ok(v) = serde_json::from_str::<Value>(json) else {
                    continue;
                };
                let n = |key: &str| v[key].as_u64().unwrap_or(0);
                // The last one said stands: a job says one.
                said.counts = Some(Counts {
                    tests: n("tests"),
                    failures: n("failures"),
                    errors: n("errors"),
                    skipped: n("skipped"),
                    findings: n("findings"),
                    more_annotations: n("more_annotations"),
                    more_failed: n("more_failed"),
                    problems: v["problems"]
                        .as_array()
                        .into_iter()
                        .flatten()
                        .filter_map(Value::as_str)
                        .take(8)
                        .map(|p| clamp(p, MESSAGE_MAX))
                        .collect(),
                });
            }
            "annotation" => {
                let Some(a) = serde_json::from_str::<Value>(json)
                    .ok()
                    .as_ref()
                    .and_then(annotation)
                else {
                    continue;
                };
                if said.annotations.len() < ANNOTATIONS_MAX {
                    said.annotations.push(a);
                } else {
                    more_annotations += 1;
                }
            }
            "failed" => {
                let Ok(v) = serde_json::from_str::<Value>(json) else {
                    continue;
                };
                let Some(name) = v["name"].as_str().filter(|n| !n.trim().is_empty()) else {
                    continue;
                };
                if said.failed.len() < FAILED_MAX {
                    said.failed.push((
                        clamp(name, TITLE_MAX),
                        clamp(v["message"].as_str().unwrap_or_default(), MESSAGE_MAX),
                    ));
                } else {
                    more_failed += 1;
                }
            }
            "summary" => {
                let Ok(part) = serde_json::from_str::<String>(json) else {
                    continue;
                };
                let room = SUMMARY_MAX.saturating_sub(said.summary.len());
                if part.len() <= room {
                    said.summary.push_str(&part);
                } else {
                    let mut end = room;
                    while !part.is_char_boundary(end) {
                        end -= 1;
                    }
                    said.summary.push_str(&part[..end]);
                }
            }
            _ => {}
        }
    }
    // What was said beyond what is kept is counted with what the job itself
    // left out. A job that said annotations and no counts has counts made
    // for it, so that what was left out is still said.
    if more_annotations > 0 || more_failed > 0 {
        let counts = said.counts.get_or_insert_with(Counts::default);
        counts.more_annotations += more_annotations;
        counts.more_failed += more_failed;
    }
    said
}

/// Text made safe to stand in a line of Markdown as code.
fn code(text: &str) -> String {
    text.replace(['`', '\n', '\r'], " ")
}

impl Said {
    /// What stands for a job whose log could not be read: nothing of the
    /// job's, and a line that says so. Why is for the operator's log, since
    /// it names the cell.
    pub(crate) fn unread() -> Said {
        Said {
            counts: Some(Counts {
                problems: vec![
                    "What this job said of its tests and its steps' summaries could not be \
                     read from its log when it ended."
                        .to_string(),
                ],
                ..Counts::default()
            }),
            ..Said::default()
        }
    }

    #[cfg(test)]
    pub(crate) fn is_empty(&self) -> bool {
        self.counts.is_none()
            && self.annotations.is_empty()
            && self.failed.is_empty()
            && self.summary.trim().is_empty()
    }

    /// The report as a check run's text, in Markdown: the counts, each test
    /// that failed, and what was left out or could not be read. Empty where
    /// the job said no report.
    pub(crate) fn text(&self) -> String {
        let mut out = String::new();
        if let Some(c) = &self.counts {
            if c.tests > 0 || c.failures > 0 || c.errors > 0 {
                let plural = |n: u64| if n == 1 { "" } else { "s" };
                out.push_str(&format!(
                    "**{} test{}**: {} failed, {} error{}, {} skipped.\n",
                    c.tests,
                    plural(c.tests),
                    c.failures,
                    c.errors,
                    plural(c.errors),
                    c.skipped
                ));
            }
            if c.findings > 0 {
                out.push_str(&format!(
                    "**{} finding{}** in its SARIF report.\n",
                    c.findings,
                    if c.findings == 1 { "" } else { "s" }
                ));
            }
        }
        if !self.failed.is_empty() {
            out.push('\n');
            for (name, message) in &self.failed {
                let first = message.lines().next().unwrap_or_default();
                if first.trim().is_empty() {
                    out.push_str(&format!("- `{}`\n", code(name)));
                } else {
                    out.push_str(&format!("- `{}`: `{}`\n", code(name), code(first)));
                }
            }
        }
        if let Some(c) = &self.counts {
            if c.more_failed > 0 {
                out.push_str(&format!("- … and {} more.\n", c.more_failed));
            }
            if c.more_annotations > 0 {
                out.push_str(&format!(
                    "\n{} more failure{} named a place and {} not annotated: a check run takes {ANNOTATIONS_MAX}.\n",
                    c.more_annotations,
                    if c.more_annotations == 1 { "" } else { "s" },
                    if c.more_annotations == 1 { "is" } else { "are" },
                ));
            }
            for problem in &c.problems {
                out.push_str(&format!("\n_{}_\n", code(problem)));
            }
        }
        out.trim().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn what_a_job_said_is_read_from_its_lines() {
        let log = [
            "::dagron-ci::step::1::start::test",
            "running 2 tests",
            "::dagron-ci::step::1::end::1",
            r#"::dagron-ci::report::{"tests":12,"failures":1,"errors":1,"skipped":2,"findings":0,"more_annotations":0,"more_failed":0,"problems":["lint.sarif could not be read: no such file"]}"#,
            r#"::dagron-ci::annotation::{"path":"src/lib.rs","start_line":7,"end_line":9,"level":"failure","title":"t.bad","message":"left != right"}"#,
            r#"::dagron-ci::failed::{"name":"t.bad","message":"left != right\nat line 7"}"#,
            r#"::dagron-ci::failed::{"name":"t.worse","message":""}"#,
            r###"::dagron-ci::summary::"## Built\n\n""###,
            r#"::dagron-ci::summary::"3 crates.""#,
            r#"::dagron-ci::result::{"exit_code":1}"#,
        ];
        let said = read(log);
        assert_eq!(
            said.annotations,
            [Annotation {
                path: "src/lib.rs".into(),
                start_line: 7,
                end_line: 9,
                level: "failure",
                title: "t.bad".into(),
                message: "left != right".into(),
            }]
        );
        assert_eq!(said.summary, "## Built\n\n3 crates.");
        assert_eq!(
            said.text(),
            "**12 tests**: 1 failed, 1 error, 2 skipped.\n\n- `t.bad`: `left != right`\n- `t.worse`\n\n_lint.sarif could not be read: no such file_"
        );
        assert!(!said.is_empty());
        // A job that said none of it has nothing to add to its check run.
        let quiet = read(["hello", "::dagron-ci::result::{}"]);
        assert!(quiet.is_empty() && quiet.text().is_empty());
    }

    /// The lines are a job's own word, on a stream its steps write to: what
    /// they may say is held to bounds here, whatever wrote them.
    #[test]
    fn what_a_job_says_is_held_to_bounds_whoever_wrote_it() {
        let ok = |path: &str, line: i64| {
            format!(
                r#"::dagron-ci::annotation::{{"path":{},"start_line":{line},"level":"shout","title":"t","message":""}}"#,
                Value::String(path.into())
            )
        };
        // A place outside the repository, and no place, are no annotation.
        let outside = [
            ok("/etc/passwd", 1),
            ok("../secrets", 1),
            ok("a/../../b", 1),
            ok("a\\b", 1),
            ok("", 1),
            ok("src/lib.rs", 0),
            ok("src/lib.rs", -4),
            "::dagron-ci::annotation::not json".to_string(),
            "::dagron-ci::annotation::[1,2]".to_string(),
        ];
        assert!(read(outside.iter().map(String::as_str))
            .annotations
            .is_empty());
        // One that is in it is kept, with a level GitHub has and a message
        // GitHub takes.
        let kept = read([ok("src/lib.rs", 3).as_str()]);
        assert_eq!(
            (
                kept.annotations[0].level,
                kept.annotations[0].message.as_str(),
                kept.annotations[0].end_line
            ),
            ("notice", "(no message)", 3)
        );
        // No more than a check run takes, and the rest is counted.
        let many: Vec<String> = (1..=ANNOTATIONS_MAX as i64 + 9)
            .map(|n| ok("src/lib.rs", n))
            .collect();
        let said = read(many.iter().map(String::as_str));
        assert_eq!(said.annotations.len(), ANNOTATIONS_MAX);
        assert_eq!(said.counts.as_ref().map(|c| c.more_annotations), Some(9));
        assert!(
            said.text().contains("9 more failures named a place"),
            "{}",
            said.text()
        );
        // A long word is cut, and a summary is kept to its size.
        let long = format!(
            r#"::dagron-ci::failed::{{"name":"{}","message":"{}"}}"#,
            "n".repeat(5000),
            "m".repeat(5000)
        );
        let said = read([long.as_str()]);
        assert!(
            said.failed[0].0.chars().count() <= TITLE_MAX
                && said.failed[0].1.chars().count() <= MESSAGE_MAX
        );
        let chunk = format!("::dagron-ci::summary::{}", Value::String("é".repeat(3000)));
        let said = read(std::iter::repeat(chunk.as_str()).take(20));
        assert!(said.summary.len() <= SUMMARY_MAX && said.summary.len() > SUMMARY_MAX - 8);
        // Markdown a test's name might hold does not leave its code span.
        let sly = r#"::dagron-ci::failed::{"name":"a`b","message":"x`y\n# heading"}"#;
        assert_eq!(read([sly]).text(), "- `a b`: `x y`");
    }
}
