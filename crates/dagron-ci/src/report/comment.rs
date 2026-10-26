//! A pull request's summary comment (CI_GITHUB_PR_COMMENT=on): the one
//! comment dagron-ci keeps on a pull request, which says how the runs of
//! its latest commit went.
//!
//! **One comment, written again in place.** A pull request has one, made
//! when the first of its runs is looked at and brought up to date as its
//! runs change. A new commit's runs take the place of the last commit's in
//! it: the conversation is not given a comment a push. One that was deleted
//! is made again the next time there is something to say.
//!
//! **What it is written from.** A look at a run of a pull request keeps
//! what the run's jobs came to (`ci_runs.jobs`), and where that changed it
//! makes the pull request's comment due, in the write that ends the look. A
//! writer claims a due comment as a reporter claims a run, reads the runs
//! of the pull request's latest commit, one a pipeline, and writes the
//! comment where it would say something else than it does.
//!
//! **Whose words are in it.** How a job ended is the engine's word. Its
//! name, its pipeline's and a test's are a repository's to choose, and a
//! pull request's author's where the pull request changes them; a test's
//! name is the job's own word besides ([`crate::said`]). Each goes in as
//! code, cut to a length, with nothing that ends a code span or a table's
//! cell: none of them is read as Markdown, names a person, or links
//! anywhere. The one link is to the run's page (CI_RUN_URL), which is the
//! operator's.
//!
//! **With a token of its own**, for `pull requests: write` alone
//! ([`crate::forge::Forge::post_comment`]). An App that was not given it
//! writes no comment, and its statuses and check runs go on.

use std::collections::BTreeMap;
use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{pipeline_name, Job, Reporter, Seen, BACKOFF, EVERY, SAID_BUDGET};
use crate::db::{CommentWrite, DueComment, DueReport, PrRun, ReportWrite};
use crate::forge::{Commented, StatusError};
use crate::said::Said;

/// What every comment of dagron-ci's begins with, unseen: what tells it
/// from a person's.
pub(crate) const MARK: &str =
    "<!-- dagron-ci: how this pull request's runs went. It is edited in place. -->";

/// How long the forge is given to take one comment, in all: a token for it
/// minted (two requests), the comment written again, and made anew where it
/// was deleted, each for as long as the forge's client waits. A write that
/// is not over by then is given up, and tried again later.
const WRITE_MAX: Duration = Duration::from_secs(150);

/// How long a writer holds the comment it writes. Longer than a write is
/// given, so that no second writer takes a comment its first may still be
/// writing: two at once would make two comments of one.
const COMMENT_LEASE: Duration = Duration::from_secs(180);

const _: () = assert!(COMMENT_LEASE.as_secs() > WRITE_MAX.as_secs());

/// The pipelines a comment lists, and the jobs in all: a pull request with
/// more says how many more.
const RUNS_MAX: usize = 20;
const ROWS_MAX: usize = 150;
/// The tests named of one job that failed, and the jobs they are named of.
const FAILED_NAMES: usize = 5;
const FAILED_JOBS: usize = 20;
/// The longest a pipeline's name, a job's and a test's are written.
const PIPELINE_MAX: usize = 60;
const JOB_MAX: usize = 80;
const TEST_MAX: usize = 120;

/// What a job's tests came to, as the job said.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Tests {
    pub tests: u64,
    /// Those that failed, and those that could not be run.
    pub failed: u64,
    pub skipped: u64,
}

/// What a job said of itself that a comment keeps.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Told {
    pub tests: Option<Tests>,
    /// The first of its tests that failed, by name, and how many more did.
    pub failed: Vec<String>,
    pub more_failed: u64,
}

impl Told {
    pub(crate) fn of(said: &Said) -> Told {
        let failed: Vec<String> = said
            .failed
            .iter()
            .take(FAILED_NAMES)
            .map(|(name, _)| name.chars().take(TEST_MAX * 2).collect())
            .collect();
        let counts = said.counts.as_ref();
        Told {
            tests: counts.filter(|c| c.tests > 0).map(|c| Tests {
                tests: c.tests,
                failed: c.failures.saturating_add(c.errors),
                skipped: c.skipped,
            }),
            more_failed: (said.failed.len() - failed.len()) as u64
                + counts.map_or(0, |c| c.more_failed),
            failed,
        }
    }
}

fn is_zero(n: &u64) -> bool {
    *n == 0
}

/// One job of a run, as its pull request's comment says it.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct JobLine {
    pub name: String,
    /// How it stands, in a word ([`state`]).
    pub state: String,
    /// The attempt this was read at: what a job said is its attempt's.
    #[serde(default)]
    pub attempt: i64,
    /// Whether its log was asked what it said. It is asked once an attempt.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub asked: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tests: Option<Tests>,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub failed: Vec<String>,
    #[serde(default, skip_serializing_if = "is_zero")]
    pub more_failed: u64,
}

/// How a job stands, in the word a comment has for it.
pub(crate) fn state(job: &Job, finished: Option<&str>) -> &'static str {
    match (job.status.as_str(), finished) {
        ("succeeded", _) => "passed",
        ("failed", _) => "failed",
        ("cancelled", _) => "cancelled",
        ("skipped", _) => "skipped",
        (_, Some(_)) => "did not finish",
        ("running", None) => "running",
        ("awaiting_approval", None) => "awaiting approval",
        (_, None) => "waiting",
    }
}

/// What a job's name is ordered by: the name, with an instance's number
/// read as one, so that `test.10` comes after `test.9`.
fn in_order(name: &str) -> (&str, u64, &str) {
    match name.rsplit_once('.') {
        Some((job, instance)) => match instance.parse() {
            Ok(n) => (job, n, ""),
            Err(_) => (name, 0, ""),
        },
        None => (name, 0, ""),
    }
}

/// Whether a job that stands so is over.
fn over(state: &str) -> bool {
    matches!(
        state,
        "passed" | "failed" | "cancelled" | "skipped" | "did not finish"
    )
}

/// `text` as code in a line of Markdown: cut to `max` characters, and with
/// nothing that would end the code span or the line, or turn the text that
/// follows around.
pub(super) fn code(text: &str, max: usize) -> String {
    let inert: String = text
        .chars()
        .map(|c| match c {
            '`' => ' ',
            // Marks that are not seen and change how what follows reads.
            '\u{200b}'..='\u{200f}' | '\u{202a}'..='\u{202e}' | '\u{2066}'..='\u{2069}' => ' ',
            '\u{feff}' => ' ',
            c if c.is_control() => ' ',
            c => c,
        })
        .collect();
    let words: Vec<&str> = inert.split_whitespace().collect();
    let line = words.join(" ");
    if line.is_empty() {
        return "`-`".into();
    }
    if line.chars().count() <= max {
        return format!("`{line}`");
    }
    let cut: String = line.chars().take(max.saturating_sub(1)).collect();
    format!("`{}…`", cut.trim_end())
}

/// [`code`] in a table's cell, where a `|` would end the cell.
pub(super) fn cell(text: &str, max: usize) -> String {
    code(text, max).replace('|', "\\|")
}

/// A count and what it counts: `1 job`, `3 jobs`.
fn count(n: u64, one: &str) -> String {
    format!("{n} {one}{}", if n == 1 { "" } else { "s" })
}

/// What a job's tests came to, in a cell.
fn tests_cell(tests: &Tests) -> String {
    let passed = tests
        .tests
        .saturating_sub(tests.failed)
        .saturating_sub(tests.skipped);
    if tests.failed > 0 {
        format!("{} of {} failed", tests.failed, tests.tests)
    } else if tests.skipped > 0 {
        format!("{passed} passed, {} skipped", tests.skipped)
    } else {
        format!("{passed} passed")
    }
}

/// The word for a run none of whose jobs has been seen, by its status on
/// dagron-ci's record.
fn run_word(status: &str) -> &'static str {
    match status {
        "submitted" => "waiting",
        "succeeded" => "passed",
        "failed" => "failed",
        "cancelled" => "cancelled",
        super::GONE => "no longer kept",
        _ => "over",
    }
}

/// How many jobs stand each way.
#[derive(Default)]
struct Tally {
    all: u64,
    passed: u64,
    failed: u64,
    cancelled: u64,
    skipped: u64,
    going: u64,
}

impl Tally {
    fn add(&mut self, state: &str) {
        self.all += 1;
        match state {
            "passed" => self.passed += 1,
            "failed" | "did not finish" => self.failed += 1,
            "cancelled" => self.cancelled += 1,
            "skipped" => self.skipped += 1,
            _ => self.going += 1,
        }
    }

    /// The comment's heading: the worst of it.
    fn heading(&self) -> String {
        if self.failed > 0 {
            format!("{} failed", count(self.failed, "job"))
        } else if self.going > 0 {
            "running".into()
        } else if self.cancelled > 0 {
            "cancelled".into()
        } else {
            "passed".into()
        }
    }

    fn line(&self) -> String {
        let mut parts = vec![count(self.all, "job")];
        for (n, what) in [
            (self.passed, "passed"),
            (self.failed, "failed"),
            (self.cancelled, "cancelled"),
            (self.skipped, "skipped"),
            (self.going, "not over"),
        ] {
            if n > 0 {
                parts.push(format!("{n} {what}"));
            }
        }
        parts.join(", ")
    }
}

/// The page of `run`, from CI_RUN_URL's pattern, where there is one and
/// what it comes to can stand in a link.
fn page(run: &PrRun, run_url: Option<&str>) -> Option<String> {
    let url = run_url?
        .replace("{run_id}", &run.run_id)
        .replace("{cell}", &run.cell);
    let plain = url
        .chars()
        .all(|c| c.is_ascii_graphic() && !matches!(c, '(' | ')' | '<' | '>' | '"' | '`' | '\\'));
    (plain && (url.starts_with("https://") || url.starts_with("http://"))).then_some(url)
}

/// What the comment says of `runs`: the runs of one commit, one a pipeline.
/// `None` where there are none, and nothing to say.
pub(crate) fn body(runs: &[PrRun], run_url: Option<&str>) -> Option<String> {
    let sha = &runs.first()?.sha;
    let mut tally = Tally::default();
    let mut rows = Vec::new();
    let mut failed = Vec::new();
    let (mut more_rows, mut more_failed) = (0u64, 0u64);
    for run in runs.iter().take(RUNS_MAX) {
        let pipeline = pipeline_name(&run.pipeline);
        let named = cell(pipeline, PIPELINE_MAX);
        // The pipeline is named on the first of its jobs' rows, as a link
        // to its run where there is a page to link to.
        let mut first = Some(match page(run, run_url) {
            Some(url) => format!("[{named}]({url})"),
            None => named,
        });
        let jobs: Vec<JobLine> = run
            .jobs
            .as_deref()
            .and_then(|jobs| serde_json::from_str(jobs).ok())
            .unwrap_or_default();
        if jobs.is_empty() || run.status == super::GONE {
            // Not looked at yet, a run with no job, or one its cell no
            // longer has, whose jobs are not known: the run's own word.
            let word = run_word(&run.status);
            tally.add(word);
            match rows.len() < ROWS_MAX {
                true => rows.push(format!(
                    "| {} | | {word} | |",
                    first.take().unwrap_or_default()
                )),
                false => more_rows += 1,
            }
            continue;
        }
        for job in &jobs {
            tally.add(&job.state);
            if !job.failed.is_empty() {
                match failed.len() < FAILED_JOBS {
                    true => failed.push((pipeline, job.clone())),
                    false => more_failed += 1,
                }
            }
            if rows.len() >= ROWS_MAX {
                more_rows += 1;
                continue;
            }
            let result = match job.state.as_str() {
                bad @ ("failed" | "did not finish") => format!("**{bad}**"),
                // One of the words [`state`] gives, and no other: what is
                // kept is dagron-ci's own, but it is read back from a row.
                state if over(state) || matches!(state, "running" | "awaiting approval") => {
                    state.to_string()
                }
                _ => "waiting".to_string(),
            };
            rows.push(format!(
                "| {} | {} | {result} | {} |",
                first.take().unwrap_or_default(),
                cell(&job.name, JOB_MAX),
                job.tests.as_ref().map(tests_cell).unwrap_or_default(),
            ));
        }
    }
    let more_runs = runs.len().saturating_sub(RUNS_MAX);

    let short: String = sha.chars().take(12).collect();
    let mut out = format!(
        "{MARK}\n### dagron-ci: {}\n\nCommit {}: {}.\n\n",
        tally.heading(),
        code(&short, 12),
        tally.line()
    );
    out.push_str("| Pipeline | Job | Result | Tests |\n|---|---|---|---|\n");
    out.push_str(&rows.join("\n"));
    out.push('\n');
    if more_rows > 0 || more_runs > 0 {
        let mut more = Vec::new();
        if more_rows > 0 {
            more.push(count(more_rows, "job"));
        }
        if more_runs > 0 {
            more.push(count(more_runs as u64, "pipeline"));
        }
        out.push_str(&format!("\nAnd {} more.\n", more.join(" and ")));
    }
    if !failed.is_empty() {
        out.push_str("\n**Tests that failed**\n\n");
        for (pipeline, job) in &failed {
            let names: Vec<String> = job.failed.iter().map(|n| code(n, TEST_MAX)).collect();
            let more = match job.more_failed {
                0 => String::new(),
                n => format!(" and {n} more"),
            };
            out.push_str(&format!(
                "- {} / {}: {}{more}\n",
                code(pipeline, PIPELINE_MAX),
                code(&job.name, JOB_MAX),
                names.join(", ")
            ));
        }
        if more_failed > 0 {
            out.push_str(&format!("- and in {}\n", count(more_failed, "more job")));
        }
    }
    out.push_str("\nA new commit's runs take the place of these.\n");
    Some(out)
}

impl Reporter {
    /// The pull request whose comment says of `due`'s run, by its
    /// repository's id and its number. `None` for a run of no pull request,
    /// and where no comment is kept.
    pub(super) fn comment_of(&self, due: &DueReport) -> Option<(String, i64)> {
        let pr = due.pr?;
        (due.kind == "pull_request" && self.forge.comments(&due.repo))
            .then(|| (due.repo.id.clone(), pr))
    }

    /// Keep what the run's jobs came to, for its pull request's comment,
    /// and make that comment due where this differs from what was kept.
    /// Nothing for a run whose pull request has no comment
    /// ([`Self::comment_of`]).
    ///
    /// A job that is over is asked what it said of itself, once an attempt:
    /// `told` has what this look already read, and the look's time for its
    /// jobs' logs is one, whoever asks (`reading`, `unread`).
    pub(super) async fn note_jobs(
        &self,
        due: &DueReport,
        base: &str,
        seen: &Seen,
        told: &BTreeMap<String, Told>,
        (reading, unread): (Instant, &mut bool),
        write: &mut ReportWrite,
    ) {
        let Some(comment) = self.comment_of(due) else {
            return;
        };
        let before: Vec<JobLine> = due
            .jobs
            .as_deref()
            .and_then(|jobs| serde_json::from_str(jobs).ok())
            .unwrap_or_default();
        let mut lines = Vec::with_capacity(seen.jobs.len());
        for job in &seen.jobs {
            let state = state(job, seen.finished.as_deref());
            let mut line = JobLine {
                name: job.name.clone(),
                state: state.into(),
                attempt: job.attempt,
                ..JobLine::default()
            };
            // A job that never started said nothing.
            if over(state) && job.attempt > 0 {
                let kept = before
                    .iter()
                    .find(|was| was.asked && was.name == job.name && was.attempt == job.attempt);
                let said = match (kept, told.get(&job.name)) {
                    (Some(was), _) => Some(Told {
                        tests: was.tests.clone(),
                        failed: was.failed.clone(),
                        more_failed: was.more_failed,
                    }),
                    (None, Some(told)) => Some(told.clone()),
                    (None, None) if *unread || reading.elapsed() > SAID_BUDGET => None,
                    (None, None) => Some(match self.said(base, &due.run_id, job).await {
                        Some(said) => Told::of(&said),
                        // Not answered: no other is asked at this look, and
                        // this one is not asked again.
                        None => {
                            *unread = true;
                            Told::default()
                        }
                    }),
                };
                if let Some(said) = said {
                    line.asked = true;
                    line.tests = said.tests;
                    line.failed = said.failed;
                    line.more_failed = said.more_failed;
                }
            }
            lines.push(line);
        }
        // By name, and not as the engine lists them: it lists a run's tasks
        // in no order of the pipeline's, and not in the same one from run
        // to run. A comment whose rows moved about would be written again
        // for nothing, and a reader would not find a job where it was.
        lines.sort_by(|a, b| in_order(&a.name).cmp(&in_order(&b.name)));
        let Ok(jobs) = serde_json::to_string(&lines) else {
            return;
        };
        // A run with no job is said by its own status, which this look may
        // have changed.
        let ended = lines.is_empty() && seen.finished.is_some();
        if due.jobs.as_deref() != Some(jobs.as_str()) {
            write.jobs = Some(jobs);
            write.comment = Some(comment);
        } else if ended {
            write.comment = Some(comment);
        }
    }

    /// Write the most overdue pull request's comment, if one is due, as
    /// `claim`. `false` when none was.
    pub(crate) async fn comment_one(&self, claim: &str) -> sqlx::Result<bool> {
        let Some(due) = self.db.claim_comment(claim, COMMENT_LEASE).await? else {
            return Ok(false);
        };
        let runs = self.db.pr_runs(&due.repo.id, due.pr).await?;
        let write = self.write_comment(&due, &runs).await?;
        if let Some(error) = &write.error {
            tracing::warn!(
                repo = %due.repo.full_name, pr = due.pr, error = %error,
                again = write.next.is_some(), "a pull request's comment"
            );
        }
        if !self.db.finish_comment(&due, claim, &write).await? {
            tracing::warn!(
                repo = %due.repo.full_name, pr = due.pr,
                "a pull request's comment: the claim lapsed mid-write; another writer has it"
            );
        }
        Ok(true)
    }

    /// Write `due`'s comment from `runs`, where it would say something else
    /// than it does, and say how the row is to be left.
    async fn write_comment(&self, due: &DueComment, runs: &[PrRun]) -> sqlx::Result<CommentWrite> {
        let mut write = CommentWrite::default();
        // Turned off since it fell due, or nothing has run: nothing to write.
        if !self.forge.comments(&due.repo) {
            return Ok(write);
        }
        let Some(body) = body(runs, self.run_url.as_deref()) else {
            return Ok(write);
        };
        let digest = hex::encode(Sha256::digest(body.as_bytes()));
        if due.comment_id.is_some() && due.said.as_deref() == Some(digest.as_str()) {
            return Ok(write);
        }
        let mut id = due.comment_id.clone();
        // The forge's calls are held to a time shorter than the claim, and
        // what is kept of them after is not: a comment that was made is
        // recorded, however long it took to make.
        let until = tokio::time::Instant::now() + WRITE_MAX;
        loop {
            let posting = self
                .forge
                .post_comment(&due.repo, due.pr, &body, id.as_deref());
            let Ok(posted) = tokio::time::timeout_at(until, posting).await else {
                write.error = Some(format!(
                    "writing the comment on pull request {}: the forge did not take it within \
                     {} s",
                    due.pr,
                    WRITE_MAX.as_secs()
                ));
                write.next = Some(BACKOFF);
                return Ok(write);
            };
            match posted {
                Ok(Commented::Written(made)) => {
                    let made = made.to_string();
                    if id.as_deref() != Some(made.as_str()) {
                        self.db.comment_made(&due.repo.id, due.pr, &made).await?;
                    }
                    write.said = Some(digest);
                }
                // Deleted since: what there is to say is said in a new one.
                Ok(Commented::Gone) if id.is_some() => {
                    id = None;
                    continue;
                }
                Ok(Commented::Gone) => {
                    write.error = Some("GitHub says a comment that was not named is gone".into());
                    write.next = Some(BACKOFF);
                }
                Err(StatusError::RateLimited { wait, why }) => {
                    write.error = Some(why);
                    write.next = Some(wait.max(EVERY));
                }
                Err(StatusError::Failed(why)) => {
                    write.error = Some(why);
                    write.next = Some(BACKOFF);
                }
                // Asking again would be refused again: it is left until a
                // run of the pull request changes.
                Err(StatusError::Refused(why) | StatusError::ThisStatus(why)) => {
                    write.error = Some(why);
                }
            }
            return Ok(write);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(name: &str, state: &str) -> JobLine {
        JobLine {
            name: name.into(),
            state: state.into(),
            attempt: 1,
            ..JobLine::default()
        }
    }

    fn run(pipeline: &str, status: &str, jobs: Option<&[JobLine]>) -> PrRun {
        PrRun {
            run_id: format!("run-{pipeline}"),
            cell: "default".into(),
            pipeline: format!(".dagron/ci/{pipeline}.yaml"),
            status: status.into(),
            jobs: jobs.map(|jobs| serde_json::to_string(jobs).unwrap()),
            sha: "4b4708e725f29891d22b4c96af9f0a403ff51c7c".into(),
        }
    }

    #[test]
    fn a_comment_says_each_pipeline_s_jobs_and_the_worst_of_them() {
        assert_eq!(body(&[], None), None, "nothing ran: nothing to say");

        let mut test = line("test", "failed");
        test.tests = Some(Tests {
            tests: 120,
            failed: 3,
            skipped: 2,
        });
        test.failed = vec!["t::one".into(), "t::two".into()];
        test.more_failed = 1;
        let mut build = line("build", "passed");
        build.tests = Some(Tests {
            tests: 12,
            failed: 0,
            skipped: 2,
        });
        let runs = [
            run(
                "ci",
                "submitted",
                Some(&[build, test, line("deploy", "waiting")]),
            ),
            run("lint", "submitted", None),
        ];
        let said = body(&runs, Some("https://ci.example/runs/{run_id}?cell={cell}")).unwrap();
        assert_eq!(
            said,
            format!(
                "{MARK}\n\
                 ### dagron-ci: 1 job failed\n\
                 \n\
                 Commit `4b4708e725f2`: 4 jobs, 1 passed, 1 failed, 2 not over.\n\
                 \n\
                 | Pipeline | Job | Result | Tests |\n\
                 |---|---|---|---|\n\
                 | [`ci`](https://ci.example/runs/run-ci?cell=default) | `build` | passed | 10 passed, 2 skipped |\n\
                 |  | `test` | **failed** | 3 of 120 failed |\n\
                 |  | `deploy` | waiting |  |\n\
                 | [`lint`](https://ci.example/runs/run-lint?cell=default) | | waiting | |\n\
                 \n\
                 **Tests that failed**\n\
                 \n\
                 - `ci` / `test`: `t::one`, `t::two` and 1 more\n\
                 \n\
                 A new commit's runs take the place of these.\n"
            )
        );

        // The heading is the worst of it; with no page to link to, a
        // pipeline is named and not linked.
        let of = |states: &[&str]| {
            let jobs: Vec<JobLine> = states.iter().map(|s| line("j", s)).collect();
            body(&[run("ci", "succeeded", Some(&jobs))], None).unwrap()
        };
        assert!(of(&["passed", "skipped"]).contains("### dagron-ci: passed\n"));
        assert!(of(&["passed", "running"]).contains("### dagron-ci: running\n"));
        assert!(of(&["passed", "cancelled"]).contains("### dagron-ci: cancelled\n"));
        assert!(of(&["cancelled", "did not finish", "failed"])
            .contains("### dagron-ci: 2 jobs failed\n"));
        assert!(of(&["passed"]).contains("| `ci` | `j` | passed |  |"));
        // A run nobody has looked at yet reads by its own status.
        assert!(body(&[run("ci", "failed", None)], None)
            .unwrap()
            .contains("| `ci` | | failed | |"));
    }

    /// A name is a repository's, or a pull request's author's, to choose:
    /// none of it is Markdown here, whatever it holds.
    #[test]
    fn a_name_is_inert_whatever_it_holds() {
        assert_eq!(code("build", 80), "`build`");
        assert_eq!(code("", 80), "`-`");
        assert_eq!(code(" \n ", 80), "`-`");
        assert_eq!(
            code("a`b\n## c\r\n<!-- d -->\u{202e}e", 80),
            "`a b ## c <!-- d --> e`"
        );
        assert_eq!(code(&"x".repeat(90), 10), format!("`{}…`", "x".repeat(9)));
        assert_eq!(cell("a|b", 80), "`a\\|b`");

        let mut bad = line(
            "x` | [click](https://evil.example) @someone\n| a | b |",
            "passed",
        );
        bad.failed = vec!["t`](https://evil.example) <img src=x>\n\n# big".into()];
        let runs = [PrRun {
            pipeline: ".dagron/ci/p`|\n.yaml".into(),
            ..run("ci", "submitted", Some(&[bad]))
        }];
        let said = body(&runs, None).unwrap();
        // Every line of the table has its four cells and no more, and
        // nothing a name held opened a line of its own.
        let table: Vec<&str> = said.lines().filter(|l| l.starts_with('|')).collect();
        assert_eq!(table.len(), 3, "{said}");
        for row in &table {
            assert_eq!(row.replace("\\|", "").matches('|').count(), 5, "{row}");
        }
        assert!(
            !said.contains("\n# big") && !said.contains("\n| a | b |"),
            "{said}"
        );
        // No backtick but those that open and close a span: an even count on
        // each line, so no span is left open for what follows.
        for row in said.lines() {
            assert_eq!(row.matches('`').count() % 2, 0, "{row}");
        }
        // A run page that could not stand in a link is not linked.
        for bad in [
            "https://ci.example/runs/{run_id}) [x](https://evil.example",
            "javascript:alert(1)//{run_id}",
            "https://ci.example/a b/{run_id}",
        ] {
            let said = body(&[run("ci", "succeeded", None)], Some(bad)).unwrap();
            assert!(said.contains("| `ci` | | passed | |"), "{bad}: {said}");
        }
    }

    #[test]
    fn a_comment_stops_at_so_many_jobs_and_says_how_many_more() {
        let jobs: Vec<JobLine> = (0..ROWS_MAX + 7)
            .map(|n| {
                let mut job = line(&format!("job-{n}"), "failed");
                job.failed = vec!["t".repeat(400)];
                job
            })
            .collect();
        let mut runs = vec![run("a", "failed", Some(&jobs))];
        runs.extend((0..RUNS_MAX + 2).map(|n| run(&format!("p{n}"), "succeeded", None)));
        let said = body(&runs, None).unwrap();
        let rows = said
            .lines()
            .filter(|l| l.starts_with("| ") || l.starts_with("|  "))
            .count();
        assert_eq!(rows, ROWS_MAX + 1, "the heading's row and the jobs'");
        // Every job is counted, listed or not.
        assert!(
            said.contains(&format!("{} jobs, ", ROWS_MAX + 7 + RUNS_MAX - 1)),
            "{said}"
        );
        assert!(
            said.contains("\nAnd 26 jobs and 3 pipelines more.\n"),
            "{said}"
        );
        assert!(said.contains(&format!(
            "- and in {} more jobs\n",
            ROWS_MAX + 7 - FAILED_JOBS
        )));
        assert!(
            said.chars().count() < crate::forge::COMMENT_MAX,
            "{} characters",
            said.chars().count()
        );
    }

    /// A writer holds a comment for longer than the forge is given to take
    /// it, whichever of the forge's calls a write needs: a token minted in
    /// two, the comment written again, and made anew.
    #[test]
    fn a_comment_is_held_for_longer_than_it_can_take_to_write() {
        assert!(WRITE_MAX >= 4 * crate::forge::REQUEST_TIMEOUT);
        assert!(COMMENT_LEASE > WRITE_MAX);
    }

    #[test]
    fn jobs_are_in_order_of_their_names_and_an_instance_s_number() {
        let mut names = vec![
            "test.10", "lint", "test.9", "build", "test.2", "v1.2", "test",
        ];
        names.sort_by(|a, b| in_order(a).cmp(&in_order(b)));
        assert_eq!(
            names,
            ["build", "lint", "test", "test.2", "test.9", "test.10", "v1.2"]
        );
    }

    #[test]
    fn what_a_job_said_is_kept_as_counts_and_a_few_names() {
        let said = crate::said::read([
            r#"::dagron-ci::report::{"tests":9,"failures":2,"errors":1,"skipped":1,"findings":0,"more_annotations":0,"more_failed":4,"problems":[]}"#,
            r#"::dagron-ci::failed::{"name":"a","message":"x"}"#,
            r#"::dagron-ci::failed::{"name":"b","message":"y"}"#,
        ]);
        assert_eq!(
            Told::of(&said),
            Told {
                tests: Some(Tests {
                    tests: 9,
                    failed: 3,
                    skipped: 1
                }),
                failed: vec!["a".into(), "b".into()],
                more_failed: 4,
            }
        );
        // A job with no report said nothing of its tests.
        assert_eq!(Told::of(&Said::default()), Told::default());
        assert_eq!(Told::of(&Said::unread()).tests, None);
    }
}
