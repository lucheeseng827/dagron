//! A job that flakes (CI_FLAKE_ISSUES): one that passed only
//! after an attempt of it failed, which a person reruns, or the engine
//! retries, and so learns nothing of.
//!
//! **What a flake is.** The look that sees a run end asks the engine, for
//! each job that passed on a later attempt, how each attempt before it in
//! the same life ended (`GET /api/runs/{id}/tasks/{tid}/attempts`): one that
//! ended `failed` makes the job a flake. A `repeat:` task's earlier passes
//! end otherwise, and are not one; and a cell that keeps no attempts
//! (`DAGRON_ATTEMPT_LOG_BYTES=0`) says of none.
//!
//! **Whose.** A trusted run's alone, and no patch bundle's: what a stranger
//! wrote files nothing on the repository. Each is kept once a run, with the
//! run's record, for CI_FLAKE_DAYS.
//!
//! **The issue.** A job that flakes CI_FLAKE_ISSUES times within those days
//! is filed one, of its pipeline: written again in place as it flakes
//! again, never a second beside it. A person closes it once it is dealt
//! with, and its flakes are counted again from then; one that was deleted
//! is not opened again until they reach the count anew.
//!
//! **Whose words are in it.** A job's name and its pipeline's are the
//! repository's, in a trusted run; each goes in as code, cut to a length,
//! as a pull request's comment has them ([`super::comment`]). The one link
//! is each run's page (CI_RUN_URL), which is the operator's.
//!
//! **With a token of its own**, for `issues: write` alone, or the GitLab
//! host's ([`crate::forge::Forge::write_issue`]).

use std::time::Duration;

use serde::Deserialize;
use sha2::{Digest, Sha256};

use super::comment::{cell, code};
use super::{pipeline_name, Job, Reporter, Seen, BACKOFF, EVERY};
use crate::db::{CommentWrite, DueFlakeIssue, DueReport, FlakeRow, Flakes};
use crate::forge::{Issued, StatusError};

/// How a job's flakes are counted: `file_after` of them within `days`
/// file an issue of it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Counted {
    pub file_after: i64,
    pub days: i64,
}

/// What every issue of dagron-ci's begins with, unseen.
pub(crate) const MARK: &str =
    "<!-- dagron-ci: a job that passes only on a later attempt. It is edited in place. -->";

/// The most jobs of one run whose attempts are read at its end.
const READS_MAX: usize = 32;

/// How long the forge is given to take one issue, in all, and how long a
/// writer holds the issue it writes: longer, so that no second writer
/// opens a second issue of the job while the first may still be opening it.
const WRITE_MAX: Duration = Duration::from_secs(150);
const ISSUE_LEASE: Duration = Duration::from_secs(180);
const _: () = assert!(ISSUE_LEASE.as_secs() > WRITE_MAX.as_secs());

/// The flakes an issue lists, newest first.
const LISTED: i64 = 30;
/// The longest a pipeline's name and a job's are written.
const PIPELINE_MAX: usize = 60;
const JOB_MAX: usize = 80;

/// A job's attempts as the cell keeps them: how each one before the last
/// ended, and the number of the one it is on.
#[derive(Deserialize)]
struct Attempts {
    current_attempt: i64,
    #[serde(default)]
    attempts: Vec<Attempt>,
}

#[derive(Deserialize)]
struct Attempt {
    attempt: i64,
    #[serde(default)]
    reason: Option<String>,
}

/// How many attempts of `job` failed before the one it passed on, in its
/// life since it was last rerun: `attempts` numbers them across its lives.
fn failed_before(job: &Job, attempts: &Attempts) -> i64 {
    let life = attempts.current_attempt - job.attempt;
    attempts
        .attempts
        .iter()
        .filter(|a| a.attempt > life && a.attempt < attempts.current_attempt)
        .filter(|a| a.reason.as_deref() == Some("failed"))
        .count() as i64
}

/// The title of a job's issue.
pub(crate) fn title(pipeline: &str, job: &str) -> String {
    let plain = |text: &str, max: usize| -> String {
        let text: String = text
            .chars()
            .map(|c| if c.is_control() { ' ' } else { c })
            .collect();
        let words = text.split_whitespace().collect::<Vec<_>>().join(" ");
        match words.chars().count() > max {
            true => format!("{}…", words.chars().take(max - 1).collect::<String>()),
            false => words,
        }
    };
    format!(
        "Flaky job: {} of {}",
        plain(job, JOB_MAX),
        plain(pipeline_name(pipeline), PIPELINE_MAX)
    )
}

/// What a job's issue says of its flakes: `rows`, the newest of `n`.
pub(crate) fn body(
    due: &DueFlakeIssue,
    rows: &[FlakeRow],
    n: i64,
    counted: Counted,
    run_url: Option<&str>,
) -> String {
    let mut out = format!(
        "{MARK}\n**{}** of {} passed only after an attempt of it failed, {} in the last {} days.\n\n",
        code(&due.job, JOB_MAX),
        code(pipeline_name(&due.pipeline), PIPELINE_MAX),
        match n {
            1 => "once".to_string(),
            n => format!("{n} times"),
        },
        counted.days
    );
    out.push_str("| When | Commit | Failed attempts | Run |\n|---|---|---|---|\n");
    for row in rows {
        let when: String = row
            .seen_at
            .chars()
            .take(16)
            .collect::<String>()
            .replace('T', " ");
        let sha: String = row.sha.chars().take(12).collect();
        let run = match page(&row.run_id, &row.cell, run_url) {
            Some(url) => format!("[{}]({url})", cell(&row.run_id, 40).trim_matches('`')),
            None => cell(&row.run_id, 40),
        };
        out.push_str(&format!(
            "| {} | {} | {} | {run} |\n",
            cell(&when, 20),
            cell(&sha, 12),
            row.failed
        ));
    }
    if n > rows.len() as i64 {
        out.push_str(&format!("\n…and {} before these.\n", n - rows.len() as i64));
    }
    out.push_str(
        "\nA job that fails and then passes on the same commit was not proved by either. dagron-ci \
         files one issue of a job, and writes it again as the job flakes again. Close it once the \
         flake is dealt with: its flakes are counted again from then.\n",
    );
    out
}

/// A run's page, from CI_RUN_URL's pattern, where what it comes to can
/// stand in a link.
fn page(run_id: &str, cell: &str, run_url: Option<&str>) -> Option<String> {
    let url = run_url?.replace("{run_id}", run_id).replace("{cell}", cell);
    let plain = url.chars().all(|c| {
        c.is_ascii_graphic() && !matches!(c, '(' | ')' | '<' | '>' | '"' | '`' | '\\' | '[' | ']')
    });
    (plain && (url.starts_with("https://") || url.starts_with("http://"))).then_some(url)
}

/// When the forge says an issue was closed, as the database takes it: an
/// ISO 8601 time, or nothing.
fn closed_at(at: Option<String>) -> Option<String> {
    at.filter(|at| {
        at.len() <= 40
            && at.starts_with(|c: char| c.is_ascii_digit())
            && at
                .chars()
                .all(|c| c.is_ascii_digit() || matches!(c, '-' | ':' | 'T' | 'Z' | '.' | '+'))
    })
}

impl Reporter {
    /// The jobs of `due`'s run that flaked, where this look saw it end, its
    /// flakes are kept, and its repository may be filed an issue: each with
    /// how many of its attempts failed before it passed. `None` otherwise.
    pub(super) async fn flakes_of(
        &self,
        base: &str,
        due: &DueReport,
        seen: &Seen,
    ) -> Option<Flakes> {
        let counted = self.flakes?;
        let kept = seen.finished.is_some()
            && due.trust == "trusted"
            && due.bundle_id.is_none()
            && self.forge.issues(&due.repo);
        if !kept {
            return None;
        }
        let mut jobs = Vec::new();
        let later = seen
            .jobs
            .iter()
            .filter(|job| job.status == "succeeded" && job.attempt > 1 && !job.id.is_empty());
        for job in later.take(READS_MAX) {
            let url = format!("{base}/api/runs/{}/tasks/{}/attempts", due.run_id, job.id);
            match self.get::<Attempts>(&url).await {
                Ok(Some(attempts)) => {
                    let failed = failed_before(job, &attempts);
                    if failed > 0 {
                        jobs.push((job.name.clone(), failed));
                    }
                }
                Ok(None) => {}
                // Not known: the job is not taken for a flake.
                Err(why) => {
                    tracing::debug!(run = %due.run_id, job = %job.name, %why, "a job's attempts")
                }
            }
        }
        Some(Flakes {
            jobs,
            file_after: counted.file_after,
            days: counted.days,
        })
    }

    /// Write the most overdue issue of a job that flakes, if one is due, as
    /// `claim`. `false` when none was.
    pub(crate) async fn flake_issue_one(&self, claim: &str) -> sqlx::Result<bool> {
        let Some(due) = self.db.claim_flake_issue(claim, ISSUE_LEASE).await? else {
            return Ok(false);
        };
        let write = self.write_flake_issue(&due).await?;
        if let Some(error) = &write.error {
            tracing::warn!(
                repo = %due.repo.full_name, pipeline = %due.pipeline, job = %due.job,
                error = %error, again = write.next.is_some(), "a flaky job's issue"
            );
        }
        if !self.db.finish_flake_issue(&due, claim, &write).await? {
            tracing::warn!(
                repo = %due.repo.full_name, job = %due.job,
                "a flaky job's issue: the claim lapsed mid-write; another writer has it"
            );
        }
        Ok(true)
    }

    /// Write `due`'s issue where it would say something else than it does,
    /// opening it where there is none and the job has flaked enough.
    async fn write_flake_issue(&self, due: &DueFlakeIssue) -> sqlx::Result<CommentWrite> {
        let mut write = CommentWrite::default();
        let Some(counted) = self.flakes else {
            return Ok(write);
        };
        if !self.forge.issues(&due.repo) {
            return Ok(write);
        }
        let mut issue = due.issue.clone();
        let until = tokio::time::Instant::now() + WRITE_MAX;
        // Twice at most: an issue found closed, then a new one.
        for _ in 0..2 {
            let (rows, n) = self.db.flakes_of(due, LISTED).await?;
            // Too few to open one; an open one is kept up to date whatever.
            if issue.is_none() && n < counted.file_after {
                return Ok(write);
            }
            let text = body(due, &rows, n, counted, self.run_url.as_deref());
            let digest = hex::encode(Sha256::digest(text.as_bytes()));
            if issue.is_some() && due.said.as_deref() == Some(digest.as_str()) {
                return Ok(write);
            }
            let named = title(&due.pipeline, &due.job);
            let writing = self
                .forge
                .write_issue(&due.repo, &named, &text, issue.as_deref());
            let Ok(written) = tokio::time::timeout_at(until, writing).await else {
                write.error = Some(format!(
                    "writing the issue of {}: the forge did not take it within {} s",
                    due.job,
                    WRITE_MAX.as_secs()
                ));
                write.next = Some(BACKOFF);
                return Ok(write);
            };
            match written {
                Ok(Issued::Written(number)) => {
                    if issue.as_deref() != Some(number.as_str()) {
                        self.db.flake_issue_is(due, Some(&number), None).await?;
                    }
                    write.said = Some(digest);
                }
                // A person dealt with it: its flakes count from then, and a
                // new one is opened only where those are enough.
                Ok(Issued::Closed(at)) => {
                    self.db
                        .flake_issue_is(due, None, closed_at(at).as_deref())
                        .await?;
                    issue = None;
                    continue;
                }
                Ok(Issued::Gone) => {
                    self.db.flake_issue_is(due, None, None).await?;
                    issue = None;
                    continue;
                }
                Err(StatusError::RateLimited { wait, why }) => {
                    write.error = Some(why);
                    write.next = Some(wait.max(EVERY));
                }
                Err(StatusError::Failed(why)) => {
                    write.error = Some(why);
                    write.next = Some(BACKOFF);
                }
                // Asked again, it would be refused again: it waits for the
                // job's next flake.
                Err(StatusError::Refused(why) | StatusError::ThisStatus(why)) => {
                    write.error = Some(why);
                }
            }
            return Ok(write);
        }
        Ok(write)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::Repo;

    fn job(attempt: i64) -> Job {
        Job {
            id: "t-1".into(),
            name: "test".into(),
            status: "succeeded".into(),
            attempt,
        }
    }

    fn attempts(current: i64, ended: &[(i64, &str)]) -> Attempts {
        Attempts {
            current_attempt: current,
            attempts: ended
                .iter()
                .map(|(attempt, reason)| Attempt {
                    attempt: *attempt,
                    reason: Some(reason.to_string()),
                })
                .collect(),
        }
    }

    /// A retry that failed makes a flake; a `repeat:` pass, or an attempt
    /// of a life before the job was rerun, does not.
    #[test]
    fn a_flake_is_a_failed_attempt_before_the_pass_in_the_same_life() {
        assert_eq!(
            failed_before(&job(3), &attempts(3, &[(1, "failed"), (2, "failed")])),
            2
        );
        assert_eq!(
            failed_before(&job(3), &attempts(3, &[(1, "iteration"), (2, "iteration")])),
            0
        );
        // Rerun after two attempts: this life began at 3, and is on its 2nd.
        assert_eq!(
            failed_before(
                &job(2),
                &attempts(4, &[(1, "failed"), (2, "rerun"), (3, "failed")])
            ),
            1
        );
        // Kept no attempts: none is known to have failed.
        assert_eq!(failed_before(&job(2), &attempts(2, &[])), 0);
    }

    fn due() -> DueFlakeIssue {
        DueFlakeIssue {
            repo: Repo {
                id: "r".into(),
                forge: "github".into(),
                forge_host: "github.com".into(),
                full_name: "acme/widgets".into(),
                workflow: None,
                cell: "default".into(),
                mode: crate::db::Mode::Native,
            },
            pipeline: ".dagron/ci/ci.yaml".into(),
            job: "test`@here\u{202e}".into(),
            issue: None,
            said: None,
        }
    }

    fn row(n: u8) -> FlakeRow {
        FlakeRow {
            run_id: format!("run-{n}"),
            cell: "default".into(),
            sha: "4b4708e725f29891d22b4c96af9f0a403ff51c7c".into(),
            failed: 1,
            seen_at: format!("2026-10-2{n}T12:30:00.000Z"),
        }
    }

    /// The issue names the job and its pipeline as code, lists each flake
    /// with a link to its run, and says how many it left out.
    #[test]
    fn an_issue_lists_the_flakes_and_its_names_are_inert() {
        let counted = Counted {
            file_after: 3,
            days: 14,
        };
        let text = body(
            &due(),
            &[row(1), row(0)],
            5,
            counted,
            Some("https://ci.example/runs/{run_id}"),
        );
        assert!(text.starts_with(MARK), "{text}");
        assert!(
            text.contains("**`test @here`** of `ci` passed only after an attempt of it failed, 5 times in the last 14 days."),
            "{text}"
        );
        assert!(text.contains("| `2026-10-21 12:30` | `4b4708e725f2` | 1 | [run-1](https://ci.example/runs/run-1) |"), "{text}");
        assert!(text.contains("…and 3 before these."), "{text}");
        // A pattern that does not make a plain link names the run alone.
        let text = body(
            &due(),
            &[row(1)],
            1,
            counted,
            Some("javascript:alert(1)//{run_id}"),
        );
        assert!(
            text.contains("| `run-1` |") && text.contains("once in the last 14 days"),
            "{text}"
        );
        assert_eq!(
            title(".dagron/ci/ci.yaml", "test\n#1"),
            "Flaky job: test #1 of ci"
        );
        assert_eq!(
            closed_at(Some("2026-10-20T12:00:00Z".into())).as_deref(),
            Some("2026-10-20T12:00:00Z")
        );
        assert_eq!(closed_at(Some("'; DROP".into())), None);
    }
}
