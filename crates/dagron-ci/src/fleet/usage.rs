//! Showback: what the fleet's runners did, over whole days (UTC), by
//! repository, by team or by label. It is for showing who used the
//! capacity, and never to charge for it: compute an operator owns is not metered,
//! and nothing here is sent anywhere.
//!
//! - **Jobs, and their time**: the jobs that started in the days on a runner
//!   of the fleet's, from their start to their end, or to now for one that
//!   runs. A job is the repository's whose workflow it is, whichever
//!   repository's look started the runner (an organization's runner may be
//!   given another's).
//! - **Runners, and their time**: the runners whose run was submitted in the
//!   days, from the submit to their end, or to now: what the capacity held,
//!   time kept warm and time waiting for a job included. A runner is the
//!   repository's it was started for.
//! - **A team** is a repository's owner on its forge: an organization, a
//!   user, a top-level group.
//!
//! What is kept is what is counted: a job's and a runner's rows leave the
//! database with the rest of its records (CI_RECORD_DAYS).

use std::collections::BTreeMap;

use serde_json::{json, Value};

use crate::db::{Day, Db};

/// The longest a report runs: a year and a day.
pub const MAX_DAYS: i64 = 366;

/// How many days a report runs where it is not told: the thirty to today.
pub const DEFAULT_DAYS: i64 = 30;

/// What a report is by.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum By {
    Repository,
    Team,
    Label,
}

impl By {
    pub fn parse(text: &str) -> Option<By> {
        match text {
            "repo" | "repository" => Some(By::Repository),
            "team" => Some(By::Team),
            "label" => Some(By::Label),
            _ => None,
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            By::Repository => "repo",
            By::Team => "team",
            By::Label => "label",
        }
    }
}

/// One line of a report.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Row {
    pub key: String,
    pub jobs: i64,
    pub job_secs: i64,
    pub runners: i64,
    pub runner_secs: i64,
}

impl Row {
    fn add(&mut self, other: &Row) {
        self.jobs += other.jobs;
        self.job_secs += other.job_secs;
        self.runners += other.runners;
        self.runner_secs += other.runner_secs;
    }

    fn json(&self) -> Value {
        json!({
            "key": self.key,
            "jobs": self.jobs,
            "job_seconds": self.job_secs,
            "runners": self.runners,
            "runner_seconds": self.runner_secs,
        })
    }
}

/// A report: from the start of `since` to the start of `until`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Usage {
    pub since: Day,
    pub until: Day,
    pub by: By,
    /// By key, in order.
    pub rows: Vec<Row>,
    pub total: Row,
}

/// A day, `YYYY-MM-DD`, as a date of the calendar: `None` for any other
/// text, and for a day no month has.
pub fn day(text: &str) -> Option<Day> {
    let mut parts = text.splitn(3, '-');
    let (y, m, d) = (parts.next()?, parts.next()?, parts.next()?);
    if y.len() != 4 || m.len() != 2 || d.len() != 2 {
        return None;
    }
    let (y, m, d): (i64, i64, i64) = (y.parse().ok()?, m.parse().ok()?, d.parse().ok()?);
    // Howard Hinnant's `days_from_civil`, the other way to `cron::civil`.
    let y = if m <= 2 { y - 1 } else { y };
    let era = y.div_euclid(400);
    let yoe = y.rem_euclid(400);
    let mp = (m + 9) % 12;
    let doy = (153 * mp + 2) / 5 + d - 1;
    let doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
    let found = Day(era * 146_097 + doe - 719_468);
    // 2026-02-30 comes out as a day of March: it is no day.
    (found.date() == text).then_some(found)
}

/// The days a report runs, from what it was asked: `since` to the start of
/// `until`, each a `YYYY-MM-DD`. Unsaid, the [`DEFAULT_DAYS`] to the end of
/// `today`. `Err` says what is wrong with what was asked.
pub fn window(since: Option<&str>, until: Option<&str>, today: Day) -> Result<(Day, Day), String> {
    let read = |what: &str, text: Option<&str>| match text {
        None => Ok(None),
        Some(text) => day(text)
            .map(Some)
            .ok_or_else(|| format!("{what}={text:?} is a day, YYYY-MM-DD")),
    };
    let until = read("until", until)?.unwrap_or(today.next());
    let since = read("since", since)?.unwrap_or(Day(until.0 - DEFAULT_DAYS));
    if since.0 >= until.0 {
        return Err(format!(
            "since={} is not before until={}: a report runs from the start of one day to the \
             start of a later one",
            since.date(),
            until.date()
        ));
    }
    if until.0 - since.0 > MAX_DAYS {
        return Err(format!(
            "a report runs {MAX_DAYS} days at most, and {} to {} is {}",
            since.date(),
            until.date(),
            until.0 - since.0
        ));
    }
    Ok((since, until))
}

/// The report for the days `since` to `until`, by `by`.
pub async fn usage(db: &Db, since: Day, until: Day, by: By) -> sqlx::Result<Usage> {
    let used = db.fleet_used(&since.start(), &until.start()).await?;
    let mut rows: BTreeMap<String, Row> = BTreeMap::new();
    let mut total = Row {
        key: "total".into(),
        ..Row::default()
    };
    for u in &used {
        let key = match by {
            By::Repository => format!("{}/{}", u.forge_host, u.full_name),
            By::Team => {
                let owner = u.full_name.split('/').next().unwrap_or_default();
                format!("{}/{}", u.forge_host, owner)
            }
            By::Label => u.class.clone(),
        };
        let row = Row {
            key: key.clone(),
            jobs: u.jobs,
            job_secs: u.job_secs,
            runners: u.runners,
            runner_secs: u.runner_secs,
        };
        rows.entry(key.clone())
            .or_insert_with(|| Row {
                key,
                ..Row::default()
            })
            .add(&row);
        total.add(&row);
    }
    Ok(Usage {
        since,
        until,
        by,
        rows: rows.into_values().collect(),
        total,
    })
}

impl Usage {
    /// As `GET /ci/v1/fleet/usage` answers it.
    pub fn json(&self) -> Value {
        json!({
            "since": self.since.date(),
            "until": self.until.date(),
            "by": self.by.as_str(),
            "rows": self.rows.iter().map(Row::json).collect::<Vec<_>>(),
            "total": self.total.json(),
        })
    }

    /// As `dagron-ci fleet usage` prints it: a line a key, with the time in
    /// hours, and the total.
    pub fn table(&self) -> String {
        let hours = |secs: i64| format!("{:.2}", secs as f64 / 3600.0);
        let mut out = format!(
            "# {} to {} (UTC), by {}\n{}\tjobs\tjob_hours\trunners\trunner_hours\n",
            self.since.date(),
            self.until.date(),
            self.by.as_str(),
            self.by.as_str()
        );
        for row in self.rows.iter().chain(std::iter::once(&self.total)) {
            out.push_str(&format!(
                "{}\t{}\t{}\t{}\t{}\n",
                row.key,
                row.jobs,
                hours(row.job_secs),
                row.runners,
                hours(row.runner_secs)
            ));
        }
        out
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::tests::{drop_schema, exec, postgres, sqlite};
    use crate::fleet::store::{Quota, TRUSTED};
    use crate::github::{Job, JobAction};

    #[test]
    fn a_day_is_a_date_of_the_calendar() {
        for text in ["1970-01-01", "2000-02-29", "2026-10-19", "2026-12-31"] {
            assert_eq!(day(text).map(|d| d.date()).as_deref(), Some(text));
        }
        assert_eq!(day("1970-01-02"), Some(Day(1)));
        for not in [
            "2026-02-30",
            "2026-13-01",
            "2026-1-1",
            "26-01-01",
            "today",
            "",
        ] {
            assert_eq!(day(not), None, "{not}");
        }
    }

    #[test]
    fn a_report_runs_whole_days_and_not_too_many() {
        let today = day("2026-10-19").unwrap();
        let (since, until) = window(None, None, today).unwrap();
        assert_eq!(
            (since.date().as_str(), until.date().as_str()),
            ("2026-09-20", "2026-10-20"),
            "the thirty days to today's end"
        );
        let (since, until) = window(Some("2026-10-01"), Some("2026-10-02"), today).unwrap();
        assert_eq!((since.0 + 1, until.0), (until.0, until.0));
        for (since, until, says) in [
            (Some("2026-10-02"), Some("2026-10-02"), "not before"),
            (Some("2024-01-01"), Some("2026-01-01"), "366 days at most"),
            (Some("yesterday"), None, "is a day"),
        ] {
            let why = window(since, until, today).unwrap_err();
            assert!(why.contains(says), "{why}");
        }
    }

    /// Jobs a fleet runner ran, and runners started, by repository, team
    /// and label, on either back end.
    async fn what_the_fleet_did(db: Db) {
        let add = |name: &'static str| {
            let db = db.clone();
            async move {
                db.add_repo("github", "github.com", name, None, "default")
                    .await
                    .unwrap()
                    .unwrap()
            }
        };
        let (widgets, gadgets) = (add("acme/widgets").await, add("acme/gadgets").await);
        let other = add("zeta/tools").await;
        let job = |id: u64, runner: Option<&str>, action: JobAction| Job {
            forge_host: "github.com".into(),
            full_name: "x/y".into(),
            action,
            id,
            run_id: 7,
            run_attempt: 1,
            name: "build".into(),
            workflow: None,
            labels: vec!["dagron".into()],
            head_sha: "4b4708e725f29891d22b4c96af9f0a403ff51c7c".into(),
            runner_id: None,
            runner_name: runner.map(str::to_string),
            conclusion: None,
        };
        for (repo, name, class) in [
            (&widgets, "r-1", "dagron"),
            (&widgets, "r-2", "dagron"),
            (&gadgets, "r-3", "big"),
            (&other, "r-4", "dagron"),
        ] {
            db.fleet_want(repo, class, name, None, TRUSTED, Quota::default())
                .await
                .unwrap();
            db.fleet_launched(name, "run").await.unwrap();
        }
        // r-1 ran a job of widgets' for ten minutes, r-3 one of gadgets', and
        // r-2 a job of gadgets' (an organization's runner) that runs still.
        // A job another runner took is not the fleet's.
        for (repo, id, runner, class) in [
            (&widgets, 1, Some("r-1"), "dagron"),
            (&gadgets, 2, Some("r-3"), "big"),
            (&gadgets, 3, Some("r-2"), "dagron"),
            (&widgets, 4, Some("theirs"), "dagron"),
        ] {
            db.fleet_job(repo, &job(id, runner, JobAction::InProgress), class)
                .await
                .unwrap();
        }
        for (id, runner) in [(1, "r-1"), (2, "r-3"), (4, "theirs")] {
            db.fleet_job(
                &widgets,
                &job(id, Some(runner), JobAction::Completed),
                "dagron",
            )
            .await
            .unwrap();
        }
        let now = Day::today();
        let at = |day: Day, hm: &str| format!("{}T{hm}:00.000Z", day.date());
        let yesterday = Day(now.0 - 1);
        for sql in [
            format!(
                "UPDATE ci_fleet_jobs SET started_at = '{}', finished_at = '{}' WHERE job_id IN ('1', '2', '4')",
                at(yesterday, "10:00"),
                at(yesterday, "10:10")
            ),
            format!(
                "UPDATE ci_fleet_jobs SET started_at = '{}' WHERE job_id = '3'",
                at(yesterday, "10:00")
            ),
            format!(
                "UPDATE ci_fleet_runners SET launched_at = '{}', ended_at = '{}'",
                at(yesterday, "09:59"),
                at(yesterday, "10:20")
            ),
            // r-4 was started a year ago: outside the report.
            format!(
                "UPDATE ci_fleet_runners SET launched_at = '{}', ended_at = '{}' WHERE name = 'r-4'",
                at(Day(now.0 - 365), "09:00"),
                at(Day(now.0 - 365), "10:00")
            ),
        ] {
            exec(&db, &sql).await;
        }

        let (since, until) = window(None, None, now).unwrap();
        let report = usage(&db, since, until, By::Repository).await.unwrap();
        let rows: Vec<(&str, i64, i64)> = report
            .rows
            .iter()
            .map(|r| (r.key.as_str(), r.jobs, r.runners))
            .collect();
        assert_eq!(
            rows,
            [
                ("github.com/acme/gadgets", 2, 1),
                ("github.com/acme/widgets", 1, 2)
            ]
        );
        let widgets_row = &report.rows[1];
        assert_eq!(
            (widgets_row.job_secs, widgets_row.runner_secs),
            (600, 2 * 21 * 60)
        );
        assert!(
            report.rows[0].job_secs >= 600 + 10 * 60,
            "a job that runs still is counted to now: {:?}",
            report.rows[0]
        );
        assert_eq!((report.total.jobs, report.total.runners), (3, 3));

        let by_team = usage(&db, since, until, By::Team).await.unwrap();
        assert_eq!(
            by_team
                .rows
                .iter()
                .map(|r| (r.key.as_str(), r.jobs))
                .collect::<Vec<_>>(),
            [("github.com/acme", 3)]
        );
        let by_label = usage(&db, since, until, By::Label).await.unwrap();
        assert_eq!(
            by_label
                .rows
                .iter()
                .map(|r| (r.key.as_str(), r.jobs, r.runners))
                .collect::<Vec<_>>(),
            [("big", 1, 1), ("dagron", 2, 2)]
        );
        let json = report.json();
        assert_eq!(json["by"], "repo");
        assert_eq!(json["total"]["job_seconds"], report.total.job_secs);
        assert!(report
            .table()
            .contains("github.com/acme/widgets\t1\t0.17\t2\t0.70"));

        // A day before them reads nothing.
        let empty = usage(&db, Day(now.0 - 3), Day(now.0 - 2), By::Repository)
            .await
            .unwrap();
        assert!(empty.rows.is_empty());
        assert_eq!(empty.total.jobs, 0);

        // The gauges: nothing waits, and r-2's job keeps it busy.
        let gauges = db.fleet_gauges().await.unwrap();
        assert!(gauges.waiting.is_empty());
        assert_eq!(
            gauges
                .runners
                .get(&("dagron".to_string(), "busy".to_string())),
            Some(&1)
        );

        // What is over before a horizon leaves; what runs is kept.
        db.fleet_ended("r-1", "its run succeeded").await.unwrap();
        db.fleet_swept("r-1").await.unwrap();
        let (jobs, runners) = db.drop_fleet_before(&Day(now.0 - 1).start()).await.unwrap();
        assert_eq!((jobs, runners), (0, 0), "nothing ended before yesterday");
        exec(
            &db,
            "UPDATE ci_fleet_runners SET swept_at = '2000-01-01T00:00:00Z' WHERE name = 'r-1'",
        )
        .await;
        let (jobs, runners) = db.drop_fleet_before(&now.start()).await.unwrap();
        assert_eq!((jobs, runners), (3, 1), "jobs 1, 2 and 4, and r-1");
        let left = usage(&db, since, until, By::Repository).await.unwrap();
        assert_eq!(left.total.jobs, 1, "the job that runs still");
    }

    #[tokio::test]
    async fn sqlite_says_what_the_fleet_did() {
        let (db, _dir) = sqlite().await;
        what_the_fleet_did(db).await;
    }

    #[tokio::test]
    async fn postgres_says_what_the_fleet_did() {
        let Some((db, schema, admin)) = postgres().await else {
            return;
        };
        what_the_fleet_did(db.clone()).await;
        db.close().await;
        drop_schema(&admin, &schema).await;
    }
}
