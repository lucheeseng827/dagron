//! Fleet mode's rows: each job as GitHub says it stands, each runner the
//! fleet started, and the claim under which a repository's fleet is looked
//! over (`ci_fleet_jobs`, `ci_fleet_runners`, `ci_repos.fleet_*`).
//!
//! **A job's deliveries arrive in any order**, and more than once. A job's
//! row moves forward only: queued, then running, then done. One that is
//! first heard of as running, or as done, is written as that.
//!
//! **A look is claimed as a sweep is.** The claim pushes the repository's
//! next look out by the lease, so a looker that stops leaves it due again.
//! When the next look is, is worked out as the look ends, from the rows as
//! they then are: a job queued while a look was going is not waited on by
//! nobody.

use std::collections::BTreeMap;
use std::time::Duration;

use sqlx::Row;

use crate::db::{lite, Db, Repo};
use crate::github::{Job, JobAction};

/// What a job is, as GitHub says of the workflow run it is of: of the
/// repository's own (`trusted`), or of a fork's pull request.
pub const TRUSTED: &str = "trusted";
pub const UNTRUSTED: &str = "untrusted";

/// A runner the fleet started, as its row has it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Runner {
    /// Its name with the forge, and its row's key.
    pub name: String,
    pub class: String,
    pub cell: String,
    /// The forge's id for its registration, once it is registered.
    pub forge_id: Option<String>,
    /// The organization it is registered for, where it is an organization's
    /// runner; `None` for a repository's.
    pub org: Option<String>,
    /// What its run was submitted as: [`TRUSTED`], or [`UNTRUSTED`] for one
    /// started for a fork's job.
    pub trust: String,
    /// The dagron run that hosts it, once one is submitted.
    pub run_id: Option<String>,
    /// `wanted`, `launched`, `busy` or `ended`.
    pub state: String,
    /// The job it took, once GitHub said it had one.
    pub job_id: Option<String>,
    /// What that job is, where it is known ([`TRUSTED`], [`UNTRUSTED`]).
    pub job_trust: Option<String>,
    /// How long that job has run without being told apart, where it is a
    /// job the fleet keeps and is not told yet: from when it started, or was
    /// queued where no start was heard.
    pub job_untold_secs: Option<i64>,
    /// How long ago its row was written.
    pub age_secs: i64,
    /// Why it ended, once it has.
    pub reason: Option<String>,
    /// How long ago it ended, once it has.
    pub ended_secs: Option<i64>,
}

/// A job that failed on a runner of the fleet's whose run did not succeed:
/// its runner died, and not the job, unless the runner had its label's time.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Died {
    pub job_id: String,
    pub workflow_run: String,
    /// The job's name in its workflow run: what a new attempt of it has too.
    pub name: String,
    pub class: String,
    /// `None` until it is decided; `due` while GitHub is to be asked.
    pub retry: Option<String>,
    /// How long its runner's run went on, from its submit to its end.
    pub lived_secs: Option<i64>,
    /// How long ago the job ended.
    pub ended_secs: i64,
}

/// Why a runner's row says it ended, where its run ended and it did not
/// succeed: the runner's process does not outlive the one job it takes, and
/// ends well whatever the job came to, so a run that failed is a runner that
/// died. (`its run {status}`, as [`crate::fleet::worker`] writes it.)
pub(crate) const DIED: [&str; 2] = ["its run failed", "its cell has no such run"];

/// What a repository's runners of one class did over a time: how many jobs
/// the fleet's runners ran for it and for how long, and how many runners
/// were started for it and how long they were out ([`crate::fleet::usage`]).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Used {
    pub forge_host: String,
    pub full_name: String,
    pub class: String,
    pub jobs: i64,
    pub job_secs: i64,
    pub runners: i64,
    pub runner_secs: i64,
}

/// The fleet as it stands, every repository together, for `/metrics`: the
/// jobs that wait by class, and the runners out by class and state.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Gauges {
    pub waiting: BTreeMap<String, i64>,
    pub runners: BTreeMap<(String, String), i64>,
}

/// How many of a class's jobs wait, by what they are.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Waiting {
    pub trusted: i64,
    pub untrusted: i64,
    /// Not yet told apart: GitHub has not been asked, or has not answered.
    pub unknown: i64,
}

impl Waiting {
    pub fn all(&self) -> i64 {
        self.trusted + self.untrusted + self.unknown
    }
}

/// A repository's fleet, at one look.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct View {
    /// How many jobs wait for a runner, by the class that serves them.
    pub queued: BTreeMap<String, Waiting>,
    /// How long ago the newest job of each class was queued, in seconds:
    /// whether its runners are kept warm.
    pub newest: BTreeMap<String, i64>,
    /// Every runner whose leavings are not yet taken back, oldest first.
    pub runners: Vec<Runner>,
}

/// The most of a class's runners out at once: for every repository
/// together, and for one. `None` is no limit.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Quota {
    pub all: Option<u32>,
    pub repository: Option<u32>,
}

/// The repository a look was claimed for, and whether it is connected
/// still: one that is not is started nothing, and what it has out is taken
/// back.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Claimed {
    pub repo: Repo,
    pub connected: bool,
}

const NOW_PG: &str = "now()";
const NOW_SQLITE: &str = "strftime('%Y-%m-%dT%H:%M:%fZ', 'now')";

/// Why a runner's row says it ended, where it ended because GitHub said
/// its job had.
pub(crate) const JOB_ENDED: &str = "its job ended";

const JOB_COLUMNS: &str = "forge_host, job_id, repo_id, workflow_run, run_attempt, name, \
     workflow, labels, class, head_sha, trust";

/// A new job's trust: what another job of its workflow run was told, where
/// one was. The run's head is one for all its jobs and attempts.
const TRUST_OF_ITS_RUN: &str = "(SELECT t.trust FROM ci_fleet_jobs t \
     WHERE t.repo_id = $3 AND t.workflow_run = $4 AND t.trust IS NOT NULL LIMIT 1)";

/// What a job's delivery that names a runner says of that runner, by its
/// name alone: an organization's runner may be given a job of any
/// repository its group lets in, and is the fleet's whichever it is. `$1`
/// is the runner's name, `$2` the job's id.
fn runner_heard(action: JobAction, now: &str) -> Option<String> {
    match action {
        JobAction::Queued => None,
        JobAction::InProgress => Some(
            "UPDATE ci_fleet_runners SET state = 'busy', job_id = $2 \
             WHERE name = $1 AND state IN ('wanted', 'launched')"
                .to_string(),
        ),
        JobAction::Completed => Some(format!(
            "UPDATE ci_fleet_runners \
             SET state = 'ended', job_id = COALESCE(job_id, $2), ended_at = {now}, \
                 reason = '{JOB_ENDED}' \
             WHERE name = $1 AND state <> 'ended'"
        )),
    }
}

impl Db {
    fn now(&self) -> &'static str {
        match self {
            Db::Postgres(_) => NOW_PG,
            Db::Sqlite(_) => NOW_SQLITE,
        }
    }

    /// The moment `secs` seconds ago, as each back end writes a time.
    fn ago(&self, secs: u64) -> String {
        match self {
            Db::Postgres(_) => format!("(now() - make_interval(secs => {secs}))"),
            Db::Sqlite(_) => format!("strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '-{secs} seconds')"),
        }
    }

    /// What a job's delivery says of a runner of the fleet's, where the job
    /// is of a repository that is not connected here: an organization's
    /// runner may be given one. Nothing of the job is kept.
    pub async fn fleet_runner_heard(&self, job: &Job) -> sqlx::Result<()> {
        let (Some(sql), Some(name)) = (runner_heard(job.action, self.now()), &job.runner_name)
        else {
            return Ok(());
        };
        let id = job.id.to_string();
        match self {
            Db::Postgres(p) => {
                sqlx::query(&sql).bind(name).bind(&id).execute(p).await?;
            }
            Db::Sqlite(p) => {
                sqlx::query(&lite(&sql))
                    .bind(name)
                    .bind(&id)
                    .execute(p)
                    .await?;
            }
        }
        Ok(())
    }

    /// Write what a `workflow_job` delivery says of a job the fleet's
    /// `class` serves, in one transaction: the job's row, moved forward and
    /// never back; the runner GitHub names, where it is one of the fleet's;
    /// and the repository made due for a look, unless one is going.
    ///
    /// A job is kept waiting only where its repository is connected, and
    /// that is decided here, in the statement that writes the job: the
    /// caller's own look at the repository was made before this, and a
    /// disconnect can come between the two. On Postgres the statement holds
    /// the repository's row while it decides, which a disconnect waits for:
    /// so a disconnect either finds the job and closes it, or the job finds
    /// the repository disconnected and is not written. Left waiting, it
    /// would be given a runner when the repository was connected again.
    pub async fn fleet_job(&self, repo: &Repo, job: &Job, class: &str) -> sqlx::Result<()> {
        let now = self.now();
        let labels = serde_json::to_string(&job.labels).unwrap_or_else(|_| "[]".into());
        let id = job.id.to_string();
        // SQLite has one writer at a time, and no row to hold.
        let held = match self {
            Db::Postgres(_) => " FOR NO KEY UPDATE OF r",
            Db::Sqlite(_) => "",
        };
        let trust = TRUST_OF_ITS_RUN;
        let write = match job.action {
            JobAction::Queued => format!(
                "INSERT INTO ci_fleet_jobs ({JOB_COLUMNS}, state) \
                 SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, {trust}, 'queued' \
                   FROM ci_repos r \
                  WHERE r.id = $3 AND r.disconnected_at IS NULL{held} \
                 ON CONFLICT (forge_host, job_id) DO NOTHING"
            ),
            JobAction::InProgress => format!(
                "INSERT INTO ci_fleet_jobs ({JOB_COLUMNS}, state, runner_name, started_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, {trust}, 'running', $11, {now}) \
                 ON CONFLICT (forge_host, job_id) DO UPDATE \
                     SET state = 'running', runner_name = excluded.runner_name, \
                         started_at = excluded.started_at \
                     WHERE ci_fleet_jobs.state = 'queued'"
            ),
            JobAction::Completed => format!(
                "INSERT INTO ci_fleet_jobs \
                     ({JOB_COLUMNS}, state, runner_name, conclusion, finished_at) \
                 VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, {trust}, 'done', $11, $12, {now}) \
                 ON CONFLICT (forge_host, job_id) DO UPDATE \
                     SET state = 'done', conclusion = excluded.conclusion, \
                         runner_name = COALESCE(excluded.runner_name, ci_fleet_jobs.runner_name), \
                         finished_at = excluded.finished_at \
                     WHERE ci_fleet_jobs.state <> 'done'"
            ),
        };
        // A runner of the fleet's that GitHub names has the job, or had it.
        let runner = runner_heard(job.action, now);
        let due = format!(
            "UPDATE ci_repos SET fleet_due_at = {now} WHERE id = $1 AND fleet_claim IS NULL"
        );
        let (write, runner, due) = match self {
            Db::Postgres(_) => (write, runner, due),
            Db::Sqlite(_) => (lite(&write), runner.as_deref().map(lite), lite(&due)),
        };
        macro_rules! record {
            ($pool:expr) => {{
                let mut tx = $pool.begin().await?;
                let mut q = sqlx::query(&write)
                    .bind(&repo.forge_host)
                    .bind(&id)
                    .bind(&repo.id)
                    .bind(job.run_id.to_string())
                    .bind(job.run_attempt as i32)
                    .bind(&job.name)
                    .bind(job.workflow.as_deref())
                    .bind(&labels)
                    .bind(class)
                    .bind(&job.head_sha);
                if job.action != JobAction::Queued {
                    q = q.bind(job.runner_name.as_deref());
                }
                if job.action == JobAction::Completed {
                    q = q.bind(job.conclusion.as_deref());
                }
                q.execute(&mut *tx).await?;
                if let (Some(sql), Some(name)) = (&runner, &job.runner_name) {
                    sqlx::query(sql)
                        .bind(name)
                        .bind(&id)
                        .execute(&mut *tx)
                        .await?;
                }
                sqlx::query(&due).bind(&repo.id).execute(&mut *tx).await?;
                tx.commit().await
            }};
        }
        match self {
            Db::Postgres(p) => record!(p),
            Db::Sqlite(p) => record!(p),
        }
    }

    /// Claim the repository whose fleet is most overdue for a look, for
    /// `lease`, as `claim`. `None` when none is due.
    pub async fn claim_fleet(&self, claim: &str, lease: Duration) -> sqlx::Result<Option<Claimed>> {
        const RETURNING: &str =
            "RETURNING id, forge, forge_host, full_name, workflow, cell, mode, \
             CAST(disconnected_at IS NULL AS INTEGER) AS connected";
        macro_rules! claimed {
            ($row:expr) => {
                match $row {
                    None => None,
                    Some(row) => Some(Claimed {
                        repo: Repo {
                            id: row.try_get("id")?,
                            forge: row.try_get("forge")?,
                            forge_host: row.try_get("forge_host")?,
                            full_name: row.try_get("full_name")?,
                            workflow: row.try_get("workflow")?,
                            cell: row.try_get("cell")?,
                            mode: crate::db::mode_of(&row.try_get::<String, _>("mode")?)?,
                        },
                        connected: row.try_get::<i32, _>("connected")? != 0,
                    }),
                }
            };
        }
        Ok(match self {
            Db::Postgres(p) => {
                let sql = format!(
                    "UPDATE ci_repos \
                     SET fleet_claim = $1, fleet_poked_at = NULL, \
                         fleet_due_at = now() + make_interval(secs => $2) \
                     WHERE id = ( \
                         SELECT id FROM ci_repos \
                         WHERE fleet_due_at IS NOT NULL AND fleet_due_at <= now() \
                         ORDER BY fleet_due_at, id LIMIT 1 FOR UPDATE SKIP LOCKED) \
                     {RETURNING}"
                );
                let row = sqlx::query(&sql)
                    .bind(claim)
                    .bind(lease.as_secs_f64())
                    .fetch_optional(p)
                    .await?;
                claimed!(row)
            }
            Db::Sqlite(p) => {
                let sql = format!(
                    "UPDATE ci_repos \
                     SET fleet_claim = ?1, fleet_poked_at = NULL, \
                         fleet_due_at = strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?2 || ' seconds') \
                     WHERE id = ( \
                         SELECT id FROM ci_repos \
                         WHERE fleet_due_at IS NOT NULL AND fleet_due_at <= {NOW_SQLITE} \
                         ORDER BY fleet_due_at, id LIMIT 1) \
                     {RETURNING}"
                );
                let row = sqlx::query(&sql)
                    .bind(claim)
                    .bind(lease.as_secs().max(1) as i64)
                    .fetch_optional(p)
                    .await?;
                claimed!(row)
            }
        })
    }

    /// End a look `claim` holds. The repository is looked over again in
    /// `again` if a job of it still waits or a runner of it is still out, as
    /// its rows now stand, or a look was asked for while this one went
    /// ([`Db::fleet_due`]), and not at all otherwise. `false` when the claim
    /// was lost: nothing changes.
    pub async fn finish_fleet(
        &self,
        repo_id: &str,
        claim: &str,
        again: Duration,
    ) -> sqlx::Result<bool> {
        // A job waits for a runner only where its repository is connected:
        // a disconnected one is started nothing, and is looked over for as
        // long as a runner of it is still out, and no longer. A job due to
        // be run again is asked for while its repository is connected, and
        // so is a look asked for while this one went.
        const OPEN: &str =
            "(ci_repos.disconnected_at IS NULL AND (ci_repos.fleet_poked_at IS NOT NULL \
                 OR EXISTS (SELECT 1 FROM ci_fleet_jobs j \
                 WHERE j.repo_id = ci_repos.id AND (j.state = 'queued' OR j.retry = 'due')))) \
             OR EXISTS (SELECT 1 FROM ci_fleet_runners r \
                 WHERE r.repo_id = ci_repos.id AND r.swept_at IS NULL)";
        let rows = match self {
            Db::Postgres(p) => {
                let sql = format!(
                    "UPDATE ci_repos SET fleet_claim = NULL, \
                         fleet_due_at = CASE WHEN {OPEN} \
                             THEN now() + make_interval(secs => $3) ELSE NULL END \
                     WHERE id = $1 AND fleet_claim = $2"
                );
                sqlx::query(&sql)
                    .bind(repo_id)
                    .bind(claim)
                    .bind(again.as_secs_f64())
                    .execute(p)
                    .await?
                    .rows_affected()
            }
            Db::Sqlite(p) => {
                let sql = format!(
                    "UPDATE ci_repos SET fleet_claim = NULL, \
                         fleet_due_at = CASE WHEN {OPEN} \
                             THEN strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+' || ?3 || ' seconds') \
                             ELSE NULL END \
                     WHERE id = ?1 AND fleet_claim = ?2"
                );
                sqlx::query(&sql)
                    .bind(repo_id)
                    .bind(claim)
                    .bind(again.as_secs().max(1) as i64)
                    .execute(p)
                    .await?
                    .rows_affected()
            }
        };
        Ok(rows == 1)
    }

    /// A repository's fleet as its rows stand: the jobs that wait, by class,
    /// and every runner not yet swept.
    pub async fn fleet_view(&self, repo_id: &str) -> sqlx::Result<View> {
        const QUEUED: &str = "SELECT class, trust, CAST(COUNT(*) AS BIGINT) AS waiting \
             FROM ci_fleet_jobs WHERE repo_id = $1 AND state = 'queued' GROUP BY class, trust";
        let since = |column: &str| match self {
            Db::Postgres(_) => format!("EXTRACT(EPOCH FROM (now() - {column}))"),
            Db::Sqlite(_) => format!("(julianday('now') - julianday({column})) * 86400"),
        };
        let (age, ended) = (since("r.created_at"), since("r.ended_at"));
        let untold = since("COALESCE(j.started_at, j.queued_at)");
        let newest = format!(
            "SELECT class, CAST({} AS BIGINT) AS newest FROM ci_fleet_jobs \
             WHERE repo_id = $1 GROUP BY class",
            since("MAX(queued_at)")
        );
        // The job a runner took may be of another repository, where the
        // runner is an organization's: it is found on the runner's forge.
        let runners = format!(
            "SELECT r.name, r.class, r.cell, r.forge_id, r.org, r.trust, r.run_id, r.state, \
                 r.job_id, j.trust AS job_trust, CAST({age} AS BIGINT) AS age_secs, r.reason, \
                 CAST({ended} AS BIGINT) AS ended_secs, \
                 CASE WHEN j.job_id IS NOT NULL AND j.trust IS NULL \
                     THEN CAST({untold} AS BIGINT) END AS job_untold_secs \
             FROM ci_fleet_runners r \
             LEFT JOIN ci_repos o ON o.id = r.repo_id \
             LEFT JOIN ci_fleet_jobs j ON j.job_id = r.job_id AND j.forge_host = o.forge_host \
             WHERE r.repo_id = $1 AND r.swept_at IS NULL \
             ORDER BY r.created_at, r.name"
        );
        macro_rules! view {
            ($pool:expr, $queued:expr, $runners:expr, $newest:expr) => {{
                let mut view = View::default();
                for row in sqlx::query($newest).bind(repo_id).fetch_all($pool).await? {
                    view.newest
                        .insert(row.try_get("class")?, row.try_get("newest")?);
                }
                for row in sqlx::query($queued).bind(repo_id).fetch_all($pool).await? {
                    let waiting: i64 = row.try_get("waiting")?;
                    let class: String = row.try_get("class")?;
                    let trust: Option<String> = row.try_get("trust")?;
                    let counts = view.queued.entry(class).or_default();
                    match trust.as_deref() {
                        Some(TRUSTED) => counts.trusted += waiting,
                        Some(_) => counts.untrusted += waiting,
                        None => counts.unknown += waiting,
                    }
                }
                for row in sqlx::query($runners).bind(repo_id).fetch_all($pool).await? {
                    view.runners.push(Runner {
                        name: row.try_get("name")?,
                        class: row.try_get("class")?,
                        cell: row.try_get("cell")?,
                        forge_id: row.try_get("forge_id")?,
                        org: row.try_get("org")?,
                        trust: row.try_get("trust")?,
                        run_id: row.try_get("run_id")?,
                        state: row.try_get("state")?,
                        job_id: row.try_get("job_id")?,
                        job_trust: row.try_get("job_trust")?,
                        job_untold_secs: row.try_get("job_untold_secs")?,
                        age_secs: row.try_get("age_secs")?,
                        reason: row.try_get("reason")?,
                        ended_secs: row.try_get("ended_secs")?,
                    });
                }
                view
            }};
        }
        Ok(match self {
            Db::Postgres(p) => view!(p, QUEUED, runners.as_str(), newest.as_str()),
            Db::Sqlite(p) => view!(
                p,
                lite(QUEUED).as_str(),
                lite(&runners).as_str(),
                lite(&newest).as_str()
            ),
        })
    }

    /// One statement on a runner's row, by its name: how many rows it
    /// changed.
    async fn fleet_runner(
        &self,
        sql: &str,
        name: &str,
        binds: &[Option<&str>],
    ) -> sqlx::Result<u64> {
        macro_rules! run {
            ($pool:expr, $sql:expr) => {{
                let mut q = sqlx::query($sql).bind(name);
                for bind in binds {
                    q = q.bind(*bind);
                }
                q.execute($pool).await?.rows_affected()
            }};
        }
        Ok(match self {
            Db::Postgres(p) => run!(p, sql),
            Db::Sqlite(p) => {
                let sql = lite(sql);
                run!(p, sql.as_str())
            }
        })
    }

    /// Make `repo_id`'s fleet due for a look now: a GitLab project's Job
    /// Hook says a job waits, and not which tags it names, which the look
    /// asks GitLab. Where a look is going, it is followed by another
    /// ([`Db::finish_fleet`]): the jobs it listed may be older than the
    /// hook's, and the hook writes no row that would say one waits.
    pub async fn fleet_due(&self, repo_id: &str) -> sqlx::Result<()> {
        let now = self.now();
        let sql = format!(
            "UPDATE ci_repos SET fleet_poked_at = {now}, \
                 fleet_due_at = CASE WHEN fleet_claim IS NULL THEN {now} ELSE fleet_due_at END \
             WHERE id = $1 AND disconnected_at IS NULL"
        );
        match self {
            Db::Postgres(p) => {
                sqlx::query(&sql).bind(repo_id).execute(p).await?;
            }
            Db::Sqlite(p) => {
                sqlx::query(&lite(&sql)).bind(repo_id).execute(p).await?;
            }
        }
        Ok(())
    }

    /// The class of the job `job_id` on `forge_host`, where the fleet keeps
    /// it.
    pub async fn fleet_job_class(
        &self,
        forge_host: &str,
        job_id: &str,
    ) -> sqlx::Result<Option<String>> {
        const CLASS: &str = "SELECT class FROM ci_fleet_jobs WHERE forge_host = $1 AND job_id = $2";
        Ok(match self {
            Db::Postgres(p) => {
                sqlx::query_scalar(CLASS)
                    .bind(forge_host)
                    .bind(job_id)
                    .fetch_optional(p)
                    .await?
            }
            Db::Sqlite(p) => {
                sqlx::query_scalar(&lite(CLASS))
                    .bind(forge_host)
                    .bind(job_id)
                    .fetch_optional(p)
                    .await?
            }
        })
    }

    /// The jobs of `repo_id` that wait, by id and class, the oldest first.
    pub async fn fleet_queued(&self, repo_id: &str) -> sqlx::Result<Vec<(String, String)>> {
        const QUEUED: &str = "SELECT job_id, class FROM ci_fleet_jobs \
             WHERE repo_id = $1 AND state = 'queued' ORDER BY queued_at, job_id";
        Ok(match self {
            Db::Postgres(p) => sqlx::query_as(QUEUED).bind(repo_id).fetch_all(p).await?,
            Db::Sqlite(p) => {
                sqlx::query_as(&lite(QUEUED))
                    .bind(repo_id)
                    .fetch_all(p)
                    .await?
            }
        })
    }

    /// The class of the fleet's runner `name`, where it has one by that name.
    pub async fn fleet_runner_class(&self, name: &str) -> sqlx::Result<Option<String>> {
        const CLASS: &str = "SELECT class FROM ci_fleet_runners WHERE name = $1";
        Ok(match self {
            Db::Postgres(p) => {
                sqlx::query_scalar(CLASS)
                    .bind(name)
                    .fetch_optional(p)
                    .await?
            }
            Db::Sqlite(p) => {
                sqlx::query_scalar(&lite(CLASS))
                    .bind(name)
                    .fetch_optional(p)
                    .await?
            }
        })
    }

    /// Write the row of a runner the fleet wants for `repo`, of `class`:
    /// nothing of it exists yet but its name. `org` is the organization it
    /// is to be registered for, where it is an organization's, and `trust`
    /// what its run is to be submitted as. `false`, and nothing written,
    /// where the class has as many runners out as `quota` lets it: for every
    /// repository together, or for this one.
    ///
    /// The count and the row are one statement, so that two looks at once
    /// cannot both take the last place: on Postgres under a lock of the
    /// class's, held to the end of the transaction, and on SQLite, which
    /// has one writer at a time, by the statement alone.
    pub async fn fleet_want(
        &self,
        repo: &Repo,
        class: &str,
        name: &str,
        org: Option<&str>,
        trust: &str,
        quota: Quota,
    ) -> sqlx::Result<bool> {
        const WANT: &str = "INSERT INTO ci_fleet_runners \
                 (name, repo_id, class, cell, state, org, trust) \
             SELECT $1, $2, $3, $4, 'wanted', $5, $6 \
             WHERE ($7 IS NULL OR (SELECT COUNT(*) FROM ci_fleet_runners \
                     WHERE class = $3 AND state <> 'ended') < $7) \
               AND ($8 IS NULL OR (SELECT COUNT(*) FROM ci_fleet_runners \
                     WHERE class = $3 AND repo_id = $2 AND state <> 'ended') < $8)";
        let (all, one) = (quota.all.map(i64::from), quota.repository.map(i64::from));
        macro_rules! want {
            ($sql:expr, $exec:expr) => {
                sqlx::query($sql)
                    .bind(name)
                    .bind(&repo.id)
                    .bind(class)
                    .bind(&repo.cell)
                    .bind(org)
                    .bind(trust)
                    .bind(all)
                    .bind(one)
                    .execute($exec)
                    .await?
                    .rows_affected()
            };
        }
        let written = match self {
            Db::Postgres(p) if all.is_some() || one.is_some() => {
                let mut tx = p.begin().await?;
                sqlx::query("SELECT pg_advisory_xact_lock(hashtext('dagron-ci fleet ' || $1))")
                    .bind(class)
                    .execute(&mut *tx)
                    .await?;
                let written = want!(WANT, &mut *tx);
                tx.commit().await?;
                written
            }
            Db::Postgres(p) => want!(WANT, p),
            Db::Sqlite(p) => want!(&lite(WANT), p),
        };
        Ok(written == 1)
    }

    /// The workflow runs of `repo_id`'s jobs not yet told trusted or not,
    /// whose class is one of `classes`, oldest first: at most `limit`.
    pub async fn fleet_untold(
        &self,
        repo_id: &str,
        classes: &[&str],
        limit: usize,
    ) -> sqlx::Result<Vec<String>> {
        const UNTOLD: &str = "SELECT workflow_run, class FROM ( \
                 SELECT workflow_run, class, MIN(queued_at) AS first_queued \
             FROM ci_fleet_jobs WHERE repo_id = $1 AND trust IS NULL AND state <> 'done' \
             GROUP BY workflow_run, class) untold ORDER BY first_queued, workflow_run";
        let rows: Vec<(String, String)> = match self {
            Db::Postgres(p) => sqlx::query_as(UNTOLD).bind(repo_id).fetch_all(p).await?,
            Db::Sqlite(p) => {
                sqlx::query_as(&lite(UNTOLD))
                    .bind(repo_id)
                    .fetch_all(p)
                    .await?
            }
        };
        let mut runs: Vec<String> = Vec::new();
        for (run, class) in rows {
            if classes.contains(&class.as_str()) && !runs.contains(&run) {
                runs.push(run);
                if runs.len() == limit {
                    break;
                }
            }
        }
        Ok(runs)
    }

    /// What GitHub says the workflow run `workflow_run` of `repo_id` is,
    /// [`TRUSTED`] or [`UNTRUSTED`]: each of its jobs is that.
    pub async fn fleet_told(
        &self,
        repo_id: &str,
        workflow_run: &str,
        trust: &str,
    ) -> sqlx::Result<()> {
        const TOLD: &str = "UPDATE ci_fleet_jobs SET trust = $3 \
             WHERE repo_id = $1 AND workflow_run = $2 AND trust IS NULL";
        macro_rules! told {
            ($pool:expr, $sql:expr) => {
                sqlx::query($sql)
                    .bind(repo_id)
                    .bind(workflow_run)
                    .bind(trust)
                    .execute($pool)
                    .await
                    .map(drop)
            };
        }
        match self {
            Db::Postgres(p) => told!(p, TOLD),
            Db::Sqlite(p) => told!(p, &lite(TOLD)),
        }
    }

    /// The jobs of `repo_id` to ask GitHub about, as their delivery may never
    /// have come, by id and class, the longest unheard of first: at most
    /// `limit`.
    ///
    /// - One that has waited longer than `after`: a job is given a runner in
    ///   seconds, and one cancelled as it waits is delivered as over at once.
    ///   Its delivery that says so may be the one that was lost.
    /// - One that GitHub said a runner of the fleet's took, whose runner has
    ///   been over longer than `after`: the job's end was not heard of.
    ///
    /// Each is asked again no sooner than `again` after the last time.
    pub async fn fleet_unheard(
        &self,
        repo_id: &str,
        after: Duration,
        again: Duration,
        limit: i64,
    ) -> sqlx::Result<Vec<(String, String)>> {
        let (after, again) = (self.ago(after.as_secs()), self.ago(again.as_secs()));
        let sql = format!(
            "SELECT j.job_id, j.class FROM ci_fleet_jobs j \
             WHERE j.repo_id = $1 AND j.state <> 'done' \
               AND (j.asked_at IS NULL OR j.asked_at < {again}) \
               AND ((j.state = 'queued' AND j.queued_at < {after}) \
                 OR (j.state = 'running' AND EXISTS (SELECT 1 FROM ci_fleet_runners r \
                     WHERE r.name = j.runner_name AND r.state = 'ended' \
                       AND r.ended_at < {after}))) \
             ORDER BY COALESCE(j.asked_at, j.queued_at), j.job_id LIMIT $2"
        );
        Ok(match self {
            Db::Postgres(p) => {
                sqlx::query_as(&sql)
                    .bind(repo_id)
                    .bind(limit)
                    .fetch_all(p)
                    .await?
            }
            Db::Sqlite(p) => {
                sqlx::query_as(&lite(&sql))
                    .bind(repo_id)
                    .bind(limit)
                    .fetch_all(p)
                    .await?
            }
        })
    }

    /// The jobs of `repo_id` that failed on a runner of the fleet's that
    /// died (its run was seen to fail or to be gone, whether or not GitHub
    /// said first that its job had ended), and that are yet to be run again
    /// or are due to be: one not yet
    /// decided, of those that ended inside `within`, and one that is due
    /// whose time to be asked again has come, whenever it ended. At most
    /// `limit`, the first to end first.
    pub async fn fleet_died(
        &self,
        repo_id: &str,
        within: Duration,
        limit: i64,
    ) -> sqlx::Result<Vec<Died>> {
        let since = |column: &str| match self {
            Db::Postgres(_) => format!("EXTRACT(EPOCH FROM (now() - {column}))"),
            Db::Sqlite(_) => format!("(julianday('now') - julianday({column})) * 86400"),
        };
        let lived = match self {
            Db::Postgres(_) => "EXTRACT(EPOCH FROM (r.ended_at - r.launched_at))".to_string(),
            Db::Sqlite(_) => "(julianday(r.ended_at) - julianday(r.launched_at)) * 86400".into(),
        };
        let (ended, within) = (since("j.finished_at"), self.ago(within.as_secs()));
        let now = self.now();
        let died = DIED.map(|reason| format!("'{reason}'")).join(", ");
        let sql = format!(
            "SELECT j.job_id, j.workflow_run, j.name, j.class, j.retry, \
                 CAST({lived} AS BIGINT) AS lived_secs, CAST({ended} AS BIGINT) AS ended_secs \
             FROM ci_fleet_jobs j JOIN ci_fleet_runners r ON r.name = j.runner_name \
             WHERE j.repo_id = $1 AND j.state = 'done' AND j.conclusion = 'failure' \
               AND (r.reason IN ({died}) OR r.run_end IN ('failed', 'gone')) \
               AND ((j.retry IS NULL AND j.finished_at > {within}) \
                 OR (j.retry = 'due' AND (j.retry_at IS NULL OR j.retry_at <= {now}))) \
             ORDER BY j.finished_at, j.job_id LIMIT $2"
        );
        macro_rules! died {
            ($pool:expr, $sql:expr) => {{
                let mut out = Vec::new();
                for row in sqlx::query($sql)
                    .bind(repo_id)
                    .bind(limit)
                    .fetch_all($pool)
                    .await?
                {
                    out.push(Died {
                        job_id: row.try_get("job_id")?,
                        workflow_run: row.try_get("workflow_run")?,
                        name: row.try_get("name")?,
                        class: row.try_get("class")?,
                        retry: row.try_get("retry")?,
                        lived_secs: row.try_get("lived_secs")?,
                        ended_secs: row.try_get("ended_secs")?,
                    });
                }
                out
            }};
        }
        Ok(match self {
            Db::Postgres(p) => died!(p, &sql),
            Db::Sqlite(p) => died!(p, &lite(&sql)),
        })
    }

    /// Whether a job named `name` of the workflow run `workflow_run` of
    /// `repo_id`, other than `job_id`, was run again already: a job is run
    /// again once, whichever of its attempts died. One that is due to be,
    /// and of which `job_id` is a later attempt ([`Db::fleet_run_again`]),
    /// was, however the ask's answer was lost.
    pub async fn fleet_retried(
        &self,
        repo_id: &str,
        workflow_run: &str,
        name: &str,
        job_id: &str,
    ) -> sqlx::Result<bool> {
        const RETRIED: &str = "SELECT CAST(COUNT(*) AS BIGINT) FROM ci_fleet_jobs o \
             JOIN ci_repos r ON r.id = o.repo_id \
             LEFT JOIN ci_fleet_jobs x ON x.repo_id = o.repo_id AND x.job_id = $4 \
             WHERE o.repo_id = $1 AND o.workflow_run = $2 AND o.name = $3 AND o.job_id <> $4 \
               AND (o.retry = 'asked' OR (o.retry = 'due' AND (x.run_attempt > o.run_attempt \
                   OR (r.forge = 'gitlab' AND x.queued_at >= o.finished_at))))";
        macro_rules! retried {
            ($pool:expr, $sql:expr) => {
                sqlx::query_scalar::<_, i64>($sql)
                    .bind(repo_id)
                    .bind(workflow_run)
                    .bind(name)
                    .bind(job_id)
                    .fetch_one($pool)
                    .await?
            };
        }
        Ok(match self {
            Db::Postgres(p) => retried!(p, RETRIED),
            Db::Sqlite(p) => retried!(p, &lite(RETRIED)),
        } > 0)
    }

    /// Whether `repo_id`'s job `job_id` was run again, by whoever asked: a
    /// later attempt of it is known. On GitHub that is a job of its name in
    /// a later attempt of its workflow run. GitLab runs a job again as a new
    /// job of the same pipeline, and names a pipeline's jobs apart: there it
    /// is a job of its name queued after it ended.
    pub async fn fleet_run_again(&self, repo_id: &str, job_id: &str) -> sqlx::Result<bool> {
        const AGAIN: &str = "SELECT CAST(COUNT(*) AS BIGINT) FROM ci_fleet_jobs o \
             JOIN ci_fleet_jobs x ON x.repo_id = o.repo_id \
                 AND x.workflow_run = o.workflow_run AND x.name = o.name \
             JOIN ci_repos r ON r.id = x.repo_id \
             WHERE x.repo_id = $1 AND x.job_id = $2 AND o.job_id <> x.job_id \
               AND (o.run_attempt > x.run_attempt \
                 OR (r.forge = 'gitlab' AND o.queued_at >= x.finished_at))";
        macro_rules! again {
            ($pool:expr, $sql:expr) => {
                sqlx::query_scalar::<_, i64>($sql)
                    .bind(repo_id)
                    .bind(job_id)
                    .fetch_one($pool)
                    .await?
            };
        }
        Ok(match self {
            Db::Postgres(p) => again!(p, AGAIN),
            Db::Sqlite(p) => again!(p, &lite(AGAIN)),
        } > 0)
    }

    /// What came of running the job `job_id` again: `retry` is `due`,
    /// `asked` or `refused`, `again` when a job that is due is asked about
    /// next, and `note` what GitHub said, or why it was not asked. A job
    /// GitHub took the ask for stays `asked`: a look that ran past its lease,
    /// and another replica's, may come to it after.
    pub async fn fleet_retry(
        &self,
        forge_host: &str,
        job_id: &str,
        retry: &str,
        again: Option<Duration>,
        note: Option<&str>,
    ) -> sqlx::Result<()> {
        let at = match (self, again) {
            (_, None) => "NULL".to_string(),
            (Db::Postgres(_), Some(again)) => {
                format!("now() + make_interval(secs => {})", again.as_secs())
            }
            (Db::Sqlite(_), Some(again)) => format!(
                "strftime('%Y-%m-%dT%H:%M:%fZ', 'now', '+{} seconds')",
                again.as_secs()
            ),
        };
        let sql = format!(
            "UPDATE ci_fleet_jobs SET retry = $3, retry_at = {at}, retry_note = $4 \
             WHERE forge_host = $1 AND job_id = $2 AND (retry IS NULL OR retry <> 'asked')"
        );
        macro_rules! retry {
            ($pool:expr, $sql:expr) => {
                sqlx::query($sql)
                    .bind(forge_host)
                    .bind(job_id)
                    .bind(retry)
                    .bind(note)
                    .execute($pool)
                    .await
                    .map(drop)
            };
        }
        match self {
            Db::Postgres(p) => retry!(p, &sql),
            Db::Sqlite(p) => retry!(p, &lite(&sql)),
        }
    }

    /// What each repository's runners of each class did from `since` to
    /// `until`, as both back ends write a time ([`crate::db::Day::start`]):
    /// the jobs that started in it on a runner of the fleet's, ended or not,
    /// whichever repository started the runner; and the runners submitted in
    /// it, for the repository they were started for, until they ended or
    /// until now.
    pub async fn fleet_used(&self, since: &str, until: &str) -> sqlx::Result<Vec<Used>> {
        let pg = matches!(self, Db::Postgres(_));
        let span = |from: &str, to: &str| match pg {
            true => format!("EXTRACT(EPOCH FROM ({to} - {from}))"),
            false => format!("(julianday({to}) - julianday({from})) * 86400"),
        };
        let time = match pg {
            true => "CAST($1 AS TIMESTAMPTZ)",
            false => "$1",
        };
        let until_time = time.replace("$1", "$2");
        let now = self.now();
        let jobs = format!(
            "SELECT o.forge_host, o.full_name, j.class, CAST(COUNT(*) AS BIGINT) AS jobs, \
                 CAST(COALESCE(SUM({}), 0) AS BIGINT) AS secs \
             FROM ci_fleet_jobs j JOIN ci_repos o ON o.id = j.repo_id \
             WHERE j.started_at >= {time} AND j.started_at < {until_time} \
               AND EXISTS (SELECT 1 FROM ci_fleet_runners f WHERE f.name = j.runner_name) \
             GROUP BY o.forge_host, o.full_name, j.class",
            span("j.started_at", &format!("COALESCE(j.finished_at, {now})"))
        );
        let runners = format!(
            "SELECT o.forge_host, o.full_name, f.class, CAST(COUNT(*) AS BIGINT) AS runners, \
                 CAST(COALESCE(SUM({}), 0) AS BIGINT) AS secs \
             FROM ci_fleet_runners f JOIN ci_repos o ON o.id = f.repo_id \
             WHERE f.launched_at >= {time} AND f.launched_at < {until_time} \
             GROUP BY o.forge_host, o.full_name, f.class",
            span("f.launched_at", &format!("COALESCE(f.ended_at, {now})"))
        );
        let mut used: BTreeMap<(String, String, String), Used> = BTreeMap::new();
        macro_rules! read {
            ($pool:expr, $jobs:expr, $runners:expr) => {{
                for (sql, runners) in [($jobs, false), ($runners, true)] {
                    for row in sqlx::query(sql)
                        .bind(since)
                        .bind(until)
                        .fetch_all($pool)
                        .await?
                    {
                        let key: (String, String, String) = (
                            row.try_get("forge_host")?,
                            row.try_get("full_name")?,
                            row.try_get("class")?,
                        );
                        let entry = used.entry(key.clone()).or_insert_with(|| Used {
                            forge_host: key.0,
                            full_name: key.1,
                            class: key.2,
                            ..Used::default()
                        });
                        let secs: i64 = row.try_get("secs")?;
                        match runners {
                            false => {
                                entry.jobs = row.try_get("jobs")?;
                                entry.job_secs = secs;
                            }
                            true => {
                                entry.runners = row.try_get("runners")?;
                                entry.runner_secs = secs;
                            }
                        }
                    }
                }
            }};
        }
        match self {
            Db::Postgres(p) => read!(p, jobs.as_str(), runners.as_str()),
            Db::Sqlite(p) => read!(p, lite(&jobs).as_str(), lite(&runners).as_str()),
        }
        Ok(used.into_values().collect())
    }

    /// The fleet as it stands, every repository together ([`Gauges`]).
    pub async fn fleet_gauges(&self) -> sqlx::Result<Gauges> {
        const WAITING: &str = "SELECT class, CAST(COUNT(*) AS BIGINT) AS n FROM ci_fleet_jobs \
             WHERE state = 'queued' GROUP BY class";
        const RUNNERS: &str = "SELECT class, state, CAST(COUNT(*) AS BIGINT) AS n \
             FROM ci_fleet_runners WHERE state <> 'ended' GROUP BY class, state";
        let mut gauges = Gauges::default();
        macro_rules! read {
            ($pool:expr) => {{
                for row in sqlx::query(WAITING).fetch_all($pool).await? {
                    gauges
                        .waiting
                        .insert(row.try_get("class")?, row.try_get("n")?);
                }
                for row in sqlx::query(RUNNERS).fetch_all($pool).await? {
                    gauges.runners.insert(
                        (row.try_get("class")?, row.try_get("state")?),
                        row.try_get("n")?,
                    );
                }
            }};
        }
        match self {
            Db::Postgres(p) => read!(p),
            Db::Sqlite(p) => read!(p),
        }
        Ok(gauges)
    }

    /// Drop the fleet's rows that are over before `horizon`, as both back
    /// ends write a time: the jobs that ended before it, and the runners
    /// cleared up after before it. What is still open is kept, however old.
    /// How many of each went.
    pub async fn drop_fleet_before(&self, horizon: &str) -> sqlx::Result<(u64, u64)> {
        let time = match self {
            Db::Postgres(_) => "CAST($1 AS TIMESTAMPTZ)",
            Db::Sqlite(_) => "$1",
        };
        let jobs = format!(
            "DELETE FROM ci_fleet_jobs WHERE state = 'done' AND finished_at < {time} \
               AND (retry IS NULL OR retry <> 'due')"
        );
        let runners = format!("DELETE FROM ci_fleet_runners WHERE swept_at < {time}");
        macro_rules! drop_rows {
            ($pool:expr, $jobs:expr, $runners:expr) => {{
                let jobs = sqlx::query($jobs).bind(horizon).execute($pool).await?;
                let runners = sqlx::query($runners).bind(horizon).execute($pool).await?;
                (jobs.rows_affected(), runners.rows_affected())
            }};
        }
        Ok(match self {
            Db::Postgres(p) => drop_rows!(p, &jobs, &runners),
            Db::Sqlite(p) => drop_rows!(p, &lite(&jobs), &lite(&runners)),
        })
    }

    /// GitHub has no job `job_id` now: its workflow run was deleted. It is
    /// over, however it ended.
    pub async fn fleet_job_gone(&self, forge_host: &str, job_id: &str) -> sqlx::Result<()> {
        let sql = format!(
            "UPDATE ci_fleet_jobs SET state = 'done', finished_at = {} \
             WHERE forge_host = $1 AND job_id = $2 AND state <> 'done'",
            self.now()
        );
        match self {
            Db::Postgres(p) => {
                sqlx::query(&sql)
                    .bind(forge_host)
                    .bind(job_id)
                    .execute(p)
                    .await?;
            }
            Db::Sqlite(p) => {
                sqlx::query(&lite(&sql))
                    .bind(forge_host)
                    .bind(job_id)
                    .execute(p)
                    .await?;
            }
        }
        Ok(())
    }

    /// GitHub was asked about the job `job_id` just now.
    pub async fn fleet_asked(&self, forge_host: &str, job_id: &str) -> sqlx::Result<()> {
        let sql = format!(
            "UPDATE ci_fleet_jobs SET asked_at = {} WHERE forge_host = $1 AND job_id = $2",
            self.now()
        );
        match self {
            Db::Postgres(p) => {
                sqlx::query(&sql)
                    .bind(forge_host)
                    .bind(job_id)
                    .execute(p)
                    .await?;
            }
            Db::Sqlite(p) => {
                sqlx::query(&lite(&sql))
                    .bind(forge_host)
                    .bind(job_id)
                    .execute(p)
                    .await?;
            }
        }
        Ok(())
    }

    /// The forge registered the runner, as `forge_id`: kept at once, so
    /// that the registration is taken back whatever becomes of the runner.
    pub async fn fleet_registered(&self, name: &str, forge_id: &str) -> sqlx::Result<()> {
        const REGISTERED: &str = "UPDATE ci_fleet_runners SET forge_id = $2 WHERE name = $1";
        self.fleet_runner(REGISTERED, name, &[Some(forge_id)])
            .await
            .map(drop)
    }

    /// The run that hosts the runner was submitted.
    pub async fn fleet_launched(&self, name: &str, run_id: &str) -> sqlx::Result<()> {
        let sql = format!(
            "UPDATE ci_fleet_runners SET state = 'launched', run_id = $2, launched_at = {} \
             WHERE name = $1 AND state = 'wanted'",
            self.now()
        );
        self.fleet_runner(&sql, name, &[Some(run_id)])
            .await
            .map(drop)
    }

    /// The runner is over, and `reason` says how. `false` when it was over
    /// already: its first reason stands.
    pub async fn fleet_ended(&self, name: &str, reason: &str) -> sqlx::Result<bool> {
        let sql = format!(
            "UPDATE ci_fleet_runners SET state = 'ended', reason = $2, ended_at = {} \
             WHERE name = $1 AND state <> 'ended'",
            self.now()
        );
        Ok(self.fleet_runner(&sql, name, &[Some(reason)]).await? == 1)
    }

    /// How the runner's run was seen to end: its status, or `gone`. The
    /// first seen stands.
    pub async fn fleet_run_end(&self, name: &str, end: &str) -> sqlx::Result<()> {
        const END: &str =
            "UPDATE ci_fleet_runners SET run_end = $2 WHERE name = $1 AND run_end IS NULL";
        self.fleet_runner(END, name, &[Some(end)]).await.map(drop)
    }

    /// What the runner left is taken back: it is looked at no more.
    pub async fn fleet_swept(&self, name: &str) -> sqlx::Result<()> {
        let sql = format!(
            "UPDATE ci_fleet_runners SET swept_at = {} WHERE name = $1 AND state = 'ended'",
            self.now()
        );
        self.fleet_runner(&sql, name, &[]).await.map(drop)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::tests::{drop_schema, exec, postgres, sqlite};

    const LEASE: Duration = Duration::from_secs(60);
    const SHA: &str = "4b4708e725f29891d22b4c96af9f0a403ff51c7c";

    fn job(id: u64, action: JobAction, runner: Option<&str>) -> Job {
        Job {
            forge_host: "github.com".into(),
            full_name: "acme/widgets".into(),
            action,
            id,
            run_id: 77,
            run_attempt: 1,
            name: "build".into(),
            workflow: Some("CI".into()),
            labels: vec!["self-hosted".into(), "dagron".into()],
            head_sha: SHA.into(),
            runner_id: runner.map(|_| 21),
            runner_name: runner.map(str::to_string),
            conclusion: (action == JobAction::Completed).then(|| "success".to_string()),
        }
    }

    async fn states(db: &Db) -> Vec<(String, String, Option<String>)> {
        let sql = "SELECT job_id, state, runner_name FROM ci_fleet_jobs ORDER BY job_id";
        match db {
            Db::Postgres(p) => sqlx::query_as(sql).fetch_all(p).await.unwrap(),
            Db::Sqlite(p) => sqlx::query_as(sql).fetch_all(p).await.unwrap(),
        }
    }

    /// The store's contract, on either back end.
    async fn the_fleet_s_rows(db: Db) {
        let repo = db
            .add_repo("github", "github.com", "acme/widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            db.claim_fleet("a", LEASE).await.unwrap(),
            None,
            "nothing to look over"
        );

        // A job is queued, twice over: one row, and the repository is due.
        for _ in 0..2 {
            db.fleet_job(&repo, &job(1, JobAction::Queued, None), "dagron")
                .await
                .unwrap();
        }
        let look = db.claim_fleet("a", LEASE).await.unwrap().expect("due");
        assert_eq!(
            (look.repo.id.as_str(), look.connected),
            (repo.id.as_str(), true)
        );
        assert_eq!(db.claim_fleet("b", LEASE).await.unwrap(), None, "held");
        let view = db.fleet_view(&repo.id).await.unwrap();
        assert_eq!(view.queued.get("dagron").map(Waiting::all), Some(1));
        assert!(view.runners.is_empty());

        // A runner is wanted, registered and launched.
        assert!(db
            .fleet_want(
                &repo,
                "dagron",
                "dagron-aaa",
                None,
                TRUSTED,
                Quota::default()
            )
            .await
            .unwrap());
        db.fleet_registered("dagron-aaa", "21").await.unwrap();
        db.fleet_launched("dagron-aaa", "run-1").await.unwrap();
        let view = db.fleet_view(&repo.id).await.unwrap();
        let runner = &view.runners[0];
        assert_eq!(
            (
                runner.state.as_str(),
                runner.forge_id.as_deref(),
                runner.run_id.as_deref()
            ),
            ("launched", Some("21"), Some("run-1"))
        );
        assert!(runner.age_secs >= 0);

        // A job queued while the look goes does not unseat it, and is not
        // forgotten when the look ends: the repository is looked over again.
        db.fleet_job(&repo, &job(2, JobAction::Queued, None), "dagron")
            .await
            .unwrap();
        assert_eq!(
            db.claim_fleet("b", LEASE).await.unwrap(),
            None,
            "still held"
        );
        assert!(
            !db.finish_fleet(&repo.id, "b", LEASE).await.unwrap(),
            "not b's"
        );
        assert!(db
            .finish_fleet(&repo.id, "a", Duration::from_secs(1000))
            .await
            .unwrap());
        assert_eq!(
            db.claim_fleet("b", LEASE).await.unwrap(),
            None,
            "in a while"
        );
        exec(
            &db,
            "UPDATE ci_repos SET fleet_due_at = '2000-01-01T00:00:00Z'",
        )
        .await;
        assert!(db.claim_fleet("b", LEASE).await.unwrap().is_some());

        // GitHub gives the runner a job, and the job ends. Heard of twice,
        // and out of order, a job's row only moves forward.
        db.fleet_job(
            &repo,
            &job(1, JobAction::InProgress, Some("dagron-aaa")),
            "dagron",
        )
        .await
        .unwrap();
        let view = db.fleet_view(&repo.id).await.unwrap();
        assert_eq!(
            view.queued.get("dagron").map(Waiting::all),
            Some(1),
            "the other still waits"
        );
        assert_eq!(
            (
                view.runners[0].state.as_str(),
                view.runners[0].job_id.as_deref()
            ),
            ("busy", Some("1"))
        );
        db.fleet_job(
            &repo,
            &job(1, JobAction::Completed, Some("dagron-aaa")),
            "dagron",
        )
        .await
        .unwrap();
        db.fleet_job(
            &repo,
            &job(1, JobAction::InProgress, Some("dagron-aaa")),
            "dagron",
        )
        .await
        .unwrap();
        db.fleet_job(&repo, &job(1, JobAction::Queued, None), "dagron")
            .await
            .unwrap();
        // One that another runner took, first heard of as it ended.
        db.fleet_job(
            &repo,
            &job(3, JobAction::Completed, Some("theirs")),
            "dagron",
        )
        .await
        .unwrap();
        assert_eq!(
            states(&db).await,
            [
                (
                    "1".to_string(),
                    "done".to_string(),
                    Some("dagron-aaa".to_string())
                ),
                ("2".to_string(), "queued".to_string(), None),
                (
                    "3".to_string(),
                    "done".to_string(),
                    Some("theirs".to_string())
                ),
            ]
        );
        let view = db.fleet_view(&repo.id).await.unwrap();
        assert_eq!(view.runners[0].state, "ended", "its job ended");
        assert!(
            !db.fleet_ended("dagron-aaa", "again").await.unwrap(),
            "over already"
        );

        // Swept, it is looked at no more. With no job waiting and no runner
        // out, the repository is not looked over at all.
        db.fleet_swept("dagron-aaa").await.unwrap();
        assert!(db.fleet_view(&repo.id).await.unwrap().runners.is_empty());
        db.fleet_job(
            &repo,
            &job(2, JobAction::Completed, Some("theirs")),
            "dagron",
        )
        .await
        .unwrap();
        assert!(db
            .finish_fleet(&repo.id, "b", Duration::from_secs(1))
            .await
            .unwrap());
        exec(&db, "UPDATE ci_repos SET fleet_due_at = '2000-01-01T00:00:00Z' WHERE fleet_due_at IS NOT NULL").await;
        assert_eq!(db.claim_fleet("c", LEASE).await.unwrap(), None);

        // A runner given up before it was launched ends with why.
        assert!(db
            .fleet_want(
                &repo,
                "dagron",
                "dagron-bbb",
                None,
                TRUSTED,
                Quota::default()
            )
            .await
            .unwrap());
        assert!(db
            .fleet_ended("dagron-bbb", "GitHub would not register it")
            .await
            .unwrap());
        db.fleet_launched("dagron-bbb", "run-2").await.unwrap();
        let view = db.fleet_view(&repo.id).await.unwrap();
        assert_eq!(
            (
                view.runners[0].state.as_str(),
                view.runners[0].run_id.as_deref()
            ),
            ("ended", None),
            "one that is over is not launched"
        );

        // A disconnected repository's fleet is still looked over, so that
        // what it has out is taken back, and the look says it is disconnected.
        db.fleet_job(&repo, &job(4, JobAction::Queued, None), "dagron")
            .await
            .unwrap();
        db.disconnect_repo("github.com", "acme/widgets", "alice")
            .await
            .unwrap();
        let look = db.claim_fleet("c", LEASE).await.unwrap().expect("due");
        assert!(!look.connected);
        // Its job that waited is closed with it: no delivery of it is taken
        // from here, so nothing would ever say it ended.
        let closed = states(&db).await;
        assert_eq!(closed[3], ("4".to_string(), "done".to_string(), None));
        assert!(db.fleet_view(&repo.id).await.unwrap().queued.is_empty());
        // A job's delivery that was let in before the disconnect, and is
        // written after it, is not kept waiting: the repository is looked
        // at again as the job is written. What says a job was taken, or had
        // ended, is still written: neither is given a runner.
        db.fleet_job(&repo, &job(5, JobAction::Queued, None), "dagron")
            .await
            .unwrap();
        assert!(states(&db).await.iter().all(|job| job.0 != "5"));
        db.fleet_job(&repo, &job(6, JobAction::Completed, None), "dagron")
            .await
            .unwrap();
        assert!(states(&db)
            .await
            .iter()
            .any(|job| job.0 == "6" && job.1 == "done"));
        assert!(db.fleet_view(&repo.id).await.unwrap().queued.is_empty());
        // It is looked over while a runner of it is out, and no longer.
        assert!(db
            .finish_fleet(&repo.id, "c", Duration::from_secs(1))
            .await
            .unwrap());
        exec(&db, "UPDATE ci_repos SET fleet_due_at = '2000-01-01T00:00:00Z' WHERE fleet_due_at IS NOT NULL").await;
        assert!(
            db.claim_fleet("d", LEASE).await.unwrap().is_some(),
            "a runner is out"
        );
        db.fleet_swept("dagron-bbb").await.unwrap();
        assert!(db
            .finish_fleet(&repo.id, "d", Duration::from_secs(1))
            .await
            .unwrap());
        exec(&db, "UPDATE ci_repos SET fleet_due_at = '2000-01-01T00:00:00Z' WHERE fleet_due_at IS NOT NULL").await;
        assert_eq!(db.claim_fleet("e", LEASE).await.unwrap(), None);
        // A job left waiting by a disconnect that did not close it (one
        // written before this was so) does not keep it looked over either.
        exec(
            &db,
            "UPDATE ci_fleet_jobs SET state = 'queued' WHERE job_id = '4'",
        )
        .await;
        exec(
            &db,
            "UPDATE ci_repos SET fleet_due_at = '2000-01-01T00:00:00Z'",
        )
        .await;
        assert!(db.claim_fleet("e", LEASE).await.unwrap().is_some());
        assert!(db
            .finish_fleet(&repo.id, "e", Duration::from_secs(1))
            .await
            .unwrap());
        exec(&db, "UPDATE ci_repos SET fleet_due_at = '2000-01-01T00:00:00Z' WHERE fleet_due_at IS NOT NULL").await;
        assert_eq!(db.claim_fleet("f", LEASE).await.unwrap(), None);

        // A look asked for while one goes, with no row to say why (a GitLab
        // project's Job Hook), is not lost: another follows it, and that
        // one's claim takes the ask.
        let project = db
            .add_repo("gitlab", "gitlab.example", "group/app", None, "default")
            .await
            .unwrap()
            .unwrap();
        db.fleet_due(&project.id).await.unwrap();
        let look = db.claim_fleet("g", LEASE).await.unwrap().expect("due");
        assert_eq!(look.repo.id, project.id);
        db.fleet_due(&project.id).await.unwrap();
        assert_eq!(db.claim_fleet("h", LEASE).await.unwrap(), None, "held");
        assert!(db
            .finish_fleet(&project.id, "g", Duration::from_secs(1))
            .await
            .unwrap());
        exec(&db, "UPDATE ci_repos SET fleet_due_at = '2000-01-01T00:00:00Z' WHERE fleet_due_at IS NOT NULL").await;
        assert!(
            db.claim_fleet("h", LEASE).await.unwrap().is_some(),
            "followed by another"
        );
        assert!(db
            .finish_fleet(&project.id, "h", Duration::from_secs(1))
            .await
            .unwrap());
        exec(&db, "UPDATE ci_repos SET fleet_due_at = '2000-01-01T00:00:00Z' WHERE fleet_due_at IS NOT NULL").await;
        assert_eq!(
            db.claim_fleet("i", LEASE).await.unwrap(),
            None,
            "and no more"
        );
    }

    /// A class's quota, what a job is told, the jobs GitHub is to be asked
    /// about, and a runner heard of by its name, on either back end.
    async fn the_fleet_s_quotas_trust_and_unheard_jobs(db: Db) {
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
        let quota = Quota {
            all: Some(3),
            repository: Some(2),
        };
        let want = |repo: &Repo, name: &str| {
            let (db, repo, name) = (db.clone(), repo.clone(), name.to_string());
            async move {
                db.fleet_want(&repo, "dagron", &name, Some("acme"), TRUSTED, quota)
                    .await
                    .unwrap()
            }
        };
        assert!(want(&widgets, "w-1").await);
        assert!(want(&widgets, "w-2").await);
        assert!(!want(&widgets, "w-3").await, "two for one repository");
        assert!(want(&gadgets, "g-1").await);
        assert!(!want(&gadgets, "g-2").await, "three for all of them");
        // Another class is counted apart, and a runner that ended is not
        // counted at all.
        assert!(db
            .fleet_want(&gadgets, "big", "b-1", None, UNTRUSTED, quota)
            .await
            .unwrap());
        assert!(db.fleet_ended("w-1", "no job waited for it").await.unwrap());
        assert!(want(&gadgets, "g-2").await);
        let view = db.fleet_view(&gadgets.id).await.unwrap();
        let mut rows: Vec<(&str, Option<&str>, &str)> = view
            .runners
            .iter()
            .map(|r| (r.name.as_str(), r.org.as_deref(), r.trust.as_str()))
            .collect();
        // Which runners are out, and not their order: the view lists them
        // oldest first, then by name, and SQLite keeps when a runner was made
        // to the millisecond, so runners made within one come by name.
        rows.sort();
        assert_eq!(
            rows,
            [
                ("b-1", None, UNTRUSTED),
                ("g-1", Some("acme"), TRUSTED),
                ("g-2", Some("acme"), TRUSTED)
            ]
        );

        // An organization's runner of widgets' is given a job of gadgets':
        // it is busy, by its name, and its job is the other repository's.
        db.fleet_job(&gadgets, &job(9, JobAction::Queued, None), "dagron")
            .await
            .unwrap();
        db.fleet_job(
            &gadgets,
            &job(9, JobAction::InProgress, Some("w-2")),
            "dagron",
        )
        .await
        .unwrap();
        let view = db.fleet_view(&widgets.id).await.unwrap();
        let w2 = view.runners.iter().find(|r| r.name == "w-2").unwrap();
        assert_eq!(
            (
                w2.state.as_str(),
                w2.job_id.as_deref(),
                w2.job_trust.as_deref()
            ),
            ("busy", Some("9"), None)
        );
        // A job of a repository that is not connected here says the same of
        // a runner, and nothing of the job is kept.
        let mut theirs = job(10, JobAction::Completed, Some("g-1"));
        theirs.full_name = "acme/elsewhere".into();
        db.fleet_runner_heard(&theirs).await.unwrap();
        let view = db.fleet_view(&gadgets.id).await.unwrap();
        let g1 = view.runners.iter().find(|r| r.name == "g-1").unwrap();
        assert_eq!(
            (
                g1.state.as_str(),
                g1.job_id.as_deref(),
                g1.reason.as_deref()
            ),
            ("ended", Some("10"), Some(JOB_ENDED))
        );

        // Jobs are told apart by their workflow run, once a run.
        let of_run = |id: u64, run: u64| {
            let mut job = job(id, JobAction::Queued, None);
            job.run_id = run;
            job
        };
        for (id, run, class) in [(20, 100, "dagron"), (21, 100, "dagron"), (22, 101, "other")] {
            db.fleet_job(&widgets, &of_run(id, run), class)
                .await
                .unwrap();
        }
        assert_eq!(
            db.fleet_untold(&widgets.id, &["dagron"], 8).await.unwrap(),
            ["100"]
        );
        assert_eq!(
            db.fleet_untold(&widgets.id, &["dagron", "other"], 1)
                .await
                .unwrap(),
            ["100"]
        );
        db.fleet_told(&widgets.id, "100", UNTRUSTED).await.unwrap();
        db.fleet_told(&widgets.id, "101", TRUSTED).await.unwrap();
        // A job of a run that was told is told as it is written.
        db.fleet_job(&widgets, &of_run(23, 100), "dagron")
            .await
            .unwrap();
        assert!(db
            .fleet_untold(&widgets.id, &["dagron", "other"], 8)
            .await
            .unwrap()
            .is_empty());
        let view = db.fleet_view(&widgets.id).await.unwrap();
        assert_eq!(
            view.queued.get("dagron"),
            Some(&Waiting {
                trusted: 0,
                untrusted: 3,
                unknown: 0
            })
        );
        assert_eq!(view.queued.get("other").map(|w| w.trusted), Some(1));

        // A job that has waited too long is to be asked about; once asked,
        // not again for a while.
        let minute = Duration::from_secs(60);
        assert!(db
            .fleet_unheard(&widgets.id, minute, minute, 10)
            .await
            .unwrap()
            .is_empty());
        exec(
            &db,
            "UPDATE ci_fleet_jobs SET queued_at = '2000-01-01T00:00:00Z' WHERE job_id = '20'",
        )
        .await;
        assert_eq!(
            db.fleet_unheard(&widgets.id, minute, minute, 10)
                .await
                .unwrap(),
            [("20".to_string(), "dagron".to_string())]
        );
        db.fleet_asked("github.com", "20").await.unwrap();
        assert!(db
            .fleet_unheard(&widgets.id, minute, minute, 10)
            .await
            .unwrap()
            .is_empty());
        // One a runner took, whose runner has been over a while.
        db.fleet_job(
            &widgets,
            &job(21, JobAction::InProgress, Some("w-2")),
            "dagron",
        )
        .await
        .unwrap();
        assert!(db.fleet_ended("w-2", "its run failed").await.unwrap());
        exec(
            &db,
            "UPDATE ci_fleet_runners SET ended_at = '2000-01-01T00:00:00Z' WHERE name = 'w-2'",
        )
        .await;
        assert_eq!(
            db.fleet_unheard(&widgets.id, minute, minute, 10)
                .await
                .unwrap(),
            [("21".to_string(), "dagron".to_string())]
        );
        // GitHub has it no more.
        db.fleet_job_gone("github.com", "21").await.unwrap();
        assert!(db
            .fleet_unheard(&widgets.id, minute, minute, 10)
            .await
            .unwrap()
            .is_empty());
    }

    /// The jobs whose runner died, and what came of running them again.
    async fn the_fleet_s_retries(db: Db) {
        let repo = db
            .add_repo("github", "github.com", "acme/widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        let hour = Duration::from_secs(3600);
        for (name, reason) in [
            ("r-1", "its run failed"),
            ("r-2", JOB_ENDED),
            ("r-3", "its cell has no such run"),
        ] {
            assert!(db
                .fleet_want(&repo, "dagron", name, None, TRUSTED, Quota::default())
                .await
                .unwrap());
            db.fleet_launched(name, &format!("run-{name}"))
                .await
                .unwrap();
            assert!(db.fleet_ended(name, reason).await.unwrap());
        }
        exec(
            &db,
            "UPDATE ci_fleet_runners SET launched_at = '2000-01-01T00:00:00Z', \
             ended_at = '2000-01-01T00:10:00Z'",
        )
        .await;
        for (id, runner) in [(1, "r-1"), (2, "r-2"), (3, "r-3")] {
            let mut failed = job(id, JobAction::Completed, Some(runner));
            failed.conclusion = Some("failure".into());
            db.fleet_job(&repo, &failed, "dagron").await.unwrap();
        }
        // One that succeeded on a runner that died is no business of this.
        db.fleet_job(&repo, &job(4, JobAction::Completed, Some("r-1")), "dagron")
            .await
            .unwrap();
        let died = db.fleet_died(&repo.id, hour, 10).await.unwrap();
        assert_eq!(
            died.iter()
                .map(|d| (d.job_id.as_str(), d.retry.as_deref(), d.lived_secs))
                .collect::<Vec<_>>(),
            [("1", None, Some(600)), ("3", None, Some(600))]
        );
        assert_eq!(
            (died[0].workflow_run.as_str(), died[0].name.as_str()),
            ("77", "build")
        );
        assert!(died[0].ended_secs < 60);
        // GitHub said first that r-2's job had ended; its run was seen to
        // fail after, and it died all the same. The first end seen stands.
        db.fleet_run_end("r-2", "failed").await.unwrap();
        db.fleet_run_end("r-2", "succeeded").await.unwrap();
        let died = db.fleet_died(&repo.id, hour, 10).await.unwrap();
        assert_eq!(
            died.iter().map(|d| d.job_id.as_str()).collect::<Vec<_>>(),
            ["1", "2", "3"]
        );
        db.fleet_retry("github.com", "2", "refused", None, Some("seen"))
            .await
            .unwrap();

        // Due, and not looked at again until its time.
        db.fleet_retry(
            "github.com",
            "1",
            "due",
            Some(Duration::from_secs(60)),
            Some("not yet"),
        )
        .await
        .unwrap();
        db.fleet_retry("github.com", "3", "refused", None, Some("no"))
            .await
            .unwrap();
        assert!(db.fleet_died(&repo.id, hour, 10).await.unwrap().is_empty());
        exec(
            &db,
            "UPDATE ci_fleet_jobs SET retry_at = '2000-01-01T00:00:00Z'",
        )
        .await;
        let died = db.fleet_died(&repo.id, hour, 10).await.unwrap();
        assert_eq!(
            (died[0].job_id.as_str(), died[0].retry.as_deref()),
            ("1", Some("due"))
        );
        assert!(!db
            .fleet_retried(&repo.id, "77", "build", "4")
            .await
            .unwrap());
        // GitHub took the ask, and its answer was lost: job 1 is due, and a
        // later attempt of it is its run again. No other of its name is run
        // again for it.
        assert!(!db.fleet_run_again(&repo.id, "1").await.unwrap());
        let mut again = job(5, JobAction::Queued, None);
        again.run_attempt = 2;
        db.fleet_job(&repo, &again, "dagron").await.unwrap();
        assert!(db.fleet_run_again(&repo.id, "1").await.unwrap());
        assert!(
            !db.fleet_run_again(&repo.id, "5").await.unwrap(),
            "the latest"
        );
        assert!(db
            .fleet_retried(&repo.id, "77", "build", "5")
            .await
            .unwrap());
        assert!(!db
            .fleet_retried(&repo.id, "77", "build", "4")
            .await
            .unwrap());
        db.fleet_retry("github.com", "1", "asked", None, None)
            .await
            .unwrap();
        assert!(db.fleet_died(&repo.id, hour, 10).await.unwrap().is_empty());
        assert!(db
            .fleet_retried(&repo.id, "77", "build", "4")
            .await
            .unwrap());
        assert!(
            !db.fleet_retried(&repo.id, "77", "build", "1")
                .await
                .unwrap(),
            "its own"
        );
        assert!(!db.fleet_retried(&repo.id, "77", "test", "4").await.unwrap());
        // Asked stays asked, whatever a look that comes to it after says.
        db.fleet_retry("github.com", "1", "due", None, Some("late"))
            .await
            .unwrap();
        assert!(db.fleet_died(&repo.id, hour, 10).await.unwrap().is_empty());
        // One not yet decided that ended long ago is not listed; one that is
        // due is, whenever it ended.
        exec(&db, "UPDATE ci_fleet_jobs SET finished_at = '2000-01-01T00:00:00Z', retry = NULL WHERE job_id = '3'").await;
        assert!(db.fleet_died(&repo.id, hour, 10).await.unwrap().is_empty());
        db.fleet_retry("github.com", "3", "due", None, None)
            .await
            .unwrap();
        let died = db.fleet_died(&repo.id, hour, 10).await.unwrap();
        assert_eq!(died[0].job_id, "3");
        assert!(died[0].ended_secs > 3600);
    }

    #[tokio::test]
    async fn sqlite_keeps_the_fleet_s_rows() {
        let (db, _dir) = sqlite().await;
        the_fleet_s_rows(db).await;
        let (db, _dir) = sqlite().await;
        the_fleet_s_quotas_trust_and_unheard_jobs(db).await;
        let (db, _dir) = sqlite().await;
        the_fleet_s_retries(db).await;
    }

    #[tokio::test]
    async fn postgres_keeps_the_fleet_s_rows() {
        let Some((db, schema, admin)) = postgres().await else {
            return;
        };
        the_fleet_s_rows(db.clone()).await;
        db.close().await;
        drop_schema(&admin, &schema).await;
        let Some((db, schema, admin)) = postgres().await else {
            return;
        };
        the_fleet_s_quotas_trust_and_unheard_jobs(db.clone()).await;
        db.close().await;
        drop_schema(&admin, &schema).await;
        let Some((db, schema, admin)) = postgres().await else {
            return;
        };
        the_fleet_s_retries(db.clone()).await;
        db.close().await;
        drop_schema(&admin, &schema).await;
    }
}
