//! The scheduler: a GitHub workflow's `on.schedule`, as GitHub runs it. Each
//! GitHub repository is looked at in its turn, as the poll sweep looks at
//! it, where dagron-ci reads workflow files (CI_GITHUB_WORKFLOWS=on):
//!
//! - **Its default branch's head**, from git's ref listing: the branch
//!   `HEAD` names, and the commit it points at. A schedule runs there, as
//!   GitHub's do, and nowhere else.
//! - **Its schedules**, read again once that head has moved: the crons of
//!   each workflow file `.dagron/ci.yaml` lists, at that commit
//!   (`ci_schedules`). A cron the file still has keeps when it last fired.
//! - **What fired**: a cron whose time has come queues a `schedule` event
//!   at the head, naming its workflow file, with the cron as its input
//!   (`github.event.schedule`). Fires missed while dagron-ci was down are
//!   not made up, as GitHub makes none up; one schedule fires at most every
//!   five minutes ([`crate::cron::SHORTEST`]).
//!
//! **Once.** A look's writes commit in one transaction, fenced on its claim,
//! and each fire's event has a key of its own: a cron fires once, however
//! many replicas look.
//!
//! A repository is looked at again when its next cron is due, and at least
//! every [`REFRESH`], to see whether its default branch has moved.

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tokio::sync::{watch, Notify};
use tokio::task::JoinSet;

use crate::cron::Cron;
use crate::db::{Db, DueSchedule, Fired, ScheduleRow};
use crate::forge::{Forge, ForgeError};
use crate::pipeline::github;

/// The longest a repository goes before its default branch is looked at
/// again, whatever its crons say.
pub const REFRESH: Duration = Duration::from_secs(600);

/// How long a scheduler holds a repository it claimed.
const LEASE: Duration = Duration::from_secs(300);

/// The longest one look may take.
const LOOK_TIMEOUT: Duration = Duration::from_secs(120);

/// How long an idle scheduler waits before looking for a due repository.
const IDLE: Duration = Duration::from_secs(5);

/// How long a scheduler waits after the database fails it.
const DB_RETRY: Duration = Duration::from_secs(5);

/// How long a look that failed waits to be tried again.
const FAILED_RETRY: Duration = Duration::from_secs(300);

/// The furthest a forge's rate limit pushes a repository's next look out.
const MAX_RATE_LIMIT_WAIT: Duration = Duration::from_secs(3600);

/// How long a repository with no schedule (or no default branch yet) goes
/// before it is looked at again: one that gains one waits this long at
/// most for it to be read.
const QUIET: Duration = Duration::from_secs(1800);

/// How late a fire may be and still fire: one later (dagron-ci was down,
/// or the forge would not answer) is passed over, as GitHub makes no
/// missed fire up.
pub const GRACE: i64 = 3600;

/// What one look did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Looked {
    /// Events queued, by id: none where nothing was due.
    Queued(Vec<String>),
    /// It failed, for this reason, and is tried again.
    Failed(String),
    /// The claim lapsed and another scheduler took the repository over.
    Lost,
}

pub struct Scheduler {
    db: Db,
    forge: Arc<Forge>,
    /// What time it is, in seconds since the epoch: the clock, or a test's.
    now: fn() -> i64,
}

fn clock() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs() as i64)
        .unwrap_or_default()
}

impl Scheduler {
    pub fn new(db: Db, forge: Arc<Forge>) -> Scheduler {
        Scheduler {
            db,
            forge,
            now: clock,
        }
    }

    #[cfg(test)]
    fn at(mut self, now: fn() -> i64) -> Scheduler {
        self.now = now;
        self
    }

    /// Claim the most overdue repository and look at it, as `claim`. `None`
    /// when none is due.
    pub async fn look_next(&self, claim: &str) -> sqlx::Result<Option<Looked>> {
        let Some(due) = self.db.claim_schedule(claim, LEASE).await? else {
            return Ok(None);
        };
        let looked = match tokio::time::timeout(LOOK_TIMEOUT, self.look(&due, claim)).await {
            Ok(looked) => looked?,
            Err(_) => Err(Failure {
                why: format!("the look took longer than {}s", LOOK_TIMEOUT.as_secs()),
                wait: None,
            }),
        };
        let looked = match looked {
            Ok(Some(ids)) => Looked::Queued(ids),
            Ok(None) => Looked::Lost,
            Err(Failure { why, wait }) => {
                // A forge's rate limit is waited for, as the poll sweep
                // waits for it.
                let next = wait
                    .map(|w| w.min(MAX_RATE_LIMIT_WAIT).max(FAILED_RETRY))
                    .unwrap_or(FAILED_RETRY);
                if self
                    .db
                    .fail_schedule(&due.repo.id, claim, &why, next)
                    .await?
                {
                    Looked::Failed(why)
                } else {
                    Looked::Lost
                }
            }
        };
        log(&due, &looked);
        Ok(Some(looked))
    }

    /// Look at `due`: its head, its schedules, what fired. `Ok(None)` when
    /// the claim was lost; `Err` for what failed.
    async fn look(
        &self,
        due: &DueSchedule,
        claim: &str,
    ) -> sqlx::Result<Result<Option<Vec<String>>, Failure>> {
        let repo = &due.repo;
        // HEAD alone: what it names and the commit it points at, whatever
        // other branches the repository has.
        let refs = match self.forge.list_refs(repo, &[]).await {
            Ok(refs) => refs,
            Err(e) => return Ok(Err(failure(e))),
        };
        let (Some(branch), Some(sha)) = (refs.head, refs.head_id) else {
            // No default branch (an empty repository, or a server that does
            // not say): no schedule, and nothing to say of it.
            return Ok(Ok(self
                .db
                .finish_schedule(&repo.id, claim, None, &[], &[], QUIET)
                .await?));
        };
        let now = (self.now)();
        let kept = self.db.schedules(&repo.id).await?;
        let mut rows = if due.sha.as_deref() == Some(sha.as_str()) {
            kept
        } else {
            match self.read(repo, &sha).await {
                Ok(crons) => carried(&kept, crons, now),
                Err(e) => return Ok(Err(e)),
            }
        };
        let fired = fire(&mut rows, now, &branch, &sha);
        let next = match rows
            .iter()
            .filter_map(|r| r.next_at)
            .map(|at| Duration::from_secs((at - now).max(1) as u64))
            .min()
        {
            Some(next) => next.min(REFRESH),
            None => QUIET,
        };
        Ok(Ok(self
            .db
            .finish_schedule(&repo.id, claim, Some(&sha), &rows, &fired, next)
            .await?))
    }

    /// The crons of each workflow file the repository's settings list, at
    /// `sha`: `(file name, cron)`. A file that cannot be read, or read as a
    /// workflow, has none here: the commit's push says what is wrong with it.
    async fn read(
        &self,
        repo: &crate::db::Repo,
        sha: &str,
    ) -> Result<Vec<(String, String)>, Failure> {
        let settings = match self.forge.file(repo, github::SETTINGS_FILE, sha).await {
            Ok(None) => return Ok(Vec::new()),
            Ok(Some(Ok(text))) => match github::settings(&text) {
                Ok(settings) => settings,
                Err(_) => return Ok(Vec::new()),
            },
            Ok(Some(Err(_))) => return Ok(Vec::new()),
            Err(e) => return Err(failure(e)),
        };
        let mut crons = Vec::new();
        for name in &settings.workflows {
            let path = format!("{}/{name}", github::WORKFLOW_DIR);
            let text = match self.forge.file(repo, &path, sha).await {
                Ok(Some(Ok(text))) => text,
                Ok(_) => continue,
                Err(e) => return Err(failure(e)),
            };
            if let Ok(workflow) = github::parse(&text, &settings.provided) {
                for cron in &workflow.on.schedule {
                    crons.push((name.clone(), cron.clone()));
                }
            }
        }
        Ok(crons)
    }
}

/// The schedules read now (`crons`), each keeping when it last fired and
/// next fires where it was already kept; a new one next fires at its first
/// time after `now`.
fn carried(kept: &[ScheduleRow], crons: Vec<(String, String)>, now: i64) -> Vec<ScheduleRow> {
    let kept: BTreeMap<(&str, &str), &ScheduleRow> = kept
        .iter()
        .map(|r| ((r.workflow.as_str(), r.cron.as_str()), r))
        .collect();
    let mut rows: Vec<ScheduleRow> = crons
        .into_iter()
        .map(
            |(workflow, cron)| match kept.get(&(workflow.as_str(), cron.as_str())) {
                Some(row) => (*row).clone(),
                None => ScheduleRow {
                    next_at: Cron::parse(&cron).ok().and_then(|c| c.next(now, None)),
                    last_at: None,
                    workflow,
                    cron,
                },
            },
        )
        .collect();
    rows.sort_by(|a, b| (&a.workflow, &a.cron).cmp(&(&b.workflow, &b.cron)));
    rows.dedup_by(|a, b| a.workflow == b.workflow && a.cron == b.cron);
    rows
}

/// Fire each row whose time has come, once, and move it to its next time
/// after `now`: what fired, as events at `branch`'s head `sha`. A time
/// more than [`GRACE`] past is passed over, not fired.
fn fire(rows: &mut [ScheduleRow], now: i64, branch: &str, sha: &str) -> Vec<Fired> {
    let mut fired = Vec::new();
    for row in rows.iter_mut() {
        let Some(at) = row.next_at.filter(|at| *at <= now) else {
            continue;
        };
        if now - at > GRACE {
            row.next_at = Cron::parse(&row.cron)
                .ok()
                .and_then(|c| c.next(now, row.last_at));
            continue;
        }
        fired.push(Fired {
            workflow: row.workflow.clone(),
            cron: row.cron.clone(),
            at,
            git_ref: branch.to_string(),
            sha: sha.to_string(),
        });
        row.last_at = Some(at);
        row.next_at = Cron::parse(&row.cron)
            .ok()
            .and_then(|c| c.next(now, Some(at)));
    }
    fired
}

/// Why a look failed, and how long the forge said to wait, where it did.
#[derive(Debug)]
struct Failure {
    why: String,
    wait: Option<Duration>,
}

fn failure(e: ForgeError) -> Failure {
    match e {
        ForgeError::RateLimited { wait, why } => Failure {
            why,
            wait: Some(wait),
        },
        ForgeError::Failed(why) => Failure { why, wait: None },
    }
}

fn log(due: &DueSchedule, looked: &Looked) {
    let repo = format!("{}/{}", due.repo.forge_host, due.repo.full_name);
    match looked {
        Looked::Queued(ids) if ids.is_empty() => tracing::debug!(repo, "no schedule was due"),
        Looked::Queued(ids) => tracing::info!(repo, events = ids.len(), "schedules fired"),
        Looked::Failed(why) => tracing::warn!(repo, reason = %why, "looking at schedules failed"),
        Looked::Lost => tracing::warn!(
            repo,
            "a look at schedules outlived its lease; another scheduler has the repository"
        ),
    }
}

/// Start this replica's scheduler. It runs until `stop` turns true.
pub fn spawn(
    scheduler: Arc<Scheduler>,
    wake: Arc<Notify>,
    stop: watch::Receiver<bool>,
) -> JoinSet<()> {
    let mut set = JoinSet::new();
    let replica = uuid::Uuid::now_v7().simple().to_string();
    let claim = format!("{}-schedule", &replica[replica.len() - 12..]);
    set.spawn(run(claim, scheduler, wake, stop));
    set
}

async fn run(
    claim: String,
    scheduler: Arc<Scheduler>,
    wake: Arc<Notify>,
    mut stop: watch::Receiver<bool>,
) {
    while !*stop.borrow() {
        let looked = tokio::select! {
            looked = scheduler.look_next(&claim) => looked,
            _ = stop.changed() => break,
        };
        let pause = match looked {
            Ok(Some(Looked::Queued(ids))) => {
                for _ in 0..ids.len().min(64) {
                    wake.notify_one();
                }
                continue;
            }
            Ok(Some(_)) => continue,
            Ok(None) => IDLE,
            Err(e) => {
                tracing::warn!(scheduler = %claim, error = %e, "scheduler: the database failed");
                DB_RETRY
            }
        };
        tokio::select! {
            _ = tokio::time::sleep(pause) => {}
            _ = stop.changed() => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// 2026-10-17T00:00:00Z.
    const T0: i64 = 1_792_195_200;

    fn row(workflow: &str, cron: &str, next_at: Option<i64>, last_at: Option<i64>) -> ScheduleRow {
        ScheduleRow {
            workflow: workflow.into(),
            cron: cron.into(),
            next_at,
            last_at,
        }
    }

    #[test]
    fn a_schedule_read_again_keeps_when_it_fired() {
        let kept = vec![
            row(
                "nightly.yml",
                "0 3 * * *",
                Some(T0 + 3 * 3600),
                Some(T0 - 21 * 3600),
            ),
            row("gone.yml", "*/5 * * * *", Some(T0 + 300), None),
        ];
        let rows = carried(
            &kept,
            vec![
                ("nightly.yml".into(), "0 3 * * *".into()),
                ("weekly.yml".into(), "0 0 * * 1".into()),
                ("weekly.yml".into(), "0 0 * * 1".into()),
            ],
            T0,
        );
        assert_eq!(
            rows,
            [
                row(
                    "nightly.yml",
                    "0 3 * * *",
                    Some(T0 + 3 * 3600),
                    Some(T0 - 21 * 3600)
                ),
                // Monday the 19th, at midnight.
                row("weekly.yml", "0 0 * * 1", Some(T0 + 2 * 86_400), None),
            ],
            "a cron the files no longer have is dropped; a new one is due at its first time"
        );
    }

    /// Make the repository due to be looked at again now.
    async fn due_now(db: &Db) {
        let sql = "UPDATE ci_repos SET schedule_due_at = NULL";
        match db {
            Db::Postgres(p) => sqlx::query(sql).execute(p).await.map(|_| ()),
            Db::Sqlite(p) => sqlx::query(sql).execute(p).await.map(|_| ()),
        }
        .unwrap();
    }

    /// A workflow file's schedule, read at the head of the default branch
    /// (what HEAD names, not another branch), fires there once its time has
    /// come: one trusted `schedule` event naming its file, with its cron.
    /// The same look again fires nothing more, and a branch that moved has
    /// its schedules read again.
    #[tokio::test]
    async fn a_schedule_fires_once_at_the_default_branch_s_head() {
        const MAIN: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        const TOPIC: &str = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb";
        const MOVED: &str = "cccccccccccccccccccccccccccccccccccccccc";
        let (db, _dir) = crate::db::tests::sqlite().await;
        let (origin, stub) = crate::poll::tests::stub_forge().await;
        let nightly = "on:\n  schedule:\n    - cron: '*/15 * * * *'\njobs:\n  a:\n    \
                       runs-on: ubuntu-latest\n    steps: [{ run: x }]\n";
        {
            let mut f = stub.lock().unwrap();
            f.refs = BTreeMap::from([
                ("refs/heads/main".to_string(), MAIN.to_string()),
                ("refs/heads/topic".to_string(), TOPIC.to_string()),
            ]);
            f.head = Some("refs/heads/main".into());
            f.files = BTreeMap::from([
                (
                    ".dagron/ci.yaml".to_string(),
                    "github:\n  workflows: [nightly.yml]\n".to_string(),
                ),
                (
                    ".github/workflows/nightly.yml".to_string(),
                    nightly.to_string(),
                ),
            ]);
        }
        let repo = db
            .add_repo("github", "github.com", "acme/widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        let forge = Arc::new(Forge::new(None).with_origin(&origin));

        // Read: nothing is due yet.
        let first = Scheduler::new(db.clone(), forge.clone()).at(|| T0);
        assert_eq!(
            first.look_next("s").await.unwrap(),
            Some(Looked::Queued(Vec::new()))
        );
        assert_eq!(
            db.schedules(&repo.id).await.unwrap(),
            [row("nightly.yml", "*/15 * * * *", Some(T0 + 900), None)]
        );
        assert_eq!(
            first.look_next("s").await.unwrap(),
            None,
            "not due again yet"
        );

        // Its time came: it fires once, at main's head.
        due_now(&db).await;
        let later = Scheduler::new(db.clone(), forge.clone()).at(|| T0 + 1200);
        let Some(Looked::Queued(ids)) = later.look_next("s").await.unwrap() else {
            panic!("a look");
        };
        assert_eq!(ids.len(), 1);
        let ev = db
            .claim_event("w", Duration::from_secs(60))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            (
                ev.kind.as_str(),
                ev.git_ref.as_str(),
                ev.sha.as_str(),
                ev.trust.as_str(),
                ev.pipeline.as_deref(),
            ),
            (
                "schedule",
                "refs/heads/main",
                MAIN,
                "trusted",
                Some(".github/workflows/nightly.yml")
            )
        );
        assert_eq!(
            ev.inputs
                .get(crate::pipeline::compile::SCHEDULE_INPUT)
                .map(String::as_str),
            Some("*/15 * * * *")
        );
        due_now(&db).await;
        assert_eq!(
            later.look_next("s").await.unwrap(),
            Some(Looked::Queued(Vec::new())),
            "the same time fires nothing more"
        );

        // main moved, and its workflow no longer has the schedule.
        {
            let mut f = stub.lock().unwrap();
            f.refs.insert("refs/heads/main".into(), MOVED.into());
            f.files.insert(
                ".github/workflows/nightly.yml".into(),
                "on: push\njobs:\n  a:\n    runs-on: ubuntu-latest\n    steps: [{ run: x }]\n"
                    .into(),
            );
        }
        due_now(&db).await;
        let moved = Scheduler::new(db.clone(), forge.clone()).at(|| T0 + 3600);
        assert_eq!(
            moved.look_next("s").await.unwrap(),
            Some(Looked::Queued(Vec::new()))
        );
        assert!(db.schedules(&repo.id).await.unwrap().is_empty());
    }

    /// A repository whose ref listing names no default branch (an empty
    /// one) has no schedule, and is no failure: it is looked at again
    /// later, quietly.
    #[tokio::test]
    async fn a_repository_with_no_default_branch_has_no_schedule() {
        let (db, _dir) = crate::db::tests::sqlite().await;
        let (origin, _stub) = crate::poll::tests::stub_forge().await;
        let repo = db
            .add_repo("github", "github.com", "acme/widgets", None, "default")
            .await
            .unwrap()
            .unwrap();
        let forge = Arc::new(Forge::new(None).with_origin(&origin));
        let scheduler = Scheduler::new(db.clone(), forge).at(|| T0);
        assert_eq!(
            scheduler.look_next("s").await.unwrap(),
            Some(Looked::Queued(Vec::new()))
        );
        assert!(db.schedules(&repo.id).await.unwrap().is_empty());
        assert_eq!(
            scheduler.look_next("s").await.unwrap(),
            None,
            "not due again yet"
        );
    }

    #[test]
    fn a_cron_whose_time_has_come_fires_once_and_moves_on() {
        let mut rows = vec![
            row("a.yml", "*/15 * * * *", Some(T0 - 1800), None),
            row("b.yml", "0 3 * * *", Some(T0 + 3 * 3600), None),
            row("c.yml", "0 0 31 2 *", None, None),
            row(
                "d.yml",
                "0 22 * * *",
                Some(T0 - 2 * 3600),
                Some(T0 - 26 * 3600),
            ),
        ];
        let fired = fire(&mut rows, T0 + 10, "refs/heads/main", "s");
        // a.yml was due half an hour ago: it fires once, not twice, as
        // GitHub makes no missed fire up. d.yml was due two hours ago, past
        // the grace: it is passed over, and moves on.
        assert_eq!(fired.len(), 1);
        assert_eq!(
            (
                fired[0].workflow.as_str(),
                fired[0].at,
                fired[0].git_ref.as_str()
            ),
            ("a.yml", T0 - 1800, "refs/heads/main")
        );
        assert_eq!(rows[0].last_at, Some(T0 - 1800));
        assert_eq!(rows[0].next_at, Some(T0 + 15 * 60));
        assert_eq!(rows[1].next_at, Some(T0 + 3 * 3600), "not due");
        assert_eq!(
            (rows[3].next_at, rows[3].last_at),
            (Some(T0 + 22 * 3600), Some(T0 - 26 * 3600))
        );
        // Each fire is a run of its own.
        let again = Fired {
            at: T0 + 15 * 60,
            ..fired[0].clone()
        };
        assert_ne!(fired[0].key("r"), again.key("r"));
    }
}
