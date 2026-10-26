//! A limit on the runs dagron-ci has in flight, for a build that sets one.
//!
//! The open build sets none: a run is submitted as soon as its event is
//! handled. A build that composes this crate can pass an [`Admission`] in its
//! [`crate::Seams`]; a worker then asks it before each run it submits, with
//! the number of runs dagron-ci submitted that have not ended. Over the limit
//! the event waits, as it waits behind a full concurrency key: no attempt is
//! spent, it is asked again later, and it never fails for it.
//!
//! **Counted once.** Admission takes a slot for the run in `ci_admissions`,
//! under its idempotency key, in the same transaction that counts. The slot
//! counts while the run is submitted; recording the run deletes it, and from
//! then on the run itself counts, until the reporter sees it end. Two workers
//! cannot both take the last slot: the count and the slot are taken under a
//! lock (Postgres) or SQLite's single writer.
//!
//! **By whom.** A build may count each run for its caller too: the actor
//! its event names (a forge's user, or an agent's `agent:<name>`), which its
//! slot keeps, and a run its event's. So it can hold one caller to fewer runs
//! than the whole (an agent, say), and one caller's many runs do not hold
//! back another's ([`Admission::admit_caller`]). The open build holds no
//! caller to any.
//!
//! **Not counted.** A run recorded before the reporter existed, which nothing
//! follows to its end; and a run dagron-api created for a worker that stopped
//! before recording it, once its slot lapses ([`HOLD`]) and until the event's
//! retry records it.

use std::collections::BTreeMap;
use std::time::Duration;

/// How long a slot holds without its run being recorded: long enough for the
/// submit and the record, and for a worker that stopped in between to have
/// its event retried. A lapsed slot no longer counts.
pub const HOLD: Duration = Duration::from_secs(120);

/// Whether one more run may start.
pub trait Admission: Send + Sync + 'static {
    /// Whether a run may start while `active` runs dagron-ci submitted have
    /// not ended. `Err` holds the event until a run ends; its text is the
    /// event's reason, so it says why and what frees the run.
    ///
    /// It is asked inside the transaction that takes the slot, under a lock
    /// every admission waits on: it must answer at once, from memory.
    fn admit(&self, active: u64) -> Result<(), String>;

    /// Whether a run of `repo` (`forge host/owner/name`) may start, the runs
    /// active now counted as `load`. By default, [`Admission::admit`] with
    /// their total. Asked as [`Admission::admit`] is: at once, from memory.
    fn admit_run(&self, repo: &str, load: &Load) -> Result<(), String> {
        let _ = repo;
        self.admit(load.active)
    }

    /// Whether [`Admission::admit_run`] reads `load.by_repo`. Counting by
    /// repository reads the repository of each run that counts, which a
    /// total does not, so it is done only where it is read.
    fn by_repository(&self) -> bool {
        false
    }

    /// Whether a run of `repo` that `caller` asked for (its event's actor,
    /// where it names one), of trust class `trust`, may start, the runs
    /// active now counted as `load`. By default, [`Admission::admit_run`]. A
    /// build that holds a caller to fewer runs than the whole (an agent, say)
    /// says so here. Asked as [`Admission::admit`] is: at once, from memory.
    fn admit_caller(
        &self,
        repo: &str,
        caller: Option<&str>,
        trust: &str,
        load: &Load,
    ) -> Result<(), String> {
        let _ = (caller, trust);
        self.admit_run(repo, load)
    }

    /// Whether [`Admission::admit_caller`] reads `load.by_caller`, which is
    /// counted only where it is read, as `by_repo` is.
    fn by_caller(&self) -> bool {
        false
    }
}

/// The runs that count against a limit when a run asks to start: those
/// dagron-ci submitted that have not ended, and the slots of runs being
/// submitted, but for the run asking.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Load {
    pub active: u64,
    /// The same runs by repository (`forge host/owner/name`), where the
    /// admission reads them ([`Admission::by_repository`]); empty otherwise.
    /// A slot taken before slots named their repository is under `""`.
    pub by_repo: BTreeMap<String, u64>,
    /// The same runs by caller (the actor their event names), where the
    /// admission reads them ([`Admission::by_caller`]); empty otherwise. A run
    /// or a slot with no caller is under `""`.
    pub by_caller: BTreeMap<String, u64>,
}
