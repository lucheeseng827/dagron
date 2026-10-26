//! What a run that is not trusted is held to.
//!
//! A workflow's author has the engine's ear. A task can have the engine call
//! an address for it (`defer`, a `wait` sensor), start another workflow, ask
//! a person for a decision, or update a dataset that other workflows start
//! on. A workflow can have the engine post to a webhook, or to a forge with
//! the engine's own token (`notify`). That was right while a workflow's
//! author was someone the installation had let in. A pull request from a
//! fork is not: its pipeline is its author's, and the run made for it is
//! recorded as `untrusted` ([`crate::envpolicy::class_of`]).
//!
//! So a run of a class that is not `trusted` is **confined**: it runs its own
//! containers, and the engine does nothing else on its word.
//!
//! - [`refusal`] names what such a run may not use. It is asked where every
//!   run is made (`db::create_run*`), so a run that names one is not made,
//!   and again of each task as it is dispatched ([`task_refusal`]).
//! - [`withhold`] takes from a task what such a run is not given, and does
//!   not fail for: the output cache, which every class would share; a slot of
//!   a named pool; a place ahead in the queue; the attempts its own output
//!   could buy it.
//! - [`Limits`] is what such a run may spend, which its author would
//!   otherwise write: a task's time and its attempts, how many tasks a run
//!   has, how many services a task has beside it, and what a container made
//!   for one may use. The operator says it.
//!
//! The lists below sort **every** key a workflow may write, and a test holds
//! them to `TaskSpec` and `DagSpec`: a key added to either has to be put in
//! one of them before this crate's tests pass. A key is allowed where it only
//! shapes which of the run's own containers start, and when.
//!
//! What this is not: the envelope a confined task's container runs in. That
//! is the engine's floor for the class and its executor's to hold, and what
//! secrets it may read is its environments' policies'
//! ([`crate::envpolicy`]).

use crate::dag::{DagGraph, DagSpec, TaskSpec};
use crate::envpolicy::TRUSTED;
use crate::isolation::{ExecutorKind, IsolationSpec, Network};

/// Whether a run of this trust class is confined: every class but `trusted`.
pub fn confined(class: &str) -> bool {
    class != TRUSTED
}

/// The engine's floor for the tasks of a confined run, in the spelling of
/// `DAGRON_TASK_ISOLATION_FLOOR`. An engine given none runs no such task.
pub const ENV_FLOOR: &str = "DAGRON_UNTRUSTED_ISOLATION_FLOOR";

/// What each task of such a run is held to, and what the run may spend
/// ([`Limits`]):
/// `memory=4g,pids=2048,cpus=2,timeout=3600,attempts=3,tasks=256,services=4`.
pub const ENV_LIMITS: &str = "DAGRON_UNTRUSTED_LIMITS";

/// Why an engine cannot run confined work with this executor and this floor
/// for it, where it cannot. Asked as the engine starts, so that an engine
/// told to run such work either can or does not start.
///
/// - **Local**: never. A task is the engine's own process, on its host.
/// - **Docker**: the engine names the network its containers are made on
///   (`names_a_network`). They join the daemon's default one otherwise,
///   beside whatever else that host runs.
/// - **Kubernetes**: the floor says what such a pod may reach, `none` or an
///   allowlist of the operator's. A pod has its namespace's whole network
///   otherwise.
pub fn engine_refusal(
    kind: ExecutorKind,
    floor: &IsolationSpec,
    names_a_network: bool,
) -> Result<(), String> {
    match kind {
        ExecutorKind::Local => Err(
            "it runs tasks as its own processes (EXECUTOR=local), and nothing stands between \
             such a task and the engine's host"
                .to_string(),
        ),
        ExecutorKind::Docker if !names_a_network => Err(
            "on Docker it has to name the network its containers are made on \
             (DAGRON_DOCKER_NETWORK): they join the daemon's default one otherwise, beside \
             whatever else the host runs"
                .to_string(),
        ),
        ExecutorKind::Docker => Ok(()),
        ExecutorKind::Kubernetes => match floor.network {
            Some(Network::None) | Some(Network::Allow(_)) => Ok(()),
            _ => Err(
                "on Kubernetes the floor has to say what such a pod may reach: `network=none`, \
                 or `network=allow:<name>` for an allowlist of the operator's"
                    .to_string(),
            ),
        },
    }
}

/// What a task of a confined run is failed with on an engine that runs no
/// such work.
pub fn not_run_here(class: &str) -> String {
    format!(
        "this run is {class}, and this engine runs no work that is not trusted: it is given \
         no floor for it ({ENV_FLOOR})"
    )
}

/// The images a confined run's tasks may name, as an operator lists them:
/// [`parse_images`]. With none listed, any.
pub const ENV_IMAGES: &str = "DAGRON_UNTRUSTED_IMAGES";

/// The images an operator listed, comma-separated patterns:
/// `docker.io/library/*,ghcr.io/acme/**`. In a pattern `*` is any run of
/// characters up to a `/`, `**` is any run of characters, and `?` is one
/// character ([`crate::envpolicy::matches`]). Nothing written is no list.
pub fn parse_images(raw: &str) -> Result<Vec<String>, String> {
    let listed: Vec<String> = raw
        .split(',')
        .map(str::trim)
        .filter(|pattern| !pattern.is_empty())
        .map(str::to_string)
        .collect();
    for pattern in &listed {
        if pattern.chars().count() > 512
            || pattern.chars().any(|c| c.is_whitespace() || c.is_control())
        {
            return Err(format!("'{pattern}' is not an image pattern"));
        }
    }
    Ok(listed)
}

/// Why a task of a confined run may not run in the image it names, or
/// beside a service in the image that names: the operator listed the images
/// such runs may name, and it is none of them.
///
/// A task's image is its author's to write, and an executor runs what its
/// host can run. On Docker a private image is one the host already holds, so
/// a task that named it would read it. A service's image is as much its
/// author's, and is run the same.
///
/// The image is matched as the task wrote it: `alpine:3.20` and
/// `docker.io/library/alpine:3.20` are two names here, and a list takes the
/// ones it spells. A task that names no image runs in the engine's own
/// default, which is the operator's choice and is not asked.
pub fn image_refusal(class: &str, allowed: &[String], task: &TaskSpec) -> Option<String> {
    if allowed.is_empty() {
        return None;
    }
    let listed = |image: &str| {
        allowed
            .iter()
            .any(|pattern| crate::envpolicy::matches(pattern, image))
    };
    if let Some(image) = task.docker_image.as_deref().filter(|image| !listed(image)) {
        return Some(format!(
            "this run is {class}, and its task names the image '{image}', which is none of \
             those a run that is not trusted may name ({ENV_IMAGES})"
        ));
    }
    let service = task
        .services
        .iter()
        .find(|service| !listed(&service.image))?;
    Some(format!(
        "this run is {class}, and its task's service '{}' names the image '{}', which is none \
         of those a run that is not trusted may name ({ENV_IMAGES})",
        service.name, service.image
    ))
}

/// The pool every task of a confined run draws from, whatever pool it named.
/// An operator who caps it (`POOLS=untrusted:4`) caps how many such tasks run
/// at once on an engine. Uncapped, as any pool is, until they do.
pub const POOL: &str = "untrusted";

/// What an engine holds each task of a confined run to, beyond its envelope:
/// what a container made for it may use, and what the run may spend. The
/// operator writes it as [`ENV_LIMITS`]; none of it can be turned off.
///
/// A workflow's author writes its timeouts, its attempts and how many tasks
/// it fans out to, which is right while the author is the installation's
/// own. A confined run's author is not, so each is the lesser of what they
/// wrote and what the operator allows.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Limits {
    /// The most memory a container made for the task may use, in bytes. It is
    /// given no swap beyond it.
    pub memory_bytes: i64,
    /// The most processes and threads in one.
    pub pids: i64,
    /// The most CPU time one may use, in billionths of a CPU.
    pub nano_cpus: i64,
    /// The longest a task may run, in seconds. A task that names no timeout
    /// has this one.
    pub timeout_secs: u64,
    /// The most attempts a task is given.
    pub max_attempts: u32,
    /// The most tasks a run may have. One with more has none of them run:
    /// a confined run is the size it was made ([`task_refusal`] refuses
    /// what would grow it), so every task of it is asked the same number.
    pub max_tasks: i64,
    /// The most services a task may have beside it. Each is a container of
    /// its own, held to the same memory, processes and CPU as the task's:
    /// what a task may use is that many times over, and once more.
    pub max_services: usize,
}

impl Default for Limits {
    /// 4 GiB, 2048 processes, 2 CPUs; an hour a task, three attempts, 256
    /// tasks a run, and four services a task.
    fn default() -> Self {
        Self {
            memory_bytes: 4 << 30,
            pids: 2048,
            nano_cpus: 2_000_000_000,
            timeout_secs: 3600,
            max_attempts: 3,
            max_tasks: 256,
            max_services: 4,
        }
    }
}

impl Limits {
    /// The limits an operator wrote: `memory=4g,pids=2048,cpus=2,timeout=3600,
    /// attempts=3,tasks=256,services=4`, any of them, the rest as
    /// [`Limits::default`]. Memory takes `k`, `m` or `g` (powers of 1024) or
    /// a count of bytes; `cpus` may be a fraction; `timeout` is in seconds.
    ///
    /// None can be turned off: a limit of nothing is refused, and so is an
    /// unknown key, since a limit with a typo in it is one that is not held.
    /// `services=0` is a limit, and the tightest: such a task has none.
    pub fn parse(raw: &str) -> Result<Self, String> {
        let mut out = Self::default();
        for entry in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let (key, value) = entry
                .split_once('=')
                .ok_or_else(|| format!("'{entry}' is not key=value"))?;
            let (key, value) = (key.trim(), value.trim());
            let count = |least: i64, most: i64, what: &str| -> Result<i64, String> {
                value
                    .parse::<i64>()
                    .ok()
                    .filter(|n| (least..=most).contains(n))
                    .ok_or_else(|| format!("{key} expects {what}, got '{value}'"))
            };
            match key {
                "memory" => {
                    let (digits, unit) = match value.chars().last() {
                        Some('k' | 'K') => (&value[..value.len() - 1], 1i64 << 10),
                        Some('m' | 'M') => (&value[..value.len() - 1], 1 << 20),
                        Some('g' | 'G') => (&value[..value.len() - 1], 1 << 30),
                        _ => (value, 1),
                    };
                    out.memory_bytes = digits
                        .parse::<i64>()
                        .ok()
                        .and_then(|n| n.checked_mul(unit))
                        // Docker refuses a container less than 6 MiB.
                        .filter(|bytes| *bytes >= 6 << 20)
                        .ok_or_else(|| {
                            format!("memory expects a size of 6m or more (512m, 4g), got '{value}'")
                        })?;
                }
                "pids" => out.pids = count(1, i64::MAX, "a count of 1 or more")?,
                "cpus" => {
                    out.nano_cpus = value
                        .parse::<f64>()
                        .ok()
                        .filter(|n| n.is_finite() && *n >= 0.01 && *n <= 4096.0)
                        .map(|n| (n * 1e9).round() as i64)
                        .ok_or_else(|| {
                            format!(
                                "cpus expects a number of CPUs of 0.01 or more (0.5, 2), got '{value}'"
                            )
                        })?;
                }
                "timeout" => {
                    out.timeout_secs =
                        count(1, 7 * 24 * 3600, "seconds, from 1 to a week's")? as u64;
                }
                "attempts" => out.max_attempts = count(1, 100, "a count from 1 to 100")? as u32,
                "tasks" => out.max_tasks = count(1, 100_000, "a count from 1 to 100000")?,
                "services" => {
                    let most = crate::service::MAX_SERVICES as i64;
                    out.max_services = count(0, most, "a count from 0 to 8")? as usize;
                }
                other => {
                    return Err(format!(
                        "unknown limit '{other}': expected memory, pids, cpus, timeout, \
                         attempts, tasks or services"
                    ))
                }
            }
        }
        Ok(out)
    }

    /// Hold a task to what it may spend: its timeout and its attempts are the
    /// lesser of its own and these. A task that names no timeout has this
    /// one, where it had none.
    pub fn spend(&self, task: &mut TaskSpec) {
        task.timeout_secs = Some(
            task.timeout_secs
                .map_or(self.timeout_secs, |own| own.min(self.timeout_secs)),
        );
        task.max_attempts = task.max_attempts.clamp(1, self.max_attempts);
    }

    /// Why a run of this many tasks has none of them run, where it has more
    /// than it may.
    pub fn too_many(&self, class: &str, tasks: i64) -> Option<String> {
        (tasks > self.max_tasks).then(|| {
            format!(
                "this run is {class}, and has {tasks} tasks: a run that is not trusted may \
                 have {} ({ENV_LIMITS}, tasks)",
                self.max_tasks
            )
        })
    }

    /// Why a task with this many services is not run, where it has more than
    /// it may.
    pub fn too_many_services(&self, class: &str, task: &TaskSpec) -> Option<String> {
        let services = task.services.len();
        (services > self.max_services).then(|| {
            format!(
                "this run is {class}, and its task has {services} services: a task of a run \
                 that is not trusted may have {} ({ENV_LIMITS}, services)",
                self.max_services
            )
        })
    }
}

/// The words every [`ConfinedRefused`] is written with. A caller that has
/// only the text of one knows it by them, as it knows an environment's
/// refusal by [`crate::envpolicy::REFUSAL_MARK`].
pub const REFUSAL_MARK: &str = "a run that is not trusted may not use";

/// A confined run named something it may not use.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfinedRefused {
    /// The run's trust class.
    pub class: String,
    /// What it named, and where: `` `defer` (task 'build') ``.
    pub what: String,
    /// Why a confined run may not have it.
    pub why: &'static str,
}

impl std::fmt::Display for ConfinedRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "this run is {}, and {REFUSAL_MARK} {}: {}",
            self.class, self.what, self.why
        )
    }
}

impl std::error::Error for ConfinedRefused {}

/// A task's keys that only shape which of the run's own containers start,
/// and when. A confined run may write them.
///
/// `isolation` is raised to the engine's floor for the class, `environment`
/// and a `value_from` in `env` are their environment's policy's to allow,
/// and `runner_class` chooses among engines that each hold the class to
/// their own floor, or run none of it. `services` are more of the run's own
/// containers: each is held as the task's is, its image is asked as the
/// task's is ([`image_refusal`]), and how many a task has is the
/// operator's to say ([`Limits::max_services`]).
pub const TASK_ALLOWED: &[&str] = &[
    "allow_failure",
    "arguments",
    // Its own run's artifacts, and its own task's to write.
    "artifact_api",
    "command",
    "depends_on",
    "docker_image",
    "env",
    "env_from",
    "environment",
    // The token service gives a run that is not trusted read alone.
    "forge_token",
    "hook",
    "image_pull_policy",
    "input",
    "instance_key",
    "isolation",
    "max_attempts",
    "name",
    "resources",
    "retry_delay_secs",
    "retry_max_delay_secs",
    "retry_on_timeout",
    "runner_class",
    "services",
    "template",
    "timeout_secs",
    // A reservation of a token budget: it holds the task, and gives it nothing.
    "tokens",
    "trigger_rule",
    "when",
    "with_items",
    "with_param",
    "workspace",
];

/// A task's keys that a confined run may not use: [`task_refusal`] says why
/// of each. `type` is refused for every kind but an ordinary task, and the
/// approval's own keys are an approval's alone.
pub const TASK_REFUSED: &[&str] = &[
    "approval_message",
    "approval_on_timeout",
    "approval_show",
    "approval_timeout_secs",
    "approvers",
    "binds",
    "defer",
    "docker_daemon",
    "gang",
    "gang_member",
    "identity",
    "not_triggerer",
    "privileged",
    "produces",
    "repeat",
    "service_account",
    "type",
    "wait",
    "with_output_of",
    "workflow",
];

/// A task's keys that a confined run is not given, and does not fail for:
/// [`withhold`] takes them away.
pub const TASK_WITHHELD: &[&str] = &["cache", "pool", "priority", "retry_budgets"];

/// A workflow's own keys that a confined run may write.
///
/// `max_active_runs` and `concurrency_key` count by the workflow's name,
/// which is its author's to write. A confined run's are its own to set, and
/// are counted among confined runs ([`hold`]): it holds no slot that a
/// workflow of the same name wanted.
///
/// `when_full` is its own to set too, and `supersede` is held where the run
/// is made (`create_run`): a confined run takes the place of the runs of
/// its own source, the same repository and pull request or ref, and of no
/// other. The key is its author's to write, and a run that named another
/// pull request's would otherwise cancel that one's.
pub const SPEC_ALLOWED: &[&str] = &[
    "budget",
    "concurrency_key",
    "deadline",
    "environment",
    "max_active_runs",
    "name",
    "param_schema",
    "parameters",
    "result_from",
    "run_timeout_secs",
    "runner_class",
    "tags",
    "task_defaults",
    "tasks",
    "templates",
    "use",
    "when_full",
];

/// A workflow's own keys that a confined run may not use.
pub const SPEC_REFUSED: &[&str] = &["datasets_mode", "notify", "on_datasets"];

/// What a task names that a confined run of `class` may not use, and why: the
/// first of them, as `(what, why)`. `None` for a task such a run may have.
///
/// An `identity` is refused to every such run but an agent's, whose task may
/// hold one where its environment's policy says so
/// ([`crate::envpolicy::identity_admits`]): the engine asks that at dispatch,
/// where the policy is read.
pub fn task_refusal(task: &TaskSpec, class: &str) -> Option<(String, &'static str)> {
    let named = |key: &str| format!("`{key}` (task '{}')", task.name);
    if let Some(kind) = task.task_type.as_deref().filter(|kind| *kind != "task") {
        let why = match kind {
            "approval" => "it asks a decision of this installation's people",
            "workflow" => "it starts a workflow that is registered here",
            "wait" => "the engine itself would watch for it",
            _ => "only an ordinary task is run for it",
        };
        return Some((format!("`type: {kind}` (task '{}')", task.name), why));
    }
    if task.service_account.is_some() {
        return Some((
            named("service_account"),
            "it is an identity of this installation's",
        ));
    }
    if task.identity.is_some() && class != crate::envpolicy::AGENT {
        return Some((
            named("identity"),
            "a token a cloud trusts is given to trusted runs, and to an agent's where its \
             environment's policy says so",
        ));
    }
    if task.docker_daemon {
        return Some((
            named("docker_daemon"),
            "the daemon's container is privileged, or root in a user namespace of its own",
        ));
    }
    if task.privileged {
        return Some((
            named("privileged"),
            "a privileged container is root on its host",
        ));
    }
    if task.defer.is_some() {
        return Some((
            named("defer"),
            "the engine itself would call what the task names",
        ));
    }
    if task.wait.is_some() {
        return Some((named("wait"), "the engine itself would watch for it"));
    }
    if task.workflow.is_some() {
        return Some((
            named("workflow"),
            "it starts a workflow that is registered here",
        ));
    }
    if !task.produces.is_empty() {
        return Some((
            named("produces"),
            "it would start the workflows that wait on a dataset",
        ));
    }
    if task.repeat.is_some() {
        return Some((
            named("repeat"),
            "how often a loop goes round is its author's to write",
        ));
    }
    // The one way a run grows after it is made. With it refused, a confined
    // run is the size it was submitted, and the most tasks it may have
    // ([`Limits::max_tasks`]) is asked of a number that does not change.
    if task.with_output_of.is_some() {
        return Some((
            named("with_output_of"),
            "how many tasks it becomes is decided as the run goes, by what a task printed",
        ));
    }
    if task.gang.is_some() || task.gang_member.is_some() {
        return Some((
            named("gang"),
            "it holds workers for tasks other than its own",
        ));
    }
    let approvals = task.approval_timeout_secs.is_some()
        || task.approval_on_timeout.is_some()
        || task.approval_message.is_some()
        || !task.approval_show.is_empty()
        || !task.binds.is_empty()
        || !task.approvers.is_empty()
        || task.not_triggerer;
    if approvals {
        return Some((
            format!("an approval's keys (task '{}')", task.name),
            "it asks a decision of this installation's people",
        ));
    }
    None
}

/// What a workflow names that a confined run may not use, and why: of its own
/// keys first, then of each task's. `None` for a workflow such a run may be
/// made of.
pub fn refusal(spec: &DagSpec, class: &str) -> Option<(String, &'static str)> {
    if spec.notify.is_some() {
        return Some((
            "`notify`".to_string(),
            "the engine itself would post where the workflow names, with what it holds",
        ));
    }
    if !spec.on_datasets.is_empty() || spec.datasets_mode.is_some() {
        return Some((
            "`on_datasets`".to_string(),
            "it would start runs that nothing asked for",
        ));
    }
    spec.tasks.iter().find_map(|task| task_refusal(task, class))
}

/// Take from a task what a confined run is not given and does not fail for.
///
/// - `cache`: an output cached under a workflow's and a task's name is every
///   run's of those names, whoever wrote them. A confined run is served none
///   and leaves none.
/// - `pool`: a slot of a named pool is one a trusted task then waits for.
///   It draws from a pool of its own ([`POOL`]), which an operator may cap.
/// - `priority`: it may stand back in the queue, never ahead.
/// - `retry_budgets`: with the class a failure is put in, they say how many
///   attempts it is given, and the class is read off what the task printed.
pub fn withhold(task: &mut TaskSpec) {
    task.cache = None;
    task.pool = Some(POOL.to_string());
    task.priority = task.priority.min(0);
    task.retry_budgets.clear();
}

/// Hold a workflow to what a run of `class` may be: refuse what it may not
/// use ([`refusal`]), and take from its tasks what it is not given
/// ([`withhold`]). Nothing is done for a `trusted` run.
pub fn hold(spec: &mut DagSpec, class: &str) -> Result<(), ConfinedRefused> {
    if !confined(class) {
        return Ok(());
    }
    if let Some((what, why)) = refusal(spec, class) {
        return Err(ConfinedRefused {
            class: class.to_string(),
            what,
            why,
        });
    }
    if let Some(defaults) = spec.task_defaults.as_mut() {
        defaults.retry_budgets.clear();
    }
    // A cap on active runs counts by the workflow's name and its key, and
    // both are the author's to write: a run that named itself as one of the
    // installation's workflows held that workflow's slot. Such a run's cap
    // is counted among such runs.
    if spec.max_active_runs.is_some() {
        spec.concurrency_key = Some(match spec.concurrency_key.take() {
            Some(key) => format!("{POOL}:{key}"),
            None => POOL.to_string(),
        });
    }
    spec.tasks.iter_mut().for_each(withhold);
    Ok(())
}

/// The graph a run of `class` is made of: `None` where it is the one given,
/// which is every `trusted` run's, and for a confined run the same graph
/// held ([`hold`]), or why it is not made.
pub fn held(dag: &DagGraph, class: &str) -> Result<Option<DagGraph>, ConfinedRefused> {
    if !confined(class) {
        return Ok(None);
    }
    let mut graph = dag.clone();
    hold(&mut graph.spec, class)?;
    Ok(Some(graph))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::envpolicy::{AGENT, UNTRUSTED};
    use std::collections::BTreeSet;

    /// The serialized field names of `pub struct {name}` in dag.rs: each
    /// `pub` field, under its `rename` when it has one.
    fn fields(name: &str) -> BTreeSet<String> {
        let source = include_str!("dag.rs").replace("\r\n", "\n");
        let start = source
            .find(&format!("pub struct {name} {{"))
            .unwrap_or_else(|| panic!("no `pub struct {name}`"));
        let body = &source[start..];
        let end = body.find("\n}\n").expect("the struct ends");
        let mut renamed: Option<String> = None;
        let mut out = BTreeSet::new();
        for line in body[..end].lines().skip(1) {
            let line = line.trim();
            if line.starts_with("#[serde(") {
                if let Some(rest) = line.split("rename = \"").nth(1) {
                    renamed = rest.split('"').next().map(str::to_string);
                }
            } else if let Some(field) = line.strip_prefix("pub ") {
                let field = field.split(':').next().expect("a field has a type").trim();
                out.insert(renamed.take().unwrap_or_else(|| field.to_string()));
            }
        }
        out
    }

    fn sorted(lists: &[&[&str]]) -> BTreeSet<String> {
        let all: Vec<&str> = lists.iter().flat_map(|l| l.iter().copied()).collect();
        let set: BTreeSet<String> = all.iter().map(|k| k.to_string()).collect();
        assert_eq!(set.len(), all.len(), "a key is in two lists: {all:?}");
        set
    }

    /// Every key a workflow may write is in one list or another. A key that
    /// is added to `TaskSpec` or `DagSpec` fails here until it is read
    /// against what a confined run may have.
    #[test]
    fn every_key_a_workflow_may_write_is_sorted() {
        let task = fields("TaskSpec");
        assert!(
            task.contains("type") && task.contains("docker_image"),
            "{task:?}"
        );
        assert_eq!(
            sorted(&[TASK_ALLOWED, TASK_REFUSED, TASK_WITHHELD]),
            task,
            "put the task's new key in TASK_ALLOWED, TASK_REFUSED or TASK_WITHHELD, and \
             hold it in task_refusal or withhold"
        );
        let spec = fields("DagSpec");
        assert!(spec.contains("use") && spec.contains("tasks"), "{spec:?}");
        assert_eq!(
            sorted(&[SPEC_ALLOWED, SPEC_REFUSED]),
            spec,
            "put the workflow's new key in SPEC_ALLOWED or SPEC_REFUSED, and hold it in \
             refusal"
        );
    }

    fn task(extra: &str) -> TaskSpec {
        serde_yaml::from_str(&format!("name: build\ncommand: [\"true\"]\n{extra}"))
            .unwrap_or_else(|e| panic!("{extra}: {e}"))
    }

    fn spec(extra: &str) -> DagSpec {
        serde_yaml::from_str(&format!(
            "name: ci\n{extra}tasks:\n  - name: build\n    command: [\"true\"]\n"
        ))
        .unwrap_or_else(|e| panic!("{extra}: {e}"))
    }

    #[test]
    fn a_confined_run_may_not_have_the_engine_act_for_it() {
        for (extra, what) in [
            (
                "service_account: deployer\n",
                "`service_account` (task 'build')",
            ),
            ("defer: { kind: spark }\n", "`defer` (task 'build')"),
            (
                "identity: { oidc: { audience: sts.amazonaws.com } }\n",
                "`identity` (task 'build')",
            ),
            ("type: wait\n", "`type: wait` (task 'build')"),
            (
                "wait: { until: \"2030-01-01T00:00:00Z\" }\n",
                "`wait` (task 'build')",
            ),
            ("type: workflow\n", "`type: workflow` (task 'build')"),
            ("workflow: deploy\n", "`workflow` (task 'build')"),
            ("type: approval\n", "`type: approval` (task 'build')"),
            (
                "approvers: [\"group:release\"]\n",
                "an approval's keys (task 'build')",
            ),
            ("not_triggerer: true\n", "an approval's keys (task 'build')"),
            (
                "produces: [\"s3://lake/orders\"]\n",
                "`produces` (task 'build')",
            ),
            ("gang: { size: 2 }\n", "`gang` (task 'build')"),
            (
                "repeat: { until: \"{{ done }}\", max_iterations: 3 }\n",
                "`repeat` (task 'build')",
            ),
            ("with_output_of: list\n", "`with_output_of` (task 'build')"),
            ("docker_daemon: true\n", "`docker_daemon` (task 'build')"),
            ("privileged: true\n", "`privileged` (task 'build')"),
        ] {
            let (named, _why) =
                task_refusal(&task(extra), UNTRUSTED).unwrap_or_else(|| panic!("{extra}"));
            assert_eq!(named, what, "{extra}");
        }
        // An ordinary task, said to be one.
        assert_eq!(task_refusal(&task("type: task\n"), UNTRUSTED), None);
        assert_eq!(task_refusal(&task(""), UNTRUSTED), None);

        for (extra, what) in [
            (
                "notify: { webhook: { url: \"http://127.0.0.1/\" } }\n",
                "`notify`",
            ),
            ("on_datasets: [\"s3://lake/orders\"]\n", "`on_datasets`"),
        ] {
            let mut s = spec(extra);
            let refused = hold(&mut s, UNTRUSTED).unwrap_err();
            assert_eq!(
                (refused.class.as_str(), refused.what.as_str()),
                (UNTRUSTED, what)
            );
            let said = refused.to_string();
            assert!(said.contains(REFUSAL_MARK), "{said}");
            assert!(
                said.starts_with(&format!(
                    "this run is untrusted, and {REFUSAL_MARK} {what}: "
                )),
                "{said}"
            );
            // A trusted run's workflow is its author's to write.
            assert_eq!(hold(&mut spec(extra), TRUSTED), Ok(()));
        }
        // A task's refusal is its workflow's.
        let mut s = spec("");
        s.tasks[0].service_account = Some("deployer".to_string());
        let refused = hold(&mut s, AGENT).unwrap_err();
        assert_eq!(
            refused.to_string(),
            "this run is agent, and a run that is not trusted may not use `service_account` \
             (task 'build'): it is an identity of this installation's"
        );
    }

    #[test]
    fn the_limits_an_operator_writes_are_read_and_none_can_be_turned_off() {
        let default = Limits::default();
        assert_eq!(
            (default.memory_bytes, default.pids, default.nano_cpus),
            (4 << 30, 2048, 2_000_000_000)
        );
        assert_eq!(
            (
                default.timeout_secs,
                default.max_attempts,
                default.max_tasks,
                default.max_services
            ),
            (3600, 3, 256, 4)
        );
        assert_eq!(Limits::parse("").unwrap(), default);
        assert_eq!(Limits::parse(" , ").unwrap(), default);
        // Any of them, the rest as they were.
        let held = Limits::parse("memory=512m, cpus=0.5").unwrap();
        assert_eq!(
            (
                held.memory_bytes,
                held.pids,
                held.nano_cpus,
                held.timeout_secs
            ),
            (512 << 20, 2048, 500_000_000, 3600)
        );
        let held =
            Limits::parse("pids=64,memory=8G,cpus=4,timeout=600,attempts=1,tasks=20,services=1")
                .unwrap();
        assert_eq!(held.max_services, 1);
        // No services at all is a limit, and the tightest.
        assert_eq!(Limits::parse("services=0").unwrap().max_services, 0);
        assert_eq!(Limits::parse("services=8").unwrap().max_services, 8);
        assert_eq!(
            (held.memory_bytes, held.pids, held.nano_cpus),
            (8 << 30, 64, 4_000_000_000)
        );
        assert_eq!(
            (held.timeout_secs, held.max_attempts, held.max_tasks),
            (600, 1, 20)
        );
        assert_eq!(
            Limits::parse("memory=16777216").unwrap().memory_bytes,
            16 << 20
        );
        assert_eq!(
            Limits::parse("memory=65536k").unwrap().memory_bytes,
            64 << 20
        );

        for bad in [
            // Nothing, or less than a container can be made with.
            "memory=0",
            "memory=1m",
            "memory=-4g",
            "memory=4t",
            "memory=",
            "memory=99999999999g",
            "pids=0",
            "pids=-1",
            "pids=many",
            // More than a task may have at all, or not a count.
            "services=9",
            "services=-1",
            "services=some",
            "cpus=0",
            "cpus=-2",
            "cpus=nan",
            "cpus=inf",
            "timeout=0",
            "timeout=-1",
            "timeout=604801",
            "timeout=1h",
            "attempts=0",
            "attempts=101",
            "tasks=0",
            "tasks=100001",
            // Not a limit, and not a way to have none.
            "memory",
            "swap=1g",
            "off",
            "unlimited",
        ] {
            assert!(Limits::parse(bad).is_err(), "{bad}");
        }
        let why = Limits::parse("mem=4g").unwrap_err();
        assert!(why.contains("unknown limit 'mem'"), "{why}");
    }

    /// What a confined run may spend is the lesser of what its author wrote
    /// and what the operator allows: an author's own numbers were the only
    /// ones there were.
    #[test]
    fn a_confined_run_spends_no_more_than_the_operator_allows() {
        let limits = Limits::parse("timeout=600,attempts=2,tasks=3").unwrap();
        // The author's, where they are less.
        let mut modest = task("timeout_secs: 60\nmax_attempts: 1\n");
        limits.spend(&mut modest);
        assert_eq!((modest.timeout_secs, modest.max_attempts), (Some(60), 1));
        // The operator's, where the author asked for more, or said nothing.
        let mut greedy = task("timeout_secs: 999999\nmax_attempts: 50\n");
        limits.spend(&mut greedy);
        assert_eq!((greedy.timeout_secs, greedy.max_attempts), (Some(600), 2));
        let mut silent = task("");
        assert_eq!(silent.timeout_secs, None);
        limits.spend(&mut silent);
        assert_eq!(silent.timeout_secs, Some(600));
        assert!((1..=2).contains(&silent.max_attempts));

        assert_eq!(limits.too_many(UNTRUSTED, 3), None);
        let why = limits.too_many(UNTRUSTED, 4).unwrap();
        assert_eq!(
            why,
            "this run is untrusted, and has 4 tasks: a run that is not trusted may have 3 \
             (DAGRON_UNTRUSTED_LIMITS, tasks)"
        );
    }

    /// A service is a container of its own, held to what the task's is: how
    /// many a task of such a run has beside it is the operator's to say, and
    /// one with more is not run. Having some is not refused.
    #[test]
    fn a_confined_task_has_no_more_services_than_the_operator_allows() {
        let with = |n: usize| {
            let services: String = (0..n)
                .map(|i| format!("  - {{ name: s{i}, image: x }}\n"))
                .collect();
            task(&format!("services:\n{services}"))
        };
        assert_eq!(
            task_refusal(&with(3), UNTRUSTED),
            None,
            "a service is not refused"
        );
        let limits = Limits::default();
        assert_eq!(limits.too_many_services(UNTRUSTED, &task("")), None);
        assert_eq!(limits.too_many_services(UNTRUSTED, &with(4)), None);
        assert_eq!(
            limits.too_many_services(UNTRUSTED, &with(5)),
            Some(
                "this run is untrusted, and its task has 5 services: a task of a run that is \
                 not trusted may have 4 (DAGRON_UNTRUSTED_LIMITS, services)"
                    .to_string()
            )
        );
        let none = Limits::parse("services=0").unwrap();
        assert_eq!(none.too_many_services(UNTRUSTED, &task("")), None);
        assert!(none.too_many_services(UNTRUSTED, &with(1)).is_some());
        // What a task of it may spend is the task's: its services are not
        // taken from it, nor changed.
        let mut held = with(2);
        withhold(&mut held);
        limits.spend(&mut held);
        assert_eq!(held.services.len(), 2);
    }

    #[test]
    fn a_confined_task_runs_in_an_image_the_operator_listed_where_they_listed_any() {
        let listed = parse_images(" docker.io/library/*, ghcr.io/acme/** ,").unwrap();
        assert_eq!(listed, ["docker.io/library/*", "ghcr.io/acme/**"]);
        assert_eq!(parse_images("").unwrap(), Vec::<String>::new());
        for bad in ["a b", "x\ty"] {
            assert!(parse_images(bad).is_err(), "{bad:?}");
        }
        let names = |image: &str| task(&format!("docker_image: \"{image}\"\n"));
        for allowed in [
            "docker.io/library/alpine:3.20",
            "docker.io/library/rust:1",
            "ghcr.io/acme/tools/build:2",
            "ghcr.io/acme/ci@sha256:0123",
        ] {
            assert_eq!(
                image_refusal(UNTRUSTED, &listed, &names(allowed)),
                None,
                "{allowed}"
            );
        }
        for other in [
            // As the task wrote it: a short name is another name.
            "alpine:3.20",
            "docker.io/acme/private:1",
            "docker.io/library/nested/alpine:3.20",
            "ghcr.io/other/tools:1",
            "internal.registry/acme/secrets:latest",
        ] {
            let why = image_refusal(UNTRUSTED, &listed, &names(other))
                .unwrap_or_else(|| panic!("{other}"));
            assert_eq!(
                why,
                format!(
                    "this run is untrusted, and its task names the image '{other}', which is \
                     none of those a run that is not trusted may name (DAGRON_UNTRUSTED_IMAGES)"
                )
            );
        }
        // No image named: the engine's own default, which is the operator's.
        assert_eq!(image_refusal(UNTRUSTED, &listed, &task("")), None);
        // A service's image is its author's too, and is asked the same: each
        // of them, after the task's own.
        let beside = |own: &str, services: &[&str]| {
            let services: String = services
                .iter()
                .enumerate()
                .map(|(i, image)| format!("  - {{ name: s{i}, image: \"{image}\" }}\n"))
                .collect();
            task(&format!("docker_image: \"{own}\"\nservices:\n{services}"))
        };
        let fine = "docker.io/library/alpine:3.20";
        assert_eq!(
            image_refusal(
                UNTRUSTED,
                &listed,
                &beside(
                    fine,
                    &["docker.io/library/postgres:16", "ghcr.io/acme/db:1"]
                )
            ),
            None
        );
        assert_eq!(
            image_refusal(
                UNTRUSTED,
                &listed,
                &beside(fine, &[fine, "internal.registry/acme/db:1"])
            ),
            Some(
                "this run is untrusted, and its task's service 's1' names the image \
                 'internal.registry/acme/db:1', which is none of those a run that is not \
                 trusted may name (DAGRON_UNTRUSTED_IMAGES)"
                    .to_string()
            )
        );
        let both = image_refusal(UNTRUSTED, &listed, &beside("alpine:3.20", &["postgres:16"]));
        assert!(
            both.is_some_and(|why| why.contains("its task names the image 'alpine:3.20'")),
            "the task's own is said first"
        );
        assert_eq!(image_refusal(UNTRUSTED, &[], &beside("x", &["y"])), None);
        // No list: any image, as before there was one.
        assert_eq!(
            image_refusal(
                UNTRUSTED,
                &[],
                &names("internal.registry/acme/secrets:latest")
            ),
            None
        );
    }

    /// A confined run's cap on its own active runs is counted apart from a
    /// trusted run's of the same workflow name and key: the name and the key
    /// are its author's to write, and a slot it held was one the
    /// installation's own run then waited for.
    #[test]
    fn a_confined_run_holds_no_slot_of_a_workflow_named_as_another() {
        let capped = "max_active_runs: 1\n";
        let keyed = "max_active_runs: 1\nconcurrency_key: main\n";
        for (written, own, theirs) in [
            (capped, None, Some("untrusted")),
            (keyed, Some("main"), Some("untrusted:main")),
            // With no cap there is no slot, and a key scopes nothing.
            ("", None, None),
        ] {
            let mut trusted = spec(written);
            hold(&mut trusted, TRUSTED).unwrap();
            assert_eq!(trusted.concurrency_key.as_deref(), own, "{written}");
            let mut confined = spec(written);
            hold(&mut confined, UNTRUSTED).unwrap();
            assert_eq!(confined.concurrency_key.as_deref(), theirs, "{written}");
            assert_eq!(confined.max_active_runs, trusted.max_active_runs);
        }
    }

    #[test]
    fn a_confined_run_is_not_given_what_other_runs_share() {
        let written = "cache: { key: one }\npool: builders\npriority: 9\n";
        let mut s = spec("");
        s.tasks[0] = task(written);
        s.tasks[0].retry_budgets.insert("oom".to_string(), 9);
        s.task_defaults = Some(crate::dag::TaskDefaults::default());
        s.task_defaults
            .as_mut()
            .unwrap()
            .retry_budgets
            .insert("oom".to_string(), 9);

        // A trusted run keeps all of it.
        let mut own = s.clone();
        hold(&mut own, TRUSTED).unwrap();
        let t = &own.tasks[0];
        assert!(t.cache.is_some() && t.pool.is_some() && t.priority == 9);
        assert_eq!(t.retry_budgets.len(), 1);

        hold(&mut s, UNTRUSTED).unwrap();
        let t = &s.tasks[0];
        assert!(t.cache.is_none(), "{t:?}");
        assert_eq!(
            t.pool.as_deref(),
            Some(POOL),
            "its own pool, not the one it named"
        );
        assert_eq!(t.priority, 0);
        assert!(t.retry_budgets.is_empty());
        assert!(s.task_defaults.unwrap().retry_budgets.is_empty());
        // And is not refused for having written it.
        assert_eq!(task_refusal(&task(written), UNTRUSTED), None);

        // It may stand back in the queue.
        let mut back = task("priority: -3\n");
        withhold(&mut back);
        assert_eq!(back.priority, -3);
    }

    /// A run made again from one that was not trusted is of its class,
    /// whoever asked for it: what it runs was written by the same hand.
    #[test]
    fn a_run_made_again_from_a_confined_one_is_confined() {
        use crate::envpolicy::class_of;
        use crate::models::{trigger_kind, RunTrigger};
        let person = || {
            RunTrigger {
                kind: trigger_kind::MANUAL.to_string(),
                principal: "user:dev@example.com".to_string(),
                actor: Some("dev@example.com".to_string()),
                ..Default::default()
            }
            .from_source("resubmit:run-1")
        };
        let fork = RunTrigger {
            kind: trigger_kind::CI.to_string(),
            principal: "ci:svc@example.com".to_string(),
            actor: Some("mallory".to_string()),
            trust: Some(UNTRUSTED.to_string()),
            forge_host: Some("github.com".to_string()),
            repo: Some("acme/widgets".to_string()),
            git_ref: Some("refs/pull/8/head".to_string()),
            sha: Some("a1".to_string()),
            base_sha: Some("b2".to_string()),
            pr: Some(8),
            event_id: Some("e-1".to_string()),
            ..Default::default()
        };
        let again = person().copying(Some(&fork));
        assert_eq!(class_of(Some(&again)), UNTRUSTED);
        assert!(confined(class_of(Some(&again))));
        // It is the caller's run, of the fork's commit.
        assert_eq!(
            (
                again.kind.as_str(),
                again.principal.as_str(),
                again.actor.as_deref()
            ),
            ("manual", "user:dev@example.com", Some("dev@example.com"))
        );
        assert_eq!(again.source.as_deref(), Some("resubmit:run-1"));
        assert_eq!(
            (
                again.repo.as_deref(),
                again.git_ref.as_deref(),
                again.sha.as_deref(),
                again.pr
            ),
            (
                Some("acme/widgets"),
                Some("refs/pull/8/head"),
                Some("a1"),
                Some(8)
            )
        );
        assert_eq!(again.event_id, None, "the event was the first run's");
        // And again from that one.
        assert_eq!(class_of(Some(&person().copying(Some(&again)))), UNTRUSTED);
        // A CI record that left its class unsaid is not trusted.
        let unsaid = RunTrigger {
            trust: None,
            ..fork.clone()
        };
        assert_eq!(class_of(Some(&person().copying(Some(&unsaid)))), UNTRUSTED);

        // A trusted run's resubmit is its caller's own, as it was: for no
        // ref, and of no class said.
        let main = RunTrigger {
            trust: Some(TRUSTED.to_string()),
            git_ref: Some("refs/heads/main".to_string()),
            ..fork.clone()
        };
        for original in [Some(&main), None] {
            assert_eq!(person().copying(original), person());
        }
    }

    #[test]
    fn an_engine_runs_confined_work_only_where_it_can_hold_it() {
        let floor = |raw: &str| IsolationSpec::parse_floor(raw).unwrap();
        let hardened = floor("hardened");
        // Never as the engine's own processes, whatever the floor.
        let why = engine_refusal(ExecutorKind::Local, &hardened, true).unwrap_err();
        assert!(why.contains("EXECUTOR=local"), "{why}");
        // On Docker, on a network the operator named.
        let why = engine_refusal(ExecutorKind::Docker, &hardened, false).unwrap_err();
        assert!(why.contains("DAGRON_DOCKER_NETWORK"), "{why}");
        assert_eq!(
            engine_refusal(ExecutorKind::Docker, &hardened, true),
            Ok(())
        );
        // On Kubernetes, with what the pod may reach said in the floor: the
        // preset says nothing of it, and `default` is the whole network.
        for silent in ["hardened", "network=default", "run_as_non_root=true"] {
            let why = engine_refusal(ExecutorKind::Kubernetes, &floor(silent), true).unwrap_err();
            assert!(why.contains("network=none"), "{silent}: {why}");
        }
        for said in ["network=none", "network=allow:forks"] {
            assert_eq!(
                engine_refusal(ExecutorKind::Kubernetes, &floor(said), false),
                Ok(()),
                "{said}"
            );
        }
        let said = not_run_here(UNTRUSTED);
        assert!(
            said.starts_with("this run is untrusted, and this engine runs no work"),
            "{said}"
        );
        assert!(said.contains(ENV_FLOOR), "{said}");
    }

    #[test]
    fn the_graph_a_confined_run_is_made_of_is_a_held_copy() {
        let yaml = "name: ci\ntasks:\n  - name: build\n    command: [\"true\"]\n    cache: { key: one }\n  - name: test\n    depends_on: [build]\n    command: [\"true\"]\n";
        let dag = DagGraph::from_yaml(yaml).unwrap();
        assert!(held(&dag, TRUSTED).unwrap().is_none());
        let copy = held(&dag, UNTRUSTED)
            .unwrap()
            .expect("a confined run's graph");
        assert!(copy.task_spec("build").unwrap().cache.is_none());
        assert_eq!(copy.task_spec("test").unwrap().depends_on, ["build"]);
        // The one given is as it was written.
        assert!(dag.task_spec("build").unwrap().cache.is_some());

        let calls = "name: ci\ntasks:\n  - name: call\n    type: workflow\n    workflow: deploy\n";
        let dag = DagGraph::from_yaml(calls).unwrap();
        let refused = held(&dag, UNTRUSTED)
            .err()
            .expect("a confined run starts no workflow");
        assert_eq!(refused.what, "`type: workflow` (task 'call')");
        assert!(held(&dag, TRUSTED).unwrap().is_none());
    }
}
