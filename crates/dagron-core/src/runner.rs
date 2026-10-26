//! The remote runner protocol: what dagron-api and `dagron-runner` say to
//! each other.
//!
//! A runner dials out. It enrols once with a join token and is given a
//! credential of its own, `runner:<id>`. Each process of it opens a
//! **session**, which says the labels it serves (the runner classes it takes
//! tasks of), how many tasks it runs at once and what it speaks. Then it
//! long-polls for tasks, heartbeats the session, streams each task's log, and
//! says how each came out. A runner holds no database credentials and owns no
//! state: a runner that dies loses only what it was running, which is taken
//! back to `ready` once its session's lease has lapsed.
//!
//! Every type here is the wire's, and both ends build on this one copy. A
//! field a newer end adds must be optional to an older one, and a change an
//! older end cannot read is a new protocol version ([`PROTOCOLS`]).

use serde::{Deserialize, Serialize};

use crate::dag::{EnvVar, ImagePullPolicy, ResourceRequirements};

/// The protocol versions this build speaks, oldest first. A session speaks
/// the newest one both ends do ([`negotiate`]).
pub const PROTOCOLS: &[u32] = &[0];

/// What a runner's credential begins with. Not an API token's `dgp_`: a
/// runner's credential is not a person's, and is looked for in its own table.
pub const CREDENTIAL_PREFIX: &str = "dgr_";

/// What a runner join token begins with.
pub const JOIN_TOKEN_PREFIX: &str = "dgrj_";

/// The setting a session's lease is read from, on dagron-api and the engine.
pub const ENV_SESSION_LEASE: &str = "DAGRON_SESSION_LEASE_SECS";

/// A session's lease where [`ENV_SESSION_LEASE`] does not say, in seconds.
pub const DEFAULT_SESSION_LEASE_SECS: i64 = 90;

/// The shortest session lease: a heartbeat every third of it, and two of them
/// may be missed before the lease lapses.
pub const MIN_SESSION_LEASE_SECS: i64 = 15;

/// How long a session's lease is, in seconds: [`ENV_SESSION_LEASE`], at least
/// [`MIN_SESSION_LEASE_SECS`]. Read once.
pub fn session_lease_secs() -> i64 {
    static LEASE: std::sync::OnceLock<i64> = std::sync::OnceLock::new();
    *LEASE.get_or_init(|| parse_session_lease(std::env::var(ENV_SESSION_LEASE).ok().as_deref()))
}

fn parse_session_lease(raw: Option<&str>) -> i64 {
    raw.and_then(|raw| raw.trim().parse::<i64>().ok())
        .unwrap_or(DEFAULT_SESSION_LEASE_SECS)
        .max(MIN_SESSION_LEASE_SECS)
}

/// How often a session is heartbeat: a third of its lease, so that two
/// heartbeats can be lost and the lease still hold.
pub fn heartbeat_secs(lease_secs: i64) -> i64 {
    (lease_secs / 3).max(1)
}

/// The protocol a session speaks: the newest version both `ours` and
/// `theirs` list. `None` where they share none.
pub fn negotiate(ours: &[u32], theirs: &[u32]) -> Option<u32> {
    ours.iter().copied().filter(|v| theirs.contains(v)).max()
}

/// Whether the release `have` is `min` or later, both as `MAJOR.MINOR.PATCH`
/// (a leading `v` and anything after a `-` or `+` are passed over). `None`
/// where either cannot be read: a build that says `dev` is no release.
pub fn version_at_least(have: &str, min: &str) -> Option<bool> {
    fn parse(text: &str) -> Option<(u64, u64, u64)> {
        let text = text.trim().trim_start_matches('v');
        let core = text.split(['-', '+']).next()?;
        let mut parts = core.split('.');
        let major = parts.next()?.parse().ok()?;
        let minor = parts.next().unwrap_or("0").parse().ok()?;
        let patch = parts.next().unwrap_or("0").parse().ok()?;
        if parts.next().is_some() {
            return None;
        }
        Some((major, minor, patch))
    }
    Some(parse(have)? >= parse(min)?)
}

/// Where a session's tasks' secrets are resolved.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Secrets {
    /// By dagron-api, from the task's environment, and sent with the task.
    #[default]
    Server,
    /// By the runner, from its own store: dagron-api sends each secret's name
    /// alone, and never reads or sends a value ("secrets stay home").
    Home,
}

impl Secrets {
    pub fn as_str(self) -> &'static str {
        match self {
            Secrets::Server => "server",
            Secrets::Home => "home",
        }
    }

    pub fn parse(text: &str) -> Option<Secrets> {
        match text.trim() {
            "server" => Some(Secrets::Server),
            "home" => Some(Secrets::Home),
            _ => None,
        }
    }
}

/// `POST /runner/v1/register`: a join token exchanged for a credential.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct RegisterRequest {
    pub join_token: String,
    /// What the runner is called where it is listed. The host's name, by
    /// default.
    #[serde(default)]
    pub name: Option<String>,
    /// The labels it will serve: those the join token allows, or some of them.
    /// Empty: all the join token allows.
    #[serde(default)]
    pub labels: Vec<String>,
    #[serde(default)]
    pub os: Option<String>,
    #[serde(default)]
    pub arch: Option<String>,
    pub capacity: u32,
    #[serde(default)]
    pub version: Option<String>,
}

/// What a registration returns. The credential is shown this once.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Registered {
    pub runner_id: String,
    pub token: String,
    pub labels: Vec<String>,
}

/// `POST /runner/v1/sessions`: a runner process's hold on the tasks it takes.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct OpenSession {
    /// The labels this process serves, of those the runner was registered
    /// with. Empty: all of them.
    #[serde(default)]
    pub labels: Vec<String>,
    /// How many tasks it runs at once.
    pub capacity: u32,
    /// Its release, for the floor an operator may set.
    pub version: String,
    /// The protocol versions it speaks.
    pub protocols: Vec<u32>,
    #[serde(default)]
    pub secrets: Secrets,
    #[serde(default)]
    pub os: Option<String>,
    #[serde(default)]
    pub arch: Option<String>,
}

/// What opening a session returns.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Session {
    pub session_id: String,
    /// The protocol the session speaks.
    pub protocol: u32,
    pub labels: Vec<String>,
    pub capacity: u32,
    pub lease_secs: i64,
    /// How often to heartbeat.
    pub heartbeat_secs: i64,
    pub lease_expires_at: String,
}

/// `POST /runner/v1/sessions/{id}/claim`: the tasks given to the session.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Claimed {
    pub tasks: Vec<Job>,
    /// The session is draining: take nothing more, finish what is
    /// running, and close the session.
    #[serde(default)]
    pub drain: bool,
}

/// One task, as a runner runs it. Everything a runner's executor is given,
/// and nothing of the database's: the runner holds none.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Job {
    pub task_id: String,
    pub run_id: String,
    pub name: String,
    /// The claim's fencing token. Every word about this task carries it, and
    /// one whose fence is not the task's any more is refused (409).
    pub fence: i64,
    /// The attempt before this one: the attempt that runs is `attempt + 1`.
    pub attempt: i64,
    pub command: Vec<String>,
    #[serde(default)]
    pub timeout_secs: Option<u64>,
    #[serde(default)]
    pub docker_image: Option<String>,
    #[serde(default)]
    pub image_pull_policy: Option<ImagePullPolicy>,
    /// The task's variables. Under [`Secrets::Server`] a secret's value is
    /// filled in, and its `value_from` kept, so that the runner masks it. Under
    /// [`Secrets::Home`] it is the name alone, for the runner to resolve.
    #[serde(default)]
    pub env: Vec<EnvVar>,
    #[serde(default)]
    pub resources: Option<ResourceRequirements>,
    #[serde(default)]
    pub service_account: Option<String>,
    /// What the task asked of its isolation. The runner raises it to its own
    /// floor and refuses it where its executor cannot hold it, as an engine
    /// does.
    #[serde(default)]
    pub isolation: Option<crate::isolation::IsolationSpec>,
    #[serde(default)]
    pub workspace: Option<crate::workspace::WorkspaceSpec>,
    #[serde(default)]
    pub services: Vec<crate::service::ServiceSpec>,
    /// The task asks for a Docker daemon beside it, which the runner starts
    /// where its executor can (`docker_daemon: true`).
    #[serde(default)]
    pub docker_daemon: bool,
    /// The task asks for an identity: its request credential, minted
    /// for this attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity: Option<Identity>,
    /// The task asks for its run's artifacts over dagron-api's API
    /// (`artifact_api: true`): the credential minted for this attempt.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub artifact_api: Option<ArtifactApi>,
}

/// A task's way into its run's artifacts, as dagron-api gives it a runner:
/// the credential it minted for the attempt. The runner gives the task it,
/// with dagron-api's `/task/v1/artifacts` where the task reaches it
/// (`DAGRON_ARTIFACTS_*`, as an engine gives them).
#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct ArtifactApi {
    pub token: String,
}

impl std::fmt::Debug for ArtifactApi {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ArtifactApi")
            .field("token", &"***")
            .finish()
    }
}

/// A task's identity, as dagron-api gives it a runner: the request credential
/// it minted for the attempt, and the audience a request that names none is
/// for. The runner gives the task both, with where to trade the credential
/// for a token: dagron-api's `/oidc/token`, where the runner reaches it
/// (`DAGRON_OIDC_REQUEST_*`, as an engine gives them).
#[derive(Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Identity {
    pub token: String,
    pub audience: String,
}

impl std::fmt::Debug for Identity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Identity")
            .field("token", &"***")
            .field("audience", &self.audience)
            .finish()
    }
}

/// One task a session holds, as its heartbeat says it.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize, Serialize)]
pub struct Held {
    pub task_id: String,
    pub fence: i64,
}

/// `POST /runner/v1/sessions/{id}/heartbeat`.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Heartbeat {
    /// Every task the runner is running for this session.
    #[serde(default)]
    pub running: Vec<Held>,
}

/// What a heartbeat returns.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct HeartbeatAck {
    pub lease_expires_at: String,
    /// Of the tasks the heartbeat named, those to stop now: asked to stop, or
    /// not this session's any more. Each is stopped and said to be
    /// [`Outcome::Cancelled`].
    #[serde(default)]
    pub cancel: Vec<String>,
    #[serde(default)]
    pub drain: bool,
}

/// `POST /runner/v1/tasks/{id}/logs/{seq}`: what a task wrote since the last
/// segment. An attempt's segments are numbered 0, 1, 2, … and the numbers
/// only rise, a seal's among them; segment 0 is the attempt's first, and
/// starts its log anew. One sent again, its answer lost, keeps its number and
/// its text.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct LogSegment {
    pub session_id: String,
    pub fence: i64,
    pub text: String,
    /// It replaces the log rather than adding to it: the attempt's first, or
    /// a seal, the whole log over again redacted as one piece.
    #[serde(default)]
    pub reset: bool,
}

/// What dagron-api keeps of one attempt's log for a runner past twice its
/// cap: a MiB for the lines dagron adds, and the notes.
pub const SESSION_LOG_LINES: usize = 1 << 20;

/// The most of one attempt's log dagron-api keeps for a runner whose cap is
/// `cap` (`DAGRON_LOG_TASK_CAP_BYTES`): its beginning, of the cap; its end,
/// of at most the cap, which is as much as an executor keeps of it whatever
/// the runner's host sets (`DAGRON_LOG_TAIL_BYTES`); and
/// [`SESSION_LOG_LINES`].
pub fn session_log_most(cap: usize) -> usize {
    cap.saturating_mul(2).saturating_add(SESSION_LOG_LINES)
}

/// How a task came out.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome {
    Succeeded,
    /// It ran and failed, or could not be started.
    Failed,
    /// Its `timeout_secs` ran out.
    TimedOut,
    /// The executor had no room for it, and nothing ran: it is given back
    /// without spending its attempt, as an engine's would be.
    Backpressure,
    /// It was stopped because it was asked to, and its work is gone.
    Cancelled,
}

impl Outcome {
    pub fn as_str(self) -> &'static str {
        match self {
            Outcome::Succeeded => "succeeded",
            Outcome::Failed => "failed",
            Outcome::TimedOut => "timed_out",
            Outcome::Backpressure => "backpressure",
            Outcome::Cancelled => "cancelled",
        }
    }

    pub fn parse(text: &str) -> Option<Outcome> {
        match text {
            "succeeded" => Some(Outcome::Succeeded),
            "failed" => Some(Outcome::Failed),
            "timed_out" => Some(Outcome::TimedOut),
            "backpressure" => Some(Outcome::Backpressure),
            "cancelled" => Some(Outcome::Cancelled),
            _ => None,
        }
    }
}

/// `POST /runner/v1/tasks/{id}/result`.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct Report {
    pub session_id: String,
    pub fence: i64,
    pub outcome: Outcome,
    #[serde(default)]
    pub exit_code: Option<i32>,
    /// What the task printed, redacted and capped by the runner: the end of
    /// it, which is what a result is.
    #[serde(default)]
    pub output: String,
}

/// `GET /runner/v1/queue`: the work there is for some labels, for whatever
/// sizes the runners that serve them.
#[derive(Debug, Clone, Default, Deserialize, Serialize)]
pub struct Queue {
    pub labels: Vec<LabelQueue>,
}

/// The work there is for one label.
#[derive(Debug, Clone, Default, PartialEq, Eq, Deserialize, Serialize)]
pub struct LabelQueue {
    pub label: String,
    /// Tasks of the class that are ready and due.
    pub ready: i64,
    /// Tasks of the class that sessions hold.
    pub running: i64,
    /// Live sessions that serve it, and are not draining.
    pub sessions: i64,
    /// What those sessions run at once, together.
    pub capacity: i64,
}

/// How many replicas a label's queue asks for: enough for the tasks
/// that are ready and those that run, each replica running `per_replica` at
/// once, within `min..=max`. Shared by the Kubernetes scaler
/// (`dagron-runner scale`) and the enterprise build's VM pools', so the two
/// scale alike.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ScalePolicy {
    pub min: i64,
    pub max: i64,
    /// What one replica runs at once: its runner's capacity.
    pub per_replica: i64,
}

impl ScalePolicy {
    pub fn desired(&self, queue: &LabelQueue) -> i64 {
        let work = queue.ready.max(0) + queue.running.max(0);
        let per = self.per_replica.max(1);
        let want = (work + per - 1) / per;
        want.clamp(self.min.max(0), self.max.max(self.min).max(0))
    }
}

/// Scaling down only once the work has asked for fewer replicas for a while:
/// a queue that empties between two bursts does not lose the runners the
/// second needs. Up at once.
#[derive(Debug, Clone)]
pub struct Damper {
    down_after: std::time::Duration,
    lower_since: Option<std::time::Instant>,
}

impl Damper {
    pub fn new(down_after: std::time::Duration) -> Damper {
        Damper {
            down_after,
            lower_since: None,
        }
    }

    /// What to scale to, from `current`, where the work asks for `desired`.
    pub fn next(&mut self, current: i64, desired: i64, now: std::time::Instant) -> i64 {
        if desired >= current {
            self.lower_since = None;
            return desired;
        }
        let since = *self.lower_since.get_or_insert(now);
        if now.duration_since(since) >= self.down_after {
            self.lower_since = None;
            desired
        } else {
            current
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_session_speaks_the_newest_version_both_ends_do() {
        assert_eq!(negotiate(&[0, 1, 2], &[1, 2, 3]), Some(2));
        assert_eq!(negotiate(&[0], &[0]), Some(0));
        assert_eq!(negotiate(&[0], &[1]), None);
        assert_eq!(negotiate(&[0], &[]), None);
    }

    /// A runner's log is kept up to twice its cap and a MiB: its beginning,
    /// an end of at most the cap, and dagron's lines; never past what a
    /// number can say.
    #[test]
    fn session_log_most_holds_twice_the_cap_and_a_mib() {
        assert_eq!(session_log_most(0), 1 << 20);
        assert_eq!(session_log_most(1000), 2000 + (1 << 20));
        assert_eq!(session_log_most(64 << 20), (128 << 20) + (1 << 20));
        assert_eq!(session_log_most(usize::MAX), usize::MAX);
        assert_eq!(session_log_most(usize::MAX / 2 + 1), usize::MAX);
    }

    #[test]
    fn a_release_is_compared_by_its_numbers() {
        assert_eq!(version_at_least("0.12.0", "0.11.3"), Some(true));
        assert_eq!(version_at_least("v0.11.3", "0.11.3"), Some(true));
        assert_eq!(version_at_least("0.11.3-rc.1", "0.11.3"), Some(true));
        assert_eq!(version_at_least("0.9.10", "0.11"), Some(false));
        assert_eq!(version_at_least("1", "0.99.99"), Some(true));
        assert_eq!(version_at_least("dev", "0.11.0"), None);
        assert_eq!(version_at_least("0.11.0", "soon"), None);
        assert_eq!(version_at_least("1.2.3.4", "1.0.0"), None);
    }

    #[test]
    fn a_session_lease_is_read_with_a_floor() {
        assert_eq!(parse_session_lease(None), DEFAULT_SESSION_LEASE_SECS);
        assert_eq!(parse_session_lease(Some("120")), 120);
        assert_eq!(parse_session_lease(Some("3")), MIN_SESSION_LEASE_SECS);
        assert_eq!(
            parse_session_lease(Some("soon")),
            DEFAULT_SESSION_LEASE_SECS
        );
        assert_eq!(heartbeat_secs(90), 30);
        assert_eq!(heartbeat_secs(2), 1);
    }

    #[test]
    fn an_outcome_and_a_secrets_mode_read_back_as_they_are_written() {
        for outcome in [
            Outcome::Succeeded,
            Outcome::Failed,
            Outcome::TimedOut,
            Outcome::Backpressure,
            Outcome::Cancelled,
        ] {
            assert_eq!(Outcome::parse(outcome.as_str()), Some(outcome));
            let json = serde_json::to_string(&outcome).unwrap();
            assert_eq!(json, format!("\"{}\"", outcome.as_str()));
        }
        assert_eq!(Outcome::parse("lost"), None);
        for secrets in [Secrets::Server, Secrets::Home] {
            assert_eq!(Secrets::parse(secrets.as_str()), Some(secrets));
        }
        assert_eq!(Secrets::parse("cloud"), None);
    }

    #[test]
    fn a_job_keeps_a_secret_s_name_and_drops_nothing_it_was_given() {
        let job: Job = serde_json::from_value(serde_json::json!({
            "task_id": "t", "run_id": "r", "name": "build", "fence": 4, "attempt": 0,
            "command": ["make"],
            "env": [{"name": "TOKEN", "value_from": {"secret": "gh"}}],
        }))
        .unwrap();
        assert_eq!(job.env[0].value, "");
        assert_eq!(job.env[0].value_from.as_ref().unwrap().secret, "gh");
        let back: Job = serde_json::from_str(&serde_json::to_string(&job).unwrap()).unwrap();
        assert_eq!(back.fence, 4);
        assert!(back.services.is_empty() && !back.docker_daemon);
    }

    #[test]
    fn the_work_asks_for_replicas_within_the_bounds() {
        let policy = ScalePolicy {
            min: 1,
            max: 5,
            per_replica: 2,
        };
        let q = |ready, running| LabelQueue {
            ready,
            running,
            ..Default::default()
        };
        assert_eq!(policy.desired(&q(0, 0)), 1);
        assert_eq!(policy.desired(&q(3, 0)), 2);
        assert_eq!(policy.desired(&q(3, 2)), 3);
        assert_eq!(policy.desired(&q(100, 0)), 5);
        let none = ScalePolicy {
            min: 0,
            max: 3,
            per_replica: 1,
        };
        assert_eq!(none.desired(&q(0, 0)), 0);
    }

    #[test]
    fn a_damper_scales_up_at_once_and_down_once_asked_for_a_while() {
        let start = std::time::Instant::now();
        let at = |secs| start + std::time::Duration::from_secs(secs);
        let mut d = Damper::new(std::time::Duration::from_secs(300));
        assert_eq!(d.next(1, 4, at(0)), 4);
        assert_eq!(d.next(4, 1, at(10)), 4);
        assert_eq!(d.next(4, 2, at(200)), 4);
        // Asked for more in between: the wait starts over.
        assert_eq!(d.next(4, 4, at(250)), 4);
        assert_eq!(d.next(4, 1, at(260)), 4);
        assert_eq!(d.next(4, 1, at(560)), 1);
    }
}
