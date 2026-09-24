//! dagron engine — the reconcile-loop daemon as a reusable library.
//!
//! [`run`] is the whole scheduler: config from env, executor + worker pool + db
//! pool + ingest actor, the ops surface, and the multi-run reconcile loop. The
//! `dagron` binary is a thin shell over it; alternate builds differ only in the
//! [`Seams`] they pass (built-in vs. extra sources; no-op vs. active
//! run-lifecycle hooks).

pub mod hooks;
pub use hooks::Seams;

// ── ops surface (feature `ops`) — the axum management API + the leadership-gated
// cron / GC / DB-schedule loops. They wire the library crates together rather than
// belonging to any one, so they live in the engine alongside the run loop.
#[cfg(feature = "ops")]
mod api;
#[cfg(feature = "enterprise")]
mod backfill;
// First-class paced backfill jobs (#18) — driven from the schedule loop.
#[cfg(feature = "ops")]
mod backfill_jobs;
#[cfg(feature = "ops")]
mod cron;
#[cfg(feature = "ops")]
mod gc;
// Environment integration: `{{ env.* }}` template params at run creation +
// DB-backed secret resolution at dispatch. Both halves read ops-only datastore
// queries (`environment_vars`, `run_environment`, `environment_secret`), so
// the real module is ops-gated and the lean build gets the shim below.
#[cfg(feature = "ops")]
mod environments;
/// Lean-build (`--no-default-features --features sqlite`) stand-in for
/// [`environments`]: the same two signatures the run loop calls, minus the
/// datastore-backed environment store a lean daemon does not carry.
///
/// `template_params` yields no `{{ env.* }}` keys — a spec that declares
/// `environment:` runs as-is, its templates unexpanded, because there is no
/// store to resolve them from. `resolve_secrets` still resolves every
/// `value_from: {secret: NAME}` reference from the process environment /
/// `DAGRON_SECRETS_DIR` through the executor's resolver — the tier that
/// predates the DB store, and the ops module's own fallback. A task must not
/// run with an empty credential in any build.
#[cfg(not(feature = "ops"))]
mod environments {
    use std::collections::BTreeMap;

    use anyhow::Result;

    use crate::{dag, db};

    pub(crate) async fn template_params(
        _pool: &db::Pool,
        _yaml: &str,
    ) -> Result<BTreeMap<String, String>> {
        Ok(BTreeMap::new())
    }

    pub(crate) async fn resolve_secrets(
        _pool: &db::Pool,
        _run_id: &str,
        env: &mut [dag::EnvVar],
    ) -> Result<()> {
        if !env.iter().any(|e| e.value_from.is_some()) {
            return Ok(());
        }
        let resolved = dagron_executor::secrets::resolve(env)?;
        for (var, done) in env.iter_mut().zip(resolved) {
            var.value = done.value;
        }
        Ok(())
    }
}
// Leadership (the `leader_election` lease row) only gates the ops loops —
// cron, GC, DB schedules, backfill pacing, the stale-ready alarm — and its
// datastore call is ops-only, so a lean build carries neither.
#[cfg(feature = "ops")]
mod leadership;
// Outbound run notifications (`notify.webhook` / `notify.slack`), fired on run
// finalization and soft-deadline breach. Best-effort, like forge feedback.
mod notify;
#[cfg(feature = "ops")]
mod schedule;
// Timezone-aware cron fire-time helper shared by cron/schedule/backfill loops.
#[cfg(feature = "ops")]
mod schedule_time;
// Unclaimable-class alarm: warns when a runner class's ready backlog ages
// because no live scheduler serves it (runner segmentation).
#[cfg(feature = "ops")]
mod stale_ready;
// Cloud archive URL → object_store dispatch (s3/gs/az), shared by the GC sink
// and the Parquet compactor.
#[cfg(feature = "archive-cloud")]
mod objstore;
// `dagron archive-compact` — fold archived run documents into Parquet
// (the analytics tier of the hot/cold split).
#[cfg(feature = "archive-parquet")]
mod archive_compact;
// Offline spec validation (`dagron validate`) — pure dagron-core, no ops needed.
mod validate;
// Central server-settings management (LOW_LATENCY §5): the knob registry, the
// DAGRON_CONFIG file/profile layer, typo warnings, the fleet fingerprint, and
// `dagron config` introspection.
mod config;
// Constrained-host gates: pressure file + free-disk floor (docs/CONFIG.md).
mod pressure;
// Wall-clock confidence on disconnected units (docs/CONFIG.md).
mod clock;

// Network policy for `wait.url` sensors: the opt-in `WAIT_URL_DENY_PRIVATE`
// guard that keeps a scheduler-issued poll from reaching addresses the task
// pod could not (see the module docs for the threat model).
mod wait_url;

// The built-in `defer.http` poller. Separate from wait_url because the policy
// inverts: that client issues an unauthenticated GET and defaults to permitting
// private addresses, this one carries the task's headers — including a bearer
// token — and defaults to refusing them.
mod defer_http;

// The engine logic now lives in three library crates. Re-alias them to the module
// paths the wiring below (and the ops modules) already use — `db::`, `dag::`,
// `executor::`, `source::`, … — so the split is pure plumbing: no call site moved.
// (A private `use` in the crate root is visible to every descendant module, so
// `crate::db` inside api.rs/cron.rs/… keeps resolving here.)
use dagron_core::{dag, db, metrics};
// Used only by the ops modules (api.rs) via `crate::models`.
#[cfg(feature = "ops")]
use dagron_core::models;
#[cfg(feature = "kubernetes")]
use dagron_executor::kube_executor;
use dagron_executor::{docker_executor, executor, worker};
use dagron_source::{ingest, source};

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::Result;
use tokio::sync::mpsc;
use tracing::{info, warn};

use ractor::Actor;

use executor::{ExecContext, LocalExecutor};
use ingest::{IngestActor, IngestArgs};
use metrics::Metrics;
use worker::{DispatchPayload, WorkerPool};

/// Build a fresh W3C `traceparent` header value for a task, plus its trace id
/// (OpenTelemetry integration, #28). Format: `00-<16-byte trace id>-<8-byte span
/// id>-01` — version `00`, `01` = sampled. Injected into a dispatched task's env
/// as `TRACEPARENT` so the task's own OpenTelemetry instrumentation joins this
/// trace (external-trace embedding). The ids come from v4 UUIDs, so the trace id
/// is never the all-zero value W3C forbids.
#[cfg_attr(not(feature = "otel"), allow(dead_code))]
fn new_traceparent() -> (String, String) {
    let trace_id = uuid::Uuid::new_v4().simple().to_string(); // 32 hex = 16 bytes
    let span_id = uuid::Uuid::new_v4().simple().to_string()[..16].to_string(); // 16 hex = 8 bytes
    (format!("00-{trace_id}-{span_id}-01"), trace_id)
}

/// The worker heartbeat over the datastore — renews the claim lease while a
/// task executes, guarded by the claim triple so a reclaimed task can never be
/// resurrected. This is what lets one task run for hours (training, stream
/// consumers) on the same short-lease crash-recovery machinery.
struct DbLeaseKeeper {
    pool: db::Pool,
}

#[async_trait::async_trait]
impl worker::LeaseKeeper for DbLeaseKeeper {
    async fn renew(&self, task_id: &str, worker_id: &str, fence: i64) -> Result<bool> {
        // Renew by the same window claims use (`LEASE_SECS`, default 30 s), so
        // a healthy task's lease always sits between ⅔ and one full window in
        // the future and a dead worker's task is reclaimed on the configured
        // timeline — shortening the window shortens crash recovery to match.
        db::renew_task_lease(&self.pool, task_id, worker_id, fence, db::lease_secs()).await
    }
}

/// Max parked `wait.url` sensors probed in a single reconcile tick. Bounds how
/// much outbound HTTP one tick can take on; the rest roll to the next tick.
const WAIT_URL_BATCH: i64 = 32;

/// Max parked `defer:` external jobs polled in a single reconcile tick, for the
/// same reason [`WAIT_URL_BATCH`] exists: one tick's outbound work is bounded,
/// and the remainder rolls to the next tick. The partial index from migration
/// 042/054 makes the SELECT cheap regardless of how many rows are parked.
///
/// The batch is polled concurrently, so this bounds how many vendor calls are in
/// flight at once rather than how long the batch takes end to end — it costs
/// about one call's deadline, not this many.
const EXTERNAL_POLL_BATCH: i64 = 32;

/// Ceiling on one registered [`hooks::ExternalPoller::poll`] call.
///
/// The trait carries no timeout contract, so without this an implementation that
/// blocks forever does not stall one job — it holds a `JoinSet` slot forever,
/// the sweep that awaits the set never finishes its pass, and that stalls the
/// reconcile loop and with it every lease, deadline and schedule the loop
/// drives. Polling the batch concurrently does not help here: it bounds the
/// *sum* of calls that return, and says nothing about one that never does. A
/// seam whose worst case takes down the scheduler is not a seam anyone can
/// safely implement.
///
/// Expiry is deliberately **not** a verdict: it becomes an `Err`, which the
/// sweep already treats as "no answer" and re-parks. A slow vendor must not
/// fail a six-hour job.
const EXTERNAL_POLLER_TIMEOUT_SECS: u64 = 30;

/// Max workloads the fleet sweep DELETES in one pass.
///
/// The cap is on deletes, never on candidates. Capping candidates would starve:
/// an apiserver lists in a stable order, so a namespace whose first N workloads
/// are long-running and live would be re-examined identically every sweep and
/// never reach the leftovers behind them. Every candidate is asked about; only
/// the acting is rationed, and the remainder is picked up next sweep because
/// leftovers are expensive rather than urgent.
const ORPHAN_DELETE_BATCH: usize = 200;

/// How many task ids the fleet sweep asks about per query.
///
/// The liveness question is chunked rather than truncated, so a namespace with
/// thousands of workloads is still fully covered while each `IN` list stays a
/// size SQLite will accept and Postgres will plan well.
const ORPHAN_QUERY_CHUNK: usize = 200;

/// Default gap between fleet sweeps (`DAGRON_ORPHAN_SWEEP_SECS`), floored at 30.
///
/// Slow on purpose. This sweep lists every workload in the namespace, which is
/// an apiserver call whose cost scales with the cluster rather than with this
/// engine's work, and what it catches — a workload whose scheduler died — does
/// not accumulate quickly.
const ORPHAN_SWEEP_SECS_DEFAULT: u64 = 300;

/// Default age below which a workload is never judged
/// (`DAGRON_ORPHAN_MIN_AGE_SECS`), floored at 60.
///
/// Generous deliberately. The cost of waiting is a leftover living a few more
/// minutes; the cost of being wrong is deleting live work. See
/// `OrphanScope::min_age` for why any floor is needed at all.
const ORPHAN_MIN_AGE_SECS_DEFAULT: u64 = 600;

/// Max teardowns attempted in a single reconcile tick, for the same reason
/// [`EXTERNAL_POLL_BATCH`] exists.
const EXTERNAL_CANCEL_BATCH: i64 = 16;

/// Failed teardown attempts before a remote job is declared an orphan.
///
/// Small on purpose. Teardown is best-effort — a job we cannot reach is a job
/// we cannot stop — and the value of retrying a vendor that has refused us
/// three times is lower than the value of telling the operator, loudly and by
/// handle, that something is still running.
const EXTERNAL_CANCEL_ATTEMPTS: i64 = 3;

/// How long a row may owe a teardown before it is orphaned regardless of
/// attempts, measured from when the task went terminal.
///
/// The attempt budget alone does not terminate: a replica with no poller for a
/// kind hands the row back *without* consuming an attempt (so it cannot exhaust
/// the budget belonging to a replica that could do the work), which means a
/// fleet where nothing owns the kind would sweep the row forever. This is the
/// bound that does not depend on fleet shape.
const EXTERNAL_CANCEL_GIVEUP_SECS: i64 = 3600;

/// Give up on tearing a remote job down: settle the row's debt, count it, and
/// say so by handle.
///
/// This is the honest end of a best-effort contract. A job we cannot reach is a
/// job we cannot stop, and the alternative to admitting that is a row that
/// sweeps forever and an operator who never learns their cluster is still
/// running work for a run they cancelled an hour ago. The log line carries the
/// kind and the handle precisely so it can be acted on by hand.
async fn orphan(
    pool: &db::Pool,
    metrics: &dagron_core::metrics::Metrics,
    row: &dagron_core::models::ExternalCancel,
    why: &str,
) -> anyhow::Result<()> {
    if db::clear_external_handle(pool, &row.id, &row.external_handle).await? {
        metrics.inc_external_orphans();
        warn!(
            task_id = %row.id,
            run_id = %row.run_id,
            kind = %row.external_kind,
            handle = %row.external_handle,
            "ORPHANED a remote job: {why}. The task is terminal but the job may still be \
             running and consuming cluster-hours — stop it by hand using this handle. \
             (scheduler_external_orphans_total)"
        );
    }
    Ok(())
}

/// The task's `defer.http` block, when it declares a `cancel:` — read from the
/// spec JSON persisted on the row, like the poll path reads its own block.
/// `None` means nothing generic can stop this job.
fn http_with_cancel(input: Option<&str>) -> Option<dag::DeferHttpSpec> {
    input
        .and_then(|j| serde_json::from_str::<dag::TaskSpec>(j).ok())
        .and_then(|t| t.defer)
        .and_then(|d| d.http)
        .filter(|h| h.cancel.is_some())
}

/// How the teardown sweep could stop this job, named for the failure reason, or
/// `None` when nothing here can stop it.
///
/// Both transports count, because the sweep tries both: a registered
/// [`ExternalPoller`] gets first refusal and `defer.http.cancel` is the generic
/// fallback. Only `defer.http.cancel` was checked before, so a deployment whose
/// poller CAN cancel still had its handle cleared at the `max_wait_secs` ceiling
/// and was never asked — the handle IS the teardown debt, so dropping it here is
/// exactly what leaves a job running with nothing tracking it.
///
/// A poller that turns out not to claim this kind answers `Ok(None)`, which the
/// sweep releases without spending an attempt; the row then ages out to an orphan
/// and increments `scheduler_external_orphans_total`. That is the honest end for a
/// job this engine could not stop, and better than the silent drop it replaces.
fn cancel_transport(input: Option<&str>, has_poller: bool) -> Option<&'static str> {
    match (has_poller, http_with_cancel(input).is_some()) {
        (true, true) => Some("the registered poller, else defer.http.cancel"),
        (true, false) => Some("the registered poller"),
        (false, true) => Some("defer.http.cancel"),
        (false, false) => None,
    }
}

/// Cache key for a sweep's resolved `defer.http` headers.
///
/// Both halves are load-bearing. `run_id` scopes the resolution to the run
/// whose `environment:` supplied the secrets. `block` is the *unresolved*
/// header list — the spec as authored, credentials still as `value_from`
/// references — so two tasks in one run that name different secrets get
/// different entries, and two that name the same one still share a resolution.
///
/// The unresolved form is what is compared, deliberately: it is what
/// distinguishes the specs, and it never holds a plaintext credential.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct HeaderCacheKey {
    run_id: String,
    block: String,
}

impl HeaderCacheKey {
    /// `None` for a spec that will not serialise, which means *do not cache* —
    /// never share a degenerate key with every other such spec. Resolving twice
    /// costs two queries; sharing costs a credential.
    fn new(run_id: &str, headers: &[dag::EnvVar]) -> Option<Self> {
        serde_json::to_string(headers)
            .ok()
            .map(|block| Self { run_id: run_id.to_string(), block })
    }
}

/// Poll one parked job over `defer.http` and return its verdict.
///
/// Every step that can fail returns `Err`, which the sweep treats as "no
/// answer" and re-parks — never as a verdict about the job. A vendor that is
/// rate-limiting us, or briefly 503ing, must not fail a six-hour run.
///
/// `secrets` is the caller's per-sweep cache: resolution is two DB queries plus
/// an AES-GCM decrypt, and a batch of parked jobs sharing one header block
/// shares one resolution.
///
/// **The key is the run AND the unresolved header block, never the run alone.**
/// `headers` is a property of the *task*, so two deferred tasks in one run can
/// name different credentials for different vendors. Keying on `run_id` would
/// hand the second task the first one's resolved headers — sending vendor A's
/// bearer token to vendor B, and authenticating as the wrong principal even
/// where it did not leak. A cache that can answer with someone else's
/// credential is not a cache.
/// Everything one parked row needs to be polled off the reconcile thread.
///
/// Carries the *resolved* headers rather than the pool, which is what makes the
/// poll safe to spawn: no datastore handle, no shared cache, nothing to
/// contend on.
struct PollPlan {
    /// The `defer.http` spec and its resolved headers, when the task declares
    /// one and they resolved. `None` means the built-in transport is not
    /// available for this row — see `http_unresolved` for which of the two
    /// reasons.
    spec: Option<(dag::DeferHttpSpec, Vec<dag::EnvVar>)>,
    /// The task DOES declare a `defer.http:` block, but its headers would not
    /// resolve this sweep.
    ///
    /// Distinguished from "declares none" because the two deserve different
    /// treatment: a missing block means nothing in this build can ever resolve
    /// the row, which is worth saying once per kind; an unresolvable credential
    /// is a transient the next sweep retries, and telling the operator to
    /// register a poller would point them at the wrong problem entirely.
    http_unresolved: bool,
}

/// One row's verdict, from whichever resolver owns it.
///
/// A registered [`hooks::ExternalPoller`] gets first refusal — it is installed
/// for a kind, so it is the more specific thing — and the built-in `defer.http`
/// transport is the fallback. `None` means nothing resolved it and the row
/// stays parked, which is deliberately different from failing it.
///
/// Every error path here returns `None`, never a verdict. A 429, a reset
/// connection, an expired timeout: none of them say anything about the remote
/// job, and encoding one as `Failed` would kill a healthy six-hour run because
/// its vendor rate-limited us.
/// Returns the verdict and whether the row was left **unresolvable** — reached
/// the end with no resolver claiming it, which is different from a resolver
/// having tried and failed. Only the first deserves the once-per-kind warning;
/// the second is a transient the next sweep retries.
async fn poll_one(
    poller: &Option<Arc<dyn hooks::ExternalPoller>>,
    client: &reqwest::Client,
    park: &dagron_core::models::ExternalPark,
    plan: &PollPlan,
) -> (Option<hooks::Verdict>, bool) {
    let ctx = hooks::PollCtx {
        kind: &park.external_kind,
        handle: &park.external_handle,
        endpoint: park.external_endpoint.as_deref(),
        run_id: &park.run_id,
        task_id: &park.id,
        epoch: park.external_epoch,
    };
    if let Some(p) = poller {
        // The trait carries no timeout contract, so this call site supplies
        // one. Concurrency alone would not be enough: without a deadline a
        // single blocked implementation holds a JoinSet slot forever, and the
        // sweep that awaits the set never finishes its pass.
        let registered = tokio::time::timeout(
            std::time::Duration::from_secs(EXTERNAL_POLLER_TIMEOUT_SECS),
            p.poll(&ctx),
        )
        .await
        .unwrap_or_else(|_| {
            Err(anyhow::anyhow!(
                "registered ExternalPoller for kind '{}' did not answer within {}s — \
                 re-parking; a timeout is not a verdict about the job",
                park.external_kind,
                EXTERNAL_POLLER_TIMEOUT_SECS
            ))
        });
        match registered {
            Ok(Some(v)) => return (Some(v), false),
            Err(e) => {
                warn!(
                    task_id = %park.id, handle = %park.external_handle,
                    kind = %park.external_kind, error = %e,
                    "deferred job poll failed — re-parking (not a verdict about the job)"
                );
                // It reached for this row and could not answer. Something owns
                // the kind, so this is not the "nothing resolves it" case.
                return (None, false);
            }
            // No registered poller claimed this kind — fall through to the
            // built-in transport.
            Ok(None) => {}
        }
    }
    // No registered poller claimed the kind. Without a usable http block too,
    // nothing resolved this row — but only a MISSING block means nothing in this
    // build ever could. Headers that failed to resolve are a transient, already
    // logged once with the actual cause.
    let Some((spec, headers)) = plan.spec.as_ref() else {
        return (None, !plan.http_unresolved);
    };
    match poll_defer_http(client, park, spec, headers).await {
        Ok(v) => (Some(v), false),
        Err(e) => {
            warn!(
                task_id = %park.id, handle = %park.external_handle, error = %e,
                "defer.http poll failed — re-parking (not a verdict about the job)"
            );
            (None, false)
        }
    }
}

/// Resolve one spec's `value_from` header refs, through the sweep's cache.
///
/// Split from the poll itself because this is the only part that touches the
/// **datastore**, and the poll is now run concurrently. Resolving here — in the
/// parent, serially, before anything is spawned — is what lets the cache stay a
/// plain `&mut HashMap` instead of becoming a shared mutex that every in-flight
/// poll contends on. It also keeps the resolution order deterministic, so a
/// batch sharing one credential still performs exactly one resolution.
async fn resolve_defer_headers(
    pool: &db::Pool,
    run_id: &str,
    spec: &dag::DeferHttpSpec,
    secrets: &mut std::collections::HashMap<HeaderCacheKey, Vec<dag::EnvVar>>,
) -> anyhow::Result<Vec<dag::EnvVar>> {
    // Resolve the headers' `value_from` refs once per (run, header block) per
    // sweep. A spec that will not serialise is not cached at all rather than
    // sharing a degenerate key with every other such spec — resolving twice
    // costs two queries, sharing costs a credential.
    let cache_key = HeaderCacheKey::new(run_id, &spec.headers);
    if let Some(h) = cache_key.as_ref().and_then(|k| secrets.get(k)) {
        return Ok(h.clone());
    }
    let mut h = spec.headers.clone();
    environments::resolve_secrets(pool, run_id, &mut h).await?;
    if let Some(k) = cache_key {
        secrets.insert(k, h.clone());
    }
    Ok(h)
}

/// Poll one parked job over `defer.http`, given headers already resolved.
///
/// Everything here is network or pure, which is what makes it safe to run
/// concurrently with the rest of the batch: no pool, no cache, no shared state.
async fn poll_defer_http(
    client: &reqwest::Client,
    park: &dagron_core::models::ExternalPark,
    spec: &dag::DeferHttpSpec,
    headers: &[dag::EnvVar],
) -> anyhow::Result<hooks::Verdict> {
    // The redactor is built from the RESOLVED headers, so it holds the actual
    // credential and masks it out of anything this function writes back to the
    // task — `value_from` marks a value secret whatever the header is called.
    let redactor = dagron_executor::redact::Redactor::from_task_env(headers);
    let pairs: Vec<(String, String)> =
        headers.iter().map(|e| (e.name.clone(), e.value.clone())).collect();

    // `{{ handle }}` is runtime state: expansion left it verbatim precisely so
    // it could be bound here, against the handle on the row.
    let url = spec.url.replace(dag::HANDLE_PLACEHOLDER, &park.external_handle);

    let doc = defer_http::fetch(client, &url, &pairs)
        .await
        // Redact before the error text goes anywhere: a vendor's error envelope
        // can echo the Authorization header straight back.
        .map_err(|e| anyhow::anyhow!("{}", redactor.redact(&e.to_string()).into_owned()))?;

    if defer_http::predicates_overlap(spec, &doc) {
        warn!(
            task_id = %park.id,
            succeed_when = %spec.succeed_when,
            fail_when = %spec.fail_when.as_deref().unwrap_or(""),
            "defer.http succeed_when and fail_when BOTH matched — one of them is wrong. \
             Treating the job as failed, because a false success advances dependents on a job \
             that produced nothing while a false failure only costs a retry."
        );
    }
    let verdict = defer_http::decide(spec, &doc)?;
    Ok(match verdict {
        hooks::Verdict::Failed { reason } => hooks::Verdict::Failed {
            reason: redactor.redact(&reason).into_owned(),
        },
        other => other,
    })
}

/// Record a task's `produces:` dataset updates after a **fenced** success.
///
/// Count and meter one task's terminal transition — **the only place** either
/// happens.
///
/// `Meter` is the quota seam: an alternate build accounts usage here and
/// enforces limits like `max_tasks_per_day`. That only bounds anything if every
/// path that lands a task in `succeeded` or `failed` calls it, and until this
/// function existed only three did — all on the worker-result path. A task that
/// parks holds no worker and resolves in a reconcile sweep, so it never
/// traverses that path: wait sensors, `wait.url`, `wait.dataset`, sub-workflow
/// triggers, approval gates, deferred `defer:` jobs and memoization cache hits
/// were metered ZERO times, and `scheduler_tasks_{succeeded,failed}_total`
/// under-reported by exactly the same set. A quota that silently stops bounding
/// half the engine is worse than no quota, because it reads as enforced.
///
/// Call it **once per row this process actually transitioned** — inside the
/// `if` on a guarded mutation's bool, or per element of a sweep's returned
/// resolutions (those already contain only the rows that sweep landed). A
/// stale-fence mutation changed nothing and must not be counted.
///
/// **Cancellation is deliberately not here.** `cancel_overdue_runs` and
/// `cancel_gang_siblings` terminalize rows as `cancelled`, which is neither
/// arm of this bool: a cancelled task did not succeed, and calling it a failure
/// would inflate the failure rate and spend the very quota the seam exists to
/// protect. Counting cancellations needs its own signal, not a coerced one.
async fn task_finished(metrics: &Metrics, seams: &Seams, success: bool) {
    if success {
        metrics.inc_succeeded();
    } else {
        metrics.inc_failed();
    }
    seams.meter.on_task_completed(success).await;
}

/// Shared by the two paths that can succeed a producer task: the ordinary worker
/// result and a memoization cache hit. A cache hit still records — `produces:` is
/// a postcondition ("after this task succeeds, the dataset is current"), and
/// staying silent would leave a downstream sensor or `on_datasets:` consumer
/// parked forever whenever the producer happens to hit its cache. Best-effort:
/// a ledger failure is logged, never fatal to the run.
async fn record_produces(
    pool: &db::Pool,
    metrics: &Metrics,
    workflow: &str,
    task_id: &str,
    task_name: &str,
    produces: &[String],
) {
    if produces.is_empty() {
        return;
    }
    match db::record_dataset_updates(pool, workflow, task_id, task_name, produces).await {
        Ok(()) => {
            metrics.inc_dataset_updates();
            info!(%task_id, datasets = produces.len(), "dataset update(s) recorded");
        }
        Err(e) => tracing::warn!(
            error = %e, %task_id,
            "dataset update record failed (best-effort)"
        ),
    }
}

/// Redact secrets from a connection target before logging: mask sensitive
/// query-string params (`password`, `token`, …) and strip URL userinfo
/// (`user:pass@`) so nothing from `DATABASE_URL` reaches the logs. SQLite file
/// paths have neither and pass through unchanged.
fn redact_conn(target: &str) -> String {
    const SENSITIVE: [&str; 6] =
        ["password", "passwd", "pwd", "secret", "token", "access_token"];
    // Mask sensitive query params first (e.g. `…?sslmode=require&password=hunter2`).
    let scrubbed = match target.split_once('?') {
        Some((base, query)) => {
            let masked = query
                .split('&')
                .map(|kv| match kv.split_once('=') {
                    Some((k, _)) if SENSITIVE.contains(&k.to_ascii_lowercase().as_str()) => {
                        format!("{k}=<redacted>")
                    }
                    _ => kv.to_string(),
                })
                .collect::<Vec<_>>()
                .join("&");
            format!("{base}?{masked}")
        }
        None => target.to_string(),
    };
    // Strip URL userinfo.
    if let Some(scheme_end) = scrubbed.find("://") {
        let (scheme, rest) = scrubbed.split_at(scheme_end + 3);
        if let Some(at) = rest.find('@') {
            return format!("{scheme}<redacted>@{}", &rest[at + 1..]);
        }
    }
    scrubbed
}

/// Resolve the `MAX_INFLIGHT_RUNS` admission cap, defaulting to 64.
///
/// **`0` disables the cap** — the contract the Helm chart and its `values.yaml`
/// have always documented. Both admission sites read it that way: `api.rs` gates
/// its `POST /runs` check on `max_inflight_runs > 0`, and the ingest actor's
/// throttle is gated the same way. Anything below zero is a nonsense cap rather
/// than a distinct mode, so it normalizes to `0` (disabled) instead of silently
/// meaning something else.
///
/// This used to clamp with `.max(1)`, which made the documented `0` unreachable
/// and turned "disable the cap" into "cap at one active run" — a near-total
/// stall for a deployment that set it.
fn parse_max_inflight_runs(raw: Option<String>) -> i64 {
    raw.and_then(|v| v.trim().parse::<i64>().ok())
        .unwrap_or(64)
        .max(0)
}

/// Seed the GitOps workflow directory with the image's bundled examples on first
/// start (when the dir is empty). Ported from the old `docker-entrypoint.sh` so
/// the container can run on a shell-less, lightweight base (distroless). Best
/// effort: a no-op when the bundled examples aren't present (local dev) and
/// non-fatal on any I/O error — never block the daemon from starting.
///
/// `WORKFLOW_DIR` (env, default `/workflows`) overrides the target directory —
/// point it at a mounted volume to manage workflows via GitOps. Both `.yaml` and
/// `.yml` example files are seeded.
fn seed_workflow_dir() {
    let target = std::env::var("WORKFLOW_DIR").unwrap_or_else(|_| "/workflows".to_string());
    let examples = std::path::Path::new("/etc/dagron/examples");
    // Only relevant inside the image (examples baked in); skip otherwise.
    if !examples.is_dir() {
        return;
    }
    let target = std::path::Path::new(&target);
    if let Err(e) = std::fs::create_dir_all(target) {
        tracing::warn!(dir = %target.display(), error = %e, "could not create workflow dir");
        return;
    }
    // Seed only when empty (GitOps init); never clobber a managed volume.
    let empty = std::fs::read_dir(target)
        .map(|mut d| d.next().is_none())
        .unwrap_or(false);
    if !empty {
        return;
    }
    let mut count = 0u32;
    if let Ok(entries) = std::fs::read_dir(examples) {
        for entry in entries.flatten() {
            let path = entry.path();
            let ext = path.extension().and_then(|s| s.to_str());
            if matches!(ext, Some("yaml") | Some("yml")) {
                if let Some(name) = path.file_name() {
                    match std::fs::copy(&path, target.join(name)) {
                        Ok(_) => count += 1,
                        Err(e) => tracing::warn!(file = %path.display(), error = %e, "seed copy failed"),
                    }
                }
            }
        }
    }
    if count > 0 {
        info!(dir = %target.display(), count, "initialized workflow dir with bundled examples");
    }
}

/// The distinct images a spec's tasks declare, in first-appearance order, as
/// one comma-separated line — what `{{ run.images }}` resolves to.
///
/// This is what makes a check on a pull request that changed an image recipe
/// say which image came out of it. The reference is content-addressed, so it is
/// in the spec rather than discovered at run time, and reading it here means an
/// author who wants it in their check does not have to plumb it through as a
/// parameter they would have to compute themselves.
///
/// Empty when no task names an image, which is the ordinary local-executor
/// workflow; a template that mentions `{{ run.images }}` then renders it as
/// nothing, which is the honest answer.
fn run_images(spec: &dag::DagSpec) -> String {
    let mut seen: Vec<&str> = Vec::new();
    let defaulted = spec
        .task_defaults
        .as_ref()
        .and_then(|d| d.docker_image.as_deref());
    for task in &spec.tasks {
        let Some(image) = task.docker_image.as_deref().or(defaulted) else {
            continue;
        };
        if !image.is_empty() && !seen.contains(&image) {
            seen.push(image);
        }
    }
    seen.join(", ")
}

/// Post a terminal commit status for a finalized run, if its spec declares a
/// `notify.git` target. Best-effort: any failure (spec missing, no notify block,
/// forge error) is logged and swallowed so run execution is never affected.
/// [`git_target`] does the resolution — templates against the spec's
/// `parameters` (e.g. `sha: "{{ commit_sha }}"` from the CI caller), plus the
/// `run.*` names only the engine can fill in.
async fn post_forge_status(
    forge: &dagron_forge::ForgeClient,
    pool: &db::Pool,
    run_id: &str,
    status: &str,
) {
    let Some(yaml) = db::spec_for_run(pool, run_id).await.ok().flatten() else {
        return;
    };
    let spec: dag::DagSpec = match serde_yaml::from_str(&yaml) {
        Ok(s) => s,
        Err(_) => return, // a spec this build can't parse; nothing to notify
    };
    let Some(target) = git_target(&spec, run_id, status) else {
        return;
    };
    let state = dagron_forge::CommitState::from_run_status(status);
    if let Err(e) = forge.post_status(&target, state).await {
        tracing::warn!(error = %e, %run_id, "forge commit status post failed");
    }
}

/// Resolve a spec's `notify.git` block into the target to post to. `None` when
/// there is no block (the common case) or when the SHA did not resolve.
///
/// Split out from the posting so the resolution — which is where the templates
/// and the run-scoped names are decided — can be tested without a database or a
/// forge.
fn git_target(spec: &dag::DagSpec, run_id: &str, status: &str) -> Option<dagron_forge::GitTarget> {
    let git = spec.notify.as_ref()?.git.as_ref()?;

    // Resolve templated fields against the workflow parameters, plus four
    // `run.*` names the author cannot supply because only the engine knows
    // them. The dot keeps them out of the way of ordinary parameter names; a
    // parameter perversely named `run.status` loses to the engine's value,
    // which is the safe direction — a status that reports whatever a caller
    // put in a parameter would be worthless.
    let mut ctx = spec.parameters.clone();
    ctx.insert("run.id".into(), run_id.to_string());
    ctx.insert("run.workflow".into(), spec.name.clone());
    ctx.insert("run.status".into(), status.to_string());
    ctx.insert("run.images".into(), run_images(spec));

    let sub = |s: &str| dagron_core::expand::substitute(s, &ctx);
    let target = dagron_forge::GitTarget {
        provider: git.provider.clone(),
        repo: sub(&git.repo),
        sha: sub(&git.sha),
        context: git
            .context
            .as_deref()
            .map(sub)
            .unwrap_or_else(|| "dagron".to_string()),
        target_url: git.target_url.as_deref().map(sub),
        description: git.description.as_deref().map(sub),
    };
    if target.sha.is_empty() || target.sha.contains("{{") {
        tracing::warn!(%run_id, "notify.git sha did not resolve (missing parameter?) — skipping forge status");
        return None;
    }
    Some(target)
}

/// Run the dagron scheduler daemon to completion (or until killed). `seams`
/// selects the extension behaviour; pass `Seams::default()` for the built-in
/// configuration.
pub async fn run(seams: Seams) -> Result<()> {
    // The DAGRON_CONFIG file/profile layer lands FIRST — it may set LOG_LEVEL
    // and any other knob, so it must be applied before anything (including
    // logging) reads the environment. Env always wins over file over profile.
    let cfg_layer = config::apply_file_layer()?;

    // Tunable, structured logging for the workflow controller (and the worker
    // pool it spawns). Verbosity/format are env-driven — see the shared
    // `dagron_logging` crate for the full knob list (RUST_LOG / LOG_LEVEL /
    // LOG_FORMAT / …).
    dagron_logging::init("controller");

    let args: Vec<String> = std::env::args().collect();

    // `dagron validate <file|dir>... [--json]` — offline spec lint through the
    // same parse → expand → graph-validate pipeline every submit path uses.
    // Handled before any daemon setup — and before `seed_workflow_dir` — so the
    // subcommand stays side-effect free (no datastore, no executor, no server,
    // no file seeding).
    // Both this and `archive-compact` below flush the exporter before returning:
    // logging (and, in an `otel` build, the batch span exporter) is already up by
    // the time we get here, so a bare `return` would drop everything buffered
    // since the last export interval — which for a subcommand that lives a
    // second or two is the entire run.
    if args.get(1).map(String::as_str) == Some("validate") {
        let result = validate::run_cli(&args[2..]);
        dagron_logging::shutdown();
        return result;
    }

    // `dagron archive-compact [db_target]` — one bounded sweep folding archived
    // run documents into the Parquet dataset (k8s CronJob shape). Like
    // `validate`, handled before daemon setup; unlike it, it needs the sink env
    // (GC_ARCHIVE_DIR / GC_ARCHIVE_URL) and optionally a datastore to stamp
    // `archived_runs`. Feature-gated: without `archive-parquet` the subcommand
    // is a clear startup error, never a silent no-op.
    if args.get(1).map(String::as_str) == Some("archive-compact") {
        #[cfg(feature = "archive-parquet")]
        {
            let result = archive_compact::run_cli(&args[2..]).await;
            dagron_logging::shutdown();
            return result;
        }
        #[cfg(not(feature = "archive-parquet"))]
        {
            dagron_logging::shutdown();
            anyhow::bail!(
                "`dagron archive-compact` requires building with `--features archive-parquet`"
            );
        }
    }

    // `dagron config [--json]` — print every knob's effective value, source
    // (env / file / profile / default), and the fleet fingerprint, exactly as
    // a daemon started in this environment would resolve them. Like
    // `validate`: handled before any daemon setup, side-effect free.
    if args.get(1).map(String::as_str) == Some("config") {
        // Surface the same typo warnings a daemon boot would (to stderr, so
        // the table on stdout stays parseable): the operator asking "what am
        // I running with" is exactly who needs to hear "…and this var you set
        // does nothing".
        config::warn_unknown_vars();
        let result = config::run_cli(&args[2..]);
        dagron_logging::shutdown();
        return result;
    }

    // One line that pins what this process is actually running with — the
    // fingerprint is the fleet-drift check (two replicas, same fingerprint =
    // same knob values), and the typo scan flags look-alike vars that would
    // otherwise silently do nothing (LOW_LATENCY §5).
    info!(
        fingerprint = %config::fingerprint(),
        config_file = cfg_layer.path.as_deref().unwrap_or("—"),
        profile = cfg_layer.profile.as_deref().unwrap_or("—"),
        "configuration resolved"
    );
    config::warn_unknown_vars();

    // GitOps init: seed /workflows from the image's bundled examples if empty.
    // (Was docker-entrypoint.sh; in-binary now so the image needs no shell.)
    seed_workflow_dir();

    // `dagron dev` (QW2) — curated zero-infra local quickstart: SQLite + the
    // management API/Swagger on a fixed local port, resident so the API (and any
    // schedules/cron) stay up. Positional args shift by one (`dev [dag] [db]`);
    // env still wins, so power users override any default. Edition 2021 → set_var
    // is safe; do it before the rest of main reads the environment.
    let dev_mode = args.get(1).map(|s| s == "dev").unwrap_or(false);
    // `dagron dev` is the API/UI quickstart; without the `ops` feature there is no
    // management server to keep the process resident, so refuse rather than start a
    // daemon that advertises an API and then exits.
    #[cfg(not(feature = "ops"))]
    if dev_mode {
        anyhow::bail!("`dagron dev` requires building with the `ops` feature (the management API)");
    }
    if dev_mode && std::env::var_os("API_ADDR").is_none() {
        std::env::set_var("API_ADDR", "127.0.0.1:8787");
    }

    // Positional args, skipping the `dev` subcommand token when present.
    let pos_offset = if dev_mode { 2 } else { 1 };
    let dag_path = args
        .get(pos_offset)
        .map(String::as_str)
        .unwrap_or("examples/simple_dag.yaml");

    // Datastore target. SQLite takes a file path (defaulting to a local file);
    // Postgres takes a connection string (else $DATABASE_URL).
    let db_target: String = {
        #[cfg(feature = "postgres")]
        {
            args.get(pos_offset + 1)
                .cloned()
                .or_else(|| std::env::var("DATABASE_URL").ok())
                .unwrap_or_else(|| "postgres://localhost/workflow".to_string())
        }
        #[cfg(feature = "sqlite")]
        {
            args.get(pos_offset + 1).cloned().unwrap_or_else(|| "workflow.db".to_string())
        }
    };

    if dev_mode {
        let api_addr = std::env::var("API_ADDR").unwrap_or_default();
        info!(
            "dagron dev — local quickstart: datastore {}, management API + Swagger UI on \
             http://{api_addr}/docs (override with API_ADDR)",
            redact_conn(&db_target)
        );
    }
    let worker_id = format!("worker-{}", uuid::Uuid::new_v4());

    // Executor backend: EXECUTOR=local|docker|k8s (default: local).
    //
    // Classified ONCE, here, into `ExecutorKind`. Two independent readings of
    // this string is how the isolation guard was defeated once already: the
    // enum's parser accepts `kube` and is case- and whitespace-insensitive,
    // while the selection below matched exact lowercase spellings, so
    // `EXECUTOR=kube` type-checked as Kubernetes for the isolation guard and
    // then ran LocalExecutor — a Kubernetes-only envelope passing validation
    // and dispatching to a bare subprocess. One classification, driving both,
    // makes that divergence unrepresentable.
    let executor_raw = std::env::var("EXECUTOR").unwrap_or_else(|_| "local".to_string());
    let executor_kind = match dagron_core::isolation::ExecutorKind::parse(&executor_raw) {
        Some(kind) => kind,
        None => {
            tracing::warn!(executor = %executor_raw, "unrecognized EXECUTOR value, defaulting to local");
            dagron_core::isolation::ExecutorKind::Local
        }
    };
    let worker_count: usize = std::env::var("WORKER_COUNT")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(16)
        .max(1); // guard against WORKER_COUNT=0 stalling the scheduler

    // Runner segmentation: RUNNER_CLASSES=etl,pulse restricts this scheduler to
    // claiming tasks in those classes, so a pool of replicas becomes a dedicated
    // runner for one workload shape. Unset/empty =
    // claim every class — the unsegmented default. Names are validated with the
    // same rule as the spec side so a typo fails at startup, not as an
    // unclaimable-forever task class.
    let runner_classes: Vec<String> = std::env::var("RUNNER_CLASSES")
        .unwrap_or_default()
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    for class in &runner_classes {
        dag::validate_runner_class(class)
            .map_err(|e| anyhow::anyhow!("invalid RUNNER_CLASSES entry: {e}"))?;
    }

    // Trust envelope (family 3): which envelope fields this scheduler's executor
    // can actually deliver. The same `executor_kind` the dispatch path uses, so
    // what is validated is always what will run.
    let executor_isolation = Some(executor_kind);
    // The floor is the privileges no task dispatched by this scheduler may go
    // below, however its workflow was written. Parsed at startup so a malformed
    // floor is a boot failure — a floor discovered to be invalid at dispatch is
    // one that was not in force for everything dispatched before it.
    let isolation_floor = match std::env::var("DAGRON_TASK_ISOLATION_FLOOR") {
        Ok(raw) if !raw.trim().is_empty() => {
            let floor = dagron_core::isolation::IsolationSpec::parse_floor(&raw)
                .map_err(|e| anyhow::anyhow!("invalid DAGRON_TASK_ISOLATION_FLOOR: {e}"))?;
            // A floor this executor cannot deliver is a deployment mistake, and
            // it must be found here rather than one task at a time: every task
            // inherits the floor, so the alternative is an engine that boots
            // cleanly and then fails literally everything it is given.
            if let Some(kind) = executor_isolation {
                floor.require_enforceable_by(kind).map_err(|e| {
                    anyhow::anyhow!("DAGRON_TASK_ISOLATION_FLOOR is not enforceable here: {e}")
                })?;
            }
            info!(floor = %floor.canonical().replace('\n', " "), "task isolation floor in force");
            Some(floor)
        }
        _ => None,
    };

    // Named concurrency pools (#21): POOLS=etl:4,db:2 caps how many tasks in each
    // pool may run at once. A task's `pool:` draws a slot; when a pool is full its
    // tasks wait in `ready` until one frees (no run is dropped). Unset/empty = no
    // pools, and the claim path is unchanged. Names are validated like the spec
    // side; a non-positive or unparseable slot count is a startup error, not a
    // silently-ignored limit.
    let pool_caps: std::collections::BTreeMap<String, i64> = {
        let raw = std::env::var("POOLS").unwrap_or_default();
        let mut m = std::collections::BTreeMap::new();
        for entry in raw.split(',').map(str::trim).filter(|s| !s.is_empty()) {
            let (name, slots) = entry.split_once(':').ok_or_else(|| {
                anyhow::anyhow!("invalid POOLS entry '{entry}': expected name:slots")
            })?;
            let name = name.trim();
            dag::validate_pool(name).map_err(|e| anyhow::anyhow!("invalid POOLS entry: {e}"))?;
            let slots: i64 = slots
                .trim()
                .parse()
                .map_err(|_| anyhow::anyhow!("invalid POOLS slots for '{name}': '{slots}'"))?;
            if slots < 1 {
                anyhow::bail!("POOLS slots for '{name}' must be >= 1, got {slots}");
            }
            // Every other malformed POOLS input is a hard startup error; a
            // silently-overwritten duplicate would be the odd one out.
            if let Some(prev) = m.insert(name.to_string(), slots) {
                anyhow::bail!(
                    "duplicate POOLS entry for '{name}' (slots {prev} then {slots}) — declare each pool once"
                );
            }
        }
        m
    };

    // Gang co-scheduling (enterprise scheduler + RUNNER_GANGS=1): claim gangs
    // all-or-nothing before filling leftover capacity with ordinary tasks, and
    // cancel a failed member's siblings (die-together). Compiled out of the
    // default build via the `enterprise` feature; inert unless opted in.
    let gangs_on = cfg!(feature = "enterprise")
        && std::env::var("RUNNER_GANGS")
            .map(|v| matches!(v.trim(), "1" | "true" | "yes"))
            .unwrap_or(false);
    if gangs_on {
        info!("gang co-scheduling enabled (RUNNER_GANGS) — all-or-nothing gang claims");
    }

    // Ingestion source: SOURCE=file|stream (default: file; `stream` follows an
    // NDJSON file or named pipe with a durable offset checkpoint). Managed
    // broker connectors (kafka/nats/sqs/redis) and the events gateway register
    // through Seams.source_factory. MAX_INFLIGHT_RUNS caps how many runs
    // may be active at once — the admission valve that lets the scheduler absorb
    // a large influx by leaving the overflow buffered in the queue.
    let source_kind = std::env::var("SOURCE").unwrap_or_else(|_| "file".to_string());
    let max_inflight_runs: i64 = parse_max_inflight_runs(std::env::var("MAX_INFLIGHT_RUNS").ok());
    // Second admission dimension. Unset/0 = off, so nothing changes for anyone
    // who has not asked for it: runs remain the only cap unless a task cap is
    // configured. See docs/CONFIG.md for why runs alone are the wrong unit.
    let max_inflight_tasks: i64 = std::env::var("MAX_INFLIGHT_TASKS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(0);
    // How many times a transient create_run failure is retried (nacked) before a
    // submission is dead-lettered. Parse failures dead-letter immediately.
    let dead_letter_max_attempts: i64 = std::env::var("DEAD_LETTER_MAX_ATTEMPTS")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(3)
        .max(1);

    info!(%worker_id, %dag_path, db = %redact_conn(&db_target), executor_kind = ?executor_kind, worker_count, %source_kind, max_inflight_runs, max_inflight_tasks, runner_classes = ?runner_classes, pools = ?pool_caps, "scheduler starting");

    // rustls 0.23 needs a process-level CryptoProvider before the kube client
    // opens TLS to the apiserver (KubeExecutor); install it once at startup.
    #[cfg(feature = "kubernetes")]
    {
        dagron_executor::install_crypto_provider();
    }

    let executor: Arc<dyn executor::Executor> = match executor_kind {
        dagron_core::isolation::ExecutorKind::Docker => {
            let image = std::env::var("DOCKER_IMAGE").unwrap_or_else(|_| "alpine:latest".to_string());
            info!(%image, "using DockerExecutor");
            Arc::new(docker_executor::DockerExecutor::connect(image).await?)
        }
        dagron_core::isolation::ExecutorKind::Kubernetes => {
            // Cluster-gated: only compiled with `--features kubernetes`. Without
            // it, fail clearly rather than silently downgrading to local.
            #[cfg(feature = "kubernetes")]
            {
                let image = std::env::var("K8S_IMAGE")
                    .or_else(|_| std::env::var("DOCKER_IMAGE"))
                    .unwrap_or_else(|_| "alpine:latest".to_string());
                let namespace =
                    std::env::var("K8S_NAMESPACE").unwrap_or_else(|_| "default".to_string());
                info!(%image, %namespace, "using KubeExecutor");
                Arc::new(kube_executor::KubeExecutor::connect(image, namespace).await?)
            }
            #[cfg(not(feature = "kubernetes"))]
            {
                anyhow::bail!(
                    "EXECUTOR=kubernetes requires building with `--features kubernetes`"
                )
            }
        }
        dagron_core::isolation::ExecutorKind::Local => {
            info!("using LocalExecutor");
            Arc::new(LocalExecutor)
        }
    };

    // Process metrics (counters + latency histograms); the management API renders
    // these alongside live datastore gauges at GET /metrics. Created before the
    // worker pool so workers can record per-task durations into it.
    let metrics = Arc::new(Metrics::new());

    let pool = db::init_pool(&db_target).await?;

    // Clock discipline: assess the wall clock against the datastore NOW —
    // awaited, so the first run this process creates already carries a
    // truthful `clock_confidence` — then keep a step detector running for the
    // life of the process. Nothing downstream is gated on the verdict.
    clock::start(pool.clone(), Arc::clone(&metrics), clock::Config::from_env()).await;

    // Free-disk admission floor (`DAGRON_MIN_FREE_BYTES`; SQLite only — a
    // Postgres datastore's disk is not this host's to probe). Enforced inside
    // `db::create_run`; reported here once, through the same probe, so an
    // operator sees the headroom the floor will be judged against.
    #[cfg(feature = "sqlite")]
    {
        let floor = pressure::min_free_bytes();
        if floor > 0 {
            let dir = std::path::Path::new(&db_target)
                .parent()
                .filter(|p| !p.as_os_str().is_empty())
                .unwrap_or_else(|| std::path::Path::new("."));
            match pressure::free_bytes(dir) {
                Ok(free) => info!(
                    dir = %dir.display(), free_bytes = free, min_free_bytes = floor,
                    "free-disk admission floor armed"
                ),
                Err(e) => tracing::warn!(
                    dir = %dir.display(), error = %e, min_free_bytes = floor,
                    "free-disk probe failed — the admission floor fails open"
                ),
            }
        }
    }

    // Lease heartbeat: workers renew a running task's lease every few seconds,
    // so a task may run for hours under the short claim-time lease — the
    // long-task keystone (training jobs, stream consumers). A task whose worker
    // dies stops heartbeating and is reclaimed by the expired-lease sweep
    // exactly as before. TASK_LEASE_HEARTBEAT=false restores the old
    // finish-inside-one-lease behaviour.
    let heartbeat_on = std::env::var("TASK_LEASE_HEARTBEAT")
        .map(|v| !matches!(v.trim().to_ascii_lowercase().as_str(), "false" | "0" | "no"))
        .unwrap_or(true);
    let lease_keeper: Option<Arc<dyn worker::LeaseKeeper>> = if heartbeat_on {
        Some(Arc::new(DbLeaseKeeper { pool: pool.clone() }))
    } else {
        info!("task lease heartbeat disabled (TASK_LEASE_HEARTBEAT) — tasks must finish inside one lease window");
        None
    };

    // Heartbeat cadence derives from the lease window: floor(lease/3), floor
    // 1 s. Floor — not ceiling — division, so the interval can only shrink
    // relative to lease/3: at any `LEASE_SECS` (floored at 3 s), two missed
    // renewals still land the third attempt at or before expiry, and the
    // default window keeps today's exact 10 s cadence (30/3). Ceiling division
    // would break that: lease 5 → 2 s beats, putting the post-miss renewal at
    // t+6 against an expiry of t+5.
    let heartbeat_every =
        std::time::Duration::from_secs((db::lease_secs() / 3).max(1) as u64);
    // Cloned rather than moved: the fleet sweep calls the same executor from
    // the reconcile loop, and it must be the SAME one — a second connection
    // would be a second client to keep healthy for no gain.
    let sweeper = Arc::clone(&executor);
    let workers = WorkerPool::with_lease_keeper(
        worker_count,
        executor,
        Arc::clone(&metrics),
        lease_keeper,
        heartbeat_every,
    )
    .await?;
    info!(size = workers.size(), "worker pool ready");

    // When an ops time-source / API is active the process must stay up after the
    // workflow source drains (future cron/schedule fires, live API). One-shot
    // file runs with no ops time-source still exit cleanly. Always-declared so the
    // drain check below compiles regardless of the `ops` feature.
    #[allow(unused_mut)]
    let mut stay_resident = false;

    // ── v5/v6 ops: management API + leadership-gated cron and retention GC ──
    // Compiled only with the (default) `ops` feature; a lean build drops this
    // block along with axum/cron. Each piece is also opt-in via env, so even the
    // full build stays a plain scheduler until configured. Cron and GC must run
    // on exactly one node, so they share a leadership lease (acquired only when
    // at least one of them is enabled).
    #[cfg(feature = "ops")]
    {
        let cron_config = std::env::var("CRON_CONFIG").ok();
        let gc_retention_secs: Option<i64> = std::env::var("GC_RETENTION_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .filter(|&n| n > 0);
        // DB-backed UI schedules are opt-in (server mode): enabling them keeps the
        // daemon resident so future fires happen, which would otherwise stop a
        // one-shot `dagron file.yaml` from exiting after its run drains.
        let db_schedules_on = std::env::var("DB_SCHEDULES")
            .map(|v| v == "1" || v.eq_ignore_ascii_case("true"))
            .unwrap_or(false);
        // Automatic backfill & self-healing (behind the `enterprise` feature):
        // opt-in via AUTO_BACKFILL=1. Like DB schedules it is a resident
        // time-source, so a
        // one-shot run does not exit while the loop is armed to heal future gaps.
        #[cfg(feature = "enterprise")]
        let auto_backfill_cfg = backfill::Config::from_env();

        // API_ADDR only makes this a server if it actually parses and the API
        // spawns. If it's set but invalid the API is disabled, so it must not
        // count toward stay_resident — otherwise a one-shot run would hang with
        // no API to drain.
        // Run-admission gate (`DAGRON_ADMISSION_FILE`). Read once here and cloned
        // into each admission site; the gate itself holds only a path, and every
        // read hits the file, so the clones cannot disagree about the verdict.
        let admission_gate = pressure::AdmissionGate::from_env();
        if let Some(p) = admission_gate.path() {
            info!(path = %p.display(), "admission gate armed — new runs are refused while this file exists");
        }

        let mut api_on = false;
        if let Ok(addr_raw) = std::env::var("API_ADDR") {
            match addr_raw.parse::<std::net::SocketAddr>() {
                Ok(addr) => {
                    api_on = true;
                    let state = api::ApiState {
                        pool: pool.clone(),
                        metrics: Arc::clone(&metrics),
                        max_inflight_runs,
                        max_inflight_tasks,
                        admission: admission_gate.clone(),
                    };
                    tokio::spawn(async move {
                        if let Err(e) = api::serve(addr, state).await {
                            tracing::error!(error = %e, "management API stopped");
                        }
                    });
                }
                Err(e) => {
                    tracing::warn!(addr = %addr_raw, error = %e, "invalid API_ADDR — management API disabled")
                }
            }
        }
        // Any of these makes the process a long-running server, not a one-shot run.
        stay_resident = api_on
            || cron_config.is_some()
            || gc_retention_secs.is_some()
            || db_schedules_on;
        #[cfg(feature = "enterprise")]
        { stay_resident = stay_resident || auto_backfill_cfg.is_some(); }

        let needs_leadership = cron_config.is_some()
            || gc_retention_secs.is_some()
            || db_schedules_on
            // #18: pace backfill jobs in any resident daemon (they can be created
            // via the API whenever the server is up, independent of DB_SCHEDULES).
            || stay_resident;
        #[cfg(feature = "enterprise")]
        let needs_leadership = needs_leadership || auto_backfill_cfg.is_some();
        if needs_leadership {
            let lease_secs: i64 = std::env::var("LEADER_LEASE_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .filter(|&n| n > 0)
                .unwrap_or(30);
            let is_leader =
                leadership::spawn(pool.clone(), "ops".to_string(), worker_id.clone(), lease_secs);

            // DB-backed UI schedules — leadership-gated firing of first-class workflows.
            if db_schedules_on {
                let (p, l, m) = (pool.clone(), Arc::clone(&is_leader), Arc::clone(&metrics));
                let g = admission_gate.clone();
                tokio::spawn(async move { schedule::run(p, l, m, g).await });
            }

            // First-class paced backfill jobs (#18) — leadership-gated, always on in
            // a resident daemon so an API-created job is paced to completion.
            {
                let (p, l, m) = (pool.clone(), Arc::clone(&is_leader), Arc::clone(&metrics));
                tokio::spawn(async move { backfill_jobs::run(p, l, m).await });
            }

            // Automatic backfill & self-healing (behind the `enterprise` feature) —
            // leadership-gated catch-up of missed fires + auto-rerun of failed runs,
            // republishing schedule lag / incomplete-run state as metrics.
            #[cfg(feature = "enterprise")]
            if let Some(cfg) = auto_backfill_cfg {
                let (p, l, m) = (pool.clone(), Arc::clone(&is_leader), Arc::clone(&metrics));
                tokio::spawn(async move { backfill::run(p, cfg, l, m).await });
            }

            if let Some(path) = cron_config {
                match cron::load(&path).await {
                    Ok(entries) => {
                        let (p, l, m) = (pool.clone(), Arc::clone(&is_leader), Arc::clone(&metrics));
                        let g = admission_gate.clone();
                        tokio::spawn(async move { cron::run(p, entries, l, m, g).await });
                    }
                    Err(e) => tracing::error!(%path, error = %e, "cron config invalid — cron disabled"),
                }
            }

            if let Some(retention) = gc_retention_secs {
                let interval: u64 = std::env::var("GC_INTERVAL_SECS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(3600);
                // Archive-before-purge (the hot/cold split):
                // GC_ARCHIVE_URL (s3://…, feature archive-s3) or GC_ARCHIVE_DIR
                // — expired runs are exported to the sink and only verified
                // exports are purged. A misconfigured sink is a startup error:
                // never fall back to purging unarchived history.
                let archive = gc::ArchiveSink::from_env()?;
                let (p, l) = (pool.clone(), Arc::clone(&is_leader));
                tokio::spawn(async move { gc::run(p, retention, interval, l, archive).await });
            }

            // Stale-ready (unclaimable-class) alert — on by default in any
            // resident daemon; READY_AGE_ALERT_SECS=0 disables.
            let ready_alert_secs: i64 = std::env::var("READY_AGE_ALERT_SECS")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(300);
            if ready_alert_secs > 0 {
                let check_interval: u64 = std::env::var("READY_AGE_CHECK_INTERVAL_SECS")
                    .ok()
                    .and_then(|v| v.parse().ok())
                    .unwrap_or(60);
                let (p, l) = (pool.clone(), Arc::clone(&is_leader));
                tokio::spawn(async move {
                    stale_ready::run(p, ready_alert_secs, check_interval, l).await
                });
            }
        }
    }

    // Reconcile-loop waker. On Postgres this is a LISTEN/NOTIFY listener that
    // wakes the loop the instant any worker changes task readiness; on SQLite it
    // degrades to the fixed-interval timer.
    let mut waker = db::Waker::connect(&pool).await?;

    // Build the configured ingestion source and start the ractor ingest actor.
    // It pulls workflow submissions and calls db::create_run for each, applying
    // MAX_INFLIGHT_RUNS admission backpressure. For the default `file` source it
    // emits one DAG then drains; for queue sources it streams indefinitely.
    // In `dagron dev` with no DAG file present, start with no initial run — just
    // serve the API/UI — instead of letting the file source error-loop on a
    // missing path. Submit work via the API once the server is up.
    let wf_source: Box<dyn source::WorkflowSource> = if dev_mode
        && source_kind == "file"
        && !std::path::Path::new(dag_path).exists()
    {
        info!(
            dag = %dag_path,
            "dagron dev — no DAG file found; starting with no initial run (submit via POST /runs)"
        );
        let (tx, rx) = tokio::sync::mpsc::channel::<String>(1);
        drop(tx); // immediately drained → no initial run; the API keeps the daemon resident
        Box::new(source::ChannelSource::new(rx))
    } else {
        source::build_pooled(&source_kind, dag_path, &pool, seams.source_factory.as_deref())
            .await?
    };
    let exhausted = Arc::new(AtomicBool::new(false));
    let (_ingest_ref, _ingest_handle) = IngestActor::spawn(
        Some("ingest".to_string()),
        IngestActor,
        IngestArgs {
            pool: pool.clone(),
            source: wf_source,
            max_inflight_runs,
            max_inflight_tasks,
            exhausted: Arc::clone(&exhausted),
            metrics: Arc::clone(&metrics),
            source_name: source_kind.clone(),
            max_validation_attempts: dead_letter_max_attempts,
        },
    )
    .await
    .map_err(|e| anyhow::anyhow!("spawn ingest actor: {e}"))?;

    let (tx, mut rx) = mpsc::unbounded_channel::<worker::TaskResult>();

    // Live-log stream (#17): workers push incremental output chunks here as tasks
    // run; the loop drains them each tick and appends to the task's stored output
    // for tailing. Separate from the result channel so partial output is visible
    // before the task's terminal result arrives.
    let (log_tx, mut log_rx) = mpsc::unbounded_channel::<dagron_executor::executor::LogChunk>();

    // Tick pacing (docs/LOW_LATENCY.md A-2/A-4). `POLL_INTERVAL_MS` bounds how
    // long the loop may sleep with work outstanding: on Postgres the timer is a
    // safety net behind LISTEN/NOTIFY and the completion wake below; on SQLite
    // it is the only timer-based wake source, so it also paces time-based
    // retries and parked sensors. Floor 10 ms — below that the tick's own query
    // work dominates and the loop degrades into a busy poll.
    let poll_interval_ms: u64 = std::env::var("POLL_INTERVAL_MS")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(500)
        .max(10);
    let poll_interval = std::time::Duration::from_millis(poll_interval_ms);
    // Maintenance sweeps (lease recovery, deadlines, approvals, sensors, …) run
    // on their own cadence instead of on every tick, so a completion- or
    // NOTIFY-woken tick pays only the hot path (advance → claim → dispatch →
    // drain → reap). Defaults to the poll interval — at stock settings that is
    // today's behaviour exactly; profiles that shrink `POLL_INTERVAL_MS` set
    // this back to ~500 ms so faster ticks don't multiply sweep load.
    let sweep_interval_ms: u64 = std::env::var("SWEEP_INTERVAL_MS")
        .ok()
        .and_then(|v| v.trim().parse().ok())
        .unwrap_or(poll_interval_ms)
        .max(10);
    let sweep_interval = std::time::Duration::from_millis(sweep_interval_ms);
    info!(
        poll_interval_ms,
        sweep_interval_ms,
        lease_secs = db::lease_secs(),
        heartbeat_secs = heartbeat_every.as_secs(),
        "tick pacing configured"
    );
    // Which dagron installation this engine is, for the fleet sweep. Unset
    // means the sweep does not run — see `LABEL_INSTALLATION`: a sweep deletes
    // workloads whose task is ABSENT from its datastore, and a foreign
    // installation's live workload is indistinguishable from that. Off is the
    // only safe default, and saying so at boot beats a metric that stays zero
    // for a reason nobody can see.
    let installation = std::env::var(dagron_executor::executor::DAGRON_INSTALLATION)
        .ok()
        .map(|v| v.trim().to_string())
        .filter(|v| !v.is_empty());
    let orphan_sweep_interval = std::time::Duration::from_secs(
        std::env::var("DAGRON_ORPHAN_SWEEP_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(ORPHAN_SWEEP_SECS_DEFAULT)
            .max(30),
    );
    let orphan_min_age = std::time::Duration::from_secs(
        std::env::var("DAGRON_ORPHAN_MIN_AGE_SECS")
            .ok()
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(ORPHAN_MIN_AGE_SECS_DEFAULT)
            .max(60),
    );
    match installation.as_deref() {
        Some(inst) => info!(
            installation = %inst,
            sweep_secs = orphan_sweep_interval.as_secs(),
            min_age_secs = orphan_min_age.as_secs(),
            "fleet sweep armed — leftover workloads carrying this installation's label whose \
             task is no longer live will be deleted"
        ),
        None => info!(
            "fleet sweep OFF ({} unset). Leftover pods/containers from a scheduler that died \
             mid-task are not collected; each dispatch still reaps its own task's predecessors. \
             Set it to a name unique to this installation to arm the sweep.",
            dagron_executor::executor::DAGRON_INSTALLATION
        ),
    }

    // None → the first tick always sweeps (recovery must not wait a window).
    let mut last_sweep: Option<std::time::Instant> = None;
    // Likewise, and it matters more here: a scheduler that has just restarted
    // after a crash is exactly when leftovers exist.
    let mut last_orphan_sweep: Option<std::time::Instant> = None;

    // Simple counter: how many tasks are currently in-flight inside the worker pool.
    let mut in_flight: usize = 0;

    // Results the completion wake (A-1) pulled out of the channel while the
    // loop was between ticks; drained ahead of the channel in step 4 so
    // arrival order is preserved.
    let mut pending_results: Vec<worker::TaskResult> = Vec::new();

    // Optional OpenLineage emitter (data lineage parity). Off unless
    // OPENLINEAGE_URL is set; emits a terminal RunEvent per finalized run.
    let lineage = dagron_lineage::OpenLineageClient::from_env();
    if lineage.is_some() {
        info!("OpenLineage emit enabled");
    }

    // Optional forge feedback (GitHub/GitLab commit statuses). Active when a
    // GITHUB_TOKEN / GITLAB_TOKEN is set; a run whose spec carries a `notify.git`
    // block posts its terminal status on finalization. Best-effort, like lineage.
    let forge = dagron_forge::ForgeClient::from_env();
    if forge.is_some() {
        info!("forge feedback enabled (notify.git commit statuses)");
    }

    // Optional artifact store for passing files between tasks. When
    // DAGRON_ARTIFACT_DIR is set, each dispatched task gets a per-run shared dir
    // via `DAGRON_ARTIFACTS` so tasks in a run can pass files. Off otherwise.
    let artifact_store = dagron_artifact::LocalFsStore::from_env();
    if artifact_store.is_some() {
        info!("artifact store enabled (DAGRON_ARTIFACTS injected per task)");
    }

    // HTTP wait-sensor polling (#27 follow-on): parked `wait.url` tasks are GETed
    // every WAIT_POLL_SECS (default 15 s) and succeed on the first 2xx. The client
    // is built once; a short per-request timeout keeps a hung endpoint from
    // stalling the reconcile tick.
    // How deep `type: workflow` may nest (#23). A workflow that names itself —
    // or a cycle of workflows — otherwise spawns child runs without end, each
    // leaving a parked parent behind; the cap turns that into one clear task
    // failure. 8 is well past any legitimate composition depth.
    let subworkflow_max_depth: i64 = std::env::var("SUBWORKFLOW_MAX_DEPTH")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&d| d > 0)
        .unwrap_or(8);

    let wait_poll_secs: u64 = std::env::var("WAIT_POLL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&s| s > 0)
        .unwrap_or(15);
    // Re-park interval for a deferred job whose poll did not resolve it. The
    // authored `defer.poll_secs` sets the FIRST poll at park time; this is the
    // cadence thereafter, and it is an operator knob rather than the author's
    // number because the thing it protects — the vendor's rate limit — belongs
    // to whoever runs the engine, not to whoever wrote the workflow.
    let external_poll_secs: u64 = std::env::var("EXTERNAL_POLL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .filter(|&s| s > 0)
        .unwrap_or(dag::DEFAULT_DEFER_POLL_SECS);
    // Redirects are disabled: the sensor only needs the origin's own status, and
    // following redirects would let an innocuous-looking external `wait.url`
    // pivot the *scheduler's* network position to loopback / link-local / cloud
    // metadata (a scheme-prefix check cannot see a redirect target). A 3xx simply
    // reads as "not ready" and re-parks.
    let mut wait_http_builder = reqwest::Client::builder()
        .redirect(reqwest::redirect::Policy::none())
        .timeout(std::time::Duration::from_secs(10));
    // Opt-in for operators who run untrusted specs: refuse to poll anything that
    // is not globally routable, enforced in the resolver the client dials with
    // so a DNS rebind has no window to exploit. Off by default — the common
    // `wait.url` *is* an in-cluster address. See `wait_url` for the threat model.
    let wait_url_deny_private = wait_url::deny_private_enabled();
    if wait_url_deny_private {
        wait_http_builder = wait_http_builder
            .dns_resolver(std::sync::Arc::new(wait_url::PublicOnlyResolver))
            // reqwest honours HTTP_PROXY / HTTPS_PROXY / ALL_PROXY by default.
            // With a proxy in play the resolver only ever sees the *proxy's*
            // host — the blocked target travels in the request line and the
            // proxy dials it — so the whole policy would rest on the proxy's
            // own egress rules. Under this policy the scheduler goes direct.
            .no_proxy();
        info!("wait.url sensors restricted to globally-routable addresses, proxies bypassed (WAIT_URL_DENY_PRIVATE)");
    }
    // The defer.http poller's own client. Separate from wait_http because the
    // network policy inverts: that one issues an unauthenticated GET and
    // permits private addresses by default; this one carries the task's
    // headers, so it refuses them unless DEFER_HTTP_ALLOW_HOSTS names the host.
    let defer_client = defer_http::client()?;
    let wait_http = wait_http_builder
        .build()
        // A failed build would silently hand back a *default* client — redirects
        // enabled, no timeout — discarding the very policy above. Fail startup.
        .map_err(|e| anyhow::anyhow!("could not build the wait-sensor HTTP client: {e}"))?;

    // Dataset-triggered scheduling (Airflow Datasets / Dagster asset sensors):
    // registered workflows with `on_datasets:` fire when their subscribed
    // datasets record new updates. On by default; DATASET_TRIGGERS=0 opts a
    // scheduler out of sweeping (sensors and `produces:` recording stay on —
    // they are run-local, not trigger machinery). The sweep itself is throttled
    // below; `dataset_trigger_sweep` is its last-run instant.
    let dataset_triggers_on = std::env::var("DATASET_TRIGGERS")
        .map(|v| !matches!(v.trim().to_ascii_lowercase().as_str(), "0" | "false" | "off" | "no"))
        .unwrap_or(true);
    let mut dataset_trigger_sweep: Option<std::time::Instant> = None;
    // Workflows this scheduler has synced subscription rows for, so a spec that
    // later drops `on_datasets:` is cleared once instead of every sweep.
    let mut subscribed_workflows: std::collections::HashSet<String> =
        std::collections::HashSet::new();
    // One warning per unowned `defer.kind`, not one per tick: a build holding a
    // job it cannot resolve is worth saying, and saying it twice a second is how
    // an operator learns to filter it out.
    let mut warned_defer_kinds: std::collections::HashSet<String> =
        std::collections::HashSet::new();

    // Cloud artifact location (DAGRON_ARTIFACT_URL — s3://, gs://, az://): the
    // engine does no cloud I/O at dispatch; it injects per-run/per-task *URLs*
    // (`DAGRON_ARTIFACTS_URL`, `DAGRON_CHECKPOINT_URL`) and tasks reach the
    // bucket with their own tooling. Resume pointers for cloud checkpoints flow
    // through the checkpoint report route (any URI), so this composes with
    // checkpoint-aware resume across machines and clouds — the substrate the
    // artifact API also serves via `dagron_artifact::store_from_env`.
    let artifact_url = std::env::var("DAGRON_ARTIFACT_URL")
        .ok()
        .map(|u| u.trim().trim_end_matches('/').to_string())
        .filter(|u| !u.is_empty());
    if let Some(url) = &artifact_url {
        info!(%url, "cloud artifact location enabled (DAGRON_ARTIFACTS_URL / DAGRON_CHECKPOINT_URL injected per task)");
    }

    info!("reconcile loop running (multi-run, queue-driven daemon)");

    // Oversized-gang alarm state (gangs_on only): slow-cadence check + a
    // warned set so each impossible gang logs exactly once per process.
    let mut oversized_gang_check: Option<std::time::Instant> = None;
    let mut warned_oversized_gangs: std::collections::HashSet<String> =
        std::collections::HashSet::new();

    // Constrained-host claim gate (`DAGRON_PRESSURE_FILE`): polled once per
    // tick, transitions logged once. See `pressure` for the contract.
    let mut pressure_gate = pressure::PressureGate::from_env();
    if let Some(p) = pressure_gate.path() {
        info!(path = %p.display(), "pressure gate armed — task claims pause while this file exists");
    }

    loop {
        // Tick timer — the recover→advance→dispatch→collect→reap span. A tick
        // pegging the CPU (a load-test finding) shows up as this histogram's
        // upper buckets filling. Excludes the wait below (idle, not work).
        let tick_start = std::time::Instant::now();

        // Set when this tick lands a fenced terminal mark (a drained result's
        // success/terminal failure, or a memoization hit) — not for a failure
        // that was merely rescheduled for a future retry, which unblocks
        // nothing now. A terminal mark decremented dependents, so
        // re-enter the tick immediately instead of sleeping — on SQLite there
        // is no NOTIFY to wake us, and on Postgres this skips a listener round
        // trip for work we already know exists.
        let mut newly_terminal = false;

        // ── Maintenance sweeps (docs/LOW_LATENCY.md A-4) ────────────────────
        // Crash recovery, run deadlines, SLA alerts, approval expiry, and the
        // sub-workflow / wait-sensor / dataset reconciliations are time-based,
        // idempotent sweeps — none of them needed to run at the moment a
        // dependency finished. They run at `SWEEP_INTERVAL_MS` cadence so a
        // completion- or NOTIFY-woken tick goes straight to advance → claim.
        let run_sweeps =
            last_sweep.is_none_or(|t: std::time::Instant| t.elapsed() >= sweep_interval);
        if run_sweeps {
            last_sweep = Some(std::time::Instant::now());
            // ── Step 0: leftover workloads from schedulers that died ────────────
            //
            // The per-dispatch reap cannot see these. It runs when a task is
            // dispatched and looks only at THAT task's predecessors, so a
            // workload whose task never runs again — the run was cancelled, the
            // task failed terminally, retention collected the row — is
            // invisible to it forever. Those are the ones that keep costing.
            //
            // Ordered list-then-query on purpose. A workload observed in the
            // listing already existed when the liveness question was asked, and
            // a task row is always written before its workload is created, so
            // "listed but not live" cannot mean "its row had not been written
            // yet". `min_age` covers the clock skew and the replication lag
            // that ordering alone does not.
            if let Some(inst) = installation.as_deref() {
                if last_orphan_sweep
                    .is_none_or(|t: std::time::Instant| t.elapsed() >= orphan_sweep_interval)
                {
                    last_orphan_sweep = Some(std::time::Instant::now());
                    let scope = dagron_executor::executor::OrphanScope {
                        installation: inst,
                        min_age: orphan_min_age,
                    };
                    match sweeper.list_orphan_candidates(&scope).await {
                        Ok(found) => {
                            // Every candidate is asked about, and the DELETES are
                            // what is capped. Capping the candidate list instead
                            // starves: an apiserver lists in a stable order, so a
                            // namespace whose first N workloads are long-running
                            // and live would be re-examined identically every
                            // sweep and never reach the leftovers behind them.
                            //
                            // The liveness question is chunked rather than
                            // truncated, so the `IN` stays bounded while the
                            // coverage does not.
                            //
                            // A datastore failure here skips the pass, it does
                            // not fail the tick. This sweep is best-effort
                            // cleanup — its listing and its deletes already say
                            // so — and `?` here would have made the opt-in
                            // cleanup feature a NEW way for a transient
                            // database blip to terminate the scheduler daemon:
                            // `main` returns `run()`'s error straight out.
                            //
                            // The break is not tidiness. A PARTIAL live set is
                            // worse than none: a task whose chunk never ran
                            // looks dead, and the whole sweep acts on absence.
                            // So one failed chunk abandons the deletes
                            // entirely, and `last_orphan_sweep` is already
                            // stamped, so the next attempt waits the normal
                            // cadence rather than hot-looping.
                            let mut live = std::collections::HashSet::new();
                            let mut liveness_complete = true;
                            for chunk in found.chunks(ORPHAN_QUERY_CHUNK) {
                                let ids: Vec<String> =
                                    chunk.iter().map(|w| w.task_id.clone()).collect();
                                match db::live_task_ids(&pool, &ids).await {
                                    Ok(ids) => live.extend(ids),
                                    Err(e) => {
                                        warn!(
                                            error = %e,
                                            "fleet sweep could not ask which tasks are live — \
                                             skipping this pass's deletes rather than acting on \
                                             a partial answer"
                                        );
                                        liveness_complete = false;
                                        break;
                                    }
                                }
                            }
                            for w in found
                                .iter()
                                .filter(|_| liveness_complete)
                                .filter(|w| !live.contains(&w.task_id))
                                .take(ORPHAN_DELETE_BATCH)
                            {
                                match sweeper.delete_workload(w).await {
                                    Ok(()) => {
                                        metrics.inc_orphan_workloads_reaped();
                                        warn!(
                                            handle = %w.handle,
                                            task_id = %w.task_id,
                                            run_id = w.run_id.as_deref().unwrap_or("<none>"),
                                            attempt = w.attempt.as_deref().unwrap_or("<none>"),
                                            "reaped a leftover workload — its task is no \
                                             longer live, so whatever created this never \
                                             finished cleaning up"
                                        );
                                    }
                                    // One undeletable leftover must not stop
                                    // the rest: the next sweep sees it again.
                                    Err(e) => warn!(
                                        handle = %w.handle, task_id = %w.task_id, error = %e,
                                        "could not delete a leftover workload — will retry \
                                         on the next fleet sweep"
                                    ),
                                }
                            }
                        }
                        // Listing is the whole sweep's input, so a failure here
                        // skips this pass rather than failing the tick. The
                        // apiserver being briefly unreachable is not a reason to
                        // stop scheduling.
                        Err(e) => warn!(error = %e, "fleet sweep could not list workloads — skipping this pass"),
                    }
                }
            }

            // ── Step 1: crash recovery ──────────────────────────────────────────
            let recovered = db::recover_expired_leases(&pool).await?;
            if recovered > 0 {
                info!(recovered, "reclaimed expired leases");
            }

            // ── Step 1b: run-level deadlines ────────────────────────────────────
            // Fail any run past its `run_timeout_secs` budget and cancel its
            // remaining tasks. Idempotent, so every scheduler may sweep; an executor
            // finishing after the sweep is rejected by the fence guard.
            for run_id in db::cancel_overdue_runs(&pool).await? {
                tracing::warn!(%run_id, "run deadline exceeded (run_timeout_secs) — run failed, tasks cancelled");
                metrics.inc_runs_deadline_exceeded();
            }

            // ── Step 1c: soft SLA deadline alerts ───────────────────────────────
            // Emit a `run.deadline_exceeded` outbox event (once) for a run past its
            // `deadline` — the run keeps running. Fire-once + winner-take-all in SQL.
            for run_id in db::fire_deadline_alerts(&pool).await? {
                tracing::warn!(%run_id, "run exceeded its soft deadline — SLA alert emitted");
                metrics.inc_deadline_alerts();
                // Push the SLA breach to any notify.webhook / notify.slack targets
                // (fire-once is guaranteed by fire_deadline_alerts above). Spawned
                // so a slow target can't stall the reconcile tick.
                {
                    let pool = pool.clone();
                    tokio::spawn(async move {
                        notify::notify_run_event(&pool, &run_id, "deadline_exceeded").await;
                    });
                }
            }

            // ── Step 1d: expire human approval gates (#19) ──────────────────────
            // Auto-resolve any `awaiting_approval` task past its `approval_timeout_secs`
            // per its `approval_on_timeout` default. Idempotent (guarded resolve), so
            // every scheduler may sweep.
            for (task_id, approved) in db::resolve_expired_approvals(&pool).await? {
                tracing::info!(%task_id, approved, "approval gate timed out — auto-resolved");
                // Both arms are terminal: `resolve_approval` writes `succeeded`
                // on approve and `failed` on reject. An approved gate is not
                // handed on to a worker, so this is its only completion.
                task_finished(&metrics, &seams, approved).await;
            }

            // Resolve any `type: workflow` trigger whose child run has finished (#23):
            // the parent succeeds/fails with the child and its dependents advance.
            // Idempotent (guarded resolve), so every scheduler may sweep.
            // Also where a `repeat:` on a trigger is evaluated: an iteration that
            // continues re-arms the task and is *not* reported here, so a quiet tick
            // during a long conversation is the loop working, not the loop stalled.
            for (task_id, succeeded) in db::reconcile_subworkflows(&pool).await? {
                tracing::info!(%task_id, succeeded, "sub-workflow finished — resolved trigger task");
                // A `repeat:` iteration that re-arms is not in this vec (the
                // sweep `continue`s past it), so a conversation is metered once
                // when it ends, not once per turn.
                task_finished(&metrics, &seams, succeeded).await;
            }

            // Resolve any parked runtime fan-out (`with_output_of:`): read the
            // producer's output, insert one task row per element, and — once
            // those are all terminal — resolve the barrier they hang from.
            // Idempotent (CAS on expand, parked-shape guard on resolve), so
            // every scheduler may sweep.
            //
            // This is the one sweep that makes a run *bigger*. Everything else
            // here resolves rows that already exist; an expansion is N tasks
            // appearing after admission decided how many there would be, which
            // is why it is logged at info with its count and why the sweep
            // re-checks the run's task ceiling before inserting.
            for (task_id, outcome) in db::reconcile_fanouts(&pool).await? {
                match outcome {
                    dagron_core::models::FanoutOutcome::Expanded { instances } => {
                        tracing::info!(%task_id, instances, "runtime fan-out expanded");
                    }
                    dagron_core::models::FanoutOutcome::Joined { succeeded, instances } => {
                        tracing::info!(%task_id, succeeded, instances, "runtime fan-out joined");
                        task_finished(&metrics, &seams, succeeded).await;
                    }
                    dagron_core::models::FanoutOutcome::Failed { reason } => {
                        tracing::warn!(%task_id, %reason, "runtime fan-out could not be resolved");
                        task_finished(&metrics, &seams, false).await;
                    }
                }
            }

            // Resolve any deferred `type: wait` sensor whose deadline has passed (#27):
            // the task succeeds and its dependents advance. Idempotent, HA-safe.
            for task_id in db::reconcile_waits(&pool).await? {
                tracing::info!(%task_id, "wait sensor elapsed — resolved");
                task_finished(&metrics, &seams, true).await;
            }

            // Poll any parked `wait.url` HTTP sensor (#27 follow-on) that is due: a 2xx
            // resolves the task (dependents advance); anything else re-parks it for the
            // next WAIT_POLL_SECS window. The parked-shape guards keep this idempotent
            // and HA-safe. A hung endpoint is bounded by the client's request timeout.
            // Probe the due batch CONCURRENTLY. Polled serially, N parked sensors
            // pointed at a black-holed endpoint would hold the tick for N × the
            // request timeout — the per-request timeout bounds one poll, not the
            // tick — stalling claim, dispatch, log drain, and run reaping behind
            // them. `WAIT_URL_BATCH` additionally caps the batch in SQL, so the work
            // one tick can take on is bounded from both ends; anything not polled
            // this tick is simply picked up by the next.
            let due = db::due_url_waits(&pool, WAIT_URL_BATCH).await?;
            if !due.is_empty() {
                let mut probes = tokio::task::JoinSet::new();
                for (task_id, url) in due {
                    let client = wait_http.clone(); // Client is an Arc handle — cheap
                    let deny_private = wait_url_deny_private;
                    probes.spawn(async move {
                        // An IP-literal host never reaches the resolver, so the
                        // deny-private policy has to be applied here as well. This
                        // reads as "not ready" and re-parks rather than failing the
                        // task: the warning is the signal, and relaxing the policy
                        // resolves the parked sensor in place. (Editing the spec
                        // would not — `wait_url` was materialized onto this row when
                        // the task was created, so a spec fix lands on the next run.)
                        if deny_private && wait_url::literal_host_blocked(&url) {
                            tracing::warn!(
                                %task_id, %url,
                                "wait.url names a non-global address — refusing to poll (WAIT_URL_DENY_PRIVATE)"
                            );
                            return (task_id, url, false);
                        }
                        let ready = match client.get(&url).send().await {
                            Ok(resp) => resp.status().is_success(),
                            Err(e) => {
                                tracing::debug!(%task_id, %url, error = %e, "http wait sensor poll errored");
                                false
                            }
                        };
                        (task_id, url, ready)
                    });
                }
                while let Some(joined) = probes.join_next().await {
                    let Ok((task_id, url, ready)) = joined else { continue }; // task panicked
                    if ready {
                        if db::resolve_url_wait(&pool, &task_id).await? {
                            tracing::info!(%task_id, %url, "http wait sensor endpoint ready (2xx) — resolved");
                            task_finished(&metrics, &seams, true).await;
                        }
                    } else {
                        let next_poll = (chrono::Utc::now()
                            + chrono::TimeDelta::seconds(wait_poll_secs as i64))
                        .to_rfc3339();
                        db::repark_url_wait(&pool, &task_id, &next_poll).await?;
                    }
                }
            }

            // Poll any parked `defer:` external job that is due. This is the
            // sweep that makes the park worth having: the row is the whole
            // contract, so whichever scheduler survives resolves a job that any
            // other scheduler submitted.
            //
            // Concurrent, for the reason the url-sensor batch above is: polled
            // serially, N parked jobs against a black-holed vendor would hold
            // the tick for N × the request timeout, stalling claim, dispatch and
            // run reaping behind them. `EXTERNAL_POLL_BATCH` caps it in SQL too,
            // so the work one tick takes on is bounded from both ends; anything
            // not polled this tick is picked up by the next.
            //
            // A poller that is absent leaves its rows parked rather than failing
            // them — an engine rebuilt without the backend that owns a running
            // job should not tear that job's task down on the next tick.
            //
            // Claimed, not listed: every scheduler sweeps, so an unclaimed read
            // would have all of them call the vendor for the same job. The claim
            // advances next_poll_at, which doubles as the crash bound — a
            // scheduler that dies mid-request leaves the row due again then.
            let due_external =
                db::claim_due_external_polls(&pool, EXTERNAL_POLL_BATCH, external_poll_secs)
                    .await?;
            if !due_external.is_empty() {
                // Secrets resolved for defer.http headers, cached for THIS sweep
                // only. Resolution is two DB queries plus an AES-GCM decrypt, and
                // a batch of parked jobs against one workspace shares one
                // credential; per-sweep rather than process-wide so a rotated
                // secret is picked up on the next tick rather than at the next
                // restart.
                let mut secrets: std::collections::HashMap<HeaderCacheKey, Vec<dag::EnvVar>> =
                    std::collections::HashMap::new();
                let now_rfc = chrono::Utc::now().to_rfc3339();

                // ── Pass 1: the ceiling, and the datastore work ─────────────
                //
                // Serial on purpose, and both halves have to be. An elapsed
                // `defer.max_wait_secs` is a failure regardless of what the
                // remote system would say, so it is decided before anything is
                // polled — a vendor we cannot reach must not keep a task parked
                // past its budget. And header resolution is the one step that
                // touches the pool and the shared cache, so doing it here keeps
                // that cache a plain `&mut HashMap` rather than a mutex every
                // in-flight poll contends on.
                let mut plans: Vec<(dagron_core::models::ExternalPark, PollPlan)> =
                    Vec::with_capacity(due_external.len());
                for park in due_external {
                    if park.external_deadline_at.as_deref().is_some_and(|d| d <= now_rfc.as_str()) {
                        // With either cancel transport the row keeps its handle, so
                        // the teardown sweep below stops the job; with neither nothing
                        // here can, and the message says so.
                        let via =
                            cancel_transport(park.input.as_deref(), seams.external_poller.is_some());
                        let outcome = match via {
                            Some(t) => format!("cancelling it ({t})"),
                            None => "the job was NOT cancelled by this engine".to_string(),
                        };
                        let reason = format!(
                            "defer.max_wait_secs elapsed while remote job '{}' ({}) was still \
                             running; {outcome}",
                            park.external_handle, park.external_kind,
                        );
                        let failed = if via.is_some() {
                            db::fail_external_keep_handle(&pool, &park.id, &park.external_handle, &reason)
                                .await?
                        } else {
                            db::fail_external(&pool, &park.id, &park.external_handle, &reason).await?
                        };
                        if failed {
                            warn!(
                                task_id = %park.id, handle = %park.external_handle,
                                kind = %park.external_kind, cancellable = via.is_some(),
                                "deferred job exceeded defer.max_wait_secs — failed; the remote \
                                 job is torn down only if a cancel transport is configured (a \
                                 registered poller, or defer.http.cancel), else it may still be \
                                 running"
                            );
                            task_finished(&metrics, &seams, false).await;
                            newly_terminal = true;
                        }
                        continue;
                    }

                    // What the concurrent half will need, resolved now. The
                    // registered poller is tried first at poll time (it is the
                    // more specific thing), so a row carrying a `defer.http:`
                    // block has its headers resolved even where a poller may
                    // claim it — bounded by the cache, which performs exactly
                    // one resolution per (run, header block) per sweep.
                    let http = park
                        .input
                        .as_deref()
                        .and_then(|j| serde_json::from_str::<dag::TaskSpec>(j).ok())
                        .and_then(|t| t.defer)
                        .and_then(|d| d.http);
                    let plan = match http {
                        Some(spec) => {
                            match resolve_defer_headers(&pool, &park.run_id, &spec, &mut secrets).await {
                                Ok(headers) => {
                                    PollPlan { spec: Some((spec, headers)), http_unresolved: false }
                                }
                                // A credential we cannot resolve is not a verdict
                                // either: the row is still POLLED, because a
                                // registered poller may own this kind and needs
                                // no header of ours, and only the built-in
                                // fallback is unavailable. Dropping the row here
                                // would silently stop resolving jobs a poller was
                                // handling perfectly well.
                                Err(e) => {
                                    warn!(
                                        task_id = %park.id, handle = %park.external_handle,
                                        error = %e,
                                        "could not resolve defer.http headers — re-parking \
                                         (not a verdict about the job)"
                                    );
                                    PollPlan { spec: None, http_unresolved: true }
                                }
                            }
                        }
                        // No http block. Still a plan: a registered poller may
                        // own this kind, and only calling it can tell us.
                        None => PollPlan { spec: None, http_unresolved: false },
                    };
                    plans.push((park, plan));
                }

                // ── Pass 2: the network, concurrently ───────────────────────
                //
                // The reason this is not a `for` loop with an `.await` in it:
                // polled serially, a batch against a black-holed vendor holds
                // the tick for `EXTERNAL_POLL_BATCH` × the request timeout —
                // 32 × 15 s of nothing happening — stalling claim, dispatch,
                // log drain and run reaping behind it. The per-request timeout
                // bounds one poll, never the tick. `EXTERNAL_POLL_BATCH` caps
                // the batch in SQL, so the fan-out is bounded by the same
                // constant that bounds the work, exactly as the `wait.url`
                // probes above are.
                let mut polls = tokio::task::JoinSet::new();
                for (park, plan) in plans {
                    let poller = seams.external_poller.clone();
                    let client = defer_client.clone(); // Client is an Arc handle — cheap
                    polls.spawn(async move {
                        let (verdict, unresolvable) =
                            poll_one(&poller, &client, &park, &plan).await;
                        (park, verdict, unresolvable)
                    });
                }

                // ── Pass 3: apply, serially ─────────────────────────────────
                //
                // Every datastore transition and every meter bump happens back
                // here, one at a time, so the accounting is single-threaded and
                // `newly_terminal` means what it says. The concurrency bought
                // wall-clock on the network, and paid for none of it in the
                // state machine.
                while let Some(joined) = polls.join_next().await {
                    let Ok((park, verdict, unresolvable)) = joined else { continue }; // task panicked
                    // Nothing in this build can resolve this row. Leave it
                    // PARKED rather than failing it — an engine rebuilt without
                    // the backend that owns a running job should not tear that
                    // job's task down on the next tick — and say so once per
                    // kind, because repeating it twice a second is how a warning
                    // stops being read.
                    if unresolvable && warned_defer_kinds.insert(park.external_kind.clone()) {
                        warn!(
                            kind = %park.external_kind, task_id = %park.id,
                            handle = %park.external_handle,
                            "nothing in this build resolves this defer.kind: no registered \
                             ExternalPoller claims it and the task declares no `defer.http:` \
                             block, so the row stays parked. Any status endpoint that answers \
                             with JSON runs on the built-in `defer.http` path today — a \
                             Databricks, EMR Serverless, Dataproc, Livy, Kyuubi or YARN job \
                             included, with the token in `value_from: {{ secret: NAME }}` \
                             (docs/EXTERNAL_JOBS.md). Additional backends register through the \
                             ExternalPoller seam (dagron_engine::Seams). Cancel the run to \
                             release this task."
                        );
                    }

                    // Apply it. One place, so every resolver produces the same
                    // accounting — and `Running` / `None` both simply leave the
                    // claim's next_poll_at standing.
                    match verdict {
                        Some(hooks::Verdict::Succeeded { output }) => {
                            if db::resolve_external(
                                &pool, &park.id, &park.external_handle, Some(&output),
                            )
                            .await?
                            {
                                info!(
                                    task_id = %park.id, handle = %park.external_handle,
                                    "deferred job succeeded — resolved"
                                );
                                task_finished(&metrics, &seams, true).await;
                                newly_terminal = true;

                                // The third path that can succeed a producer
                                // task, after the worker result and the cache
                                // hit. `produces:` is a postcondition — "after
                                // this task succeeds, the dataset is current" —
                                // and a deferred task succeeds HERE, in the
                                // sweep, not on the worker result path. Without
                                // this, `defer:` + `produces:` would validate,
                                // run, and record nothing, leaving every
                                // downstream sensor and `on_datasets:` consumer
                                // parked forever.
                                //
                                // Guarded on the resolve having actually landed,
                                // matching the `if marked` discipline on the
                                // worker path: a re-sweep that lost the race
                                // must not fabricate a second lineage row.
                                let produces = park
                                    .input
                                    .as_deref()
                                    .and_then(|j| serde_json::from_str::<dag::TaskSpec>(j).ok())
                                    .map(|t| t.produces)
                                    .unwrap_or_default();
                                if !produces.is_empty() {
                                    let wf = db::workflow_name_for_task(&pool, &park.id)
                                        .await
                                        .ok()
                                        .flatten()
                                        .unwrap_or_default();
                                    record_produces(
                                        &pool, &metrics, &wf, &park.id, &park.name, &produces,
                                    )
                                    .await;
                                }
                            }
                        }
                        Some(hooks::Verdict::Failed { reason }) => {
                            if db::fail_external(&pool, &park.id, &park.external_handle, &reason)
                                .await?
                            {
                                info!(
                                    task_id = %park.id, handle = %park.external_handle,
                                    "deferred job failed — resolved"
                                );
                                task_finished(&metrics, &seams, false).await;
                                newly_terminal = true;
                            }
                        }
                        Some(hooks::Verdict::Running) | None => {}
                    }
                }
            }

            // Tear down the remote jobs that terminated tasks left running.
            //
            // Cancelling a run is pure SQL — it flips rows terminal and clears
            // leases — so before this sweep "we cancelled your run" meant "we
            // stopped watching your cluster bill". The debt is row state rather
            // than something the cancel stamps, and deliberately: the product's
            // primary cancel path is inlined SQL in dagron-api, a binary that by
            // design holds no Seams, so a teardown the cancel *performed* would
            // be one the SDK and MCP server never triggered. A cancel that
            // merely leaves evidence is one every caller performs for free.
            let owing = db::claim_due_external_cancels(
                &pool,
                EXTERNAL_CANCEL_BATCH,
                external_poll_secs.max(30),
            )
            .await?;
            if !owing.is_empty() {
                let now = chrono::Utc::now();
                // Header secrets resolved here, serially, for the same reason the
                // poll's are: the cache is a plain `&mut HashMap`, not a mutex.
                let mut cancel_secrets: std::collections::HashMap<HeaderCacheKey, Vec<dag::EnvVar>> =
                    std::collections::HashMap::new();
                // Concurrently, bounded by the batch: a vendor that is timing
                // out must not hold the reconcile tick, the same reason the
                // wait.url probes are spawned.
                let mut jobs = tokio::task::JoinSet::new();
                for row in owing {
                    // Give up on wall clock as well as attempts. Without this a
                    // fleet where nothing owns the kind sweeps the row forever,
                    // because the no-poller path deliberately does not consume
                    // an attempt.
                    let stale = row
                        .finished_at
                        .as_deref()
                        .and_then(|f| chrono::DateTime::parse_from_rfc3339(f).ok())
                        .is_some_and(|f| {
                            (now - f.with_timezone(&chrono::Utc)).num_seconds()
                                > EXTERNAL_CANCEL_GIVEUP_SECS
                        });
                    if stale {
                        orphan(&pool, &metrics, &row, "teardown was owed for too long").await?;
                        continue;
                    }
                    // The generic transport: a `defer.http.cancel` on the spec, tried
                    // only when no registered poller claims the kind. Unresolvable
                    // headers are a failed attempt, not a skipped one — the row is
                    // given up on and logged by handle after three, rather than
                    // sweeping forever on a credential that will not resolve.
                    let http_cancel = match http_with_cancel(row.input.as_deref()) {
                        Some(spec) => Some(
                            resolve_defer_headers(&pool, &row.run_id, &spec, &mut cancel_secrets)
                                .await
                                .map(|headers| (spec, headers)),
                        ),
                        None => None,
                    };
                    let poller = seams.external_poller.clone();
                    let client = defer_client.clone();
                    let pool = pool.clone();
                    let metrics = Arc::clone(&metrics);
                    jobs.spawn(async move {
                        let ctx = hooks::PollCtx {
                            kind: &row.external_kind,
                            handle: &row.external_handle,
                            endpoint: row.external_endpoint.as_deref(),
                            run_id: &row.run_id,
                            task_id: &row.id,
                            epoch: row.external_epoch,
                        };
                        let mut outcome = match &poller {
                            Some(p) => p.cancel(&ctx).await,
                            None => Ok(None),
                        };
                        if matches!(outcome, Ok(None)) {
                            if let Some(plan) = http_cancel {
                                outcome = match plan {
                                    Ok((spec, headers)) => {
                                        let cancel = spec.cancel.as_ref().expect("filtered on it");
                                        defer_http::send_cancel(
                                            &client, cancel, &row.external_handle, &headers,
                                        )
                                        .await
                                        .map(Some)
                                    }
                                    Err(e) => Err(e),
                                };
                            }
                        }
                        let next = dag::delayed_retry_at(60);
                        match outcome {
                            // Torn down. The debt is settled.
                            Ok(Some(())) => {
                                if db::clear_external_handle(&pool, &row.id, &row.external_handle)
                                    .await
                                    .unwrap_or(false)
                                {
                                    info!(
                                        task_id = %row.id, handle = %row.external_handle,
                                        kind = %row.external_kind,
                                        "remote job torn down after cancellation"
                                    );
                                }
                            }
                            // Nothing here owns this kind. Hand the row back
                            // WITHOUT consuming an attempt: a RUNNER_CLASSES
                            // pool runs the same binary with different seams, so
                            // the replica that can do this work must not have
                            // its budget spent by the ones that cannot.
                            Ok(None) => {
                                let _ = db::release_external_cancel_claim(
                                    &pool, &row.id, &row.external_handle, &next,
                                )
                                .await;
                            }
                            // Tried and failed. This one is on the budget.
                            Err(e) => {
                                let attempts = db::record_external_cancel_failure(
                                    &pool, &row.id, &row.external_handle, &next,
                                )
                                .await
                                .unwrap_or(EXTERNAL_CANCEL_ATTEMPTS);
                                warn!(
                                    task_id = %row.id, handle = %row.external_handle,
                                    kind = %row.external_kind, attempts, error = %e,
                                    "could not tear down the remote job — will retry"
                                );
                                if attempts >= EXTERNAL_CANCEL_ATTEMPTS {
                                    let _ = orphan(
                                        &pool, &metrics, &row,
                                        "the remote system could not be reached",
                                    )
                                    .await;
                                }
                            }
                        }
                    });
                }
                while jobs.join_next().await.is_some() {}
            }


            // Resolve any parked `wait.dataset` sensor whose dataset recorded an
            // update after the park: the task succeeds and its dependents advance.
            // Idempotent, HA-safe (guarded UPDATE).
            for (task_id, uri) in db::reconcile_dataset_waits(&pool).await? {
                tracing::info!(%task_id, dataset = %uri, "dataset sensor saw a new update — resolved");
                task_finished(&metrics, &seams, true).await;
            }

            // Dataset-triggered scheduling: sync `on_datasets:` subscriptions from
            // the workflow registry, then claim-and-fire triggers whose datasets
            // recorded new updates. HA-safe with no leadership — claiming is a CAS
            // cursor advance, so exactly one scheduler creates each run; every
            // scheduler may sweep. Throttled: registry parsing every tick would be
            // wasted work at a 500 ms cadence.
            if dataset_triggers_on
                && dataset_trigger_sweep
                    .is_none_or(|t: std::time::Instant| t.elapsed().as_secs() >= 5)
            {
                dataset_trigger_sweep = Some(std::time::Instant::now());

                // 1. Sync subscriptions for workflows that declare any. A spec with
                //    no `on_datasets:` is skipped entirely rather than issuing a
                //    per-workflow DELETE every sweep — with a few hundred registered
                //    workflows that would be a few hundred pointless writes a minute,
                //    forever. `subscribed` remembers who we synced, so a workflow that
                //    *drops* its `on_datasets:` still gets its rows cleared exactly
                //    once (and the orphan prune below is the backstop for the rest).
                //    Every subscription syncs, whatever its arity or mode:
                //    multi-dataset and all-of composition are open, and
                //    `claim_due_dataset_triggers` has handled `mode="all"` on
                //    both backends all along.
                match db::list_registered_workflows(&pool).await {
                    Ok(wfs) => {
                        let mut still_subscribed: std::collections::HashSet<String> =
                            std::collections::HashSet::new();
                        for (name, spec) in wfs {
                            match dag::dataset_subscriptions(&spec) {
                                Some((uris, mode)) => {
                                    still_subscribed.insert(name.clone());
                                    if let Err(e) =
                                        db::sync_dataset_triggers(&pool, &name, &uris, &mode).await
                                    {
                                        tracing::warn!(workflow = %name, error = %e, "dataset trigger sync failed");
                                        still_subscribed.remove(&name);
                                    }
                                }
                                None => {
                                    // Only clear when this scheduler previously synced
                                    // rows for it — otherwise this is a no-op workflow.
                                    if subscribed_workflows.contains(&name) {
                                        let _ = db::sync_dataset_triggers(&pool, &name, &[], "any").await;
                                    }
                                }
                            }
                        }
                        subscribed_workflows = still_subscribed;
                    }
                    Err(e) => tracing::warn!(error = %e, "dataset trigger sweep: workflow listing failed"),
                }
                if let Err(e) = db::prune_dataset_triggers(&pool).await {
                    tracing::warn!(error = %e, "dataset trigger prune failed");
                }

                // 2. Claim + fire. The triggering dataset is injected as the
                //    `{{ trigger_dataset }}` parameter so the fired run can reference
                //    what woke it. A fire refused at the workflow's max_active_runs
                //    cap rolls its cursors back and retries on a later sweep; a spec
                //    that no longer parses keeps its advanced cursor (skipping the
                //    fire) so a broken spec can't warn-loop forever.
                for fire in db::claim_due_dataset_triggers(&pool).await? {
                    let Some(spec_yaml) =
                        db::workflow_spec_by_name(&pool, &fire.workflow_name).await?
                    else {
                        continue; // deregistered between sync and claim — prune gets it
                    };
                    let mut params = std::collections::BTreeMap::new();
                    params.insert("trigger_dataset".to_string(), fire.trigger_uri.clone());
                    let parsed = match environments::template_params(&pool, &spec_yaml).await {
                        Ok(extra) => {
                            params.extend(extra);
                            dag::DagGraph::from_yaml_with_params(&spec_yaml, &params)
                        }
                        Err(e) => Err(e),
                    };
                    match parsed {
                        Ok(child_dag) => match db::create_run(&pool, &child_dag, &spec_yaml).await {
                            Ok(run_id) => {
                                metrics.inc_runs_created();
                                metrics.inc_dataset_fires();
                                if let Err(e) =
                                    db::stamp_dataset_trigger_fired(&pool, &fire.workflow_name, &run_id)
                                        .await
                                {
                                    tracing::warn!(workflow = %fire.workflow_name, error = %e, "dataset trigger stamp failed");
                                }
                                info!(
                                    workflow = %fire.workflow_name, %run_id, dataset = %fire.trigger_uri,
                                    "dataset trigger fired run"
                                );
                            }
                            Err(e)
                                if dagron_core::models::is_capacity_refusal(&e) =>
                            {
                                // Capacity, not fault (the workflow's cap or the
                                // datastore's free-disk floor): roll the cursors
                                // back quietly and let a later sweep retry.
                                db::unclaim_dataset_trigger(&pool, &fire.workflow_name, &fire.advanced)
                                    .await?;
                                info!(workflow = %fire.workflow_name, reason = %e, "dataset fire refused admission (capacity) — will retry");
                            }
                            Err(e) => {
                                db::unclaim_dataset_trigger(&pool, &fire.workflow_name, &fire.advanced)
                                    .await?;
                                tracing::warn!(workflow = %fire.workflow_name, error = %e, "dataset fire create_run failed — will retry");
                            }
                        },
                        Err(e) => {
                            tracing::warn!(workflow = %fire.workflow_name, error = %e, "dataset-triggered workflow no longer parses — fire skipped");
                        }
                    }
                }
            }
        }

        // ── Step 2: unblock tasks whose deps just completed ─────────────────
        db::advance_ready_tasks(&pool).await?;

        // ── Step 3: claim and dispatch ──────────────────────────────────────
        // Oversized-gang alarm: a gang bigger than this scheduler's whole pool
        // can never be claimed HERE and would sit ready silently if no larger
        // peer exists. Warn loudly, once per gang, on a slow cadence (a peer
        // with a bigger pool may still claim it, so this warns rather than
        // fails — but it must never be silent).
        if gangs_on
            && oversized_gang_check
                .map_or(true, |t: std::time::Instant| t.elapsed().as_secs() >= 60)
        {
            oversized_gang_check = Some(std::time::Instant::now());
            match db::oversized_ready_gangs(&pool, workers.size() as i64).await {
                Ok(gangs) => {
                    for (gang_id, size) in gangs {
                        if warned_oversized_gangs.insert(gang_id.clone()) {
                            tracing::warn!(
                                %gang_id,
                                gang_size = size,
                                pool_size = workers.size(),
                                "gang needs more slots than this scheduler's whole worker pool — it cannot be claimed here; raise WORKER_COUNT, lower gang.size, or run a larger scheduler"
                            );
                        }
                    }
                }
                Err(e) => tracing::warn!(error = %e, "oversized-gang check failed"),
            }
        }

        // Pressure file present ⇒ capacity 0: nothing new is claimed, runs
        // stay queued in the datastore, in-flight tasks finish, and the
        // sweeps above keep running (recovery is never gated). A one-shot run
        // cannot drain while paused, so the process stays resident until the
        // file is removed — which is the point of a maintenance hold.
        let claims_paused = pressure_gate.poll();
        metrics.set_claims_paused(claims_paused);
        let capacity = if claims_paused { 0 } else { workers.size().saturating_sub(in_flight) };
        if capacity > 0 {
            // Both claim paths take the same `pool_caps` (#21) and order by
            // priority (#25), so gang co-scheduling never bypasses either.
            let claimed = if gangs_on {
                // Gangs first (all-or-nothing, and only into a pool that can seat
                // the whole gang), then fill leftover capacity with ordinary
                // tasks — never claiming a gang member solo.
                let mut claimed = db::claim_ready_gang(
                    &pool,
                    &worker_id,
                    capacity as i64,
                    &runner_classes,
                    &pool_caps,
                )
                .await?;
                let remaining = capacity as i64 - claimed.len() as i64;
                if remaining > 0 {
                    claimed.extend(
                        db::claim_ready_classes_nongang(
                            &pool,
                            &worker_id,
                            remaining,
                            &runner_classes,
                            &pool_caps,
                        )
                        .await?,
                    );
                }
                claimed
            } else {
                db::claim_ready_classes(
                    &pool,
                    &worker_id,
                    capacity as i64,
                    &runner_classes,
                    &pool_caps,
                )
                .await?
            };
            if !claimed.is_empty() {
                metrics.observe_claim_batch(claimed.len());
            }
            for task in claimed {
                let (mut ctx, max_attempts, retry_delay_secs, retry_max_delay_secs, retry_on_timeout, cache, sub_workflow, wait, gang_member, produces, carried_spec) = match &task.input {
                    Some(json) => match serde_json::from_str::<dag::TaskSpec>(json) {
                        Ok(spec) => {
                            // Carried through dispatch and echoed back in the
                            // TaskResult (B-4), so the result path never
                            // re-reads and re-parses this row's input JSON.
                            let carried_spec = Box::new(spec.clone());
                            // Sub-workflow trigger target (#23) and wait-sensor
                            // config (#27), captured before `spec`'s fields are
                            // moved into the exec context below.
                            // The trigger's target *and* its arguments: the
                            // arguments are the child run's parameters, so they
                            // travel together or the child is created without
                            // them and every conversation looks the same.
                            let sub_workflow = if spec.task_type.as_deref() == Some("workflow") {
                                spec.workflow.clone().map(|w| (w, spec.arguments.clone()))
                            } else {
                                None
                            };
                            let wait = if spec.task_type.as_deref() == Some("wait") {
                                spec.wait.clone()
                            } else {
                                None
                            };
                            // Start with the declared env, then append any top-level
                            // string keys from `input` so parameterized reruns
                            // (deep-merged `params`) visibly change task behavior
                            // without requiring the workflow author to thread each
                            // param through an explicit `env:` entry.
                            // One binding for both feature worlds now: the open
                            // build also appends here (the `defer:` identity
                            // pair below), so the split that existed only to
                            // keep the non-enterprise build free of an unused
                            // `mut` no longer buys anything.
                            let mut env = spec.env;
                            // Behind the `enterprise` feature: merge top-level string keys
                            // from `input` as env vars so parameterized reruns
                            // (params deep-merge) visibly change
                            // task behavior without threading each param through an explicit
                            // `env:` entry. `spec.env` is authoritative: skip any key whose
                            // uppercased form already appears there, and reject names with
                            // characters outside [A-Z0-9_] to prevent injection of reserved
                            // names (PATH, HOME, etc.).
                            #[cfg(feature = "enterprise")]
                            if let Some(serde_json::Value::Object(map)) = &spec.input {
                                let declared: std::collections::HashSet<String> =
                                    env.iter().map(|e| e.name.clone()).collect();
                                for (k, v) in map {
                                    if let Some(s) = v.as_str() {
                                        let name = k.to_uppercase();
                                        if declared.contains(&name)
                                            || !name.chars().all(|c| c == '_' || c.is_ascii_alphanumeric())
                                        {
                                            continue;
                                        }
                                        env.push(dag::EnvVar { name, value: s.to_string(), value_from: None });
                                    }
                                }
                            }
                            // A deferred submit needs a name for its remote
                            // job that is the SAME on a post-crash resubmit and
                            // DIFFERENT on a deliberate retry. These two are
                            // that name: `dagron-<task_id>-<epoch>`.
                            //
                            // `task_runs.id` is stable across lease recovery, so
                            // a resubmit after a crash reuses the name, the
                            // remote system answers AlreadyExists, and the step
                            // adopts the running job rather than starting a
                            // second one. `attempt` is deliberately not used —
                            // it increments on every claim *including* recovery,
                            // so a name built from it changes at exactly the
                            // moment adoption is needed.
                            //
                            // Pushed here, while the spec is still in scope and
                            // before `resolve_secrets`, for the same ordering
                            // reason the resume pointers are: substituting after
                            // secret resolution would make any secret whose
                            // plaintext contains `{{ … }}` a template-injection
                            // surface.
                            if spec.defer.is_some() {
                                env.push(dag::EnvVar {
                                    name: "DAGRON_TASK_ID".to_string(),
                                    value: task.id.clone(),
                                    value_from: None,
                                });
                                env.push(dag::EnvVar {
                                    name: "DAGRON_EXTERNAL_EPOCH".to_string(),
                                    value: task.external_epoch.to_string(),
                                    value_from: None,
                                });
                            }
                            // Raise the task's declared envelope to the
                            // operator's floor, then refuse it outright if this
                            // executor cannot deliver what it now says. Both
                            // steps happen here, at dispatch, because this is
                            // the last point that sees the task *and* knows
                            // which executor will run it — and because a task
                            // that runs believing it is sandboxed when it is
                            // not is the one outcome this must never produce.
                            let isolation = match (&spec.isolation, &isolation_floor) {
                                (None, None) => None,
                                (declared, floor) => {
                                    let declared = declared.clone().unwrap_or_default();
                                    let (effective, tightened) = match floor {
                                        Some(f) => declared.apply_floor(f),
                                        None => (declared, Vec::new()),
                                    };
                                    let overridden: Vec<String> = tightened
                                        .iter()
                                        .filter(|t| t.requested != "unset")
                                        .map(|t| t.to_string())
                                        .collect();
                                    if !overridden.is_empty() {
                                        tracing::warn!(
                                            task = %task.name,
                                            tightened = %overridden.join("; "),
                                            "isolation floor overrode what this task asked for"
                                        );
                                    }
                                    // Enforceability is checked after this
                                    // match, where a single task can be failed
                                    // without taking the loop with it.
                                    if effective.is_empty() { None } else { Some(effective) }
                                }
                            };
                            (
                                ExecContext {
                                    command: spec.command,
                                    timeout_secs: spec.timeout_secs,
                                    docker_image: spec.docker_image,
                                    env,
                                    resources: spec.resources,
                                    service_account: spec.service_account,
                                    isolation,
                                    // Wired per-attempt by the worker from `log_tx`.
                                    log_sink: None,
                                    // Stamped below, once, for every arm.
                                    identity: None,
                                },
                                spec.max_attempts,
                                spec.retry_delay_secs,
                                spec.retry_max_delay_secs,
                                spec.retry_on_timeout.unwrap_or(true),
                                spec.cache,
                                sub_workflow,
                                wait,
                                // Carried out of the parse so the gang rendezvous
                                // env below needs no second deserialization.
                                spec.gang_member,
                                // Carried so a cache hit can record the same
                                // dataset updates a normal success would.
                                spec.produces,
                                Some(carried_spec),
                            )
                        }
                        Err(e) => {
                            // Poison row: a persisted spec this build can't parse.
                            // Failing the whole loop here would crash-loop the
                            // daemon every time the lease is recovered, so fail
                            // just this task terminally and carry on.
                            tracing::error!(
                                task = %task.name, task_id = %task.id, error = %e,
                                "unparseable task spec — marking task failed"
                            );
                            if db::mark_task_failed(
                                &pool,
                                &task.id,
                                &worker_id,
                                task.version.saturating_add(1),
                                Some(format!("unparseable task spec: {e}")),
                            )
                            .await?
                            {
                                task_finished(&metrics, &seams, false).await;
                            }
                            continue;
                        }
                    },
                    None => (ExecContext::new(vec!["true".to_string()], None, None), 1, 0, None, true, None, None, None, None, Vec::new(), None),
                };

                // Stamp who this execution belongs to, for every arm above.
                //
                // A backend that creates a remote workload labels it with this,
                // which is what lets a workload be found by something other than
                // the process that created it. Without it, `KubeExecutor` and
                // `DockerExecutor` named their pod/container after a random UUID
                // held only on the creating task's stack — so a lease expiry
                // started a SECOND one while the first still ran, and a
                // scheduler crash orphaned the first beyond any possibility of
                // cleanup. `attempt` is the discriminator because it increments
                // on every claim, recovery included: a workload carrying a
                // different attempt for this task id is a predecessor.
                ctx.identity = Some(dagron_executor::executor::TaskIdentity {
                    task_id: task.id.clone(),
                    run_id: task.run_id.clone(),
                    attempt: task.attempt,
                    // Stamped even when the sweep is off, so arming it later
                    // finds the workloads already running rather than only
                    // those dispatched after the restart.
                    installation: installation.clone(),
                });

                // Refuse a trust envelope this executor cannot deliver — as a
                // terminal failure of THIS task, never of the loop. Same
                // reasoning as the unparseable-spec arm above, with a sharper
                // edge: in a multi-tenant engine, propagating here would let any
                // tenant halt a scheduler running everyone else's work by
                // submitting one workflow that asks for gVisor on a plain
                // Docker runner.
                if let (Some(iso), Some(kind)) = (&ctx.isolation, executor_isolation) {
                    if let Err(e) = iso.require_enforceable_by(kind) {
                        tracing::error!(
                            task = %task.name, task_id = %task.id, error = %e,
                            "declared isolation cannot be enforced — marking task failed"
                        );
                        if db::mark_task_failed(
                            &pool,
                            &task.id,
                            &worker_id,
                            task.version.saturating_add(1),
                            Some(e.to_string()),
                        )
                        .await?
                        {
                            task_finished(&metrics, &seams, false).await;
                        }
                        continue;
                    }
                }

                // Deferrable wait sensor (#27): park this task with no worker held
                // and let a reconcile sweep resolve it. A `wait.url` HTTP sensor
                // (#27 follow-on) parks on the endpoint and the poll sweep GETs it;
                // a `wait.dataset` sensor parks on the lineage ledger's current
                // high-water mark and resolves on the next recorded update; a
                // time sensor parks on its resume deadline.
                if let Some(w) = &wait {
                    let fence = task.version.saturating_add(1);
                    let now = chrono::Utc::now();
                    if let Some(ds) = &w.dataset {
                        db::park_wait_dataset(&pool, &task.id, fence, ds.trim()).await?;
                        info!(task = %task.name, task_id = %task.id, dataset = %ds.trim(), "dataset sensor deferred — parked with no worker until the dataset next updates");
                        continue;
                    }
                    if let Some(url) = &w.url {
                        // Park immediately due for its first poll on the next tick.
                        db::park_wait_url(&pool, &task.id, fence, url.trim(), &now.to_rfc3339()).await?;
                        info!(task = %task.name, task_id = %task.id, url = %url.trim(), "http wait sensor deferred — parked with no worker until the endpoint returns 2xx");
                        continue;
                    }
                    // Spec validation normally rejects an unparseable deadline,
                    // but a row persisted by a different build lands here — and
                    // defaulting to "already elapsed" would silently succeed the
                    // sensor and unblock the whole downstream DAG. Fail instead.
                    let deadline: Result<chrono::DateTime<chrono::Utc>, String> =
                        if let Some(dur) = &w.wait_for {
                            dag::parse_duration_secs(dur)
                                .map(|secs| {
                                    now + chrono::TimeDelta::seconds(secs.min(i64::MAX as u64) as i64)
                                })
                                .map_err(|e| format!("invalid wait.for '{dur}': {e}"))
                        } else if let Some(until) = &w.until {
                            chrono::DateTime::parse_from_rfc3339(until)
                                .map(|t| t.with_timezone(&chrono::Utc))
                                .map_err(|e| format!("invalid wait.until '{until}': {e}"))
                        } else {
                            Ok(now)
                        };
                    let wake_dt = match deadline {
                        Ok(dt) => dt,
                        Err(msg) => {
                            tracing::error!(task = %task.name, task_id = %task.id, %msg, "unreadable wait deadline — failing task");
                            if db::mark_task_failed(&pool, &task.id, &worker_id, fence, Some(msg))
                                .await?
                            {
                                task_finished(&metrics, &seams, false).await;
                            }
                            continue;
                        }
                    };
                    if wake_dt <= now {
                        // Already past: the sensor succeeds here and never parks,
                        // so this is the one place it can be counted.
                        if db::mark_task_succeeded(&pool, &task.id, &worker_id, fence, Some("wait elapsed".into())).await? {
                            task_finished(&metrics, &seams, true).await;
                        }
                    } else {
                        db::park_wait(&pool, &task.id, fence, &wake_dt.to_rfc3339()).await?;
                        info!(task = %task.name, task_id = %task.id, wake_at = %wake_dt.to_rfc3339(), "wait sensor deferred — parked with no worker until the deadline");
                    }
                    continue;
                }

                // Sub-workflow trigger (#23): submit the named registered workflow
                // as a child run and park this task until the child is terminal —
                // no worker is dispatched. The parent succeeds/fails with the child.
                if let Some((child_name, child_args)) = &sub_workflow {
                    let fence = task.version.saturating_add(1);
                    // Recursion guard: nothing stops a workflow from naming
                    // itself (directly or through a cycle of workflows), and
                    // each hop creates a real run whose parked parent holds a
                    // row — so an unguarded self-reference is an unbounded run
                    // factory, not a stack overflow that fails loudly. Depth is
                    // read by walking `sub_run_id` up from this task's own run;
                    // the walk stops at the cap, so the check costs at most
                    // `SUBWORKFLOW_MAX_DEPTH` lookups on an indexed column.
                    let depth = db::sub_workflow_depth(&pool, &task.run_id, subworkflow_max_depth)
                        .await
                        .unwrap_or(0);
                    if depth >= subworkflow_max_depth {
                        tracing::error!(
                            task = %task.name, workflow = %child_name, depth,
                            max = subworkflow_max_depth,
                            "sub-workflow nesting hit SUBWORKFLOW_MAX_DEPTH — refusing to trigger"
                        );
                        if db::mark_task_failed(
                            &pool,
                            &task.id,
                            &worker_id,
                            fence,
                            Some(format!(
                                "sub-workflow nesting depth {depth} reached SUBWORKFLOW_MAX_DEPTH ({subworkflow_max_depth}) — refusing to trigger '{child_name}' (recursive workflow?)"
                            )),
                        )
                        .await?
                        {
                            task_finished(&metrics, &seams, false).await;
                        }
                        continue;
                    }
                    match db::workflow_spec_by_name(&pool, child_name).await? {
                        None => {
                            tracing::error!(task = %task.name, workflow = %child_name, "type: workflow names an unknown workflow — failing task");
                            if db::mark_task_failed(&pool, &task.id, &worker_id, fence, Some(format!("unknown workflow '{child_name}'"))).await? {
                                task_finished(&metrics, &seams, false).await;
                            }
                        }
                        // Built *with* the trigger's arguments as parameters, the
                        // same call `POST /api/runs` makes for a caller-supplied
                        // `parameters` map. The stored spec below is still the
                        // child's own YAML — parameters are applied to the graph,
                        // not written into the definition, so the workflow keeps
                        // one definition however many ways it is called.
                        Some(child_yaml) => match dag::DagGraph::from_yaml_with_params(
                            &child_yaml,
                            child_args,
                        ) {
                            Err(e) => {
                                tracing::error!(task = %task.name, workflow = %child_name, error = %e, "child workflow spec no longer parses — failing task");
                                if db::mark_task_failed(&pool, &task.id, &worker_id, fence, Some(format!("child workflow '{child_name}' invalid: {e}"))).await? {
                                    task_finished(&metrics, &seams, false).await;
                                }
                            }
                            Ok(child_dag) => match db::create_run(&pool, &child_dag, &child_yaml).await {
                                Ok(child_run) => {
                                    db::park_subworkflow(&pool, &task.id, fence, &child_run).await?;
                                    info!(task = %task.name, task_id = %task.id, workflow = %child_name, %child_run, "sub-workflow triggered — parking until it finishes");
                                }
                                Err(e)
                                    if dagron_core::models::is_capacity_refusal(&e) =>
                                {
                                    // Child refused on capacity — its concurrency
                                    // cap, or the datastore's free-disk floor —
                                    // not on fault: release the task back to ready
                                    // and retry on a later tick.
                                    db::release_subworkflow_task(&pool, &task.id, fence).await?;
                                    info!(task = %task.name, workflow = %child_name, reason = %e, "child workflow refused admission (capacity) — will retry");
                                }
                                Err(e) => {
                                    if db::mark_task_failed(&pool, &task.id, &worker_id, fence, Some(format!("failed to start child workflow '{child_name}': {e}"))).await? {
                                        task_finished(&metrics, &seams, false).await;
                                    }
                                }
                            },
                        },
                    }
                    continue;
                }

                // Memoization (#22): a cache hit resolves the task from a prior
                // run's output with no worker, secrets, or artifacts — then the
                // dependents advance exactly as for a normal success.
                if let Some(cache) = &cache {
                    let wf_name =
                        db::workflow_name_for_run(&pool, &task.run_id).await?.unwrap_or_default();
                    if let Some(cached) = db::memo_lookup(
                        &pool,
                        &wf_name,
                        &task.name,
                        &cache.key,
                        cache.max_age_secs,
                    )
                    .await?
                    {
                        info!(task = %task.name, task_id = %task.id, key = %cache.key, "cache hit — reusing memoized output");
                        metrics.inc_cache_hits();
                        // `_cached` stamps `cache_hit` on the row: a hit is
                        // otherwise a normal `succeeded` with a reused output,
                        // and the log line + counter above are not attached to
                        // the run anyone is looking at.
                        let marked = db::mark_task_succeeded_cached(
                            &pool,
                            &task.id,
                            &worker_id,
                            task.version.saturating_add(1),
                            Some(cached),
                        )
                        .await?;
                        // A cache hit is a success, so it records the task's
                        // `produces:` datasets exactly as a real execution would —
                        // otherwise a downstream sensor or `on_datasets:` consumer
                        // would park forever whenever the producer hits its cache.
                        if marked {
                            // A cache hit is a real success: it advances
                            // dependents and records `produces:`, so it spends
                            // quota exactly as an execution would.
                            task_finished(&metrics, &seams, true).await;
                            newly_terminal = true;
                            record_produces(
                                &pool,
                                &metrics,
                                &wf_name,
                                &task.id,
                                &task.name,
                                &produces,
                            )
                            .await;
                        }
                        continue;
                    }
                }

                // Gang rendezvous env: members learn their gang, rank, and size
                // (torchrun-style rendezvous wires MASTER_ADDR off rank 0 — on
                // Kubernetes, a headless Service per gang_id). Not gated on
                // `gangs_on`: the spec, the expansion into member rows, and this
                // env are the open programming model — only the all-or-nothing
                // *claimer* is feature-gated. A member row carries its rank however
                // it was scheduled, so ranks must be readable in either build.
                if let Some(member) = gang_member {
                    for (name, value) in [
                        ("DAGRON_GANG_ID", member.id),
                        ("DAGRON_GANG_RANK", member.rank.to_string()),
                        ("DAGRON_GANG_SIZE", member.size.to_string()),
                    ] {
                        ctx.env.push(dag::EnvVar {
                            name: name.to_string(),
                            value,
                            value_from: None,
                        });
                    }
                }

                // Task identity env: lets a task talk about itself to the
                // management API — report a checkpoint, tail a sibling's logs —
                // without the workflow author threading ids through params.
                for (name, value) in [
                    ("DAGRON_RUN_ID", task.run_id.clone()),
                    ("DAGRON_TASK", task.name.clone()),
                    ("DAGRON_TASK_ID", task.id.clone()),
                ] {
                    ctx.env.push(dag::EnvVar {
                        name: name.to_string(),
                        value,
                        value_from: None,
                    });
                }

                // Artifact passing: give this task the run's shared artifact dir,
                // plus a per-task checkpoint dir under it (the file-convention
                // half of checkpoint-aware resume — see below).
                let mut checkpoint_dir: Option<std::path::PathBuf> = None;
                if let Some(store) = &artifact_store {
                    match store.prepare_run_dir(&task.run_id).await {
                        Ok(dir) => {
                            let ck = std::path::Path::new(&dir)
                                .join(".checkpoints")
                                .join(&task.name);
                            match tokio::fs::create_dir_all(&ck).await {
                                Ok(()) => {
                                    ctx.env.push(dag::EnvVar {
                                        name: "DAGRON_CHECKPOINT_DIR".to_string(),
                                        value: ck.to_string_lossy().into_owned(),
                                        value_from: None,
                                    });
                                    checkpoint_dir = Some(ck);
                                }
                                Err(e) => {
                                    tracing::warn!(error = %e, run = %task.run_id, "could not prepare checkpoint dir")
                                }
                            }
                            ctx.env.push(dag::EnvVar {
                                name: "DAGRON_ARTIFACTS".to_string(),
                                value: dir,
                                value_from: None,
                            });
                        }
                        Err(e) => {
                            tracing::warn!(error = %e, run = %task.run_id, "could not prepare artifact dir")
                        }
                    }
                }

                // Cloud artifact/checkpoint URLs: pure string composition (no
                // I/O), matching the cloud store's sanitized layout so a task's
                // uploads land where the artifact API reads them.
                if let Some(url) = &artifact_url {
                    let run_seg = dagron_artifact::sanitize_component(&task.run_id);
                    let task_seg = dagron_artifact::sanitize_component(&task.name);
                    for (name, value) in [
                        ("DAGRON_ARTIFACTS_URL", format!("{url}/{run_seg}")),
                        (
                            "DAGRON_CHECKPOINT_URL",
                            format!("{url}/{run_seg}/.checkpoints/{task_seg}"),
                        ),
                    ] {
                        ctx.env.push(dag::EnvVar {
                            name: name.to_string(),
                            value,
                            value_from: None,
                        });
                    }
                }

                // Checkpoint-aware resume: a retry attempt is handed the last
                // committed checkpoint instead of restarting from zero. The
                // pointer comes from the datastore (reported via
                // POST …/tasks/{id}/checkpoint), falling back to the
                // `<checkpoint_dir>/latest` file convention for setups that run
                // without the management API. dagron owns pointer durability;
                // what is inside the checkpoint belongs to the task.
                if task.attempt > 0 {
                    let mut resume: Option<(String, Option<String>)> = None;
                    match db::task_checkpoint(&pool, &task.id).await {
                        Ok(found @ Some(_)) => resume = found,
                        Ok(None) => {}
                        Err(e) => {
                            tracing::warn!(error = %e, task = %task.name, "checkpoint lookup failed — dispatching without resume pointer")
                        }
                    }
                    if resume.is_none() {
                        if let Some(ck) = &checkpoint_dir {
                            if let Ok(uri) = tokio::fs::read_to_string(ck.join("latest")).await {
                                let uri = uri.trim().to_string();
                                if !uri.is_empty() {
                                    resume = Some((uri, None));
                                }
                            }
                        }
                    }
                    if let Some((uri, marker)) = resume {
                        info!(task = %task.name, attempt = task.attempt + 1, resume_from = %uri, "dispatching with checkpoint resume");
                        ctx.env.push(dag::EnvVar {
                            name: "DAGRON_RESUME_FROM".to_string(),
                            value: uri,
                            value_from: None,
                        });
                        if let Some(marker) = marker {
                            ctx.env.push(dag::EnvVar {
                                name: "DAGRON_RESUME_MARKER".to_string(),
                                value: marker,
                                value_from: None,
                            });
                        }
                    }
                }


                // Resolve `value_from` secret refs into concrete env values just
                // before dispatch (#9): the run's environment secret store
                // first (DB, decrypted), then process env / secrets dir. A
                // missing secret fails the task rather than running it with an
                // empty credential.
                if let Err(e) =
                    environments::resolve_secrets(&pool, &task.run_id, &mut ctx.env).await
                {
                    tracing::error!(task = %task.name, task_id = %task.id, error = %e, "secret resolution failed — marking task failed");
                    if db::mark_task_failed(
                        &pool,
                        &task.id,
                        &worker_id,
                        task.version.saturating_add(1),
                        Some(format!("secret resolution failed: {e}")),
                    )
                    .await?
                    {
                        task_finished(&metrics, &seams, false).await;
                    }
                    continue;
                }

                // Became-claimable → dispatched (A-5). `scheduled_at` is the
                // readiness stamp (advance sets it on the ready flip; retries
                // set it to the due time), so this is the scheduler's own
                // queueing latency — the low-latency profile's SLO metric. A
                // NULL stamp (a UI retry) just skips the observation.
                if let Some(ready_at) = task
                    .scheduled_at
                    .as_deref()
                    .and_then(|s| chrono::DateTime::parse_from_rfc3339(s).ok())
                {
                    let waited = chrono::Utc::now() - ready_at.with_timezone(&chrono::Utc);
                    if let Some(us) = waited.num_microseconds().filter(|&us| us >= 0) {
                        metrics.observe_dispatch_latency(us as f64 / 1_000_000.0);
                    }
                }

                // Per-task dispatch span (#28): groups the trace-context
                // propagation and the enqueue under one `tracing` span, so an OTLP
                // exporter (`--features otel` + `OTEL_EXPORTER_OTLP_ENDPOINT`)
                // renders a span per dispatched task. Entered only around this
                // synchronous section — no `.await` occurs inside the closure, so
                // the span opens and closes cleanly on this thread.
                let dispatch_span = tracing::info_span!(
                    "task.dispatch",
                    task = %task.name,
                    task_id = %task.id,
                    run_id = %task.run_id,
                    attempt = task.attempt + 1,
                );
                dispatch_span.in_scope(|| -> anyhow::Result<()> {
                    // OpenTelemetry (#28): hand this task a W3C trace context in
                    // the standard `TRACEPARENT` carrier, so its own
                    // instrumentation joins the trace rather than starting an
                    // island. Prefer the **active** `task.dispatch` span's
                    // context, injected through the global propagator — that is
                    // what makes the task's spans children of the span the OTLP
                    // exporter ships. When no `tracing-opentelemetry` layer is
                    // installed (an `otel` build without
                    // `OTEL_EXPORTER_OTLP_ENDPOINT`) the context is empty and the
                    // propagator injects nothing, so fall back to a freshly
                    // generated context — that build still gets a valid, unique
                    // traceparent, exactly as before. Off entirely without
                    // `--features otel`.
                    #[cfg(feature = "otel")]
                    {
                        use tracing_opentelemetry::OpenTelemetrySpanExt;
                        let mut carrier = std::collections::HashMap::<String, String>::new();
                        let cx = tracing::Span::current().context();
                        opentelemetry::global::get_text_map_propagator(|p| {
                            p.inject_context(&cx, &mut carrier)
                        });
                        let (traceparent, trace_id, linked) = match carrier.remove("traceparent") {
                            Some(tp) => {
                                // "00-<32-hex trace>-<16-hex span>-<flags>"
                                let id = tp.split('-').nth(1).unwrap_or_default().to_string();
                                (tp, id, true)
                            }
                            None => {
                                let (tp, id) = new_traceparent();
                                (tp, id, false)
                            }
                        };
                        ctx.env.push(dag::EnvVar {
                            name: "TRACEPARENT".to_string(),
                            value: traceparent,
                            value_from: None,
                        });
                        info!(task = %task.name, task_id = %task.id, trace_id = %trace_id, linked, "trace context propagated to task");
                    }

                    info!(
                        task = %task.name,
                        attempt = task.attempt + 1,
                        max_attempts,
                        // Log only the program, not full argv — args may carry secrets.
                        cmd = %ctx.command.first().map(String::as_str).unwrap_or("<empty>"),
                        "dispatching"
                    );

                    workers.dispatch(DispatchPayload {
                        task_id: task.id.clone(),
                        worker_id: worker_id.clone(),
                        ctx,
                        spec: carried_spec,
                        attempt: task.attempt,
                        max_attempts,
                        retry_delay_secs,
                        retry_max_delay_secs,
                        retry_on_timeout,
                        // Post-claim version is the fencing token (claim_ready bumped
                        // version from task.version to task.version + 1). saturating_add
                        // guards the theoretical i64 overflow without a debug panic.
                        fence: task.version.saturating_add(1),
                        result_tx: tx.clone(),
                        log_tx: Some(log_tx.clone()),
                    })?;
                    metrics.inc_dispatched();
                    in_flight += 1;
                    Ok(())
                })?;
            }
        }

        // ── Step 3b: drain live-log chunks (#17) ────────────────────────────
        // Append incremental output from still-running tasks so the API/UI can
        // tail it before the task exits. Fence-guarded, so a stale attempt's late
        // chunk can't corrupt a re-run; the first chunk of an attempt resets any
        // prior-attempt output. Best-effort: a failed append is logged, not fatal.
        while let Ok(chunk) = log_rx.try_recv() {
            if let Err(e) =
                db::append_task_output(&pool, &chunk.task_id, chunk.fence, &chunk.chunk, chunk.first)
                    .await
            {
                tracing::warn!(task_id = %chunk.task_id, error = %e, "live-log append failed");
            }
        }

        // ── Step 4: collect finished tasks ──────────────────────────────────
        let mut results = std::mem::take(&mut pending_results);
        while let Ok(result) = rx.try_recv() {
            results.push(result);
        }
        for mut result in results {
            // Executor finished → drained here (A-5). Before the completion
            // wake this sat at "remainder of the poll interval" for every
            // locally-run task; it should now track tick cost alone.
            metrics.observe_result_wait(result.finished.elapsed().as_secs_f64());
            in_flight = in_flight.saturating_sub(1);

            if result.success {
                // Loop operator (`repeat:`): a successful iteration only counts
                // as task success once `until` holds. Otherwise the task is
                // re-queued (reusing the retry machinery: fence-guarded ready +
                // scheduled_at delay; `attempt` doubles as the iteration count),
                // and after max_iterations the loop fails loudly — a condition
                // that never came true is an error, not a success.
                // The spec rode the dispatch payload and came back on the
                // result (B-4) — `repeat` (below) and `cache` (the memo write
                // after success) read from it with no DB round trip and no
                // re-parse per completion.
                let task_spec: Option<dag::TaskSpec> = result.spec.take().map(|b| *b);
                let repeat = task_spec.as_ref().and_then(|s| s.repeat.clone());
                if let Some(rep) = repeat {
                    let iteration = result.attempt + 1; // the iteration that just ran
                    let output = result.output.clone().unwrap_or_default();
                    // One decision function, two callers: this path, and the
                    // sub-workflow sweep in `db::reconcile_subworkflows`. They
                    // share no machinery — this one holds a worker claim and a
                    // fence, that one holds a parked row nobody owns — so the
                    // decision is the only thing that *can* be shared, and two
                    // copies of a loop operator is how the two come to disagree
                    // about when a loop is over.
                    match rep.decide(&output, iteration) {
                        dag::RepeatDecision::Done => {} // fall through to success
                        dag::RepeatDecision::Again { delay_secs } => {
                            // Shared, panic-free retry-time computation: an
                            // unbounded `delay_secs` would otherwise panic
                            // `TimeDelta::seconds`.
                            let retry_at = dag::delayed_retry_at(delay_secs);
                            info!(
                                task_id = %result.task_id,
                                iteration,
                                max_iterations = rep.max_iterations,
                                delay_secs,
                                "repeat.until not yet satisfied — re-queueing iteration"
                            );
                            db::retry_task(
                                &pool,
                                &result.task_id,
                                &result.worker_id,
                                result.fence,
                                result.output,
                                retry_at,
                                // This iteration's output is about to be
                                // overwritten by the next one; `retry_task`
                                // keeps a bounded tail of it so the log can
                                // show the whole loop rather than its last
                                // pass (dagron_core::attempt_log).
                                dagron_core::attempt_log::AttemptEnd::Iteration,
                            )
                            .await?;
                            continue;
                        }
                        dag::RepeatDecision::Fail { reason } => {
                            info!(task_id = %result.task_id, iteration, %reason, "repeat loop failed");
                            if db::mark_task_failed(
                                &pool,
                                &result.task_id,
                                &result.worker_id,
                                result.fence,
                                Some(reason),
                            )
                            .await?
                            {
                                task_finished(&metrics, &seams, false).await;
                                newly_terminal = true;
                            }
                            continue;
                        }
                    }
                }

                // `defer:` — the command that just succeeded was a *submit*, not
                // the work. Park the row on the remote job it named instead of
                // completing the task: claim dropped, lease NULLed, still
                // `running`, handle on the row. A sweep resolves it later.
                //
                // Structurally a sibling of the `repeat:` branch above — same
                // place, same `task_spec` off the dispatch payload, same
                // `continue` past `mark_task_succeeded`.
                //
                // A submit that succeeded but named no handle is a **failure**,
                // and deliberately so: succeeding the task would advance
                // dependents on work that has not happened, which is the exact
                // silent-success the `wait: { url: … }` sensor already gets
                // wrong. Failing it is loud, costs one attempt, and retries.
                if let Some(def) = task_spec.as_ref().and_then(|s| s.defer.clone()) {
                    let output = result.output.clone().unwrap_or_default();
                    match dag::parse_handle(&output) {
                        Some(handle) => {
                            let next_poll_at = dag::delayed_retry_at(def.poll_secs);
                            let deadline_at = def.max_wait_secs.map(dag::delayed_retry_at);
                            let endpoint = std::env::var("DAGRON_DEFER_ENDPOINT").ok();
                            if db::park_external(
                                &pool,
                                &result.task_id,
                                result.fence,
                                &def.kind,
                                &handle,
                                endpoint.as_deref(),
                                &next_poll_at,
                                deadline_at.as_deref(),
                            )
                            .await?
                            {
                                info!(
                                    task_id = %result.task_id,
                                    kind = %def.kind,
                                    %handle,
                                    poll_secs = def.poll_secs,
                                    "submitted — parked on the remote job (holding no worker)"
                                );
                            } else {
                                // The fence did not hold: this attempt's lease
                                // was reclaimed while the submit ran. The row
                                // belongs to a newer attempt now, and the job
                                // this one started is an orphan — say so, because
                                // a cluster running work nobody is watching is
                                // worth a line in the log.
                                warn!(
                                    task_id = %result.task_id, %handle,
                                    "submit finished on a reclaimed lease — remote job is \
                                     orphaned and will not be polled by this row"
                                );
                            }
                            continue;
                        }
                        None => {
                            let reason = format!(
                                "defer.kind '{}': the submit exited 0 but printed no `{}<handle>` \
                                 line, so there is no remote job to wait for. The command must \
                                 print the handle its submission returned.",
                                def.kind,
                                dag::HANDLE_PREFIX
                            );
                            warn!(task_id = %result.task_id, "deferred submit named no handle");
                            if db::mark_task_failed(
                                &pool,
                                &result.task_id,
                                &result.worker_id,
                                result.fence,
                                Some(reason),
                            )
                            .await?
                            {
                                task_finished(&metrics, &seams, false).await;
                                newly_terminal = true;
                            }
                            continue;
                        }
                    }
                }

                info!(task_id = %result.task_id, "task succeeded");
                // Keep the output for the memo write below — `mark_task_succeeded`
                // consumes `result.output`, and the memo must not be written until
                // that fenced mutation has actually landed.
                let memo_output = task_spec
                    .as_ref()
                    .filter(|s| s.cache.is_some())
                    .map(|_| result.output.clone().unwrap_or_default());
                let marked = db::mark_task_succeeded(
                    &pool,
                    &result.task_id,
                    &result.worker_id,
                    result.fence,
                    result.output,
                )
                .await?;
                // Post-success side effects — the memoization write (#22) and the
                // `produces:` dataset ledger — run **only when the fence held**.
                // A reclaimed attempt's late result gets `marked == false`, and
                // must neither poison the cache with a stale output nor fabricate
                // lineage. Both are best-effort: a failure here never fails the
                // run. Only tasks that use a feature pay for the name lookup.
                if marked {
                    task_finished(&metrics, &seams, true).await;
                    newly_terminal = true;
                    let needs_wf = task_spec
                        .as_ref()
                        .is_some_and(|s| s.cache.is_some() || !s.produces.is_empty());
                    let wf = if needs_wf {
                        db::workflow_name_for_task(&pool, &result.task_id)
                            .await
                            .ok()
                            .flatten()
                            .unwrap_or_default()
                    } else {
                        String::new()
                    };
                    if let (Some(spec), Some(cache), Some(output)) = (
                        task_spec.as_ref(),
                        task_spec.as_ref().and_then(|s| s.cache.as_ref()),
                        memo_output,
                    ) {
                        if let Err(e) =
                            db::memo_store(&pool, &wf, &spec.name, &cache.key, &output).await
                        {
                            tracing::warn!(error = %e, task_id = %result.task_id, "memo store failed (best-effort)");
                        }
                    }
                    if let Some(spec) = task_spec.as_ref() {
                        record_produces(
                            &pool,
                            &metrics,
                            &wf,
                            &result.task_id,
                            &spec.name,
                            &spec.produces,
                        )
                        .await;
                    }
                }
            } else {
                // attempt + 1 = the attempt number that just ran (claim_ready increments
                // the counter in the DB, but the snapshot we received is pre-claim).
                let actual_attempt = result.attempt + 1;

                // ── Fault attribution ────────────────────────────────────────
                // What broke, decided before whether another attempt is worth
                // anything. The classifier reads the failure text the executor
                // already handed back, so this costs one scan of a string that
                // is in hand — no extra DB read, no log fetch, nothing that can
                // make the retry path slower or more fallible than it was.
                //
                // Classification never *blocks* the transition: an unmatched
                // failure is `None`, which resolves the budget to
                // `max_attempts` — exactly the pre-attribution behaviour.
                let classification = result
                    .output
                    .as_deref()
                    .and_then(dagron_core::fault::classify_text);
                let fault_class = classification.as_ref().map(|c| c.class);
                // The author's per-class budget, if they wrote one for *this*
                // class. Read off the spec that already rode the dispatch
                // payload (B-4) — no re-parse of the row's input JSON.
                let budget_override = fault_class.and_then(|c| {
                    result
                        .spec
                        .as_ref()
                        .and_then(|s| s.retry_budgets.get(c.as_str()).copied())
                });
                if let Some(c) = classification.as_ref() {
                    metrics.inc_fault(c.class);
                    // Fenced like the transition below it, and best-effort: a
                    // lost breadcrumb must never cost a state transition, and a
                    // stale attempt's write is rejected by the fence rather
                    // than relabelling the newer attempt's failure.
                    if let Err(e) = db::record_task_fault(
                        &pool,
                        &result.task_id,
                        &result.worker_id,
                        result.fence,
                        c.class,
                        Some(c.evidence.as_str()),
                        c.confidence,
                    )
                    .await
                    {
                        tracing::warn!(
                            error = %e, task_id = %result.task_id,
                            "recording fault attribution failed (best-effort)"
                        );
                    }
                }
                // The budget actually in force, resolved once so the log can
                // say which number it used: "not retrying, budget 1 for
                // nan-loss" is an answer; "not retrying" is not.
                let budget = dagron_core::models::effective_budget(
                    result.max_attempts,
                    fault_class,
                    budget_override,
                );

                // Normally retry while the budget allows, but a `timeout_secs`
                // deadline kill on a task with `retry_on_timeout: false` fails at
                // once — a timeout usually recurs (#24).
                if dagron_core::models::should_retry_failed_with_class(
                    actual_attempt,
                    result.max_attempts,
                    result.timed_out,
                    result.retry_on_timeout,
                    fault_class,
                    budget_override,
                ) {
                    // Exponential backoff: base * 2^(attempt-1), capped at 2^10 doublings
                    // and clamped to the spec's optional retry_max_delay_secs ceiling.
                    let shift = (actual_attempt as u32).saturating_sub(1).min(10);
                    let mut delay_secs = result.retry_delay_secs.saturating_mul(1u64 << shift);
                    if let Some(cap) = result.retry_max_delay_secs {
                        delay_secs = delay_secs.min(cap);
                    }
                    let delay_i64 = i64::try_from(delay_secs).unwrap_or(i64::MAX);
                    let retry_at = (chrono::Utc::now()
                        + chrono::TimeDelta::seconds(delay_i64))
                    .to_rfc3339();
                    info!(
                        task_id = %result.task_id,
                        attempt = actual_attempt,
                        max_attempts = result.max_attempts,
                        budget,
                        fault_class = fault_class.map(|c| c.as_str()).unwrap_or("unclassified"),
                        retry_in_secs = delay_secs,
                        "task failed — scheduling retry"
                    );
                    metrics.inc_retried();
                    db::retry_task(
                        &pool,
                        &result.task_id,
                        &result.worker_id,
                        result.fence,
                        result.output,
                        retry_at,
                        // Same overwrite, different reason: without a retained
                        // tail the attempts that explain the failure are gone
                        // and only the one that eventually passed is readable.
                        dagron_core::attempt_log::AttemptEnd::Failed,
                    )
                    .await?;
                } else {
                    // Terminal failure. Either attempts are exhausted, or the task
                    // timed out and opted out of timeout retries (#24) — record
                    // which so an operator can tell a budget-exhausted task from a
                    // deliberate no-retry-on-timeout one.
                    let not_retried_due_to_timeout = result.timed_out && !result.retry_on_timeout;
                    info!(
                        task_id = %result.task_id,
                        attempt = actual_attempt,
                        max_attempts = result.max_attempts,
                        // The budget in force and what set it — the difference
                        // between "we gave up" and "we deliberately did not
                        // spend eight more GPU-hours reproducing a NaN".
                        budget,
                        fault_class = fault_class.map(|c| c.as_str()).unwrap_or("unclassified"),
                        fault_disposition = fault_class
                            .map(|c| c.disposition().as_str())
                            .unwrap_or("unclassified"),
                        timed_out = result.timed_out,
                        not_retried_due_to_timeout,
                        "task failed — not retrying"
                    );
                    if db::mark_task_failed(
                        &pool,
                        &result.task_id,
                        &result.worker_id,
                        result.fence,
                        result.output,
                    )
                    .await?
                    {
                        task_finished(&metrics, &seams, false).await;
                        newly_terminal = true;
                    }
                    // Die-together: a failed gang member takes its siblings
                    // with it (their heartbeats lose the fenced claim and
                    // abort). The gang retries as a unit via run-level rerun.
                    if gangs_on {
                        match db::cancel_gang_siblings(&pool, &result.task_id).await {
                            Ok(0) => {}
                            Ok(n) => info!(task_id = %result.task_id, cancelled = n, "gang member failed — siblings cancelled"),
                            Err(e) => tracing::warn!(error = %e, "gang sibling cancel failed"),
                        }
                    }
                }
            }
        }

        // ── Step 5: finalize any runs whose tasks are all terminal ──────────
        for (run_id, status) in db::reap_completed_runs(&pool).await? {
            info!(%run_id, %status, "run complete");
            // Extension seam: no-op by default; an alternate build may emit the run event.
            seams.run_sink.on_run_completed(&run_id, &status.to_string()).await;
            // OpenLineage: emit the terminal RunEvent (best-effort — a lineage
            // backend being down never affects run execution).
            if let Some(ol) = &lineage {
                let job = db::workflow_name_for_run(&pool, &run_id)
                    .await
                    .ok()
                    .flatten()
                    .unwrap_or_else(|| run_id.clone());
                let failed = status.to_string() == "failed";
                if let Err(e) = ol.emit_run_completed(&run_id, &job, failed).await {
                    tracing::warn!(error = %e, %run_id, "OpenLineage emit failed");
                }
            }
            // Forge feedback: if the run's spec has a `notify.git` block, post the
            // terminal commit status (best-effort — a forge being down never
            // affects run execution).
            if let Some(forge) = &forge {
                post_forge_status(forge, &pool, &run_id, &status.to_string()).await;
            }
            // Operator notifications: push the terminal status to any
            // notify.webhook / notify.slack targets in the run's spec. Spawned
            // so up to four sequential HTTP posts (each with a 10s timeout)
            // can't stall task dispatch for every other run in this tick.
            {
                let pool = pool.clone();
                let run_id = run_id.clone();
                let status = status.to_string();
                tokio::spawn(async move {
                    notify::notify_run_event(&pool, &run_id, &status).await;
                });
            }
        }

        // ── Step 6: drain-mode shutdown ─────────────────────────────────────
        // Only one-shot sources (file) ever set `exhausted`; streaming queue
        // sources keep it false, so this daemon runs until killed. Once the
        // source is drained, no task is in flight, and no run is still active,
        // there is nothing left to do — exit cleanly. UNLESS an ops time-source
        // (cron / DB schedules) or the management API is active: then the process
        // is a long-running server and must stay up for future fires.
        if !stay_resident && exhausted.load(Ordering::SeqCst) && in_flight == 0 {
            let active = db::count_active_runs(&pool).await?;
            if active == 0 {
                info!("all runs drained — scheduler exiting");
                break;
            }
        }

        metrics.observe_reconcile_tick(tick_start.elapsed().as_secs_f64());

        // This tick marked something terminal, so dependents' counters just hit
        // zero: advance/claim them NOW instead of sleeping. Without this, SQLite
        // (no NOTIFY) parks a ready dependent for a full poll interval, and
        // Postgres burns a listener round trip on its own self-NOTIFY.
        if newly_terminal {
            continue;
        }

        // ── Wake sources, in priority order (docs/LOW_LATENCY.md A-1) ───────
        // 1. A worker finished a task — the completion wake. Before this
        //    existed, a locally-finished task sat undrained in the mpsc channel
        //    for the remainder of the poll interval (up to 500 ms per
        //    dependency hop, measured in docs/LOW_LATENCY.md §1); the loop's
        //    only wake sources were the NOTIFY listener and the timer, and
        //    nothing NOTIFYs between dispatch and the mark.
        // 2. The datastore waker: Postgres LISTEN/NOTIFY (a peer changed task
        //    readiness) with the poll timer as the safety net; on SQLite the
        //    timer alone. The timer also paces time-based retries and parked
        //    sensors, and covers the (rare) notification a cancelled listener
        //    poll could drop — the timer has always been that safety net.
        // Live-log chunks are deliberately NOT a wake source: tailing has no
        // latency SLO, and waking per chunk would let one chatty task spin the
        // loop at its output rate. Chunks keep draining at tick cadence.
        // Both futures are cancel-safe to drop mid-wait: an mpsc message stays
        // queued until actually received.
        tokio::select! {
            biased;
            result = rx.recv() => match result {
                Some(r) => pending_results.push(r),
                // Unreachable while the loop owns `tx` (it lives to fn end),
                // but a closed channel must degrade to timer pacing, not spin.
                None => tokio::time::sleep(poll_interval).await,
            },
            _ = waker.wait(poll_interval) => {}
        }
    }

    pool.close().await;
    // Flush any spans the OTLP batch exporter is still holding (no-op unless the
    // `otel` feature is built in and an endpoint was configured).
    dagron_logging::shutdown();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{
        dag, git_target, new_traceparent, parse_max_inflight_runs, run_images, HeaderCacheKey,
    };

    fn header(name: &str, secret: &str) -> dag::EnvVar {
        dag::EnvVar {
            name: name.to_string(),
            value: String::new(),
            value_from: Some(dag::SecretRef { secret: secret.to_string() }),
        }
    }

    /// The bug this key replaced: the sweep cached resolved `defer.http` headers
    /// under `run_id` alone, but `headers` is a property of the **task**. A run
    /// with two deferred tasks aimed at two vendors handed the second task the
    /// first one's resolved credential — vendor A's bearer token sent to vendor
    /// B, and the wrong principal authenticated even where nothing leaked.
    #[test]
    fn two_header_blocks_in_one_run_never_share_a_cache_entry() {
        let a = [header("Authorization", "VENDOR_A_TOKEN")];
        let b = [header("X-Api-Key", "VENDOR_B_KEY")];
        assert_ne!(
            HeaderCacheKey::new("run-1", &a),
            HeaderCacheKey::new("run-1", &b),
            "same run, different credentials — these must not collide"
        );
    }

    /// Two tasks naming the SAME secret still share one resolution, which is the
    /// whole point of the cache: resolution is two queries plus a decrypt.
    #[test]
    fn an_identical_header_block_still_shares_one_resolution() {
        let a = [header("Authorization", "VENDOR_A_TOKEN")];
        let same = [header("Authorization", "VENDOR_A_TOKEN")];
        assert_eq!(HeaderCacheKey::new("run-1", &a), HeaderCacheKey::new("run-1", &same));
    }

    /// The run half is load-bearing too: `environment:` is per-run, so the same
    /// header block resolves to different secrets in different runs.
    #[test]
    fn the_same_block_in_two_runs_is_two_entries() {
        let a = [header("Authorization", "VENDOR_A_TOKEN")];
        assert_ne!(HeaderCacheKey::new("run-1", &a), HeaderCacheKey::new("run-2", &a));
    }

    /// A header order swap is a different block. Over-keying costs one extra
    /// resolution; under-keying costs a credential, so the key errs that way.
    #[test]
    fn the_key_is_built_from_the_unresolved_spec() {
        let k = HeaderCacheKey::new("run-1", &[header("Authorization", "TOK")]).expect("serialises");
        assert!(k.block.contains("TOK"), "the SECRET NAME is in the key, not its value: {}", k.block);
        assert!(!k.block.contains("value_from\":null"));
    }

    /// The helper every terminal transition funnels through actually does both
    /// halves, in both directions.
    ///
    /// This test is the load-bearing half of the pair below it. A conformance
    /// scan that says "nothing bumps the counters outside `task_finished`"
    /// proves nothing on its own — an empty `task_finished` would satisfy it
    /// while metering nothing at all. So assert the behaviour here and the
    /// funnel there; neither is worth much alone.
    #[tokio::test]
    async fn task_finished_counts_and_meters_both_outcomes() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        #[derive(Default)]
        struct CountingMeter {
            ok: AtomicUsize,
            bad: AtomicUsize,
        }
        #[async_trait::async_trait]
        impl super::hooks::Meter for CountingMeter {
            async fn on_task_completed(&self, success: bool) {
                if success {
                    self.ok.fetch_add(1, Ordering::Relaxed);
                } else {
                    self.bad.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        let meter = std::sync::Arc::new(CountingMeter::default());
        let seams = super::Seams {
            meter: meter.clone(),
            ..Default::default()
        };
        let metrics = dagron_core::metrics::Metrics::new();

        super::task_finished(&metrics, &seams, true).await;
        super::task_finished(&metrics, &seams, false).await;
        super::task_finished(&metrics, &seams, false).await;

        assert_eq!(meter.ok.load(Ordering::Relaxed), 1, "one success metered");
        assert_eq!(meter.bad.load(Ordering::Relaxed), 2, "two failures metered");

        // And the counters behind `scheduler_tasks_{succeeded,failed}_total`
        // moved with it — the two must never be able to disagree, which is the
        // whole reason they share a function.
        assert_eq!(metrics.tasks_succeeded.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.tasks_failed.load(Ordering::Relaxed), 2);
    }

    /// Nothing in this crate may count or meter a task outside `task_finished`.
    ///
    /// The bug this exists to prevent has already happened twice. `Meter` is
    /// the quota seam, and for most of this engine's life only the worker-result
    /// path called it — so every park shape (wait, `wait.url`, `wait.dataset`,
    /// sub-workflow, approval gate, `defer:`) and every memo cache hit resolved
    /// without spending quota, and the task counters under-reported by the same
    /// set. Adding a park shape is exactly the change that re-opens it, and
    /// nothing about writing one makes you think about metering.
    ///
    /// So the check is not "did you remember" — it is that the symbols are
    /// unreachable. A new terminal path cannot bump a counter without going
    /// through the function that also meters, because a bare bump does not
    /// compile past this test.
    ///
    /// The needles are assembled with `concat!` so this test's own source does
    /// not contain them and cannot satisfy itself — the failure mode that made
    /// the signpost conformance test in `dagron-core` vacuous three separate
    /// ways before it bit.
    #[test]
    fn counting_and_metering_a_task_is_reachable_only_through_task_finished() {
        // (needle, what it is, how many times the funnel itself uses it)
        let needles: [(&str, &str, usize); 3] = [
            (concat!("inc_", "succeeded()"), "the succeeded counter", 1),
            (concat!("inc_", "failed()"), "the failed counter", 1),
            (concat!("meter.on_task_", "completed("), "the quota seam", 1),
        ];

        let src_dir = std::path::PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/src"));
        let mut totals = [0usize; 3];
        let mut seen_any_file = false;

        // Walked at runtime rather than listed as `include_str!`s: a module
        // added next week must be covered without anyone remembering to add it
        // here, which is the same failure this test is about.
        let mut stack = vec![src_dir.clone()];
        while let Some(dir) = stack.pop() {
            for entry in std::fs::read_dir(&dir).expect("read dagron-engine/src") {
                let path = entry.expect("dir entry").path();
                if path.is_dir() {
                    stack.push(path);
                    continue;
                }
                if path.extension().and_then(|e| e.to_str()) != Some("rs") {
                    continue;
                }
                seen_any_file = true;
                let text = std::fs::read_to_string(&path).expect("read source file");
                for (i, (needle, _, _)) in needles.iter().enumerate() {
                    totals[i] += text.matches(needle).count();
                }
            }
        }
        assert!(seen_any_file, "the source walk found no .rs files — it is not looking where it thinks");

        for (i, (needle, what, allowed)) in needles.iter().enumerate() {
            assert_eq!(
                totals[i], *allowed,
                "{what} ({needle}) is used {} times across dagron-engine/src, expected {allowed} \
                 — all inside task_finished. A terminal transition that bumps it directly \
                 desynchronises the counters from the Meter quota seam; call task_finished instead.",
                totals[i]
            );
        }
    }

    /// `{{ run.images }}` in a `notify.git` field: the distinct images the
    /// spec's tasks declare, in the order they first appear, with the workflow's
    /// `task_defaults` filling in for a task that names none.
    ///
    /// This is what puts the built image in the check on a pull request that
    /// changed its recipe — the reference is content-addressed, so it is in the
    /// spec rather than something the run has to report back.
    #[test]
    /// The README's `notify.git` example, verbatim: it advertises
    /// `{{ run.images }}`, so its task must actually name an image or the
    /// rendered description is the word "built" and a space.
    #[test]
    fn the_readme_notify_example_resolves_run_images() {
        let spec: dag::DagSpec = serde_yaml::from_str(
            "name: ci_build\n\
             parameters:\n  commit_sha: \"\"\n\
             tasks:\n\
             - { name: build, docker_image: golang:1.23, command: [\"make\"] }\n",
        )
        .expect("the README example must parse as a spec");
        assert_eq!(run_images(&spec), "golang:1.23");
    }

    fn run_images_lists_the_distinct_task_images() {
        let spec = |yaml: &str| -> dag::DagSpec { serde_yaml::from_str(yaml).unwrap() };

        // The image-build shape: one recipe, one built image, used twice.
        let s = spec(
            "name: etl\n\
             tasks:\n\
             - name: build\n  command: [dagron-build]\n  docker_image: registry.local/build:1\n\
             - name: load\n  command: [x]\n  docker_image: registry.local/ws/etl:r-d086f3af\n\
             - name: check\n  command: [y]\n  docker_image: registry.local/ws/etl:r-d086f3af\n",
        );
        assert_eq!(
            run_images(&s),
            "registry.local/build:1, registry.local/ws/etl:r-d086f3af",
            "each image once, in first-appearance order"
        );

        // `task_defaults` stands in for a task that names no image, because
        // that is where a DRY workflow puts the one image it runs everything on.
        let s = spec(
            "name: etl\n\
             task_defaults:\n  docker_image: registry.local/ws/etl:r-abc\n\
             tasks:\n\
             - name: a\n  command: [x]\n\
             - name: b\n  command: [y]\n",
        );
        assert_eq!(run_images(&s), "registry.local/ws/etl:r-abc");

        // A local-executor workflow names no image at all. Empty is the honest
        // answer; inventing one would put a lie in a commit status.
        let s = spec("name: etl\ntasks:\n- name: a\n  command: [x]\n");
        assert_eq!(run_images(&s), "");
    }

    /// A `notify.git` block resolved as the engine resolves it at finalization.
    ///
    /// The case this exists for: a pull request changes an image recipe, CI
    /// submits the run with the commit SHA as a parameter, and the check that
    /// lands on the PR names the image the change produced instead of saying
    /// only that something passed.
    #[test]
    fn a_notify_git_block_resolves_parameters_and_run_scoped_names() {
        let spec: dag::DagSpec = serde_yaml::from_str(
            "name: etl\n\
             parameters:\n  commit_sha: 9f1c2e4\n  repo: acme/etl\n\
             notify:\n  git:\n\
             \x20   provider: github\n\
             \x20   repo: \"{{ repo }}\"\n\
             \x20   sha: \"{{ commit_sha }}\"\n\
             \x20   context: dagron/image\n\
             \x20   target_url: \"https://dagron.example/runs/{{ run.id }}\"\n\
             \x20   description: \"{{ run.workflow }} {{ run.status }}: built {{ run.images }}\"\n\
             tasks:\n\
             - name: load\n  command: [x]\n  docker_image: registry.local/ws/etl:r-d086f3af\n",
        )
        .unwrap();

        let t = git_target(&spec, "run-42", "succeeded").expect("a resolved target");
        assert_eq!(t.repo, "acme/etl");
        assert_eq!(t.sha, "9f1c2e4");
        assert_eq!(t.context, "dagron/image");
        // run.id in target_url is the reason it is run-scoped: a caller cannot
        // pass in an id the engine has not minted yet.
        assert_eq!(
            t.target_url.as_deref(),
            Some("https://dagron.example/runs/run-42")
        );
        assert_eq!(
            t.description.as_deref(),
            Some("etl succeeded: built registry.local/ws/etl:r-d086f3af")
        );

        // And the whole chain, to the body that goes on the wire.
        let (url, body) = dagron_forge::github_request(
            "https://api.github.com",
            &t,
            dagron_forge::CommitState::Success,
        );
        assert_eq!(url, "https://api.github.com/repos/acme/etl/statuses/9f1c2e4");
        assert_eq!(body["state"], "success");
        assert_eq!(
            body["description"],
            "etl succeeded: built registry.local/ws/etl:r-d086f3af"
        );

        // The same spec on a failed run says so, from the same template.
        let t = git_target(&spec, "run-42", "failed").unwrap();
        assert!(t.description.unwrap().contains("etl failed:"));
    }

    /// A parameter cannot shadow a run-scoped name, and an unresolved `{{ … }}`
    /// in the SHA skips the post rather than attaching a status to a commit
    /// called `{{ commit_sha }}`.
    #[test]
    fn run_scoped_names_are_reserved_and_an_unresolved_sha_skips() {
        // A workflow parameter literally named `run.status` is not reachable
        // through `{{ run.status }}` — the engine's value wins.
        let spec: dag::DagSpec = serde_yaml::from_str(
            "name: etl\n\
             parameters:\n  \"run.status\": lies\n  sha: abc\n\
             notify:\n  git:\n\
             \x20   provider: github\n\
             \x20   repo: acme/etl\n\
             \x20   sha: \"{{ sha }}\"\n\
             \x20   description: \"{{ run.status }}\"\n\
             tasks:\n- name: a\n  command: [x]\n",
        )
        .unwrap();
        assert_eq!(
            git_target(&spec, "run-1", "succeeded").unwrap().description.as_deref(),
            Some("succeeded")
        );

        // A SHA whose parameter was never supplied: skipped, because posting a
        // status against the literal text would attach it to nothing.
        let spec: dag::DagSpec = serde_yaml::from_str(
            "name: etl\n\
             notify:\n  git:\n\
             \x20   provider: github\n\
             \x20   repo: acme/etl\n\
             \x20   sha: \"{{ commit_sha }}\"\n\
             tasks:\n- name: a\n  command: [x]\n",
        )
        .unwrap();
        assert!(git_target(&spec, "run-1", "succeeded").is_none());

        // No notify block at all is the common case and stays silent.
        let spec: dag::DagSpec =
            serde_yaml::from_str("name: etl\ntasks:\n- name: a\n  command: [x]\n").unwrap();
        assert!(git_target(&spec, "run-1", "succeeded").is_none());
    }

    /// A spec written before this field existed still parses and still gets the
    /// wording it always got — the field is additive, not a migration.
    #[test]
    fn a_spec_without_a_description_keeps_the_old_check() {
        let spec: dag::DagSpec = serde_yaml::from_str(
            "name: etl\n\
             notify:\n  git:\n\
             \x20   provider: gitlab\n\
             \x20   repo: group/etl\n\
             \x20   sha: deadbeef\n\
             tasks:\n- name: a\n  command: [x]\n",
        )
        .unwrap();
        let t = git_target(&spec, "run-1", "succeeded").unwrap();
        assert!(t.description.is_none());
        assert_eq!(t.context, "dagron", "the default context is unchanged");
        let (_, body) = dagron_forge::gitlab_request(
            "https://gitlab.com/api/v4",
            &t,
            dagron_forge::CommitState::Success,
        );
        assert_eq!(body["description"], "dagron run succeeded");
    }

    /// `MAX_INFLIGHT_RUNS` contract, as the Helm chart and `values.yaml`
    /// document it: default 64, an explicit number is honoured verbatim, and
    /// `0` **disables** the cap rather than clamping to a cap of one.
    #[test]
    fn max_inflight_runs_zero_disables_the_cap() {
        let cap = |s: &str| parse_max_inflight_runs(Some(s.to_string()));
        assert_eq!(parse_max_inflight_runs(None), 64, "unset → default");
        assert_eq!(cap("200"), 200);
        assert_eq!(cap(" 200 "), 200, "whitespace tolerated");
        assert_eq!(cap("1"), 1, "a cap of one is still a cap");
        assert_eq!(cap("0"), 0, "0 disables — never clamped to 1");
        assert_eq!(cap("-5"), 0, "negative normalizes to disabled");
        assert_eq!(cap("banana"), 64, "unparseable → default");
        assert_eq!(cap(""), 64, "empty → default");
    }

    /// A generated traceparent is a valid W3C header: version 00, 32-hex trace
    /// id, 16-hex span id, sampled flag — and the trace id is never all-zero.
    #[test]
    fn traceparent_is_valid_w3c() {
        let (tp, trace_id) = new_traceparent();
        let parts: Vec<&str> = tp.split('-').collect();
        assert_eq!(parts.len(), 4, "traceparent has four fields: {tp}");
        assert_eq!(parts[0], "00", "version 00");
        assert_eq!(parts[1].len(), 32, "trace id is 16 bytes");
        assert_eq!(parts[2].len(), 16, "span id is 8 bytes");
        assert_eq!(parts[3], "01", "sampled");
        assert_eq!(parts[1], trace_id, "returned trace id matches the header");
        assert!(parts[1].chars().all(|c| c.is_ascii_hexdigit()));
        assert!(parts[2].chars().all(|c| c.is_ascii_hexdigit()));
        assert_ne!(parts[1], "0".repeat(32), "trace id is never the forbidden all-zero");
        // Two calls produce distinct traces.
        assert_ne!(new_traceparent().0, tp);
    }

    use super::{poll_one, PollPlan};
    use crate::hooks::{ExternalPoller, PollCtx, Verdict};
    use std::sync::Arc;

    fn park(kind: &str, handle: &str) -> dagron_core::models::ExternalPark {
        dagron_core::models::ExternalPark {
            id: "task-1".into(),
            name: "rollup".into(),
            run_id: "run-1".into(),
            external_kind: kind.into(),
            external_handle: handle.into(),
            external_endpoint: None,
            external_epoch: 1,
            external_deadline_at: None,
            input: None,
        }
    }

    struct Fake;
    #[async_trait::async_trait]
    impl ExternalPoller for Fake {
        async fn poll(&self, ctx: &PollCtx<'_>) -> anyhow::Result<Option<Verdict>> {
            match (ctx.kind, ctx.handle) {
                ("mine", "done") => Ok(Some(Verdict::Succeeded { output: "ok".into() })),
                ("mine", "throttled") => Err(anyhow::anyhow!("429 Too Many Requests")),
                _ => Ok(None), // not my kind
            }
        }
    }

    struct Hangs;
    #[async_trait::async_trait]
    impl ExternalPoller for Hangs {
        async fn poll(&self, _ctx: &PollCtx<'_>) -> anyhow::Result<Option<Verdict>> {
            std::future::pending::<()>().await;
            unreachable!()
        }
    }

    fn client() -> reqwest::Client {
        reqwest::Client::builder().build().expect("client")
    }

    #[tokio::test]
    async fn a_registered_poller_that_claims_the_row_settles_it() {
        let p: Option<Arc<dyn ExternalPoller>> = Some(Arc::new(Fake));
        let plan = PollPlan { spec: None, http_unresolved: false };
        let (v, unresolvable) = poll_one(&p, &client(), &park("mine", "done"), &plan).await;
        assert!(matches!(v, Some(Verdict::Succeeded { .. })));
        assert!(!unresolvable, "something owns this kind");
    }

    /// The once-per-kind warning's actual trigger: nothing owns the kind and the
    /// task declares no `defer.http:` block, so no future sweep can resolve it
    /// either. This is the only case that should say so.
    #[tokio::test]
    async fn a_row_no_transport_can_ever_resolve_is_reported_unresolvable() {
        let p: Option<Arc<dyn ExternalPoller>> = Some(Arc::new(Fake));
        let plan = PollPlan { spec: None, http_unresolved: false };
        let (v, unresolvable) = poll_one(&p, &client(), &park("other", "x"), &plan).await;
        assert!(v.is_none());
        assert!(unresolvable, "no poller claimed it and it declares no http block");
    }

    /// The regression this pair exists to prevent. Headers that would not
    /// resolve are a **transient** — the secret may appear, the environment may
    /// be fixed — so the row must not be reported as something no build can
    /// resolve. Telling an operator to register a poller when their credential
    /// is simply missing points them at the wrong problem.
    #[tokio::test]
    async fn unresolvable_headers_are_not_reported_as_an_unresolvable_kind() {
        let p: Option<Arc<dyn ExternalPoller>> = Some(Arc::new(Fake));
        let plan = PollPlan { spec: None, http_unresolved: true };
        let (v, unresolvable) = poll_one(&p, &client(), &park("other", "x"), &plan).await;
        assert!(v.is_none(), "still no verdict this sweep");
        assert!(!unresolvable, "a missing credential is transient, not a missing transport");
    }

    /// And the row still reaches the registered poller even when its headers
    /// failed: a poller needs no header of ours, so dropping the row would
    /// silently stop resolving jobs it was handling perfectly well.
    #[tokio::test]
    async fn a_row_with_unresolvable_headers_is_still_offered_to_the_poller() {
        let p: Option<Arc<dyn ExternalPoller>> = Some(Arc::new(Fake));
        let plan = PollPlan { spec: None, http_unresolved: true };
        let (v, _) = poll_one(&p, &client(), &park("mine", "done"), &plan).await;
        assert!(matches!(v, Some(Verdict::Succeeded { .. })), "the poller still got its chance");
    }

    /// A transport failure is never a verdict — a 429 says nothing about the
    /// job, and failing on it would kill a healthy six-hour run.
    #[tokio::test]
    async fn a_poller_error_is_not_a_verdict() {
        let p: Option<Arc<dyn ExternalPoller>> = Some(Arc::new(Fake));
        let plan = PollPlan { spec: None, http_unresolved: false };
        let (v, unresolvable) = poll_one(&p, &client(), &park("mine", "throttled"), &plan).await;
        assert!(v.is_none());
        assert!(!unresolvable, "it reached for the row; the kind is owned");
    }

    /// Concurrency alone does not bound a blocked implementation: without this
    /// deadline one hung poller holds a JoinSet slot forever and the sweep that
    /// awaits the set never finishes its pass. Expiry re-parks, never fails.
    #[tokio::test(start_paused = true)]
    async fn a_hung_poller_times_out_and_re_parks_rather_than_failing() {
        let p: Option<Arc<dyn ExternalPoller>> = Some(Arc::new(Hangs));
        let plan = PollPlan { spec: None, http_unresolved: false };
        let (v, unresolvable) = poll_one(&p, &client(), &park("mine", "slow"), &plan).await;
        assert!(v.is_none(), "a timeout is not a verdict about the job");
        assert!(!unresolvable);
    }

    /// A registered poller is a cancel transport too, so the row keeps its handle
    /// for it.
    ///
    /// Before this, `max_wait_secs` looked only at `defer.http.cancel`: a deployment
    /// whose poller could have stopped the job had the handle cleared out from under
    /// it and was never asked. Both are named in the reason, in the order the sweep
    /// actually tries them.
    #[test]
    fn a_registered_poller_counts_as_a_cancel_transport() {
        use super::{cancel_transport, http_with_cancel};
        let spec_json = |defer: &str| {
            let yaml = format!("name: p\ntasks:\n  - {{ name: a, command: [x], defer: {defer} }}\n");
            let g = crate::dag::DagGraph::from_yaml(&yaml).unwrap();
            serde_json::to_string(g.task_spec("a").unwrap()).unwrap()
        };
        let poll_only = spec_json(r#"{ kind: k, http: { url: "https://h/j", succeed_when: "s == D" } }"#);
        let with_cancel = spec_json(
            r#"{ kind: k, http: { url: "https://h/j", succeed_when: "s == D", cancel: { url: "https://h/j/{{ handle }}" } } }"#,
        );

        // The case this fixes: a poll-only spec IS cancellable when a poller is registered.
        assert!(http_with_cancel(Some(&poll_only)).is_none(), "no http transport");
        assert_eq!(
            cancel_transport(Some(&poll_only), true),
            Some("the registered poller"),
            "the poller can stop it, so the handle must be kept"
        );

        // Neither transport: still nothing this engine can do, and the handle goes.
        assert_eq!(cancel_transport(Some(&poll_only), false), None);
        assert_eq!(cancel_transport(None, false), None);

        // A spec without any defer block still rides the poller.
        assert_eq!(cancel_transport(None, true), Some("the registered poller"));

        // http alone, and both — named in the order the sweep tries them.
        assert_eq!(cancel_transport(Some(&with_cancel), false), Some("defer.http.cancel"));
        assert_eq!(
            cancel_transport(Some(&with_cancel), true),
            Some("the registered poller, else defer.http.cancel"),
        );
    }

    /// The teardown decision: only a spec that declares `defer.http.cancel`
    /// gives the engine anything generic to send, and a poll-only block, no
    /// block, or a row without a spec all mean "nothing here can stop it".
    #[test]
    fn only_a_spec_with_a_cancel_block_is_cancellable() {
        use super::http_with_cancel;
        let spec_json = |defer: &str| {
            let yaml = format!("name: p\ntasks:\n  - {{ name: a, command: [x], defer: {defer} }}\n");
            let g = crate::dag::DagGraph::from_yaml(&yaml).unwrap();
            serde_json::to_string(g.task_spec("a").unwrap()).unwrap()
        };
        let poll_only = spec_json(r#"{ kind: k, http: { url: "https://h/j", succeed_when: "s == D" } }"#);
        let with_cancel = spec_json(
            r#"{ kind: k, http: { url: "https://h/j", succeed_when: "s == D", cancel: { url: "https://h/j/{{ handle }}" } } }"#,
        );
        let no_http = spec_json("{ kind: k }");

        assert!(http_with_cancel(Some(&poll_only)).is_none());
        assert!(http_with_cancel(Some(&no_http)).is_none());
        assert!(http_with_cancel(Some("not json")).is_none());
        assert!(http_with_cancel(None).is_none());
        let h = http_with_cancel(Some(&with_cancel)).expect("declares cancel");
        assert_eq!(h.cancel.unwrap().method(), "DELETE");
    }

}
