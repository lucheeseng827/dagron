//! Central server-settings management (docs/LOW_LATENCY.md §5).
//!
//! The engine's configuration surface is env vars — ~70 of them across the
//! crates this binary links. This module makes that surface *governable*
//! without changing how any knob is read:
//!
//! * **Registry** — every knob the engine binary reads, in one table, with its
//!   default and whether its value is a secret. `docs/CONFIG.md` documents the
//!   same list; the registry is what makes the rest of this module possible.
//! * **File layer + profiles** (`DAGRON_CONFIG=/etc/dagron/dagron.yaml`) — a
//!   reviewable YAML file layered UNDER the environment (env always wins),
//!   with an optional `profile:` applying a named preset under both. YAML, not
//!   TOML, because everything else dagron reads is YAML. Precedence:
//!   env > file > profile > compiled default.
//! * **Typo detection** — an env var that looks like one of ours but isn't in
//!   the registry warns at startup (`POLL_INTERVAL_M` silently doing nothing
//!   is the failure mode this kills); an unknown key in the config *file* is a
//!   hard startup error, because a reviewed file has no excuse.
//! * **Introspection** — `dagron config [--json]` prints every knob's
//!   effective value (secrets redacted) and where it came from
//!   (env / file / profile / default); the ops API serves the same at
//!   `GET /config`.
//! * **Fingerprint** — a stable 64-bit hash of the raw effective values,
//!   logged at startup and served with the introspection, so a fleet can
//!   alert on "replica 3 is not running the reviewed configuration".
//!
//! Deliberately NOT here: hot reload. Boot-immutability is the audit story a
//! regulated operator wants; the short allow-list of runtime tunables stays a
//! proposal (LOW_LATENCY S-5).

use std::collections::BTreeMap;
use std::sync::OnceLock;

use anyhow::{bail, Context, Result};

/// One configuration knob: its env name, the default shown when unset (display
/// text, not necessarily a parseable value — "—" means "off/unset"), and
/// whether its value must never be printed.
pub struct Knob {
    pub name: &'static str,
    pub default: &'static str,
    pub redact: bool,
}

const K: fn(&'static str, &'static str) -> Knob =
    |name, default| Knob { name, default, redact: false };
const KR: fn(&'static str, &'static str) -> Knob =
    |name, default| Knob { name, default, redact: true };

/// Every env var the engine binary reads (this crate plus the library crates
/// it links: core, executor, source, logging, artifact, crypto, forge,
/// lineage). Mechanically derived from the source; keep sorted by name.
pub fn knobs() -> &'static [Knob] {
    static KNOBS: OnceLock<Vec<Knob>> = OnceLock::new();
    KNOBS.get_or_init(|| {
        vec![
            K("API_ADDR", "— (ops API disabled)"),
            K("AUTO_BACKFILL", "— (off)"),
            K("AUTO_RERUN_FAILED", "— (off)"),
            KR("AZURE_CLIENT_ID", "—"),
            KR("AZURE_CLIENT_SECRET", "—"),
            KR("AZURE_TENANT_ID", "—"),
            K("BACKFILL_PACE_PER_TICK", "1"),
            K("CRON_CONFIG", "— (cron disabled)"),
            // How long the results service keeps a run's artifacts
            // (`actions_cache::artifacts`), and how large one may be.
            K("DAGRON_ACTIONS_ARTIFACT_MAX_BYTES", "5368709120 (5 GiB)"),
            K("DAGRON_ACTIONS_ARTIFACT_RETENTION_DAYS", "90"),
            // The results service a workflow's `actions/cache` and artifact
            // actions speak (`actions_cache`): where it listens, the URL jobs
            // reach it at, whether that may be plain http, and where uploads
            // wait.
            K(
                "DAGRON_ACTIONS_CACHE_DIR",
                "<temp dir>/dagron-actions-cache",
            ),
            K("DAGRON_ACTIONS_CACHE_INSECURE", "— (https only)"),
            K(
                "DAGRON_ACTIONS_CACHE_LISTEN",
                "— (no cache service: actions/cache misses)",
            ),
            K("DAGRON_ACTIONS_CACHE_URL", "— (no cache service)"),
            K("DAGRON_ADMISSION_FILE", "— (admission always open)"),
            // Where a task that asks for its run's artifacts over dagron-api's
            // API reaches it (`dagron_core::artifact_api`), and whether
            // that may be plain http.
            K("DAGRON_ARTIFACT_API_INSECURE", "— (https only)"),
            K(
                "DAGRON_ARTIFACT_API_URL",
                "— (a task that asks for the artifact API is refused)",
            ),
            K("DAGRON_ARTIFACT_DIR", "— (artifacts off)"),
            K("DAGRON_ARTIFACT_SYNC_SECS", "60 (0 disables)"),
            K("DAGRON_ARTIFACT_TIER", "— (tiering off)"),
            K("DAGRON_ARTIFACT_UPLINK_BYTES_PER_DAY", "— (unlimited)"),
            K("DAGRON_ARTIFACT_URL", "— (cloud artifacts off)"),
            // Per-attempt output retention (dagron_core::attempt_log). Registered
            // so an operator who turns retention down — the reason this knob
            // exists — is not told the knob is not one; the same omission that
            // made DAGRON_MAX_TASK_TIMEOUT_SECS warn for every deployment that
            // set it.
            K("DAGRON_ATTEMPT_LOG_BYTES", "4096 (0 = retention off)"),
            K("DAGRON_ATTEMPT_LOG_KEEP", "50 (0 = unlimited)"),
            K("DAGRON_BUNDLE_PUBKEYS", "— (signed bundles refused)"),
            // Path caches (`path_cache`): the store `workspace.cache` keeps
            // entries in, and what it keeps to.
            K("DAGRON_CACHE_MAX_ENTRY_BYTES", "4294967296 (4 GiB)"),
            K(
                "DAGRON_CACHE_QUOTA_BYTES",
                "10737418240 (10 GiB a repository)",
            ),
            K("DAGRON_CACHE_SWEEP_SECS", "30"),
            K("DAGRON_CACHE_UNUSED_DAYS", "7"),
            K(
                "DAGRON_CACHE_URL",
                "— (path caches off: every task starts cold)",
            ),
            K(
                "DAGRON_CACHE_URL_SECS",
                "21600 (a task with a timeout: it and 15 minutes)",
            ),
            K("DAGRON_CDEVENTS_SOURCE", "dagron"),
            // A receiver's URL can carry userinfo, so it is redacted.
            KR("DAGRON_CDEVENTS_URL", "— (CDEvents off)"),
            // What a workspace's checkout is fetched with on Docker and
            // Kubernetes, and the service each attempt's credential comes from
            // (dagron-executor's `checkout` module).
            K(
                "DAGRON_CHECKOUT_IMAGE",
                "— (a checkout fails on Docker and Kubernetes)",
            ),
            K(
                "DAGRON_CHECKOUT_PATCH_INSECURE",
                "— (a patch's link is https, or http:// to this machine)",
            ),
            K(
                "DAGRON_CHECKOUT_TOKEN_SECRET_FILE",
                "— (with DAGRON_CHECKOUT_TOKEN_URL)",
            ),
            K(
                "DAGRON_CHECKOUT_TOKEN_URL",
                "— (every checkout without a credential)",
            ),
            // The image that installs dagron-ci-exec into each workspace task
            // on Docker and Kubernetes (dagron-executor's `workspace` module).
            K("DAGRON_CI_EXEC_IMAGE", "— (no init container)"),
            K("DAGRON_CLOCK_CHECK_SECS", "30"),
            K("DAGRON_CLOCK_STEP_TOLERANCE_MS", "1000"),
            K("DAGRON_CLOCK_SYNC_FILE", "— (no positive sync evidence)"),
            K("DAGRON_CONSOLE", "— (console mounted)"),
            // Where a deferred task's job is polled, pinned onto its row when
            // it is parked (docs/EXTERNAL_JOBS.md). Read since `defer:`
            // landed and listed nowhere here: an engine that set it was told
            // at every start that it looked mistyped.
            K("DAGRON_DEFER_ENDPOINT", "— (none pinned)"),
            // Whether a trusted run's step may have its image built from a
            // Dockerfile on the Docker executor's daemon.
            K(
                "DAGRON_DOCKER_BUILD_ACTIONS",
                "— (no step's image is built)",
            ),
            // A Docker daemon beside a task that asks for one, and its image.
            K(
                "DAGRON_DOCKER_DAEMON",
                "— (a task that asks for one is refused)",
            ),
            K(
                "DAGRON_DOCKER_DAEMON_IMAGE",
                dagron_core::docker_daemon::DEFAULT_IMAGE,
            ),
            K(
                "DAGRON_DOCKER_DAEMON_RUNTIME",
                "— (the daemon is privileged, on the host's default runtime)",
            ),
            // What every Docker task container gets from the operator: mounts
            // and a working directory. The local `dagron-ci run` sets both to
            // put the working tree into every task.
            K(
                "DAGRON_DOCKER_LOG_DRIVER",
                "— (the daemon's own; `json-file` keeps what is read back of a container whole)",
            ),
            K("DAGRON_DOCKER_MOUNTS", "— (none)"),
            // The network every container the Docker executor makes is on.
            K("DAGRON_DOCKER_NETWORK", "— (the daemon's default)"),
            K("DAGRON_DOCKER_WORKDIR", "— (the image's)"),
            // The KEK/KMS envelope family (dagron-crypto, docs/CONFIG.md
            // "Encryption at rest"): the current provider's inputs plus their
            // `*_OLD` twins, which the key-rotation sweep reads for the
            // retiring KEK. Registered so the fingerprint covers a rotation
            // in flight and the typo scan stays quiet about legitimate
            // rotation setups. Key material is redacted; selectors, ids, and
            // command lines are what an operator diffs when wrapping fails.
            KR("DAGRON_ENV_KEK", "— (local provider only)"),
            KR("DAGRON_ENV_KEK_OLD", "— (rotation source off)"),
            K("DAGRON_ENV_KEK_PROVIDER", "— (v1 single-key path)"),
            K("DAGRON_ENV_KEK_PROVIDER_OLD", "— (rotation source off)"),
            K("DAGRON_ENV_KMS_ID", "provider default"),
            K("DAGRON_ENV_KMS_ID_OLD", "provider default"),
            K("DAGRON_ENV_KMS_KEY_ID", "—"),
            K("DAGRON_ENV_KMS_KEY_ID_OLD", "—"),
            K("DAGRON_ENV_KMS_KEY_VERSION", "—"),
            K("DAGRON_ENV_KMS_KEY_VERSION_OLD", "—"),
            K("DAGRON_ENV_KMS_TIMEOUT_SECS", "30"),
            K("DAGRON_ENV_KMS_UNWRAP_CMD", "— (command provider only)"),
            K("DAGRON_ENV_KMS_UNWRAP_CMD_OLD", "—"),
            KR("DAGRON_ENV_KMS_VAULT_URL", "—"),
            KR("DAGRON_ENV_KMS_VAULT_URL_OLD", "—"),
            K("DAGRON_ENV_KMS_WRAP_CMD", "— (command provider only)"),
            K("DAGRON_ENV_KMS_WRAP_CMD_OLD", "—"),
            // The key stored environment secrets are decrypted with at
            // dispatch (dagron-crypto's `KEY_ENV`, docs/CONFIG.md). The chart
            // sets it on every engine that has the store, and it was not
            // here: each such engine warned of a typo, and `dagron config`
            // did not show that it was set.
            KR("DAGRON_ENV_SECRET_KEY", "— (stored secrets not read)"),
            // The `source` of the CloudEvents a `notify.webhook` sends
            // (dagron_core::cloudevents).
            K("DAGRON_EVENTS_SOURCE", "/dagron"),
            // The image cache (dagron-executor's `images` module):
            // a trusted run's pod's pull secrets on Kubernetes, the engine's
            // registry credentials on Docker, and each registry's mirror.
            K("DAGRON_IMAGE_PULL_SECRETS", "— (a pod names none)"),
            // The fleet sweep's installation, its pace and its floor (the
            // DAGRON_ORPHAN_* pair, below). Documented in docs/CONFIG.md and
            // read since the sweep landed, yet listed nowhere here: an engine
            // that armed the sweep warned of a typo, and `dagron config` left
            // the three out.
            K("DAGRON_INSTALLATION", "— (fleet sweep off)"),
            // The two local-pool knobs (dagron-executor's `pool` module). A
            // pool is an engine process with RUNNER_CLASSES set, so these are
            // per-pool by construction. Both unset = a local pool behaves
            // exactly as it always has.
            K("DAGRON_LOCAL_COMMAND_ALLOWLIST", "— (any command)"),
            K("DAGRON_LOCAL_ENV_PASSTHROUGH", "— (inherit the pool's env)"),
            // Where every local task process starts, and each workspace task
            // works; `dagron-ci run --executor local` sets it to the tree.
            K("DAGRON_LOCAL_WORKDIR", "— (the engine's; a new directory per workspace task)"),
            // Where task logs are kept once out of the task rows, and the size
            // a segment closes at (dagron-artifact's log_segments).
            K("DAGRON_LOG_SEGMENT_BYTES", "8388608 (8 MiB)"),
            K("DAGRON_LOG_STORE", "— (each log in its task row)"),
            // How often the store's GC sweeps it (`log_sweep`).
            K("DAGRON_LOG_SWEEP_SECS", "30 (0 = removes nothing)"),
            // How much of the end of a log past its cap is kept, on top of
            // the cap.
            K("DAGRON_LOG_TAIL_BYTES", "a quarter of the cap (no more than it; 0 = none)"),
            K("DAGRON_LOG_TASK_CAP_BYTES", "67108864 (64 MiB, of each attempt's log and output)"),
            K("DAGRON_MAX_TASKS_PER_RUN", "100000 (compiled ceiling)"),
            // Registered late: the executor has read this since the task-clock
            // ceiling landed and `docs/CONFIG.md` documents it, but it was
            // never added here — so every deployment that set it (the cloud
            // provisioner stamps it on every workspace engine) drew a "looks
            // like a dagron knob but is not one" warning for a knob that is.
            K("DAGRON_MAX_TASK_TIMEOUT_SECS", "— (no ceiling)"),
            // How many runs may wait for one workflow's `max_active_runs`
            // (`when_full`), read where a run is made: dagron-core's.
            K("DAGRON_MAX_WAITING_RUNS", "100 (floor 1)"),
            K("DAGRON_MIN_FREE_BYTES", "0 (off)"),
            // Whether that URL may be plain http (a closed network's).
            K("DAGRON_OIDC_REQUEST_INSECURE", "— (https only)"),
            // Where a task that asks for an identity asks dagron-api for
            // tokens (`dagron_core::oidc::request_credential`).
            K(
                "DAGRON_OIDC_REQUEST_URL",
                "— (a task that asks for an identity is refused)",
            ),
            K("DAGRON_ORPHAN_MIN_AGE_SECS", "600 (floor 60)"),
            K("DAGRON_ORPHAN_SWEEP_SECS", "300 (floor 30)"),
            K("DAGRON_PRESSURE_FILE", "— (no pressure gate)"),
            // Signed provenance of each workflow artifact
            // (`actions_cache::provenance`): a key of the operator's, or a KMS
            // command and its public half (enterprise), and who the builder
            // says it is.
            K(
                "DAGRON_PROVENANCE_BUILDER_ID",
                "https://dagron.dev/builders/dagron-engine",
            ),
            K("DAGRON_PROVENANCE_KEY", "— (artifacts kept without provenance)"),
            K("DAGRON_PROVENANCE_PUBLIC_KEY", "— (with DAGRON_PROVENANCE_SIGN_CMD)"),
            K("DAGRON_PROVENANCE_SIGN_CMD", "— (enterprise: a KMS key signs)"),
            K("DAGRON_QUOTA_WAIT_MAX_SECS", "21600 (0 = quota refusals are failed attempts)"),
            K("DAGRON_READY_TIMEOUT_MS", "500 (floor 50)"),
            K("DAGRON_REDACT_ENV", "true"),
            K("DAGRON_REGISTRY_AUTH_FILE", "— (images pulled with no credential)"),
            K("DAGRON_REGISTRY_MIRRORS", "— (each image from its own registry)"),
            K("DAGRON_SECRETS_DIR", "—"),
            K("DAGRON_SENSITIVE_ENV_PATTERNS", "built-in patterns"),
            K("DAGRON_TASK_ACTIVE_DEADLINE_SECS", "—"),
            // The task ServiceAccounts a workflow may name. A hosted
            // deployment's operator sets it to the workspace's task identity,
            // `dagron-task`, and each such engine warned of a typo until it was
            // listed here.
            K("DAGRON_TASK_ALLOWED_SERVICE_ACCOUNTS", "— (a task that names one is refused)"),
            K("DAGRON_TASK_AUTOMOUNT_SA_TOKEN", "— (off)"),
            K("DAGRON_TASK_DROP_ALL_CAPABILITIES", "— (off)"),
            // A claim workspace's group; `none` leaves it to the platform, as
            // OpenShift's restricted SCCs want.
            K("DAGRON_TASK_FS_GROUP", "65532 (none: the platform's)"),
            K("DAGRON_TASK_ISOLATION_FLOOR", "— (no floor)"),
            // The allowlists the operator has made, by name: what a task
            // that names one (isolation.network: allow:<name>) needs. A
            // NetworkPolicy of theirs on Kubernetes, an internal network of
            // theirs on Docker.
            K(
                "DAGRON_TASK_NETWORK_ALLOWLISTS",
                "— (a task that names an allowlist is refused)",
            ),
            // The operator's word that the cluster's CNI enforces
            // NetworkPolicy: what a task that asks for no network needs.
            K(
                "DAGRON_TASK_NETWORK_POLICY",
                "— (a task that asks for no network is refused on Kubernetes)",
            ),
            K("DAGRON_TASK_NODE_SELECTOR", "—"),
            K("DAGRON_TASK_POD_WATCH", "on"),
            // A task pod's PriorityClass and tolerations: a hosted deployment's
            // operator sets the class on every engine of a tiered workspace,
            // and each such engine warned of a typo until it was listed here.
            K("DAGRON_TASK_PRIORITY_CLASS", "— (none)"),
            // A task that asks to be privileged, on a dedicated pool.
            K("DAGRON_TASK_PRIVILEGED", "— (a task that asks is refused)"),
            K("DAGRON_TASK_READ_ONLY_ROOT_FS", "— (off)"),
            K("DAGRON_TASK_RUNTIME_CLASS", "—"),
            // Non-root without a uid of the engine's, which a platform that
            // gives one (OpenShift's SCCs) wants.
            K("DAGRON_TASK_RUN_AS_NON_ROOT", "— (off)"),
            K("DAGRON_TASK_RUN_AS_USER", "—"),
            K("DAGRON_TASK_SECCOMP_RUNTIME_DEFAULT", "— (off)"),
            K("DAGRON_TASK_SECRET_ENV", "secret"),
            // Labels a task pod that runs as a service account of its own is
            // given, for a workload identity webhook (AKS's).
            K("DAGRON_TASK_SERVICE_ACCOUNT_POD_LABELS", "— (none)"),
            K("DAGRON_TASK_TOLERATIONS", "— (none)"),
            // The floor for the tasks of a run that is not trusted, and the
            // limits of a container made for one.
            K(
                "DAGRON_UNTRUSTED_IMAGES",
                "— (a task of a run that is not trusted may name any image)",
            ),
            K(
                "DAGRON_UNTRUSTED_ISOLATION_FLOOR",
                "— (a run that is not trusted has no task run here)",
            ),
            K(
                "DAGRON_UNTRUSTED_LIMITS",
                "memory=4g,pids=2048,cpus=2,timeout=3600,attempts=3,tasks=256,services=4",
            ),
            // Kubernetes workspaces from a size up are volumes of their own.
            K("DAGRON_WORKSPACE_PVC_FROM", "— (always an emptyDir)"),
            K("DAGRON_WORKSPACE_STORAGE_CLASS", "— (the cluster's default)"),
            KR("DATABASE_LISTEN_URL", "— (share the pool DSN)"),
            KR("DATABASE_URL", "postgres://localhost/workflow (pg builds)"),
            K("DATASET_TRIGGERS", "true"),
            K("DB_MAX_CONNECTIONS", "8"),
            K("DB_SCHEDULES", "— (off)"),
            K("DEAD_LETTER_MAX_ATTEMPTS", "3"),
            K("DIR_POLL_MS", "2000 (floor 100)"),
            K("DOCKER_IMAGE", "alpine:latest"),
            K("EXECUTOR", "local"),
            K("GC_ARCHIVE_COMPACT_MIN_AGE_DAYS", "30"),
            K("GC_ARCHIVE_DIR", "— (plain purge)"),
            K("GC_ARCHIVE_URL", "— (plain purge)"),
            K("GC_INTERVAL_SECS", "3600"),
            K("GC_RETENTION_SECS", "— (GC off)"),
            // The API bases are endpoints, not credentials — printable, and an
            // operator diffing /config against the reviewed deployment needs
            // to see a GHE/self-managed override. Only the tokens are secret.
            K("GITHUB_API_BASE", "https://api.github.com"),
            KR("GITHUB_TOKEN", "— (forge feedback off)"),
            K("GITLAB_API_BASE", "https://gitlab.com/api/v4"),
            KR("GITLAB_TOKEN", "— (forge feedback off)"),
            K("K8S_IMAGE", "falls back to DOCKER_IMAGE"),
            K("K8S_NAMESPACE", "default"),
            K("LEADER_LEASE_SECS", "30"),
            K("LEASE_SECS", "30 (floor 3)"),
            K("LOG_FORMAT", "full"),
            K("LOG_LEVEL", "info"),
            K("LOG_SPAN_EVENTS", "— (off)"),
            K("MAX_INFLIGHT_RUNS", "64 (0 disables)"),
            K("MAX_INFLIGHT_TASKS", "0 (off)"),
            K("METRICS_CACHE_SECS", "10 (0 disables)"),
            K(
                "METRICS_HISTORY_CACHE_SECS",
                "300, 0 with METRICS_CACHE_SECS=0 (0 disables)",
            ),
            K("MQTT_CLEAN_SESSION", "false with MQTT_CLIENT_ID, else true"),
            K("MQTT_CLIENT_ID", "dagron-<uuid>"),
            K("MQTT_DLQ_TOPIC", "— (datastore dead_letters only)"),
            K("MQTT_KEEPALIVE_SECS", "30"),
            KR("MQTT_PASSWORD", "—"),
            K("MQTT_POSITION_FIELD", "— (at-least-once)"),
            K("MQTT_QOS", "1"),
            K("MQTT_TOPIC", "dagron/workflows"),
            // An MQTT origin can carry userinfo (mqtt://user:pass@broker), so it
            // is redacted like every other credential-bearing URL in this registry.
            KR("MQTT_URL", "mqtt://127.0.0.1:1883"),
            K("MQTT_USERNAME", "—"),
            K("OPENLINEAGE_NAMESPACE", "dagron"),
            KR("OPENLINEAGE_URL", "— (lineage off)"),
            KR("OTEL_EXPORTER_OTLP_ENDPOINT", "— (otel export off)"),
            K("POLL_INTERVAL_MS", "500 (floor 10)"),
            K("POOLS", "— (no pools)"),
            K("READY_AGE_ALERT_SECS", "300 (0 disables)"),
            K("READY_AGE_CHECK_INTERVAL_SECS", "60"),
            K(
                "REAP_INTERVAL_MS",
                "5000 (0: every tick looks at every run)",
            ),
            K("RUNNER_CLASSES", "— (claim every class)"),
            K("RUNNER_GANGS", "— (off)"),
            K("RUST_LOG", "— (LOG_LEVEL applies)"),
            K("SOURCE", "file"),
            K("STREAM_DLQ_PATH", "<STREAM_PATH>.dlq"),
            K("STREAM_FOLLOW", "true"),
            K("STREAM_MAX_PARTITIONS", "unlimited"),
            K("STREAM_MODE", "auto"),
            K("STREAM_OFFSET_PATH", "<STREAM_PATH>.offset"),
            K("STREAM_PATH", "— (required for SOURCE=stream)"),
            K("STREAM_POLL_MS", "500"),
            K("STREAM_SUFFIX", ".ndjson"),
            K("SUBWORKFLOW_MAX_DEPTH", "8"),
            K(
                "SWEEP_INTERVALS",
                "— (each sweep at SWEEP_INTERVAL_MS, or its own default)",
            ),
            K("SWEEP_INTERVAL_MS", "= POLL_INTERVAL_MS (floor 10)"),
            K("SWEEP_SHARE", "on (one engine a window)"),
            K("TASK_LEASE_HEARTBEAT", "true"),
            K("WAIT_POLL_SECS", "15"),
            K("WAIT_URL_DENY_PRIVATE", "— (off)"),
            K("WORKER_COUNT", "16 (min 1)"),
            K("WORKFLOW_DIR", "/workflows"),
        ]
    })
}

/// Vars the typo scan must stay silent about: families owned by *other*
/// dagron components (the API edge, the MCP adapter) that legitimately appear
/// in a shared environment — a local `dagron dev` shell exporting the API's
/// `DAGRON_JWT_SECRET`, say — plus the engine's one *unenumerable* family,
/// `DAGRON_SECRET_<NAME>`, whose suffix is author-chosen per secret and so can
/// never be a registry entry.
const FOREIGN_PREFIXES: &[&str] = &[
    "DAGRON_SECRET_",
    "DAGRON_JWT_",
    "DAGRON_COOKIE_",
    "DAGRON_ADMIN_",
    "DAGRON_SESSION_",
    "DAGRON_LOGIN_",
    "DAGRON_TRUST_",
    "DAGRON_PW_",
    "DAGRON_AUDIT_",
    "DAGRON_GIT_",
    "DAGRON_DB_",
    "DAGRON_SHUTDOWN_",
    "DAGRON_ARTIFACT_MAX_",
    // dagron-api's cache of `GET /api/metrics`.
    "DAGRON_METRICS_",
    "DAGRON_KEK",
    "DAGRON_MCP_",
    "DAGRON_API_",
    // The fleet-link worker (an optional sidecar loop, not the engine) reads its
    // own family; registering it here keeps a unit's shell quiet.
    "DAGRON_FLEET_",
    // The build task binary's deployment configuration. The engine never reads
    // any of it — it is the environment a `build`-class task's subprocess is
    // handed — so none of it can be a registered knob, and without this entry a
    // build pool logged one "looks like a dagron knob but is not one" warning
    // per variable at every boot, for a dozen variables its own chart sets.
    // Over-warning is what trains an operator to ignore the scan.
    "DAGRON_BUILD_",
    // dagron-ci's commands that speak to a server (`run --remote`, `logs`,
    // `rerun`, `cancel`) read where it is, and the caller's token, from
    // these. They are a person's shell's, and an engine started from that
    // shell reads none of them. Named whole, and not as `DAGRON_CI_`: the
    // engine's own DAGRON_CI_EXEC_IMAGE starts so, and stays in the scan.
    "DAGRON_CI_URL",
    "DAGRON_CI_TOKEN",
];

/// Prefix families the typo scan claims: an env var starting with one of these
/// that is neither a registered knob nor a known foreign var draws a warning.
/// Deliberately narrow — over-warning trains operators to ignore the signal.
const OUR_PREFIXES: &[&str] = &[
    "DAGRON_",
    "STREAM_",
    "MQTT_",
    "GC_ARCHIVE_",
    "READY_AGE_",
    "MAX_INFLIGHT_",
    "DEAD_LETTER_",
    "SUBWORKFLOW_",
    "OPENLINEAGE_",
    "POLL_INTERVAL",
    // `SWEEP_INTERVAL_MS`, `SWEEP_INTERVALS` and `SWEEP_SHARE`.
    "SWEEP_",
    "LEASE_SECS",
];

/// Where an effective value came from, in precedence order.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Source {
    Env,
    File,
    Profile,
    Default,
}

impl Source {
    pub fn as_str(self) -> &'static str {
        match self {
            Source::Env => "env",
            Source::File => "file",
            Source::Profile => "profile",
            Source::Default => "default",
        }
    }
}

/// What the file layer resolved: which file and profile (if any), and which
/// keys the layer itself set (everything else set in the environment was
/// there before us, i.e. `Source::Env`).
#[derive(Default)]
pub struct FileLayer {
    pub path: Option<String>,
    pub profile: Option<String>,
    set_by: BTreeMap<String, Source>,
}

static LAYER: OnceLock<FileLayer> = OnceLock::new();

/// The named profiles: preset knob values applied under the file's own keys.
/// `throughput` is the stock tuning, so its preset is empty on purpose — it
/// exists so a fleet can *declare* the intent in a reviewed file.
fn profile_preset(name: &str) -> Option<&'static [(&'static str, &'static str)]> {
    match name {
        // docs/LOW_LATENCY.md §6 — the trading-desk profile's engine knobs.
        "low-latency" => Some(&[
            ("POLL_INTERVAL_MS", "25"),
            ("SWEEP_INTERVAL_MS", "500"),
            ("LEASE_SECS", "5"),
        ]),
        "throughput" => Some(&[]),
        // docs/CONFIG.md — constrained gateways, robots, vehicles: few workers,
        // slow ticks, small in-flight caps, and a free-disk floor so a full
        // flash device refuses new runs instead of corrupting the datastore.
        "edge" => Some(&[
            ("WORKER_COUNT", "2"),
            ("POLL_INTERVAL_MS", "1000"),
            ("SWEEP_INTERVAL_MS", "5000"),
            ("MAX_INFLIGHT_RUNS", "4"),
            ("MAX_INFLIGHT_TASKS", "64"),
            ("DAGRON_MIN_FREE_BYTES", "67108864"),
        ]),
        _ => None,
    }
}

/// Apply `DAGRON_CONFIG` (and its `profile:`) UNDER the environment: for every
/// key the file or profile provides, set the env var **only if it is not
/// already set**, so explicit environment always wins. Must run before
/// anything reads configuration (including logging init — the file may set
/// `LOG_LEVEL`). Call exactly once; the resolved layer is retained for
/// introspection. Errors are startup-fatal: a reviewed config file that does
/// not parse, names an unknown profile, or carries an unknown key must never
/// half-apply silently.
pub fn apply_file_layer() -> Result<&'static FileLayer> {
    let mut layer = FileLayer::default();
    if let Ok(path) = std::env::var("DAGRON_CONFIG") {
        let path = path.trim().to_string();
        if !path.is_empty() {
            let text = std::fs::read_to_string(&path)
                .with_context(|| format!("reading DAGRON_CONFIG file '{path}'"))?;
            let doc: serde_yaml::Value = serde_yaml::from_str(&text)
                .with_context(|| format!("parsing DAGRON_CONFIG file '{path}' as YAML"))?;
            let map = doc
                .as_mapping()
                .with_context(|| format!("DAGRON_CONFIG file '{path}' must be a YAML mapping"))?;

            // Resolve the profile first so file keys override its preset.
            let mut pending: BTreeMap<String, (String, Source)> = BTreeMap::new();
            if let Some(profile) = map.get("profile") {
                // A non-string profile is a review mistake, not a missing key —
                // silently ignoring `profile: 1` would half-apply the file.
                let Some(profile) = profile.as_str() else {
                    bail!(
                        "DAGRON_CONFIG file '{path}': 'profile' must be a string \
                         (known: low-latency, throughput, edge), got {profile:?}"
                    );
                };
                let preset = profile_preset(profile).with_context(|| {
                    format!(
                        "DAGRON_CONFIG names unknown profile '{profile}' \
                         (known: low-latency, throughput, edge)"
                    )
                })?;
                for (k, v) in preset {
                    pending.insert((*k).to_string(), ((*v).to_string(), Source::Profile));
                }
                layer.profile = Some(profile.to_string());
            }

            let known = |k: &str| knobs().iter().any(|kn| kn.name == k);
            for (key, value) in map {
                let Some(key) = key.as_str() else {
                    bail!("DAGRON_CONFIG file '{path}': non-string key {key:?}");
                };
                if key == "profile" {
                    continue;
                }
                if !known(key) {
                    bail!(
                        "DAGRON_CONFIG file '{path}': unknown key '{key}' — keys are the \
                         engine's env var names (see docs/CONFIG.md)"
                    );
                }
                let rendered = match value {
                    serde_yaml::Value::String(s) => s.clone(),
                    serde_yaml::Value::Number(n) => n.to_string(),
                    serde_yaml::Value::Bool(b) => b.to_string(),
                    other => bail!(
                        "DAGRON_CONFIG file '{path}': key '{key}' must be a scalar, got {other:?}"
                    ),
                };
                pending.insert(key.to_string(), (rendered, Source::File));
            }

            for (key, (value, source)) in pending {
                if std::env::var_os(&key).is_none() {
                    // Single-threaded startup (before the runtime spawns
                    // workers that read env) — same contract as the existing
                    // `dagron dev` set_var.
                    std::env::set_var(&key, &value);
                    layer.set_by.insert(key, source);
                }
            }
            layer.path = Some(path);
        }
    }
    Ok(LAYER.get_or_init(|| layer))
}

fn layer() -> &'static FileLayer {
    LAYER.get_or_init(FileLayer::default)
}

/// The resolved file layer's provenance — (config file path, profile name) —
/// for introspection surfaces outside this module (the ops API's `GET
/// /config` mirrors `dagron config`'s header lines with it).
pub fn layer_info() -> (Option<String>, Option<String>) {
    let l = layer();
    (l.path.clone(), l.profile.clone())
}

/// Every knob's effective value: `(name, display value, source)`. Secrets show
/// `<redacted>`; unset knobs show their documented default. The *display*
/// value is for humans — the fingerprint below hashes raw values instead.
pub fn effective() -> Vec<(&'static str, String, Source)> {
    let layer = layer();
    knobs()
        .iter()
        .map(|k| match std::env::var(k.name) {
            Ok(v) => {
                let source = layer.set_by.get(k.name).copied().unwrap_or(Source::Env);
                let shown = if k.redact { "<redacted>".to_string() } else { v };
                (k.name, shown, source)
            }
            Err(_) => (k.name, k.default.to_string(), Source::Default),
        })
        .collect()
}

/// Stable fingerprint of the raw effective configuration (FNV-1a 64 over
/// `name=value` pairs in registry order; unset knobs hash as their name
/// alone). Two replicas with the same fingerprint run the same knob values —
/// the fleet-drift check. Not a security hash; drift detection only.
pub fn fingerprint() -> String {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    let mut eat = |bytes: &[u8]| {
        for &b in bytes {
            h ^= b as u64;
            h = h.wrapping_mul(0x0000_0100_0000_01b3);
        }
    };
    for k in knobs() {
        eat(k.name.as_bytes());
        if let Ok(v) = std::env::var(k.name) {
            eat(b"=");
            eat(v.as_bytes());
        }
        eat(b"\n");
    }
    format!("{h:016x}")
}

/// Every cargo feature the manifest declares that names a part of the engine,
/// in the manifest's order, and whether this build has it. `default` is a set
/// of the others and `archive-cloud` is plumbing the three object stores
/// share: neither is a part.
const FEATURES: &[(&str, bool)] = &[
    ("sqlite", cfg!(feature = "sqlite")),
    ("postgres", cfg!(feature = "postgres")),
    ("ops", cfg!(feature = "ops")),
    ("kubernetes", cfg!(feature = "kubernetes")),
    ("mqtt", cfg!(feature = "mqtt")),
    ("otel", cfg!(feature = "otel")),
    ("enterprise", cfg!(feature = "enterprise")),
    ("archive-s3", cfg!(feature = "archive-s3")),
    ("archive-gcs", cfg!(feature = "archive-gcs")),
    ("archive-azure", cfg!(feature = "archive-azure")),
    ("archive-parquet", cfg!(feature = "archive-parquet")),
];

/// The optional parts this engine was built with, as the cargo features that
/// compile them in.
///
/// Logged once at startup. A setting that needs a part the build lacks is
/// refused when it is read (`EXECUTOR=kubernetes`, `GC_ARCHIVE_URL=s3://…`):
/// this is how an operator knows what an image can do before asking it to.
pub fn built_with() -> Vec<&'static str> {
    FEATURES
        .iter()
        .filter_map(|(name, built)| built.then_some(*name))
        .collect()
}

/// Warn once per env var that matches one of our prefix families but is
/// neither a registered knob nor a known other-component var — the
/// `POLL_INTERVAL_M`-style silent typo. `DAGRON_CONFIG_NO_WARN=1` silences the
/// scan for environments that intentionally carry look-alike vars.
///
/// `also_known` is what the build this engine runs in reads for itself
/// ([`crate::Seams::env_names`]).
pub fn warn_unknown_vars(also_known: &[&str]) {
    if std::env::var_os("DAGRON_CONFIG_NO_WARN").is_some() {
        return;
    }
    // `vars_os`, not `vars`: the scan must never panic a boot over some
    // unrelated non-UTF-8 variable in the environment. A non-UTF-8 *name*
    // cannot be one of ours (every knob name is ASCII), so it is skipped.
    let names: Vec<String> = std::env::vars_os()
        .filter_map(|(name, _)| name.into_string().ok())
        .collect();
    for name in unknown_vars(&names, also_known) {
        tracing::warn!(
            var = %name,
            "environment variable looks like a dagron knob but is not one — typo? \
             (see docs/CONFIG.md; DAGRON_CONFIG_NO_WARN=1 silences this check)"
        );
    }
}

/// The names in `names` the typo scan warns about: in one of our prefix
/// families, and neither a registered knob, a known other-component var, one
/// of Kubernetes' service links, nor one of `also_known`: a name as it is
/// set, or a family's prefix, which ends in `_`.
fn unknown_vars<'a>(names: &'a [String], also_known: &[&str]) -> Vec<&'a str> {
    let services: Vec<&str> = names
        .iter()
        .filter_map(|name| name.strip_suffix("_SERVICE_HOST"))
        .collect();
    let known = |name: &str| {
        name == "DAGRON_CONFIG"
            || name == "DAGRON_CONFIG_NO_WARN"
            || knobs().iter().any(|k| k.name == name)
            || FOREIGN_PREFIXES.iter().any(|p| name.starts_with(p))
            || services.iter().any(|s| is_service_link(name, s))
            || also_known.iter().any(|known| match known.ends_with('_') {
                true => name.starts_with(known),
                false => name == *known,
            })
    };
    names
        .iter()
        .map(String::as_str)
        .filter(|name| OUR_PREFIXES.iter().any(|p| name.starts_with(p)) && !known(name))
        .collect()
}

/// Is `name` one of the variables the kubelet sets in every pod for a Service
/// in its namespace? `service` is the Service's name upper-cased, dashes as
/// underscores, and the variables are `<service>_SERVICE_HOST`,
/// `_SERVICE_PORT`, `_SERVICE_PORT_<port name>`, `_PORT`, and
/// `_PORT_<number>_<protocol>` with its `_PROTO`, `_PORT` and `_ADDR`. The
/// chart's own `dagron-engine` Service gives its engine eight
/// `DAGRON_ENGINE_*`. The caller takes `service` only from a
/// `<service>_SERVICE_HOST` in the same environment, which nothing but the
/// kubelet sets, so a typo keeps its warning.
fn is_service_link(name: &str, service: &str) -> bool {
    let Some(rest) = name.strip_prefix(service) else {
        return false;
    };
    if matches!(rest, "_SERVICE_HOST" | "_SERVICE_PORT" | "_PORT")
        || rest.starts_with("_SERVICE_PORT_")
    {
        return true;
    }
    let Some(port) = rest.strip_prefix("_PORT_") else {
        return false;
    };
    let mut parts = port.split('_');
    let number = parts
        .next()
        .is_some_and(|n| !n.is_empty() && n.bytes().all(|b| b.is_ascii_digit()));
    let protocol = parts
        .next()
        .is_some_and(|p| matches!(p, "TCP" | "UDP" | "SCTP"));
    let field = parts
        .next()
        .is_none_or(|f| matches!(f, "PROTO" | "PORT" | "ADDR"));
    number && protocol && field && parts.next().is_none()
}

/// `dagron config [--json]` — print every knob's effective value and source.
/// The file layer has already been applied by the caller, so what prints here
/// is exactly what a daemon started in this environment would run with.
pub fn run_cli(args: &[String]) -> Result<()> {
    let json = args.iter().any(|a| a == "--json");
    let layer = layer();
    let rows = effective();
    if json {
        let knobs: Vec<serde_json::Value> = rows
            .iter()
            .map(|(name, value, source)| {
                serde_json::json!({ "name": name, "value": value, "source": source.as_str() })
            })
            .collect();
        let doc = serde_json::json!({
            "fingerprint": fingerprint(),
            "config_file": layer.path,
            "profile": layer.profile,
            "knobs": knobs,
        });
        println!("{}", serde_json::to_string_pretty(&doc)?);
        return Ok(());
    }
    println!("# dagron effective configuration");
    println!("# fingerprint: {}", fingerprint());
    if let Some(p) = &layer.path {
        println!("# config file: {p}");
    }
    if let Some(p) = &layer.profile {
        println!("# profile: {p}");
    }
    let width = knobs().iter().map(|k| k.name.len()).max().unwrap_or(0);
    for (name, value, source) in rows {
        println!("{name:width$}  [{:7}]  {value}", source.as_str());
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Env-var mutation is process-global; serialize the tests that do it.
    static ENV_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// A knob the engine reads but the registry does not list warns at startup
    /// as a typo — for a knob that is real. That happened: the task-clock
    /// ceiling was read by the executor and documented in `docs/CONFIG.md` for
    /// releases while the cloud provisioner stamped it onto every workspace
    /// engine, and each of those engines logged it as a probable typo.
    ///
    /// Pinning the names against the constants the other crate exports is what
    /// keeps that from happening again silently: a knob added over there and
    /// forgotten here turns this red rather than turning up in a support log.
    #[test]
    fn every_knob_another_crate_reads_is_registered() {
        let registered = |name: &str| knobs().iter().any(|k| k.name == name);
        for name in [
            dagron_executor::pool::ENV_COMMAND_ALLOWLIST,
            dagron_executor::pool::ENV_PASSTHROUGH,
            "DAGRON_MAX_TASK_TIMEOUT_SECS",
            "DAGRON_SENSITIVE_ENV_PATTERNS",
            "DAGRON_REDACT_ENV",
            // The Kubernetes executor's task ServiceAccount allow-list, and how
            // it hands a task its secret env.
            "DAGRON_TASK_ALLOWED_SERVICE_ACCOUNTS",
            "DAGRON_TASK_SECRET_ENV",
            // Its one watch of the task pods.
            "DAGRON_TASK_POD_WATCH",
            // Its task placement: the class a tier gives every task, and the
            // tolerations a pool's taint asks for.
            "DAGRON_TASK_PRIORITY_CLASS",
            "DAGRON_TASK_TOLERATIONS",
            // The isolation floor a sandbox mode gives every task, and the
            // operator's word that the cluster enforces NetworkPolicy.
            "DAGRON_TASK_ISOLATION_FLOOR",
            "DAGRON_TASK_NETWORK_POLICY",
            dagron_core::isolation::ENV_NETWORK_ALLOWLISTS,
            // The floor and the limits of a run that is not trusted.
            dagron_core::confine::ENV_FLOOR,
            dagron_core::confine::ENV_IMAGES,
            dagron_core::confine::ENV_LIMITS,
            // A privileged task, on a dedicated pool.
            dagron_core::privileged::ENV,
            // A task's Docker daemon.
            dagron_core::docker_daemon::ENV,
            dagron_core::docker_daemon::ENV_IMAGE,
            dagron_core::docker_daemon::ENV_RUNTIME,
            // The Docker executor's task mounts and working directory.
            dagron_executor::docker_executor::ENV_MOUNTS,
            dagron_executor::docker_executor::ENV_WORKDIR,
            // And the log driver each of its containers is made with, and the
            // network each is made on.
            dagron_executor::docker_executor::ENV_LOG_DRIVER,
            dagron_executor::docker_executor::ENV_NETWORK,
            // And whether a trusted run's step may have its image built.
            dagron_executor::docker_executor::ENV_BUILD_ACTIONS,
            // Where images are pulled from, and with what.
            dagron_executor::images::ENV_AUTH_FILE,
            dagron_executor::images::ENV_MIRRORS,
            dagron_executor::images::ENV_PULL_SECRETS,
            // Where task logs are kept.
            dagron_artifact::log_segments::ENV_STORE,
            dagron_artifact::log_segments::ENV_SEGMENT_BYTES,
            // A task's workspace: the image that installs dagron-ci-exec, the
            // local directory, and when a Kubernetes one is its own volume.
            dagron_executor::workspace::ENV_CI_EXEC_IMAGE,
            dagron_executor::workspace::ENV_LOCAL_WORKDIR,
            dagron_executor::workspace::ENV_PVC_FROM,
            dagron_executor::workspace::ENV_STORAGE_CLASS,
            // Which installation the fleet sweep works for.
            dagron_executor::executor::DAGRON_INSTALLATION,
            // How much of an attempt's log, and of its output, is kept; and
            // of the end of a log past its cap.
            dagron_executor::executor::ENV_LOG_TASK_CAP,
            dagron_executor::executor::ENV_LOG_TAIL,
            "DAGRON_ORPHAN_SWEEP_SECS",
            "DAGRON_ORPHAN_MIN_AGE_SECS",
            // How often the log store's GC sweeps it.
            crate::log_sweep::ENV_SWEEP_SECS,
        ] {
            assert!(
                registered(name),
                "{name} is read by the engine but missing from the knob registry, so \
                 setting it draws a typo warning"
            );
        }
    }

    /// A feature added to the manifest and not to [`FEATURES`] would be a part
    /// an image has and its startup line does not say.
    #[test]
    fn every_part_the_manifest_declares_can_be_said() {
        let manifest = include_str!("../Cargo.toml");
        let table = manifest
            .split("\n[features]")
            .nth(1)
            .and_then(|rest| rest.split("\n[").next())
            .expect("the manifest's [features] table");
        // A feature is a line that starts at the margin with its name: not a
        // comment, and not a line of a list that runs over several.
        let declared: Vec<&str> = table
            .lines()
            .filter(|line| line.starts_with(|c: char| c.is_ascii_lowercase()))
            .filter_map(|line| line.split_once(" = ").map(|(name, _)| name))
            .filter(|name| !matches!(*name, "default" | "archive-cloud"))
            .collect();
        let said: Vec<&str> = FEATURES.iter().map(|(name, _)| *name).collect();
        assert_eq!(said, declared);
    }

    /// A build has one datastore, and says which.
    #[test]
    fn a_build_says_its_one_datastore() {
        let stores: Vec<&str> = built_with()
            .into_iter()
            .filter(|part| matches!(*part, "sqlite" | "postgres"))
            .collect();
        assert_eq!(stores.len(), 1, "{stores:?}");
    }

    /// The registry is kept sorted by name so a reader can find a knob and a
    /// diff adding one is one line.
    #[test]
    fn the_registry_is_sorted() {
        let names: Vec<&str> = knobs().iter().map(|k| k.name).collect();
        let mut sorted = names.clone();
        sorted.sort_unstable();
        assert_eq!(names, sorted, "keep knobs() sorted by name");
    }

    /// The fingerprint is stable for a fixed environment and moves when any
    /// registered knob moves.
    #[test]
    fn fingerprint_tracks_knob_changes() {
        let _guard = ENV_LOCK.lock().unwrap();
        let a = fingerprint();
        let b = fingerprint();
        assert_eq!(a, b, "stable for a fixed environment");
        std::env::set_var("WAIT_POLL_SECS", "17");
        let c = fingerprint();
        std::env::remove_var("WAIT_POLL_SECS");
        assert_ne!(a, c, "a changed knob changes the fingerprint");
        assert_eq!(a, fingerprint(), "restoring the env restores it");
    }

    /// Redacted knobs never display their value; unset knobs display defaults.
    #[test]
    fn effective_redacts_and_defaults() {
        let _guard = ENV_LOCK.lock().unwrap();
        std::env::set_var("GITHUB_TOKEN", "ghp_super_secret");
        let rows = effective();
        let (_, shown, source) =
            rows.iter().find(|(n, _, _)| *n == "GITHUB_TOKEN").unwrap();
        assert_eq!(shown, "<redacted>");
        assert!(matches!(source, Source::Env));
        std::env::remove_var("GITHUB_TOKEN");
        let rows = effective();
        let (_, shown, source) =
            rows.iter().find(|(n, _, _)| *n == "EXECUTOR").unwrap();
        if std::env::var_os("EXECUTOR").is_none() {
            assert_eq!(shown, "local");
            assert!(matches!(source, Source::Default));
        }
    }

    /// Profile presets resolve; unknown profiles are an error the file layer
    /// surfaces (exercised through profile_preset directly — the layer itself
    /// is a OnceLock and can only be applied once per process).
    #[test]
    fn profiles_are_known() {
        assert!(profile_preset("low-latency").is_some());
        assert!(profile_preset("throughput").is_some());
        assert!(profile_preset("hyperspeed").is_none());
        let ll = profile_preset("low-latency").unwrap();
        assert!(ll.iter().any(|(k, v)| *k == "POLL_INTERVAL_MS" && *v == "25"));
    }
}

#[cfg(test)]
mod build_pool_warning_tests {
    use super::*;

    /// The build pool's own knobs must not read as typos.
    ///
    /// `OUR_PREFIXES` claims the bare `DAGRON_` prefix, and the image builder
    /// reads its deployment configuration from `DAGRON_BUILD_*` — variables the
    /// engine binary never reads and so can never register. Before
    /// `DAGRON_BUILD_` joined `FOREIGN_PREFIXES`, every build pool logged one
    /// "looks like a dagron knob but is not one" warning per variable at every
    /// boot, for thirteen variables its own chart sets. That is exactly the
    /// over-warning the comment on `OUR_PREFIXES` says trains operators to
    /// ignore the signal.
    #[test]
    fn the_build_pools_knobs_are_not_reported_as_typos() {
        let known = |name: &str| {
            knobs().iter().any(|k| k.name == name)
                || FOREIGN_PREFIXES.iter().any(|p| name.starts_with(p))
        };
        // Every DAGRON_BUILD_* a build pool's chart renders onto it.
        for name in [
            "DAGRON_BUILD_NAMESPACE",
            "DAGRON_BUILD_SANDBOX",
            "DAGRON_BUILD_CONTEXT_DELIVERY",
            "DAGRON_BUILD_CONTEXT_MAX_BYTES",
            "DAGRON_BUILD_BUILDKIT_IMAGE",
            "DAGRON_BUILD_BUILDKIT_USERNS_IMAGE",
            "DAGRON_BUILD_FETCH_IMAGE",
            "DAGRON_BUILD_PUSH",
            "DAGRON_BUILD_TIMEOUT_SECS",
            "DAGRON_BUILD_REGISTRY_INSECURE",
            "DAGRON_BUILD_PUSH_SECRET",
            "DAGRON_BUILD_SERVICE_ACCOUNT",
            "DAGRON_BUILD_NODE_SELECTOR",
            "DAGRON_BUILD_RESOURCES",
            "DAGRON_BUILD_RECIPE",
            "DAGRON_BUILD_BACKEND",
            "DAGRON_BUILD_ATTEST_KEY",
            "DAGRON_BUILD_ATTEST_PUBKEYS",
        ] {
            assert!(known(name), "{name} would be warned about as a typo at every boot");
        }

        // The prefix must not swallow a real engine knob that happens to start
        // the same way — a foreign prefix is a hole in the typo scan, so it has
        // to be exactly as wide as the family it covers.
        assert!(
            !FOREIGN_PREFIXES.iter().any(|p| "DAGRON_BUNDLE_PUBKEYS".starts_with(p)),
            "DAGRON_BUNDLE_PUBKEYS is the engine's own and must stay in the scan"
        );
    }

    /// dagron-ci's command line reads its server and its token from a
    /// person's shell. An engine started there must not take them for
    /// typos, and its own setting that starts the same way stays one the
    /// scan knows.
    #[test]
    fn the_ci_commands_variables_are_not_reported_as_typos() {
        let foreign = |name: &str| FOREIGN_PREFIXES.iter().any(|p| name.starts_with(p));
        for name in ["DAGRON_CI_URL", "DAGRON_CI_TOKEN", "DAGRON_CI_TOKEN_FILE"] {
            assert!(foreign(name), "{name} would be warned about as a typo");
        }
        for own in [
            "DAGRON_CI_EXEC_IMAGE",
            "DAGRON_CI_EXEC_IMAGES",
            "DAGRON_CI_TOKENS",
        ] {
            assert_eq!(foreign(own), own == "DAGRON_CI_TOKENS", "{own}");
        }
        assert!(knobs().iter().any(|k| k.name == "DAGRON_CI_EXEC_IMAGE"));
    }
}

#[cfg(test)]
mod service_link_warning_tests {
    use super::*;

    fn env(names: &[&str]) -> Vec<String> {
        names.iter().map(|n| n.to_string()).collect()
    }

    /// The kubelet gives every pod its namespace's Services as variables, and
    /// the chart's own are named `dagron-*`: its engine read eight
    /// `DAGRON_ENGINE_*`, and the chart's Postgres seven `DAGRON_POSTGRES_*`, as
    /// typos at every boot. A real typo beside them still warns.
    #[test]
    fn a_services_links_are_not_reported_as_typos() {
        let names = env(&[
            "DAGRON_ENGINE_SERVICE_HOST",
            "DAGRON_ENGINE_SERVICE_PORT",
            "DAGRON_ENGINE_SERVICE_PORT_HTTP",
            "DAGRON_ENGINE_PORT",
            "DAGRON_ENGINE_PORT_8080_TCP",
            "DAGRON_ENGINE_PORT_8080_TCP_PROTO",
            "DAGRON_ENGINE_PORT_8080_TCP_PORT",
            "DAGRON_ENGINE_PORT_8080_TCP_ADDR",
            "DAGRON_POSTGRES_SERVICE_HOST",
            "DAGRON_POSTGRES_PORT_5432_TCP_ADDR",
            "DAGRON_MAX_TASK_TIMEOUT_SECS",
            "DAGRON_MAX_TASK_TIMEOUT_SEC",
        ]);
        assert_eq!(unknown_vars(&names, &[]), ["DAGRON_MAX_TASK_TIMEOUT_SEC"]);
    }

    /// What the build around the engine reads for itself is not a typo: a
    /// name it gives, or a family by its prefix. A name that only starts like
    /// one it gave still is, and with none given so are they all.
    #[test]
    fn a_build_s_own_variables_are_not_typos() {
        let names = env(&[
            "DAGRON_SITE_ID",
            "DAGRON_SITE_IDS",
            "DAGRON_HOOK_URL",
            "DAGRON_HOOKS",
        ]);
        assert_eq!(
            unknown_vars(&names, &[]),
            [
                "DAGRON_SITE_ID",
                "DAGRON_SITE_IDS",
                "DAGRON_HOOK_URL",
                "DAGRON_HOOKS"
            ]
        );
        assert_eq!(
            unknown_vars(&names, &["DAGRON_SITE_ID", "DAGRON_HOOK_"]),
            ["DAGRON_SITE_IDS", "DAGRON_HOOKS"]
        );
    }

    /// The two settings the engine read and its registry did not list are in
    /// it, the key among the values never printed.
    #[test]
    fn the_settings_the_engine_reads_are_registered() {
        let names = env(&["DAGRON_DEFER_ENDPOINT", "DAGRON_ENV_SECRET_KEY"]);
        assert_eq!(unknown_vars(&names, &[]), Vec::<&str>::new());
        let key = knobs()
            .iter()
            .find(|knob| knob.name == "DAGRON_ENV_SECRET_KEY")
            .expect("registered");
        assert!(key.redact, "a key is never printed");
    }

    /// The sweeps' settings are known, and one misspelt draws
    /// the warning: `SWEEP_SHARE=off` is the rollback, and a typo of it
    /// would leave every sweep shared with no word said.
    #[test]
    fn a_misspelt_sweep_setting_is_a_typo() {
        let names = env(&[
            "SWEEP_INTERVAL_MS",
            "SWEEP_INTERVALS",
            "SWEEP_SHARE",
            "SWEEP_SHAER",
            "METRICS_HISTORY_CACHE_SECS",
        ]);
        assert_eq!(unknown_vars(&names, &[]), ["SWEEP_SHAER"]);
    }

    /// A link counts only beside its Service's `_SERVICE_HOST`, which nothing
    /// but the kubelet sets, and only in the kubelet's shapes: anything else
    /// under the same prefix is still a typo.
    #[test]
    fn only_the_kubelets_shapes_beside_their_host_are_links() {
        let names = env(&["DAGRON_ENGINE_PORT", "DAGRON_ENGINE_SERVICE_PORT"]);
        assert_eq!(
            unknown_vars(&names, &[]),
            ["DAGRON_ENGINE_PORT", "DAGRON_ENGINE_SERVICE_PORT"]
        );
        let names = env(&[
            "DAGRON_ENGINE_SERVICE_HOST",
            "DAGRON_ENGINE_PORTS",
            "DAGRON_ENGINE_PORT_HTTP",
            "DAGRON_ENGINE_PORT_8080_TLS",
            "DAGRON_ENGINE_PORT_8080_TCP_HOST",
            "DAGRON_ENGINE_PORT_8080_TCP_ADDR_2",
        ]);
        assert_eq!(
            unknown_vars(&names, &[]),
            [
                "DAGRON_ENGINE_PORTS",
                "DAGRON_ENGINE_PORT_HTTP",
                "DAGRON_ENGINE_PORT_8080_TLS",
                "DAGRON_ENGINE_PORT_8080_TCP_HOST",
                "DAGRON_ENGINE_PORT_8080_TCP_ADDR_2",
            ]
        );
    }
}
