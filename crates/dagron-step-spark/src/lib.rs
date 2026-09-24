//! dagron Spark step — submit a job, hand its id back, exit.
//!
//! The task's `command` is the **submit**, and nothing else. This binary starts
//! a Spark job, prints `dagron::handle=<id>`, and exits 0; the engine then parks
//! the task row on that id and a reconcile sweep polls it. So a six-hour Spark
//! run costs one row, not a worker slot — and if every scheduler dies mid-job,
//! the row is untouched and whichever replica comes back resumes the poll.
//!
//! ## The name is the idempotency
//!
//! The one genuinely dangerous window is: the cluster accepts the job, and the
//! engine dies before the park commits. A naive retry starts a second 200-node
//! cluster.
//!
//! So the job is named `dagron-<DAGRON_TASK_ID>-<DAGRON_EXTERNAL_EPOCH>`, both
//! injected by the engine at dispatch. `task_runs.id` is stable across lease
//! recovery, so the retried submit reuses the name, the cluster answers
//! AlreadyExists, and this step **adopts** the running job instead of starting
//! another. The epoch changes only when a row is deliberately re-armed for a
//! fresh job, so a genuine retry gets a genuine new job.
//!
//! `attempt` could not serve this: it increments on every claim *including*
//! lease recovery, so a name built from it would change at exactly the moment
//! adoption is needed.
//!
//! Adoption is conditional on the found job being **non-terminal**. Adopting a
//! job that already failed would park the task on a corpse it can never leave.
//!
//! ## …on `k8s`. The other two backends are weaker, and you should know which
//!
//! Adoption is only as strong as what the submission surface enforces:
//!
//! * **`k8s`** — real. `AlreadyExists` is the Kubernetes API's own guarantee on
//!   the object name, enforced by the apiserver, so a duplicate is impossible
//!   rather than unlikely.
//! * **`rest`** — only as strong as the body you write. `{{ name }}` is
//!   substituted wherever you put it; if you do not place it in a field the
//!   vendor treats as an idempotency key (Databricks `idempotency_token`, and
//!   its equivalents elsewhere), a retry after a crash submits a second job.
//!   The step cannot check this for you without knowing the vendor's schema,
//!   which is the coupling `rest` exists to avoid.
//! * **`spark-submit`** — none. `--name` is `spark.app.name`, a label; Spark
//!   offers no submission idempotency and this step does no pre-submit lookup,
//!   so a retried submit starts a second application. Use `k8s` where a crash
//!   during submit must not double-spend a cluster.
//!
//! ## Backends
//!
//! | `SPARK_BACKEND` | What it does |
//! |---|---|
//! | `k8s` (default) | creates a `SparkApplication` CR — vendor-free, no account, and AlreadyExists is the API's own semantics |
//! | `rest` | POSTs a body you supply to a URL you supply, and reads the id out of the response by path — one adapter for Databricks, EMR Serverless, Dataproc, Livy, Kyuubi |
//! | `spark-submit` | spawns the CLI. The honest escape hatch; needs a version-matched Spark distribution in the task image, which is the dependency complaint dagron positions against, so it lives in the *step's* image and never the control plane's |

use std::collections::BTreeMap;

use anyhow::{bail, Context, Result};
use serde_json::Value;

/// What a deferred submit prints to hand the engine its remote handle.
///
/// Must stay byte-identical to `dagron_core::dag::HANDLE_PREFIX`. Duplicated
/// rather than imported because importing it would mean depending on
/// `dagron-core`, whose `default = ["sqlite"]` links a bundled SQLite into every
/// task image running this step. A test pins the literal; the engine's own test
/// pins the other side.
pub const HANDLE_PREFIX: &str = "dagron::handle=";

/// Longest DNS-1123 label a Kubernetes object name may be.
const MAX_LABEL: usize = 63;

/// Which submission surface to use.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Backend {
    K8s,
    Rest,
    SparkSubmit,
}

impl Backend {
    pub fn parse(raw: Option<&str>) -> Result<Self> {
        match raw.map(str::trim).filter(|s| !s.is_empty()) {
            None | Some("k8s") => Ok(Backend::K8s),
            Some("rest") => Ok(Backend::Rest),
            Some("spark-submit") => Ok(Backend::SparkSubmit),
            // Named rather than lumped: `livy` is the one people will try, and
            // it works — through `rest`, at zero maintenance surface here.
            Some("livy") | Some("kyuubi") | Some("databricks") | Some("emr") | Some("dataproc") => {
                bail!(
                    "SPARK_BACKEND='{}' is not a separate backend: it submits over HTTP like \
                     every other vendor, so use SPARK_BACKEND=rest with SPARK_REST_URL, \
                     SPARK_REST_BODY and SPARK_REST_HANDLE_PATH. Naming each vendor here would \
                     buy a permanent surface for a request shape you can already write.",
                    raw.unwrap_or_default()
                )
            }
            Some(other) => bail!(
                "unknown SPARK_BACKEND '{other}'. Expected 'k8s' (a SparkApplication CR), \
                 'rest' (any submit API that answers with JSON), or 'spark-submit' (the CLI)."
            ),
        }
    }
}

/// Whether the step returns immediately (the engine parks) or blocks.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Wait {
    /// Print the handle and exit 0. The engine parks the row.
    Defer,
    /// Block until the job finishes. The fallback for an engine too old to
    /// park, and a trap worth naming: the executor's default `timeout_secs` is
    /// 25 seconds, so an inline wait without an explicit, generous
    /// `timeout_secs:` is a task that is killed long before any real job ends.
    Inline,
}

impl Wait {
    pub fn parse(raw: Option<&str>) -> Result<Self> {
        match raw.map(str::trim).filter(|s| !s.is_empty()) {
            None | Some("defer") => Ok(Wait::Defer),
            Some("inline") => Ok(Wait::Inline),
            Some(other) => bail!("unknown SPARK_WAIT '{other}'. Expected 'defer' or 'inline'."),
        }
    }
}

/// The remote job's name: stable across recovery, fresh after a re-arm.
///
/// Lowercased and validated as a DNS-1123 label, because the `k8s` backend uses
/// it as an object name and a name the cluster rejects is a submit that fails
/// for a reason nobody can act on. The other backends use the same string as an
/// idempotency token, so keeping one rule means a workflow behaves the same
/// whichever backend it names.
pub fn remote_name(task_id: &str, epoch: &str) -> Result<String> {
    let task_id = task_id.trim();
    let epoch = epoch.trim();
    if task_id.is_empty() {
        bail!(
            "DAGRON_TASK_ID is empty — the engine injects it for a task carrying `defer:`. \
             Running this step outside a deferred task means there is no stable name to make \
             the submit idempotent, so a retry after a crash would start a second job."
        );
    }
    if epoch.is_empty() || epoch.parse::<u64>().is_err() {
        bail!("DAGRON_EXTERNAL_EPOCH must be a non-negative integer, got '{epoch}'");
    }
    let name = format!("dagron-{}-{}", task_id.to_ascii_lowercase(), epoch);
    if name.len() > MAX_LABEL {
        bail!(
            "the remote job name '{name}' is {} characters, over the {MAX_LABEL}-character \
             DNS-1123 limit. A task id is normally a 36-character UUID, which leaves room; a \
             longer one has to be shortened before it can name a Kubernetes object.",
            name.len()
        );
    }
    if !name.bytes().all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-') {
        bail!(
            "the remote job name '{name}' is not a legal DNS-1123 label (lowercase \
             alphanumerics and '-' only) — DAGRON_TASK_ID contains something unexpected"
        );
    }
    // `dagron-` guarantees an alphanumeric start; the epoch digits guarantee the end.
    Ok(name)
}

/// Resolved step configuration.
#[derive(Debug, Clone)]
pub struct StepConfig {
    pub backend: Backend,
    pub wait: Wait,
    /// The remote job's name / idempotency token.
    pub name: String,
    /// The application to run (a jar or a .py URI).
    pub app: String,
    /// Namespace for the `k8s` backend.
    pub namespace: String,
    /// Spark image for the `k8s` backend.
    pub image: String,
    /// ServiceAccount the driver pod runs as (`k8s` backend). `None` leaves the
    /// operator's default, which is the namespace's `default` account — one that
    /// usually cannot create the executor pods the driver asks for.
    pub driver_service_account: Option<String>,
    /// Extra `sparkConf` / CLI `--conf` entries, from `SPARK_CONF_*`.
    pub conf: BTreeMap<String, String>,
}

impl StepConfig {
    /// Build from the process environment plus an optional `--app` override.
    pub fn from_env(app_arg: Option<String>) -> Result<Self> {
        Self::build(app_arg, |k| std::env::var(k).ok())
    }

    /// Testable core: `get` resolves one environment variable.
    pub fn build(app_arg: Option<String>, get: impl Fn(&str) -> Option<String>) -> Result<Self> {
        let backend = Backend::parse(get("SPARK_BACKEND").as_deref())?;
        let wait = Wait::parse(get("SPARK_WAIT").as_deref())?;
        let name = remote_name(
            &get("DAGRON_TASK_ID").unwrap_or_default(),
            &get("DAGRON_EXTERNAL_EPOCH").unwrap_or_default(),
        )?;
        let app = app_arg
            .or_else(|| get("SPARK_APP"))
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .context("no application to run: pass `--app <uri>` or set SPARK_APP")?;
        // `SPARK_CONF_SPARK_EXECUTOR_INSTANCES=4` → `spark.executor.instances=4`.
        // Underscores become dots because an environment variable cannot carry
        // one, which is the whole reason the mapping exists.
        let mut conf = BTreeMap::new();
        for (k, v) in std::env::vars() {
            if let Some(rest) = k.strip_prefix("SPARK_CONF_") {
                if rest.is_empty() {
                    continue;
                }
                conf.insert(rest.to_ascii_lowercase().replace('_', "."), v);
            }
        }
        Ok(StepConfig {
            backend,
            wait,
            name,
            app,
            namespace: get("SPARK_NAMESPACE").unwrap_or_else(|| "default".into()),
            image: get("SPARK_IMAGE").unwrap_or_else(|| "spark:3.5.3".into()),
            driver_service_account: get("SPARK_DRIVER_SERVICE_ACCOUNT").filter(|s| !s.is_empty()),
            conf,
        })
    }
}

/// The `SparkApplication` custom resource this step creates.
///
/// Built as JSON rather than against a generated CRD type: the schema is the
/// operator's, not ours, and a typed mirror would be a second copy to keep in
/// step with a project that versions independently of dagron.
pub fn spark_application(cfg: &StepConfig) -> Value {
    let kind = if cfg.app.ends_with(".py") { "Python" } else { "Scala" };
    let mut cr = serde_json::json!({
        "apiVersion": "sparkoperator.k8s.io/v1beta2",
        "kind": "SparkApplication",
        "metadata": {
            "name": cfg.name,
            "namespace": cfg.namespace,
            // Stamped so an operator sweeping the cluster can tell which jobs
            // dagron started, and which run each belongs to, without parsing
            // the name.
            "labels": {
                "app.kubernetes.io/managed-by": "dagron",
                "io.dagron.step": "spark",
            },
        },
        "spec": {
            "type": kind,
            "mode": "cluster",
            "image": cfg.image,
            "mainApplicationFile": cfg.app,
            "sparkVersion": "3.5.3",
            "restartPolicy": { "type": "Never" },
            "sparkConf": cfg.conf,
        },
    });
    // Not through `SPARK_CONF_*`: that mapping lowercases every key, and Spark's
    // conf keys are case-sensitive, so `…driver.serviceAccountName` cannot be
    // written that way — it would be sent as `…serviceaccountname` and ignored.
    if let Some(sa) = &cfg.driver_service_account {
        cr["spec"]["driver"] = serde_json::json!({ "serviceAccount": sa });
    }
    cr
}

/// States a job can be in that mean "finished". Adopting one of these would
/// park the task on a job it can never see move.
const TERMINAL_STATES: &[&str] =
    &["COMPLETED", "FAILED", "SUBMISSION_FAILED", "KILLED", "FAILING", "SUCCEEDED", "ERROR"];

/// Whether a state string reported by an existing job means it has finished.
///
/// Compared case-insensitively against a list covering the spark-operator's
/// `applicationState.state` and the common REST spellings, because a submit
/// that finds an existing job has to decide *adopt or fail* and getting it
/// wrong in either direction is expensive: adopting a corpse parks the task
/// forever, and refusing to adopt a live job duplicates a cluster.
pub fn is_terminal(state: &str) -> bool {
    let s = state.trim().to_ascii_uppercase();
    TERMINAL_STATES.contains(&s.as_str())
}

/// Decide what to do when the remote system says the name already exists.
///
/// `Ok(())` = adopt it, print the handle, let the engine poll. `Err` = the job
/// is already finished, so this row must not park on it.
pub fn adopt_or_fail(name: &str, state: Option<&str>) -> Result<()> {
    match state {
        // A job whose state we cannot read is adopted. The poller will tell us
        // what it is within one interval, which is a better answer than
        // refusing a job that is very likely running.
        None => Ok(()),
        Some(s) if !is_terminal(s) => Ok(()),
        Some(s) => bail!(
            "remote job '{name}' already exists and is already {s}. This task's epoch names a \
             job that has finished, so parking on it would wait forever. A genuine retry takes \
             a fresh epoch (the engine bumps it on a resolved failure, a rerun, or a cleared \
             task); if you are seeing this on a first attempt, something else created a job \
             with this exact name."
        ),
    }
}

/// Follow a dotted path into a JSON document.
///
/// Twelve lines rather than a dependency on `dagron_core::jsonpred`, for the
/// reason the manifest records: importing it would link a bundled SQLite into
/// every task image that runs this step. The grammar is deliberately the same
/// one `defer.http` uses, so an author writes `state.result_state` in both
/// places and it means the same thing.
pub fn dotted_get<'a>(doc: &'a Value, path: &str) -> Option<&'a Value> {
    let mut cur = doc;
    for seg in path.trim().split('.') {
        if seg.is_empty() {
            return None;
        }
        cur = match cur {
            Value::Object(m) => m.get(seg)?,
            Value::Array(a) => a.get(seg.parse::<usize>().ok()?)?,
            _ => return None,
        };
    }
    Some(cur)
}

/// The line that hands the engine its handle. The **last** such line wins, so a
/// step may log freely before it.
pub fn handle_line(handle: &str) -> String {
    format!("{HANDLE_PREFIX}{handle}")
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn the_handle_prefix_matches_the_engines() {
        // Pinned as a literal on both sides: this crate cannot import the
        // engine's constant without dragging a database into a task image, so
        // the contract is held by two tests rather than one symbol.
        assert_eq!(HANDLE_PREFIX, "dagron::handle=");
        assert_eq!(handle_line("app-7"), "dagron::handle=app-7");
    }

    #[test]
    fn the_name_is_stable_across_recovery_and_fresh_after_a_rearm() {
        let id = "6f1e2b9c-1111-4222-8333-444455556666";
        // Same task, same epoch → same name. This is what makes a post-crash
        // resubmit adopt instead of duplicating.
        assert_eq!(remote_name(id, "0").unwrap(), remote_name(id, "0").unwrap());
        // A bumped epoch is a different job.
        assert_ne!(remote_name(id, "0").unwrap(), remote_name(id, "1").unwrap());
        assert_eq!(remote_name(id, "0").unwrap(), format!("dagron-{id}-0"));
        // …and it is a legal DNS-1123 label, which is why 36-char UUIDs fit.
        let n = remote_name(id, "12").unwrap();
        assert!(n.len() <= MAX_LABEL, "len {}", n.len());
    }

    #[test]
    fn a_name_that_cannot_be_a_kubernetes_object_is_refused_at_submit() {
        assert!(remote_name("", "0").unwrap_err().to_string().contains("DAGRON_TASK_ID is empty"));
        assert!(remote_name("abc", "").unwrap_err().to_string().contains("EXTERNAL_EPOCH"));
        assert!(remote_name("abc", "nope").unwrap_err().to_string().contains("EXTERNAL_EPOCH"));
        let long = remote_name(&"a".repeat(80), "0").unwrap_err().to_string();
        assert!(long.contains("over the 63-character"), "{long}");
        let bad = remote_name("has_underscore", "0").unwrap_err().to_string();
        assert!(bad.contains("DNS-1123"), "{bad}");
        // Uppercase is normalised rather than refused — a task id is not the
        // author's to choose, so failing on its case would be unactionable.
        assert_eq!(remote_name("ABC", "0").unwrap(), "dagron-abc-0");
    }

    #[test]
    fn backends_parse_and_vendors_are_pointed_at_rest() {
        assert_eq!(Backend::parse(None).unwrap(), Backend::K8s);
        assert_eq!(Backend::parse(Some(" rest ")).unwrap(), Backend::Rest);
        assert_eq!(Backend::parse(Some("spark-submit")).unwrap(), Backend::SparkSubmit);
        // A vendor name is a real thing to try, so the error teaches rather
        // than just refusing.
        let livy = Backend::parse(Some("livy")).unwrap_err().to_string();
        assert!(livy.contains("SPARK_BACKEND=rest"), "{livy}");
        assert!(Backend::parse(Some("nope")).unwrap_err().to_string().contains("unknown"));
        assert_eq!(Wait::parse(None).unwrap(), Wait::Defer, "defer is the default");
        assert_eq!(Wait::parse(Some("inline")).unwrap(), Wait::Inline);
    }

    #[test]
    fn config_reads_the_env_and_the_app_argument_wins() {
        let env = |k: &str| match k {
            "DAGRON_TASK_ID" => Some("6f1e2b9c-1111-4222-8333-444455556666".to_string()),
            "DAGRON_EXTERNAL_EPOCH" => Some("2".into()),
            "SPARK_APP" => Some("s3://from-env.py".into()),
            "SPARK_NAMESPACE" => Some("data".into()),
            _ => None,
        };
        let c = StepConfig::build(None, env).unwrap();
        assert_eq!(c.app, "s3://from-env.py");
        assert_eq!(c.namespace, "data");
        assert_eq!(c.backend, Backend::K8s);
        assert!(c.name.ends_with("-2"));

        let c = StepConfig::build(Some("s3://from-arg.py".into()), env).unwrap();
        assert_eq!(c.app, "s3://from-arg.py", "--app overrides SPARK_APP");

        let missing = StepConfig::build(None, |k| match k {
            "DAGRON_TASK_ID" => Some("abc".into()),
            "DAGRON_EXTERNAL_EPOCH" => Some("0".into()),
            _ => None,
        });
        assert!(missing.unwrap_err().to_string().contains("no application to run"));
    }

    #[test]
    fn the_cr_names_the_job_and_picks_the_language_from_the_app() {
        let mut cfg = StepConfig::build(Some("s3://j/rollup.py".into()), |k| match k {
            "DAGRON_TASK_ID" => Some("6f1e2b9c-1111-4222-8333-444455556666".into()),
            "DAGRON_EXTERNAL_EPOCH" => Some("0".into()),
            _ => None,
        })
        .unwrap();
        let cr = spark_application(&cfg);
        assert_eq!(cr["metadata"]["name"], json!(cfg.name));
        assert_eq!(cr["spec"]["type"], json!("Python"));
        assert_eq!(cr["spec"]["restartPolicy"]["type"], json!("Never"),
            "the ENGINE retries, so the operator must not also restart the job");
        assert_eq!(cr["metadata"]["labels"]["app.kubernetes.io/managed-by"], json!("dagron"));

        cfg.app = "s3://j/rollup.jar".into();
        assert_eq!(spark_application(&cfg)["spec"]["type"], json!("Scala"));
    }

    #[test]
    fn the_driver_service_account_is_set_only_when_asked_for() {
        let build = |sa: Option<&str>| {
            let sa = sa.map(str::to_string);
            StepConfig::build(Some("s3://j/r.py".into()), move |k| match k {
                "DAGRON_TASK_ID" => Some("6f1e2b9c-1111-4222-8333-444455556666".into()),
                "DAGRON_EXTERNAL_EPOCH" => Some("0".into()),
                "SPARK_DRIVER_SERVICE_ACCOUNT" => sa.clone(),
                _ => None,
            })
            .unwrap()
        };
        let cr = spark_application(&build(Some("spark")));
        assert_eq!(cr["spec"]["driver"]["serviceAccount"], json!("spark"));
        // Unset or empty: no `driver` block at all, so the operator's defaults stand.
        assert!(spark_application(&build(None))["spec"].get("driver").is_none());
        assert!(spark_application(&build(Some("")))["spec"].get("driver").is_none());
    }

    /// The adopt-or-fail decision, which is the whole point of the stable name.
    #[test]
    fn an_existing_job_is_adopted_only_while_it_is_still_running() {
        assert!(adopt_or_fail("j", Some("RUNNING")).is_ok());
        assert!(adopt_or_fail("j", Some("SUBMITTED")).is_ok());
        assert!(adopt_or_fail("j", Some("PENDING_RERUN")).is_ok());
        // Unknown state → adopt; the poller resolves it within an interval,
        // which beats refusing a job that is probably running.
        assert!(adopt_or_fail("j", None).is_ok());

        for dead in ["COMPLETED", "failed", " Killed ", "SUBMISSION_FAILED"] {
            let e = adopt_or_fail("j", Some(dead)).unwrap_err().to_string();
            assert!(e.contains("already"), "{dead}: {e}");
            assert!(e.contains("fresh epoch"), "says how to get a new job: {e}");
        }
    }

    #[test]
    fn dotted_get_matches_the_poller_grammar() {
        let d = json!({"state": {"result_state": "SUCCESS"}, "items": [{"id": "a"}]});
        assert_eq!(dotted_get(&d, "state.result_state"), Some(&json!("SUCCESS")));
        assert_eq!(dotted_get(&d, "items.0.id"), Some(&json!("a")));
        assert_eq!(dotted_get(&d, "state.missing"), None);
        assert_eq!(dotted_get(&d, "state..x"), None, "an empty segment is a typo");
    }
}
