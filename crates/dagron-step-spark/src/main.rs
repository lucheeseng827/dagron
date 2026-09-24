//! dagron-step-spark — submit a Spark job as a workflow task.
//!
//! Invoked as a task `command`. Submits, prints `dagron::handle=<id>`, exits 0.
//! The engine parks the task on that id; a reconcile sweep polls it. See
//! [`dagron_step_spark`] for the naming and adoption contract.
//!
//! Usage: `dagron-step-spark submit [--app <uri>]`

use anyhow::{bail, Context, Result};
use dagron_step_spark::{dotted_get, handle_line, Backend, StepConfig, Wait};
// Only the k8s backend builds a SparkApplication CR, and only it can meet an
// AlreadyExists that has to be adopted or refused — the REST and CLI paths get
// their idempotency from the vendor's own token and from a deterministic name.
#[cfg(feature = "k8s")]
use dagron_step_spark::{adopt_or_fail, spark_application};

#[tokio::main]
async fn main() -> Result<()> {
    dagron_logging::init("dagron-step-spark");

    let mut args = std::env::args().skip(1);
    let verb = args.next().unwrap_or_else(|| "submit".into());
    if verb != "submit" {
        bail!(
            "unknown command '{verb}'. This step submits: `dagron-step-spark submit \
             [--app <uri>]`. Polling and cancelling are the engine's job, through `defer:` — \
             see docs/EXTERNAL_JOBS.md."
        );
    }
    let mut app = None;
    while let Some(a) = args.next() {
        match a.as_str() {
            "--app" => app = args.next(),
            other => bail!("unknown argument '{other}'; expected --app <uri>"),
        }
    }

    let cfg = StepConfig::from_env(app)?;
    if cfg.wait == Wait::Inline {
        // `inline` is only real on `spark-submit`, where `Command::output()`
        // genuinely blocks on the child. The `k8s` and `rest` backends submit
        // and return: creating a CR or POSTing a body does not wait for the
        // job, so an "inline" task there would print a handle and succeed
        // *immediately*, advancing every dependent while the Spark job is still
        // starting. Reporting completion for work that has not happened is the
        // one outcome this step exists to prevent, so it is refused rather than
        // warned about — a warning is not a guard, and the old one additionally
        // claimed a worker was being held when it was not.
        match cfg.backend {
            Backend::SparkSubmit => tracing::warn!(
                "SPARK_WAIT=inline holds this worker for the whole job. The task needs an \
                 explicit, generous `timeout_secs:` — the default is 25 seconds, which kills \
                 the task long before any real Spark job finishes. Prefer the default (defer), \
                 where the engine parks the row and holds no worker."
            ),
            Backend::K8s | Backend::Rest => bail!(
                "SPARK_WAIT=inline is not implemented for SPARK_BACKEND={}. That backend \
                 submits and returns, so 'inline' would succeed the task the moment the job \
                 was accepted and release every dependent against a job that has not run. \
                 Use the default (SPARK_WAIT=defer) with a `defer:` block — the engine parks \
                 the row and a sweep resolves it when the job actually finishes.",
                match cfg.backend {
                    Backend::K8s => "k8s",
                    Backend::Rest => "rest",
                    Backend::SparkSubmit => unreachable!(),
                }
            ),
        }
    }

    let handle = match cfg.backend {
        Backend::K8s => submit_k8s(&cfg).await?,
        Backend::Rest => submit_rest(&cfg).await?,
        Backend::SparkSubmit => submit_cli(&cfg).await?,
    };

    // The handle goes to stdout, last line, so the engine reads it off the
    // task's output. Everything else this binary says goes to stderr via
    // tracing — stdout is the protocol.
    println!("{}", handle_line(&handle));
    Ok(())
}

/// Create the `SparkApplication` CR, adopting an existing one under the same
/// name when the cluster says it is already there.
#[cfg(feature = "k8s")]
async fn submit_k8s(cfg: &StepConfig) -> Result<String> {
    use kube::api::{Api, DynamicObject, GroupVersionKind, PostParams};
    use kube::discovery::ApiResource;

    // kube's rustls client needs a process-wide provider installed before it
    // opens TLS to the apiserver, or it fails at runtime with a message that
    // does not name this as the cause.
    let _ = rustls::crypto::ring::default_provider().install_default();

    let client = kube::Client::try_default()
        .await
        .context("connecting to the Kubernetes apiserver (in-cluster config or ~/.kube/config)")?;
    let gvk = GroupVersionKind::gvk("sparkoperator.k8s.io", "v1beta2", "SparkApplication");
    let api: Api<DynamicObject> =
        Api::namespaced_with(client, &cfg.namespace, &ApiResource::from_gvk(&gvk));

    let cr: DynamicObject = serde_json::from_value(spark_application(cfg))
        .context("building the SparkApplication resource")?;
    match api.create(&PostParams::default(), &cr).await {
        Ok(_) => {
            tracing::info!(name = %cfg.name, namespace = %cfg.namespace, "submitted SparkApplication");
            Ok(cfg.name.clone())
        }
        // THE adoption path, and it is the API's own semantics rather than
        // anything dagron invented: the name is deterministic, so a resubmit
        // after a crash lands here and finds the job it already started.
        Err(kube::Error::Api(e)) if e.code == 409 => {
            let existing = api
                .get(&cfg.name)
                .await
                .with_context(|| format!("reading the existing SparkApplication '{}'", cfg.name))?;
            let state = existing
                .data
                .get("status")
                .and_then(|s| dotted_get(s, "applicationState.state"))
                .and_then(|v| v.as_str())
                .map(str::to_string);
            adopt_or_fail(&cfg.name, state.as_deref())?;
            tracing::info!(
                name = %cfg.name, state = %state.as_deref().unwrap_or("unknown"),
                "SparkApplication already exists and is not finished — adopting it rather than \
                 starting a second job"
            );
            Ok(cfg.name.clone())
        }
        Err(e) => Err(e).context("creating the SparkApplication"),
    }
}

#[cfg(not(feature = "k8s"))]
async fn submit_k8s(_cfg: &StepConfig) -> Result<String> {
    bail!(
        "SPARK_BACKEND=k8s needs a build with `--features k8s` (the Kubernetes client is not \
         linked into this binary — it is a large dependency tree a task image submitting over \
         HTTP should not carry). Use the image built with it, or SPARK_BACKEND=rest against the \
         apiserver or your vendor's submit API. See docs/EXTERNAL_JOBS.md."
    )
}

/// POST a caller-supplied body to a caller-supplied URL and read the job id out
/// of the response by path.
///
/// No vendor code: the body is yours, the URL is yours, and the id's location is
/// a dotted path. That is what lets one adapter reach Databricks, EMR
/// Serverless, Dataproc, Livy and Kyuubi — and what keeps their API changes out
/// of dagron's release notes.
async fn submit_rest(cfg: &StepConfig) -> Result<String> {
    let url = std::env::var("SPARK_REST_URL")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .context("SPARK_BACKEND=rest needs SPARK_REST_URL")?;
    let path = std::env::var("SPARK_REST_HANDLE_PATH")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .context(
            "SPARK_BACKEND=rest needs SPARK_REST_HANDLE_PATH — the dotted path to the job id in \
             the submit response (e.g. `run_id`, `id`, `jobRun.id`)",
        )?;
    let raw = match std::env::var("SPARK_REST_BODY_FILE").ok().filter(|s| !s.trim().is_empty()) {
        Some(f) => std::fs::read_to_string(&f).with_context(|| format!("reading {f}"))?,
        None => std::env::var("SPARK_REST_BODY")
            .ok()
            .filter(|s| !s.trim().is_empty())
            .context("SPARK_BACKEND=rest needs SPARK_REST_BODY or SPARK_REST_BODY_FILE")?,
    };
    // `{{ name }}` is where the vendor's idempotency token goes — Databricks'
    // `idempotency_token`, EMR's `clientToken`, Dataproc's `requestId`. Putting
    // the deterministic name there is what makes a resubmit after a crash adopt
    // rather than duplicate, exactly as the k8s backend gets from AlreadyExists.
    let body = raw.replace("{{ name }}", &cfg.name).replace("{{ app }}", &cfg.app);
    let body: serde_json::Value = serde_json::from_str(&body)
        .context("SPARK_REST_BODY is not valid JSON after substitution")?;

    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .build()
        .context("building the submit client")?;
    let mut req = client.post(&url).json(&body);
    for (k, v) in std::env::vars() {
        if let Some(h) = k.strip_prefix("SPARK_REST_HEADER_") {
            // `SPARK_REST_HEADER_AUTHORIZATION=Bearer x` → `Authorization: Bearer x`.
            req = req.header(h.replace('_', "-"), v);
        }
    }
    let resp = req.send().await.with_context(|| format!("POST {url}"))?;
    let status = resp.status();
    let text = resp.text().await.unwrap_or_default();
    if !status.is_success() {
        bail!("submit endpoint returned {status}: {}", text.chars().take(512).collect::<String>());
    }
    let doc: serde_json::Value =
        serde_json::from_str(&text).context("the submit endpoint did not answer with JSON")?;
    let handle = dotted_get(&doc, &path)
        .map(|v| match v {
            serde_json::Value::String(s) => s.clone(),
            other => other.to_string(),
        })
        .filter(|s| !s.is_empty())
        .with_context(|| {
            format!("the submit response has no job id at '{path}' — check SPARK_REST_HANDLE_PATH")
        })?;
    tracing::info!(%handle, "submitted over REST");
    Ok(handle)
}

/// Spawn `spark-submit`. The escape hatch: it needs a version-matched Spark
/// distribution in the task's image, which is exactly the dependency problem
/// dagron positions against — so it lives in the step's image, never the
/// control plane's.
/// The one `waitAppCompletion` key whose default works against `inline`.
///
/// YARN and Kubernetes default their equivalents to *true* in cluster mode, so
/// inline already blocks there; standalone defaults to false.
const STANDALONE_WAIT_KEY: &str = "spark.standalone.submit.waitAppCompletion";

/// The operator's own `SPARK_CONF_*` entry that would cancel the wait `inline`
/// promises, if there is one.
///
/// Anything that is not literally `true` counts, not just `false`: Spark reads
/// this as a boolean, so a typo is a `false`, and a `false` here is a task that
/// reports success against a job that has not run. Refused rather than
/// overridden, on the same reasoning that refuses `SPARK_WAIT=inline` on `k8s`
/// and `rest` — a warning does not stop a false success.
fn inline_wait_conflict(
    conf: &std::collections::BTreeMap<String, String>,
) -> Option<(&String, &String)> {
    conf.iter().find(|(k, v)| {
        k.eq_ignore_ascii_case(STANDALONE_WAIT_KEY) && !v.trim().eq_ignore_ascii_case("true")
    })
}

async fn submit_cli(cfg: &StepConfig) -> Result<String> {
    let bin = std::env::var("SPARK_SUBMIT_BIN").unwrap_or_else(|_| "spark-submit".into());
    let master = std::env::var("SPARK_MASTER")
        .ok()
        .filter(|s| !s.trim().is_empty())
        .context("SPARK_BACKEND=spark-submit needs SPARK_MASTER")?;
    let mut cmd = tokio::process::Command::new(&bin);
    cmd.arg("--master").arg(&master).arg("--deploy-mode").arg("cluster");
    // The name is the handle here too, so `--status`/`--kill` by name work and
    // a resubmit is recognisable in the cluster UI.
    cmd.arg("--name").arg(&cfg.name);
    // In cluster mode both YARN and Kubernetes make `spark-submit` WAIT for the
    // remote application to finish. That is the opposite of a deferred submit:
    // the process would block for the whole job, and `timeout_secs` — which
    // bounds the submit, not the job — would kill the task before the handle
    // was ever printed. So a deferred submit asks for fire-and-forget.
    //
    // Both keys are set because the master string is not always classifiable
    // (`yarn`, `k8s://…`, a proxy URL), and each manager ignores the other's
    // key. Set before `cfg.conf` so an operator who deliberately wants the
    // blocking behaviour can override it with SPARK_CONF_*.
    if cfg.wait == Wait::Defer {
        cmd.arg("--conf").arg("spark.yarn.submit.waitAppCompletion=false");
        cmd.arg("--conf").arg("spark.kubernetes.submission.waitAppCompletion=false");
    } else {
        // `inline` promises the task holds its worker until the JOB finishes.
        // On a **standalone** master that is not what `--deploy-mode cluster`
        // does: Spark defaults `spark.standalone.submit.waitAppCompletion` to
        // FALSE, so the CLI returns as soon as the driver is accepted and the
        // task succeeds against a job that has not run — releasing every
        // dependent against work still in flight. YARN and Kubernetes default
        // the other way, which is why only this one needs saying.
        //
        // Refused rather than overridden when the operator's own conf turns it
        // off, on the same reasoning that refuses `SPARK_WAIT=inline` on `k8s`
        // and `rest`: a warning does not stop a false success.
        if let Some((k, v)) = inline_wait_conflict(&cfg.conf) {
            bail!(
                "SPARK_WAIT=inline with {k}={v} contradicts itself: 'inline' means this \
                 task waits for the job, and that setting is what makes spark-submit \
                 return as soon as a standalone master accepts the driver. The task \
                 would succeed against a job that has not run, releasing every dependent \
                 against work still in flight. Drop the override, or use the default \
                 SPARK_WAIT=defer with a `defer:` block."
            );
        }
        cmd.arg("--conf").arg(format!("{STANDALONE_WAIT_KEY}=true"));
    }
    for (k, v) in &cfg.conf {
        cmd.arg("--conf").arg(format!("{k}={v}"));
    }
    cmd.arg(&cfg.app);
    let out = cmd
        .output()
        .await
        .with_context(|| format!("spawning {bin} (is a Spark distribution in this image?)"))?;
    if !out.status.success() {
        bail!(
            "{bin} exited {}: {}",
            out.status,
            String::from_utf8_lossy(&out.stderr).chars().take(1024).collect::<String>()
        );
    }
    // The name is deterministic, so it is the handle regardless of what the CLI
    // prints — which differs by master type and is not worth parsing.
    tracing::info!(name = %cfg.name, "submitted via spark-submit");
    Ok(cfg.name.clone())
}


#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    fn conf(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs.iter().map(|(k, v)| (k.to_string(), v.to_string())).collect()
    }

    /// `inline` means this task waits for the JOB. On a standalone master
    /// `spark.standalone.submit.waitAppCompletion` defaults to false, so the step
    /// sets it — and an operator conf that turns it back off is a contradiction,
    /// not a preference: the task would succeed against a job that has not run.
    #[test]
    fn an_inline_wait_turned_off_by_conf_is_a_conflict() {
        let c = conf(&[("spark.standalone.submit.waitAppCompletion", "false")]);
        assert!(inline_wait_conflict(&c).is_some());
    }

    /// A boolean Spark cannot parse is a boolean Spark treats as false, so a typo
    /// must not slip past a guard that only looked for the literal "false".
    #[test]
    fn anything_that_is_not_true_is_a_conflict() {
        for v in ["false", "0", "no", "FALSE ", "yes", "", "tru"] {
            let c = conf(&[("spark.standalone.submit.waitAppCompletion", v)]);
            assert!(inline_wait_conflict(&c).is_some(), "{v:?} should conflict");
        }
    }

    #[test]
    fn an_explicit_true_agrees_with_inline_and_is_allowed() {
        for v in ["true", "TRUE", " true "] {
            let c = conf(&[("spark.standalone.submit.waitAppCompletion", v)]);
            assert!(inline_wait_conflict(&c).is_none(), "{v:?} should be fine");
        }
    }

    /// The operator writes this key through `SPARK_CONF_*` env-var mangling, so
    /// the guard compares case-insensitively rather than letting a
    /// capitalisation walk past it.
    #[test]
    fn the_key_match_is_case_insensitive() {
        let c = conf(&[("SPARK.STANDALONE.SUBMIT.WAITAPPCOMPLETION", "false")]);
        assert!(inline_wait_conflict(&c).is_some());
    }

    /// Narrow on purpose: the YARN key is the DEFERRED path's, and turning that
    /// one off is exactly what a deferred submit asks for.
    #[test]
    fn other_conf_entries_are_left_alone() {
        let c = conf(&[
            ("spark.executor.instances", "8"),
            ("spark.yarn.submit.waitAppCompletion", "false"),
        ]);
        assert!(inline_wait_conflict(&c).is_none());
    }
}
