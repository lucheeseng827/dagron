//! OpenLineage emitter — emits data-lineage events to an OpenLineage backend.
//!
//! On run finalization the engine calls [`OpenLineageClient::emit_run_completed`],
//! which POSTs an OpenLineage `RunEvent` (`COMPLETE` / `FAIL`) to a lineage backend
//! (Marquez, etc.). Best-effort: a lineage backend being down never affects run
//! execution. `runId` reuses dagron's run id (a UUID, as OpenLineage requires);
//! `job.name` is the workflow name.
//!
//! The event names the run's datasets: `inputs` are what it consumed (its
//! workflow's `on_datasets:` and its `wait: { dataset: … }` sensors), `outputs`
//! what it produced (`produces:` entries the ledger recorded for it). Both sit
//! on the workflow's job, not on a task: `job.name` is the workflow, so that is
//! the granularity the graph gets. Only the terminal event is emitted; `START`
//! is a follow-up. Configure with `OPENLINEAGE_URL` (+ optional
//! `OPENLINEAGE_NAMESPACE`, default `dagron`).

use anyhow::{Context, Result};
use serde_json::{json, Value};

const PRODUCER: &str = "https://github.com/lucheeseng827/dagron";
const SCHEMA_URL: &str = "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent";

/// The dataset URIs a run read and wrote, as dagron names them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct RunDatasets {
    pub inputs: Vec<String>,
    pub outputs: Vec<String>,
}

/// Posts OpenLineage RunEvents to `{url}/api/v1/lineage`.
pub struct OpenLineageClient {
    http: reqwest::Client,
    url: String,
    namespace: String,
}

impl OpenLineageClient {
    pub fn new(url: impl Into<String>, namespace: impl Into<String>) -> Self {
        Self {
            http: reqwest::Client::new(),
            url: url.into(),
            namespace: namespace.into(),
        }
    }

    /// Build from `OPENLINEAGE_URL` (required → `None` if unset) + optional
    /// `OPENLINEAGE_NAMESPACE` (default `dagron`).
    pub fn from_env() -> Option<Self> {
        let url = std::env::var("OPENLINEAGE_URL")
            .ok()
            .filter(|u| !u.is_empty())?;
        let namespace =
            std::env::var("OPENLINEAGE_NAMESPACE").unwrap_or_else(|_| "dagron".to_string());
        Some(Self::new(url, namespace))
    }

    /// Emit the terminal RunEvent for a finished run. Best-effort.
    pub async fn emit_run_completed(
        &self,
        run_id: &str,
        job_name: &str,
        failed: bool,
        datasets: &RunDatasets,
    ) -> Result<()> {
        let event = build_event(&self.namespace, run_id, job_name, failed, datasets);
        let endpoint = format!("{}/api/v1/lineage", self.url.trim_end_matches('/'));
        let resp = self
            .http
            .post(endpoint)
            .json(&event)
            .send()
            .await
            .context("posting OpenLineage event")?;
        if !resp.status().is_success() {
            anyhow::bail!("OpenLineage backend returned {}", resp.status());
        }
        Ok(())
    }
}

/// Build an OpenLineage `RunEvent` (`COMPLETE` or `FAIL`).
pub fn build_event(
    namespace: &str,
    run_id: &str,
    job_name: &str,
    failed: bool,
    datasets: &RunDatasets,
) -> Value {
    let named = |uris: &[String]| -> Vec<Value> {
        uris.iter()
            .map(|uri| {
                let (ns, name) = dataset_name(namespace, uri);
                json!({ "namespace": ns, "name": name })
            })
            .collect()
    };
    json!({
        "eventType": if failed { "FAIL" } else { "COMPLETE" },
        "eventTime": chrono::Utc::now().to_rfc3339(),
        "producer": PRODUCER,
        "schemaURL": SCHEMA_URL,
        "run": { "runId": run_id },
        "job": { "namespace": namespace, "name": job_name },
        "inputs": named(&datasets.inputs),
        "outputs": named(&datasets.outputs)
    })
}

/// OpenLineage `(namespace, name)` for a dagron dataset URI.
///
/// OpenLineage names a dataset by where it lives (`scheme://authority`) and its
/// path within that, which is how `s3://bucket/key` and
/// `clickhouse://analytics/marts/daily` split. dagron never dereferences a
/// dataset URI and accepts any string without whitespace, so a URI with no path
/// is named whole, and a name with no scheme at all lives in the job's
/// `namespace`: the same string always maps to the same dataset, which is what
/// joins one run's output to another's input.
pub fn dataset_name(namespace: &str, uri: &str) -> (String, String) {
    if let Some((scheme, rest)) = uri.split_once("://") {
        if let Some((authority, path)) = rest.split_once('/') {
            if !scheme.is_empty() && !authority.is_empty() && !path.is_empty() {
                return (format!("{scheme}://{authority}"), path.to_string());
            }
        }
        return (format!("{scheme}://"), rest.to_string());
    }
    (namespace.to_string(), uri.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_and_fail_events_are_well_formed() {
        let none = RunDatasets::default();
        let ok = build_event(
            "dagron",
            "11111111-1111-4111-8111-111111111111",
            "etl",
            false,
            &none,
        );
        assert_eq!(ok["eventType"], "COMPLETE");
        assert_eq!(ok["run"]["runId"], "11111111-1111-4111-8111-111111111111");
        assert_eq!(ok["job"]["namespace"], "dagron");
        assert_eq!(ok["job"]["name"], "etl");
        assert_eq!(ok["producer"], PRODUCER);
        assert!(ok["eventTime"].as_str().unwrap().contains('T'));
        // No datasets is two empty lists, not missing keys.
        assert_eq!(ok["inputs"], json!([]));
        assert_eq!(ok["outputs"], json!([]));

        let bad = build_event("dagron", "r2", "etl", true, &none);
        assert_eq!(bad["eventType"], "FAIL");
    }

    #[test]
    fn the_runs_datasets_are_its_inputs_and_outputs() {
        let datasets = RunDatasets {
            inputs: vec!["s3://raw/events/2026-10-03".into()],
            outputs: vec!["clickhouse://analytics/marts/daily_rollup".into()],
        };
        let ev = build_event("dagron", "r1", "rollup", false, &datasets);
        assert_eq!(
            ev["inputs"],
            json!([{ "namespace": "s3://raw", "name": "events/2026-10-03" }])
        );
        assert_eq!(
            ev["outputs"],
            json!([{ "namespace": "clickhouse://analytics", "name": "marts/daily_rollup" }])
        );
    }

    #[test]
    fn a_dataset_uri_splits_where_openlineage_expects() {
        let cases = [
            ("s3://bucket/a/b.parquet", ("s3://bucket", "a/b.parquet")),
            (
                "snowflake://analytics/marketing/fact_ad_spend",
                ("snowflake://analytics", "marketing/fact_ad_spend"),
            ),
            (
                "postgres://db:5432/public.orders",
                ("postgres://db:5432", "public.orders"),
            ),
            // No path: named whole, so it still identifies one dataset.
            ("kafka://orders", ("kafka://", "orders")),
            ("file:///data/x.csv", ("file://", "/data/x.csv")),
            // Not a URI at all: the job's namespace.
            ("orders_daily", ("dagron", "orders_daily")),
        ];
        for (uri, (ns, name)) in cases {
            assert_eq!(
                dataset_name("dagron", uri),
                (ns.to_string(), name.to_string()),
                "{uri}"
            );
        }
    }
}
