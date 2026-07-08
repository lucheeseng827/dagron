//! OpenLineage emitter — emits data-lineage events to an OpenLineage backend.
//!
//! On run finalization the engine calls [`OpenLineageClient::emit_run_completed`],
//! which POSTs an OpenLineage `RunEvent` (`COMPLETE` / `FAIL`) to a lineage backend
//! (Marquez, etc.). Best-effort: a lineage backend being down never affects run
//! execution. `runId` reuses dagron's run id (a UUID, as OpenLineage requires);
//! `job.name` is the workflow name.
//!
//! v0 emits the terminal event; `START` (and dataset inputs/outputs) are a
//! follow-up. Configure with `OPENLINEAGE_URL` (+ optional `OPENLINEAGE_NAMESPACE`,
//! default `dagron`).

use anyhow::{Context, Result};
use serde_json::{json, Value};

const PRODUCER: &str = "https://github.com/lucheeseng827/dagron";
const SCHEMA_URL: &str =
    "https://openlineage.io/spec/2-0-2/OpenLineage.json#/$defs/RunEvent";

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
        let url = std::env::var("OPENLINEAGE_URL").ok().filter(|u| !u.is_empty())?;
        let namespace =
            std::env::var("OPENLINEAGE_NAMESPACE").unwrap_or_else(|_| "dagron".to_string());
        Some(Self::new(url, namespace))
    }

    /// Emit the terminal RunEvent for a finished run. Best-effort.
    pub async fn emit_run_completed(&self, run_id: &str, job_name: &str, failed: bool) -> Result<()> {
        let event = build_event(&self.namespace, run_id, job_name, failed);
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
pub fn build_event(namespace: &str, run_id: &str, job_name: &str, failed: bool) -> Value {
    json!({
        "eventType": if failed { "FAIL" } else { "COMPLETE" },
        "eventTime": chrono::Utc::now().to_rfc3339(),
        "producer": PRODUCER,
        "schemaURL": SCHEMA_URL,
        "run": { "runId": run_id },
        "job": { "namespace": namespace, "name": job_name }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn complete_and_fail_events_are_well_formed() {
        let ok = build_event("dagron", "11111111-1111-4111-8111-111111111111", "etl", false);
        assert_eq!(ok["eventType"], "COMPLETE");
        assert_eq!(ok["run"]["runId"], "11111111-1111-4111-8111-111111111111");
        assert_eq!(ok["job"]["namespace"], "dagron");
        assert_eq!(ok["job"]["name"], "etl");
        assert_eq!(ok["producer"], PRODUCER);
        assert!(ok["eventTime"].as_str().unwrap().contains('T'));

        let bad = build_event("dagron", "r2", "etl", true);
        assert_eq!(bad["eventType"], "FAIL");
    }
}
