# dagron-lineage — OpenLineage emitter

`dagron-lineage` emits data-lineage events to an **OpenLineage** backend
(Marquez, etc.). On run finalization the engine calls
[`OpenLineageClient::emit_run_completed`], which POSTs an OpenLineage `RunEvent`
(`COMPLETE` / `FAIL`) to the configured backend. It is **best-effort**: a lineage
backend being down never affects run execution. `runId` reuses dagron's run id (a
UUID, as OpenLineage requires) and `job.name` is the workflow name.

v0 emits only the terminal event; `START` and dataset inputs/outputs are a
follow-up.

## What it does

- `OpenLineageClient` — posts RunEvents to `{url}/api/v1/lineage`.
  - `OpenLineageClient::new(url, namespace)` — explicit construction.
  - `OpenLineageClient::from_env()` — returns `None` when `OPENLINEAGE_URL` is
    unset (feature off), so the engine skips the call.
  - `emit_run_completed(run_id, job_name, failed)` — emits the terminal RunEvent;
    a non-2xx response is returned as an error for the caller to log.
- `build_event(namespace, run_id, job_name, failed)` — builds the OpenLineage
  `RunEvent` JSON (`COMPLETE` when `failed` is false, else `FAIL`), stamped with
  the dagron producer URL, spec `schemaURL`, and an RFC3339 `eventTime`.

## Quickstart

Wired into the engine's run-finalization path:

```rust
if let Some(client) = dagron_lineage::OpenLineageClient::from_env() {
    // best-effort: log, don't fail the run
    let _ = client.emit_run_completed(&run_id, &workflow_name, run_failed).await;
}
```

## Config

A client is returned from `from_env()` only if `OPENLINEAGE_URL` is set.

| Env | Purpose |
|-----|---------|
| `OPENLINEAGE_URL` | OpenLineage backend base URL (required; events POST to `{url}/api/v1/lineage`) |
| `OPENLINEAGE_NAMESPACE` | Job namespace (default `dagron`) |
