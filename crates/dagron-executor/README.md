# dagron-executor — task executors and the worker pool

`dagron-executor` defines *how* a claimed task runs. It provides the `Executor`
trait and its Local / Docker / Kubernetes backends, plus the ractor worker pool that
dispatches claimed tasks to the configured executor and reports results back to the
engine's reconcile loop. It borrows core types from `dagron-core` but has no
knowledge of scheduling or ingestion.

## What it does

- **`executor`** — the `Executor` trait and the shared `ExecContext` /
  `ExecOutput` types, plus the always-available `LocalExecutor` that runs each task
  as a subprocess. `LogChunk` carries incremental output for live tailing.
- **`docker_executor`** — `DockerExecutor`, running each task in a container over a
  Docker / podman socket (`bollard`).
- **`kube_executor`** — `KubeExecutor`, running each task as a Kubernetes pod. Behind
  the `kubernetes` feature (pure-Rust client; compiles without a cluster).
- **`worker`** — the ractor `WorkerPool` that dispatches `DispatchPayload`s to the
  executor, records per-task durations into the core `Metrics` registry, and returns
  each `TaskResult` to the reconcile loop.
- **`secrets`** — resolves `value_from` env references into concrete values just
  before dispatch. **`redact`** — scrubbing helpers for sensitive output.
- **`install_crypto_provider()`** — installs the process-wide rustls `CryptoProvider`
  the kube client needs before opening TLS (feature `kubernetes`).

## Feature flags

| Feature | Effect |
|---------|--------|
| `kubernetes` | Enables `kube_executor` (`EXECUTOR=kubernetes`); pulls in `kube` / `k8s-openapi` / `rustls`. |

## Quickstart

Library only — the engine wires it together. Pick an executor and build a pool:

```rust
use std::sync::Arc;
use dagron_executor::{executor::{Executor, LocalExecutor}, worker::WorkerPool};
use dagron_core::metrics::Metrics;

let executor: Arc<dyn Executor> = Arc::new(LocalExecutor);
let metrics = Arc::new(Metrics::new());
let workers = WorkerPool::new(16, executor, metrics).await?;

// The reconcile loop dispatches claimed tasks and collects TaskResults:
workers.dispatch(payload)?;
```

The engine selects the backend from `EXECUTOR` (`local` / `docker` / `kubernetes`)
and, for the Kubernetes build, calls `install_crypto_provider()` once at startup.
