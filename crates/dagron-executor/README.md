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
- **`pool`** — what an `EXECUTOR=local` pool permits: which programs a task may
  run (`DAGRON_LOCAL_COMMAND_ALLOWLIST`) and which of the pool's own environment
  variables it may see (`DAGRON_LOCAL_ENV_PASSTHROUGH`). Both opt-in — unset, a
  local pool behaves exactly as it always has. They exist because a local pool
  runs its tasks as subprocesses of the engine, so `command:` comes from the
  workflow and `Command` inherits the parent environment: on such a pool a spec
  author could run any program the pool's user can and read every variable it
  holds, the datastore DSN included. The allowlist matches a **resolved file**
  rather than the word a task wrote, since a comparison on `command[0]` is
  defeated by a planted program on a task-supplied `PATH`. See the module doc
  for the measurements.
- **`secrets`** — resolves `value_from` env references into concrete values just
  before dispatch. **`redact`** — scrubbing helpers for sensitive output. Note
  what redaction is *not*: it masks values in stored output, and cannot stop a
  task reading a value it was handed. `pool` is the module that stops the
  reading.
- **`install_crypto_provider()`** — installs the process-wide rustls `CryptoProvider`
  the kube client needs before opening TLS (feature `kubernetes`).

## Architecture

One trait, three backends, and a policy gate only the local one passes
through. The engine never names a backend: it hands the pool a `DispatchPayload`
and the pool calls whichever `Executor` was configured at startup.

```mermaid
flowchart TD
    ENG[dagron-engine<br/>reconcile loop] -->|DispatchPayload| WP[worker::WorkerPool<br/>ractor actors, N workers]
    WP -->|ExecContext| EX{{Executor trait}}

    EX --> LOC[LocalExecutor<br/>subprocess]
    EX --> DOC[DockerExecutor<br/>container over the socket]
    EX --> KUB[KubeExecutor<br/>pod, feature kubernetes]

    SEC[secrets<br/>resolve value_from] -->|concrete env| WP
    LOC --> BC[executor::build_command]
    BC --> POL[pool::policy<br/>DAGRON_LOCAL_COMMAND_ALLOWLIST<br/>DAGRON_LOCAL_ENV_PASSTHROUGH]
    POL -->|resolved path + filtered env| PROC[std::process::Command]

    LOC --> RES[ExecOutput + LogChunk]
    DOC --> RES
    KUB --> RES
    RES --> RED[redact<br/>mask values in stored output]
    RED -->|TaskResult| ENG

    POL -. "the gate is on the LOCAL path only:<br/>a container or a pod already has<br/>its own boundary" .-> LOC
```

Note where the policy sits. `redact` masks values on the way *out* and cannot
stop a task reading what it was handed; `pool` is the module that stops the
reading, and it is only meaningful for `LocalExecutor` because that is the
backend with no boundary of its own.

## Event flow

One claimed task, on a hardened local pool. The refusal path is the interesting
one: it happens before the process exists, and it is a *configuration* fault, so
the engine does not retry it.

```mermaid
sequenceDiagram
    participant Engine as reconcile loop
    participant Pool as WorkerPool
    participant Sec as secrets
    participant Exec as LocalExecutor
    participant Policy as pool::policy
    participant Proc as subprocess

    Engine->>Pool: dispatch(DispatchPayload)
    Pool->>Sec: resolve value_from refs
    Sec-->>Pool: concrete env
    Pool->>Exec: run(ExecContext)
    Exec->>Policy: build_command(command, env)
    Note over Policy: argv[0] is RESOLVED against PATH<br/>and canonicalised, then matched --<br/>not compared as the word written

    alt allowlisted
        Policy-->>Exec: Command with env_clear + passthrough
        Exec->>Proc: spawn
        Proc-->>Exec: stdout/stderr as LogChunks
        Exec-->>Pool: ExecOutput
    else refused
        Policy-->>Exec: Err(dagron_local_command_allowlist)
        Note over Exec,Engine: FaultClass::Config -> budget 1,<br/>so a refusal is never retried
        Exec-->>Pool: Err
    end

    Pool-->>Engine: TaskResult (redacted)
```

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
