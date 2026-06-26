# dagron

A small, durable **DAG workflow runner**. Define a workflow as a graph of tasks
in YAML; dagron validates it, then runs each task as soon as its dependencies
succeed — concurrently, with retries and exponential backoff. Single static
binary, zero infrastructure to get started.

[![License: Apache-2.0](https://img.shields.io/badge/License-Apache_2.0-blue.svg)](LICENSE)

## See it in action

The optional web UI (the `dagron-api` gateway + Next.js frontend, brought up with
`docker compose up`) gives you a live view over the same engine the CLI drives —
submit workflows, watch runs stream, inspect the DAG, and read task logs.

| Overview — scheduler health, runs today, success rate, GitOps sync | Run detail — the live DAG graph with per-task status + output |
|---|---|
| [![Overview dashboard](docs/images/overview.png)](docs/images/overview.png) | [![Run DAG graph](docs/images/run-graph.png)](docs/images/run-graph.png) |

| Workflows — saved definitions, schedules, recent-run history | Runs — every execution across all workflows | Metrics — live run/task counts by status |
|---|---|---|
| [![Workflows list](docs/images/workflows.png)](docs/images/workflows.png) | [![Runs list](docs/images/runs.png)](docs/images/runs.png) | [![Metrics](docs/images/metrics.png)](docs/images/metrics.png) |

## Why dagron

- **Lightweight** — a Rust binary, no Python/Celery/etc. to operate.
- **Declarative & GitOps-friendly** — workflows are plain YAML you can version.
- **Pluggable** — two small traits are the whole extension surface:
  - [`Executor`](src/executor.rs) — *how a task runs* (ships `LocalExecutor`, a
    subprocess backend).
  - [`WorkflowSource`](src/source.rs) — *where workflows come from* (ships
    `FileSource` and an in-process `ChannelSource`).
- **Runs anywhere** — no daemon or database required for the local runner.

## Quick start

```bash
# build
cargo build --release

# run the bundled example
./target/release/dagron run examples/simple_dag.yaml
```

Output:

```text
DAG 'simple_dag': SUCCEEDED
  ✓ a                        succeeded (attempts: 1)
  ✓ b                        succeeded (attempts: 1)
  ✓ c                        succeeded (attempts: 1)
  ✓ d                        succeeded (attempts: 1)
```

A task that exits non-zero is retried up to `max_attempts` with
`retry_delay_secs * 2^(attempt-1)` backoff; if it still fails, its downstream
tasks are skipped and the run exits non-zero.

## Workflow format

```yaml
name: my_workflow
tasks:
  - name: build
    command: ["cargo", "build"]
  - name: test
    command: ["cargo", "test"]
    depends_on: ["build"]
    max_attempts: 3        # default 1 (no retries)
    retry_delay_secs: 2    # base backoff; default 0 (immediate)
    timeout_secs: 600      # per-task; default 25s
```

The runner rejects duplicate task names, unknown dependencies, and cycles before
running anything.

## Use it as a library

```rust
use std::sync::Arc;
use dagron::{run_dag, LocalExecutor};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let yaml = std::fs::read_to_string("examples/simple_dag.yaml")?;
    let report = run_dag(&yaml, Arc::new(LocalExecutor)).await?;
    println!("succeeded = {}", report.succeeded);
    Ok(())
}
```

Implement `Executor` to run tasks on a different substrate (containers, remote
workers) without changing the runner.

## Editions

dagron is **open core**. This repository is the engine, under **Apache-2.0**, and
is the same code that runs in production. A separate commercial distribution adds
the multi-tenant operations layer (single sign-on, role-based access, long-term
run history, audit, and a fully managed hosted service) for teams that want those
operated for them. You can run the open-source engine, on your own hardware,
indefinitely and for free.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md). Contributions are accepted under the
Developer Certificate of Origin (a `Signed-off-by` line, `git commit -s`).
Security reports: see [SECURITY.md](SECURITY.md).

## License

Licensed under the Apache License, Version 2.0. See [LICENSE](LICENSE) and
[NOTICE](NOTICE).
