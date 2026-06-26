# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project adheres to
[Semantic Versioning](https://semver.org/) (pre-1.0: minor = breaking).

## [Unreleased]

### Added
- The Python and TypeScript SDKs (`sdks/`) now ship in the OSS distribution, so
  the `examples/sdk/` scripts resolve against the bundled SDK out of the box.
- Runnable SDK examples under `examples/sdk/` (Python + TypeScript) that drive a
  live `dagron-api`: quickstart, workflow+schedule, live SSE streaming, and
  cascade-rerun recovery, with a README covering setup and env config.
- Initial open-source cut of the dagron engine.

### Fixed
- **TypeScript SDK `Dag.submit()`** posted the raw spec to `POST /api/runs`; the
  gateway expects `{"yaml": "<spec>"}` and rejected it with `422 missing field
  yaml`. It now wraps the spec and returns the parsed `run_id` (`@dagron/sdk`
  0.1.0 → 0.1.1). Mirrors the Python SDK's v0.2 fix.
- `Executor` trait + `LocalExecutor` (subprocess) reference backend.
- `WorkflowSource` trait + `FileSource` and `ChannelSource` reference sources.
- In-memory `run_dag` scheduler: dependency-driven concurrency, retries with
  exponential backoff, and downstream skip-on-failure.
- `dagron run <file.yaml>` CLI and a bundled example DAG.
