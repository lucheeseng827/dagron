# dagron-artifact — artifact store seam for passing files between tasks

`dagron-artifact` is the **artifact store seam** of the dagron stack. It abstracts
*where* a run's data files live so tasks in a run can pass files to one another.
The engine ships a zero-infra local-filesystem store by default; an S3/GCS backend
plugs in behind the same trait without touching the engine. The trait is also the
programmatic surface the API and alternate builds use to read/write artifacts by key.

## What it does

- **`ArtifactStore`** — the async trait (`put` / `get` / `exists` / `run_location`)
  that every backend implements. `run_location` returns the task-visible directory
  a run's tasks share, or `None` for stores with no such location.
- **`ArtifactKey`** — identifies one artifact by `run_id` / `task` / `name`. Its
  `rel_path()` produces a sanitized `run_id/task/name` locator; `sanitize` keeps only
  path-safe characters and rejects `.` / `..` / empty components to block traversal.
- **`LocalFsStore`** — the default filesystem backend rooted at a base dir; lays
  artifacts out at `base/<run_id>/<task>/<name>`. `from_env()` builds it from
  `DAGRON_ARTIFACT_DIR`; `prepare_run_dir()` creates and returns the per-run shared
  directory the engine injects as `DAGRON_ARTIFACTS`.
- **`NoopStore`** — the unconfigured default. Every read/write errors loudly (a
  workflow that needs artifacts fails rather than silently losing data) and it
  advertises no task-visible location.

## Quickstart

```rust
use dagron_artifact::{ArtifactStore, ArtifactKey, LocalFsStore};

// Built by the engine from DAGRON_ARTIFACT_DIR when set.
let store = LocalFsStore::new("/var/lib/dagron/artifacts");

// Per-run shared dir handed to tasks as DAGRON_ARTIFACTS.
let dir = store.prepare_run_dir("run-123").await?;

// Read/write by key from the API or an alternate build.
let key = ArtifactKey::new("run-123", "build", "out.txt");
store.put(&key, b"hello").await?;
let bytes = store.get(&key).await?;
```

In the engine, `LocalFsStore::from_env()` is called at startup; when it returns
`Some`, each dispatched task gets its run's shared dir via the `DAGRON_ARTIFACTS`
env var so tasks in the same run can exchange files.

## Config

| Env | Purpose |
|-----|---------|
| `DAGRON_ARTIFACT_DIR` | Base directory for the local-filesystem store. Unset/empty disables artifacts (`NoopStore`). |
