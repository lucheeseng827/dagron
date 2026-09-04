# dagron-artifact — artifact store seam for passing files between tasks

`dagron-artifact` is the **artifact store seam** of the dagron stack. It abstracts
*where* a run's data files live — so tasks in a run can pass files to one
another, so a checkpoint written on one machine can be resumed on another, and so
a unit on a metered link can capture now and upload later. The engine ships a
zero-infra local-filesystem store by default; a cloud backend, an encrypting
decorator and a tiered (local + deferred upload) decorator plug in behind the
same trait without touching the engine. The trait is also the programmatic
surface the management API uses to read/write artifacts by key.

## What it does

- **`ArtifactStore`** — the async trait every backend implements: `put` / `get` /
  `exists` / `run_location` / `list` / `put_stream` / `get_stream` /
  `rotate_from` / `sync`. `run_location` returns the task-visible directory a
  run's tasks share (or `None`); `sync` drains deferred work (the tiered store's
  uplink — a no-op everywhere else). `Box<T>` and `Arc<T>` are stores too
  (forwarding impls), so a type-erased store composes like any other.
- **`ArtifactKey`** — `run_id` / `task` / `name`. `rel_path()` is the sanitized
  `run_id/task/name` locator every backend keys by; `.` / `..` / empty components
  are rejected so a key can never escape the store root.
- **`LocalFsStore`** — the default backend, laid out at
  `base/<run_id>/<task>/<name>`. `from_env()` builds it from `DAGRON_ARTIFACT_DIR`;
  `prepare_run_dir()` creates the per-run shared directory the engine injects as
  `DAGRON_ARTIFACTS`; `list()` enumerates files at exactly that depth.
- **`cloud::CloudStore`** (features `s3` / `gcs` / `azure`) — S3 / GCS / Azure Blob
  behind the same trait, selected by `DAGRON_ARTIFACT_URL`. Same layout, streaming
  multipart uploads, credentials from the standard `AWS_*` / `GOOGLE_*` / `AZURE_*`
  environment. A URL whose backend feature is not compiled in is a hard error.
- **`EncryptedStore<S>`** — envelope encryption at rest over *any* store: a fresh
  per-object data key wrapped by a KEK provider from `dagron-crypto`, so the
  backend (disk or bucket) sees only ciphertext. `rotate_from` re-keys every
  object (data key rewrap only). `run_location` is `None` under encryption — a
  plaintext shared directory would defeat it.
- **`TieredStore<L, R>`** — a local tier that answers every write immediately plus
  a budgeted, deferred, deduplicated upload to a remote tier. Below.
- **`NoopStore`** — the unconfigured default. Every read/write errors loudly (a
  workflow that needs artifacts fails rather than silently losing data).

## Tiering — capture now, upload within budget

`TieredStore` is the capture → uplink primitive for a unit on a metered or
intermittent link (a robot, a vehicle, a plant cell). It is a decorator over two
ordinary stores — normally `LocalFsStore` under `CloudStore` — not new storage.

- **Writes** (`put` / `put_stream`) land on the **local** tier and return at once,
  ledgered `pending` with the SHA-256 of the bytes. Nothing touches the link.
- **`sync()`** — the trait method the management API's periodic loop
  (`DAGRON_ARTIFACT_SYNC_SECS`) and `POST /api/artifacts/sync` call — first scans
  the local tier for files tasks wrote directly into their run directory, then
  moves pending artifacts to the **remote** tier, oldest first, under the
  **uplink budget** (`UplinkBudget`: bytes per UTC day). An artifact that does not
  fit today waits for tomorrow; one larger than the whole daily budget is skipped
  with a warning rather than blocking everything behind it. A remote fault ends
  the drain with an error; what already moved stays ledgered and the next drain
  resumes. Returns how many artifacts left `pending`.
- **Dedup:** identical bytes already uploaded under another key are not
  transferred again — an `uploaded_by_sha` alias is ledgered and reads resolve to
  the canonical object, but only while that canonical is still `done` with the
  same hash. If the canonical is overwritten, an alias that still has a local
  copy is re-queued under its own key; one that has none is forgotten. A stale
  alias is never served another key's bytes.
- **Reads** (`get` / `get_stream` / `exists`) are local-first with remote
  fallback. When both tiers miss, the **local** `NotFound` error comes back
  unchanged — that is what the management API maps to `404`. A remote *fault* on
  that path is logged and treated as a miss. `run_location` and `list` are the
  local tier's, so tasks keep sharing the run directory exactly as before.
- **Ledger:** `<DAGRON_ARTIFACT_DIR>/.tiered/ledger.ndjson` — one JSON record per
  line (`pending` / `done` / `uploaded_by_sha` / `day` / `forget`), appended as
  things happen and compacted with an atomic temp-file + rename. It sits at depth
  2, where the local listing never reports it, and `.tiered` is therefore a
  reserved `run_id`. Replay is last-write-wins and tolerates a torn last line; a
  lost record costs at most one re-upload (remote puts are idempotent) or one
  re-scan. The ledger is a cache of *what has moved*, never the only copy of
  anything, so it is fsync'd only at compaction.
- **What the scan sees:** the local listing reports files at exactly
  `<run>/<task>/<name>`. The engine's per-task checkpoint directory
  `<run>/.checkpoints/<task>/…` is one level deeper and is **not** scanned; a file
  placed directly under `<run>/.checkpoints/` is reported as task `.checkpoints`
  and uplinked like any other artifact. Files are hashed and uplinked in whatever
  state they are in at scan time — write them atomically (temp + rename). A
  `done` entry is not re-hashed by later scans; `put` the key again to re-queue it.

This is the open, single-unit primitive: one local tier, one remote tier, a byte
budget, dedup within the unit. Managed transfer through the fleet plane with
resumable chunks and central dedup is not in this build
(<https://github.com/lucheeseng827/dagron#what-this-build-does-not-do>); the open build
drains straight to `DAGRON_ARTIFACT_URL` as described above.

### Composing with encryption

`store_from_env` wraps the tiered store in `EncryptedStore` when a KEK provider is
configured, so **both tiers hold ciphertext**. Two consequences:

- the tiered store hashes the bytes it *receives* — ciphertext under a fresh data
  key per object — so two identical plaintexts hash differently and dedup is
  effectively off under encryption (the price of never letting a storage tier
  see plaintext);
- a key rotation rewrites every object on the local tier (each becomes `pending`
  again and is re-uploaded), so the management API runs the drain and the
  rotation under one single-flight lock — a drain overlapping a re-key would
  upload objects still wrapped under the retiring KEK. Quiesce writes, rotate,
  then drain.

## Quickstart

```rust
use std::sync::Arc;
use dagron_artifact::{ArtifactKey, ArtifactStore, LocalFsStore, TieredStore, UplinkBudget};

// Built by the engine from DAGRON_ARTIFACT_DIR when set.
let local = LocalFsStore::new("/var/lib/dagron/artifacts");

// Per-run shared dir handed to tasks as DAGRON_ARTIFACTS.
let dir = local.prepare_run_dir("run-123").await?;

// Read/write by key from the API or an alternate build.
let key = ArtifactKey::new("run-123", "build", "out.txt");
local.put(&key, b"hello").await?;
let bytes = local.get(&key).await?;

// Tier it under a remote store (any ArtifactStore — here a type-erased one),
// 512 MiB of uplink per UTC day. The ledger lives under the local base.
let remote: Box<dyn ArtifactStore> = Box::new(some_remote_store);
let base = local.base().to_path_buf();
let tiered = TieredStore::new(local, remote, base, UplinkBudget::per_day(512 << 20));
tiered.put(&key, b"captured-now").await?;   // local only
let moved = tiered.sync().await?;           // uploads within budget, dedups by hash
```

In the engine, `LocalFsStore::from_env()` is called at startup; when it returns
`Some`, each dispatched task gets its run's shared dir via the `DAGRON_ARTIFACTS`
env var so tasks in the same run can exchange files. The management API builds
the programmatic store with `store_from_env()` (below), which is where tiering
and encryption are assembled.

## Config

Read by `store_from_env()` (the management API's programmatic store); the engine
reads only `DAGRON_ARTIFACT_DIR` / `DAGRON_ARTIFACT_URL` to compose task-visible
locations.

| Env | Purpose |
|-----|---------|
| `DAGRON_ARTIFACT_DIR` | Base directory for the local-filesystem store — the **local tier** under tiering. Unset/empty disables artifacts (`NoopStore`). |
| `DAGRON_ARTIFACT_URL` | `s3://` / `gs://` / `az://` bucket/prefix — the cloud store (needs the matching feature); the **remote tier** under tiering. Without tiering it takes precedence over the directory for programmatic reads/writes. |
| `DAGRON_ARTIFACT_TIER` | `1` opts into `TieredStore`; then **both** locations above are required and a build without a cloud backend feature refuses to start. Unset / `0` = off; the precedence rules above are unchanged. |
| `DAGRON_ARTIFACT_UPLINK_BYTES_PER_DAY` | Uplink budget for the drain, bytes per UTC day. Unset / `0` = unlimited; a malformed value is a startup error (silently meaning "unlimited" on a metered link is the surprise this knob exists to prevent). |
| `DAGRON_ARTIFACT_SYNC_SECS` | Read by the management API: period of the automatic drain (default `60`; `0` = drain only on `POST /api/artifacts/sync`). |
| `DAGRON_ENV_KEK_PROVIDER` (+ its provider variables) | Wraps whichever store results in `EncryptedStore` — see `dagron-crypto`. |

## Verification

```sh
cargo test --locked -p dagron-artifact                    # local, encrypted, tiered (in-memory remote double)
cargo test --locked -p dagron-artifact --features cloud   # + the cloud backend over an in-memory object store
```
