# The edge profile — dagron on a constrained host

This is the due-diligence sheet for running **one dagron engine on a box that
is not a server**: a gateway, a robot, a vehicle, a kiosk. A few cores, a flash
device instead of a disk, a clock that may be wrong at boot, an uplink that
comes and goes. Everything an embedded engineer checks before adopting a
daemon is on this page — what `profile: edge` sets, the two gates that keep a
constrained host from cooking itself or filling its disk, what SQLite does on
flash when the power goes, how a run records the confidence of the clock it
was stamped under, and, measured on one host with the exact commands, how big
the binary is and how much memory an idle daemon holds.

Companion pages: [`CONFIG.md`](CONFIG.md) (every knob, the `DAGRON_CONFIG`
file format), [`OPERATIONS.md`](OPERATIONS.md) (backup, troubleshooting),
[`STREAMING.md`](STREAMING.md) (`SOURCE=mqtt`, the broker adapter a plant
floor or a robot talks), [`BUNDLES.md`](BUNDLES.md) (signed workflow bundles,
the OTA channel for the workflow layer).

**Scope.** The free engine, on one unit. It is the same binary and the same
datastore as everywhere else — the profile tunes it, it does not fork it.
Operating *many* units together — enrolment, fan-out to a cohort, staged bundle
rollout with automatic rollback, the store-and-forward link that carries work
down and results up through a metered uplink — is not in this build
. This build runs
one unit and hands it work through `SOURCE=dir`, `SOURCE=stream`, `SOURCE=mqtt`
or GitOps sync.

## 1. What `profile: edge` sets

```yaml
# /etc/dagron/dagron.yaml   (DAGRON_CONFIG=/etc/dagron/dagron.yaml)
profile: edge
```

That is the whole file. Precedence is unchanged — **environment → file →
profile → compiled default** — so a container or systemd override of any
single knob still wins, and the preset is what you get for the ones you do not
mention. `dagron config` prints every knob with its source; the six below show
`profile` and everything else `default` (the real output, cut to those lines):

```text
$ DAGRON_CONFIG=/etc/dagron/dagron.yaml dagron config
# dagron effective configuration
# config file: /etc/dagron/dagron.yaml
# profile: edge
DAGRON_MIN_FREE_BYTES                 [profile]  67108864
MAX_INFLIGHT_RUNS                     [profile]  4
MAX_INFLIGHT_TASKS                    [profile]  64
POLL_INTERVAL_MS                      [profile]  1000
SWEEP_INTERVAL_MS                     [profile]  5000
WORKER_COUNT                          [profile]  2
```

| Knob | `edge` | Stock default | Why the profile moves it |
| --- | --- | --- | --- |
| `WORKER_COUNT` | `2` | `16` | Worker slots are the number of task processes running at once. Sixteen subprocesses on a two- or four-core board starve the thing the board exists for (the control loop, the sensor pipeline). Two keeps a chain moving while leaving the CPU to its owner. |
| `POLL_INTERVAL_MS` | `1000` | `500` | The reconcile timer. On SQLite it is the only timer-based wake (there is no `LISTEN/NOTIFY`), so halving the tick rate halves the idle wake-ups and the claim queries they run; the price is up to half a second more before a newly scheduled retry or parked sensor is noticed. A gateway does not care; a trading desk would, and has its own profile. |
| `SWEEP_INTERVAL_MS` | `5000` | `= POLL_INTERVAL_MS` | The maintenance sweeps (expired-lease recovery, run deadlines, SLA alerts, approval expiry, sensor and dataset reconciliation) run every five seconds instead of on every tick. On a single unit there is no peer whose crashed tasks need reclaiming quickly — the only crashed engine is this one, and it reclaims its own on restart — so a slower sweep costs nothing that matters. |
| `MAX_INFLIGHT_RUNS` | `4` | `64` | The admission valve. Above it, `POST /runs` answers `429` + `Retry-After` and the ingest actor stops pulling from its source; overflow stays *at the source* — in the broker, the drop-box directory, the stream file — rather than in the datastore. Four runs is what two workers can service without a queue of leased-but-idle tasks building up on flash. |
| `MAX_INFLIGHT_TASKS` | `64` | `0` (off) | The second admission dimension, on tasks: a run of ten thousand tasks and a run of four both count as one run. Off by default because it costs a `task_runs` count per admission; at four runs that query is nothing, and the cap is what stops one wide matrix from sizing the datastore for the board. |
| `DAGRON_MIN_FREE_BYTES` | `67108864` (64 MiB) | `0` (off) | The free-disk floor, §2.2. Sixteen times the largest WAL the default checkpoint cadence allows (§3), so the engine refuses *new* runs once free space falls below the margin. It is a floor checked at `create_run`, **not a reservation**: nothing holds 64 MiB aside for an accepted run, and a running workload can still exhaust the remainder and hit `ENOSPC`. What it buys is that admission stops before the datastore is the thing that fails. |

What the profile deliberately leaves alone: `LEASE_SECS` (30 s — a lease only
matters after a crash, and a unit restarting after a power cut is not waiting
on a peer), `EXECUTOR` (`local`; the tasks are the board's own binaries), the
pressure file (§2.1 — the *path* is the host's decision, and a preset cannot
know it), and everything about the API (`dagron dev` still binds
`127.0.0.1:8787`; a unit that must not listen at all leaves `API_ADDR` unset
and runs a lean build, §5b).

## 2. The constrained-host gates

Two knobs, both off by default, both meant to be driven by something that
already knows the state of the board. Neither is a scheduler policy: they are
the places where a host's physical limits reach the engine.

### 2.1 Pressure file — `DAGRON_PRESSURE_FILE`

The engine never reads thermal zones, battery gauges or maintenance windows
itself; every board exposes those differently, and a daemon that does know —
`thermald`, a battery-management agent, the maintenance script that is about
to swap a drive — expresses it by writing **one file**.

| Condition | Effect |
| --- | --- |
| `DAGRON_PRESSURE_FILE` unset | No gate. The knob is registered (it shows in `dagron config`) but nothing is checked. |
| Path set, file **absent** | Open — claims proceed. |
| Path set, file **exists** | Closed — the reconcile tick's claim step runs with capacity **0**. No new task is started. Tasks already running continue; their leases keep heartbeating; ingestion keeps admitting runs up to `MAX_INFLIGHT_*`, so work queues in the datastore rather than being refused. |
| File exists with content `0`, `false`, `off` or `resume` | Open again — a daemon can flip the gate without deleting the file it owns. Any other content (including empty) means closed. |

The gate is re-read every tick, so it takes effect within one
`POLL_INTERVAL_MS`. The transition is logged **once** — a warning on
open → closed, an info on closed → open — never per tick, so a board that sits
hot for an hour produces two log lines, not three thousand. The gauge
`scheduler_claims_paused` (0/1) is the same fact for a scraper.

What it does *not* pause, on purpose: the maintenance sweeps (a lease that
expired while the board was hot should still be recovered), lease heartbeats,
the API, and ingestion. A pause is "start nothing new", not "stop".

```sh
# A thermal guard in ten lines. Hysteresis is the caller's — the engine only
# reads presence. Run from cron or a systemd timer.
zone=/sys/class/thermal/thermal_zone0/temp
t=$(cat "$zone")                          # millidegrees
if   [ "$t" -gt 80000 ]; then touch /run/dagron/pressure
elif [ "$t" -lt 70000 ]; then rm -f /run/dagron/pressure
fi
```

with `DAGRON_PRESSURE_FILE=/run/dagron/pressure` in the engine's environment.
The same shape covers a battery floor, a "charging only" policy, or a
maintenance window; anything that can `touch` a file can drive it.

### 2.2 Free-disk floor — `DAGRON_MIN_FREE_BYTES`

A full flash device is the failure a unit *will* have, and SQLite's behaviour
on `ENOSPC` is correct but unhelpful: the transaction that cannot write rolls
back, and so does every transaction after it — including the ones that would
record a running task's result. The floor stops **new** work while the disk can
still absorb the old work finishing.

| Rule | Detail |
| --- | --- |
| Where | The SQLite backend only, on run creation (`create_run`, and the same path the API's redrive uses). Postgres is a no-op: the database server's disk is not the unit's. |
| What is measured | Free bytes (`statvfs`) of the filesystem that holds **the datastore file itself** — derived from the pool's own path, never from the working directory, so a unit whose `workflow.db` lives on a different partition from `/` measures the right one. |
| When it trips | `free < DAGRON_MIN_FREE_BYTES`. `0` disables. |
| What refuses | A typed error carrying `{free, floor}`. The ops API answers `POST /runs` and redrive with **`507 Insufficient Storage`** and `Retry-After: 1`. The ingest actor **nacks** the message back to its source and throttles — it is not dead-lettered, because a full disk is not the message's fault, and the message must be there when space returns. |
| Counter | `admission_refused_disk_total`. |
| If the probe itself fails | **Fail open**, with one warning. A `statvfs` that errors (an exotic filesystem, a container with a masked mount) must never brick a unit that has plenty of room. |

What fills the disk on a unit, in the order it usually happens: task output
(stored in the datastore, bounded by `GC_RETENTION_SECS` — see
[`CONFIG.md`](CONFIG.md)), artifacts under `DAGRON_ARTIFACT_DIR`, and the WAL
(§3, bounded by the checkpoint cadence). Set the floor above the sum of one
checkpoint's worth of WAL plus the largest artifact a single run writes, and
put `workflow.db` on the same filesystem you intend the floor to guard.

## 3. SQLite on flash — durability and wear

The datastore is one file, opened by
[`crates/dagron-core/src/db/sqlite.rs`](../crates/dagron-core/src/db/sqlite.rs)
`init_pool`. These are the pragmas a pool opened that way actually runs with —
read back through `PRAGMA` on the open connection, not copied from the connect
options (SQLite 3.46.0, bundled by `libsqlite3-sys` 0.30.1 and pinned by
`Cargo.lock`; no system SQLite is involved on any platform):

| Pragma | Value | Meaning for a flash device |
| --- | --- | --- |
| `journal_mode` | `wal` | Commits append to `workflow.db-wal`; the main file is rewritten only at checkpoints. Readers never block the writer. |
| `synchronous` | `2` (FULL) | The WAL is `fsync`ed **before every commit returns**. SQLite's own default; `init_pool` does not lower it. |
| `busy_timeout` | `5000` | An outside reader (`sqlite3 workflow.db`, a probe) waits up to 5 s for a lock rather than failing with `SQLITE_BUSY`. |
| `foreign_keys` | `1` | Referential integrity is enforced by the engine, not trusted to the application. |
| `wal_autocheckpoint` | `1000` | Once the WAL passes 1000 pages the next commit folds it back into `workflow.db`. |
| `page_size` | `4096` | One page = one 4 KiB write. 1000 pages ≈ 4 MiB of WAL before an automatic checkpoint. |
| `journal_size_limit` | `-1` | The WAL file is **reused, not truncated**, after a checkpoint. A `-wal` of a few MiB sitting on disk is steady state, not growth. |
| `cache_size` | `-2000` | 2 MiB page cache per connection (there is one connection). |
| `locking_mode` / `mmap_size` / `temp_store` / `auto_vacuum` | `normal` / `0` / `0` / `0` | SQLite defaults. No memory-mapped I/O; temp tables follow the compile default; the file does not shrink on its own. |

Pool size is **1**: SQLite is single-writer, and `claim_ready` reads then
writes inside one deferred transaction, which a second pooled connection could
race for the write lock and lose *instantly* (a busy timeout cannot rescue a
lock upgrade). One daemon per file; a second process on the same file is a
reader, and `OPERATIONS.md` says what to do when that goes wrong.

**Power loss during a commit.** A commit appends frames to the WAL and
`fsync`s it; `create_run`, `mark_*` and the offset commit of a streaming source
do not return until that sync has. Every WAL frame carries a checksum chained
from the WAL header's salt. If the power goes mid-append, the tail of the WAL
is torn, its checksum fails, and on the next open SQLite ignores that frame and
everything after it: **the datastore reopens at the last commit that
returned.** There is no half-created run and no run whose source offset was
committed without it — they are one transaction (`create_run_with_offset`).
This is SQLite's documented WAL recovery, not a dagron invention; what dagron
contributes is not weakening it (`synchronous=NORMAL` in WAL mode is safe
against corruption but lets the last commits before a cut vanish, which for a
run acknowledged to a broker is a lost message).

**The caveat that is not SQLite's to fix.** `FULL` asks the kernel to flush;
a flash controller with a volatile write cache that acknowledges the flush
before the cells are programmed defeats every database on the device, not just
this one. Consumer SD cards do this. For a unit that must keep its promises
across a hard power cut, use industrial eMMC or a pSLC partition, keep the
filesystem's write barriers on (the ext4 default — never `nobarrier`), and
test it with the plug, not with `kill -9`. A container with the network
unplugged tests none of this.

**Write amplification, per commit.** At least one 4 KiB page plus a 24-byte
WAL frame header, and one `fsync`. The `-shm` file is a memory-mapped index
and is not synced. A checkpoint rewrites every dirty page into the main file
and syncs that once. The reconcile tick itself does not write — a tick with
nothing to claim is a read — but the **default build is not silent while
idle**: its leadership lease (the `ops` surface's cron/GC/schedule
singleton) is renewed by a commit every ⌊`LEADER_LEASE_SECS`/2⌋ = 15 s, which
§5c measures at two WAL frames and ~20 KiB of device writes per 30 s. Raise
`LEADER_LEASE_SECS` on a unit that has no peer, or run the lean build (§5b),
which has no leadership. Beyond that, the write load on the device is the run
load — one commit per task state change, batched inside a tick — not a
function of uptime.

**After a crash.** The datastore is the truth. A task that was running when the
engine died holds a lease (`LEASE_SECS`, 30 s by default); when the engine
restarts, the lapsed lease is reclaimed from the row alone and the task runs
again — **at-least-once** after a crash — while the row's version fence keeps a
stale attempt that somehow survived from writing over the new one. Nothing is
stranded, nothing needs an operator. This is the same mechanism a multi-node
Postgres deployment recovers a dead peer with; on one unit the dead peer is
always the previous incarnation of itself.

**Backups** on a live file: `sqlite3 workflow.db ".backup backup.db"`. Never
copy `workflow.db` alone from a running daemon — the `-wal` sidecar holds
committed data the main file does not yet — see [`OPERATIONS.md`](OPERATIONS.md)
and [`BACKUP_RECOVERY.md`](BACKUP_RECOVERY.md).

## 4. Clock discipline

A unit that boots with no RTC battery and no network starts in 1970, or at
whatever time its last shutdown wrote; a unit whose GPS fix arrives after the
first run steps its clock by years. Leases, schedules and the `created_at` a
regulator reads are all computed from the process's wall clock. The engine
cannot make that clock right; it can refuse to pretend it is.

**Every run records the confidence of the clock it was created under.** Three
columns on the run, returned by the engine's `GET /runs/{id}`:

| Field | Values | Meaning |
| --- | --- | --- |
| `clock_confidence` | `synced` · `drifted` · `unknown` | `unknown` is the default and the honest answer when there is no evidence either way. |
| `clock_offset_ms` | integer, nullable | The step the detector saw, when it saw one. |
| `clock_source` | `sync-file` · `behind-datastore` · `step` · null | What the verdict is based on. |

**How the verdict is reached** (the detector starts right after the datastore
opens):

| Evidence | Verdict |
| --- | --- |
| At boot, `now` is **earlier than the newest run the datastore already holds** | `drifted`, source `behind-datastore`. The datastore cannot be from the future; the clock is behind. |
| Every `DAGRON_CLOCK_CHECK_SECS` (30; `0` disables), the wall clock advanced by more than `DAGRON_CLOCK_STEP_TOLERANCE_MS` (1000) relative to the monotonic clock | `drifted`, source `step`, offset recorded. Runs **currently running** are re-stamped `drifted` too — they were in flight across the step. |
| `DAGRON_CLOCK_SYNC_FILE` names a file that exists (rechecked every check) | `synced`, source `sync-file`. Positive evidence only: a `chrony`/`systemd-timesyncd` hook, or the GPS daemon, touches the file on sync and removes it on loss. The engine never infers sync from silence. |

**What the confidence does not do:** gate recovery. A drifted clock marks
records; it never stops leases from being reclaimed or runs from being
executed, because a unit whose clock is wrong still has work to do and the
alternative — an engine that refuses to run until NTP appears — is a unit that
never runs. Lease arithmetic is also derived from a **single read** of `now`
per claim (the `scheduled_at <= now` gate and the lease expiry come from the
same timestamp), so a step landing between two reads cannot mint a lease that
is already expired.

Metrics: `scheduler_clock_confidence` (a gauge carrying the current state) and
`scheduler_clock_steps_total`. Reconciling records stamped under an unsynced
clock against a central timeline when the unit reconnects is the fleet link's
job and is not in this build; in this build the three fields are on
the run for your own pipeline to act on.

## 5. Measurements

Every number below was taken on **one** host with the commands shown, and is
that host's. What it is *not*: a figure for a different feature set (see 5b), or
a measurement of real flash or real power cuts (§3 says why a container cannot
provide one). Re-measure on the board you ship.

> **arm64, under load, is measured separately.**
> [`RASPBERRY_PI.md`](RASPBERRY_PI.md) runs the whole quickstart stack — engine,
> API and Postgres — on a Raspberry Pi 4B and attaches a ceiling: sustained
> **4 runs/s (~40 tasks/s)** with nothing shed, shedding from **~5.5 runs/s**,
> and a degraded floor of **~3.3 runs/s** under overload with zero failures.
> Peak memory ~340 MB of 3.7 GB. The binding resource there is Postgres CPU and
> disk I/O, not the engine. This page is one engine on a constrained host; that
> page is the full stack with a database under it.

**Host and toolchain.** x86_64 (Intel Xeon @ 2.80 GHz), a 4-vCPU container,
16 GB RAM, no swap, Linux 6.18. `rustc 1.94.1 (e408947bf 2026-03-25)`,
`cargo 1.94.1`. Release profile = Cargo's default (the workspace declares no
`[profile.release]`: `opt-level = 3`, no LTO, `codegen-units = 16`, no
debuginfo, symbols **not** stripped — hence the two sizes in 5a).

**The caveat on the sizes.** The binary measured here is a native
`x86_64-unknown-linux-gnu` build — dynamically linked against the host's glibc
— because that is what `cargo build --release` produces on this host. The
release assets are **static musl** binaries (`x86_64-unknown-linux-musl`,
`aarch64-unknown-linux-musl`, `armv7-unknown-linux-musleabihf`), which carry
their own libc and allocator and are a different size. Same code, different
link; the number to quote for an asset is the asset's.

### 5a. Binary size — default build (`sqlite` + `ops`)

```sh
cargo build --release --locked -p dagron
stat -c %s target/release/dagron                     # unstripped
cp target/release/dagron /tmp/dagron && strip /tmp/dagron && stat -c %s /tmp/dagron
```

| Binary (`x86_64-unknown-linux-gnu`, this host) | Bytes | |
| --- | ---: | --- |
| `target/release/dagron`, as built | **25,601,816** | 24.4 MiB |
| after `strip` | **20,781,600** | 19.8 MiB |

`file` reports `ELF 64-bit LSB pie executable, x86-64, dynamically linked,
interpreter /lib64/ld-linux-x86-64.so.2`; `ldd` lists `libc.so.6`, `libm.so.6`
and `libgcc_s.so.1` and nothing else — no OpenSSL, no system SQLite. A cold
release build (no cached release artifacts) took **2 m 55 s** wall on the four
vCPUs (10 m 04 s user). The binary that was measured is the one that then
passed `dagron validate examples/simple_dag.yaml` (`OK (4 tasks)`), which is
the same smoke test the release workflow runs.

The ~4.6 MiB that `strip` removes is the symbol table (the release profile
emits no debuginfo but keeps symbols). The release workflow does **not** strip,
so a downloaded asset carries symbols and a panic backtrace names functions;
strip it yourself on a unit where 5 MiB of flash matters more.

### 5b. Binary size — lean build (`--no-default-features --features sqlite`)

The reconcile-only daemon the root `Cargo.toml` describes: no management API,
no cron, no retention GC, no leadership — a unit that takes work from
`SOURCE=dir`/`stream`/`mqtt` and never listens on a port.

```sh
cargo build --release --locked -p dagron --no-default-features --features sqlite
```

**No size is quoted for this build.** On the tree these measurements were
taken from it did not compile — three errors in `dagron-engine`, all one
shape: code that only the `ops` surface needs, reached from ungated code
(the exact output, 45 s in):

```text
error[E0425]: cannot find function `try_acquire_leadership` in module `db`
  --> crates/dagron-engine/src/leadership.rs:45:23

error[E0433]: failed to resolve: use of unresolved module or unlinked crate `environments`
    --> crates/dagron-engine/src/lib.rs:1208:40
     |
1208 |                     let parsed = match environments::template_params(&pool, &spec_yaml).await {
     = note: found an item that was configured out — `mod environments` is gated behind the `ops` feature

error[E0433]: failed to resolve: use of unresolved module or unlinked crate `environments`
    --> crates/dagron-engine/src/lib.rs:1755:21
     |
1755 |                     environments::resolve_secrets(&pool, &task.run_id, &mut ctx.env).await

error: could not compile `dagron-engine` (lib) due to 3 previous errors
```

The fix is mechanical and lands with the constrained-host gates: `leadership`
(a lease only the ops loops use) gated under `ops` together with its spawn
site, and a `not(ops)` stand-in for `environments` whose `template_params`
returns an empty map and whose `resolve_secrets` does nothing — so a lean unit
runs a spec **as written**, with no environment templating and no secret
resolution, which is the honest contract for a daemon with no API to manage
environments through. What it keeps: the reconcile loop, the local executor,
leases and crash recovery, every source, the artifact store, admission caps,
the pressure file and the free-disk floor. What it drops: the management API
and console, cron, the retention GC, DB-backed schedules, leadership, and
`dagron dev` (which refuses to start without `ops`, because there would be
nothing to keep it resident).

When it compiles, the command above is the measurement, and its number is
**not** interchangeable with 5a's: same repository, different feature set,
different binary. Quote it with the feature list attached.

### 5c. Idle memory — `dagron dev` after 10 s

`dagron dev` in an empty directory: it creates `workflow.db`, runs the
migrations, finds no DAG file, and idles serving the management API. Ten
seconds later, `/proc/<pid>/status` is read while the process is still
running, and the datastore files are sized before it is stopped.

```sh
mkdir /tmp/idle && cd /tmp/idle
API_ADDR=127.0.0.1:18791 dagron dev &                # stock knobs
sleep 10; grep -E 'VmRSS|VmHWM|Threads' /proc/$!/status; grep write_bytes /proc/$!/io
ls -l workflow.db*; kill $!
# the same with the profile:
printf 'profile: edge\n' > /tmp/edge.yaml
DAGRON_CONFIG=/tmp/edge.yaml API_ADDR=127.0.0.1:18792 dagron dev &
```

Each configuration was run twice, in a fresh directory each time, so the
spread between runs is visible next to the spread between configurations:

| At 10 s (`/proc/<pid>/status`, kB) | stock, run 1 | stock, run 2 | `profile: edge`, run 1 | `profile: edge`, run 2 |
| --- | ---: | ---: | ---: | ---: |
| `VmRSS` (= `VmHWM` in every run) | **17,180** | **17,580** | **16,548** | **16,980** |
| of which `RssAnon` (heap) | 3,540 | 3,552 | 3,348 | 3,344 |
| of which `RssFile` (mapped binary) | 13,640 | 14,028 | 13,200 | 13,636 |
| `VmSize` (virtual, mostly stacks and arenas never touched) | 362,928 | 362,928 | 362,660 | 362,660 |
| `Threads` | 6 | 6 | 6 | 6 |
| `write_bytes` since start (`/proc/<pid>/io`) | 1,703,936 | 1,679,360 | 1,687,552 | 1,683,456 |
| `workflow.db` / `-wal` / `-shm` (bytes) | 4,096 / 1,293,712 / 32,768 | same | same | same |

**Then thirty more seconds of idle** (run 2 of each, sampled again at 40 s):

| 10 s → 40 s | stock | `profile: edge` |
| --- | ---: | ---: |
| `VmRSS` | +12 kB | +0 kB |
| `write_bytes` | +20,480 | +20,480 |
| `workflow.db-wal` | 1,293,712 → 1,301,952 (+8,240) | 1,293,712 → 1,301,952 (+8,240) |

What the numbers say:

* **An idle engine holds ~17 MiB**, and about 13.5 MiB of it is the binary's
  own pages mapped from disk (`RssFile`) — the part that a static musl asset,
  with its own allocator and no shared libc, will size differently. The heap
  is ~3.5 MiB. Six threads regardless of `WORKER_COUNT`: workers are tasks
  in flight, not OS threads.
* **The profile is not a memory knob.** Its ~0.5 MiB saving is the same size
  as the spread between two runs of the same configuration. What it buys is
  CPU and disk *under load* (§1); do not pick it expecting a smaller daemon.
* **The idle write rate is the leadership lease, not the reconcile loop.**
  The WAL gained exactly two frames (2 × 4,120 B) in thirty seconds under
  both configurations — one commit every ~15 s, which is the ops build's
  leadership renewal at ⌊`LEADER_LEASE_SECS`/2⌋ with the default 30 s. Each
  is a commit plus an `fsync`; ~20 KiB of device writes per 30 s. On a flash
  budget that matters, raise `LEADER_LEASE_SECS` (a single unit has no peer
  to lose leadership to), or run the lean build (5b), which has no
  leadership and by construction commits nothing while idle. The reconcile
  tick itself wrote nothing: a tick with nothing to claim is a read.
* **Startup writes ~1.6 MiB**: the schema migrations, appended to the WAL as
  314 frames and not yet checkpointed at 10 s (well under the 1000-page
  autocheckpoint threshold). That WAL is steady state, not growth (§3).

The first sample in each pair was taken with `sampled_after_secs: 10.0` and
the process confirmed alive (`alive_at_sample: true`) before `/proc` was read;
the daemon was stopped with SIGTERM afterwards.

## 6. Release binaries and platforms

A tagged release attaches a `dagron` archive for each platform leg that
completed, plus one `SHA256SUMS` (built by `.github/workflows/binaries.yml`).
The `armv7` leg is allowed to fail without stranding the others (see below), so
that archive is best-effort and can be absent from a release until the workflow
enforces asset presence:

| Target | Runner | Linkage | Smoke-tested on the runner |
| --- | --- | --- | --- |
| `x86_64-unknown-linux-musl` | ubuntu-latest | static | yes |
| `aarch64-unknown-linux-musl` | ubuntu-24.04-arm (native arm64) | static | yes |
| `armv7-unknown-linux-musleabihf` | ubuntu-latest, **cross-compiled** | static | **no** — see below |
| `aarch64-apple-darwin` | macos-latest | dynamic (system libs) | yes |
| `x86_64-pc-windows-msvc` | windows-latest | dynamic (system libs) | yes |

Linux assets are musl rather than glibc so one binary runs on any distro —
including the minimal, years-old image a gateway tends to have — instead of
inheriting the builder's glibc floor.

**armv7 is the leg to read carefully.** There is no hosted 32-bit ARM runner,
so it is built on x86_64 with a cross toolchain (musl.cc's
`arm-linux-musleabihf-cross`, pinned by checksum; both the bundled SQLite and
`ring` compile C, so a Rust target alone would not link). The runner cannot
execute what it built, so the smoke test every other leg runs is skipped there,
and the job is allowed to fail without stranding the other four archives.
Until a release has actually produced and someone has run it on a board, treat
the armv7 asset as *linked, not proven*; running the smoke test under user-mode
qemu is the obvious next step and has not been done. `dagron validate
examples/simple_dag.yaml` on the target is the one-line proof.

## 7. What this build does, and what it does not

| Need | This build | Needs a fleet plane |
| --- | --- | --- |
| Run workflows on one unit, disconnected, across power loss | yes — this page | same engine |
| Take work from a broker / a directory / a stream | `SOURCE=mqtt`, `SOURCE=dir`, `SOURCE=stream` ([`STREAMING.md`](STREAMING.md)) | managed sources: credentials, fleet-wide config, delivery guarantees |
| Verify a signed workflow bundle before applying it | yes ([`BUNDLES.md`](BUNDLES.md)) | cohorts, staged rollout, automatic rollback |
| Enrol units, fan a workflow out to a cohort, roll results up | — | the fleet plane |
| Carry results and artifacts up a metered link, resumably, with a spool budget | the transactional outbox and local artifact store are here; the link is not | the fleet link |
| Reconcile clock-drifted records centrally | the fields are on every run (§4) | reconciliation on uplink |

Every gate in this build names which of these it hit rather than failing
silently. One unit is entirely served by the code on this page; the right-hand
column is the work that begins at the *second* unit you have to manage
together, and none of it is implemented here.
