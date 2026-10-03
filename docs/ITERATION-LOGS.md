# Per-iteration logs — what it costs to stop overwriting

> The viability question, answered with measurements rather than estimates:
> **what does the system consume if the log keeps every iteration of a loop
> instead of only the last one?**
>
> Short answer: retaining *everything* is not viable, because it multiplies a
> quantity that is already unbounded. Retaining a **bounded tail per
> superseded attempt** costs a measured **228 KiB per looping task, worst
> case**, and **nothing at all** for a task that neither loops nor retries.
> That is the shape this repo implements.

## Why the log shows one iteration

Not a UI limitation. A task row has exactly one place to put output:

```sql
CREATE TABLE task_runs (
    ...
    attempt          INTEGER NOT NULL DEFAULT 0,
    output           TEXT,            -- ← singular
    ...
);
```

There is no log table. `GET /runs/{id}/logs` and the console's log view both
read `task_runs.output` and nothing else
([`api.rs`](../crates/dagron-engine/src/api.rs),
[`routes/logs.rs`](../crates/dagron-api/src/routes/logs.rs)); `dagron-logging`
is the process's own `tracing` bootstrap, not task output. So the whole
task-log surface is one column, and three writers take turns clobbering it:

| Writer | Statement | When |
|---|---|---|
| [`append_task_output`](../crates/dagron-core/src/db/sqlite.rs) | `SET output = ?` when `reset` | first live chunk of an attempt |
| [`retry_task`](../crates/dagron-core/src/db/sqlite.rs) | `SET output = ?` | an iteration ends and another is queued |
| `mark_task_succeeded_inner` | `SET output = ?` | the task finally succeeds |

`repeat:` reaches the second one directly — the engine re-uses the retry
machinery for loop iterations, because an iteration and an attempt are the same
thing to the scheduler:

```rust
// crates/dagron-engine/src/lib.rs — RepeatDecision::Again
db::retry_task(&pool, &result.task_id, &result.worker_id,
               result.fence, result.output, retry_at).await?;
```

Two consequences worth stating plainly:

* **It is not only loops.** Ordinary retries lose their per-attempt output the
  same way, and always have. A task that failed twice and passed on the third
  try shows only the third try's output — the two that explain the failure are
  gone. The same single write site causes both, so fixing one fixes both.
* **`attempt` is already the iteration counter.** It is incremented at claim
  time, so when `retry_task` runs, `task_runs.attempt` is the 1-based number of
  the iteration that just finished. Nothing needs inventing to number them.

## Why "just keep them all" is not viable

Retaining every iteration multiplies three quantities, and none of them is
bounded by anything the operator has set:

**1. Output per iteration is unbounded.** The executor accumulates a task's
stdout into a `String` with no ceiling:

```rust
// crates/dagron-executor/src/executor.rs
let mut acc = String::new();
... acc.push_str(&line);
```

No write path caps it. `mark_task_succeeded_inner` and `retry_task` both bind
the whole thing. The **read** path has a cap (`DEFAULT_LIMIT = 2_000` lines,
plus a tail-preserving merge cap in the whole-run endpoint), which is why a
chatty task does not blow up the console today — but that cap is applied after
the bytes are already on disk.

**2. The iteration count is effectively unbounded.** `RepeatSpec.max_iterations`
is a `u32`. The console clamps its own control to 1000
([`loop-model.ts`](../frontend/src/lib/loop-model.ts)), but a hand-written spec
is limited only by the type.

**3. Nothing deletes it.** `GC_RETENTION_SECS` is **unset by default, which
means GC is off** ([`CONFIG.md`](CONFIG.md)). In the default deployment task
output accumulates for the life of the database. The one valve that exists,
`DAGRON_MIN_FREE_BYTES`, defaults to `0` (off), is SQLite-only, fails open if
the probe errors — and when it does fire it refuses **new run creation**, which
does not help a run already 400 iterations into a loop.

Measured, on the real schema, VACUUMed, including the primary key and index:

| Output kept per iteration | 1000 iterations of **one** task |
|---|---|
| 4 KiB | 4.5 MiB |
| 64 KiB | 63.1 MiB |
| 1 MiB | 1001.4 MiB |

One task. The run-level ceiling (`DAGRON_MAX_TASKS_PER_RUN`, 100 000) applies
to rows, not to bytes, so there is no number in the system today that would
have refused any of those.

There is a fourth multiplier further out: `archive_doc_for_run` serialises
`SELECT * FROM task_runs` into a single JSON document before the GC sweep
purges, so per-iteration output would land in the archive document too, at full
size, in memory, one run at a time. That document is the history's *last*
chance to exist — the purge deletes the task rows, and the attempt rows cascade
with them — so it has to carry them, which is another reason the per-attempt
size has to be a number rather than whatever the task printed.

## What is actually worth keeping

The useful observation is that **the iteration you are still running is not the
one you lost**. `task_runs.output` already holds the live attempt at full
fidelity, streamed chunk by chunk, and that is what tailing a running task
needs. What gets destroyed is the *superseded* attempts — and what anyone wants
from iteration 7 of 300 is not its complete stdout. It is: did it run, what did
it print, roughly, and why did `until` not hold yet.

So the policy is:

> **`task_runs.output` keeps the current attempt, whole and unchanged.**
> **A new `task_attempts` table keeps a bounded tail of each superseded one.**

which makes the added storage a product of two constants instead of a product
of three unbounded quantities:

```text
added bytes  ≤  iterating_tasks × DAGRON_ATTEMPT_LOG_KEEP × DAGRON_ATTEMPT_LOG_BYTES
```

Every term on the right is known before the run starts, which is the property
that matters: it is a number `budget:` could refuse at submit, the way it
already refuses a fan-out blow-up.

### Measured cost of the bounded policy

Defaults: keep the last **50** superseded attempts per task, each capped at
**4096 bytes** of tail.

| | Added on disk |
|---|---|
| A task that never loops and never retries | **0** — no rows written |
| A task that retried 3 times | 12.0 KiB |
| A 1000-iteration loop (ceiling) | **228.0 KiB** |
| 100 iterating tasks in one run | 22.3 MiB |
| 1000 iterating tasks in one run | 222.7 MiB |

The first row is the one that makes this affordable: the cost is paid only by
tasks that actually iterate, and today those are exactly the tasks whose logs
are wrong.

The tail is kept rather than the head, matching `tail` in the log filter — a
loop that ran out of iterations is diagnosed from its end. Truncation is
recorded on the row (`truncated`) rather than inferred, and cuts on a character
boundary, so a multibyte scalar is never split.

### Knobs

| Variable | Default | Meaning |
|---|---|---|
| `DAGRON_ATTEMPT_LOG_BYTES` | `4096` | Tail bytes kept per superseded attempt. **`0` disables retention entirely** — the old overwrite behaviour, byte for byte. |
| `DAGRON_ATTEMPT_LOG_KEEP` | `50` | Superseded attempts kept per task. Oldest are dropped first. `0` = unlimited, and is the only way to reach the unbounded numbers above — it exists for a debugging session, not for a deployment. The **read** stays bounded either way: one request returns at most the newest 200 attempts, so an unlimited retention window never means an unlimited response. |

Setting `DAGRON_ATTEMPT_LOG_BYTES=0` is a complete opt-out: no table writes, no
extra transaction work, no rows. An operator with a full disk can turn it off
and get the previous consumption back exactly.

## What this does *not* fix

`task_runs.output` itself is still uncapped. A single task that prints 2 GiB
still stores 2 GiB, and always did — that is a pre-existing property of the
write path, not something per-iteration retention introduces, and capping it
would change what a normal run stores. It is called out here because it is the
larger number, and because the cap chosen above only bounds the *new* cost.

The archive document still materialises a whole run in memory, and now carries
`task_attempts` as well (a sibling key; the compactor reads `tasks` and is
unaffected). Per-iteration rows add to it at
`DAGRON_ATTEMPT_LOG_KEEP × DAGRON_ATTEMPT_LOG_BYTES` per task, which is
bounded, but bounded is not free.

## Why this is the prerequisite for iterating over a task's output

`with_output_of:` ([`LOOPS.md`](LOOPS.md#iterating-over-a-tasks-output)) is
built on one sentence: *the sweep reads the producer's output*. That sentence
assumes the output is **there** to read, which is the assumption this page has
just spent its length showing is false for anything that iterates or retries —
the value in `task_runs.output` is whatever the most recent writer left.

For a producer that runs once, today's single column serves. The moment the
producer is itself looping — the obvious next question, *"iterate over what
each pass produced"* — there would be no stored value to iterate over, only a
race between the sweep and the next iteration's overwrite. Per-attempt
retention is what turns that into a readable history.

## See also

* [`LOOPS.md`](LOOPS.md) — the two loop mechanisms and what the console writes.
* [`CONFIG.md`](CONFIG.md) — `GC_RETENTION_SECS`, `DAGRON_MIN_FREE_BYTES`, and
  the knobs above.
