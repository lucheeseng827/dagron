# Datasets — data-aware scheduling (produce → track → trigger)

> Airflow Datasets / Dagster asset-sensor parity for the whole authoring loop:
> produce, track, sense, and trigger — on one dataset or on a fan-in across
> several. Where that loop stops is
> [below](#limits-of-this-build--datasets) — stated as an error at the
> boundary, never as a silent no-op.

Time-based schedules answer "run at 02:00 and hope the data landed."
Dataset-aware scheduling answers "run **because** the data landed":

```text
producer workflow                         consumer workflow
┌─────────────────────┐   dataset ledger  ┌──────────────────────────┐
│ task: load           │  ─────────────▶  │ on_datasets:              │
│   produces:          │  s3://lake/orders│   - s3://lake/orders      │
│     - s3://lake/orders│                 │ (fires when it updates)   │
└─────────────────────┘                   └──────────────────────────┘
```

## The three pieces

### 1. `produces:` — declare what a task updates

```yaml
name: ingest-orders
tasks:
  - name: load
    command: ["python", "load_orders.py"]
    produces:
      - s3://lake/orders
```

When `load` **succeeds**, the engine upserts `s3://lake/orders` in the
`datasets` registry and appends a row to the `dataset_events` lineage ledger
(producing workflow, run, task, timestamp). Recording is declarative — moving
the actual bytes is the task's job; dagron never dereferences the URI. A
dataset URI is an opaque identity (1–512 chars, no whitespace), matched by
exact string equality, exactly like Airflow dataset URIs. URIs template at
expansion, so a fan-out can produce per-shard datasets
(`produces: ["s3://lake/orders/{{ item }}"]`).

Only command tasks may declare `produces:` (approval gates, sub-workflow
triggers, and wait sensors resolve outside the worker result path — a
`produces:` there would be silently dropped, so validation rejects it).

Recording is **fence-guarded**: it happens only after the task's success
mutation actually lands, so a reclaimed attempt's late result can never
fabricate lineage. A task resolved from the **memoization cache** (`cache:`)
records its datasets too — `produces:` is a postcondition ("after this task
succeeds, the dataset is current"), and staying silent on a cache hit would
park every downstream sensor and `on_datasets:` consumer forever whenever the
producer happened to hit its cache.

**Registry + lineage are queryable** (management API):

```text
GET  /datasets                  → every dataset, latest update first
GET  /datasets/events?uri=...   → the update trail: who updated what, when
```

That trail is the cross-workflow update tracking: producer runs on one
workflow, consumers on others, one ledger connecting them.

### 2. `wait: { dataset: … }` — the dataset sensor

```yaml
name: enrich-orders
tasks:
  - name: fresh-orders
    type: wait
    wait: { dataset: "s3://lake/orders" }
  - name: enrich
    command: ["python", "enrich.py"]
    depends_on: [fresh-orders]
```

A mid-DAG join point on data freshness. Parks holding **no worker slot**
(same machinery as the time/HTTP sensors — `running` + NULL lease, so the
claim scan skips it and lease recovery leaves it alone), and resolves when the
dataset records an update **after** the park — the sensor waits for *fresh*
data, never satisfied by history (its cursor is the ledger's high-water mark
at park time). Exactly one of `wait.for` / `until` / `url` / `dataset`.

### 3. `on_datasets:` — dataset-triggered workflows

```yaml
name: daily-report
on_datasets:
  - s3://lake/orders
tasks:
  - name: report
    command: ["python", "report.py", "--woken-by", "{{ trigger_dataset }}"]
```

A **registered** workflow with `on_datasets:` fires a run whenever the
subscribed dataset records a new update. The triggering URI is injected as
`{{ trigger_dataset }}`. Semantics:

- **Registering never fires on history** — subscriptions start at the
  ledger's current high-water mark.
- **Updates coalesce** — N rapid updates between sweeps produce one run, not N.
- **HA-safe with no leadership** — firing is a CAS cursor advance; with many
  schedulers sweeping, exactly one wins each fire.
- **`max_active_runs` is honored** — a fire refused at the cap rolls its
  cursor back and retries once a slot frees; nothing is lost.
- Sweep cadence is ~5 s; `DATASET_TRIGGERS=0` opts a scheduler out.

## Partitioning a dataset URI by the fire's date

A dataset URI templates per instance, and three variables carry the fire's
**logical** date — the date the run is *for*, not the date it ran:

| Variable | Example |
|---|---|
| `{{ scheduled_time }}` | `2026-09-14T22:30:00+00:00` |
| `{{ ds }}` | `2026-09-14` |
| `{{ ds_nodash }}` | `20260914` |

```yaml
produces: ["clickhouse://analytics/marts/daily_rollup/{{ ds }}"]
```

All three are bound identically on every fire path — cron, schedule, backfill
and backfill jobs — from one derivation, because a backfill and a cron fire that
partition by the same expression must produce the same string or they write to
different partitions of one table. `ds` is the UTC day of `scheduled_time`,
matching the timestamp beside it rather than any local calendar.

That is what makes a replay correct: backfilling last March writes March's
partitions, not today's.

A worked example is [`examples/warehouse/`](../examples/warehouse/), which pairs
this with a deferred Spark job whose `produces:` is recorded when the *remote*
job finishes.

## What the loop does and does not cover

| Capability | This build |
|---|---|
| `produces:` recording, registry + lineage ledger and their read APIs | full |
| `wait: { dataset: … }` sensor | full |
| Dataset-triggered workflows | full — any number of datasets |
| Multi-dataset composition (`on_datasets: [a, b, …]` + `datasets_mode: any\|all`) | full (open since 0.10.0) |
| External dataset events (`POST /datasets/events`) — CDC, S3 notifications, other orchestrators | `403`, with the reason |
| Freshness SLAs, a lineage graph UI, dataset partitions | not implemented |

The **authoring loop is complete on its own**: workflows produce, sense, fire on
one dataset or fan in across several, the lineage is fully queryable, and HA is
included. What is missing is integration and reporting at org scale — data
arriving from systems outside dagron, and the freshness/graph surfaces built on
top. External events refuse loudly rather than half-working, which is the
property that matters when a workflow's trigger is the thing you are debugging;
the rest simply do not exist yet and say so.

## Limits of this build — datasets

Every gate is a **signpost, not a dead end** — it names what was attempted,
where it ships, and what to do instead in this build (the pattern
`dagron-source`'s connector errors established). The full list across the
product, and what to do if you want the capability rather than the fallback, is
[what this build does not do](https://github.com/lucheeseng827/dagron#what-this-build-does-not-do) in the README; this section covers the ones that are dataset-specific.

> **Multi-dataset composition is open.** `on_datasets: [a, b]` with
> `datasets_mode: any|all` was refused here until 0.10.0, with the advice to
> "trigger on one dataset and put a `wait: { dataset: … }` sensor on the other".
> That advice is **wrong** in the case it was written for. A sensor stamps its
> cursor when it parks, so it resolves only on an update that lands *after* that
> moment — an upstream that refreshed before the run started never satisfies it,
> so the run waits for tomorrow's load and hangs to its `run_timeout_secs`.
> `datasets_mode: all` does not have that race. A gate whose documented
> alternative loses data is a gate on correctness, so it is gone.

- **External dataset events.** `POST /datasets/events` answers `403`, so data
  landing from outside dagron (CDC, S3 notifications, another orchestrator)
  cannot announce itself directly. Work around it with a small `produces:` task
  that records the dataset once the external load finishes; it works, but it
  scales poorly as more systems feed the platform, which is why the managed
  events gateway and CDC connectors exist.
- **Freshness SLAs, the lineage graph UI, and dataset partitions** are not in
  this build and have no workaround — they are org-level surfaces on the same
  shelf as SSO, RBAC and audit.

## Operations

- **Tables:** `datasets` (registry), `dataset_events` (append-only ledger),
  `dataset_triggers` (subscriptions + cursors) — SQLite migration 032,
  Postgres 039; sensor columns on `task_runs` (`wait_dataset`,
  `wait_dataset_cursor`) — SQLite 033, Postgres 040.
- **Metrics:** `scheduler_dataset_updates_total`,
  `scheduler_dataset_fires_total` on `/metrics`.
- **Env:** `DATASET_TRIGGERS=0` disables the trigger sweep on a scheduler
  (produces-recording and sensors are run-local and stay on).
- The ledger is append-only; entries are small (a URI + ids). GC/retention for
  `dataset_events` rides the same operational posture as the run GC — a
  retention sweep is a follow-on if ledgers grow past what a `DELETE … WHERE
  id < ?` maintenance query handles.
