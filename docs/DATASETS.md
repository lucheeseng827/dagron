# Datasets — data-aware scheduling (produce → track → trigger)

> Airflow Datasets / Dagster asset-sensor parity. The open-core split: the
> single-team loop — produce, track, sense, single-dataset trigger — is
> Apache-2.0; the
> **full-blown data-aware scheduler** (multi-dataset composition, external
> events, the org-level lineage layer) ships with **dagron Enterprise**.

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

## Open vs. Enterprise

| Capability | Open (Apache-2.0) | Enterprise |
|---|---|---|
| `produces:` recording, registry + lineage ledger and their read APIs | ✅ full | ✅ |
| `wait: { dataset: … }` sensor | ✅ full | ✅ |
| Dataset-triggered workflows | ✅ **one** dataset per workflow | ✅ unlimited |
| Multi-dataset composition (`on_datasets: [a, b, …]` + `datasets_mode: any\|all`) | — signposted error | ✅ AND/OR fan-in ("fire when *both* upstream tables landed") |
| External dataset events (`POST /datasets/events`) — CDC, S3 notifications, other orchestrators | — `403` with signpost | ✅ (pairs with the managed CloudEvents ingest gateway) |
| Freshness SLAs (fire/alert when a dataset goes stale), lineage graph UI, dataset partitions | — | on the Enterprise roadmap |

The line follows the §3 honesty test the way the SOURCE connectors drew it:
the **single-team loop is complete and free** — one workflow produces, another
senses or fires on it, lineage fully queryable, HA included. What's paid is
*composition and integration at org scale*: fan-in across many teams' datasets,
events from systems outside dagron, and the org-level lineage/freshness layer —
value that only materializes once many teams share the data platform.

## The OSS → paid funnel, concretely

Every gate is a **signpost, not a dead end** — it names what was attempted,
where it ships, and what the open build does instead (the pattern
`dagron-source`'s connector errors established):

1. **Adoption hook (free).** A single-team user wires `produces:` +
   `on_datasets:` in minutes and gets Airflow-Datasets behavior out of a single
   binary. This is top-of-funnel; gating it would poison adoption.
2. **First friction = first touchpoint.** The natural next step — "fire the
   join once *both* tables refresh" — is a one-line spec change
   (`on_datasets: [a, b]`). Validation answers with the Enterprise signpost and
   the exact workaround (split consumers, one workflow per dataset). The user
   learns the edition line at the moment they feel the need, in their own
   terminal, with a working fallback in hand.
3. **Integration pull.** The moment data lands from outside dagron (CDC,
   S3 events), `POST /datasets/events` answers `403` with the same signpost —
   and the workaround (a tiny `produces:` task) works but visibly scales
   poorly, which is honest: the paid feature is the *managed integration*
   (a hosted events gateway and CDC connectors), not an artificial lock.
4. **Org gravity.** Lineage across dozens of teams' workflows, freshness SLAs,
   and the graph UI are org-level surfaces (the same shelf as SSO/RBAC/audit) —
   they only matter at exactly the scale where there's a budget.

Sales-signal side: the signposts make intent measurable — a support question or
GitHub issue quoting the multi-dataset error *is* a qualified lead, the same
way SOURCE-connector signpost questions are.

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
