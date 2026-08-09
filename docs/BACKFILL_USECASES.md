# Backfill use cases — Data Engineering, Data Science, MLOps

> How dagron's backfill + self-healing features solve real reprocessing problems.
> 30 cases across three personas, each mapped to the exact dagron mechanism.

Backfill is "run a workflow for time/data slots it didn't run (or didn't finish)
the first time." dagron treats this the same way it treats everything else — as
**state transitions on the datastore** (the source of truth) — so a backfill is
not a special bolt-on: it is the normal create-run / rerun path, gated by a dedup
ledger and an admission valve so it is **safe, idempotent, and bounded**.

---

## Mechanism cheat-sheet

The cases below reference these building blocks by name.

| # | Mechanism | What it is | Trigger |
|---|-----------|-----------|---------|
| **M1** | **Manual bounded backfill** | `POST /api/schedules/{id}/backfill` `{from,to,max_runs?}` — enumerates the schedule's cron fire-times in `[from,to]`, creates one run per slot. | Operator / API |
| **M2** | **Auto catch-up loop** | Engine loop (`AUTO_BACKFILL=1`): per-schedule `catchup=1` heals missed fires between `max(last_fired_at, now-window)` and now. | Automatic, leader-gated |
| **M3** | **Auto-rerun incomplete** | `AUTO_RERUN_FAILED=1`: re-arms terminally-`failed` runs from their failure frontier, bounded by attempt cap + cooldown (`run_reruns` ledger). | Automatic, leader-gated |
| **M4** | **Rerun-from-failed (QW1)** | `POST /api/runs/{id}/rerun` — resets only the failed/cancelled cone to `pending`, keeps succeeded tasks; recomputes `remaining_deps`. | Operator / API |
| **M5** | **Parameterized re-run (QW4)** | Same `rerun` endpoint with `{"params":{…}}` — deep-merged into each reset task's `input`. Fix-forward without re-authoring. | Operator / API |
| **M6** | **Dedup ledger** | `schedule_backfills (schedule_id, logical_date)` PK — claimed `INSERT … ON CONFLICT DO NOTHING`, so a slot already materialized is `skipped`, never double-run. | Automatic (under M1/M2) |
| **M7** | **Admission valve** | `MAX_INFLIGHT_RUNS` + `count_active_runs` — caps concurrently-active runs; a wide backfill buffers instead of stampeding. | Automatic |
| **M8** | **State gauges → eventing** | `scheduler_schedule_lag_seconds`, `scheduler_overdue_schedules`, `scheduler_incomplete_runs` (stall SLA `RUN_STALL_SECS`); `*_catchup_runs_total`, `*_auto_reruns_total`. Scrape → alert → act. | Automatic |
| **M9** | **Transactional outbox** | Every catch-up / auto-rerun / completion writes `backfill.catchup` / `run.auto_rerun` / `run.completed` to `event_outbox`, drained to webhooks/queues. | Automatic |
| **M10** | **Dead-letter store** | Poison submissions park in `dead_letters` (not infinite-retried); inspect + `POST /api/dead-letters/{id}/redrive` after a fix. | Automatic + operator |
| **M11** | **Leadership singleton** | All time-driven loops (cron/schedule/catch-up/GC) fire on exactly one node via a DB lease — no double-fire across N replicas. | Automatic |
| **M12** | **Artifact passing** | `DAGRON_ARTIFACTS` per-run shared dir — a backfilled run is self-contained and reproducible. | Automatic |

---

## Data Engineer (10)

### DE-1 — Pipeline missed runs during a deploy / outage
**Pain:** the scheduler (or DB) was down 02:00–06:00; the hourly `sales_rollup`
never fired for those slots.
**dagron:** **M2**. Mark the schedule `catchup=1`; on recovery the leader's
catch-up loop enumerates 02:00/03:00/04:00/05:00 and materializes them through the
**M6** dedup ledger. Bounded by `catchup_window_secs` so a long outage can't replay
unbounded history, and `catchup_max_runs` so it heals across ticks, not in one burst.
```sql
UPDATE schedules SET catchup=1, catchup_window_secs=43200, catchup_max_runs=24 WHERE id='…';
```

### DE-2 — New daily table needs 2 years of history loaded
**Pain:** you just authored `dim_customer_daily`; the warehouse needs every day
since 2024.
**dagron:** **M1** + **M7**. One call enumerates ~730 fire-times; `max_runs` +
`MAX_INFLIGHT_RUNS` drain them in controlled waves instead of 730 simultaneous runs.
```bash
curl -X POST $API/api/schedules/$SID/backfill \
  -d '{"from":"2024-01-01T00:00:00Z","to":"2026-01-01T00:00:00Z","max_runs":1000}'
# → {"scheduled": 731, "skipped": 0, "run_ids":[…]}
```

### DE-3 — Upstream landed late / was empty; reprocess one partition
**Pain:** the 2026-06-20 partition ran against an empty source.
**dagron:** **M5**. Re-run that day's run with the corrected logical date as a param;
only the affected run reprocesses.
```bash
curl -X POST $API/api/runs/$RUN/rerun -d '{"params":{"ds":"2026-06-20","force":true}}'
```

### DE-4 — Schema/transform change requires reprocessing last 90 days
**Pain:** you fixed a revenue calculation; the last quarter is now wrong.
**dagron:** **M1**. Bounded backfill over the 90-day window; **M6** makes re-issuing
the same window safe if you need to re-run the command (already-done slots come back
as `skipped`).

### DE-5 — Transient DB/network blip failed a DAG mid-flight
**Pain:** task 7 of 12 hit a connection reset; the run is `failed`.
**dagron:** **M3** (hands-off) or **M4** (explicit). Auto-rerun re-arms only the
failed cone; the 6 succeeded tasks are **not** recomputed. Attempt cap + cooldown
stop it looping if the failure is deterministic.
```bash
AUTO_RERUN_FAILED=1 AUTO_RERUN_MAX_ATTEMPTS=3 AUTO_RERUN_COOLDOWN_SECS=300
```

### DE-6 — Paused DAG re-enabled (the Airflow `catchup` foot-gun)
**Pain:** a DAG paused for a week un-pauses and stampedes the cluster with every
missed interval.
**dagron:** **M2**. `last_fired_at` is frozen at the pause; catch-up fills
pause→now **clamped to the window** and **capped per sweep** — heals deliberately,
no stampede. The bound is the whole point.

### DE-7 — Ops re-issues a backfill by mistake (idempotency)
**Pain:** someone runs the same `2024-01..2024-06` backfill twice.
**dagron:** **M6**. The second call's slots collide on `(schedule_id, logical_date)`
and return as `skipped` — zero double-runs. `{"scheduled":0,"skipped":181}`.

### DE-8 — Backfill without melting the warehouse
**Pain:** a 10k-interval backfill would open 10k warehouse connections.
**dagron:** **M1** `max_runs` (per-call hard cap 1000) + **M7** `MAX_INFLIGHT_RUNS`
(active-run ceiling). The backlog sits durably in the store; admission admits only
N at a time — "the queue is the buffer, active-run count is the valve."

### DE-9 — Late-arriving data lands hours after the partition closes
**Pain:** the 00:00 partition is only complete by 04:00.
**dagron:** **M1** targeted at the single logical slot once data lands, or schedule
the DAG later and let **M2** catch any genuinely-missed slot. **M6** guarantees the
late re-run and any normal run don't both fire the same slot.

### DE-10 — A run wedges and never completes
**Pain:** a task hangs on a dead external API; the run sits `running` forever.
**dagron:** **M8**. `scheduler_incomplete_runs` (runs past `RUN_STALL_SECS`) +
`scheduler_schedule_lag_seconds` rise → alert fires → operator `POST …/cancel`
then **M4** rerun. (Stall is signal-only by design — auto-killing a maybe-still-
progressing run is unsafe.)

---

## Data Scientist (10)

### DS-1 — Backfill a feature table for a brand-new feature
**Pain:** your model needs `rolling_30d_spend` computed over 18 months of history.
**dagron:** **M1** over the historical window; **M12** gives each backfilled run an
isolated artifact dir so feature outputs are self-contained and comparable.

### DS-2 — Re-run an experiment pipeline with new hyperparameters over a date range
**Pain:** you changed the smoothing window and want it applied to past cohorts.
**dagron:** **M5**. Rerun each affected run with `{"params":{"smoothing":0.2}}`;
inputs change without re-authoring the workflow YAML.

### DS-3 — A labeling bug corrupted several days of training labels
**Pain:** days 2026-05-01..05-07 have flipped labels.
**dagron:** **M1** scoped to that week + **M6** dedup so a retry of the fix is safe.
Succeeded-elsewhere days are untouched.

### DS-4 — Recompute A/B metrics for past cohorts after a metric-definition change
**Pain:** "conversion" was redefined; historical dashboards are stale.
**dagron:** **M1** by logical date across the reporting window; **M9** emits a
`backfill.catchup`/completion event per slot so the BI refresh can react downstream.

### DS-5 — One date failed inside a multi-date sweep
**Pain:** a 90-run parameter sweep had 1 OOM failure.
**dagron:** **M4**. `POST /api/runs/{failed_id}/rerun` re-arms just that run from its
frontier; the other 89 results stand.

### DS-6 — Seasonal/holiday windows need special reprocessing
**Pain:** Black Friday week needs a different feature config applied retroactively.
**dagron:** **M5** targeted backfill of those dates with `{"params":{"profile":"peak"}}`.

### DS-7 — Reproduce an exact historical run deterministically
**Pain:** a reviewer asks "regenerate the 2026-03-15 result exactly."
**dagron:** **M5** with the original `logical_date` param + **M12** artifacts + **M6**
ledger — the slot is keyed by logical date, so the reproduction is the same slot,
not a new one.

### DS-8 — Lookback window extended (30 → 90 days)
**Pain:** the cohort definition grew; the extra 60 days were never computed.
**dagron:** **M1** for just the newly-in-scope dates; everything already computed
comes back `skipped` via **M6**.

### DS-9 — Data drift detected in a key metric
**Pain:** monitoring flags drift; you want the last N days recomputed with the new
preprocessing.
**dagron:** **M2** with a tightened `catchup_window_secs`, or **M1** for an exact
window; **M8** lag/`catchup_runs_total` confirm the recompute completed.

### DS-10 — Compare two model versions over identical historical dates
**Pain:** you need v1 and v2 scored on the same 60 days.
**dagron:** **M5** twice — `{"params":{"model_version":"v1"}}` and `…"v2"` — over the
same range. Distinct param sets produce distinct runs; the dedup ledger keys the
*schedule* slot, while ad-hoc reruns are explicit, so both versions coexist.

---

## MLOps (10)

### MLO-1 — Scheduled training missed during cluster maintenance
**Pain:** the nightly `train_ranker` didn't fire during a 6-hour GPU-pool upgrade.
**dagron:** **M2**. `catchup=1` heals the missed nightly slot on recovery, **M11**
ensures only one controller node fires it even across a multi-replica HA deployment.

### MLO-2 — Training failed on a transient node eviction
**Pain:** a spot/preemptible node was reclaimed mid-epoch.
**dagron:** **M3**. Auto-rerun re-arms the failed training run; `AUTO_RERUN_MAX_ATTEMPTS`
stops it after N tries so a *real* bug (bad data, OOM-on-purpose) doesn't burn GPUs
in a loop — it's left for a human after the cap.

### MLO-3 — Training must wait until the feature pipeline is complete (data-state gate)
**Pain:** training kicked off before features finished and trained on partial data.
**dagron:** **M8** + **M9**. `scheduler_incomplete_runs` exposes "feature run still
in flight"; an alert/automation consuming the `run.completed` outbox event for the
feature run is the gate that releases (or backfills) training — state registered as
a metric *triggers* the eventing.

### MLO-4 — Drift / registry event should trigger a retraining backfill
**Pain:** the model registry flags a drift breach and wants the last 14 days
retrained.
**dagron:** **M9** inbound (your drift job) → **M1** outbound: the webhook calls the
backfill endpoint for the 14-day window. Bounded + deduped so repeated drift alerts
don't pile up duplicate retrains.

### MLO-5 — Stand up a feature store for a new model needing history
**Pain:** a new model needs 12 months of materialized features before it can train.
**dagron:** **M1** large bounded backfill + **M7** admission valve to protect the
online store + **M12** artifacts for hand-off between feature and training tasks.

### MLO-6 — Bad model shipped; re-run inference over the affected window
**Pain:** v7 scored 3 days of traffic wrong; you rolled back to v6.
**dagron:** **M5**. Re-run those days' inference with `{"params":{"model_version":"v6"}}`
— fix-forward over the exact window without editing the pipeline.

### MLO-7 — SLA alerting for overdue pipelines
**Pain:** you need to know *before* the data consumer does that a pipeline is late.
**dagron:** **M8**. `scheduler_schedule_lag_seconds` / `scheduler_overdue_schedules`
→ Prometheus alert → PagerDuty. The same lag the catch-up loop heals is the signal
you page on.

### MLO-8 — A poison config fails every attempt
**Pain:** a malformed spec would retry forever and mask the real problem.
**dagron:** **M10** + **M3** cap. Unparseable submissions dead-letter instead of
nack-looping; failing runs stop at the attempt cap. Fix the config, then
`POST /api/dead-letters/{id}/redrive`.

### MLO-9 — HA: guarantee only one node runs the backfill
**Pain:** three controller replicas would each start the same catch-up.
**dagron:** **M11**. The catch-up/cron/schedule loops fire only on the lease holder;
followers idle. The **M6** ledger makes even a stray double-sweep *safe*, but gating
keeps the work on one node.

### MLO-10 — Audit + observability of every self-healing action
**Pain:** compliance needs a record of every automated re-run and what it touched.
**dagron:** **M9**. Each catch-up emits `backfill.catchup {run_id, schedule_id,
logical_date}` and each auto-rerun emits `run.auto_rerun {run_id, reset_tasks}` to
the durable outbox — drained to Slack/audit-log/queue by the same worker that ships
`run.completed`. Counters `scheduler_catchup_runs_total` / `*_auto_reruns_total`
give the aggregate.

---

## Picking the right mechanism

```text
Did the run START but FAIL/stall?            → M4 rerun (manual) / M3 auto-rerun
   …and you need to change inputs?           → M5 parameterized rerun
Did the run NEVER fire (missed a schedule)?
   …a known explicit window?                 → M1 manual backfill
   …heal automatically going forward?        → M2 catch-up loop
Worried about double-runs?                   → M6 dedup ledger (always on under M1/M2)
Worried about stampede / load?               → M7 admission valve + max_runs/cap
Need to know / react when it happens?        → M8 gauges + M9 outbox events
Running N replicas?                          → M11 leadership singleton (automatic)
```

**Golden rule:** a *failed* run is a **rerun** (resume the broken cone, keep good
work); a *missing* run is a **backfill** (create the slot). dagron makes both the
same durable, idempotent, bounded state transition — and, with `AUTO_BACKFILL` /
`AUTO_RERUN_FAILED`, does them for you while publishing the lag/incomplete state as
the metrics you alert on.
