# Backfill examples — try DE-1, DS-2, MLO-6 end-to-end

Three runnable workflows + the exact config to exercise dagron's backfill /
self-healing features. Cases and mechanism IDs (M1–M12) come from
[`../../docs/BACKFILL_USECASES.md`](../../docs/BACKFILL_USECASES.md).

| File | Case | Mechanism |
|------|------|-----------|
| `sales_rollup.yaml` | **DE-1** — missed runs after an outage | **M2** auto catch-up |
| `train_experiment.yaml` | **DS-2** — re-run with new hyperparameters | **M5** parameterized re-run |
| `score_batch.yaml` | **MLO-6** — re-run inference after a bad model | **M5** parameterized re-run |

## Which surface serves what (read this first)

dagron has two HTTP surfaces, and the features split across them:

- **Engine** (`dagron`, the reconcile daemon) runs the **auto catch-up** + **auto-rerun**
  loops and exposes a small ops API (`/runs`, `/runs/{id}/rerun|cancel`, `/metrics`).
  Works on SQLite **or** Postgres.
- **dagron-api** (the management gateway, **Postgres only**) owns workflow/schedule
  **CRUD**, the **manual backfill** endpoint, and **parameterized** re-run
  (`params`). The engine's single-binary `rerun` does a plain frontier reset — the
  `params` override lives only here.

All three walkthroughs below therefore use the **engine + dagron-api + Postgres**
stack so every endpoint exists. (Pure-SQLite `dagron dev` also runs catch-up /
auto-rerun, but without the gateway's workflow/schedule/backfill/`params` HTTP API.)

## One-time setup

```bash
cd rust_modules/lab/module_54

# 1) Postgres
docker run -d --name dagron-pg -e POSTGRES_PASSWORD=dagron -e POSTGRES_DB=dagron \
  -p 5432:5432 postgres:16
export DATABASE_URL="postgres://postgres:dagron@localhost:5432/dagron"

# 2) Engine — applies migrations_pg (incl. 012 catch-up columns), runs the
#    reconcile loop + the QW3 auto-catchup / auto-rerun loops, serves /metrics:8088.
DATABASE_URL="$DATABASE_URL" \
API_ADDR=127.0.0.1:8088 \
DB_SCHEDULES=1 \
AUTO_BACKFILL=1 \
AUTO_BACKFILL_INTERVAL_SECS=15 \
CATCHUP_DEFAULT_WINDOW_SECS=21600 \
CATCHUP_DEFAULT_MAX_RUNS=24 \
AUTO_RERUN_FAILED=1 \
AUTO_RERUN_MAX_ATTEMPTS=3 \
RUN_STALL_SECS=300 \
cargo run --no-default-features --features postgres,ops &
# (it also fires the bundled examples/simple_dag.yaml once on boot — harmless.)

# 3) Management gateway — workflow/schedule CRUD + manual backfill + params rerun.
DATABASE_URL="$DATABASE_URL" \
DAGRON_JWT_SECRET=dev-secret-change-me \
DAGRON_ADMIN_EMAIL=admin@example.com \
DAGRON_ADMIN_PASSWORD=dagron-admin-pw \
PORT=8080 \
cargo run -p dagron-api &
```

```bash
# 4) Log in once; reuse $TOKEN / $auth below.
TOKEN=$(curl -s localhost:8080/api/login -H 'content-type: application/json' \
  -d '{"email":"admin@example.com","password":"dagron-admin-pw"}' | jq -r .token)
auth=(-H "authorization: Bearer $TOKEN" -H 'content-type: application/json')

# Helper: turn a YAML file into a JSON string for the {"spec": …} body.
spec() { jq -Rs . < "$1"; }
```

---

## DE-1 — missed runs after an outage → auto catch-up (M2)

A fresh hourly schedule with `catchup: true` and a 6-hour window: on its first
sweep (~15 s) the engine enumerates the **6 hourly fire-times in the last 6 hours**
and materializes one run each through the dedup ledger — exactly the runs a
6-hour outage would have missed.

```bash
WF=$(curl -s localhost:8080/api/workflows "${auth[@]}" \
  -d "{\"name\":\"sales_rollup\",\"spec\":$(spec examples/backfill/sales_rollup.yaml)}" | jq -r .id)

SID=$(curl -s localhost:8080/api/schedules "${auth[@]}" -d "{
  \"workflow_id\":\"$WF\",
  \"cron_expr\":\"0 0 * * * *\",
  \"catchup\":true,
  \"catchup_window_secs\":21600,
  \"catchup_max_runs\":24
}" | jq -r .id)

# Wait one sweep, then observe.
sleep 20
curl -s localhost:8080/api/runs "${auth[@]}" | jq '[.[]|select(.name=="sales_rollup")] | length'   # ≈ 6
curl -s localhost:8088/metrics | grep -E 'scheduler_(catchup_runs_total|schedule_lag_seconds|overdue_schedules)'
```

**What you'll see:** ~6 `sales_rollup` runs created at once, `scheduler_catchup_runs_total`
climb, and `scheduler_schedule_lag_seconds` reflect the oldest missed slot. Run it
again (or restart the engine) — **no duplicates**: the `schedule_backfills` ledger
makes re-sweeps idempotent (`skipped`, not re-run).

> Real-world framing: a schedule **paused for a week then re-enabled** has its
> `last_fired_at` frozen at the pause, so catch-up fills pause→now (clamped to the
> window) instead of stampeding — the anti-Airflow-`catchup` guard. Toggle it live:
> `PUT /api/schedules/$SID {"catchup":false}` … re-enable later with `{"catchup":true}`.

---

## DS-2 — re-run an experiment with new hyperparameters → parameterized re-run (M5)

`rerun` applies to a **failed/cancelled** run, so the pattern is *run → cancel the
default-config run → rerun with new params*. The rerun resets the cancelled cone
and deep-merges `params` into each reset task's stored `input`.

```bash
WF=$(curl -s localhost:8080/api/workflows "${auth[@]}" \
  -d "{\"name\":\"train_experiment\",\"spec\":$(spec examples/backfill/train_experiment.yaml)}" | jq -r .id)

RUN=$(curl -s -X POST localhost:8080/api/workflows/$WF/run "${auth[@]}" | jq -r .run_id)

# prepare-features sleeps 20s → cancel the in-flight default-config run.
curl -s -X POST localhost:8080/api/runs/$RUN/cancel "${auth[@]}" >/dev/null

# Re-run from the cancelled frontier with the NEW hyperparameters.
curl -s -X POST localhost:8080/api/runs/$RUN/rerun "${auth[@]}" \
  -d '{"params":{"learning_rate":"0.05","epochs":"25"}}' | jq
# → {"run_id":"…","rerun":<n tasks reset>}

# The run re-arms and completes; the override is recorded on the reset tasks' spec.
sleep 25
curl -s localhost:8080/api/runs/$RUN "${auth[@]}" | jq '{status, tasks: [.tasks[]|{name,status}]}'
psql "$DATABASE_URL" -c \
  "SELECT name, input::json -> 'input' AS params FROM task_runs WHERE run_id='$RUN' ORDER BY name;"
```

**What you'll see:** `rerun` returns the count of tasks reset; the run finishes
`succeeded`; and the `psql` query shows `{"learning_rate":"0.05","epochs":"25"}`
merged into each re-armed task's spec — a fix-forward without re-authoring the
workflow. (The bundled `sh` tasks echo their `env`; a production training image
reads these merged params from its `input`.)

---

## MLO-6 — re-run inference after a bad model → parameterized re-run (M5)

Same shape, different intent: a bad model (v7) shipped, you roll back to v6 and
reprocess the window. Cancel the bad-model run, rerun with the rolled-back version.

```bash
WF=$(curl -s localhost:8080/api/workflows "${auth[@]}" \
  -d "{\"name\":\"score_batch\",\"spec\":$(spec examples/backfill/score_batch.yaml)}" | jq -r .id)

RUN=$(curl -s -X POST localhost:8080/api/workflows/$WF/run "${auth[@]}" | jq -r .run_id)
curl -s -X POST localhost:8080/api/runs/$RUN/cancel "${auth[@]}" >/dev/null    # pull the bad model

curl -s -X POST localhost:8080/api/runs/$RUN/rerun "${auth[@]}" \
  -d '{"params":{"model_version":"v6"}}' | jq

sleep 25
psql "$DATABASE_URL" -c \
  "SELECT name, input::json -> 'input' AS params FROM task_runs WHERE run_id='$RUN' ORDER BY name;"
curl -s localhost:8088/metrics | grep -E 'scheduler_(auto_reruns_total|incomplete_runs)'
```

**What you'll see:** the cancelled cone re-arms with `{"model_version":"v6"}` merged
into each reset task; the run completes. For a *whole window*, loop the cancel+rerun
over each affected run id — or, when the runs **failed** (not cancelled), enable
`AUTO_RERUN_FAILED=1` and the engine re-arms them for you (bounded by
`AUTO_RERUN_MAX_ATTEMPTS` + cooldown), bumping `scheduler_auto_reruns_total`.

---

## Bonus — manual bounded backfill (M1), explicit window

Once a schedule exists (DE-1), backfill an arbitrary historical range on demand —
capped + deduped against the same ledger the auto loop uses:

```bash
curl -s -X POST localhost:8080/api/schedules/$SID/backfill "${auth[@]}" -d '{
  "from":"2026-06-01T00:00:00Z",
  "to":"2026-06-02T00:00:00Z",
  "max_runs":24
}' | jq      # → {"scheduled":24,"skipped":0,"run_ids":[…]}  — re-run it → "skipped":24
```

## Cleanup

```bash
kill %1 %2 2>/dev/null            # engine + gateway
docker rm -f dagron-pg
```

## Knob reference (engine)

| Env | Default | Meaning |
|-----|---------|---------|
| `AUTO_BACKFILL` | off | enable the catch-up + auto-rerun loop |
| `AUTO_BACKFILL_INTERVAL_SECS` | 60 | sweep cadence |
| `CATCHUP_DEFAULT_WINDOW_SECS` | 86400 | catch-up look-back (per-schedule override: `catchup_window_secs`) |
| `CATCHUP_DEFAULT_MAX_RUNS` | 50 | per-sweep cap (override: `catchup_max_runs`; hard ceiling 1000) |
| `AUTO_RERUN_FAILED` | off | auto-rerun terminally-failed runs |
| `AUTO_RERUN_MAX_ATTEMPTS` | 3 | per-run rerun cap |
| `AUTO_RERUN_COOLDOWN_SECS` | 300 | min gap between a run's reruns |
| `RUN_STALL_SECS` | 3600 | age after which a still-running run counts as `scheduler_incomplete_runs` |
