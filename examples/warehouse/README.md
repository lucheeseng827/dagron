# Warehouse rollup — `{{ ds }}`, `defer:` and a partition that announces itself

[`daily_rollup.yaml`](daily_rollup.yaml) is the shape most warehouse pipelines
actually have: wait for upstream data, transform it somewhere else, announce what
came out.

## What to look at

**`{{ ds }}`** is the fire's logical date (`ds_nodash` is the same day as
`20260914`). It comes from the schedule, the backfill, or the parameter default —
so replaying last March writes March's partitions rather than today's. That is
the whole reason a backfill is worth running.

**`defer:`** means the Spark job holds no worker. The task's `command` is the
*submit*; it prints the job's id, the engine parks the row on it, and a reconcile
sweep polls the status endpoint. Three hundred of these cost three hundred rows,
not three hundred workers — and if every scheduler dies mid-job, the rows are
untouched and whichever replica returns resumes the poll.

Note the two different timeouts, because conflating them is the usual mistake:

| Setting | Bounds |
|---|---|
| `timeout_secs: 300` | the submit command |
| `defer.max_wait_secs: 21600` | the Spark job |
| `run_timeout_secs: 28800` | the whole run |

The executor's default `timeout_secs` is **25 seconds**. Right for a submit,
absurd for a six-hour job.

**`produces:` on a deferred task** records the partition when the *remote job*
finishes, in the sweep — not when the submit returns. A consumer waiting on
`clickhouse://analytics/marts/daily_rollup/{{ ds }}` unblocks at the right moment
rather than six hours early.

**`pool: spark` means something different here.** For a normal task it rations
worker slots. For a deferred one it rations *concurrent remote jobs*, because a
parked row is burning someone's cluster the whole time it waits. With
`POOLS=spark:4` this workflow runs at most four Spark jobs at once — the only
bound on remote spend the open build has, and worth setting.

## Running it

Needs an engine, a Spark operator, and a ClickHouse — so it is written to be read
first. To adapt it:

1. Point `SPARK_NS`, `K8S_API` and `CH_HOST` at your own, via the run's
   `environment:` variables.
2. Put `K8S_TOKEN` and `CLICKHOUSE_PASSWORD` in that environment's secrets. Note
   the SQL step takes its credential in `SQL_PASSWORD` rather than inside the
   DSN — a DSN is masked by nothing, and the step refuses an inline one.
3. Backfill a range: the planner paces the fires and dedupes them, so a
   re-submitted range does not double-run a partition. See
   [`../../docs/BACKFILL_USECASES.md`](../../docs/BACKFILL_USECASES.md).

For the deferral contract itself — the handle protocol, adoption after a crash,
what happens on cancel — see
[`../../docs/EXTERNAL_JOBS.md`](../../docs/EXTERNAL_JOBS.md).
