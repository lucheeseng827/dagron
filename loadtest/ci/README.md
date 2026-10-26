# The CI load harness

Loads a dagron cell the way CI does: pushes arrive at dagron-ci's webhook
door, each becomes a run, each run's job holds an engine slot for minutes and
writes a log. It measures what that costs the cell while a set number of jobs
are running at once.

It has three parts.

- **A synthetic forge** (`forge.mjs`): the part of GitHub's REST API that
  dagron-ci calls for a push. It answers which pipelines a commit has, and
  takes the commit statuses. It answers from memory, for any commit.
- **Synthetic jobs** (`pipelines/load.yaml`): one job that lasts as long as
  its branch name says, writes as many log lines a second as it says, and
  writes an artifact of the size it says. The durability demo's repository,
  `load/durable`, has a pipeline of its own instead
  (`pipelines/load/durable/proof.yaml`): three jobs in a line, each of
  which writes down when it starts and when it ends.
- **A driver and a reporter** (`drive.mjs`, `report.mjs`, `run.sh`): the
  driver signs and sends push deliveries at a set rate and reads each
  engine's `/metrics`; `run.sh` reads the database's statement counts and
  each container's CPU time and memory at the two ends of the measured
  window (`peek.mjs`, from the runtime's own cgroup tree); the reporter
  puts them together. `durable.mjs` checks a durability step. `grafana/` has a dashboard for a run that is
  watched through Prometheus.

## Run it

It needs a container runtime (Docker or Podman) and nothing else.

```sh
CONTAINER=podman ./run.sh build                    # three images from this checkout
CONTAINER=podman ./run.sh up                       # the cell
CONTAINER=podman ./run.sh step first 100 120 180   # 100 jobs of 120 s at once, measured for 180 s
CONTAINER=podman ./run.sh campaign campaigns/stage0
CONTAINER=podman REPLICAS=3 WORKERS=700 ./run.sh campaign campaigns/sweeps
CONTAINER=podman REPLICAS=3 WORKERS=700 ./run.sh campaign campaigns/proof   # webhook to job, and cost
CONTAINER=podman ./run.sh durable one engine mid 5 30   # kill an engine mid-pipeline
CONTAINER=podman ./run.sh campaign campaigns/durable    # every durability step
CONTAINER=podman ./run.sh watch                    # to look: Prometheus, and Grafana with the dashboard
CONTAINER=podman ./run.sh down
```

`run.sh`'s header lists what can be set: the number of engines, each engine's
job slots, images to use in place of built ones, extra engine settings.

A step named `first` leaves its files in `out/first/`: `summary.json` (every
figure below), `deliveries.jsonl` (each delivery and how long its answer
took), `samples.jsonl` (each scrape of the engines and dagron-ci), the two
readings of the database, and for a step with `PARK`, `parked.json` (what was
posted, run by run).

## What a step is

`step NAME JOBS SECS HOLD [LPS] [KIB] [PARK]`

Jobs last `SECS` seconds each. To have `JOBS` of them running at once, the
driver delivers `JOBS / SECS` pushes a second, and keeps doing so whether or
not the cell keeps up (an open loop: a cell that falls behind is not sent
less). After `SECS` the cell holds `JOBS` running jobs if it kept up. The
`HOLD` seconds that follow are the **measured window**. Then deliveries stop
and the driver waits for the cell to be quiet: no event waiting in dagron-ci,
no task ready or running, and each engine's reconcile loop back to its
timer's pace. The forge's counts are read after that, once dagron-ci follows
none of the step's runs (each run's status final, and none of its statuses
due): its last statuses are posted a second or two after their jobs end.
If dagron-ci still follows some after two minutes, the step is read as it
is and is **not final**: `summary.json` says so (`final`, `followed`), its
rows in the campaign's tables are marked, and the step, or the campaign
at its end, fails.

The last is a condition of its own: a step begun with the engines still busy
on the step before it is measured with that in it. An engine takes the
notifications that are waiting together and runs one pass of its loop for
them, so its loop is idle soon after the work is. An engine that does not
(0.11.0 and earlier) runs a pass for each, and works off afterwards the ones
it was sent faster than it could run them. How long the loops stayed busy
with nothing left to run, and how many passes that was, are in
`summary.json` (`drain`).

`LPS` is each job's log lines a second and `KIB` its artifact's size; both
default to none.

`PARK` is work that waits beside the jobs, which the engine's sweeps read
on every pass of its loop: time sensors, approval gates, fan-outs whose
instances still run, HTTP sensors, and time sensors that come due as the
step goes. A sensor or a gate holds no job slot while it waits; a
fan-out's instances run on the engines' workers as jobs do, and hold a
slot each while they run, so `fanouts=50:3600` takes 100 of the slots for
the hour, beside the step's jobs. `drive.mjs park` posts it to the
engines' own API as the step begins, as runs named `park-…`, and
`campaigns/sweeps` says how it is written (`waits=5000:3600,…`; the
grammar is in `drive.mjs`). Its tasks are not jobs: the count of jobs
running leaves them out by their workflows' names, and so do the engine
database's figures. The statement counts cannot: `pg_stat_statements`
does not say which run a statement was for. So in a step with fan-outs,
or with time sensors that come due as it goes, the statements per running
job are theirs and the jobs' together over the jobs alone. An instance
renews its lease as a job does, so `fanouts=50:3600` beside 1,000 jobs
adds about a tenth to a job's heartbeats. A sensor that comes due is a run
made, a task claimed and ended and a run ended, as a job is, so `due=10:30`
(ten a second) beside 1,000 jobs of 120 s (about eight a second) makes
those kinds about 2.2 times what the jobs alone cost; a sensor renews no
lease, so the heartbeats are the jobs' own. An HTTP sensor polls the
forge's `/_load/wait`, which answers 503 and counts each poll. Once the
step is over, `drive.mjs unpark` cancels what is left of it, so that the
next step's sweeps do not read it.
The engines are started with `MAX_INFLIGHT_RUNS=0` for this: their API's
in-flight valve would count the runs dagron-ci made, and turn these away.

## What it measures

All of it is of the measured window.

| Figure | Where it is taken | What it is |
|---|---|---|
| ack | the driver | a delivery sent to dagron-ci's answer (202) |
| event to run | dagron-ci's database | an event received to its run recorded (`ci_events.received_at` to `ci_runs.created_at`) |
| run to job start | the engine's database, and the job | a run made to the job's own first line, which is the time it began |
| tick | the engines' `/metrics` | one pass of the reconcile loop (`scheduler_reconcile_tick_seconds`) |
| notifications | the engines' `/metrics` | the notifications the loops took (`scheduler_notifications_total`); over the passes' count, how many one pass answered. In `summary.json` only, and null for an engine that does not count them |
| dispatch latency | the engines' `/metrics` | a task claimable to handed to a worker (`scheduler_dispatch_latency_seconds`) |
| statements a second, by kind | `pg_stat_statements` | every statement the two databases ran, named by what it is for |
| statements per running job per minute | the same | what one running job costs the database each minute, for the kinds a job causes; in a step whose `PARK` has fan-outs or time sensors that come due (`due=`), their statements are counted in it too (above) |
| a sweep's pass | the same | for each of the reconcile loop's sweeps, by the statement that reads what it has to do: passes a second, and a pass's time, rows and buffers (shared blocks hit or read). In `summary.json` (`statements.sweeps`), and printed for those that ran |
| sweep passes, by engine | the engines' `/metrics` | each sweep's passes a second, all engines, run here or left to the engine that took it (`scheduler_sweep_passes_total`). In `summary.json` only, and empty for engines from before it |
| HOT updates, index reads | the engine database's `pg_stat_user_tables` and `pg_stat_user_indexes` | of `task_runs` and `workflow_runs`: updates a second and the share that were HOT, and each index that was read, how often and how many rows a read. In `summary.json` (`tables`) |
| an HTTP sensor's polls | the forge | how often each of the HTTP sensors `PARK` made was polled, a minute, from when they were posted to when the forge's counts are read after the step (`parked`) |
| CPU, in cores | each container's cgroup | CPU time used over the window, for Postgres, each engine, dagron-api and dagron-ci |
| webhook to job | both databases, and the job | run by run, an event received to its job claimed and to its job's first line, and the parts of it (below). In `summary.json` (`webhook_to_job`) |
| cost per 1,000 jobs | each container's cgroup, the engines' `/metrics`, `pg_stat_statements` | the compute and memory of the cell's own parts over the jobs that ended in the window, and a price (below). In `summary.json` (`cost`) |
| jobs running | the engines' `/metrics` | how many jobs the cell did hold, against how many were asked |

Two of these need saying more of.

- **Tick and dispatch latency are histogram buckets.** A figure such as
  "p99 25 ms" is the upper edge of the bucket the 99th percentile fell in.
- **Run to job start is the harness's own measurement.** The engine records
  when a task became ready and when it finished, and not when it started, so
  the job writes the time as its first line. The clock is the job's.

A statement's kind is given by `report.mjs`'s list of patterns, which name the
statements a job causes (a claim, a lease renewal, a log append, the end of a
task and of a run, a notify, the making of a run) by how the engine writes
them today. Everything else the engine reads and writes of runs and tasks is
in two "other" kinds, and most of that runs once a tick whatever is running.
The counts a scrape of `/metrics` reads are a kind of their own ("metrics
snapshot"): the driver scrapes each engine every five seconds, and what that
costs is not a sweep's. `summary.json` lists the statements that took the
most time and the most calls, with their text, and their rows and buffers a
call.

A sweep is named the same way, by patterns of its own in `report.mjs`. Each
matches the sweep's statement as the engine wrote it before its sweeps had
intervals of their own and were shared (`SWEEP_INTERVALS`, `SWEEP_SHARE`) and
as it writes it after, so that a step of each can be set beside the other;
one that comes to match two statements says so (`statements`).

## The public proof

Three figures, each from a step or a campaign of this harness, and each
of one machine: `campaigns/proof` for the first and the third,
`campaigns/durable` for the second.

### Webhook to job

For each event received in a step's window, run by run, from what the
system wrote down (`collect` in `run.sh`, `latency` in `report.mjs`):

| Point | Written by | Clock |
|---|---|---|
| sent | the driver, as it sends the delivery (`deliveries.jsonl`) | the driver's |
| received | dagron-ci, as it takes the delivery (`ci_events.received_at`) | Postgres's |
| run made | dagron-api, the engine's run (`workflow_runs.created_at`, which dagron-api stamps with its own `chrono::Utc::now()`) | dagron-api's |
| claimed | the engine's claim of the job, in its transaction: the outbox's `task.started` (`event_outbox.created_at`, from `clock_timestamp()`) | Postgres's |
| first line | the job, as its first command (`load-job-started`) | the job's |
| recorded, pending posted | dagron-ci: its run (`ci_runs.created_at`), and the first pending status it posted (`ci_report_log.posted_at`) | Postgres's |

**Received to claimed** is the system's own figure, end to end, on one
clock: both databases are one Postgres's. Cut at the run made, its two
parts each cross from Postgres's clock to dagron-api's or back.
**Received to first line** adds the job's start, by the job's clock;
**sent to first line** adds the driver's delivery. On one machine every
container reads one kernel's clock, so these add up; on several they would
not. The `task.started`
event is written while a dagron-ci claims the cell's outbox, which it does
from its start: a step whose runs have no claim counts them
(`without_a_claim`). Every event received in the window is counted
(`events`), and one that never became a run is counted apart
(`events_without_a_run`): it has no span to measure, and is not left out
of the counts.

### Cost per 1,000 jobs, on compute you bring

What the cell's own parts used over the window, over the jobs that ended
in it, times a price (`cost` in `report.mjs`):

- **compute**: the CPU time of Postgres's, dagron-api's and dagron-ci's
  cgroups, and of the engines' own processes (`process_cpu_seconds_total`);
- **memory**: Postgres's, dagron-api's and dagron-ci's cgroups
  (`memory.current`, which counts the page cache they caused), the mean of
  the window's two readings, and each engine process's resident set at
  its end, held for the window;
- **the price**: `PRICE_VCPU_SECOND` and `PRICE_GB_SECOND`, by default AWS
  Fargate's Linux/x86 on-demand price in US East (N. Virginia), as its
  pricing page gave it on 2026-10-24: $0.000011244 a vCPU-second and
  $0.000001235 a GB-second, a GiB taken for a GB.

A step with a reading missing, a priced part's CPU or memory at either end
of the window or an engine process's CPU or resident set, has no price:
`cost` says `incomplete` and which readings it lacks (`missing`), and
prices nothing as zero.

**The jobs' own compute is not priced:** with compute you bring, it is
your machines'. Here a job is a process in its engine's container (the
local executor), so it is shown apart, as the engines' cgroups less the
engines' own processes. Also shown, and not priced: Postgres's time
executing statements (which is in its CPU) and the databases' growth.
It is a measure of what was used, not of what a deployment would be
sized at: one machine, every part on it, and nothing idle beside it.

### The durability demo

`run.sh durable NAME TARGET AT DOWN [PIPELINES] [SHAPE]` pushes PIPELINES
commits to `load/durable`, kills TARGET with SIGKILL (`podman kill -s
KILL`), starts it again DOWN seconds later, waits for every pipeline to
end in dagron-ci, and has `durable.mjs` check what happened. TARGET is
`engine` (the first engine), `engines` (every engine at once), `ci` or
`api`. AT is `mid`, once every pipeline's second job runs, or `intake`,
once a third of the pushes (sent two a second) were taken: the rest find
dagron-ci, or dagron-api behind it, down. A push that is not taken is sent
again with its delivery id, as GitHub redelivers, and five that were taken
are sent again once all were.

Each job's start and end is a line on a volume every engine mounts
(`/marks`), which outlives an engine killed. The checks, each from what the
system or the jobs wrote down:

- every push is one event, every event is done with one run, and the
  engine made one run for each commit;
- every pipeline succeeded, in the engine and in dagron-ci, and every job;
- every job ran to its end exactly once; an execution that never ended was
  on a container that was killed and began before the kill; a job's
  execution that ended is the one of it that began last, and none of it
  began after one had ended; no job began before the one it depends on
  ended;
- at the forge, each job's last status is success, posted once, and none
  was a failure; dagron-ci's own record (`ci_report_log`) matches the
  forge's, status for status (commit, context and state). dagron-ci writes
  its row after the post, so a dagron-ci killed between the two leaves a
  post with no row: that is allowed only when dagron-ci was what was
  killed and the post came while the kill was being made, and a row the
  forge never had is never allowed;
- the kill caught the pipelines in flight: an execution was running on
  what was killed, or one began or ended while it was down, or a run was
  recorded after the kill. A demo that killed an idle part shows nothing.

A step whose checks do not hold fails, and a campaign with one fails at
its end. `durable.json` has each check and how long recovery took: the
kill to the jobs begun again, and the first push to the last pipeline's
end, against the jobs' own length.

**A job cut short with its engine runs again from its start.** On the
local executor a job dies with its engine's container, and is claimed
again once the dead engine's lease lapses (30 s by default). On Docker a
job's container outlives the engine, and the engine stops it before the
next attempt's starts; this harness does not run Docker jobs, and does not
show that.

## What it does not measure

- **Not a deployment's capacity.** The whole cell is on one machine: Postgres,
  the engines, dagron-api, dagron-ci, the forge, the driver and every job
  share its CPUs and its disk. What bends first here may not be what bends
  first where each has a machine of its own.
- **Not what a job costs to run.** Jobs are processes inside the engine
  containers (the local executor). There is no pod, no container, no image
  pull and no network between engine and job. The Kubernetes and Docker
  executors' own costs (a poll of each pod, a log read back from the runtime)
  are not here. So webhook to job has no container start, image pull or
  checkout in it: on Docker or Kubernetes the job's first line comes after
  those.
- **Not a real forge.** The forge never refuses, limits or pages, holds no
  git objects, and answers in microseconds. A job does not check out.
- **Not the artifact API.** A job's artifact goes to the engine's local
  artifact directory when it is given one, and through no API.
- **Not a long run.** A step is minutes. History that grows over days
  (retention, table size, the archive) is not exercised.
- **One repository, one pipeline, one job a run, for load.** No fan-out, no
  dependencies between jobs, no concurrency keys. The durability demo's
  pipelines have three jobs in a line, and are not load. What `PARK` adds beside
  them (fan-outs among it) is there for the sweeps to read, and none of it is
  measured as a job.
- **Not a bill.** The cost per 1,000 jobs is use at a list price: no
  reserved or spot price, no egress, storage, load balancer, backups, or a
  machine's idle headroom.
- **Not every way to fail.** The durability demo kills one process at a
  time with SIGKILL and starts it again on the same machine. Not Postgres,
  not a machine lost with its disk, not a network cut, and not two parts at
  once but the engines together.

## The dashboard

`grafana/dagron-ci-load.json` is for a cell watched through Prometheus. It
reads the engines' and dagron-ci's own series. Its two database panels read
what the usual exporters publish (cAdvisor for the Postgres container's CPU,
postgres-exporter for transactions a second), and say so.

`run.sh watch` starts a Prometheus that scrapes the cell, postgres-exporter,
and a Grafana with the dashboard loaded, and prints where Grafana is. Nothing
a step measures comes from them, and a Prometheus that scrapes the engines
every few seconds is load of its own: watch a step to look at it, not to
measure it. The cAdvisor panel stays empty there: a cluster's kubelet has
cAdvisor, and one machine's runtime does not.

It does not show the harness's own two latencies, which are in no
Prometheus: run to job start, and event to run. The engine's share of the
first is on it, as dispatch latency. Heartbeats a second is drawn from the
number of running tasks and the renewal interval, which is what the engine
does by design; the measured figure is the reporter's, from the database.

`grafana/check.mjs` says whether a live cell gives the dashboard what it
draws: read against each `/metrics`, which of its series the cell does not
publish; asked of a Prometheus, which of its queries had no series.
