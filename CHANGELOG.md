# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/), and this project adheres to
[Semantic Versioning](https://semver.org/) (while dagron is 0.x: a minor bump may be breaking).

## [Unreleased]

### Added
- **`dagron-step-sql parity`: is a partition the same on two stores?**
  The check a warehouse move is cut over on. Each side is read under its own prefix (`PARITY_SOURCE_*`, `PARITY_TARGET_*`) with
  the step's own variables and transports, so every credential, deadline and
  budget rule holds. Two numbers per side, computed where the data is: the
  row count, and the sum over rows of 60 bits of the MD5 of each row's
  canonical text, which ignores row order and counts a duplicate twice.
  Columns come from each side's `information_schema` and are compared by
  family: integers and decimals as value × 10^scale, floats rounded to
  `PARITY_FLOAT_DIGITS`, booleans as `1`/`0`, dates and timestamps as days
  and microseconds since 1970, text as itself. Cells are length-prefixed, so
  NULL is not `''`. A column only one side has is a difference, and a type
  outside the families is refused. Exit `0` equal, `2` shown different, `1`
  not compared, with `parity.json` in `$DAGRON_ARTIFACTS`. Shown on a live
  Postgres to catch a dropped, a duplicated and a changed row and to pass a
  faithful copy (`tests/parity.rs`, now run by `scripts/ci-test.sh` against
  the Postgres it starts). The Redshift, Snowflake, BigQuery and Databricks
  forms are unit-pinned and not yet run against those stores.
- **`SQL_ENGINE=duckdb`: the engine a copy uses where a store cannot read or
  write Parquet.** The DuckDB CLI as a child process, with
  the script on stdin, `-bail`, stdout dropped, stderr kept, the deadline
  killing it, and `SQL_PASSWORD` handed over as `PGPASSWORD` for a Postgres
  it attaches through libpq's environment. `exec` and `script` only. The
  transports now take a `Settings` (one store's connection, read under a
  prefix) and hand rows to a sink the caller owns, which is how one process
  reaches two stores; behaviour under `SQL_*` is unchanged.
- **dagron-state compiles a warehouse move.** A
  `freshet move --json` document (contract `freshet-move/1`) becomes
  `<unit>__unload → <unit>__load → <unit>__parity` per partition, the units
  independent of one another, at `POST /api/state/moves` and
  `/moves/submit`, or offline with `dagron-state move [--json]`. The move
  names no host and holds no credential: connections come per kind of task
  (`options.unload`, `.load`, `.parity`, each `env` and `secret_env`), and
  options may not set what the move sets. `check_only` compiles just the
  parity tasks for a side-by-side period. Tested on the producer's own
  captured output, and parsed through dagron's real `DagGraph::from_yaml`.
- **`dagron-step-sql` runs Snowflake, BigQuery and Databricks.** `SQL_ENGINE=snowflake`, `bigquery` and `databricks` drive each
  vendor's statement API over HTTPS (Snowflake's SQL API v2, BigQuery's
  `jobs.query`, Databricks' Statement Execution API 2.0). Each submits, polls
  the handle it gets back, and reads the result a page at a time. Every
  dialect `freshet plan --sql` renders now has a step that runs it, and a
  dialect's name is its `SQL_ENGINE`, so `SQL_ENGINE: "{{ dialect }}"` serves
  all five. The DSN is the API's host with the session as query parameters
  (`?warehouse=…`, `?project=…&dataset=…`, `?warehouse_id=…`); an unknown
  parameter is refused, and so is plain `http://` to anywhere but loopback.
  The token rides in `SQL_PASSWORD` (`SQL_TOKEN_TYPE` names Snowflake's
  kind). BigQuery with none takes its service account's token from the GCE
  metadata server. All four modes work. A script goes to Snowflake with
  `MULTI_STATEMENT_COUNT=0`, to BigQuery as it is, and to Databricks as one
  `BEGIN … END` compound statement. These transports hold a handle, so a
  statement still running at `SQL_TIMEOUT_SECS` is **cancelled** rather than
  left running, and Snowflake is given the deadline as its own `timeout`. A
  poll or a page lost in transit is retried; a submit never is. The budget
  reaches the store where the API lets it (one row past `SQL_MAX_ROWS` as
  BigQuery's `maxResults` and Databricks' `row_limit`), and a page is read
  whole but refused while still arriving once it passes twice `SQL_MAX_BYTES`
  plus 1 MiB. Values are the APIs' text, not re-typed. They are in the default
  build, with no driver beyond the HTTP client; reqwest gains `gzip`, for
  Snowflake's compressed partitions. Tested against stand-ins in each API's
  documented shapes (`tests/statement_api.rs`), not yet against the services.

### Changed
- **dagron-ci: a job that failed and passed when a person reran it is a
  flake.** Until now only the engine's own retries counted
  (`CI_FLAKE_ISSUES`). A rerun starts a job's attempts again, so a job that
  failed, was rerun (`dagron-ci rerun`, a check's Re-run, the console) and
  then passed on the same commit filed nothing. Now a run that was rerun has
  each passed job's attempts read, and a failed life before the pass counts.
  A job that passed and was rerun whole, or was cancelled and rerun, is not
  a flake. Each run's job is still kept once.

### Fixed
- **Release: an armv7 binary.** The 32-bit ARM leg fetched its cross
  toolchain from musl.cc, which never answers GitHub's runners, so no
  release had one. It now builds with cargo-zigbuild (Zig from PyPI, pinned
  by sha256) and is run under `qemu-arm` before it ships: `--version` and
  `validate`, as the native legs are.
- **Release: a signature is verified until Docker Hub lists it.** v0.11.0's
  image merges and its chart failed "no signatures found" seconds after
  signing, and verified later. Verification is now tried for a minute
  before it fails.
- **dagron-ci: a GitHub App token GitHub failed on its side is asked for
  again.** github.com answers an installation token's request with an empty
  500 now and then, and the next request is answered: in the first run
  against github.com, two mints in eleven, each failing a job at its
  checkout. A mint's two calls are now sent up to three times where GitHub
  answers a 5xx or does not take the connection, 250 ms and then 500 ms
  apart, inside the 8 s a mint already had. A refusal, a rate limit and a
  timeout are not sent again.

## [0.11.0] - 2026-10-26

### Added
- **The engine and dagron-api read the connection limits they run under.** On Postgres each reads, as it starts and
  every 5 minutes after, its database's `CONNECTION LIMIT`, its login's,
  and the server's `max_connections` less its reserved slots, with the
  sessions it may see under each. It says, once for each set of limits,
  where its pool and its LISTEN session are past one (it will be refused
  sessions) or past half of one (it will be while a rollout runs a second
  pod beside it), and never stops on it. The engine publishes them as
  `scheduler_db_connection_limit{scope}` and
  `scheduler_db_connection_sessions{scope}` (`database`, `login`,
  `server`; a scope with no limit has no limit sample), and dagron-api as
  `db_connections` in `GET /api/metrics`. A build may add databases of its
  own to the engine's look (`Seams::connection_checks`).
- **dagron-ci's public proof, through the load harness.**
  `loadtest/ci` measures webhook to job from the system's own timestamps
  (the event received and its job claimed as the outbox's `task.started`
  has it, both of one Postgres's clock, with the engine's run made, by
  dagron-api's, between; then the job's first line), and what the cell's
  own parts cost per 1,000 jobs
  on compute you bring (the CPU and memory of Postgres, dagron-api,
  dagron-ci and the engines' processes, at a stated list price; the jobs'
  own compute shown apart). `run.sh durable` is the durability demo: it
  kills an engine, every engine, dagron-ci or dagron-api with SIGKILL at
  intake or mid-pipeline, starts it again, and checks twelve things of
  what the databases, the forge and the jobs wrote down, a failed one
  failing the step. On one machine, in fresh cells, as measured on
  2026-10-24:
  23 to 34 ms from the event to its job's claim at the median and 78 ms
  at most at the 99th percentile, to 1,000 jobs at once; every pipeline
  finished, and each job's last status was posted once, whichever of
  those was killed (Postgres and the forge were not), a job
  cut short running again from its start once the lease lapsed (24 to
  30 s); $0.0018 to $0.0054 per 1,000 jobs at AWS Fargate's on-demand
  price, Postgres most of it. The harness now reads dagron-api's and
  dagron-ci's CPU too (`peek.mjs`, from the runtime's cgroup tree; their
  images have no shell), and its forge serves a repository its own
  pipelines.
- **dagron-ci: GitLab's merge trains**. A GitLab
  project's `Pipeline Hook` of a merge train's pipeline
  (`refs/merge-requests/{iid}/train`, which the hook does not say: it names
  the merge request's source branch for each of the request's pipelines, so
  GitLab's pipelines API is asked, with the host's token, by the pipeline's
  id, for 5 seconds at most) that has yet to end, as GitLab says too, is a
  merge group's
  event, as GitHub's `merge_group` is: the pipelines that take
  `on.merge_group` run at the train's commit, trusted, once a commit however
  many of its steps GitLab tells, and their statuses go to that commit under
  the train's ref, named `(merge_request)`, so GitLab counts them in the
  train's pipeline and merges on them. A train pipeline cancelled has its
  runs cancelled. The project's webhook needs Pipeline events, and the
  train's pipeline is GitLab's own: until dagron-ci's first status is in, it
  holds the project's `.gitlab-ci.yml` jobs alone, so a job for the train
  that ends first lets the request merge without dagron-ci's checks
  (the README says how to give it one that outlasts them).
- **dagron-ci: how a repository is served, and the repository by its id**. `dagron-ci repo add|set … --mode native|compat|fleet`,
  and `mode` on `PUT /ci/v1/repos/…` (migration 0035, `ci_repos.mode`):
  `native` runs the repository's `.dagron/ci` pipelines; `compat` those and
  the forge's own files its `.dagron/ci.yaml` names, and a GitHub workflow's
  schedules; `fleet` none, as its forge runs them: its pushes and merge
  requests are answered and dropped (a merge group let go of still cancels
  its runs from before), it is not swept, and a run asked of it, or asked
  again, is refused (409). A job that asks for a fleet label is served in every
  mode. Each change of mode is recorded with who made it, and one out of
  `fleet` sweeps from a baseline. `GET`, `PUT` and `DELETE
  /ci/v1/repos/{id}` take the id dagron-ci gave a repository in its name's
  place; a `PUT` of a disconnected one's id connects it again, on the forge
  it was on, as a `PUT` of its name and `dagron-ci repo add` now do too.
- **dagron-ci: admission by caller**. A build's
  `Admission` may count the runs in flight by caller as well
  (`Admission::admit_caller`, asked with the run's caller, the actor its event
  names, and its trust class, where `by_caller` asks for `Load::by_caller`).
  A slot keeps its caller (`ci_admissions.actor`, migration 0034), and a run
  counts for its event's actor. A build whose admission says nothing of
  callers is asked as before.
- **A run's artifacts from a container: `artifact_api: true`**. A task in a container, a pod or on a remote runner, which
  does not see `DAGRON_ARTIFACT_DIR`, is given `DAGRON_ARTIFACTS_API` and a
  credential of its attempt's own, `DAGRON_ARTIFACTS_TOKEN`, with which,
  while it runs, it reads its run's artifacts and writes its own task's
  through dagron-api (`GET`/`PUT /task/v1/artifacts/{task}/{name}`), in the
  store an approval's `binds` is checked against. The engine's operator
  offers it with `DAGRON_ARTIFACT_API_URL` (`https://`, or
  `DAGRON_ARTIFACT_API_INSECURE=allow`); a remote runner uses
  `DAGRON_RUNNER_ARTIFACTS_URL`, else its dagron-api's. The fix agent's
  jobs use it, so its patch and the approval of it no longer need the
  store's directory mounted into their containers.
- **What a model call spent, said by the task**. A
  task that prints a line `dagron::tokens=<n>` has every such line taken out
  of the output the engine keeps, so `{{ tasks.X.output }}`, `when:`,
  `repeat.until` and `env_from` read what it printed and not its report; the
  fix agent prints each turn's. `tokens:` on a command task says what it may
  spend, which it reserves of a token budget where one is kept: 1 to 2^48.
  `budget: { tokens }` is refused by this build, which keeps no meter, with
  a pointer to what bounds a run here (signpost gate `token_budget`).
- **The fix agent's model reached with its run's identity**, not a key (`examples/ci/agents/fix.yaml`). Its `fix` job sends
  the OIDC token of its run for a gateway's audience where the task is given
  `identity`, and the environment's `model_api_key` where not, which is now
  optional; eggrd checks the token against dagron's issuer, injects the key
  its claims choose, and holds each run to a budget of its own
  (`claim:run_id`). The agents README shows both halves set up.
- **An environment's policy gives an agent's run an identity, and can
  protect the environment**. `allowed_identity:
  ["agent"]` lets an agent's task in the environment hold an identity (OIDC
  tokens saying `"trust": "agent"`), asked at dispatch and again by the
  issuer at each token; `agent` alone may be named. `approvers` makes it a
  protected environment: each task of a run that is not trusted that names
  it waits on a gate of those approvers (`<task>.environment`), put in as
  the run is made and rejected after a day with no answer. A trusted run
  is not asked. Two columns on `environment_policies` (a migration).
- **dagron-ci: an issue of a job that keeps flaking** (`CI_FLAKE_ISSUES=N`,
  `CI_FLAKE_DAYS`). Off by default. A job of a trusted
  run that passed only after an attempt of it failed is kept once a run,
  and at its Nth such flake within the days (14 unset) its repository is
  filed one issue of it, which lists each, and is written again in place
  as it flakes again. A `repeat:` task's passes are not flakes. A person
  closes the issue, and the job's flakes are counted again from then. The
  App writes it with a token for `issues: write` alone (an App without the
  permission files none, and the rest goes on), or the GitLab host's token.
- **dagron-ci: an agent fixes a failure in CI, up to an approval of the
  exact patch** (`examples/ci/agents/fix.yaml`). When
  `ci` fails on main, a job asks a model for a patch, runs `ci` on it as
  the agent (a patch bundle), and does so again, up to four turns, until a
  run of it passes. A maintainer then approves the patch's bytes
  (`binds: [fix/patch.diff]`), and a last job checks the approved digest,
  pushes the patch to a branch and opens a pull request. It needs dagron's
  artifact store where the job runs, which holds the patch the approval
  binds.
- **dagron-ci: a run of a working tree's change, not pushed**
  (`dagron-ci run --remote --bundle-from DIR`). It
  makes the patch bundle of the git working tree at DIR since `--sha` or
  its `HEAD` (commits since, staged, changed and untracked files), uploads
  it, and runs it. The same change makes the same bytes. dagron-mcp's
  `dagron_ci_run` takes `bundle_from`, for a tree under the directory it
  was started in (or `DAGRON_MCP_WORKTREE`). `GET /ci/v1/runs/{id}` says the
  bundle a run is of and its SHA-256 (`bundle: {id, sha256}`).
- **dagron-ci: a pipeline's job asks for a forge token**
  (`forge_token: { permissions: { contents: write } }`), as a workflow's
  job is given its `GITHUB_TOKEN`: by a GitHub App's names, each `read` or
  `write`, read alone for a run that is not trusted, and refused for a job
  that checks out nothing. It was refused as reserved. `dagron-ci compile`
  takes `--event failed`, and `validate` names `schedule` and `failed`
  among what a pipeline runs on.
- **dagron-ci: `on: failed`**. A pipeline runs when a
  run of another of the repository's fails: `failed: [ci]`, or `failed: {
  pipelines: [ci], branches: [main] }`. The reporter raises the `failed`
  event in the write that records the run's end, once for each failure, at
  the failed run's commit, ref, actor and trust, and the run gets the failed
  run's id, pipeline, first failed job, fault class and disposition as
  `CI_FAILED_*`. Only a trusted run's failure raises one: a fork's, an
  agent's and a patch bundle's raise nothing, and a failure's own run none.
- **Sweeps that scale.** Each of the reconcile
  loop's sweeps of parked tasks reads an index of its own: the fan-out
  barriers, the time, HTTP and dataset sensors and the sub-workflow
  triggers, of running rows alone and led by `status` (Postgres 101 to 105,
  each `CREATE INDEX CONCURRENTLY` in a migration of its own; SQLite 076,
  with the lease index SQLite never had and one for the external polls), so
  a sweep of parked tasks no longer reads the jobs running beside them.
  Measured on one machine with `loadtest/ci`'s `campaigns/sweeps`, the
  images before and after in one session:
  with jobs alone, nothing parked, the fan-out and sensor sweeps read one
  buffer a pass at 500, 1,000 and 2,000 jobs, where they had read 135 to
  510, and each shared sweep ran a third as often; beside 5,000 parked time
  sensors and 50 fan-outs, none of them due, the time and fan-out sweeps
  read 43 and 163 buffers a pass, where they had read 1,119 and with
  nothing parked read 1: what is parked still adds to both, the fan-out
  sweep's by probing each barrier, the time sweep's by what the
  measurement does not name. New settings: `SWEEP_INTERVALS`
  (`name:ms,…`, each at most a day: one longer stops the start),
  `SWEEP_SHARE` (`on`), `METRICS_HISTORY_CACHE_SECS` (300, or 0 where it
  is not set and `METRICS_CACHE_SECS` is 0), and dagron-api's
  `DAGRON_METRICS_CACHE_SECS` (2) and `DAGRON_METRICS_HISTORY_CACHE_SECS`
  (300, or 0 where it is not set and the first is 0). `SWEEP_INTERVAL_MS`
  and `REAP_INTERVAL_MS` above a day are a day, where a sweep so set ran
  only at the start: a slot far longer is one Postgres cannot write. New
  metrics:
  `scheduler_sweep_passes_total{sweep,outcome}`,
  `scheduler_sweep_seconds_total`, `scheduler_sweep_rows_total`,
  `scheduler_sweep_last_pass_timestamp_seconds`,
  `scheduler_sweep_interval_seconds`, `scheduler_sweep_lateness_seconds`,
  `scheduler_metrics_history_age_seconds` and
  `scheduler_db_invalid_indexes`. A sweep records its last pass whether or
  not it found anything to do, and one switched off on an engine shows an
  interval of 0 there, so the alert on a sweep that has stopped
  (`docs/METRICS.md`) is quiet on an idle or a stock engine; it looks at
  one installation at a time, and waits five minutes at the least, so that
  the age of the last scrape does not set it off. An index a cut-short
  build left INVALID, which `IF NOT EXISTS` takes for built when its
  migration runs again, is looked for as the engine starts and every five
  minutes after, logged with the `REINDEX INDEX CONCURRENTLY` that
  rebuilds it, and gone from the gauge at the next look once it is
  rebuilt.

  **Upgrading.** On Postgres, migrations 101 to 105 build five indexes of
  `task_runs` concurrently, each a scan of the table twice that waits out
  older transactions; the first engine to take the migration lock
  migrates while the others wait for it, on a direct connection. Through a
  pooler in transaction mode the lock keeps no two engines apart, and two
  that build one of these indexes at once deadlock (Fixed, above): there,
  upgrade with one engine. 106 adds the table `sweep_slots`
  and sets any HTTP sensor parked with no next poll due now. On SQLite,
  076 builds seven indexes of `task_runs` (the five, the lease index and
  one for the external polls) in its own transaction, and 077 sets those
  HTTP sensors due as 106 does. Engines from before this ignore the slots
  and sweep every window themselves, which is safe: every shared sweep may
  run on two engines at once. Set `SWEEP_INTERVALS` the same on every
  engine: the engine that takes a slot sets the cadence for the window. A
  role granted table by table needs `SELECT`, `INSERT` and `UPDATE` on
  `sweep_slots` for the engine.
- **What the log store keeps leaves it once nothing reads it: the store's GC.** With `DAGRON_LOG_STORE`, every
  engine with the store removes from it what no task row names: the logs of
  a task whose row was deleted (by the retention GC, an archive's purge, the
  engine's or dagron-api's, a rerun's or a fan-out's drop, or by hand),
  about a minute after it went, and a writing of a log a later one took the
  place of (a seal, a retry, a rerun, a clear) at once. Each is recorded in
  `log_sweeps` in the statement that deletes the row (a trigger) or in the
  reset's transaction, and taken by lease, so engines sweep side by side,
  leader or not, and the row decides what stays. A record is let go once a
  look finds nothing left, and looked at again 15 minutes after one that
  removed something; a removal that fails is tried again, backing off to six
  hours. `DAGRON_LOG_SWEEP_SECS` (30; `0`, this engine removes nothing). New
  metrics: `scheduler_log_store_removed_objects_total`,
  `scheduler_log_store_removed_bytes_total`,
  `scheduler_log_sweep_failures_total`, `scheduler_log_sweeps_waiting`,
  `scheduler_log_sweeps_failing`, `scheduler_log_sweeps_oldest_age_seconds`.
  The engine's log batch no longer removes the writing before on the
  reconcile loop: a seal's `HEAD` and `DELETE` calls are off it, and a
  removal that failed no longer loses the new writing's first text; and an
  engine that had not written a task whose row is gone no longer makes a
  file of it. Not built: an attempt's last part closed into the store as
  the attempt ends (on `file://`, a finished writing's duplicate `.open`
  parts stay until the writing is superseded or its task deleted).

  **Upgrading.** The migrations (Postgres 098, SQLite 073) add
  `task_runs.log_writings` (nullable with no default, so no table rewrite;
  a row with closed segments is marked), the table `log_sweeps`, and a
  row-level trigger on the deletion of a `task_runs` row. It writes only
  for a row whose log the store holds: an install with no store pays next
  to nothing. With one, a large purge (`gc_old_runs` is one transaction)
  writes a record for each such task, and takes longer for it: deleting
  200,000 task rows on Postgres 16 in a container here took 21.7 s with no
  trigger, 22.0 s with it and no row marked, and 30.5 s with every row
  marked. Not recorded, and left in the store until cleared by inserting a
  record for each (`docs/BACKUP_RECOVERY.md` §6.9): tasks deleted before
  the upgrade, `file://` tasks written before it that never closed a
  segment, and tasks an older engine began during a rolling upgrade (an
  older engine also goes on removing a seal's writing before itself, which
  is safe beside a new one). A role granted table by table needs `SELECT`,
  `INSERT` and `UPDATE` on `log_sweeps` for every process that deletes
  runs (the trigger's upsert reads the record it updates): dagron-api's
  archive route, and the engine, which also needs `DELETE` for its sweeper. Every engine and dagron-api must
  name one store, and an engine that sweeps must never be run against a
  copy of the database that names the production store.
- **A log past its cap keeps its end: head and tail.** A task's log was kept up to `DAGRON_LOG_TASK_CAP_BYTES`
  and ended there, on a line that said it stopped: a job that wrote much and
  then failed lost the lines that said why, and dagron-ci the marker of its
  last step's end. Now the log is held at the cap while the task runs, and
  once the attempt is over its last `DAGRON_LOG_TAIL_BYTES` (a quarter of
  the cap by default, on top of it and no more than it; `0`, the beginning
  alone, as before) are added, from a line's start and never from inside a
  secret, masked as one text, under a line that says how much of what the
  task wrote was left out, in bytes and lines counted before masking
  (`[dagron] 1.2 GiB of this attempt's output (1288490189 bytes and 3456789
  lines, counted before masking) is left out here; what follows is its
  end`, a form `docs/LOG_PIPELINE.md` §7.4 gives). A seal that writes the
  log again writes its end in the same text; a log no executor sealed says
  its end was not kept, a cancelled one among them; one whose claim went
  to another ends where it was held. The log stops at an exact byte of what the task wrote, so the
  counts add up, and the sink no longer masks what it does not send. New
  metrics: `scheduler_task_logs_capped_total`,
  `scheduler_task_log_bytes_omitted_total`, and an info line for each
  attempt past its cap. dagron-api keeps a runner's log up to twice the cap
  and 1 MiB (`dagron_core::runner::session_log_most`), where it kept the
  cap alone and refused the segment that carried the line a capped log
  ended on, and with it the attempt's notes; and a runner sends a task's
  last segments and then its result from a task of their own, trying a
  segment whose answer was lost again for about four seconds, so that a
  long end holds up no other task's log or result and one lost answer no
  longer drops it. No migration.

  **Upgrading.** The line a capped log is held at now reads `[dagron] this
  attempt's log reached 64 MiB (DAGRON_LOG_TASK_CAP_BYTES): what the task
  writes from here is held back, and the last 16 MiB of it
  (DAGRON_LOG_TAIL_BYTES) is added below when it ends`, and its twin for a
  task with secrets (`… counted before masking, passed …`) likewise: an
  alert or a script that matches `and stops here` must change, or set
  `DAGRON_LOG_TAIL_BYTES=0`, which keeps the old lines and the old log (but
  that a log of a task with secrets, written again by its seal, now ends
  where it stopped, a few bytes before where it did). A
  capped log is now up to 1.25 times the cap by default, and twice it at
  most: in its task row with no log store, in a remote runner's row, in the
  log store, in an archive, and in what a one-shot log view puts together.
  Upgrade dagron-api before its remote runners: an older dagron-api keeps a
  runner's log up to its cap alone, and answers a newer runner's segments
  of a log's end `409`, which loses that end and the notes after it, as it
  lost the cap's line before. Older runners against a newer dagron-api, and
  going back, need nothing.
- **dagron-ci's seams for a build's own rules and audit trail.** A build
  that composes dagron-ci (its binary is `dagron_ci::cli::run` with its own
  `Seams`) can now pass:
  - `governance`: pipelines it requires of a repository, run beside the
    repository's own as `.dagron/required/<name>.yaml` (a file of the
    repository's of that name is refused); its word on each run once it is
    compiled, before it is submitted, which refuses the run as a refused
    pipeline is refused, on the event and on the commit; and the person a
    forge login is, which the run's trigger record names in place of the
    login.
  - `audit`: told of each act dagron-ci records, where it is done: a
    delivery refused for its signature or token, a run submitted, a pipeline
    refused, a cancel and a rerun through dagron-ci's doors with their
    caller, a repository connected, changed or disconnected through the API,
    and each checkout credential, by its SHA-256.
  - An admission that reads the runs in flight by repository
    (`Admission::admit_run`, `by_repository`). Admission slots name their
    repository (migration 0030, on both databases).

  The open build passes none, and none of this changes what it does.
- **Docker and Kubernetes task logs followed as a task writes them.** The Docker executor attaches to a
  task's container before it starts and reads its stdout and stderr as the
  task writes them, whole lines into its log, so the live relay follows a
  Docker job's log as it runs. A task's own log no longer depends on the
  container's log driver: the journal's rate limit, `local`'s 100 MB, a
  daemon's rotation and `mode=non-blocking` no longer cost it lines. What
  is read back from the driver once a container has stopped still does: a
  Docker action's step output, the words of a container that failed before
  its task started, and a task's log where its container could not be
  attached to or the follow of it was cut, which is read back after what
  was followed, and says so where it cannot be placed. The engine's warning
  of such a driver now begins `what is read back of a container may not be
  whole`. A task that runs out of time keeps what it wrote, before the line
  that says it timed out. The Kubernetes executor follows a task
  container's log from when the container has started (with each line's
  time, so that a follow cut while the pod runs is opened again from where
  it got to, no line twice but where a line was written across a rotation
  of the kubelet's), where a Kubernetes task had no log and its
  output was one read of the kubelet's current file once the pod had ended;
  why its pod failed is in its log too. What either executor holds of a
  running task's output is bounded, where the Docker executor held all of
  it: of its beginning, `DAGRON_LOG_TASK_CAP_BYTES` and a few hundred KiB
  more, for its log; of its end, up to twice `DAGRON_LOG_TASK_CAP_BYTES`
  and a few hundred KiB, for its output (whose line that says what is not
  kept counts what the executor did not keep as the task wrote it, and is
  masked from past a secret that end begins in); more of each with a
  secret over 64 KiB. So that the first is bounded as the task wrote it, **a log of a
  task with secrets also stops once what the task wrote, counted before it
  is masked, is past the cap by 64 KiB** (or by its longest secret), on a
  line that says so (`… counted before masking, passed 64 MiB …, and its
  log stops here`): masks shorten a text, and a log of a secret printed
  again and again would reach its cap only far past it. A log's last
  masking, of the whole, ends where the log stopped, on that line or the
  cap's; before, it went on to the cap with what the task wrote after,
  and could end in part of a secret. **A
  Kubernetes task's log view now shows its log**, kept from its
  beginning up to `DAGRON_LOG_TASK_CAP_BYTES`, where it showed its
  output's end. What waits to be written of a log waits a second more for
  each whole MiB its append would rewrite, up to 30 s, so that a long log
  is not rewritten whole every second. In a log, which is masked a line
  at a time as it is written, each line of a secret of several lines that
  could be secret is masked on its own too: a line of 8 characters or
  more, or, where it is a `key: value` (or `key = value`), one whose value
  is (so not `password: hunter2`); not a key with no value, an INI file's
  section, nor a PEM key's armour alone on its line. An output is masked
  with whole values, as before. And in either, **the value of a line of a
  secret of several lines whose key names a secret**, as a sensitive env
  var's name does (`token: …`, `password = …`, `AWS_SECRET_ACCESS_KEY=…`,
  `DAGRON_SENSITIVE_ENV_PATTERNS`), or is one that holds a secret in such
  files (a kubeconfig's `client-key-data`, a Docker config's `auth`, a
  `passphrase`), is masked on its own, where it is of 8
  characters or more: a task that reads it out of the whole may print it
  alone. Not yet: retention and the store's
  clean-up, the console following a log in place of polling it.
- **`dagron-ci login` and `logout`, and `dagron-ci import`.**
  - **login:** `dagron-ci login [--server URL]` reads the caller's
    dagron-api token from `--token-file`, `DAGRON_CI_TOKEN_FILE` or
    `DAGRON_CI_TOKEN`, else from standard input (typed at a terminal, which
    does not show it, or piped in); never from an argument. The server is
    held to the rule every command speaks to it by (`https://`, or `http://`
    to this machine), and asked whose the token is (the new `GET
    /ci/v1/me`) before anything is kept. A token it refuses is not kept,
    and nor is one it answers 200 for without naming a subject (a page
    served for every path, say), or naming one, or an email, of more than
    256 bytes or with a control character in it. The token is kept in
    `$XDG_CONFIG_HOME/dagron-ci/credentials.json` (else `~/.config/…`), for
    that server alone: the directory 0700, the file 0600, written whole and
    renamed into place, and read again under a lock (`credentials.lock`)
    once the server has answered, so a login or logout that finished
    meanwhile is not undone. A file others may read, or another user's, is
    refused, as ssh refuses a key. Ctrl-C at the prompt gives the terminal
    back as it was, showing what is typed. `run --remote`, `rerun`,
    `cancel` and `logs` then need no flag or variable: the server is the
    one logged in to last, and a token from a flag or a variable still
    comes first. `dagron-ci logout [--server URL]` forgets the token (of
    `--server`, `DAGRON_CI_URL` or the one logged in to last); it works
    until it is revoked in dagron-api, where it was made.
  - **`GET /ci/v1/me`:** who the `default` cell's dagron-api says presents
    the credential, `{sub, email, groups, trust}`; 401 for one it refuses,
    404 where no cell is configured, 503 where dagron-api cannot be asked.
  - **import:** `dagron-ci import [FILE]...` runs the importers of
    `dagron-import` (GitHub Actions, GitLab CI, Jenkins Declarative
    Pipelines), now a library of dagron-ci's, and holds each result to
    dagron-ci's own reader and compiler, for every kind of event, before it
    says ok. With no file it imports what the checkout holds:
    `.github/workflows/*`, `.gitlab-ci.yml` (or the file `.dagron/ci.yaml`
    names) and `Jenkinsfile`. Without `--write` it prints the one file's
    pipeline under its report, as `dagron-import` does; with it, it writes
    each to `.dagron/ci/<name>.yaml`, never over a file that is there unless
    `--force`, and two files that would be one pipeline are refused before
    anything is written. A `.dagron` or `.dagron/ci` that is a link out of
    the checkout is not written into, and nothing is made in it. A GitLab
    pipeline's includes are read from the checkout alone: a path up out of
    it, or a link that leads out, is not read, and is left to do. As
    GitLab, at most 150 are read, each path as it is written, and none over
    2 MiB. The images `.dagron/ci.yaml` gives a label or a tag are the
    importer's, under those `--image` gives.
  - `dagron-ci validate` reads a GitLab pipeline's includes from the
    checkout alone too: a link in it that leads out is not followed.
  - `dagron-import`'s library gives what its command and `dagron-ci import`
    share: `gitlab::read_included` and `pipeline_name`. Its note for an
    include it did not read says `--root` is `dagron-import`'s.
    `read_included` reads at most 150 files, none over 2 MiB, so
    `dagron-import gitlab` is held to GitLab's bounds too.
- **A run is run again whole.** A rerun
  was from a run's failed jobs alone, and a run that succeeded was run
  again only by a new dispatch: new engine runs, under checks of their own.
  - **dagron-ci:** `POST /ci/v1/runs/{id}/rerun?failed=false`, refused
    before, and `dagron-ci rerun ID --all` run each of an event's runs that
    is over, one that succeeded too, whole: every job. They are the same
    engine runs, so the same checks on the same commit, followed again as
    any rerun is, under a build's limit like a new run. Where a build counts
    runs, each is counted as the run run again: an end is told to the meter
    with how many times its run was rerun whole (`whole_reruns`, which
    dagron-api's `GET /api/runs/{id}/wait` now says, and dagron-ci keeps in
    its migration 0029). A cell whose dagron-api is older than this answers
    400, and that run is left with why: the door answers 409, not 503. The
    default, `?failed=true`, and GitHub's "Re-run" and "Re-run all checks",
    are from the failed jobs as before. A `failed` that is neither `true`
    nor `false` is refused with a problem document (400), where it was a
    bare text 400 the API's metric did not count.
  - **dagron-api and the engine:** `POST /api/runs/{id}/rerun` (in the
    Postgres and the SQLite build) and the engine's own `POST
    /runs/{id}/rerun` take `{"from": "all"}`, one implementation behind
    both (`db::rerun_all`). Every task of a run that succeeded, failed or
    was cancelled is put back as a clear puts one back, its checkpoint
    dropped. The attempts it replaces are kept under `rerun`, its
    `run.rerun` event says `"from": "all"`, and a full concurrency key is a
    429. A runtime fan-out's instances are deleted, and made again from
    what its producer prints this time: their kept attempts and logs go
    with them. An approval gate asks again. It is counted on the run
    (`workflow_runs.whole_reruns`). A run with a task that still owes the
    teardown of a remote job (a deferred task cancelled while its job ran)
    is a 409, with nothing written, until the engine has torn the job down
    or given up on it, as `gc_old_runs` will not collect such a run: the
    rerun would have dropped the handle the teardown sweep had still to
    reach, and the job would have run on uncounted.
- **A paths filter lists a change larger than the forge's compare API.** GitHub's compare lists at most 300
  files of a change and GitLab's 1000, and a change past that (or a
  comparison over 16 MiB, or one GitLab timed out on) ran every
  paths-filtered pipeline. dagron-ci now fetches the two commits itself, at
  depth 1 over git's protocol v2 with the repository's own credential and
  without their files' contents where the server can leave them out
  (`filter blob:none`), reads the pack in its own process (the image has
  no git) and compares the two trees, as `git diff --name-only` does, from
  the merge base the compare API compared from: GitHub's, named by the
  comparison or by a page of it that lists no files, and GitLab's
  `repository/merge_base`. So a push forced after a rebase still lists its
  branch's own files, which its old tip and its new have alike. The fetch
  is bounded (32 MiB of pack, 64 MiB inflated, 200,000 objects, 256
  directories deep, 64 MiB of trees read, 200,000 paths of at most 4,096
  bytes and 16 MiB in all, 20 s or a third of `CI_EVENT_LEASE_SECS`, one at
  a time in a replica), which keeps a fetch to about 160 MiB, inside the
  charts' 256 MiB for dagron-ci. Where it cannot list the change either,
  the pipeline still runs, and the event's reason names both failures; a
  new branch, two commits the forge cannot compare, and a change whose
  merge base the forge does not name run as before. A `pack` fuzz target
  reads what the fetch is answered with.
- **dagron-ci asks a token service for its App's tokens**
  (`CI_GITHUB_APP_TOKEN_URL`). One App's private key mints for every
  installation of it, so where one App serves many dagron-ci instances, each
  holding the key is each able to read every installation's repositories.
  The key may now be held by one service, which dagron-ci asks, under
  `CI_GITHUB_APP_TOKEN_SECRET_FILE`'s secret, for each installation token it
  needs: GitHub's own request by repository (`{repository, permissions,
  one_repository}`), answered with GitHub's own `{token, expires_at}`, and
  404 where the App is not installed for the asker, which dagron-ci reads as
  an App that is not installed there. Such a dagron-ci holds no key and signs
  nothing; it still revokes a token as itself. `https://`, or `http://` to
  this machine; `CI_GITHUB_APP_TOKEN_URL_INSECURE=allow` for plain http on a
  closed network. `CI_GITHUB_APP_KEY_FILE` and the URL are one or the other.
- **dagron-ci takes a cell over plain http where its operator says so**
  (`CI_CELLS_INSECURE=allow`), on a closed network where that is accepted:
  a dagron-api beside dagron-ci, behind a network policy, as one
  installation's are. Unset, a cell over plain http to another host stops
  start-up as before, and the error now names the setting.
- **dagron-ci's own OpenTelemetry.** A build
  with the `otel` feature, which the dagron-ci image now
  is, sends dagron-ci's log records and what its `/metrics` serves (as OTLP
  sums, gauges and histograms, each `OTEL_METRIC_EXPORT_INTERVAL`), as the
  engine does: to the collector `OTEL_EXPORTER_OTLP_ENDPOINT` names, or a
  signal's own `OTEL_EXPORTER_OTLP_LOGS_ENDPOINT` /
  `OTEL_EXPORTER_OTLP_METRICS_ENDPOINT` for that signal alone. It opens no
  spans of its own yet. A binary whose name begins `dagron-` is now
  `service.name` by that name (`dagron-ci`, `dagron-step-sql`), where it was `dagron-dagron-…`.
- **Task logs followed live.**
  `GET /api/runs/{id}/logs/stream` and `GET /api/runs/{id}/tasks/{tid}/logs/stream`
  send a run's task logs as server-sent events: each task read from a
  cursor (`<task>:<fence>:<gen>:<byte>`), its start, its end, or its last
  256 KiB, and then followed as it is written, every line once; `start`,
  `log`, `end` and `eof` events, the log filter's line predicates server
  side, `Last-Event-ID` on the one-task stream. One reader for each watched
  run, whatever the number of watchers, woken by the run's notification and
  on `DAGRON_LOG_STREAM_POLL_MS`, let go `DAGRON_LOG_STREAM_LINGER_MS` after
  its last watcher; `DAGRON_LOG_MAX_TAILS` runs at most (`429` past it) and
  `DAGRON_LOG_RELAY_BYTES` of text held, `DAGRON_LOG_MAX_STREAMS` streams; a
  slow client is read from the store at its own pace, one that takes nothing
  for 60 s is let go, and a stream lasts 10 minutes at most before it is
  resumed (its caller asked again who they are). `/api/health` says how many runs are followed
  (`log_tails`); the task view gives a `cursor` to go on from. dagron-ci's
  `GET /ci/v1/runs/{id}/logs?follow=true` passes one run's stream on as the
  caller, and `dagron-ci logs ID --follow [--from-start]` follows every run of
  an event, new ones as they come, going on from its cursors after a drop.
  Not yet: Docker's and Kubernetes' logs followed as a task writes them (a
  Docker job's log comes whole at its container's end), retention and the
  store's clean-up, the console following a log in place of polling it.
- **Task logs kept out of the scheduling database.** With `DAGRON_LOG_STORE` (`file:///DIR`, which the engine
  and dagron-api share, or an object store's `s3://`, `gs://`, `az://`), a
  task attempt's log is kept as zstd segments of `DAGRON_LOG_SEGMENT_BYTES`
  (8 MiB) with a row of `log_segments` each, and an open part: a file, or,
  with an object store, the task row's `log`, never larger than a segment.
  dagron-api's log routes and the engine's log views read a task's segments
  and open part; a log from before, and a remote runner's, from its row as
  before. Unset, nothing changes. **The upgrade's migrations add
  `log_segments`.** Not yet: the live relay, Docker and Kubernetes logs
  followed as they are written, retention and the store's clean-up.
- **The image cache.** `DAGRON_REGISTRY_MIRRORS` pulls each
  image through its registry's mirror (`docker.io=mirror.local:5000/hub`, `*`
  for every other registry), on Docker and Kubernetes, with no fallback.
  `DAGRON_REGISTRY_AUTH_FILE` (Docker, a `config.json`'s `auths`) and
  `DAGRON_IMAGE_PULL_SECRETS` (Kubernetes) are the engine's credentials, sent
  with a trusted run's pulls alone: a run that is not trusted pulls as no one.
  The Docker executor pulls one image once at a time on a host, pins a run's
  tag to the digest it first had it at for the run's later tasks, and fetches a
  run's images side by side as its first task is dispatched. New metrics:
  `scheduler_image_pulls_total`, `scheduler_image_pull_failures_total`,
  `scheduler_image_cache_hits_total`, `scheduler_image_pull_seconds`.
- **A privileged task, on a dedicated pool: `privileged: true`.** A
  job that runs a daemon or a cluster inside itself has its own container
  privileged, where the engine's operator allows it
  (`DAGRON_TASK_PRIVILEGED=allow`), and nowhere else.
  - **A dedicated pool alone.** An engine that allows it does not start
    unless it claims its own runner classes (`RUNNER_CLASSES`, without
    `default`), runs no work that is not trusted, and has no floor that
    drops capabilities, forbids new privileges or names a runtime class or
    seccomp's default profile. On Kubernetes it also needs task pods on
    nodes of their own (`DAGRON_TASK_NODE_SELECTOR`), and a namespace that
    admits a privileged pod: it asks the API server, as a dry run, and one
    whose Pod Security refuses stops it.
  - **The task's container alone**: on Docker the container is privileged,
    on Kubernetes its security context; what installs its tools, fetches
    its checkout, its services and its steps are not. A read-only root asked
    of it stays.
  - A task that asks where it is not allowed, or is held to what
    contradicts it, is failed once with the reason; a remote runner is
    given none; a run that is not trusted is refused the key.
  - **Pod Security**: the pool's task namespace is labelled
    `pod-security.kubernetes.io/enforce: privileged`, and is a namespace of
    the pool's own; the others keep theirs.
  - In `dagron-core`, a module `privileged` and `TaskSpec::privileged`; in
    `dagron-executor`, `ExecContext::privileged`,
    `DockerExecutor::with_privileged` and `KubeExecutor::with_privileged`.
  - Not run: a cluster, its Pod Security admission included.
- **An allowlist on Docker, and a list by hostname on either executor.** A
  task's `isolation: { network: allow:<name> }` was refused on Docker.
  - **On Docker a list is a network of the operator's**,
    `dagron-task-allow-<name>`, made `--internal` and with no address of
    the host's on it (`-o com.docker.network.bridge.inhibit_ipv4=true`: on
    an internal bridge the host has one, and a task would reach what the
    host serves there), with on it what the list allows.
    `DAGRON_TASK_NETWORK_ALLOWLISTS` names them, as on Kubernetes. The
    engine (and `dagron-runner`) reads each as it starts and does not start
    where one is missing, has a way out, or gives the host an address, and reads it again before
    each task held to it: one removed since has its task refused before
    anything is made. The task's container, its services, its steps and its
    daemon are on that network alone; its checkout, tools and actions are
    fetched first, on `DAGRON_DOCKER_NETWORK`. A floor may name one there too.
  - **A list by hostname is a proxy of the operator's.** A network's label,
    or a NetworkPolicy's annotation, `dagron.dev/proxy` says which:
    `http://` or `https://`, a host and a port, no user and no path. A task
    held to the list is told `HTTP_PROXY`, `HTTPS_PROXY`, `http_proxy`,
    `https_proxy`, and `NO_PROXY`/`no_proxy` for itself and its services,
    each unless it set it itself; so are its services (its daemon among
    them), each under its own setting, its steps, and on Kubernetes its
    checkout's and actions' fetches. The chart
    writes the annotation from `engine.taskNetworkAllowlists.<name>.proxy`.
    What the proxy lets through is the proxy's: dagron runs none.
  - In `dagron-core`, `isolation` has `ENV_NETWORK_ALLOWLISTS`,
    `parse_allowlists`, `allowlist_object_name`, `ALLOWLIST_PROXY`,
    `parse_allowlist_proxy` and `proxy_env`; `Enforcer::allowlists` is now
    held by the Docker executor as well. `DockerExecutor` has
    `with_network_allowlists`.
  - Not run: Calico or Cilium, whose own policy kinds have rules by name.
- **A run on a patch bundle.** dagron-ci's `POST /ci/v1/bundles` keeps a
  tar of the files a change touched since a base commit (and a
  `.dagron-deleted` of those it deletes), up to 32 MiB as the body or 256 MiB
  through an upload link the object store signs, for seven days; and
  `POST /ci/v1/runs` with `bundle` runs the repository's pipelines on the
  base with the bundle laid over it, the bundle's own pipelines where it
  changes them. Such a run is untrusted whoever asks, its uploader's alone,
  and tells the forge nothing. A job's checkout lays the patch over the
  base from a link of an hour the checkout credential door gives each
  attempt, at `CI_URL_FOR_JOBS` (new: https://, or http:// to this machine,
  or plain http with `CI_URL_FOR_JOBS_INSECURE=allow`; the engine likewise
  takes a plain http link to another host only with
  `DAGRON_CHECKOUT_PATCH_INSECURE=allow`); the checkout's image needs `tar`,
  and `curl` or `wget`. The engine's `workspace.checkout` takes `patched`.
  **The upgrade's migrations add `ci_bundles` and `ci_events.bundle_id`.**
- **Fleet mode tells a fork's job apart.** A job of a workflow run
  whose head is in another repository is given what its label's
  `untrusted:` says: no runner (`refuse`, unless said), a runner submitted
  as a run that is not trusted and held to the engine's floor for such runs
  (`floor`, in an environment of its own, `CI_FLEET_UNTRUSTED_ENVIRONMENT`,
  with `untrusted_runner_class`), or a runner as any job's (`serve`). A
  trusted runner GitHub gives a fork's job is stopped as dagron-ci hears of
  it, and so is one whose job could not be told apart within thirty
  seconds. A job that asks for `self-hosted` alone, taken by a runner of the
  fleet's, is kept as a job of that runner's label.
  [`crates/dagron-ci/README.md`](crates/dagron-ci/README.md#fleet-mode).
- **Fleet mode's organization runners, quotas and Docker daemon.**
  `scope: organization` registers a label's runner for the repository's
  owner, in its `runner_group`, with a token of its own for the
  organization's **Self-hosted runners** permission. `max_runners` and
  `max_per_repository` hold a label's runners out; the count and the row
  are one statement, so two replicas cannot both take the last place.
  `docker: true` gives the runner a Docker daemon beside it. A job that has
  waited a minute, and one whose runner has been over a minute with no end
  of it heard, is asked of GitHub (`GET …/actions/jobs/{id}`).
- **Fleet mode keeps runners warm.** A label's `warm` runners are
  kept out beyond the jobs that wait, for a repository that has had a job of
  the label within its `warm_for_secs` (an hour unless said), counted
  against its quota and started anew once they have waited half an hour,
  or a quarter of the label's time where that is less. A warm runner is an
  idle trusted run, which GitHub would give a fork's job: a label keeps
  runners warm only where it says `untrusted: serve`, or names a
  `runner_group` restricted to selected workflows. On the chart,
  `ci.fleet.prepull` pulls each label's image onto each node a runner may
  start on, before one does.
- **GitLab's fleet.** With `CI_GITLAB_FLEET_FILE`, a GitLab
  project's job whose `tags` name one of the operator's labels is given
  GitLab's own runner, `gitlab-runner run-single`, made for the project or
  its group just before it starts and removed once it is over, as a dagron
  run of one task. A job is heard of by its Job Hook, and the project's
  jobs that wait are asked of GitLab; a merge request's job from a fork is
  told apart as GitHub's is, and one a runner took before a look listed it
  by its pipeline's ref. A runner running a job is not removed.
  [`crates/dagron-ci/README.md`](crates/dagron-ci/README.md#gitlabs-fleet).
- **A fleet job whose runner died is run again, once.** A job that
  failed on a runner whose run did not succeed is run again through the
  forge's API: GitHub's as a new attempt of its workflow run, asked each
  minute for an hour until the run is over; GitLab's at once. Not for a
  runner that had its label's whole time or that dagron-ci stopped, and
  never twice for a job of one name of a run. Whether the runner died is
  its run's to say, whichever of GitHub and the look heard first; a job of
  which a later attempt is heard of is not asked for again. `retry: false`
  turns it off for a label. On GitHub it needs **Actions: Read and
  write**.
- **Fleet mode's showback.** `GET /ci/v1/fleet/usage` (an admin's)
  and `dagron-ci fleet usage` say what the fleet's runners did over whole
  days, by repository, team or label: the jobs that started on them and how
  long they ran, the runners submitted and how long they were out. It is
  for showing who used the capacity, never to charge for it. `/metrics` gains
  `dagron_ci_fleet_jobs_waiting{label}` and
  `dagron_ci_fleet_runners{label,state}`; `CI_RECORD_DAYS` clears a fleet's
  ended jobs and runners as it clears runs. dagron-ci's schema gains
  columns of `ci_fleet_jobs`, `ci_fleet_runners` and `ci_repos`, and
  indexes, which it adds when it starts.
  [`crates/dagron-ci/README.md`](crates/dagron-ci/README.md#showback).
- **A smoke test for a managed Postgres.**
  `scripts/managed-postgres-smoke.sh <url>` runs dagron on scratch databases
  of a server you name: RDS, Cloud SQL, Azure Flexible Server, or your own.
  It checks the server is Postgres 14 or later and the connection is
  encrypted. It applies the migrations and runs workflows, has two engines
  share one queue through the provider's endpoint, and restores a `pg_dump`
  of the database with the restore drill. It exits 0 only on a pass. CI runs
  it once a quarter on the services whose servers are configured.
- **dagron on GKE, AKS and any Kubernetes.** Three
  terraform modules install the chart beside the EKS path: `terraform/gke`
  (a regional cluster with Workload Identity and Dataplane V2, a tainted
  task pool, Spot if asked, an artifacts bucket and a versioned backups
  bucket each reached by a Google service account of its own, Cloud SQL on a
  private IP), `terraform/aks` (Workload Identity, Azure CNI with Cilium, a
  storage account with no shared key, Flexible Server on a private network)
  and `terraform/kubernetes` (the chart on a cluster you already have, with
  your database, object store, backup target and task pool). Each has a
  `terraform test` suite with every provider mocked. `docs/CLOUDS.md` says
  what each makes. The chart takes `engine.serviceAccount.annotations` and
  `engine.podLabels`, which is what GKE's and AKS's workload identity need.
- **Azure Blob URLs as artifact stores.** `DAGRON_ARTIFACT_URL` (and a
  backup target) may be `https://<account>.blob.core.windows.net/<container>/…`
  as well as `az://<container>/…`. That form names its account, so it needs no
  `AZURE_STORAGE_ACCOUNT_NAME`.
- **`DAGRON_TASK_SERVICE_ACCOUNT_POD_LABELS`.** Labels for every task pod
  that runs as a ServiceAccount, and for no other pod. This is for a cloud
  whose workload-identity webhook acts only on labelled pods, as AKS's does
  (`azure.workload.identity/use=true`). A malformed value, or one of the
  engine's own label keys, stops the engine at start.
- **Backups to S3, GCS, Azure or a directory, and a restore drill.** `dagron backup put|list|get|prune` moves a database's dumps
  to a target and back: `s3://` (MinIO, Ceph and R2 through
  `AWS_ENDPOINT_URL`), `gs://`, `az://` or `file:///dir`, each with its own
  credentials or the pod's workload identity, in every published engine
  image. `put` streams and checks the bytes landed, `get` renames a dump
  into place once whole, and `--keep N` keeps the newest N and never none.
  The chart's `backup.enabled` makes a nightly CronJob: `pg_dump` (custom
  format, no owners) and `dagron backup put`, of whichever database the
  engine uses; a `file://` target names its claim. `scripts/restore-drill.sh`
  is the restore rehearsal as a command: the newest dump from a target,
  restored into a scratch database, held to the live one, an engine started
  on it. `scripts/backup-e2e.sh` runs it on a directory, an S3 store, a GCS
  emulator and Azurite, after three nights' dumps were sent to each: all
  four passed. `docs/BACKUP_ONPREM.md` is the runbook with no cloud: NFS,
  MinIO, off-site copies, an alert, the drill on a schedule.
- **dagron-api on SQLite, for one node.** Built
  `--no-default-features --features sqlite`, dagron-api is one process beside
  one engine on the same SQLite file, with no Postgres: sign-in, users,
  tokens, workflows (stored, versioned, run), runs (submitted, listed, read,
  cancelled, rerun), approvals, logs and live updates, and the console at
  `/`. Live updates are polled, every `DAGRON_SQLITE_POLL_MS` (500 ms). What
  only the Postgres build serves (search, schedules, artifacts, datasets,
  runners, the fleet and the rest) answers `501` saying so, as does
  approving a gate bound to artifacts. Its tables are the engine's migration
  069, so either process may start first; a process that meets the other
  opening the file or applying the same migration tries again, where it
  used to stop. The
  Postgres build is unchanged and remains the default. Seen in
  `scripts/one-node-e2e.sh`, which `dagron-one-node-e2e.yml` runs in each
  pull request that touches it; [docs/ONE_NODE.md](docs/ONE_NODE.md). No
  published image of it yet.
- **The chart installs on OpenShift.** Under
  `restricted-v2`, the SCC an ordinary project's pods get, with no `anyuid`
  and no SCC of its own. `global.openshift` (`auto`: when the cluster
  serves `security.openshift.io/v1`; `true`; `false`) leaves each pod's uid
  and fsGroup to the SCC, which refuses a pod that names its own: every pod
  stays `runAsNonRoot` under `RuntimeDefault`, every container escalates
  nothing and drops every capability, and the bundled Postgres is held to
  the same. The engine gives its task pods the same envelope there, through
  two new knobs: `DAGRON_TASK_FS_GROUP` (`none`: a workspace volume names
  no fsGroup; a number: that one; unset: 65532, as before) and
  `DAGRON_TASK_RUN_AS_NON_ROOT` (non-root without a uid). The images run as
  a uid no passwd names, in group 0: the engine's `/workflows` and
  `/artifacts`, dagron-api's `/artifacts` and the runner's state directory
  are group 0's with the owner's rights, and dagron-gitops gives such a uid
  a name through `nss_wrapper`, which OpenSSH needs to clone over `ssh://`.
  Off OpenShift every pod renders as it did. `scripts/openshift-k3s.sh`
  checks the render, then installs the chart on k3s under Pod Security
  `restricted` with each pod given a uid and an fsGroup as the SCC would:
  Postgres, the engine and dagron-api start, and a task pod runs as a
  non-root uid in group 0 to success. Not run on OpenShift itself
  ([docs/OPENSHIFT.md](docs/OPENSHIFT.md)).
- **CloudEvents on every webhook dagron sends.** A
  `notify.webhook` post, the workflow's or the instance's, is a CloudEvent
  1.0 in the HTTP binding's binary mode: the body is the JSON it always was,
  and `ce-specversion`, `ce-id`, `ce-source`, `ce-type`
  (`dev.dagron.notify.<event>`), `ce-subject` (the run) and `ce-time` ride
  in headers, so a receiver that knows nothing of CloudEvents reads what it
  read before and one built on an SDK reads an event. One occurrence has
  one id at every target. `DAGRON_EVENTS_SOURCE` names the installation
  (`/dagron` by default). The settings' test notification is one too
  (`dagron_core::cloudevents`).
- **OTLP logs and metrics.** With the `otel` feature
  the published images carry, the endpoint that sends spans now sends log
  records and the engine's metrics as well. Each event the log filter lets
  through is also an OTLP log record, with its trace and span. The engine
  sends what `GET /metrics` serves every `OTEL_METRIC_EXPORT_INTERVAL`
  (60 s): counters as monotonic sums, gauges, histograms with their bounds,
  read through the same cache a scrape uses, with or without `API_ADDR`. A
  signal's own `OTEL_EXPORTER_OTLP_<SIGNAL>_ENDPOINT` and
  `OTEL_<SIGNAL>_EXPORTER=none` are honoured for each of the three. Seen
  against an OpenTelemetry Collector 0.111.0: 59 metric families, the run's
  log records and 22 spans from one run. dagron-api sends logs and spans;
  it has no metrics. dagron-ci has no `otel` build yet.
- **Signed provenance of each build output.** Where the
  engine is given a key (`DAGRON_PROVENANCE_KEY`, an ECDSA P-256 private key
  of the operator's), each workflow artifact the results service finalizes
  (`actions/upload-artifact`, a GitLab job's `artifacts:`) is made its run's
  with its SLSA v1 provenance, signed: the zip's SHA-256, and the
  repository, ref, commit, trigger and trust class of the run's record, the
  workflow, the job and its attempt, said by the engine and not by the job.
  - **Its shape is cosign's**: an in-toto Statement v1 in a DSSE envelope,
    ECDSA P-256 over SHA-256. `cosign verify-blob-attestation --key … --type
    slsaprovenance1 --insecure-ignore-tlog` checks it against the zip, with
    nothing written to Sigstore's public log, so an air-gapped installation
    signs all the same.
  - **Signed or not kept**: an artifact whose provenance could not be signed
    is refused, and the job's upload fails saying so.
  - `GET /api/runs/{run_id}/workflow-artifacts/{name}/provenance` serves the
    envelope; a listing says `provenance: true` of an artifact that has one.
    `DAGRON_PROVENANCE_BUILDER_ID` names the builder a verifier's policy
    trusts. Migrations: `093_workflow_artifact_provenance` (Postgres),
    `068_workflow_artifact_provenance` (SQLite), a nullable column.
- **A remote runner's task may have an identity.** A task with
  `identity: { oidc: … }` in a runner class that a remote runner serves was
  failed; dagron-api now mints its request credential as an engine does,
  and the runner gives the task `DAGRON_OIDC_REQUEST_URL` (its
  `DAGRON_RUNNER_URL` + `/oidc/token`, or `DAGRON_RUNNER_OIDC_REQUEST_URL`),
  `_TOKEN` and `_AUDIENCE`. A dagron-api that is no issuer fails the task
  saying so. A job carries it as `identity: {token, audience}` (protocol 0,
  an optional field).
- **A remote runner: `dagron-runner`.** A task can now run
  where no engine runs: a Mac, a Windows host, a GPU box, a team's own
  cluster, a host behind NAT. The runner dials dagron-api over HTTPS
  (`/runner/v1`, protocol 0), holds no database credentials, and takes the
  ready tasks of the runner classes it serves.
  - **The claim is the engine's own**, made by dagron-api for the runner's
    session: the same priorities, pools (`POOLS`, now read by dagron-api as
    well) and fences. An engine whose `RUNNER_CLASSES` leaves a class out
    leaves it to the runners.
  - **Session leases.** A runner process opens a session, and
    one heartbeat statement renews it and every task it holds; a task row is
    not written by it. A runner that dies (`kill -9`) loses only what it ran:
    once its session's lease (`DAGRON_SESSION_LEASE_SECS`, 90 s) lapses, the
    engine's sweep takes its tasks back to `ready`, and another runner
    completes them. A word it says late is refused (409).
  - **A result is kept, not applied, by dagron-api**: an engine takes it
    and applies it as it applies its own workers', so retries, `repeat:`,
    `defer:`, the cache, `produces:` and dead letters are the same for a
    remote task as for a local one.
  - **Executors**: `local`, `docker`, and `k8s` (the runner as an
    in-cluster controller, each task a pod), as the engine makes them, with
    its isolation floor (`DAGRON_TASK_ISOLATION_FLOOR`) applied on the
    runner's host. Release assets for Linux x86_64 and arm64 (static),
    macOS arm64 and Windows x86_64, and an image.
  - **Secrets stay home**: with `DAGRON_RUNNER_SECRETS=home` dagron-api
    sends a secret's name alone and the runner reads it from its own store;
    by default dagron-api sends the values from the run's environment, by
    its policy. Either way they are masked in what the runner sends back.
  - **Version negotiation and drain**: a session speaks the newest
    protocol both ends do (426 where none, or a release older than
    `DAGRON_RUNNER_MIN_VERSION`); SIGTERM, or `POST /api/runners/{id}/drain`,
    lets running tasks finish and ends the runner.
  - **Autoscaling**: `dagron-runner scale` sizes Kubernetes
    Deployments of runners by their label's ready and running tasks
    (`GET /runner/v1/queue`).
  - Admins mint join tokens (`POST /api/runners/join-tokens`) and list,
    drain and revoke runners (`/api/runners`). What protocol 0 does not carry
    is refused, the task failed saying so: a run that is not trusted, a
    `workflow` or `wait` task, a gang's member, an `identity` or a
    `forge_token`.
  - The engine's environment-secret policy and decryption, `env_from` and
    the `POOLS` parser moved into dagron-core, which dagron-api shares: one
    copy of who may read which secret.
  - `scripts/runner-e2e.sh` runs the engine, dagron-api and runners end to
    end against Postgres.
  - **Upgrade:** two engine migrations (Postgres 091 and 092, which builds
    an index concurrently; SQLite 067) add `runner_sessions`,
    `runner_results`, `task_runs.session_id`, and dagron-api's `runners` and
    `runner_join_tokens`. Every engine claim now clears `session_id`.
- **dagron-ci's reporter is told of a run's changes.** A task's start and
  end are events in the engine's outbox (`task.started`, `task.finished`),
  written by a trigger on `task_runs` only while a sink that asks for them
  claims, so a deployment without dagron-ci writes none. dagron-api hands a
  service (a user in `ci:trigger`) its own sink of the outbox, the events of
  the runs it submitted alone (`POST /api/outbox/ci/claim`, `/ack`). dagron-ci
  follows each cell's and looks at a run when it changed; while a cell's
  outbox answers, a running run with nothing new is looked at once a minute,
  where it was every five seconds. A dagron-api of an earlier release has no
  outbox to hand, and its runs are looked at as before. **The upgrade's
  migrations add a trigger to `task_runs` and a column to `ci_runs`.**
- **`dagron-import gitlab` and `dagron-import jenkins` write a dagron-ci
  pipeline for a GitLab CI pipeline and a Jenkins Declarative Pipeline**,
  under the report `dagron-import github` writes: what was
  changed on the way, what was not carried over, and what is left to do by
  hand, with a job that has something left starting with a step that
  fails and says what.
  - `gitlab` reads a `.gitlab-ci.yml` with the files it includes from the
    project (`--root`), its templates, references and anchors written out
    in each job. Stages and `needs` are the order, a job a run leaves out
    taking its place in it with it; `workflow:rules` is the
    pipeline's `on:` and a job's `rules` (or `only` and `except`) its
    `when:`, read for every event at once, exactly where a rule names the
    event's kind, a branch, a tag or a merge request's target, and wider,
    left to do, where it reads what is not known before a run. GitLab's
    variables a script reads are set at its head from dagron-ci's own;
    services, caches, matrices, manual jobs (a gate before them), jobs
    that may fail (steps that go on past a failure, waited for as on
    GitLab) and `docker:dind` (a daemon of the job's own) are carried
    over. A pipeline for schedules alone is a run asked for.
  - `jenkins` reads the Groovy of a `Jenkinsfile` as far as its structure.
    The stages of one agent are the steps of one job, as they shared a
    workspace; a stage with an agent of its own is a job of its own; a
    matrix is one job over its instances; an `input` is a gate. A stage's
    `when` is a test at the head of its steps and its `post`'s,
    credentials are an environment's secrets (a stage's held by names of
    their own, which its steps set the stage's names from), a `catchError`
    block is one step that may fail, and `parameters` a dispatch's inputs.
    Groovy (`script`, shared libraries) and plugins' steps are left to do;
    a Scripted Pipeline is refused by name.
  - The shared report and the rules a pipeline is written by are the
    importers' own module. The GitHub importer writes what it wrote
    before, but for a thing left to do said twice of one place: the report
    and the job's first step now say it once.
  - `make import-gitlab` and `make import-jenkins`, beside
    `make import-github`
    ([`docs/CI_MIGRATING.md`](docs/CI_MIGRATING.md#from-jenkins)).
- **A run's end as CDEvents.** With `DAGRON_CDEVENTS_URL`, the engine sends
  each run's end as CDEvents v0.5.1, CloudEvents in HTTP binary mode: a
  `taskrun.finished` for each task that ran, then the run's
  `pipelinerun.finished`, with the times they happened, through the outbox
  as a part of the run's end-of-run work, retried with ids that stay the
  same. `DAGRON_CDEVENTS_SOURCE` names the events' source (default
  `dagron`). A run's queued and started events are not sent yet.
- **Every consumer of the outbox is handed every event.** The transactional
  outbox kept one delivery state an event, so the first consumer to claim
  one decided that no other saw it, and `dagron-ee` and `dagron-unit`
  refused to start with both `DAGRON_WEBHOOK_URL` and `DAGRON_FLEET_URL`.
  Each consumer is now a sink with its own state of each event (new tables
  `outbox_sinks` and `outbox_deliveries`): the webhook and the fleet uplink
  run side by side, a dead letter or a shed event is one sink's, and the
  fleet spool's budgets are the uplink's alone. A sink is handed the events
  written after it was first configured; the webhook and the uplink, which
  drained the old outbox, also those it still held pending when the upgrade
  ran, and never one it delivered. `event_outbox.status` is no longer written. **The upgrade's
  Postgres migration builds an index on `event_outbox (created_at)`
  concurrently.**
- **A run's end-of-run work leaves the reconcile loop.** The run sink,
  OpenLineage's terminal event, the `notify.git` commit status and
  `notify.webhook` / `notify.slack` are each done from the run's
  `run.completed` event by a worker of their own, so a forge or lineage
  backend that is down or slow no longer holds back runs ending and tasks
  being dispatched. Each is retried with backoff (eight tries, as the
  webhook's), where it was tried once; none is lost to a crash between a
  run's end and its work; an end superseded before it was done (a cancelled
  run rerun) is not done; and one older than a day when its part first
  reaches it is let go. The lineage post has a 15-second timeout, where it
  had none. A `RunSink` is now called at least once for each end, and may
  be called twice for one after a crash. A cancel no longer stamps
  `completion_pending_at`; a run a release before this one stamped has its
  end done once, as before.
- **dagron-ci keeps its records by day, writes them out, and says what it
  posted.** On Postgres, `ci_events` is partitioned by day; the upgrade's
  migration rebuilds the table as a partitioned one and copies every event
  into the partition of its day, in one transaction that holds `ci_events`
  locked until it commits: the queue and the API wait for it, for as long as
  copying the table takes, so a large queue upgrades in a maintenance window. `CI_RECORD_DAYS` drops the days older than
  it each hour, with their runs' rows, run API requests, report log,
  deliveries and stored payloads; unset, as by default, every record is
  kept, as before. With `CI_OBJECT_STORE_URL` (`s3://`, `gs://`, `az://` or a
  directory), a day is first written there as zstd segments, a table each,
  read back and checked before it leaves the database, and kept
  `CI_SEGMENT_DAYS` (or for good); and a webhook payload over 64 KiB is kept
  there, where it was not kept at all, so a workflow's `github.event` has it.
  Each status dagron-ci posts is a row of a new report log with what the
  forge answered, which `GET /ci/v1/runs/{id}/statuses` reads, from the
  database or from the segments.
- **dagron-ci's reporter writes in batches, and dagron-api knows a service
  token from a short cache.** The reporter looks at up to eight due runs at
  once, and writes the looks that have ended in one statement, with their
  posts in one more, so a
  burst of run ends is a few statements; new metrics say how many wait and
  for how long. dagron-api answers who a `ci:trigger` user's token is (such
  as dagron-ci's own, which asks on every call) from a cache of ten seconds,
  forgotten when a token is revoked through it; a person's and an agent's
  tokens are read every call, as before.
- **A GitLab merge request shows dagron-ci's run as a pipeline, and can
  have a note.** Each job's commit status names its ref (the branch or tag,
  a merge request's source branch), so GitLab keeps a run's statuses as one
  external pipeline of that ref, which the merge request shows and
  "Pipelines must succeed" waits for; a status names a merge request's
  event `merge_request`, as GitLab does. With `CI_GITLAB_MR_NOTES=on`, a
  merge request has one note that says how its latest commit's runs went,
  written again in place as they change, with the host's token: what a pull
  request's comment is on GitHub.
- **A `.gitlab-ci.yml` runs as it is.** Where the operator turns it on
  (`CI_GITLAB_PIPELINES=on`) and a GitLab project names the file in its
  `.dagron/ci.yaml` (`gitlab: pipeline: .gitlab-ci.yml`), dagron-ci reads it,
  with the files it includes from the project, and runs it as one pipeline,
  `gitlab-ci`. As GitLab makes a pipeline:
  - stages, `needs` and `dependencies`;
  - `rules` (GitLab's expression language, `changes`, `when`, `variables`)
    and `workflow:rules`;
  - `parallel` and its matrix, and a manual job as an approval;
  - `variables`, expanded with each other and with the predefined
    `CI_COMMIT_*`, `CI_MERGE_REQUEST_*`, `CI_PIPELINE_SOURCE` and their like;
  - `services`, `cache`, `environment`, `timeout`, `retry`;
  - `default`, `extends`, `!reference` and anchors;
  - a job's image, or the one its runner tag is given (`gitlab: tags:`).

  A job's `artifacts:paths` are packed into a zip and uploaded by the step
  runner through the engine's artifact service when its scripts are over,
  and fetched into the workspace of the jobs after it that take them.
  `dagron-ci validate` checks one. What is not taken is refused with what
  it is ([docs/CI_GITLAB.md](docs/CI_GITLAB.md)).
  A job with artifacts, or that may fail, is handed on in step protocol 5,
  which this release's dagron-ci-exec speaks: **an engine whose
  `DAGRON_CI_EXEC_IMAGE` is older refuses such a job**, rather than run it
  without them. A job that may fail, and fails, ends as succeeded, with a
  warning that says it failed, so that the stages after it run.
- **GitLab as a forge.** dagron-ci serves a GitLab project, gitlab.com's
  or a self-managed one's (`dagron-ci repo add … --forge gitlab`), as it
  serves a GitHub repository: `POST /hooks/gitlab` takes its pushes, tags
  and merge requests (`X-Gitlab-Token` compared in constant time with
  `CI_GITLAB_WEBHOOK_SECRET_FILE`'s, deduped on `webhook-id`), the
  poll sweep reads its open merge requests, its `.dagron/ci` pipelines and
  changed files are read through GitLab's REST API, and each job's outcome
  goes back as a commit status. A merge request is a pull request's event
  at `refs/merge-requests/{iid}/head`, trusted from the project's own
  branch and not from a fork. A private project is read with the token
  `CI_GITLAB_TOKENS_DIR` keeps for its host, a file per host, which goes to
  that host alone and to no job. A checkout is given a project access token
  of its own, minted with the host's token for that project alone (Reporter,
  `read_repository`, lapsing within two days) and revoked by its id once the
  attempt is over; its id is kept with the credential's row
  (`ci_checkout_credentials.forge_token_id`, a migration). Minting one
  needs the host's token to be a personal access token, a user's or a
  service account's, that is the project's Maintainer: GitLab lets no group
  or project access token mint another. A `.gitlab-ci.yml`
  is not read yet.
  `dagron_ci_webhook_deliveries_total` counts GitLab's deliveries under
  `forge="gitlab"`.
- **The rest of a GitHub workflow's `github.event`, and its artifacts.** On
  dagron-ci's GitHub front-end, `github.event` is the forge's own payload
  as its webhook delivered it (a pull request's title, labels and author, a
  push's commits), where dagron-ci kept it (at most 64 KiB), with
  dagron-ci's own fields over it; read as an outsider's, as before.
  `actions/upload-artifact/merge` runs against the engine's artifact
  service. `download-artifact`'s `run-id` reads a run of dagron's through
  dagron-ci's new GitHub API door, where the operator serves one
  (`CI_GITHUB_API_FOR_JOBS`): it answers GitHub's two calls for the job's
  own repository and `GITHUB_TOKEN`, in a trusted run, and passes every
  other call on to GitHub. The console lists a run's workflow artifacts and
  downloads each one's zip, which dagron-api streams from the cache store
  (`GET /api/runs/{run_id}/workflow-artifacts`, `…/{name}`,
  `GET /api/workflow-artifacts/{id}`), given `DAGRON_CACHE_URL` as the
  engine is.
- **A private action of the repository's own owner's.** On dagron-ci's
  GitHub front-end, a step of a trusted run that uses an action of the
  repository's owner's (`acme/tools@v1` in a workflow of `acme/widgets`)
  has it fetched with a credential of dagron-ci's GitHub App, minted for
  that one attempt and that one repository, to read it, and revoked after,
  as a checkout's is; dagron-ci reads its `action.yml` with the App too. A
  workspace's `actions[]` takes `credential: true` for it, and the engine
  asks its token service (`"action": true`) for one; dagron-ci gives one
  for a trusted run and the owner's own repository alone, and only where
  that repository's Actions access shares its actions, as GitHub has it:
  the App needs the new repository permission **Administration: Read** to
  ask. Another owner's private action, a run that is not trusted, and a
  mirror get none, as before.
- **A Docker action whose image is a Dockerfile.** On dagron-ci's GitHub
  front-end, an action of `runs.using: docker` whose `runs.image` is a
  Dockerfile of its directory has its image built first, as on GitHub: the
  step runner sends the Dockerfile's directory to the engine's results
  service (`POST /dagron/v1/steps/images`, at most 64 MiB), and the Docker
  executor builds it on its daemon, named for a digest of what it was
  built from, so that it is built once. Only for a trusted run, and only
  where the operator says so with the new `DAGRON_DOCKER_BUILD_ACTIONS=allow`;
  on Kubernetes and the local executor such a step fails, and says so, as
  it did before everywhere.
- **A composite action's own actions.** On dagron-ci's GitHub front-end, a
  composite action's step may use an action of another repository, a
  local one or an image, as on GitHub: dagron-ci reads each action a
  job's steps use as it handles the event, and fetches the actions of
  other repositories theirs use with the job's own, held to the same
  allow-list and pins, ten deep and 32 in all. An action's file is read
  from github.com's raw files, or from a mirror's where the new
  `CI_ACTIONS_MIRROR_RAW` says. Such a step failed before.
- **A GitHub workflow's `on.schedule`.** dagron-ci's scheduler reads each
  GitHub repository's default branch (what `HEAD` names, from git's ref
  listing) and the crons of the workflow files `.dagron/ci.yaml` lists at
  its head, and queues a `schedule` event at that head as each cron fires:
  five fields in UTC as POSIX reads them, at most every five minutes, a
  missed fire not made up, and once however many replicas run.
  `github.event.schedule` is the cron. It runs where workflow files are
  read (`CI_GITHUB_WORKFLOWS=on`). dagron-ci's database gains
  `ci_schedules` and the scheduler's columns of `ci_repos` (migration
  0019), and its ref listing asks for `HEAD`.
- **More of a workflow's `github` context, and `secrets` in a job's
  `env:`.** On dagron-ci's GitHub front-end, `github.run_id` and
  `github.run_number` are the run's own numbers, which dagron-ci gives a
  workflow as it makes its run (so a concurrency group of
  `github.head_ref || github.run_id` is no longer one group for every
  push), and `github.actor`, `triggering_actor`, `head_ref` (a pull
  request's head branch, now kept with its event), `run_attempt`,
  `api_url` and `graphql_url` are there, each a step's `GITHUB_*` variable
  too. A job's or the workflow's `env:` may read `secrets.NAME`: the step
  runner reads the variable as the job starts. `::debug::` lines are
  printed only where `ACTIONS_STEP_DEBUG` is `true` (`RUNNER_DEBUG=1`), and
  the console folds a log's `::group::` lines. **A step's `::debug::` lines,
  printed before, are now not printed by default.** dagron-ci's database
  gains `ci_events.head_ref` and the table `ci_run_numbers` (migration
  0018).
- **A repository's secrets in its workflow files.** On dagron-ci's GitHub
  front-end, `${{ secrets.NAME }}` in a step (its script, `env:` or
  `with:`) is a secret of the repository's environment, named by its
  address (`github.com/acme/widgets`), which an admin makes and fills
  through dagron-api: the engine decrypts each one the job's steps read as
  it is dispatched, a missing one reads as empty, as on GitHub, and a run
  that is not trusted is given none. The engine gives such an environment
  to that repository's runs alone, and the step runner takes the secrets
  out of its environment, so a step sees one only where its workflow reads
  it. It is masked in the job's log. A workflow that read one was refused
  before. dagron-api takes an environment named for a repository, and a
  task's secret reference may be `optional: true`: its environment's
  alone, and empty where it holds none, never the engine's own store.
- **GitHub's workflow commands in a workflow's steps.** The step runner
  reads what a step prints before it reaches the job's log:
  `::add-mask::VALUE` has the value printed `***` from then on, and the
  job's own credentials (`GITHUB_TOKEN`, the results service's token, the
  OIDC request token) are masked from the start; `::error::`,
  `::warning::` and `::notice::` are annotations on the job's check run,
  with `file`, `line`, `endLine` and `title`; and `::stop-commands::`
  stops both until its token. They were lines of the log before, and
  masked nothing. **A job that prints its `GITHUB_TOKEN` now prints
  `***`, and a job output that holds a masked value is no longer handed
  on to the jobs after it**, as on GitHub (the job warns that it is not).
- **`hashFiles(…)` in a workflow's steps.** On dagron-ci's GitHub
  front-end, a step's script, `with:`, `env:` and `if:` may call it, as a
  cache's key most often does (`hashFiles('**/Cargo.lock')`): the SHA-256
  of each matched file's SHA-256, in their paths' order, of the job's
  workspace, with its patterns read as GitHub's runner reads them (a
  directory's name is all of it, `!` leaves out, links to files are
  hashed), and empty where nothing matched. It failed the step before. The
  glob matcher `on:` filters use moved to dagron-ci-expr, which the step
  runner reads the patterns with.
- **A Docker daemon of a task's own** (`docker_daemon: true`): a task that
  runs `docker build` or `docker run` is given a daemon beside it, Docker
  in Docker, as a service named `docker` that it reaches as
  `DOCKER_HOST=tcp://127.0.0.1:2375`, with its workspace and the operator's
  mounts at their own paths (but where a daemon keeps its own), and
  removed with the attempt. Its container is privileged, so the engine's
  operator offers it (`DAGRON_DOCKER_DAEMON=dind`, with
  `DAGRON_DOCKER_DAEMON_IMAGE`), on the Docker executor only, and an engine
  that offers it runs no work that is not trusted; a run that is not
  trusted is refused it. A repository's workflow jobs ask for one with
  `github.docker: true` in `.dagron/ci.yaml`, where dagron-ci's operator
  lists the repository (`CI_GITHUB_DOCKER`), and `dagron-ci run` gives one
  with `--docker-daemon`. Where the operator names Sysbox's runtime for it
  (`DAGRON_DOCKER_DAEMON_RUNTIME=sysbox-runc`, Sysbox installed on the
  host; `dagron-ci run` passes the variable on), the daemon runs
  unprivileged, in a user namespace of its own, as the task's first
  service.
- **The conformance suite** (`conformance/`): GitHub's workflow templates
  for Rust, Go, Node.js, Python and a Docker image, each with
  a small project, run on dagron-ci (`conformance/run.sh`) and on GitHub
  (a workflow dispatched by hand), and their
  jobs' conclusions compared (`conformance/score.py`). The first score was
  4 of 5 jobs agreeing (80%): the Docker template's `docker build` had no
  daemon on dagron-ci. With a job's own daemon, all five succeed on both:
  5 of 5 (100%).
- **`dagron-ci run` runs workflow files.** A file of `.github/workflows`
  named, or listed in `.dagron/ci.yaml` (`github.workflows`), is read as the
  event workers read one, with the repository's `runs_on` and `provided`.
  Its actions of other repositories are not fetched here: a step that uses
  one fails unless `provided` lists it.
- **`dagron-ci run --report FILE`** writes each pipeline's result, and each
  of its jobs', as JSON. (`uses: docker://image`, and an
  action of `runs.using: docker` whose image is one), on the Docker
  executor, in a container of the image the engine starts beside the job.
  - It has the job's workspace at `/github/workspace`, where it starts, the
    job's network and user, and what the job is held to; a run that is not
    trusted names an image of `DAGRON_UNTRUSTED_IMAGES`.
  - It has the step's variables, each input as `INPUT_<NAME>`, and runner
    files of its own, which are the job's once it has ended; an action's
    `runs.args` are read against its inputs, and its `pre-entrypoint` and
    `post-entrypoint` run.
  - The step runner asks the engine's results service for it
    (`dagron-ci-exec container`), so it needs `DAGRON_ACTIONS_CACHE_LISTEN`.
  - On Kubernetes too, as an ephemeral container of the job's own pod,
    with its workspace, its network and its job's container's security
    context. A run that is not trusted runs none there: Kubernetes holds
    an ephemeral container to no limit. **The chart's engine Role gains
    `get` and `patch` on `pods/ephemeralcontainers`;** an engine whose
    Role is its operator's own needs them granted.
  - Not yet: an action whose image is a Dockerfile; its step fails, and
    says so.
- **A workflow's `actions/upload-artifact` and `actions/download-artifact`
  work** (v4). The engine's results service, the one `actions/cache`
  speaks, is GitHub's artifact service too.
  - An artifact is its run's, by its name: any job of the run lists and
    downloads it, no job of another run sees it, and a name already in
    the run is refused, as on GitHub.
  - Its bytes are in the cache store (`DAGRON_CACHE_URL`) under
    `workflow-artifacts/`, with their SHA-256; the index is the engine's
    table `workflow_artifacts`. A download reads the store directly.
  - It is kept as long as the job asks and no longer than
    `DAGRON_ACTIONS_ARTIFACT_RETENTION_DAYS` (90), and may be as large as
    `DAGRON_ACTIONS_ARTIFACT_MAX_BYTES` (5 GiB).
  - **`DAGRON_ACTIONS_CACHE_URL` may no longer have a path**: the artifact
    actions keep only its origin. And `ACTIONS_RUNTIME_TOKEN` is now a JWT,
    whose `scp` names the run and the job, as GitHub's does.
- **A workflow's `actions/cache` works.** The engine answers GitHub's cache
  service, version 2, which `actions/cache` v4.2 and later speak, where the
  operator sets `DAGRON_ACTIONS_CACHE_LISTEN` and `DAGRON_ACTIONS_CACHE_URL`
  (`https://`, unless `DAGRON_ACTIONS_CACHE_INSECURE=allow`), beside the
  path caches' store (`DAGRON_CACHE_URL`).
  - Entries are path caches: their quota, eviction and scopes. A job
    restores from its own ref, then from the branch a pull request merges
    into. A run that is not trusted saves nothing.
  - A restore reads the store at a URL the engine signs. A save is uploaded
    to the engine, kept in `DAGRON_ACTIONS_CACHE_DIR`, and put in the store
    as the action finalizes it.
  - A job is given `ACTIONS_RESULTS_URL`, `ACTIONS_CACHE_SERVICE_V2` and a
    credential of its own, `ACTIONS_RUNTIME_TOKEN`, signed by the engine.
    dagron-ci gives a job that uses actions the ref it restores from after
    its own, `DAGRON_ACTIONS_CACHE_RESTORE_FROM`, which a workflow or a
    pipeline may not set.
- **A workflow's job is given a `GITHUB_TOKEN`.** Where the operator sets
  `CI_GITHUB_TOKEN=on`, the engine asks dagron-ci's token service for one
  as each attempt starts, and has it revoked once the attempt is over.
  - It is an installation token of dagron-ci's GitHub App, for the job's
    repository alone.
  - It may do what the job's `permissions` grant, by the App's names,
    `contents: read` where nothing says. A run that is not trusted is given
    read alone.
  - A step reads it as `secrets.GITHUB_TOKEN` or `github.token`. It is
    masked in the log, and on Kubernetes it is in the task's Secret.
  - dagron's task key is `forge_token: { permissions }`. The token
    service's credential row records the permissions each token was given.
- **A workflow's job may ask for an OIDC token.** Where the operator sets
  `CI_GITHUB_OIDC=on`, a job whose `permissions` give it `id-token: write`
  (or `write-all`) is given the identity the engine gives a task, and
  GitHub's `ACTIONS_ID_TOKEN_REQUEST_URL` and `ACTIONS_ID_TOKEN_REQUEST_TOKEN`
  for it.
  - Its audiences are the forge's own (`https://github.com/<owner>`, as
    GitHub's), then those the repository names in `.dagron/ci.yaml`
    (`github.oidc_audiences`).
  - A run that is not trusted is given none, as GitHub gives a fork's pull
    request none.
  - No step may set `ACTIONS_*` for the steps after it through
    `GITHUB_ENV`.
- **A JavaScript action runs.** On dagron-ci's GitHub front-end, an
  action of `runs.using: node20` or `node24`, local or of another
  repository, now runs, as GitHub's runner runs one.
  - Its `pre`, its `main` (inputs as `INPUT_<NAME>`, its outputs the
    step's) and its `post`, when the job's steps are over, the last first,
    with `GITHUB_STATE` carried between them.
  - It runs on Node.js 24, which the engine installs beside the step
    runner for a job whose steps use an action (`workspace.tools: [node]`,
    `dagron-ci-exec install DIR --with node`). It is one build that runs
    in a job image of any libc: Alpine's, with its loader and C++
    libraries beside it.
  - The step runner's image (`DAGRON_CI_EXEC_IMAGE`) holds it, and is
    some 130 MB larger for it. A job that uses no action is not given it.
  - What a JavaScript action leans on is not there yet: no cache or
    artifact service, no token. `actions/cache` warns and goes on.
- **A workflow step may use an action of another repository.** On
  dagron-ci's GitHub front-end, `uses: owner/repo@ref` (and
  `owner/repo/dir@ref`) is no longer refused. A composite action runs as a
  local one does.
  - The engine fetches each repository a job's steps use before the job
    starts, once for that ref, with `DAGRON_CHECKOUT_IMAGE` and no
    credential. It goes into a directory the job reads at `/dagron/actions`
    and cannot change. A task's `workspace.actions` names them; dagron-ci
    writes it for a workflow.
  - `CI_ACTIONS_MIRROR` fetches actions from a mirror in place of
    github.com. `CI_ACTIONS_ALLOW` lists the actions that may be used,
    `owner/*` or `owner/repo`; where it is set, each must be pinned to a
    commit's full id.
  - Not run yet, and each fails its step saying so: an action of
    JavaScript or of a Docker image (the repository is fetched, and the
    step fails as it starts), and an action that a composite action's own
    step uses. A Docker action (`docker://…`) is refused when the file is
    read.
- **A workflow step may use a local action.** On dagron-ci's GitHub
  front-end, a step with `uses: ./dir` and its `with:` is no longer
  refused. The step runner reads the composite action from the job's
  workspace as the step starts, `action.yml` or `action.yaml`, and runs
  its steps in the step's place.
  - Its inputs are what the step gives it, and its defaults; its outputs
    are the step's.
  - Its steps have a `steps` context of their own, and may use another
    local action, ten deep at most.
  - An input reaches a script of it as a variable, never as the script's
    text.
  - A job with such a step is handed on in a fourth step protocol, which
    only a dagron-ci-exec of this release speaks. A job without one is
    handed on as before.
  - An action of JavaScript or of a Docker image still fails its step,
    and says so. `docs/CI_GITHUB.md` has the details.
- **dagron's Postgres-backed tests run against Postgres 14 to 17.** A
  scheduled matrix runs dagron-api's, dagron-gitops' and dagron-ci's tests
  against each of 14, 15, 16 and 17 when the schema, the queries or those
  crates change; a pull request still runs them against 16.
  `docs/CONFIG.md` says so.
- **A merge-queue group that is let go of has its runs cancelled.** When
  GitHub's merge queue drops a group without merging it (`merge_group`
  `destroyed`, invalidated by a group ahead of it, or dequeued), dagron-ci
  cancels that group's runs still going, through each run's cell, and
  withdraws its events still waiting; the group's checks then read
  cancelled. A group that merged is left alone. The event is
  `merge_group_destroyed`.
- **Agents in CI: three recipes, `examples/ci/agents`.**
  - **Review**: `review.yaml` sends a pull request's diff to any endpoint
    that answers OpenAI's chat completions API. It keeps the findings that
    name a line the pull request adds, and writes them as SARIF:
    annotations on those lines where jobs are check runs. It is advisory: a
    finding fails nothing, a model that cannot be asked fails the job. The
    model's address and key are an environment's (`agent-review`), whose
    policy says which runs are given the key: by default, trusted runs alone.
  - **Triage**: `retry_budgets` by what broke a job. A run's failure summary
    (`GET /api/runs/{id}`, `/wait`) now says what broke its failed task and
    what another attempt is worth (`fault_class`, `disposition`), and each
    task row its `fault_class`.
  - **Fix on failure**: a coding agent runs the pipelines through
    dagron-mcp with an agent's token, reads what failed, and pushes a fix.
  - Not yet: a pipeline that starts when another fails, and the loop run
    inside CI.
- **An agent has a token of its own, and dagron-ci can run its work as the
  `agent` class.**
  - A person signed in mints one in dagron-api: `POST /api/tokens` with
    `agent: "<name>"`. It presents as `agent:<name>`, never as the person,
    with the person's groups less `admin` and `ci:trigger`. It reads, and
    reruns or cancels the runs whose trigger names it; any other change is a
    `403`. An agent's name stays its first owner's (`agent_names`,
    migrations 086 pg and 063 sqlite), so its runs never pass to another
    person's agent.
  - dagron-ci runs an agent's dispatch where `CI_AGENT_RUNS=untrusted`, on a
    cell that takes the trigger record, as an `agent` run: confined as an
    untrusted run is (the floor and the limits, no approval, no identity, no
    cache written), reading an environment only where its policy names
    `agent`. The agent reruns and cancels the events it asked for, and no
    other. Nothing of an agent's dispatch that did not run is said on the
    commit; why is on its event. `refuse`, the default, keeps the class
    refused.
  - Not yet: a run of a patch bundle, and an approval that an agent's run
    waits on before a deploy.
- **dagron-mcp drives CI: four `dagron_ci_*` tools.** With `DAGRON_CI_URL`
  set, an agent runs a repository's pipelines at a ref (`dagron_ci_run`,
  with an idempotency key), waits for them up to ten minutes and is told the
  outcome and the first failed job's last lines (`dagron_ci_wait`), reads
  their logs failures first within a byte budget (`dagron_ci_logs`), and runs
  the failed jobs again (`dagron_ci_rerun_failed`). They call dagron-ci's run
  API with the server's own token, under the same rule for plaintext `http://`
  as dagron-api's; the two writes are reviewed in `docs/MCP.md`. With an
  agent's token (below), the runs are the agent's.
- **A job can be given an identity a cloud trusts: `identity: { oidc: {
  audience } }`.** It asks dagron-api's issuer for an OpenID Connect token
  and trades it with AWS, GCP or Azure for credentials, holding no secret of
  the cloud's.
  - dagron-api is the issuer where `DAGRON_OIDC_ISSUER` is set:
    `/.well-known/openid-configuration` and `/.well-known/jwks` at its path,
    RS256 keys from `DAGRON_OIDC_KEYS` (a PEM file or a directory of them; the
    newest signs, every one is published, so a key is rotated by adding
    one), and tokens of `DAGRON_OIDC_TOKEN_SECS` (600).
  - The engine gives such a task, as it dispatches it, where to ask
    (`DAGRON_OIDC_REQUEST_URL`, the engine's own setting), a request
    credential of its own (`DAGRON_OIDC_REQUEST_TOKEN`) and its first
    audience. `GET /oidc/token?audience=…` takes the credential while the task
    runs and answers `{"value": "<jwt>"}`, as GitHub's request endpoint does.
    Only the credential's SHA-256 is kept (`oidc_requests`, migrations 085 pg
    / 062 sqlite).
  - The claims are the run record's: `sub` is `repo:<host>/<owner>/<name>`
    and `:environment:<name>`, `:pull_request` or `:ref:<ref>`; beside it the
    repository, ref, commit, pull request, trust, pipeline, run, task,
    attempt, environment and actor.
  - Only a trusted run's task is given one. A run that is not trusted has the
    task refused (`identity` is among what such a run may not use), and the
    issuer refuses it too; a run that may not use the task's environment is
    refused the identity, since a token names the environment. An engine
    with no `DAGRON_OIDC_REQUEST_URL` refuses such a task and says why.
  - dagron-ci takes `identity:` on a job, checked as dagron checks it, where
    it was a reserved key that was refused.
- **A job keeps directories between runs: `workspace.cache`.** A cache is a
  prefix, the files of the workspace that make its key (`key_files`, globs),
  and the directories it keeps (`paths`). It is restored before the job's
  steps (the entry of its key, else the newest of the prefix, of the run's
  own ref and then of the branch a pull request or merge group merges into)
  and saved after they succeed, unless the job restored that very key.
  - The store is the engine's: `DAGRON_CACHE_URL` (`s3://`, `gs://`, `az://`),
    an object store that signs URLs. The engine signs them as it dispatches a
    job with caches (`DAGRON_CACHE`) and never moves the bytes; a store that
    cannot sign is refused at start. A repository's entries are kept to
    `DAGRON_CACHE_QUOTA_BYTES` (10 GiB), the least recently read going first,
    and an entry unread for `DAGRON_CACHE_UNUSED_DAYS` (7) goes. The index is
    a table of the engine's (`path_caches`, migrations 084 pg / 061 sqlite).
  - Only a trusted run is given a URL to save with. A run that is not trusted
    (a fork's pull request) restores and never writes what a later run reads,
    and where a job restores from is dagron-ci's to say, not its pipeline's.
  - `dagron-ci-cache`, a static tool in the tools image beside
    `dagron-ci-exec`, restores and saves from inside the job, which must
    reach the store's host. `dagron-ci-exec install` copies it too; the
    release's `dagron-ci-exec` archive carries both.
  - A job's steps are told `CI_BASE_REF`: the branch a pull request or merge
    group merges into, as a ref. Every compiled pipeline declares
    `ci_base_ref`.
  - `dagron-import github` writes a cache from `actions/cache` (the prefix
    from the key's own words, its files from `hashFiles(…)`),
    `Swatinem/rust-cache`, and setup-node's, setup-python's, setup-go's and
    setup-java's `cache:`.
  - A cache never fails a job: one not restored or saved says why in the
    job's log. Not built: a store behind a private certificate authority;
    GitHub's cache service for a workflow's jobs.
- **`dagron-import github` writes a dagron-ci pipeline for a GitHub Actions
  workflow.** Its triggers and their filters, its jobs as tasks, `needs`,
  `run:` steps, a matrix written out as a task's items, services, timeouts
  and conditions of the kinds a pipeline can hold. A label GitHub hosts and
  an action that installs a toolchain become the job's image; an
  expression in a script becomes the variable a step reads here. Whatever
  has no place is named in a report at the head of the file, as changed,
  not carried over, or left to do by hand, and a job with something left
  to do starts with a step that fails and says what, so that what is
  written is a pipeline `dagron-ci validate` takes and nobody takes for
  finished. What is left of the workflow as a whole is every job's to
  say; a pattern GitHub reads another way (`+`, `?`) is written wider and
  left to do; and no `{{` of the workflow's is read as a template
  ([`docs/CI_MIGRATING.md`](docs/CI_MIGRATING.md#a-workflow-as-a-pipeline)).
- **A golden corpus and fuzz targets for dagron-ci's compiler.**
  `crates/dagron-ci/tests/corpus` holds 141 cases, a pipeline or a workflow
  file with an event, each beside a file with all of what came of it; a
  change to the compiler shows there to be read. Twenty workflows hold
  the importer the same way. `crates/dagron-ci/fuzz` has a `cargo fuzz`
  target for each parser that reads something written outside dagron-ci
  ([its README](crates/dagron-ci/fuzz/README.md)).
- **A run over its workflow's cap can wait its turn, or take the older
  run's place (`when_full`).** A workflow with `max_active_runs` refused a
  run that arrived while that many were in flight, and still does unless it
  says otherwise. With `when_full: queue` the run is made instead, and
  waits: the submit answers `201`, the run reads `running` with `queued_at`
  set, none of its tasks advances, and it is let in when a run before it
  has ended, oldest first. With `when_full: supersede` (and a cap of 1) the
  run is made and the runs of its key before it are cancelled, as a
  person's cancel would: a newer data refresh, or a newer push, takes the
  place of the older one, and starts when what that one had running has
  stopped. A superseded run ends `cancelled`, is reported as any ended run
  is, and says which run took its place.
  - A run that waits is a run. It is cancelled as any other, it holds its
    place in its `concurrency_key`, and its `run_timeout_secs` is counted
    from when it is let in. The console (its runs list and the run's page,
    where a duration would be), the engine's own page and both
    APIs say that it is queued (`queued_at`, `admitted_at`).
  - At most `DAGRON_MAX_WAITING_RUNS` (100) runs wait for one workflow, or
    one key of it. A run past that is refused, as before.
  - A rerun, a retry or a clear of a run that has ended does not wait, and
    takes nobody's place: while its key is full it is refused, as before.
  - A run that is not trusted, a pull request from a fork, supersedes the
    runs of its own pull request alone, whatever key its workflow writes.
  - `scheduler_runs_waiting`, `scheduler_runs_waiting_oldest_age_seconds`
    and `scheduler_runs_admitted_total` ([docs/METRICS.md](docs/METRICS.md)).
  - Two columns and a partial index on `workflow_runs` (migrations 082 and
    083 on Postgres, 060 on SQLite). Upgrade every engine before a
    workflow says `when_full`: an older engine lets no waiting run in
    ([docs/UPGRADING.md](docs/UPGRADING.md)).
- **A pipeline's newer run can cancel the older one: `concurrency:
  { cancel_superseded: true }`.** dagron-ci refused it, and takes it now:
  a push to a pull request cancels the run of the push before, whose
  checks say so, and starts when its jobs have stopped. A workflow file's
  `cancel-in-progress: true` is read the same way. Without it a run over
  its key still waits its turn, and now waits **in the engine**: the run
  is made at once, its jobs show on the commit as queued, and its page
  says so, where the event used to wait in dagron-ci with nothing to see
  ([docs/CI_PIPELINES.md](docs/CI_PIPELINES.md#concurrency)).
- **A pull request is given one comment that says how its runs went.**
  With `CI_GITHUB_PR_COMMENT=on`, dagron-ci keeps one comment on each pull
  request of a repository the App is installed on: each pipeline that ran
  for the pull request's latest commit, linked to its run's page, each of
  its jobs with how it ended, what a job said of its tests, and the tests
  that failed by name. It is written again in place as the runs change (at
  most once in twenty seconds), a new commit's runs take the place of the
  last one's, and one that was deleted is made again. It is written with
  a token of the App's for `pull requests: write` alone: an App without
  the permission writes none, and its statuses and check runs go on. Names
  a pull request's author may have chosen go in as code, and are not read
  as Markdown. Off unless asked for. dagron-ci's schema gains a table,
  `ci_pr_comments`, a column of `ci_runs` and an index of `ci_events`,
  which it adds when it starts
  ([README](crates/dagron-ci/README.md#a-pull-requests-comment)).
- **The chart takes fleet mode's label map as a value.** `ci.fleet.labels`
  is the map a file named by `CI_GITHUB_FLEET_FILE` holds: the chart writes
  it to a ConfigMap, mounts that in dagron-ci's container in either place
  it runs, and names the file to it. Until now the chart mounted no file of
  an operator's, so the map had to be in dagron-ci's image. dagron-ci reads
  the map once, as it starts, so its pod is annotated with the map's
  checksum and a map that changed starts it again. The render fails on what
  dagron-ci would not start on: a fleet with no cell and token or no App,
  and a map it would refuse (a key a label does not take, a label GitHub
  gives every runner, a label with no image). It fails on a fleet with no
  `ci.webhookSecret` too: a job is heard of by its delivery alone, and a
  release's replicas are all set alike.
- **Three documents for dagron-ci, and a starter for a dbt project.**
  [`docs/CI_MIGRATING.md`](docs/CI_MIGRATING.md) maps a GitHub Actions
  workflow, a `Jenkinsfile` and a `.gitlab-ci.yml` to a pipeline, key by
  key, with what has no place to go yet.
  [`docs/CI_SECURITY.md`](docs/CI_SECURITY.md) says whose word dagron-ci
  takes for what, what an untrusted run is held to, what each of its
  credentials can do, and what is not held.
  [`docs/CI_RUNNERS.md`](docs/CI_RUNNERS.md) is an operator's notes on what
  runs CI jobs: setting up, bounding, watching and repairing it, fleet
  mode's runners among it. [`examples/ci/dbt.yaml`](examples/ci/dbt.yaml)
  builds a
  dbt project against a Postgres of the job's own. The pipeline reference
  now says two things it left out: a job that its own `when:` skips does
  not hold back the jobs after it, and a job with steps hands its status,
  and no value it printed, to a later job.
- **The console shows a repository's branches and pull requests, and a CI
  job's log by step.** A **Repositories** page lists each repository a CI
  service has run for, and a repository's page lists its pull requests,
  branches and tags, each with the runs for its latest commit, one a
  pipeline. A run's page gains a **Jobs** view, where a job that ran as
  several instances (a matrix, a fan-out) is a row of them. Its task's
  drawer shows a CI job's log by step, the failed and running steps open,
  with what the job said of its tests (`report:`) and its steps' summaries
  above it. Its header links the repository's page and the same event's
  other pipelines. All of it reads dagron-api, over the trigger record
  every run is made with: `GET /api/runs/sources`,
  `GET /api/runs/refs?repo=`, and `?event=` on the run list. Not built: a
  job's log is still read whole on each poll, a matrix's values are not
  shown beyond an instance's name, and nothing is annotated beside the
  source. [`docs/CI.md`](docs/CI.md#in-the-console).
- **Starter pipelines for Go and for Java**, beside Rust, Node and Python.
  The Python one says what its tests found: pytest's JUnit file, named
  with `report:`. A suite that pytest names for itself counts a test's
  line from 0, and its lines are read as one more.
  [`examples/ci`](examples/ci).
- **Releases are signed.** The release workflows sign what they publish,
  with cosign and no key: the workflow's own identity, which GitHub
  vouches for, recorded in a public log. Each image and the chart is
  signed by its digest, and each image carries an SBOM (SPDX) and SLSA
  provenance from BuildKit for each platform. The binaries' `SHA256SUMS`
  is signed, each release file has a signed statement of where it was
  built, and each binary has an SBOM (CycloneDX). Each workflow verifies
  what it signed before it is done. Nothing published before this is
  signed, and the first release cut with these workflows is what proves
  them. [`docs/VERIFY.md`](docs/VERIFY.md).
- **Artifacts are indexed, and go straight to the store where it can take
  them.** dagron-api records each artifact put through it (its size, its
  sha256, when it expires), lists a run's
  (`GET /api/runs/{id}/artifacts`), deletes one (`DELETE` on its
  route), and deletes those whose time is up
  (`DAGRON_ARTIFACT_RETENTION_DAYS`, or `?retention_days=` on an upload).
  `POST …/upload` and `GET …/download` answer with a URL the store signs
  for the one object (S3 and what speaks it, GCS, Azure Blob), so the
  bytes need not pass through the API or under its 128 MiB cap; a store
  with none to give (a directory, one that seals what it keeps, a tiered
  one) is relayed as before, and a client is written for both
  (`DAGRON_ARTIFACT_PRESIGN`). Not built: a direct upload in parts,
  quotas, a tiered store's delete. A file a task writes for itself is not
  in the index until `POST …/complete` records it.
  [`docs/API.md`](docs/API.md#artifacts-encrypted-at-rest--g-c2).
- **Fleet mode: dagron-ci runs GitHub Actions' own jobs on your
  capacity.** With `CI_GITHUB_FLEET_FILE`, a job whose `runs-on` asks for
  one of an operator's labels is given GitHub's own runner, registered
  just in time and started as a dagron run of one task, with the label's
  image, runner class, pool and priority. The runner's configuration is
  kept as a secret of an environment of the cell, and is in nothing a
  reader of the run sees. A runner nothing waits for is taken back, its
  registration first. A fork's job, an organization's runners, quotas and
  warm capacity are the entries above.
  [`crates/dagron-ci/README.md`](crates/dagron-ci/README.md#fleet-mode).
- **dagron-ci reports a job as a check run, where it is told to.**
  `CI_GITHUB_CHECKS=on` posts each job of a repository the App is installed
  on, with `checks: write`, as a check run and not a commit status, under
  the name its status had. It is posted with a token of the App's that is
  for check runs alone, brought up to date as the job moves, and reads
  cancelled when its run is. A check's "Re-run" on GitHub reruns its run
  from its failed jobs, and "Re-run all checks" each run of the commit
  that was reported as check runs. An App without the permission, and any
  other repository, keeps its statuses.
  [`crates/dagron-ci/README.md`](crates/dagron-ci/README.md#check-runs).
- **A job's test report and its steps' summaries go on its check run.** A
  pipeline's task names the files its steps write of its tests
  (`report: { junit: …, sarif: … }`), and dagron-ci-exec reads them when
  the steps are over, whatever became of them. Where a repository's jobs
  are check runs, each failed test and each SARIF result that names a
  file of the repository is an annotation on that line of the commit, up
  to 50, and the check's page has the counts, each failure by name, and
  what the steps wrote to their summaries (`CI_STEP_SUMMARY`;
  `GITHUB_STEP_SUMMARY` in a job that came from a GitHub workflow). They
  are the job's own word, read from its log, and never how it ended.
  `dagron-ci run` prints a report's counts, places and failures as the
  job ends. `report` on a task was refused until now. Not built:
  annotations from a workflow's commands, and a report for a workflow's
  job.
  [`docs/CI_PIPELINES.md`](docs/CI_PIPELINES.md#report).
- **dagron-ci's repositories are connected through its API, and each
  change says who made it.** `GET`, `PUT` and `DELETE
  /ci/v1/repos/{host}/{owner}/{name}`, and `GET /ci/v1/repos`, for an
  admin with a dagron-api credential. A change is recorded with its caller,
  and one made with `dagron-ci repo` with the operator's name;
  `dagron-ci repo show` prints them.
  [`crates/dagron-ci/README.md`](crates/dagron-ci/README.md#connecting-a-repository).
- **A checkout may come with its history: `workspace.checkout.depth`.**
  Unset or 1 is the commit alone, as before. 0 is every branch's history
  and the tags, for a task that asks git what changed. A dagron-ci job
  says `workspace: { checkout: { depth: 0 } }`, and a workflow's checkout
  step its `fetch-depth`.
- **A task is given what an earlier task came to: `env_from:`.** A
  variable holds an earlier task's status, or the last line it printed
  (`env_from: [{ name: TARGET, task: decide, read: last_line }]`), read
  when the task is claimed. The task named is one it depends on. The value
  is a variable's whole value and is never filled into a text; a line over
  32 KiB fails the task that asked for it. [`docs/HOWTO.md`](docs/HOWTO.md)
  §11.
- **A workflow's matrix may be what an earlier job computed.** An axis
  that is one expression (`module: ${{ fromJSON(needs.find.outputs.list) }}`),
  or the whole matrix. A task plans the job's instances from what the jobs
  it needs came to, and the engine makes a task of each as the run goes.
  A checkout step's `fetch-depth` is taken, and the `github` context has
  a push's `event.before` and `event.after`.
- **A workflow's jobs hand each other values.** dagron-ci's GitHub
  front-end takes a job's `outputs:`, and `needs` in a job's `if:`, its
  steps and its outputs. A job whose `if:` reads `needs` is decided as it
  starts, by the step runner, which speaks a third step protocol for it: a
  job says its result and outputs on its last line, and writes everything
  to one stream.
- **dagron-ci reads a `.github/workflows` file, at level 1.** A workflow
  file a repository lists runs as a pipeline, beside its native ones: its
  triggers, its jobs, their matrices, containers and services, and their
  `run:` steps. Off unless an operator sets `CI_GITHUB_WORKFLOWS=on`, and
  then only for the files a repository lists in its `.dagron/ci.yaml`.
  - **Level 1 is the file itself.** An action (`uses:`) is not run. The
    checkout is answered by the job's own workspace, an action the
    repository says its image provides is left out, and any other is
    refused by name, with a job's `outputs`, `needs` in an expression,
    `secrets`, a matrix that is an expression and `cancel-in-progress`.
  - **Expressions** are read by GitHub's documented grammar, in a crate of
    their own (`dagron-ci-expr`).
  - **A job's `if:`** is decided when the run is made. A job it leaves out
    gets a passing status that says it was skipped.
  - **A step is the step runner's**: dagron-ci-exec speaks a second step
    protocol, for a step's `if:`, its shell and directory, and the files
    it writes for the steps after it (`GITHUB_ENV`, `GITHUB_PATH`,
    `GITHUB_OUTPUT`).
  - **An expression in a script** is pasted where no outsider chooses its
    value, and is a shell variable where one may: a branch's name or a pull
    request's title is a value, and never part of a command.
  - **A `{{` in a workflow file is text.** dagron fills `{{ name }}` in a
    native pipeline's text; nothing of a workflow file, or of what a caller
    sends it as an input, is read so.
  - **`dagron-ci validate`** checks a workflow file and the settings file.
  - Not here: most of the `github` context beyond what an event tells.
    (`hashFiles`, `GITHUB_TOKEN` and workflow commands came later, above.)
  ([docs/CI_GITHUB.md](docs/CI_GITHUB.md))
- **A service beside a task: `services:`.** A task names the containers it
  wants beside it while it runs, a database for a test or a proxy for a
  data task, each from an image of its own. The executor starts each before
  the task and sees it ready (`ready`, a command run in the service's own
  container until it exits 0), and removes it when the task ends, however
  it ends. The task reaches a service at its name (`db:5432`) and at
  `localhost`: it and its services share one network.
  - **On Kubernetes** a service is a sidecar container of the task's pod,
    with `ready` as its startup probe. It needs Kubernetes 1.29 or later,
    and a task with services is refused on an older cluster. The task has
    ended when its own container has: its pod is not waited for while its
    services stop.
  - **On Docker** a service is a container made for the attempt, and the
    task's container is made in its first service's network.
  - **An engine that runs tasks as its own processes** (`EXECUTOR=local`)
    fails a task with services once, with the reason.
  - **A service that is not ready fails the attempt**, with the service's
    name and the last it wrote, and the task never starts.
  - **A service is held as its task is**: its isolation, and in a run that
    is not trusted the same limits and the same list of images.
    `DAGRON_UNTRUSTED_LIMITS` takes `services`, the most such a task may
    have (4 by default, 0 for none).
  - **In a dagron-ci pipeline** a job's `services:` was refused as not
    built. It is taken now, and `dagron-ci validate` checks it as dagron
    does.
  - Not here: a secret in a service's environment, which is plain values;
    a service's own log, which is kept only for why it was not ready; and
    on Podman, a service's name for a task that asked for no network, which
    reaches it at `localhost`.
  ([docs/HOWTO.md](docs/HOWTO.md), "A container beside a task")
- **What an untrusted run may spend is the operator's to say.** A fork's
  pull request, run as untrusted, wrote its own timeouts, its attempts and
  how many jobs it has, took a place in line by naming itself as one of the
  installation's pipelines, and could name any image its host can run. Each
  was found by having such a pull request try it.
  - **Time, attempts and tasks.** `DAGRON_UNTRUSTED_LIMITS` takes three
    more: `timeout` (a task's longest, 3600 s by default, and what a task
    that names none has), `attempts` (3) and `tasks` (256 a run). A task's
    own numbers stand where they are less. A run with more tasks than it
    may have has none of them run, and such a run is the size it was made:
    a fan-out over what a task printed (`with_output_of`) is refused, since
    it grows a run after its first tasks have run.
  - **A pool of their own.** Every task of such a run draws from the pool
    `untrusted`, whatever pool it named: `POOLS=untrusted:4` caps how many
    run at once on an engine. A task's own `pool` was dropped before.
  - **A line of their own.** `max_active_runs` counts by a workflow's name
    and its `concurrency_key`, which its author writes. Such a run's cap is
    now counted among such runs: it holds no slot of a workflow named as
    one of the installation's, and is still capped among its own kind.
  - **The images it may name** (`DAGRON_UNTRUSTED_IMAGES`, patterns such as
    `docker.io/library/*`). On Docker a private image is one the host
    already holds, and a task that named it would read it. Unset, any
    image, as before.
  - **A loop is refused** (`repeat`): how often it goes round is its
    author's to write. So is `with_output_of`, for the same reason.
  - What is still its author's: a pod's `resources` and a workspace's size
    on Kubernetes, which are its namespace's to cap.
  ([docs/HOWTO.md](docs/HOWTO.md), "Runs that are not trusted")
- **dagron-ci's run API checks the commit it is given against the ref.**
  `POST /ci/v1/runs` with a `sha` ran that commit as the ref's, whatever
  commit it was: any commit of a fork is reachable in the repository it
  forks, so a person who may ask for runs could have one of their own run
  as main's, and read what an environment's policy keeps for main. The
  commit is now the ref's own, or one the ref has behind it, or the run is
  not made (`422`, "the commit is not on the ref"). A GitHub repository is
  asked through its API; on another forge only the ref's own commit is
  taken. A request that names a commit now needs the forge, where it
  needed none.
- **A pull request from a fork can be run, as an untrusted run.** It was
  refused, and still is unless an operator says otherwise
  (`CI_FORK_PRS=untrusted` on dagron-ci, and a floor for such runs on the
  engine). A fork's pipeline is its author's to write, so what holds its
  run is the run's record, which says `untrusted`, and never the pipeline.
  - **The engine does nothing on such a run's word but run its containers.**
    A run that is not trusted may not use `notify`, `on_datasets`, or a
    task's `produces`, `defer`, `service_account`, `gang`, or a `type:`
    other than an ordinary task's: each has the engine post, call, start
    or ask something for the workflow. Such a run is not made (`403`, with
    the reason), and a task that names one is failed at dispatch. Every key
    a workflow may write is sorted into what such a run may use, may not,
    and is not given; a test fails when a key is added to a workflow and to
    none of the three.
  - **It is not given what other runs share.** A task's `cache` (an output
    kept under a workflow's and a task's name was every run's of those
    names), a slot of a `pool`, a `priority` above the default, and the
    attempts a failure's class gives, which is read off what the task
    printed.
  - **An engine runs such work only where it is told to, and can hold it.**
    `DAGRON_UNTRUSTED_ISOLATION_FLOOR` is a second floor, for those runs'
    tasks. An engine with none fails each of their tasks with the reason.
    One with a floor does not start with `EXECUTOR=local`, on Docker
    without `DAGRON_DOCKER_NETWORK`, or on Kubernetes with a floor that
    does not say what the pod may reach. Such a task is given no
    service-account token.
  - **On Docker** such a task is given none of `DAGRON_DOCKER_MOUNTS` and
    not `DAGRON_DOCKER_WORKDIR`, and each container made for it has a limit
    on memory, processes and CPUs (`DAGRON_UNTRUSTED_LIMITS`, default
    `memory=4g,pids=2048,cpus=2`). The Docker executor set none before.
  - **A first contribution waits for an approval** (`CI_FORK_APPROVAL`:
    `first-time`, `always` or `never`; `CI_FORK_APPROVERS`). No pipeline of
    the pull request is submitted until it is given. The approval is a run
    of its own, of one task that dagron-ci writes, which carries the pull
    request's number and its commit and nothing its author wrote. It is not
    a task in front of the pipeline's: a rejected task skips what needed it
    and runs what a pipeline wrote to follow a failure, and the pipeline is
    the pull request's. One commit has one approval, whichever of a
    delivery and a sweep saw it.
  - **It is never run without its record.** dagron-ci sends the record with
    every submit of such an event, and refuses the pull request on a cell
    whose dagron-api would not take it (the `ci:trigger` group): a run with
    no record reads as a person's own.
  - **A resubmit keeps the class.** `POST /api/runs/{id}/resubmit` of a run
    that was not trusted makes a run of that class, for the same commit,
    whoever asks. It made the caller's own, which is trusted.
  - dagron-ci keeps what the forge says a pull request's author is to the
    repository (migration 0013, one column).
  - What it does not hold: the host's kernel, unless the floor names a
    sandbox runtime; and a pod's limits on Kubernetes, which are its
    namespace's. (How long such a run takes, how often it is tried and
    whose place in line it takes are held by the entry above.) An approval
    is not withdrawn when its pull request is closed. Not shown: any of it
    on Kubernetes, or against github.com.
  ([docs/HOWTO.md](docs/HOWTO.md), "Runs that are not trusted";
  [docs/CI_PIPELINES.md](docs/CI_PIPELINES.md), "Pull requests from forks")
- **An environment has a policy: which runs may use it.** Any run that named
  an environment had its variables and its secrets. That was right while
  every run was written by someone who could read the environment anyway.
  It is also all that would stand between a pull request and every secret a
  cell holds, once a pull request from outside can run.
  - A policy admits runs by **trust class** (`allowed_trust`), by **ref**
    (`allowed_refs`, patterns such as `refs/tags/v*`) and by **pipeline**
    (`allowed_pipelines`). A run is admitted when it passes every part that
    is set. `PUT` and `DELETE /api/environments/{id}/policy`, for an admin
    alone; an environment's view has it as `policy`.
  - **An environment with no policy admits `trusted` runs and no others.**
    Every run an installation makes for itself is `trusted`, so nothing
    changes for one that has no CI. A run a CI service makes carries the
    class and the ref of the event it verified, and one whose record names
    no class is read as `untrusted`. A run that a `type: workflow` task
    starts is of the class of the run that called it.
  - **Asked when a run is made.** A run that names an environment that does
    not admit it is refused (`403`, with the reason) before its variables
    are put into anything stored. dagron-ci says the refusal on the commit,
    under the pipeline's name.
  - **Asked when a task's secrets are resolved**, which is the check that
    holds however the run was made: the task fails with the reason, and is
    not started. It is asked before a task's `cache:` is looked in. A task
    served from the cache had never reached it, so what one run printed
    with a secret would have been handed to a run that may not read it.
  - **The engine's own store is a `trusted` run's alone.**
    `DAGRON_SECRET_<NAME>` and the secrets directory belong to no
    environment, so no policy speaks for them. A run of another class reads
    what an environment that admits it holds, and nothing else.
  - A deferred job's headers are asked for when its task is dispatched, as
    well as when the job is polled, so a task that may not have them is not
    started.
  - Migrations: SQLite 058, Postgres 078 (one table,
    `environment_policies`). dagron-api makes the table itself where it
    starts before an engine of this release.
  - A policy by ref holds a person who may ask for runs as it holds a pull
    request: a run asked for through dagron-ci's run API is of a commit
    that is on the ref it names (the entry above).
  - Not in this change: an approval before a protected environment is read;
    a page for a policy in the console, which shows none yet; and anything
    that runs a pull request from a fork, which is still refused. No run
    is `untrusted` yet, so the classes other than `trusted` are held by
    tests alone.
  ([docs/HOWTO.md](docs/HOWTO.md), "Who may use an environment")
- **A task may name an environment of its own** (`environment:` on a task).
  Its `value_from` secrets are resolved from that one in place of the
  workflow's, where its policy admits the run. A pipeline's job may too:
  dagron-ci refused the key until dagron took it. The workflow's
  `{{ env.NAME }}` variables are still the workflow's own environment's.
  Neither name is ever filled in from a parameter.
- **On the Helm chart, a private repository is checked out: dagron-ci runs
  in the engine's pod.** The engine asks dagron-ci for each checkout's
  credential, and takes no address for that over plain http but its own
  machine's. With the two in separate pods the chart had no way to give a
  private repository's checkout one, and its jobs failed there.
  `ci.enabled` now puts dagron-ci in the engine's pod (`ci.placement:
  engine`, the default), as `compose.ci.yaml` has the two in one network
  namespace. With `ci.checkoutTokenSecret` named, the chart sets the
  engine's `DAGRON_CHECKOUT_TOKEN_URL` to the pod's own address and mounts
  the secret in both containers.
  - dagron-ci then has the engine's replicas and its place on a node, and
    the pod is ready when both are. `ci.replicas` and its node settings are
    for `ci.placement: deployment`, a Deployment of its own, and the render
    fails where one is set and would not be used. With a Deployment of its
    own the engine is not told to ask, and a private checkout fails.
  - The pod mounts no service-account token. The engine's container mounts
    its own, and dagron-ci, which takes a forge's webhooks, has none. The
    two do share an address, so dagron-ci reaches the engine's API on it,
    which no NetworkPolicy closes inside a pod (`docs/HARDENING.md`).
  - With `networkPolicy.enabled` the engine's policy admits dagron-ci's
    port, and the engine's own stays closed.
  - `crates/dagron-ci/tests/k3s.sh` checks that pod on k3s: the policy,
    which container holds the token, where the engine is told to ask, and
    that the door takes the secret named and no other. Not shown: a
    private repository checked out on a cluster, from end to end.
- **CI can be learnt: a quickstart, a pipeline reference, what it refuses,
  and three pipelines to start from.**
  - **`docs/CI_QUICKSTART.md`** goes from a GitHub App to a first green
    check on one machine, with **`compose.ci.yaml`**: Postgres, the engine
    on the Docker executor, dagron-api and dagron-ci from published images.
    The three share one network namespace, as a pod does, so that each
    reaches the next on loopback: dagron-ci and the engine start on no
    plain-http address that is not their own machine. It finds a push by
    looking, so nothing has to reach the machine. A section says what the
    Helm chart needs.
    **`compose.ci.build.yaml`** beside it builds the images from a
    checkout, for use before a release has published them.
  - **`docs/CI_PIPELINES.md`** is the reference for a pipeline: `on:`,
    jobs and steps, the workspace, `concurrency:`, what a step knows of its
    event, secrets, and how to check and run one before it is pushed.
  - **`docs/CI.md`** says what dagron-ci does, each thing it refuses with
    why and where it says so, and what this release does not do.
  - **`examples/ci`** has a starter for Rust, for Node and for Python. A
    test compiles each and has dagron read the result
    (`crates/dagron-ci/tests/starters.rs`), so a starter dagron would
    refuse does not ship.
- **A run can be acted on through dagron-ci: stopped, read, and asked for
  from a terminal.**
  - **`POST /ci/v1/runs/{id}/cancel`** stops an event's runs. Each is asked
    of its cell's dagron-api with the caller's own credential, as a rerun
    is, and dagron-ci's record follows the run to its end. An event that
    still waits to be dispatched is withdrawn: it ends `skipped`, with
    who cancelled it, and no worker takes it. One a worker is dispatching
    says so, for the caller to ask again.
  - **`GET /ci/v1/runs/{id}/logs`** is what an event's runs wrote.
    dagron-ci keeps no log: each run's is read from its cell as the caller,
    narrowed by `job`, `status`, `q`, `limit` and `tail`. It is not
    followed, and `follow` is refused so that nobody waits on a stream
    that is not there.
  - **A run's own id.** Each of the run API's doors takes an engine run's id
    in place of an event's. It is the id a commit status links to
    (`CI_RUN_URL`), and so the one a person looking at a failed check has.
    It means that run of its event and no other.
  - **`dagron-ci run --remote`, `rerun`, `cancel` and `logs`** ask those
    doors from a terminal. `run --remote --wait` waits for what it queued
    and exits by how it ended. The server (`--server`, `DAGRON_CI_URL`) is
    `https://`, or `http://` to this machine. The token comes from a file
    or the environment (`--token-file`, `DAGRON_CI_TOKEN_FILE`,
    `DAGRON_CI_TOKEN`), never from a flag, and is stored nowhere: there is
    no login. A redirect from the server is not followed.
  - Not built: a rerun of every job (`failed=false` is still refused; a
    dispatch of the commit runs a pipeline whole again, under checks of its
    own), and a log that is followed.
- **A check that is skipped says so.** A pipeline that takes an event, and
  whose `paths:` name none of the files it changed, does not run, and said
  nothing: a branch that required one of its checks waited for ever. Each
  of its jobs now gets a `success` status under the context its own would
  have had, reading "skipped: no changed file matches on.push.paths" (or
  `on.pull_request.paths`), once the event is settled. A pipeline whose
  `on:` does not take the event says nothing, as before. Nor does one with
  a run of its own at the commit, from another event of it: a passing
  "skipped" is not put over what that run's jobs came to. A run recorded
  while the "skipped" is on its way posts its own statuses again after it.
- **A row for each checkout credential dagron-ci mints.** What said a
  credential had been minted, and revoked, was a log line and two counters.
  `ci_checkout_credentials` now has a row for each: the run, the task and
  the attempt it was for, its repository and commit, the cell that asked,
  when it was minted, when GitHub says it lapses, and when it was revoked.
  - The row is written before the credential is handed over. One that
    cannot be recorded is revoked, and the attempt fails saying so (503).
  - It holds the credential's SHA-256, by which the revoke finds it, and
    never the credential.
  - Nothing drops these rows yet, and no command reads them: they are read
    with SQL.
- **A task can name an allowlist, on Kubernetes: `isolation: { network:
  allow:<name> }`.** Between a task that reaches everything and one that
  reaches nothing: one that reaches what a list lets it, and nothing else.
  - **A task names a list and cannot write one.** What a name allows is the
    operator's, a NetworkPolicy of theirs: `dagron-task-allow-<name>`, the
    pods labelled `dagron.dev/network=allow-<name>`, nothing in, and out only
    its egress rules. `DAGRON_TASK_NETWORK_ALLOWLISTS` names the lists there
    are, and the chart makes them and sets it
    (`engine.taskNetworkAllowlists`). A task that names another is refused.
  - It rests on what `network: none` rests on: `DAGRON_TASK_NETWORK_POLICY`,
    each list's policy read as the engine starts, the namespace's other
    policies read then and before each such task, and a gate before the
    task. A policy with an egress rule that names no `to` is not taken for a
    list: that is every address.
  - **The gate** is `dagron-ci-exec wait-allowlist`. It asks the cluster's
    API server alone, since a list may well allow a name server. So **an
    allowlist must not allow the API server**: tasks under one that did fail
    unstarted, saying so. It needs an exec image of this release or later.
  - A task with a `checkout:` may name a list, which then has to allow the
    forge. A floor may name one (`network=allow:<name>`): `none` is over any
    list, a list is over the executor's own network, and a floor's list is
    pinned as its runtime class is.
  - The Docker executor and the local one refuse it. It was run on k3s, and
    on no other CNI. `docs/FAMILIES.md` says what it does not do: it does
    not judge what a list allows, and it has no rule for a hostname.
  - In `dagron-core`, `isolation::Network` has a third value,
    `Allow(String)`, and is no longer `Copy` or ordered; an `Enforcer`
    carries the lists its operator named. Its spelling in a spec, a floor
    and an attested envelope is one: `allow:<name>`.
- **A task can ask for no network on Kubernetes, and is not started until its
  pod has none.** `isolation: { network: none }` was refused there: a pod has
  no switch for its network, and the executor made no NetworkPolicy.
  - **`DAGRON_TASK_NETWORK_POLICY=enforced`** is the operator's word that the
    cluster's CNI enforces NetworkPolicy, which nothing in the API says. With
    it, such a task's pod is labelled `dagron.dev/network=none`, and the
    standing policy `dagron-task-no-network` of the task namespace allows a
    labelled pod nothing in and nothing out. The chart makes the policy and
    sets the engine (`engine.taskNetworkPolicy`). The engine makes none: it
    reads the policy as it starts, and does not start where it is missing,
    selects other pods or allows anything.
  - **No other policy may give some of it back.** A pod has what *any*
    NetworkPolicy that selects it allows, so a namespace's "every pod may
    reach the name server" would leave such a task resolving names, and the
    gate would not see it. The engine reads every policy of the namespace, as
    it starts and again before each such task is made (`get` and `list` on
    `networkpolicies`), and refuses beside one that selects the task's pod and
    has a rule in either direction. It names the policy; keep it from these
    pods with `{key: dagron.dev/network, operator: DoesNotExist}` in its
    `podSelector`.
  - **The gate.** The task's pod gets one more init container, last, from the
    `dagron-ci-exec` image: `dagron-ci-exec wait-no-network`. It asks for a
    connection at the cluster's API server and at the pod's name servers, and
    goes on once none is made at any of them three times running. A pod that
    still has a network after 30 s fails, and its task is not started. So
    this needs `DAGRON_CI_EXEC_IMAGE`, and an image of this release or later.
    A gate that ended any other way (killed, not started, unable to ask) says
    so, and is not reported as a cluster that enforces nothing.
  - The chart keeps the policy through an uninstall or an upgrade without it
    (`helm.sh/resource-policy: keep`): the tasks it holds are no part of the
    release and go on running. `engine.taskNetworkPolicyCreate: false` leaves
    the policy to whoever applies the namespace's.
  - Why a gate: on k3s, whose own controller applies a policy a second or two
    after a pod starts, a labelled pod reached the API server and another pod
    as it began. A task held by the gate reaches nothing from its first
    instant there.
  - A task with a `checkout:` cannot ask for no network on Kubernetes: the
    checkout is fetched in the same pod. The Docker executor takes both.
  - It was run on k3s, and on no other CNI. `docs/FAMILIES.md` says what the
    gate checks and what it does not: it cannot tell a held pod from an
    outage of every address it asks.
- **The Docker executor puts a task's containers on a network the operator
  names, and holds a task to its `isolation:`.** A container was made with no
  network named and no envelope asked for. So a job was on the daemon's
  default network, beside every other container on the host that named
  none, and a task that declared `isolation:` was refused on Docker, since
  nothing there could hold it to one.
  - **`DAGRON_DOCKER_NETWORK`** is the network every container the executor
    makes is on: a task's, the one that installs its tools, and the one that
    fetches its checkout. One made with `docker network create --internal`
    has no route off the host. It is asked of the daemon at startup, and one
    the daemon does not have stops the engine. `host` is refused. Unset, a
    container is made as it always was.
  - **`isolation: { network: none }`** gives a task's own container a
    loopback interface and nothing else. What is run for it first keeps the
    operator's network, since a checkout is fetched over it. A floor can ask
    it of every task (`network=none`); the `hardened` preset does not.
    On Kubernetes it needs the operator's word and a gate: the entry above.
  - **The envelope is asked of the daemon.** `read_only_root_fs`,
    `drop_all_capabilities` and `no_new_privileges` are asked of every
    container made for the task, `run_as_user` of the task's own, and
    `seccomp: unconfined` where a task opts out of the runtime's profile.
    `run_as_non_root` is held as a kubelet holds it: a task that names no
    uid is refused unless its image's `USER` is a number other than 0.
    `runtime_class` is still refused on Docker.
  - So `DAGRON_TASK_ISOLATION_FLOOR=hardened` starts on the Docker executor,
    where it was a startup error, and a task whose image runs as root is then
    refused with what to change.
  - It is what the daemon was asked for, and a container shares its host's
    kernel: `docs/FAMILIES.md` says what each field becomes and what it does
    not give. Tested against a daemon by a task that reads its own
    `/proc`.
- **A load harness for CI.** `loadtest/ci` loads one cell the way CI does:
  pushes arrive at dagron-ci's webhook door, each becomes a run, and each
  run's job holds an engine slot for minutes and writes a log. It needs a
  container runtime and nothing else.
  - **A synthetic forge** answers the part of GitHub's API that dagron-ci
    calls for a push, for any commit, and takes the commit statuses.
  - **A driver** signs and sends pushes at the rate that keeps a set number
    of jobs running, and reads each engine's `/metrics`. **A reporter** puts
    one step's figures together: how long a delivery, an event and a run
    each waited, the reconcile loop's ticks, each container's CPU, and every
    statement the databases ran, named by what it is for.
  - `run.sh` builds the images from the checkout, stands up Postgres, three
    engines, dagron-api and dagron-ci, runs a step or a file of them, and
    can start a Prometheus and a Grafana beside the cell with the harness's
    dashboard loaded.
  - A step ends when the engines' loops are idle again, not only when
    nothing is left to run: a step begun with the loops still busy on the
    one before it would be measured with that in it.
  - Jobs run on the engines' local executor, as processes. So it loads the
    control plane and nothing of what a job costs on Kubernetes or Docker,
    and what it prints is a measurement of the machine it ran on, not a
    capacity figure. Its README says what each figure is and is not.
  - Nothing in CI runs it.
  ([loadtest/ci/README.md](loadtest/ci/README.md))
- **The published engine and dagron-api images carry the object stores and
  OpenTelemetry.** Both were cargo features that only a source build had: an
  operator who wanted archived runs or artifacts in a bucket, or spans at a
  collector, had to build an image of their own.
  - **The engine images** (`dagron-engine`, `dagron-engine-localdev`) are
    built with `otel`, `archive-s3`, `archive-gcs` and `archive-azure` beside
    `postgres,ops,kubernetes`. `GC_ARCHIVE_URL` takes `s3://`, `gs://`,
    `az://` and `azure://`, and `OTEL_EXPORTER_OTLP_ENDPOINT` turns span
    delivery on.
  - **The dagron-api image** is built with the same three stores, for
    archived runs (`GC_ARCHIVE_URL`) and for the artifact store
    (`DAGRON_ARTIFACT_URL`), and with a new `otel` feature of its own under
    the same variable. It sends the spans it opens, which today are its HTTP
    requests at `debug`: at the default log level it sends nothing, and
    `RUST_LOG=info,tower_http=debug` sends them. What it holds when it is
    stopped is sent as it shuts down.
  - **Each binary says what it was built with** as it starts, on its
    `configuration resolved` line (`features=…`). A part an image lacked
    was found only by the setting that needs it, when it was refused.
  - **The engine binary can be built with a store by name.** `cargo build -p
    dagron --features archive-s3` failed: the features were the engine
    library's, and the binary crate forwarded none of them. It forwards the
    three stores and `archive-parquet` now.
  - **CI compiles what a release will build.** The two feature sets are
    written once, in the release workflow. `scripts/published-features.sh`
    reads them from there, compiles both binaries that way and runs the
    tests those features gate. A release tag was the first build of them
    before.
  - Not in any published image: `mqtt` and `archive-parquet`. A source build
    with default features is as it was.
  ([docs/CONFIG.md](docs/CONFIG.md))
- **An engine on Docker says what the host's log driver loses of a task's
  log, and can keep each container's log in a file.** The Docker executor
  reads a task's log back from the daemon once the container has exited, so
  the log is what the container's log driver kept. Its containers name no
  driver and get the daemon's own default. On Podman that is the journal,
  which drops lines past a rate limit (10,000 in 30 seconds by default)
  that all of a user's containers share: a job that wrote 30,000 lines in a
  second had fewer than 10,000 of them in its log and its output, which
  ended as a whole one does, with nothing to say so.
  - **A warning at startup.** The engine asks the daemon which driver its
    containers get and says what it costs a task's log: the journal's rate
    limit, the 100 MB that Docker's `local` keeps by default, or that a
    driver which sends logs elsewhere may keep no copy to read back. A
    driver that keeps none at all (`none`, `passthrough`) stops the engine,
    where every task's log and output were silently empty. Podman's daemon
    does not say which driver it gives, so there the engine reads it off the
    first container it makes and says the same then. And no daemon says what
    options it gives a driver. Two of them cost a log: a rotation
    (`max-size`, `max-file`) keeps the end of a long one, and
    `mode=non-blocking` has the daemon drop what a container writes while
    its buffer is full, whichever driver keeps the rest. Both are read off
    the first container, and each is warned of once, with its figures.
  - **`DAGRON_DOCKER_LOG_DRIVER`** names the log driver for every container
    the executor makes: a task's, and the ones that install its tools and
    fetch its checkout. `json-file` keeps each one's whole log in a file
    the daemon reads back (Podman takes the name for its own file driver).
    Unset, nothing changes: each container gets the daemon's own.
  - **Why it is not the default.** Nothing bounds that file but the
    daemon's own options for the driver, until the container is removed when
    its task ends. On a host whose own driver is bounded, the bound may be
    what keeps a task that writes without end from filling the disk.
  - Not changed: on Kubernetes the log is still read once the pod has
    exited, from what the kubelet kept, which rotates at
    `containerLogMaxSize`; [docs/OPERATIONS.md](docs/OPERATIONS.md) says
    what to raise. Reading a task's output as it is written, which would
    end the dependence on either runtime's log, is not built.
  ([docs/CONFIG.md](docs/CONFIG.md))
- **Every run says what started it, and `not_triggerer` reads who caused
  it.** A run a CI service submitted carried a trigger record. Every other
  run's trigger was decided when it was read, from a backfill's ledger row
  or a schedule's stamp, and anything else read `manual`: a cron entry's
  fire, a dataset's update, a sub-workflow, a message from a queue, a call on
  the engine's own API.
  - Every run is now made with a record (`run_triggers`, in the run's own
    transaction, never updated), on SQLite as on Postgres:
    `{kind, principal, actor, trust, source, forge_host, repo, ref, sha,
    base_sha, pr, event_id}`. `kind` is `manual`, `api`, `schedule`,
    `backfill`, `dataset`, `workflow`, `source` or `ci`. `principal` is what
    submitted the run (`user:<caller>`, `ci:<service>`, `engine`, or
    `ops-api` for the engine's API, which has no login). `source`, which is
    new, is what the run came from where that is no repository
    (`schedule:<id>`, `cron:<name>`, `dataset:<uri>`,
    `run:<parent run>/<task>`, `source:<name>`, `dead-letter:<id>`,
    `resubmit:<run>`), and `event_id` is the fire's time, the backfill's
    date or the source's position.
  - `trigger_kind` and `?trigger=` read the record first. A run from before
    it is decided as it was. `GET /api/runs/{id}` returns the record as
    `trigger` for every run, where it was `null` for all but CI's, and the
    engine's own `GET /runs/{id}` returns it too. The console's Runs filter
    and badge know the new kinds.
  - **A sub-workflow's run keeps where its parent's work came from.** A run a
    `type: workflow` task starts is recorded as that task's, and takes its
    parent's `actor`, `trust`, repository, commit and `event_id`.
  - **`not_triggerer` compares an approver with the record's `actor`.** It
    read the run's submitter. For a run a CI service submits, that is the
    service's own token, so the rule never barred the person who asked for
    the run; and a sub-workflow's run had no submitter, so a gate in one
    barred nobody, whoever had started its parent. A record with no actor
    (a schedule's fire) bars nobody, as before, and a run from before the
    record is its submitter's, as before.
  - **What changes for a caller:** `trigger` is no longer `null` on a run a
    person submitted; a run that read `manual` only because nothing knew
    better reads `schedule` (a cron entry's fire), `dataset`, `workflow`,
    `source` or `api`; and a person who starts a workflow can no longer
    approve a `not_triggerer` gate in a workflow it calls. A backfill a
    person asks for through `POST /api/schedules/{id}/backfill` is recorded
    as theirs, so that rule now applies to them in its runs as well.
  - **For an upgrade:** the record's table and its new column are made by
    the engine's migrations (SQLite 057, Postgres 077), and a dagron-api of
    this release writes to them with every run it makes. So an engine of
    this release starts first, as [docs/UPGRADING.md](docs/UPGRADING.md) has
    it for every release: a dagron-api upgraded ahead of it refuses submits
    until one has.
  - Not done: no kind for an agent (a run an agent submits is its token's
    user's, `manual`); a forge login in `actor` is compared as text, with
    no mapping to a dagron-api user; and nothing reads the record when an
    ordinary task is dispatched.
  ([docs/API.md](docs/API.md))
- **A task's log is written once a second, not once a line, and an attempt
  keeps so much of it and no more.** Each line a task wrote was a statement
  of its own against the scheduling database: an append to its row's log,
  which rewrites what the log already holds, and on Postgres a notification
  to every subscriber. The engine made them one after another inside its
  reconcile loop, so a task that wrote a great deal held up scheduling for
  every other; and nothing bounded how much one could write, into a row on
  the scheduler's disk.
  - The engine now holds a task's lines and writes them together: one append
    for each task once a second at most, and one when the task ends, before
    its status changes, so a task that ended by itself has its whole log. A
    tail of a running task is about a second behind it, where it was a tick
    of the loop (`POLL_INTERVAL_MS`). What waits is lost with the engine, and
    to a task cancelled before it is written: up to that second of its log.
  - `DAGRON_LOG_TASK_CAP_BYTES` (default `67108864`, 64 MiB) is how much is
    kept of one attempt's log, and of its output, each. A log that reaches it
    keeps its beginning and ends on a line that says so; what the task writes
    after that is dropped before it is queued for the loop. An output over it
    keeps its end, under a first line that says how much of its beginning is
    gone. The cap counts what the task wrote: those two lines, and a line
    dagron adds itself, such as why the attempt failed, come on top of it,
    and a capped log is given them all the same. There was no cap before: a
    deployment with a task that writes more than 64 MiB in one attempt, and
    reads all of it, sets this higher.
  - Not changed: the log is still a value in the scheduling database, each
    append still rewrites it, and an executor still holds a running task's
    output in memory.
  ([docs/CONFIG.md](docs/CONFIG.md))
- **Every binary answers `--version`, with the release it was built for.**
  Only `dagron-ci` and `dagron-ci-exec` answered, with their crate's
  `0.1.0`, which is the same for every release. `dagron --version` took the
  flag for a workflow's path and started an engine, and `dagron-api` ignored
  it and served.
  - `--version` (or `-V`), alone, now prints one line from every binary, its
    name and the release, and nothing is read or started:
    `dagron 0.11.0`. That is `dagron`, `dagron-api`, `dagron-ci`,
    `dagron-ci-exec`, `dagron-gitops`, `dagron-mcp`, the three steps, and
    the `dagron-plan`, `dagron-state`, `dagron-import` and
    `dagron-autopsy` tools.
  - The release is what the build was told: `APP_VERSION`, which the image
    workflow now passes to every image and the binaries workflow sets for
    each build. A build no release made says `dev`, as the console does. It
    is not a crate's version, which no release changes.
  - `dagron_ci_build_info{version}` and what `dagron-mcp` tells a client it
    is carry the same.
  - A release binary is now checked to say its release before it is
    attached.
- **dagron-ci ships as an image, and the chart can deploy it.** Nothing
  packaged dagron-ci: it was a binary to build and a service to start by hand.
  - **`mancube/dagron-ci`** is built with the other images at a release, for
    `linux/amd64` and `linux/arm64`: the binary on distroless, as nonroot,
    with no shell and no `git` (it speaks git's protocol itself). Its
    entrypoint serves; `repo add`, `repo list` and the rest run from the
    same image.
  - **The Helm chart deploys it with `ci.enabled`**, off by default: a
    Deployment and a Service on 8090, with startup and readiness probes on
    `/readyz` and a liveness probe on `/healthz`. It keeps its queue in the
    release's database, in its own schema, or in one named by
    `ci.databaseSecret`.
  - Each secret is a Secret you name (`ci.apiTokenSecret`,
    `ci.webhookSecret`, `ci.githubApp.keySecret`, `ci.githubTokenSecret`,
    `ci.checkoutTokenSecret`), its key mounted as a file. dagron-ci reads no
    secret from its environment, and the chart keeps none in its values.
  - What dagron-ci refuses to start on fails the render instead: an App id
    without its key, cells without a token.
  - With `ci.enabled`, the chart's NetworkPolicy lets dagron-ci reach the
    bundled Postgres. With a webhook secret and the ingress, `/hooks/github`
    is routed to it, and no other path of dagron-ci's.
  - A default install renders what it rendered before.
  - Not in it: the engine's side. Pipelines whose jobs have a workspace need
    `DAGRON_CI_EXEC_IMAGE` and `DAGRON_CHECKOUT_IMAGE` on the engine
    (`engine.extraEnv`).
  - `crates/dagron-ci/tests/k3s.sh` builds the image, installs the chart in
    k3s and checks that the pod turns ready, that its commands run in it, and
    that a named Secret is the webhook's secret. CI runs it.
- **dagron-ci makes its own calls to github.com as the App.** dagron-ci's App
  (`CI_GITHUB_APP_ID`) minted each checkout's credential and nothing else:
  the refs, pull requests, pipelines and changed files dagron-ci itself reads,
  and the statuses it posts, all went out under one static token for every
  repository (`CI_GITHUB_TOKEN_FILE`).
  - Where the App is installed on a repository, those calls are now made
    with an installation token minted for **that repository alone**: contents
    and pull requests to read, commit statuses to write, and nothing else.
    GitHub refuses it for any other repository.
  - A token is used for fifty minutes and minted again. One GitHub answers
    401 to is dropped at once, and a status posted with one held a minute or
    more is tried again with another.
  - `CI_GITHUB_TOKEN_FILE` is the fallback: where the App is not installed
    on a repository (asked again every five minutes), where GitHub will not
    mint the token (said in the log, asked again in a minute), and where
    there is no App. So a dagron-ci with the App and no token reads a private
    repository's pipelines and posts its statuses.
  - Where GitHub does not answer the request for a token (it is not reached,
    it fails, or it is limiting requests), the fallback makes the call. With
    no fallback the call fails and is tried again, and is not made as nobody:
    GitHub answers an anonymous caller as if a private repository held
    nothing, which would be taken for a commit with no pipelines.
  - **The App needs two more permissions for this**: pull requests, read, and
    commit statuses, read and write, beside contents, read. An App with
    contents alone goes on minting checkouts' credentials, and dagron-ci's
    own calls fall back to the token.
  - The hint on a refused read names the App beside the token, and is of
    what the call carried.
  - Not changed: a repository on GitHub Enterprise or another forge is still
    read without credentials. These tokens are not revoked when dagron-ci
    stops; GitHub expires them within the hour.
  - Also here, from the review of the change before it: with two pipelines
    of an event that cannot run, a status that could not be posted for one
    no longer keeps the other from being said.
- **A run a CI service submitted says so, and a list can be narrowed to
  CI's.** A run's trigger was derived when it was read: a backfill, a
  schedule, and otherwise manual. So every run dagron-ci submitted read
  "manual", nothing on it said which repository or commit it was for, and no
  list of runs or workflows could be narrowed to CI's.
  - **The trigger record** (`run_triggers`, Postgres migration 072).
    `POST /api/runs` and `POST /api/workflows/{id}/run` take
    `trigger: {kind: "ci", actor, trust, forge_host, repo, ref, sha, base_sha,
    pr, event_id}`. It is written in the run's own transaction and never
    updated, and dagron-api sets its `principal` from the caller it
    authenticated. The engine's migrator makes the table and dagron-api's run
    list reads it: upgrade the engine with dagron-api, or before it.
  - **Who may write one:** a caller in the new `ci:trigger` group, and no
    other. Anyone else's submit that names a trigger is refused with `403`
    and makes no run, because what reads the record believes it. `422` for a
    kind other than `ci`, or a field that is too long or unprintable.
  - **What reads it.** `trigger_kind` is `ci` in the run list, a run's detail
    and a workflow's history; `GET /api/runs?trigger=ci` returns such runs and
    no other, and `?repo=<host/owner/name>` those for one repository; a run's
    detail carries the record as `trigger`. The record is no
    part of an `Idempotency-Key`'s request: two events for one commit replay
    one run, whose record is the first event's.
  - **dagron-ci sends it** with every run, for a compiled pipeline and for a
    bound workflow, where the cell's dagron-api takes one from its token. It
    asks first (`GET /api/me`, every five minutes) and sends none where the
    token's user is not in the group, so an install that has not been given
    the group goes on running, unmarked, with one warning in the log. To mark
    runs, create dagron-ci's user with `groups: ["ci:trigger"]`.
  - **Every compiled workflow is tagged `ci`,** beside its author's own tags
    (`dagron_ci::pipeline::compile::CI_TAG`). `dagron-ci compile` prints two
    more lines.
  - **`GET /api/workflows?include=ci`** lists, after the stored workflows, the
    pipelines CI compiled: a read-only row for each repository and pipeline
    name (`source: "ci"`), with the usual run digest. Two repositories may
    each have a pipeline of one name, so a row's runs are its name's and its
    repository's (`?name=&repo=`). The list without it is unchanged.
  - **Console.** Runs has a CI trigger filter and badge, and takes `?trigger=`
    and `?name=` from its address. A run's page shows the repository, the pull
    request or ref, and the commit it was for. Workflows has a CI filter, and
    lists a compiled pipeline as a row that leads to its runs, with nothing to
    edit, run, pause or delete: it is changed in its repository.
  - **Not in this change**, and done by a later one in this release ("Every
    run says what started it", above): a record for the other kinds, a record
    on SQLite, and `not_triggerer` reading who caused the run. Still
    derived: an agent's.
- **dagron-ci: a limit on runs in flight, for a build that sets one.**
  `dagron_ci::Seams` gains `admission` (`dagron_ci::Admission`), asked before
  each run a worker submits with the runs dagron-ci submitted that have not
  ended. The count and a slot for the run (`ci_admissions`, migration 0008)
  are taken in one transaction, under a table lock on Postgres and SQLite's
  one writer, so runs asking at once never pass the limit. Recording the run
  lets its slot go; the reporter seeing it end frees its place. Over the limit
  the event waits like one behind a full concurrency key: no attempt spent,
  asked again every 30 s, never failed for it. The open build sets no limit
  and is unchanged.
- **dagron-ci: a checkout secret per cell.** `CI_CHECKOUT_TOKEN_SECRETS_DIR`
  holds one file per cell, named as the cell, the way a Kubernetes Secret with
  a key per cell mounts. An engine presenting a cell's secret is answered for
  the runs dagron-ci submitted to that cell alone; a run on another cell is
  refused with 403. Where cells are tenants, one tenant's engine can no longer
  mint a checkout credential for another's run. `CI_CHECKOUT_TOKEN_SECRET_FILE`
  still answers for every cell. Start-up refuses a file for a cell `CI_CELLS`
  does not name, one secret shared by two cells, and a cell's secret equal to
  every cell's.
- **dagron-ci: a run meter, for a build that counts runs.**
  `dagron_ci::Seams` gains `run_meter` (`dagron_ci::RunMeter`), told each run
  once dagron-ci has recorded it, and each run's end once the reporter has:
  its status, or `gone`, and whether any of its jobs started (a job's
  `attempt` above 0). Recording an end marks it due in the same write
  (`ci_runs.meter_due_at`, migration 0009, which also keeps `started`), and
  the reporter tells each due end, again a minute later while the meter
  refuses it, so no end is lost to a meter that is down. The open build sets
  none and marks nothing due.
- **`dagron-ci`, the start of CI on dagron: intake.** A new sidecar crate
  (`crates/dagron-ci`, binary `dagron-ci`) receives forge events and queues
  them for pipelines that run as ordinary dagron runs. The engine learns
  nothing about CI. This entry is its intake; the entries below run what it
  queues and post how it went.
  - `POST /hooks/github` checks `X-Hub-Signature-256` over the raw body, in
    constant time, before it parses anything. It dedupes on
    `X-GitHub-Delivery` in the same transaction as the event, so a redelivery
    or a replayed capture never queues a second event. It queues pushes,
    tags, deletions, pull requests (opened, updated, reopened, closed) and
    merge-queue groups, and answers 202.
  - Each event carries a trust class decided from fields GitHub vouches for.
    A pull request is trusted only when its head is a branch of the
    repository itself and its author is an owner, member or collaborator. A
    fork's, or anyone else's, is untrusted.
  - Each event also carries a commit-level key, so a webhook and a later poll
    of the same commit will make one run.
  - It keeps its own `dagron_ci` schema on SQLite or Postgres. On Postgres it
    uses a schema of its own, migration history included, so it can share
    dagron-api's database.
  - `/healthz`, `/readyz` and `/metrics`: deliveries by outcome, queue depth,
    age of the oldest pending event.
  - `dagron-ci repo add|list|rm` connects the repositories it serves.

  Tests run on both databases: the receiver's contract (a replay writes
  nothing, a bad signature gets 401 and writes nothing, an unconnected
  repository gets 404) and each trust rule. Eight replicas starting together
  each create the schema and migrate. Without the guard for that race the test
  fails in most runs.
- **`docs/UPGRADING.md` — how to upgrade, and what each release asks first.**
  The README's upgrade note covered only 0.8.x, so moving to 0.10.0 had no
  guide at all, though it asks for a ServiceAccount allow-list, `secrets`
  permissions on a hand-written engine Role, `allowFrom` past the chart's new
  NetworkPolicy and the `admin` group for workflow deletion and repository
  management before the new engine starts. The guide has the general procedure
  (Compose, Helm, binaries), a before-you-start table for 0.9.x → 0.10.0, the
  migrations each starting version runs, the one mixed-version hazard
  (`defer:`), and how to roll back. `HARDENING.md` joins the mirror so its link
  resolves.
- **dagron-ci runs what it queues: event workers.** Each replica runs
  `CI_EVENT_WORKERS` workers (8) once `CI_CELLS` names the dagron-api to run
  on and `CI_API_TOKEN_FILE` holds a token for it.
  - A worker claims the oldest due event under a lease (`FOR UPDATE SKIP
    LOCKED` on Postgres, so replicas share the queue). A trusted event runs
    the workflow bound to its repository, stored in dagron-api, through
    `POST /api/workflows/{id}/run`, with the event as parameters (`ci_sha`,
    `ci_ref`, `ci_event`, `ci_pr`, `ci_group`, …).
  - What cannot run is skipped, with the reason: a pull request from a fork
    or a non-member (`CI_FORK_PRS=refuse`, the only value until the untrusted
    floor exists), a deleted ref, a closed pull request, a repository with no
    workflow bound. What dagron-api refuses fails, with its answer. An
    unreachable or busy dagron-api is retried with backoff (5 s doubling to
    10 min) until `CI_EVENT_MAX_ATTEMPTS` (8).
  - Nothing is lost and nothing runs twice. A worker that stops mid-event
    loses nothing: its lease lapses (`CI_EVENT_LEASE_SECS`, 60) and the next
    claim takes the event over. Each run is submitted under an
    `Idempotency-Key` derived from the event's commit key and the workflow,
    and recorded in `ci_runs`. A second event for the same commit finds the
    record and submits nothing. A worker that died between submitting and
    recording gets the same run back.
  - `dagron-ci repo add … --workflow ID [--cell NAME]` binds a repository;
    `dagron-ci repo set` changes it; `dagron-ci events [--state S]` lists
    what became of each event. `/metrics` adds the worker count and events
    finished by outcome.
  - The bound workflow is what runs for a commit without pipelines
    (`.dagron/ci`, below). With its `notify.git` (`sha: "{{ ci_sha }}"`), a
    push shows on its commit as `pending`, then its result.
  - Tested on both databases (the claim, the lease takeover, the fenced
    finish, retries), against a stub dagron-api (every answer it can give),
    and through the shipped binary: a signed push runs the workflow once, a
    replay and a second event for the commit run nothing more, and SIGTERM
    stops the server and its workers cleanly.
- **dagron-ci polls for what a webhook missed.** Each repository is swept
  every `CI_POLL_SECS` (300; `dagron-ci repo set … --poll-secs N` gives it
  its own interval, `0` never polls it), and what changed since its last
  sweep is queued, as a webhook would have queued it. No inbound path is
  needed, so the sweep also serves a forge that cannot reach dagron-ci.
  - Branches and tags come from git's own ref listing over HTTPS, the request
    `git ls-remote --heads --tags` makes. dagron-ci speaks the protocol itself
    (v2, or v0 from an older server), so it needs no git binary, a token never
    reaches a command line, and any forge that serves git over HTTPS works.
  - On GitHub, open pull requests come from the API, under a conditional
    request while they fit on one page: an unchanged list answers 304, which
    the rate limit does not count. Their trust is decided as for a webhook's.
    A spent rate limit holds the repository's next sweep until it resets.
  - A moved branch queues a `push`, a moved tag a `tag`, a ref gone a
    `delete`, and a pull request opened or pushed to a `pull_request`. The
    first sweep of a repository records what it finds and queues nothing, so
    connecting one does not run every branch it has.
  - A polled event carries the commit key a webhook's would, so a commit both
    doors see runs once. Tested end to end on both databases, webhook first
    and poll first, against a stub forge and a stub dagron-api.
  - Replicas share the sweeps (`FOR UPDATE SKIP LOCKED` on Postgres). A sweep
    commits its events and what it saw in one transaction, fenced on its
    claim. `dagron-ci repo list` shows when each repository was last polled
    and why a sweep failed; `/metrics` adds sweeps by outcome and the events
    they queued.
  - `CI_GITHUB_TOKEN_FILE` reads private github.com repositories and is sent
    to no other host. Repositories elsewhere are polled without credentials
    until per-repository credentials exist.
- **dagron-ci's run API: `POST /ci/v1/runs`.** A developer, a script or a
  release job asks for a run instead of pushing for one:
  `{"repo": "github.com/acme/widgets", "ref": "refs/heads/main", "inputs": {…}}`
  queues a `dispatch` of the repository's bound workflow, which runs with
  `ci_event=dispatch` and the inputs as parameters. `sha` pins the commit;
  without it, the ref is read from the forge and its commit pinned.
  `GET /ci/v1/runs/{id}` reads any event back, with the engine run it
  submitted.
  - dagron-ci keeps no users. The caller's dagron-api session or API token
    goes to the dagron-api of the repository's cell, the one its runs go to,
    which says who the caller is (`GET /api/me`). A viewer may read and run
    nothing; anyone else may run. The credential is never stored, and a
    caller dagron-api has not identified learns nothing about a repository.
  - An `Idempotency-Key`, scoped to the caller, makes a repeat answer with the
    first request's run, even after the branch moved; the same key with
    another request is a 409. Without a key, every request is a new run.
  - A caller whose subject is `agent:<name>` is recorded with the `agent`
    trust class, and its runs are refused until that class has its floor.
  - Tested on both databases (who may call, every refusal, the key's
    replays and conflicts, ref resolution, the workers running a dispatch
    with its inputs), and through the shipped binary.
- **dagron-ci-exec, the step runner (`crates/dagron-ci-exec`).** A static
  binary (musl; x86_64 and arm64; under 1 MB, nothing needed at run time but
  `sh`) that runs a compiled task's steps. dagron-ci hands steps over as
  environment variables, step protocol 1 (`CI_STEP_PROTOCOL`,
  `CI_STEP_COUNT`, `CI_STEP_<n>_{RUN,NAME,TIMEOUT_SECS,CONTINUE_ON_ERROR,ENV_*}`),
  and the compiled command hands them to `dagron-ci-exec` where the image has
  one that answers `--speaks 1` (at `/dagron/bin/dagron-ci-exec`, or on
  `PATH`), and runs them itself, the POSIX `sh` script it carried, where it
  does not. A conformance test runs compiled tasks both ways and holds them to
  the same stdout, stderr and exit code.
  - Each step runs as `sh -ec` in its own process group, with stdin closed.
    A timeout sends SIGTERM, then SIGKILL after `CI_STEP_GRACE_SECS` (10),
    with `timeout(1)`'s codes (124; 137 when killed), in images without
    `timeout(1)` as well.
  - A cancelled task's SIGTERM, SIGINT, SIGHUP or SIGQUIT reaches the
    running step; no further step starts; the task exits 128 plus the signal.
  - On Linux it is a child subreaper: what a step leaves running lives on
    through the later steps and is killed when the task ends, so a leftover
    no longer holds the task's output open. A step's shell dies with a runner
    killed outright (parent-death signal).
  - Each step's outcome, as GitHub reports one (`outcome` and `conclusion`),
    is logged as the last line, `::dagron-ci::result::{…}`, and written to
    `CI_RESULT_FILE` when set. Its own failures exit 125.
  - `dagron-ci-exec install DIR` copies it into a volume, what an init
    container will run from an image that holds nothing else.
  - Release assets carry it for both Linux musl targets, smoke-tested on
    native runners; CI checks the build stays static.
  - The validator refuses a task of more than 1000 steps, or whose steps
    carry more than 512 KiB of scripts and variables: they travel in the
    task's environment, which the kernel caps for an exec and Kubernetes
    for a pod, so such a task would fail to start, late and obscurely.
- **Step markers go to stderr.** Before, the step script printed them to
  stdout, so a task with `steps:` could not feed `with_output_of` or a
  `when:` that reads its output: the markers were in it. Now a task's output
  is its steps' stdout, on the local executor. (The Docker and Kubernetes
  executors still store stderr in a task's output.)
- **A task's workspace: `workspace:`.** A directory a task starts in, empty
  at the start of each attempt and removed after it, for the files a task's
  later commands read: a build's `target/`, a download it unpacks. On
  Kubernetes an `emptyDir` at `/workspace`, with `size` (`20Gi`) as its
  `sizeLimit`, or, from `DAGRON_WORKSPACE_PVC_FROM` up, a generic ephemeral
  volume of that size (`DAGRON_WORKSPACE_STORAGE_CLASS`). On Docker an
  anonymous volume at `/workspace`, removed with its container on every path,
  the predecessor reap and the fleet sweep included. Locally a new temporary
  directory, removed after the attempt even where a Go module cache made it
  read-only. Where the operator chose where tasks work (`DAGRON_DOCKER_WORKDIR`,
  a mount at `/workspace`, the new `DAGRON_LOCAL_WORKDIR`), a workspace task
  works there instead. Command tasks only; a size is checked at validation.
- **`dagron-ci-exec` in every workspace task, from an init container.** With
  `DAGRON_CI_EXEC_IMAGE` set, the Kubernetes and Docker executors start each
  task that has a workspace with an init container from that image, which
  runs `dagron-ci-exec install /dagron/bin` into a volume of its own; the task
  mounts it read-only at `/dagron/bin`, where a compiled dagron-ci task looks
  first, so the step runner is in every image whatever the image is. On
  Kubernetes the init container is held to the task's hardening and asks for
  its cpu and memory, which costs the pod nothing; one that fails fails the
  task in its own words. On Docker the image is pulled at startup, and one
  that cannot be had stops the engine. The new `dagron-ci-exec` image (the
  static binary on scratch, uid 65532) is published with the others.
  - dagron-ci gives every task with `steps:` a workspace unless it sets its
    own (`{ size: … }`, or `null` for none); a pipeline's `workspace:` is now
    dagron's key rather than a reserved one, and `workspace.cache` is refused
    until workspace caches exist. `dagron-ci run` keeps every task in the
    working tree, as before.
  - Tested on a real Docker daemon (the volume, the read-only tools, a failed
    install, nothing left behind), with the real static binary from a scratch
    image under a compiled pipeline, and against a real apiserver, which
    admits each pod shape in a dry run.
- **dagron-ci posts commit statuses itself.** One per job, under
  `dagron-ci/<pipeline>/<job> (<event>)`, so a later run of a pipeline on the
  same kind of event updates the same checks and a push and a pull request on
  one commit keep their own (a merge group reports under its pull requests'
  names, which is what a merge queue waits for): `pending` while a job waits,
  awaits approval or runs, then `success`, `failure`, or `error` for a
  cancelled job (GitHub's statuses have no cancelled) and for one the run left
  unfinished. A skipped job stays pending until the run ends, then passes only
  if the run did, since dagron skips a job whose upstream failed as it skips
  one whose `when:` is false. Each replica's reporter claims a recorded run as
  a worker claims an event, reads its jobs from dagron-api's graph (no
  outputs), and posts only what changed; a refusal is recorded once and not
  retried, GitHub's rate limit is waited out, and other failures are retried
  in 30 s. `CI_RUN_URL` links each status to the run's page;
  `dagron_ci_commit_statuses_total` counts them. Statuses go to github.com
  repositories with `CI_GITHUB_TOKEN_FILE`'s token, which needs
  `statuses: write`. Tested on both databases against stubs of dagron-api and
  GitHub, and through the shipped binary.
- **A workspace can start with a commit: `workspace.checkout`.**
  `checkout: { repository, commit }` on a task's `workspace:` fetches that one
  commit by its id (depth 1, no tags) into the workspace before the task runs,
  and checks it out detached, so a branch that moved on changes nothing. Both
  may name the run's parameters (`{{ ci_sha }}`) and are checked once those
  are filled in: an `https://` repository (`http://` only for one fetched
  without a credential), with no user, query or `..` in it, and a full commit
  id. On Kubernetes an init container from the new `DAGRON_CHECKOUT_IMAGE`
  (any image with `git` and `sh`) fetches it, after the one that installs
  `dagron-ci-exec`; on Docker a container from that image fetches it into the
  task's workspace volume before the task starts; locally, the engine's own
  `git`. A fetch that fails fails the attempt with git's words, and a Docker
  or Kubernetes engine without `DAGRON_CHECKOUT_IMAGE` refuses such a task
  rather than run it without its code. Git in the task works on the checkout
  as it is: the task's environment marks the workspace a safe directory.
  - **A credential for each attempt.** With `DAGRON_CHECKOUT_TOKEN_URL` and
    `DAGRON_CHECKOUT_TOKEN_SECRET_FILE`, the engine asks that service for a
    credential before each attempt with a checkout, and tells it to revoke
    the credential once the attempt is over. The credential reaches git as an
    HTTP header in git's environment, never the task's environment, the
    repository's config or a URL. On Kubernetes it goes in the task's Secret,
    which only the checkout's init container reads. It is redacted from the
    task's log, and sent over `https://` only (or to the engine's own
    machine). The fetch that carries it follows no redirect, since git sends
    the header on to wherever a redirect moved the fetch: a repository that
    answers with one fails its checkout instead.
  - Tested against a git smart-HTTP server that asks for basic auth: locally
    (the credential reaches the server, and neither the task nor
    `.git/config`), on a real Docker daemon (a task image that runs as
    non-root included), and on k3s (below).
- **dagron-ci: each job starts with its commit, and a token for that attempt
  alone.** The compiler gives every workspace a checkout of the event's
  repository at its commit (`https://{{ ci_forge_host }}/{{ ci_repo }}.git` at
  `{{ ci_sha }}`) unless the pipeline's `workspace:` says `checkout: false`.
  `dagron-ci run` gives none, since its tasks work in the working tree.
  `POST /ci/v1/checkout-tokens` is the token service an engine asks. It
  answers an engine holding `CI_CHECKOUT_TOKEN_SECRET_FILE`'s secret, and only
  for a run dagron-ci submitted that is not over, that run's own repository
  and its own commit, so the secret cannot be spent on anything else. A run
  is over once the reporter, which follows every run to its end whether or
  not its commit takes statuses, has seen it end; a run dagron-api no longer
  has is over too (`gone`). It mints an installation token of the GitHub App
  named by `CI_GITHUB_APP_ID` and `CI_GITHUB_APP_KEY_FILE`, for that one
  repository with `contents: read`, and `DELETE` revokes it. Without an App,
  or for a repository the App does not cover, it answers 204 and the commit is
  fetched without a credential, which is all a public repository needs.
  `dagron_ci_checkout_tokens_total{outcome}` counts the requests. Tested
  through the router against a stub GitHub that checks the App's RS256 token
  and the scope it asks for (each refusal, the mint, the revoke, GitHub
  failing), and the run lookup on both databases.
- **Workspace tasks run on a real Kubernetes in CI.** The new `kubernetes`
  job runs `crates/dagron-executor/tests/k3s.sh`: k3s in Docker, the
  `dagron-ci-exec` image built from its Dockerfile and loaded into it, and
  `tests/kube_workspace.rs` against it. Steps share a workspace with the
  binary installed beside them; an install that fails fails its task in the
  installer's words; a workspace past its size is evicted and its task says
  so; a large workspace is a volume claim of its own; and a workspace starts
  with its checkout, fetched by an init container from a git server on the
  runner, or fails in git's words. Where the kernel refuses a negative
  `oom_score_adj` (some sandboxed hosts), the script restricts containerd's,
  without which no pod starts there.
- **`dagron-ci run`: a checkout's pipelines, on this machine.** The run a
  push would start, here and now. The event is read from the checkout: a push
  of the branch HEAD is on, at HEAD, for `origin`'s repository, with
  `compile`'s flags to change any of it. Each pipeline compiles as the event
  workers compile it, and each one that runs goes to a `dagron` engine started
  for it alone: a one-shot run on a SQLite file of its own. dagron-ci drives
  the engine binary (`--engine`, else beside `dagron-ci`, else on `PATH`)
  rather than linking it, and reads the database it writes.
  - On Docker, the default, each task runs in its image, pulled first if it
    is not here, with the working tree mounted read-write at the path it has
    on the host, as its working directory, and a `dagron-ci-exec` found
    beside `dagron-ci` mounted at `/dagron/bin`. With `--executor local`,
    each task is a process in the working tree, with dagron-ci-exec first on
    its `PATH`.
  - A paths filter reads the files the working tree changed since it parted
    from its upstream (or `--base-ref`, or `--base-sha`), committed or not,
    untracked files included. A branch with nothing to compare with lets the
    pipeline run, and says so.
  - The engine runs without its own settings: every name `dagron config`
    lists, and every `DAGRON_` variable, is removed from its environment, so
    a shell's `API_ADDR`, `RUNNER_CLASSES`, `DAGRON_CONFIG`, `GITHUB_TOKEN` or
    `DAGRON_INSTALLATION` can neither keep a run from ending nor reach past
    it.
  - Each task's log is printed as it comes, split by step. It exits 0 when
    every pipeline that ran succeeded, 3 when none runs, 1 when one failed or
    the engine refused it (with the engine's reason), and keeps a failed
    run's workflow, database and engine log. ctrl-c stops the engine and
    every task it runs, removes the run's containers, and exits 130.
  - Tested through the binaries on a real engine: a run on each executor,
    a failure, a skip, a paths filter against an upstream, a workflow the
    engine refuses, the settings kept out, and ctrl-c leaving no step,
    engine or container behind.
- **The Docker executor gives every task the operator's mounts.**
  `DAGRON_DOCKER_MOUNTS` (`SOURCE:TARGET[:ro|rw]`, comma separated: an
  absolute host path, bound as it is, or a volume's name) and
  `DAGRON_DOCKER_WORKDIR` (where each task container starts). They are the
  engine's settings, never a workflow's, since a workflow that could name a
  host path could read the host, and a bad one is a startup error, since
  every task gets them. `dagron-ci run` sets both to put the working tree into
  every task.
- **A dispatch may name the one pipeline it runs.** `POST /ci/v1/runs` takes
  `pipeline`, a file in `.dagron/ci` (`deploy` or `deploy.yaml`); without it,
  every pipeline whose `on:` takes `dispatch` runs. A name the commit does
  not hold, or a pipeline that does not take `dispatch`, fails the event, and
  the bound workflow never runs in its place. The name is part of the
  request an `Idempotency-Key` repeats, and `GET /ci/v1/runs/{id}` shows it.
- **dagron-ci runs a repository's own pipelines.** For each trusted event, a
  worker reads `.dagron/ci/*.yaml` at the event's commit, decides each
  pipeline's `on:`, and submits each that runs as its own dagron run
  (`POST /api/runs`), compiled. A commit without pipelines runs the workflow
  bound to the repository, as before.
  - Pipelines are read through GitHub's contents API (github.com,
    `*.ghe.com`, Enterprise Server). A repository on another forge runs its
    bound workflow until dagron-ci reads that forge's files.
  - A paths filter is decided with the files the event changed, asked of
    GitHub's compare API once per event and only when a pipeline needs them.
    A pull request is compared with its base; a push with where its branch
    was (a webhook's `before`, or what the last poll saw), so pushes now
    carry `ci_base_sha`. When the files cannot be listed (a new branch, a
    change past GitHub's 300 files), the pipeline runs, and the event says
    why.
  - Events carry the branch a pull request or merge group merges into, so
    `on.pull_request.branches` and `on.merge_group.branches` decide.
  - One run per pipeline per commit: each run's `Idempotency-Key` derives
    from the commit key and the pipeline's path, so a webhook and a poll of
    one commit still run each pipeline once.
  - An event whose run's concurrency key is full waits (dagron-api's 429)
    and asks again every 30 s without spending an attempt, as it does for a
    forge's spent rate limit; `dagron_ci_events_finished_total` counts these
    `waited`. A pipeline that cannot run fails the event once every other has
    run; one dagron-api cannot take now retries it first.
  - A compiled `concurrency_key` is prefixed with the repository, and a
    `max_active_runs` set alone gets the repository as its key: dagron counts
    a cap per workflow name and key, so two repositories' pipelines called
    `ci` on one cell would otherwise wait on each other. `concurrency:` beside
    a `max_active_runs` under 1 is refused, as dagron would refuse it.
  - `GET /ci/v1/runs/{id}` lists every engine run an event submitted
    (`engine_runs`, each with its pipeline) in place of `engine_run`.
  - `CI_GITHUB_API_URL` reaches github.com's API through a proxy or a cache.
  - Tested on both databases, and through the shipped binary against a stub
    GitHub: a pushed commit's pipeline reaches dagron-api compiled.
- **dagron-ci compiles pipelines: `dagron-ci compile`.** A pipeline and an
  event in; out, the dagron workflow a run submits and its parameters, or why
  the pipeline does not run. A pure function: the same inputs give the same
  bytes, and a golden file pins them.
  - `on:` decides by event kind, then branch, tag or base branch, then the
    files the event changed. A paths filter never guesses: without the
    changed files, `compile` asks for them.
  - Event data reaches tasks as `CI_*` variables, which dagron fills in from
    the run's parameters; the workflow itself holds no event text and is the
    same for every event that runs it. Tested through dagron's own expander:
    a branch named `x;rm -rf /$(curl evil)` ends up in `CI_REF` and in no
    command.
  - A task's `steps:` become one POSIX `sh` script, the compiler's alone,
    running each step in its own `sh -e`, its script, name and `env:` passed
    as variables. A failing step ends the task unless `continue_on_error`;
    `timeout_secs` is kept by `timeout(1)` where the image has one; step
    markers split the log. The script is tested under a real `sh`.
  - Every task gets a timeout (`task_defaults.timeout_secs`, else the new
    `CI_DEFAULT_TIMEOUT_SECS`, 3600). `concurrency.key` becomes
    `concurrency_key` with `max_active_runs: 1`. A dispatch's inputs are taken
    only when declared, required ones must be sent, defaults fill the rest.
  - `dagron-ci compile FILE --event … --ref …` prints the workflow, or with
    `--json` the body `POST /api/runs` takes; it exits 3 when the pipeline
    does not run. The validator now also refuses `cancel_superseded: true`
    (the engine cannot cancel the older run yet), `steps:` beside a template
    or an approval, `CI_` names in dagron's own `env:` lists, `ci_` pipeline
    parameters, and `tasks.X.output` outside `when:`, where dagron reads it.
- **dagron-ci reads pipelines: `.dagron/ci/*.yaml` and `dagron-ci validate`.**
  A pipeline is a dagron workflow with `on:` (which events run it: `push`,
  branches and tags, `pull_request`, `merge_group`, `dispatch`, each with
  branch, tag and path globs) and `steps:` (commands run in order inside a
  task), and `concurrency:` for dagron's `concurrency_key`.
  - `dagron-ci validate [FILE|DIR]…` (`.dagron/ci` by default) needs no
    database: it prints `ok`, or every problem with where it is, and exits 1.
    `dagron-ci schema` prints the format's JSON Schema (draft 2020-12) for
    editors.
  - dagron's own keys pass through as written. A key that is neither
    dagron's nor the pipeline's is refused, with the nearest known key, since
    dagron would drop it without a word; a test reads dagron-core's own
    structs so the list cannot fall behind.
  - A command names nothing an outsider chose: a `{{ … }}` in a `run:`, a
    `command:` or a `when:` may name only values dagron-ci or the pipeline
    decides (`ci_sha`, `ci_event`, the pipeline's parameters, a matrix's
    `item`, …). A branch name is refused there, and read as `$CI_REF`
    instead.
- **`POST /api/workflows/{id}/run` takes an `Idempotency-Key`**, as
  `POST /api/runs` does: a repeat with the same workflow and parameters
  answers `200` with the first run, and the same key with other parameters is
  a `409`. dagron-ci's workers run workflows through it.
- **`notify.git` posts `pending` when a run starts.** A run created with a
  `notify.git` block is stamped (`workflow_runs.start_pending_at`), and the
  next tick of an engine holding a forge token posts `pending`, so the check
  shows on the commit from the moment the run exists. That covers runs
  submitted through dagron-api, which holds no forge token. A run that ends
  before any tick announces it gets its terminal status alone. If the run ends
  while the `pending` post is in flight, the engine posts the end again, so
  the check always finishes on the run's result. The `pending` check reads
  "dagron run in progress", whatever `description:` says, since a run that has
  just started has produced nothing.
- **Runs keep the parameters they were created with** (`workflow_runs.parameters`):
  the spec's defaults with the caller's overrides merged over them, exactly
  what the tasks were expanded with. `notify.git`, `notify.webhook` and
  `notify.slack` resolve their templates against them when the run ends. The
  README promised that `sha: "{{ commit_sha }}"` picks up the SHA a CI caller
  submits. It did not: the end resolved against the stored YAML, whose
  `parameters:` block holds only defaults, so the SHA came out empty and the
  status was skipped. Runs created before this change resolve against their
  defaults, as before.

### Changed
- **On Postgres a task's end wakes its run, whoever ends it.** The skips of an advance after a failure, a fan-out's
  barrier, the time, HTTP and dataset sensors, an external job's end and a
  sub-workflow's trigger sent no notification, so such a run was ended by
  the reap's slice, up to `REAP_INTERVAL_MS` (5 s) for each 500 running
  runs late, and counted in `scheduler_runs_reaped_by_sweep_total`. A
  trigger on `task_runs` (migration `migrations_pg/107`) now sends the run
  on `task_events` when a task ends, is made ended, or is taken away not
  ended, in the transaction of the change: through the binary, such runs
  ended 11 to 16 ms after their last task, where they ended 4.2 to 5.3 s
  after it, and the slice's count stayed 0. Postgres folds a transaction's
  equal notifications into one, so a cancel of 10,000 tasks is one
  notification, 18 ms dearer (1.8 µs a task); an end whose function also
  notifies after its commit (a `repeat:` sub-workflow failed) is heard
  twice, in one tick.
- **SQLite's count of active runs, the run cap and supersede read the
  running runs alone.** The `MAX_INFLIGHT_RUNS`
  gate of each `POST /runs`, the count of a capped workflow's runs at each
  run it makes, and supersede's read of the runs a new one takes the place
  of each scanned every run kept on OSS: 70 to 73 ms at a million runs.
  Migration 080 makes a partial index of the running runs by their
  concurrency key: 0.13 to 0.98 ms at every size, and on an enterprise
  build the cap and supersede of a key read only its runs. The drift mark and the running
  runs listed read it too. It moves none of the plans 078 and 079 made.
- **dagron-api's live stream on SQLite reads what changed, not every run.** Every `DAGRON_SQLITE_POLL_MS` (500 ms) it read
  each run not finished or finished in the last 30 s with its tasks, on its
  one connection: 85 ms at a million runs kept. Now the file's triggers
  (migration 081) write each run a change touched to `run_changes`, with a
  sequence, and the stream reads past a cursor of its own: 0.5 to 0.8 ms
  at every size. It sends what it sent but in three cases: a change undone
  between two polls, and one to a run finished more than 30 s ago, are
  each sent now; a finished task taken away from a live run is no longer
  a change on its own (another change follows it in practice). It writes
  nothing, so two dagron-api processes on one file each see every change;
  the file keeps the last 65,536 changes, and a stream at its start,
  further behind, or past the file's newest change (the file put back from
  an older copy), reads every live run once, as before. The triggers
  cost a cancel of 10,000 tasks 8 to 17 ms. **Upgrade note:** 080 builds
  an index at start-up, 71 ms at a million runs; 081 scans nothing.
- **SQLite's run sweeps, its reap and its live counts read indexes of
  their own.** On an enterprise build's SQLite file the deadline,
  alert, stopped-run, admission, start and completion sweeps read its
  `(status, created_at)` index, so each pass read every running run, and
  the completion sweep, every tick, every finished one: 235 ms a tick at a
  million runs kept. They now read the partial indexes made for them, led
  by `status` (migration 078); the completion sweep 0.4 ms. Each tick of
  a SQLite engine joined every running run to its tasks, so on OSS its
  cost grew with every run ever kept: 165 ms at a million runs, on the one
  connection the API and the workers wait for. Now triggers on the file
  (migration 079) record each run a change may have ended, whoever made
  the change, dagron-api included, in `run_wakes`; a tick looks at those
  runs alone, 0.3 ms when there are none, and a slice of the running runs
  each `REAP_INTERVAL_MS` is the net under them, as on Postgres.
  `REAP_INTERVAL_MS=0` walks every slice each tick, which is dearer than
  the join was on a small file and on an enterprise build. The live run count, the recent
  unfinished runs and dagron-api's `/api/health` read a partial index of
  the live runs: 0.14 to 0.72 ms at a million runs, where they scanned
  every run (74 to 75 ms). `scheduler_runs_reaped_by_sweep_total` now
  counts on SQLite too: a run that ended with no wake written, the case
  to alert on, and now and then one whose wake came between a take and
  the slice. **Upgrade note:** 078 and 079 rebuild six indexes and
  build two, at start-up, about one scan of `workflow_runs` each (0.55 s
  together at a million runs); on a large file, start the engine before
  dagron-api. Run no `ANALYZE` on the file
  ([docs/ONE_NODE.md](docs/ONE_NODE.md)).
- **dagron-ci reads a repository's forge files in compat mode alone.** A
  repository has a mode now (below): a GitHub workflow file or a
  `.gitlab-ci.yml` its `.dagron/ci.yaml` names is run where the repository is
  in `compat` mode, as well as where `CI_GITHUB_WORKFLOWS` or
  `CI_GITLAB_PIPELINES` is on. Every repository connected before this is in
  `compat`, and runs as it did; **one connected after it is `native` unless
  it is connected with `--mode compat`** (`"mode": "compat"`).
- **A merge-queue group is its ref and its head.** A group let go of has the
  runs and waiting events of that ref at that commit cancelled, and no
  others: a GitLab train's ref names each pipeline the train makes of its
  merge request.
- **The maintenance sweeps run once an installation a window, each at an
  interval of its own.** Every engine ran every
  sweep: lease recovery, run deadlines and soft deadlines, approval
  timeouts, the runs that wait for their cap, the time, HTTP and dataset
  sensors, sub-workflows, fan-outs, dataset triggers and the slow reap of
  running runs. Each ran once each sweep interval (`SWEEP_INTERVAL_MS`, by
  default the poll interval), the last two every 5 s; with N engines each
  sweep ran N times an interval, and a due row could be read by more than
  one of them. Now each sweep runs once its own interval has passed
  (`SWEEP_INTERVALS`; every default is the cadence it had), and on Postgres
  the installation's sweeps are run by one engine a window: a tick asks, in
  one statement on the database's clock, for the slots (`sweep_slots`) of
  those due but the reap, which asks for its own in a second statement at
  the tick's end; it runs those it took, and the rest wait a whole interval.
  A slot is taken for nine tenths of the interval, a pass that read a full
  batch hands it back, and the first tick after a start runs every sweep. A
  claim also takes a slot whose `due_at` is later than the claim's own
  window would end (the database's clock as the row is locked, plus nine
  tenths of the interval), which no engine of the same interval can have
  set: so a step back of the database's clock, which leaves every slot due
  that much later, does not stop the sweeps until the clock catches up. The
  sweeps of an engine's own state (its leftover workloads, its stops, its
  teardowns, its quota summary and gang check) stay on every engine, and so
  do those that claim each row before a remote call: the HTTP sensors, and
  the polls and cancels of `defer:` jobs. So the HTTP sensors' read is not
  shared, and a due one may be read by more than one engine (about 1.4 times
  with three, as before); only its GET is made once (below). Schedules and
  cron stay on the leadership lease. With three engines the average wait
  before a shared sweep picks up a due row grows from about a quarter of the
  interval to about half. The worst case was not measured, and by the code
  it is longer: a slot is held for nine tenths of an interval, and an engine
  that finds it taken waits a whole interval, so a holder that stalls after
  its claim delays that sweep by up to nearly two intervals, where before
  the other engines' passes covered it. **`SWEEP_SHARE=off` restores every
  engine running every sweep.** The slow reap of running runs is walked once
  an installation, from where the engine before left off. The reasons for
  slots over run-id sharding and the leader's lease are in
  [docs/HA.md](docs/HA.md) §2.
- **A `wait.url` sensor is polled once per due time, where each engine
  could poll it.**
  A due HTTP sensor is claimed before its GET, as a deferred job's poll is,
  for `WAIT_POLL_SECS` or 11 s, whichever is more (longer than the GET's
  timeout); an engine that dies mid-GET leaves the sensor due again when the
  claim lapses, up to 15 s later by default. Each engine still reads the
  sensors that are due and claims each one it read, and the claim is a
  write: with three engines on one machine, the sensors' read, claim and
  repark cost Postgres 12.1 ms a second, where their read and repark had
  cost 10.8, while their GETs fell by a quarter.
  The sweeps' reads take only what is due: a fan-out barrier whose
  instances still run, a trigger whose child has not ended, and a sensor
  not yet due are left out of what a sweep takes, though the database
  still looks at the parked barriers, triggers and dataset sensors to
  leave them out. The time and dataset sensors, fan-outs and sub-workflows
  are read 256 at a time.
- **The terminal-status gauges on `/metrics` can be 5 minutes old, and
  more.** The engine's datastore gauges are cached in two tiers: what has
  not finished (runs `pending` and `running`, those of the last 24 hours
  by workflow and environment included, tasks still to end, the queue, the
  dead letters, the waits) for `METRICS_CACHE_SECS` as before, and the
  history, `scheduler_runs` and `scheduler_tasks` of the finished statuses
  and the runs of the last 24 hours that have finished, for
  `METRICS_HISTORY_CACHE_SECS` (300 s): they grow with every run, and
  reading them was most of what a scrape cost. A scrape that finds the
  history stale is served the last one at once, and the history is read
  again behind it, so no scrape waits for it once a read of it has
  succeeded: the first after a start does, and each after a first read
  that failed or was given up. A scrape that times out while it waits
  leaves the read to go on and be cached for the next. So the history can
  be as old as its TTL, plus the time until the scrape that finds it
  stale, plus the read, and older while reads of it fail: the last is
  served until one succeeds
  (`scheduler_metrics_history_age_seconds` says how old). A sum over every
  status moves at each refresh.
  **`METRICS_HISTORY_CACHE_SECS=0` reads the history with every scrape, as
  before, and so does `METRICS_CACHE_SECS=0` where the history's is not
  set.** A read that does not return is given up, after 30 s for what has
  not finished and after the history's TTL or 60 s, whichever is longer,
  for the history: a scrape that waited for it answers `500`, as for a
  read that failed. The OTLP export now reads the API's cache instead of a
  second one of its own, and skips an export whose read has not ended in
  its interval or 30 s, whichever is longer. dagron-api's
  `GET /api/metrics`, which ran its counts over the whole run tables on
  every call of every open Metrics page, is cached the same way
  (`DAGRON_METRICS_CACHE_SECS`, 2 s; `DAGRON_METRICS_HISTORY_CACHE_SECS`,
  300 s, or 0 where it is not set and the first is 0), reads the three
  counts it answers and nothing more, and its answer gains
  `history_age_secs`.
- **Fleet mode refuses a fork's job unless its label says otherwise.** A
  label's runner was started for any job that asked for it. A job of a
  fork's pull request is now given none, unless the label says
  `untrusted: floor` or `untrusted: serve`, and telling a fork's job apart
  needs the App's **Actions: Read**: with an App without it, every job of
  such a label is taken for a fork's. A label for a repository whose forks
  are all its own people's says `untrusted: serve` to keep things as they
  were.
- **The engine's OIDC request URL is `https://`.** A task sends its request
  credential to `DAGRON_OIDC_REQUEST_URL` from its own container, and with it
  a cloud's tokens are had, so the engine no longer starts with a plain
  `http://` one, nor one that carries a user. An operator whose network may
  carry it in the clear sets `DAGRON_OIDC_REQUEST_INSECURE=allow`, and the
  engine warns at start. **An engine set up with an `http://` request URL
  needs one of the two.**
- **The engine's `/metrics` DB gauges are cached for 10 seconds**
  (SCALE_ROADMAP 0.3). `status_counts` ran its aggregate queries on every
  scrape; the handler now serves the snapshot from a short TTL cache
  (`db_gauges`), so a fleet of Prometheus pollers costs one DB round-trip
  per 10 seconds instead of one per scrape. Process counters and pool
  stats stay live and exact; only the DB gauges may lag by up to the TTL, and each
  engine caches on its own, so two engines can differ by that much. `METRICS_CACHE_SECS`
  sets the TTL (default `10`); **`0` reads the datastore on every scrape**, as before,
  and a failed read is never answered from the cache (`/metrics` still returns `500`).
  The e2e rig and the load-test harness run with `0`, because they scrape right after
  they change state. The gauge descriptions in `openapi.yaml`, `docs/METRICS.md`, and
  the Grafana "Scrape duration" panel no longer say the gauges are read on every scrape.
- **dagron-ci says when a fleet's replica takes no delivery.** With
  `CI_GITHUB_FLEET_FILE` set and no `CI_GITHUB_WEBHOOK_SECRET_FILE` it
  starts as before, and now warns: a job is heard of by its delivery
  alone, so that replica serves only the jobs another replica takes, and
  none where no replica has the secret.
- **On Kubernetes a task is told that its pod has changed, where it
  asked.** The engine keeps one watch of its task pods, and a task waits
  on it for its own. Each task asked the API server for its pod twice a
  second for as long as it ran. A task still asks until the watch has
  first seen its pod, and whenever the watch is not following the
  namespace, which it says once in the log. `DAGRON_TASK_POD_WATCH=off`
  is the executor as it was. The watch needs `list` and `watch` on pods,
  which the chart's Role has.
- **The engine looks for ended runs among the runs it was told of.** On
  Postgres a notification names the run a task changed state in. The
  reconcile loop keeps those names, and each tick ends the named runs
  whose tasks are all over, by their keys, where every tick of every
  replica joined every running run to all its tasks. The rest of what is
  running is covered by a slow sweep, 500 runs a pass in turn
  (`REAP_INTERVAL_MS`, 5 s; `0` is the loop as it was), and
  `scheduler_runs_reaped_by_sweep_total` counts what that ends. A replica
  that has just started, or whose listener lost its connection, looks at
  every run once. SQLite is unchanged: nothing notifies there.
- **A disconnected repository keeps its events and runs.**
  `dagron-ci repo rm` deleted the repository's row, and its events and
  runs with it. It now marks the repository disconnected: they stay to be
  read, the events that waited end `skipped` with the reason, and no
  checkout credential is minted for the repository. A run already going is
  not stopped. Connected again, the repository is the one it was, set to
  what that connect names, and its next sweep runs nothing.
- **The engine starts on no checkout token service over plain http, unless
  it is this machine.** `DAGRON_CHECKOUT_TOKEN_URL` as `http://` to another
  host was taken with a warning. The engine's secret goes there and every
  credential comes back from there, so the engine now stops at startup and
  says why. `https://`, or `http://` to `localhost` or a loopback address,
  is taken as before. **An engine configured so no longer starts:** put TLS
  before the service, as before a cell's dagron-api. And the engine no
  longer follows a redirect from the service: a revoke carries the
  credential in its body, and would have taken it wherever one led.
- **What is sent to an address of this machine goes by no proxy.** Plain
  http is taken for an address of the machine itself alone: a cell
  (`CI_CELLS`), GitHub's API behind a sidecar (`CI_GITHUB_API_URL`), the
  engine's token service (`DAGRON_CHECKOUT_TOKEN_URL`), a server named to
  the commands. A proxy named in the environment (`HTTP_PROXY`,
  `ALL_PROXY`) was still used for such a request, and would have been
  handed it, with the token or the secret in it, to carry off the machine
  in the clear. Each client now uses no proxy for a plain-http address. An
  https request keeps the environment's proxy: it is TLS to its end.
- **dagron-ci starts on no every-cell checkout secret where it has two
  cells.** `CI_CHECKOUT_TOKEN_SECRET_FILE`'s secret answers for a run on any
  cell. With one cell that is the cell's own. With two, an engine of either
  could ask for the credential of the other's run, so dagron-ci now stops
  at startup and says why. Each cell takes its own secret, a file in
  `CI_CHECKOUT_TOKEN_SECRETS_DIR`. The chart refuses the same at render
  (`ci.checkoutTokenSecret` with more than one cell in `ci.cells`); it
  mounts the one secret, and not yet a directory of them.
- **A task's log append no longer notifies, and a tick's appends are made
  side by side.** On Postgres, each append to a running task's log was a
  transaction that also notified, so that a browser's tail of the task
  updates promptly. A transaction that has notified takes one lock for the
  whole database as it commits, which Postgres keeps its notification queue
  in order by, and holds it until its commit is flushed. So the appends of
  every engine were committed one at a time, and whatever else notifies, a
  task's end or a run's making, waited in the same line. With enough tasks
  writing at once the line was seconds long, and the reconcile loop of every
  engine stood in it.
  - An append is one statement, and notifies nobody. The runs a tick's
    appends were made to are told of afterwards, each once, in one
    statement: a tail is told as soon as it was, and the lock is taken once
    a tick where it was taken once an append.
  - The appends of one tick are made up to four at a time, and with no more
    than half of the pool's connections (`DB_MAX_CONNECTIONS`).
  - In `dagron-core`, `db::append_task_outputs` makes a batch of appends
    and tells of their runs, and is what the engine calls.
    `db::append_task_output` returns the run it appended to and no longer
    notifies, and `db::notify_runs` tells of several. SQLite has no
    notifications, and makes its appends one at a time as before.
  - `docs/ARCHITECTURE.md` (the event-driven wake) says what a notification
    costs at commit, for whoever adds one.
- **A burst of notifications is one pass of the reconcile loop.** On
  Postgres every engine is notified of each change that can make work
  ready and of each append to a task's log, and an engine ran one pass of
  its loop for each notification it was sent. A pass looks at everything,
  whichever notification woke it, so a pass for each of a burst found
  nothing the first had not. And when notifications arrived faster than the
  loop ran, as with many jobs writing logs at once, an engine went on
  running passes through what was left of them after the work was over.
  - The loop takes every notification that is waiting, and runs once. What
    it runs in a second is bounded by how long a pass takes, and it is idle
    when the work is.
  - `scheduler_notifications_total` counts the notifications taken. Over
    `scheduler_reconcile_tick_seconds_count` it is how many a pass answered
    (`docs/METRICS.md`).
  - Nothing else about a wake changes. There is one channel, every engine
    is woken by every change, and a pass costs what it did. SQLite has no
    notifications, and its loop is as it was.
- **A task run by a published engine image has a `TRACEPARENT` in its
  environment.** The images are built with `otel` now (under Added), and an
  engine built with it hands each task a W3C trace context, whether or not
  a collector is named, and logs a line as it does. A source build with
  default features does neither.
  - **A task that names its own keeps it.** A workflow that gives a task a
    `TRACEPARENT`, to carry its caller's trace into it, had the engine's
    added after it in an `otel` build, and the engine's is the one the
    task saw. The engine adds none to such a task now.
- **A pod refused by a full namespace ResourceQuota waits instead of failing.**
  On the Kubernetes executor, a pod the apiserver refused with `exceeded quota:
  tenant-quota, requested: limits.cpu=550m, used: limits.cpu=3750m, limited:
  limits.cpu=4` was a failed attempt, so a task refused on every attempt failed
  its run though its command had never started. That refusal is now
  back-pressure: the task goes back to `ready` with a future `scheduled_at`, its
  attempt is given back, and it is dispatched again about 5 s later, then at
  doubling intervals up to about a minute, until a pod is admitted. Nothing is
  counted as a retry, a failure or a fault while it waits, and no attempt log is
  written. The refusal is read from the typed `403` of the pod create (code,
  reason, and the `pods "<name>" is forbidden: exceeded quota:` sentence for the
  pod that call named), never from task output. A request above the quota's hard
  limit can never fit, so it still fails the attempt, as do LimitRange, RBAC,
  PodSecurity and admission-webhook denials.
  `DAGRON_QUOTA_WAIT_MAX_SECS` (default `21600`, 6 h) bounds one wait. Its clock
  starts at the first refusal and is stored on the task row, so for a task at
  rest in the queue it survives a restart or a hand-over to another replica. A
  task whose probe was in flight when its engine stopped starts a new wait when
  its lease is recovered, and, as with any lease recovery, that dispatch's
  attempt is not given back. Past the bound the task fails for good, whatever
  its `max_attempts`, with the last refusal in its output. Four metrics, each
  emitted at zero from the first scrape: `scheduler_quota_refusals_total`,
  `scheduler_quota_wait_expired_total`, `scheduler_tasks_waiting_on_quota` and
  `scheduler_quota_wait_oldest_age_seconds`. A waiting task's row carries
  `quota_wait_since`, `quota_wait_detail` and `quota_waits`, and the console
  shows it as **Waiting on quota**. Migrations 051 (SQLite) and 066 (Postgres)
  add four columns to `task_runs`. **Behaviour change:** for Kubernetes users,
  attempt counts and time-to-failure change. A quota refusal no longer uses an
  attempt, and a task that failed once `max_attempts` dispatches had been
  refused now waits up to the bound before it fails. Two paths do not wait:
  `gang:` members under `RUNNER_GANGS`, where a refusal still fails the member
  and cancels its siblings, and `DAGRON_QUOTA_WAIT_MAX_SECS=0`, which makes a
  refusal a failed attempt again. On both, the failure text is now `not started:
  pod refused by namespace ResourceQuota 'tenant-quota' (requested: …, used: …,
  limited: …)` instead of `create pod '…': … exceeded quota: …`, so anything
  matching the old text needs updating. A dispatch refused by a full quota
  records no sample in the task-duration histogram, whether or not the task
  waits. During a rolling
  upgrade, an engine not yet upgraded still fails a refusal the old way. The
  sentence is what a Kubernetes 1.36 apiserver sends, for one resource over and
  for several; the tests carry both as it sent them. A request that fits the
  hard limit but not what other long-lived pods in the namespace leave free
  (the engine's own pod, where it runs under the same quota) still reads as a
  full quota, and waits until the bound. A quota refusal that does not qualify
  fails the
  attempt and logs the WARN `quota refusal not treated as back-pressure`: one
  whose request can never fit, one whose lists do not parse, and any other `403`
  that says `exceeded quota`, such as one that does not start with the sentence
  above, which is where an apiserver that words it differently would show. See
  [`docs/OPERATIONS.md`](docs/OPERATIONS.md).

### Fixed
- **dagron-ci acknowledges a GitLab pipeline hook of no merge request,
  however little it carries.** The merge-train reading of `Pipeline Hook`
  deliveries read the whole payload before it looked for a merge request,
  so a delivery without `object_attributes` or `project` was refused with
  400, where a pipeline of no merge request is dropped with 202. Whether it
  names a merge request is now read first, and alone.
- **A workflow that drops its `on_datasets:` has its dataset triggers
  cleared by any engine.** An engine cleared a workflow's subscriptions only
  where it had synced them itself since it started, so one that dropped
  `on_datasets:` while every engine restarted kept firing on its old
  datasets. Every registered workflow that declares none now has its rows
  cleared, in one statement, by whichever engine sweeps the triggers. A
  spec an engine cannot read (in a rolling upgrade, one a newer engine
  reads) is neither synced nor cleared by it, and a fire of it that it
  claims is rolled back for an engine that can read it. The first such
  fire of each workflow an engine rolls back is logged as a warning, with
  why the spec cannot be read; where no engine can read it (a spec that is
  broken, not newer), its fires are rolled back on every pass, and wait,
  until it is fixed. A pass that could not list the workflows claims no
  fire, as it cannot tell which specs it reads.
- **On a direct connection, an engine or dagron-ci that waits to migrate no
  longer deadlocks another engine's concurrent index build.** An engine that
  found another migrating waited for the migration lock inside a statement,
  which holds a snapshot, and `CREATE INDEX CONCURRENTLY` waits out older
  snapshots. So the build of the engine that held the lock waited for the
  engine that waited for the lock, and Postgres failed one of them as a
  deadlock: the waiter, which exited, when the build came to wait for it
  within a second (`deadlock_timeout`) of the waiter's own wait; else the
  build, as over a populated table, whose index was left INVALID, and the
  engine that ran it exited. The next engine to take the lock found the
  index there, took it for built (`IF NOT EXISTS`), and recorded the
  migration as applied. It could hit any migration marked
  `-- no-transaction` (the concurrent index builds) whenever an engine
  started while another migrated: two that a Helm install with
  `engine.replicas` above 1 starts at once, the next one an upgrade's
  rollout starts while the one before still migrates, or one that restarts.
  dagron-ci met it too: where it shares the engine's database, as it does by
  default (the chart's `ci.placement: engine` starts it in each engine's
  pod, and the compose stack points it at `workflow`), its migrator takes
  the same lock, whose key is the database's whatever the schema. An engine
  and dagron-ci now wait for the migration lock without holding a snapshot:
  each polls for the same advisory lock that earlier releases take. The lock
  is a session's, so through a pooler in transaction mode (a cell's
  PgBouncer, which a workspace engine's `DATABASE_URL` names) it keeps no
  two migrators apart, as it never did, and two engines that come to the
  same index build through it still deadlock: there, run an upgrade that
  builds indexes with one engine, or give the engines the direct endpoint.
  An engine or a dagron-ci of an earlier release still waits the old way, so
  one started beside this release's index builds (an old container restarted
  during the upgrade, a rollback, or a `ci.image` pinned to an earlier tag)
  still meets the deadlock. An index left INVALID this way is found by the
  engine's check, as it starts and every 5 minutes
  (`scheduler_db_invalid_indexes`), which logs at error level the `REINDEX
  INDEX CONCURRENTLY` that rebuilds it; nothing runs that for you.
- **dagron-api's start no longer breaks an engine's concurrent index build
  of `workflow_runs`.** Each start added `workflow_runs`' `environment`
  column if it was missing, with an `ALTER TABLE` that takes the table's
  exclusive lock even when the column is there. Started while an engine
  built an index of `workflow_runs` concurrently (migrations 047 to 083
  build six, which an upgrade from a release before them runs), the ALTER
  queued behind the build and the build waited out the ALTER; over a
  populated table Postgres failed the build as the deadlock's victim, which
  left its index INVALID, and the next engine recorded the migration as
  applied. The ALTER now runs only where the column is missing, as it is
  only before the engine's migration 026. A dagron-api of an earlier
  release still runs it at every start, so one started beside such a build
  (restarted during the upgrade, or on a rollback) still meets the
  deadlock; an index left INVALID so is found, and its `REINDEX` logged, by
  the same check.
- **A job that uses an action runs on Docker before 29.** On the Docker
  executor, every task with `workspace.actions` (every dagron-ci job with a
  step that uses an action of another repository) failed as its container
  was made: its actions were in a volume made for the task's own container,
  read-only, which Docker refuses before 29 ("must not set ReadOnly mode
  when using anonymous volumes"). Their volume is now made, before the
  task's container, by a container that holds it: one made with none of the
  fetch's variables and never started. The fetch is a container of its own
  that mounts it, the only one with the actions' credentials, and is removed
  before the task's container is made. The task mounts the volume by name,
  read-only, as it does its tools: it still reads its actions and cannot
  change them. The holder goes with its volume after the rest of the
  attempt, however it ends, once that rest is seen gone; so does an earlier
  attempt's, at the next dispatch or a teardown. The fleet sweep takes a
  holder, or an init container, only once nothing else of its attempt is
  left.
- **A checkout's credential is in no container while its task runs.** On
  the Docker executor, the container that fetches a workspace's checkout is
  the only one with the repository's credential, in its settings, and it
  was removed with no word of whether the daemon did. One the daemon did
  not remove stayed beside the running task, where `docker inspect` read
  the credential. It is now seen removed before the task starts, and asked
  for again, for ten seconds at most, while a removal is under way. One
  that is not seen removed fails the attempt, as a fetch of the task's
  actions does; a fetch that failed still says so in git's words. That
  container is then left, the credential still in its settings, until the
  task's next dispatch, a stop's teardown or the fleet sweep removes it;
  the credential itself is revoked once the attempt is over
  (`DAGRON_CHECKOUT_TOKEN_URL`), in the background, and lives until it
  expires where that revoke fails.
- **A workspace no init container made is not left behind.** On the Docker
  executor without `DAGRON_CI_EXEC_IMAGE`, the workspace made for a task is
  a volume of the task's own container, which its checkout's and its
  steps' containers mount by name. The task's container did not wait for
  them: where one of them was not seen removed, the task's went anyway,
  and its volume, still in use, was skipped by Docker and never removed
  after. The task's container now goes after them. At the attempt's end
  and at a stop it goes only once they are seen gone, and is otherwise
  killed and left, volume and all: the task's next attempt removes it as
  it starts, a stop's teardown does, and so does the fleet sweep where it
  runs (`DAGRON_INSTALLATION`); a stop that leaves it says so. Until then
  its settings keep the task's variables, its secrets among them, as a
  container whose removal failed always kept its own. A task that has
  ended is not dispatched again, and its attempt's end says what it left
  in a warning alone: where the fleet sweep does not run, that container
  stays until it is removed by hand, after those the warning names. Each of those takes
  a task's container only after its checkout's and its steps', whichever
  version left them.
- **A stop while a task's steps are taken back leaves none of them.** On
  the Docker executor, a task's end forgot its steps' containers as it
  began to remove them, so a stop that landed then did not find those not
  yet removed: it removed the init container, or the task's own, from
  under them, and a step's container ran on, its workspace's volume left
  for good once it went. A task's steps are now kept until each is seen
  removed, and the stop takes them back as the task's end does. A step's
  container being made as its task ends is waited for, ten seconds at
  most, and removed with the rest, and none is made after; one still
  being made after that leaves every step's container killed, for the
  next dispatch or the fleet sweep to remove. Two attempts of one task on one engine no
  longer share what is lent to their steps: an attempt's end took back a
  later one's steps, and a later one's start hid an earlier one's. **A
  volume either of these left under an earlier version stays.** It is
  anonymous, used by no container, and carries none of dagron's labels.
  `docker volume prune` reclaims it where Docker labelled it anonymous, as
  Docker 28 does; `docker volume prune --all` reclaims it whatever version
  made it, together with every other volume no container uses, named ones
  too.
- **A run that runs again is held to what it runs now**. Each of these holds for the rerun, the rerun of every job,
  the retry and the clear, on SQLite and on Postgres, but where it says
  otherwise:
  - **Its deadlines count from then.** A run set running again after it
    ended kept the `run_timeout_secs` deadline it was made with, so a rerun
    of a run older than that was failed by the deadline sweep before any
    task of it ran ("run deadline exceeded"). Its deadline and its
    `deadline` alert are now counted again from its definition, and an
    alert that fired may fire again.
  - **A task put back is parked again from nothing.** What parked it last
    time (its child run, its wake time, its dataset and cursor) and an
    approval gate's decision were left on the row. Between the task's next
    claim and its next park, a sweep could resolve it from its last child
    run or its last deadline, and release what depended on it; and a gate
    asked again showed the last decider beside its Approve button. They go
    with the rest of its result. A decided gate's decision is kept as the
    gate's attempt (`…/attempts`), where it was dropped.
  - **`not_triggerer` bars whoever put the gate back.** The person who
    reran, retried or cleared a run through dagron-api could approve the
    gate that asked again, the only person involved. dagron-api records the
    caller on each task it puts back (`task_runs.reset_by`, migrations
    072/097), and refuses that person's approval (403), as it refuses the
    run's triggerer's; `GET /api/approvals` says so.
  - **A job run again replaces what it uploaded.** An artifact an earlier
    attempt of the same job uploaded, before a rerun, a clear or a retry
    queued this one, gives way to the new upload, where
    the upload was refused (409) and the job failed. A second upload of a name in one attempt, or
    one of another job's name, is refused as before. A build output's
    provenance is numbered by the job's attempts for its life, as
    `…/attempts` numbers them: a rerun's build no longer has the first's
    `invocationId`, and a first attempt reads 1, where it read 2.
  - **A round's first attempt leaves no old checkpoint to its retries.** A
    task's `latest` file in its checkpoint directory is removed when the
    first attempt of the round a rerun or a clear begins is dispatched, so a
    retry in that round is not handed the round before's as
    `DAGRON_RESUME_FROM`. A pointer reported through the API is kept as it
    was, and a retry of one task goes on from its attempts as before.
- **dagron-api's pool is at least 2 where its live-events listener shares
  it.** Without `DATABASE_LISTEN_URL`, the listener holds one of the pool's
  connections for as long as it listens, so `DAGRON_DB_MAX_CONNECTIONS=1`
  left none for requests, and every request waited out its acquire timeout.
  It is now read as 2 there, as the engine reads `DB_MAX_CONNECTIONS`; with
  `DATABASE_LISTEN_URL` set, 1 stands.
- **A run its parameters leave no task in ends.** A workflow whose every
  task a `when:` of its parameters leaves out (a dagron-ci pipeline whose
  jobs are all for another event, as `dagron-import gitlab` often writes)
  made a run of no task, which stayed `running`: the reaper ended a run
  once all its tasks had, and this one had none. A caller waiting on it
  waited for ever, and `dagron-ci run` until it was stopped. The reaper,
  SQLite's and Postgres's, now ends such a run as one that succeeded, with
  its `run.completed` event; `dagron-ci run` says that no job ran.
- **A job its `when:` leaves out says so on the commit.** A pipeline's job
  that a `when:` of the event and its parameters left out (`"{{ ci_event }}
  == tag"` on a push) had no task in the run, and so no status: a branch
  that required its check waited for ever, and a run that left out every
  job posted nothing at all. dagron-ci now reads the run's tasks once it is
  made, and posts a passing status for each job left out, "skipped: its
  when: leaves it out of this event", as it does for a GitHub workflow's job
  its `if:` leaves out. `dagron-ci run --report` lists such a job as
  skipped.
- **A passing "skipped" no longer covers a run's failure for a moment.**
  Two events of one commit can share a job's check (two pull requests with
  the same head, say), one leaving the job out and the other running it. An
  event saying the job skipped looked for the other's statuses, and then
  posted: a failure the run posted in between was stood over by a pass
  until the run, told to, posted again, seconds later or longer where its
  reporter was behind. A run now owns a check from its first post under it,
  and nothing is said skipped under a check a run owns; a skipped status
  goes up under a lease on the check (dagron-ci's migration 0031,
  `ci_contexts`), which a run waits out before its first post. The run's
  word is the later in every order. A lease lasts three minutes, longer than
  any post, and one not given back lapses. A row goes with the day it was
  last written in (owned or leased) where `CI_RECORD_DAYS` is set.
- **A log begun again is seen whole or not at all.** In
  a store that keeps the open part in the row (an object store), an
  attempt's log written again from its start (its first chunk, a seal) was
  in the row before its old segments' rows went, so a reader could read the
  old segments and then the whole new text, and a reset the row refused (the
  attempt over) still dropped what was kept of it. A reset is now one
  transaction with its segments' rows (`reset_task_log`), a refused one
  changes nothing, and each writing of a log is named apart in the store
  (`<task>/<fence>.<gen>/…`), so a reader of one never reads another's bytes
  under the names it counted. And a log is its attempt's (`log_fence`): a
  teardown's claim, which moves the row's version on, no longer hides the
  attempt's log from the views. The engine learns an open part's size in
  bytes, where it took characters. A seal that would write what was sent
  already writes nothing, and the engine's notes stay in the log. **The
  upgrade's migrations add `task_runs.log_gen` (and, on SQLite,
  `log_fence`).**
- **A remote runner's log segment is written once.** A runner sends a task's
  live log in numbered segments, and sends one again when it does not hear
  the answer. Where dagron-api had written it and only the answer was lost,
  the text was appended twice; and one held up on its way past the seal
  that ends a task with secrets (the whole log, redacted as one piece) was
  added after it. dagron-api now keeps the last segment it wrote with the
  task, and its fence (`task_runs.log_seq`, `log_fence`), and answers a
  segment numbered at or below it under that fence with `204` without
  writing it. An attempt's segment numbers now only rise, a seal's among
  them, and a seal says so (`reset: true` in the segment): it used to be a
  new segment 0. The runner, for its part, sends a segment again as it was:
  before, it could carry what the task wrote since, which dagron-api,
  reading the number as written, would now have dropped. Migration:
  `094_task_runs_log_seq` (Postgres; runners dial dagron-api alone, so
  SQLite has none).
- **The images' writable directories were 0755 whatever the build meant.**
  A `COPY --from` of a directory makes its destination 0755, so the
  engine's `/workflows` and `/artifacts`, dagron-api's `/artifacts` and the
  runner's state directory were their owner's alone. They are 0775 now
  (`COPY --chmod`), group 0's: what a uid other than 65532 in group 0, as
  OpenShift runs them, needs to seed and write there.
- **`dagron-ci run` pulls an image a job's items fill in as each item's.**
  A job over `with_items` whose `docker_image` names the item
  (`node:{{ item }}-slim`, `{{ item.image }}`) had that text pulled as it
  is written, braces and all, and the run stopped at "invalid reference
  format" before any job started. Each item's image is pulled now. One
  that names something else is pulled by the executor, once the run has
  said what.
- **What `dagron-import` writes, at the edges of what a file holds.**
  - A control character in what the report quotes (`runs-on:
    "self\ahosted"`) was written into its comment as it was, and
    `dagron-ci validate` refused the file. It is a space there now.
  - A brace of the file's just before an expression (`"{${{ matrix.os
    }}}"`) made the template the importer wrote of it no template, and
    nothing said so. It is parted from the expression and said, as two
    braces together are.
  - What is said of such braces read "the text holds `[[`" in the step a
    job starts with, which shows every two braces as `[[`. It names them
    in words now.
  - An apostrophe in a comment after `;` (`echo hi;# it's`), or in a
    here-document's body, was read as opening single quotes, and an
    expression after it was left to do as though it stood in them.
  - Two keys of a file that were one but for the two characters the
    importer marks its own templates with (`build`, `"build\u0001"`): the
    second was written over the first. Both are kept, the second as
    `build_`.
  - A path that is not UTF-8 stopped the program; it is said so, with the
    usage.
- **The pipeline reference said a tag's push reads `push`.** `CI_EVENT` is
  `tag` for one, as `ci_event` was documented to be. The workflow
  document's example of an expression made a variable named
  `github.head_ref`, which is not answered here: it names
  `github.ref_name`.
- **An engine built without `ops` did not compile**
  (`--no-default-features --features sqlite`). The run loop names the
  type of a run's trigger record, for a sub-workflow's run and a dataset's,
  and the import was the `ops` build's alone.
- **A dispatch that named its repository with capitals was told it is not
  connected.** `POST /ci/v1/runs` with `github.com/Acme/Widgets` answered
  404 for a connected `acme/widgets`: a repository is kept in lower case,
  every other door matched it so, and this lookup did not. GitHub shows a
  name with its capitals, and that is what people copy. It is matched
  without regard to case now, and so is the same from
  `dagron-ci run --remote`.
- **A job's `env:` written as a map was left out of the job without a
  word.** A task's `env:` is dagron's own, a list of names and values; a
  step's is a map. `dagron-ci validate` took a task's written as a map, and
  the compiler, which adds the steps to that list, dropped it: the job ran
  without the variables, and without a secret it named. It is refused now,
  for a task and for `task_defaults`, and so is an entry with no name. A
  `task_defaults` that is not a mapping went the same way, and is refused
  as well. The schema says each of these too.
- **The pipeline schema refused an event written with no filters.**
  `on: { push: }` is what `dagron-ci validate` takes and what the schema's
  own type allows, and an editor reading
  `crates/dagron-ci/schema/pipeline.v1.json` marked it wrong: each rule
  that a filter and its `_ignore` twin do not go together held of a bare
  key too. The same for `pull_request:` and `merge_group:`.
- **A Docker task's line is no longer cut in two by a line of its other
  stream.** The daemon hands a container's stdout and stderr over as frames,
  in the order it read them, and a frame need not end where a line does: on
  Docker only in a line longer than 16 KiB, on Podman wherever a read of the
  container's pipe happened to end. The Docker executor joined the frames as
  they came. So where a part-line of stdout was followed by a line of stderr,
  a task's log and its output held one wrong line and a second half with no
  beginning. A job that wrote `line 27515` on a Podman host read
  `line 275`, the lines dagron-ci's exec writes to stderr as a step ends,
  and then `15`.
  - Each stream's unfinished line now waits for its end, and is placed where
    it ended. A last line with no newline is kept as it was.
  - It waits as bytes, so a character that a frame cut in two is one
    character, where it had been U+FFFD marks.
  - More than 64 KiB of one line is not waited for, and is passed on as it
    is, up to its last whole character.
  - The Kubernetes executor reads one stream that the kubelet has already
    joined, and the local executor reads each stream a line at a time:
    neither changes.
- **A NUL byte in what a task wrote no longer stops the engine.** Postgres
  takes no NUL in a text value, and refuses the whole statement that holds
  one. A task that wrote one, and `printf '\0'` in a job is enough, failed
  the statement that recorded its result, and the engine exited on that
  error with the task still running in its row. The line of its log that
  held the byte was refused the same way. A NUL in a task's log, in its
  output, and in the reason an attempt failed with is now kept as U+FFFD,
  the mark a byte that is no text already has, on every executor and on both
  databases.
- **dagron-ci starts on no cell reached over plain http, and the chart makes
  none up.** dagron-ci sends its token for dagron-api to each cell
  (`CI_CELLS`) with every run it submits or follows, and the credential of
  each caller of its run API. It took a cell at any `http://` URL, and the
  Helm chart's `ci.enabled` made one up: `http://<release>-api:8080`, the
  release's own dagron-api Service, which serves no TLS.
  - **A cell is now `https://`, or `http://` to this machine** (a sidecar
    that carries it on, a test), as `CI_GITHUB_API_URL` already was.
    dagron-ci stops at start on any other, and names the cell.
  - **Breaking for a dagron-ci whose `CI_CELLS` names an `http://` URL on
    another host**, which 0.11.0 took: it no longer starts. Put TLS before
    that dagron-api (an ingress, a mesh's sidecar on localhost, a proxy) and
    name that.
  - **The chart derives no cell.** With `ci.apiTokenSecret`, `ci.cells` is
    required, and a cell that dagron-ci would refuse fails the render. Where
    the ingress has TLS, the ingress host is one to name. The chart puts no
    TLS before its own dagron-api.
  - Not changed by this: the engine's token service URL
    (`DAGRON_CHECKOUT_TOKEN_URL`), which was still taken over plain http to
    another host, with a warning. It is refused since ("Changed", below).
- **Logs carry colour only for a terminal.** `LOG_ANSI` was documented as
  automatic and was on: every line of the default format carried colour codes
  into files, pipes and container logs, and `NO_COLOR` was not read.
  - With `LOG_ANSI` unset, lines are now coloured only when stdout is a
    terminal and `NO_COLOR` is not set. `LOG_ANSI=1` still forces colour, and
    `LOG_ANSI=0` none.
  - **If you read coloured logs through `docker logs` or `kubectl logs` and
    want them so, set `LOG_ANSI=1`.** A parser that stripped the codes has
    nothing to strip.
  - `dagron-mcp`, whose log is its stderr, does the same there.
- **A task's stderr is not logged by the engine as a warning.** On Docker the
  engine wrote one warning for every line of a task's stderr, and on the local
  executor one for each task that wrote any, whether the task failed or not.
  A dagron-ci job's step markers are stderr lines, so a job that succeeded
  drew a warning for each.
  - The lines are where they were, in the task's own log. The engine's log
    notes them at `debug`.
  - On Docker that line was also written unmasked. It is masked now, as the
    local executor's was.
- **Two settings the engine reads no longer look like typos to it.**
  `DAGRON_ENV_SECRET_KEY`, which the chart sets on every engine that has the
  environment secret store, and `DAGRON_DEFER_ENDPOINT` were read and were
  not in the engine's registry: each drew "looks like a dagron knob but is
  not one" at every start, and `dagron config` showed neither.
  - Both are registered, the key among the values never printed. As with any
    setting added to the registry, the fleet fingerprint of every engine
    changes with this release.
  - A build that reads settings of its own beside the engine can tell the
    scan their names (the `env_names` seam).
- **A task stopped while its container is being made leaves nothing.** On
  Docker, a stop (a cancel, `run_timeout_secs`, a gang sibling's cancel)
  drops the attempt where it stands and removes what it made. A stop that
  landed while the daemon was still making a container looked for it, found
  none, and said the work was gone; the daemon then finished making it. What
  was left was a container made and never started, with its volume, until
  the task's next dispatch or the orphan sweep. A run cancelled as it starts
  is how to meet it: the window is tens of milliseconds wide.
  - The request that makes a container now runs to its end whatever becomes
    of the attempt, and the stop waits for it before it removes anything. So
    what was being made is there to be removed, and a run that reads
    cancelled has nothing of that attempt left.
  - It ran nothing: the container was never started. It did hold the task's
    workspace volume.
  - Not changed: the Kubernetes executor, whose stop has the same shape and
    may leave a pod that was being created the same way. No cluster was at
    hand to show it or a fix for it.
- **dagron-ci: no check waits on silence.** Two ways a commit could be left
  with nothing said, beside the refused pull request's:
  - **A pipeline that cannot run says so on the commit.** One that is invalid
    as written, that was asked for by name and does not take the event, or
    whose run dagron-api refuses, made no run and left no status: the event
    failed, where only someone reading dagron-ci's events would see it. The
    commit now gets one `error` status under the pipeline's own name,
    `dagron-ci/<pipeline> (<event>)`, reading "not run:" and the reason. It
    is posted once the event has ended for good, not at each turn of one
    that is retried or waits. An event that runs out of attempts says it on
    the last of them.
  - **A status GitHub refuses for itself alone stops only its context.**
    GitHub keeps a thousand statuses under one context on a commit, and then
    answers 422. dagron-ci read every 422 as a refusal of the commit, and
    posted nothing more for the run: its other jobs' statuses stopped too. A
    422 is now the commit's only where GitHub says it has no such commit.
    Any other stops that context for the run, the run's other statuses go
    on, and the run's record says which context and why, in GitHub's own
    words where it gave any.
  - A 422 whose body cannot be read is tried again, as a 403's is.
  - Not covered: a bound workflow that dagron-api refuses, on a repository
    without pipelines, still leaves no status.
- **A task's image is pulled on Docker, and a task can say when.** The Docker
  executor made a task's container from whatever the daemon held. The Docker
  API pulls nothing when a container is made, as `docker run` does, so a task
  that named an image the host lacked failed with the daemon's "No such
  image" unless someone had pulled it by hand. A machine that "worked" had
  the image cached.
  - The executor now fetches a task's image before its container is made:
    pulled when the host does not hold it, as the engine's own two images
    (`DAGRON_CI_EXEC_IMAGE`, `DAGRON_CHECKOUT_IMAGE`) always were.
  - **`image_pull_policy`**, a new task key (and `task_defaults` key):
    `if_not_present`, `always` or `never`. `always` pulls before every
    attempt, and an attempt whose pull fails does not run on what the host
    holds. `never` pulls nothing, and an attempt on a host without the image
    fails, saying so. Any other word is refused when the workflow is parsed.
    So is the key on a `template:` call, which hands its template nothing
    but arguments: set it on the template's tasks, or in `task_defaults`,
    which reaches them.
  - A pull that fails fails the attempt, with the daemon's words and the
    image's name, before anything is made for it.
  - A pull is not bounded by the task's `timeout_secs`, which is for its
    work. It is given up when the daemon says nothing of it for five minutes.
  - On Kubernetes the policy is the pod's `imagePullPolicy`. A task that
    states none leaves it out, as before, and the kubelet's default applies.
  - dagron-ci's pipelines take the key on a task, as they take dagron's
    others.
  - Not here: pull credentials. The executor pulls with none, so a private
    image is still one the host holds already.
  - Not run: Kubernetes, beyond the pod that is built; and `always` against a
    tag that has moved, which needs a registry the test controls.
- **A cancelled task's container or pod is stopped.** A cancel changed rows:
  the worker dropped its wait at its next lease renewal, and on Docker and
  Kubernetes the container or pod ran on to the end of its command, with its
  volumes. So did the work of a run past `run_timeout_secs` and of a gang
  sibling's cancel.
  - A worker whose claim is gone now tells the executor (`ExecContext::stop`)
    and waits for it. The Docker executor stops the task's container with the
    task's grace (`CI_STEP_GRACE_SECS`, `10` s), then removes it, the
    checkout's container and the init container with their volumes. The
    Kubernetes executor deletes the pod with that grace, deletes its env
    Secret, and waits to see the pod gone. The Secret's delete is best
    effort: it is tried once, and the pod owns the Secret so that it goes
    with the pod where the delete fails. Each acts on the names that attempt
    made, never on the task's labels, which a later attempt's workload carries
    too.
  - A teardown nobody saw through says so. An attempt's result reads "stopped
    and removed" (`ClaimLost`) only once the daemon has said each container is
    gone, or the apiserver the pod. A removal that is refused or not answered,
    a pod still there twenty seconds past its grace, and an executor that does
    not return in twelve minutes each read "what this attempt started may
    still be running", with the reason (`ClaimLost::unseen`).
  - **A rerun no longer runs beside what it replaced.** Workloads carry a
    `dagron.dev/fence` label, the task row's version once claimed, and a
    dispatch removes the same task's workloads with a lower one. The attempt
    it compared before is set back to 0 by a rerun, a console retry and a
    clear, so the container a cancelled attempt left running passed for the
    rerun's own. A workload with no fence label is judged by its attempt, as
    it was.
  - **A task's next attempt starts after its earlier one is gone.** That
    dispatch stops what a lower fence left, with the task's grace, and looks
    again. While any of it is still there, or the daemon or the apiserver will
    not say, it creates nothing and is handed back as back-pressure: the task
    returns to its queue with no attempt spent and is tried again in a few
    seconds (with `DAGRON_QUOTA_WAIT_MAX_SECS=0`, or for a gang member, the
    attempt fails instead). It was best effort before: a dispatch started
    beside a pod still in its grace, and beside a container whose removal had
    timed out. Such a wait is counted and shown with the quota waiters; its
    refusal text says "an earlier attempt of this task may still be running".
- **A cancelled run reads cancelled once its work has stopped.** A cancel
  wrote `cancelled` on the run and on every task of it in one statement. A
  task a worker held went on for a lease renewal and its grace while every
  record said it was over, the run's `run.completed` event and commit status
  were already out, and its concurrency key was free for the next run to start
  beside it. With `TASK_LEASE_HEARTBEAT=false` nothing ever told the work to
  stop. The same held for a run past its `run_timeout_secs` and for a gang
  sibling's cancel.
  - **A cancel asks.** A task that had not started, or was waiting (an
    approval, a sensor, a deferred job), is `cancelled` at once, as before. A
    task a worker holds is asked to stop and stays `running`, with
    `stop_requested_at` set, until that worker says its container, pod or
    process is gone. Its run stays `running`, with `stopping_as` saying what
    it will end as (`cancelled`, or `failed` for a deadline), and ends with
    the last such task: the event, the commit status and the run sink follow
    the work. There is no new status. A client that knows neither field sees
    a run that is running, and then one that is cancelled.
  - **The engine tells the work at its next sweep** (`SWEEP_INTERVAL_MS`, half
    a second by default), not at the worker's next lease renewal, and with or
    without a lease heartbeat. It also tells work whose claim another
    scheduler took.
  - **A concurrency key is held until the work is gone.** `max_active_runs`
    counts running runs, and a run whose work is stopping is one.
  - **Work nobody is watching is looked for again.** A task that is asked to
    stop has a whole lease from then to answer in. A stopping task whose
    worker died, or could not see its teardown through, is never put back to
    run. It is claimed again for a teardown alone (`Executor::teardown`), by
    an engine that does not still have its execution out:
    Docker and Kubernetes stop what the task's earlier claims left and see it
    gone, and nothing is run. A workload that cannot be seen gone keeps its
    task reading as still stopping, and its run holding its key, until it can.
  - **A task that was asked to stop takes no other result.** One that ends by
    itself first is cancelled where it would have succeeded, failed, been
    retried or handed back to its queue, and it is not parked.
  - `POST /api/runs/{id}/cancel` answers `{cancelled, stopping}`: the tasks
    called off, and how many of them are still to stop. A second cancel while
    a run is stopping changes nothing. The engine's own `POST
    /runs/{id}/cancel` answers `stopping` too. A rerun, and a retry of a task
    being stopped, are refused until the run has ended, as for any running
    run.
  - The console shows a stopping run as **stopping**, and a task being
    stopped says so in its panel.
  - Columns `task_runs.stop_requested_at`, `workflow_runs.stopping_as` and
    `workflow_runs.stop_requested_at` (migration 055; Postgres 073 to 075),
    all NULL on existing rows.
  - **Engines of two versions.** An engine from before this change does not
    look for a stop request: a task it holds runs on as it did before, and it
    ends no run. An engine of this version ends the run such an engine leaves
    (a sweep), so upgrade the engines with or before dagron-api.
  - Not run on a cluster: the Kubernetes teardown. It compiles, and is the
    reap a dispatch already did.
- **dagron-ci: a refused pull request says so on its commit.** A pull request
  from a fork or a non-member runs nothing (`CI_FORK_PRS=refuse`). Nothing
  told its author: the event was skipped, and the head commit carried no
  status, so a pull request sat waiting on checks that were never coming.
  - The worker that refuses it now gives the head commit one status:
    `error`, under `dagron-ci (pull_request)`, reading "not run: pull
    requests from forks and from non-members are refused here
    (CI_FORK_PRS=refuse)". Never one that passes.
  - It names no pipeline and no job: nothing of the pull request is read to
    post it. A check that branch protection requires of such a pull request
    still has no status of its own.
  - GitHub's rate limit is waited out and a failed post is tried again, as
    for any event. A post GitHub refuses (the token, the repository, the
    commit) leaves its reason on the event instead, and the event is still
    skipped. A repository that takes no statuses is not told, as before.
  - **A rate limit that names no wait is waited out.** GitHub can answer a
    secondary rate limit with a 403 that carries neither `retry-after` nor a
    spent `x-ratelimit-remaining`. A status post read that as a refusal of
    the token, which is not asked again: for a run, nothing more was posted.
    The body is now read first, and one that says it is a rate limit waits a
    minute, for a run's statuses as for a refused pull request's. A wait the
    headers name still comes first. A 403 whose body cannot be read is tried
    again, not taken for a refusal.
  - Each event of a refused pull request posts it, under the one context: a
    webhook and a sweep that see one head post it twice.
  - **A closed pull request is skipped for being closed, whoever opened
    it.** One from a fork or a non-member was skipped with the fork refusal's
    reason. It gets no status.
  - `dagron_ci_commit_statuses_total` counts these statuses beside a run's.
- **dagron-ci: an engine that names it as its token service can run other
  workflows.** Such an engine asks dagron-ci about every checkout. For a run
  dagron-ci had not submitted the answer was "not one dagron-ci submitted"
  (404), and the engine failed the task: naming dagron-ci made every checkout
  on that engine CI's, a hand-written workflow's over a public repository
  included.
  - For a run that is not on its record, `POST /ci/v1/checkout-tokens` now
    answers by whose repository the checkout names. One dagron-ci does not
    serve: `204`, no credential, and the commit is fetched without one. One it
    serves: refused as before, since the runs of a connected repository are
    dagron-ci's to submit.
  - Either is said once the run has been looked for (two seconds), as before,
    so such a task starts two seconds later than on an engine with no token
    service. Neither hands anything out.
  - The repository is told by its host and `owner/name`, without regard to
    case, a trailing `.git` or the scheme.
- **An `Idempotency-Key` and its run are one write.** A submit under a key
  claimed the key, created the run, and then pointed the claim at the run in a
  statement of its own, whose failure was logged and let go. A submitter that
  died between the last two, or whose last statement failed, left a run that
  its key did not name. Five minutes on the claim went stale, the caller's
  retry took the key over, and a second run was made: the one thing the key is
  there to prevent. A submit slower than those five minutes did the same with
  no failure at all.
  - The claim is resolved in the transaction that creates the run
    (`dagron_core::db::create_run_claimed`, on Postgres), for both routes
    that take a key: `POST /api/runs` and `POST /api/workflows/{id}/run`.
    The claim's row is locked first, so two submits of one key queue there.
  - A submit whose claim was taken over while it was in flight now creates
    nothing, and is answered from what holds the key: the other submit's run
    (`200`), or `409` while that one is still in flight.
  - A refused submit gives the key back, as before, and can no longer give
    away a claim that a later submit holds.
  - What a caller sees is otherwise unchanged: `201` for the first submit,
    `200` with the same run for a repeat, `409` for another body.
- **Rerun, retry and clear are one implementation, and keep what they
  replace.** Each is asked for through the engine's API and through
  dagron-api's, and dagron-api carried its own SQL for all three, "kept in
  sync" by a comment. The copies had drifted, and all of them wiped the
  attempt they replaced.
  - **One implementation.** Both APIs call the engine's (`dagron_core::db`).
    For dagron-api that changes what a rerun, a retry and a clear do: a rerun
    resets rule-skipped tasks with the failed and cancelled ones (it left them
    skipped, so a task skipped for an upstream failure never ran), and all
    three clear a reset task's log, its fault verdict and its deferred job's
    handle, which the copies did not. `rerun` in the rerun's answer counts
    the skipped tasks too. The engine's API gains
    `POST /runs/{id}/tasks/{task_id}/retry`, which only dagron-api had.
  - **What is replaced is kept.** A task that ran has that attempt's status,
    the times it was queued and ended, and the end of what it printed put in
    `task_attempts` before its row is reset, under `reason` `rerun`, `retry`
    or `clear`, by the policy a worker's retry is kept under
    (`DAGRON_ATTEMPT_LOG_BYTES`, `DAGRON_ATTEMPT_LOG_KEEP`).
    `GET …/tasks/{id}/attempts` returns them with `status` and
    `scheduled_at`, and the console's task panel shows them.
  - **Attempts are numbered through a task's life.** A rerun and a clear start
    the row's `attempt` again from 0, as before, so that the task has its
    `max_attempts` again. Kept attempts were keyed by that number, so a second
    round's attempt 1 overwrote the first round's. The rounds before are now
    counted beside it (`task_runs.attempt_base`), and `attempt` in the log
    and attempts routes is the sum. The task log route adds
    `earlier_attempts`. `attempt` on a run's task rows is unchanged.
  - **A run that had ended asks its concurrency key before it runs again.**
    With `max_active_runs`, a rerun, a retry or a clear that would set an
    ended run running is refused while the key is full (`429`, nothing
    written), as a new run of the key would be. It was admitted, so a
    serialized workflow could run twice at once. The engine's auto-rerun
    leaves such a run for a later sweep, and dagron-ci's rerun door
    (`POST /ci/v1/runs/{id}/rerun`) leaves it with "waiting" and why.
  - **A retry sets a cancelled run running again**, as it did a failed one. A
    task retried in a cancelled run used to run while its run read cancelled.
  - **Each writes an outbox event** in its own transaction: `run.rerun`, with
    `{run_id, by: rerun|retry|clear, reset_tasks, task_id?}`. The engine's
    auto-rerun keeps `run.auto_rerun` and its payload, now written with the
    rerun or not at all.
  - Migrations: SQLite 056, Postgres 076 (one column on `task_runs`, two on
    `task_attempts`; none rewrites its table). Upgrade the engine with
    dagron-api, or before it: dagron-api's routes read the new columns.
  - Not in this change: the tail of a replaced attempt is cut in characters,
    not bytes, so for output that is not ASCII it can be up to four times
    `DAGRON_ATTEMPT_LOG_BYTES`.
- **A resubmit carries the run's parameters.** `POST /api/runs/{id}/resubmit`
  (the console's "Re-run unchanged") made its new run from the stored
  definition and passed nothing over it. That definition holds only the
  spec's defaults, so a run submitted with parameters was resubmitted without
  them: one given `commit_sha` ran with it empty, and one whose parameters
  fill in a field the spec cannot run without was refused (`400`).
  - The new run is created with the parameters the run was created with
    (`workflow_runs.parameters`). A run submitted without any, or made before
    they were kept, resubmits with its spec's defaults, as before.
  - A declared `environment:` is read again, as for any new run. What the
    first run was given from it is not carried as if a caller had passed it:
    a variable changed since is the new value, and one removed since is gone.
  - Stored parameters that cannot be read refuse the resubmit (`500`): a run
    does not go out without them.
  - For a run dagron-ci submitted this changes what a resubmit is, not whose
    it is. It was refused, its checkout naming no repository. It is accepted
    now, and is a new run that dagron-ci did not submit: it has no checks, and
    where the engine names dagron-ci as its token service its checkout is
    given no credential. A check is run again by a rerun.
- **dagron-ci: a run that is run again is followed again.** dagron-ci
  followed a run to its first end and never looked again: its record's status
  moved once. A rerun, a retry or a clear through dagron-api moved the same
  run on, and nothing told dagron-ci. The rerun's checkout was refused its
  credential ("the run is over", 409), so its job never started; the commit
  kept the first attempt's statuses; and the run held no place under a build's
  limit. A failed or cancelled check could not be run again on its commit and
  have the commit show it.
  - **A record re-opens when dagron-ci sees the run running** in its cell's
    dagron-api (`GET /api/runs/{id}`, with its own token), and on nothing
    less: not for a run that reads over, one dagron-api no longer has, or a
    dagron-api that could not be asked, and only on a sight later than the end
    it has on record (`ci_runs.ended_at`, migration 0011). It is `submitted`
    again: followed, given its checkouts' credentials, and counted under a
    build's limit.
  - **It looks in three places.** A checkout credential asked for a run on
    record as over: dagron-ci asks the cell before it answers, and mints for a
    run the cell says is running, never on the request's word (a cell that
    cannot be asked is a 503, not a refusal). Every 30 s, each cell's running
    runs (`GET /api/runs?status=running`: all of them, so a run dagron-ci
    could not mark as CI's is seen too), for a run whose jobs check nothing
    out. And its own door, next.
  - **`POST /ci/v1/runs/{id}/rerun`** reruns an event's failed and cancelled
    runs, each from its failed jobs, for a dagron-api user who is not a
    viewer. The cell's dagron-api is asked with the caller's own credential,
    so who may rerun is its rule and its record names the caller, and
    dagron-ci's record re-opens with the answer. A rerun passes a build's
    limit like a new run. `200` lists the runs running again and the ones
    left, each with why; `409` when there is nothing to rerun. A rerun of
    every job (`failed=false`) is not built, and is refused.
  - **The checks read `pending` again, then how they end,** under the names
    they had. A job that did not succeed is posted again after each
    re-opening, though it reads as before (`ci_runs.reopens`): it is a new
    result. A job that had succeeded, which a rerun keeps, is left alone.
  - **The count does not move.** A run's next end is told to a build's meter
    again, which already had to take a repeat as a no-op.
  - `GET /ci/v1/runs/{id}` says how dagron-ci has each engine run
    (`engine_runs[].status`). New series:
    `dagron_ci_runs_reopened_total{by}`, `by` one of `rerun`, `credential`,
    `sweep`.
  - Not covered. A run moved through dagron-api whose jobs check nothing out,
    and that is over again inside one 30 s look, is not seen, and its checks
    keep the earlier result. That look reads up to 10,000 running runs a
    cell. A rerun that is over before dagron-ci's next look leaves no
    `pending` between its two results.
- **dagron-ci: a job that starts before its run is on record gets its
  checkout.** dagron-api creates a run, and its engine can start the run's
  first task, before dagron-ci has read the answer that names the run. The
  engine then asked `POST /ci/v1/checkout-tokens` about a run dagron-ci had
  submitted a moment before and had no record of yet. It was told at once
  that the run was "not one dagron-ci submitted" (404), and the job failed
  with nothing wrong with it. Seen twice in a day, at two different runs'
  first jobs.
  - A run that is not on record is now looked for again, for two seconds,
    before that answer is given. A run dagron-ci never submitted is refused
    as before, two seconds later.
  - The looks come out of the eight seconds an answer may take, not on top of
    them: GitHub is given what is left, so the engine is still answered inside
    its ten. Each read of the run is held to them too: a database that has not
    answered by then is a 503, where the read was not bounded before.
  - What a credential is given for has not changed: a run on record that is
    not over, its own repository and commit, and its cell.
  - Not covered: a run whose record could not be written at all, which stays
    refused until the event is tried again.
- **dagron-ci: the poll sweep reads git's own server.** Asked for protocol v2,
  `git http-backend` opens its answer to the ref advertisement with
  `version 2`, as git's protocol document has it. github.com names the service
  and flushes first, and dagron-ci read that form alone: against any server
  that runs stock git, every sweep failed ("not a git smart HTTP server"), and
  so did a dispatch that names no commit. Both forms are read now, and v0 as
  before. A test holds the bytes git 2.54's server sent. Which form other
  hosted forges send was not checked.
- **dagron-ci: an annotated tag runs at the commit it names.** A tag made with
  `git tag -a` is an object of its own, and its event carried that object's
  id as if it were a commit's: `CI_SHA` was the tag's id while the checkout's
  `HEAD` was the commit, and the commit statuses were posted on the tag's id.
  - The event keeps the tag's own id, which is its key: a webhook and a poll
    sweep that see one tag still make one run. The commit the tag names is
    kept beside it (`ci_events.commit_sha`, migration 0010), and it is what
    the pipelines are read at, what `ci_sha` and the checkout say, what a
    checkout's credential is checked against, and where the statuses go.
  - Each door says the commit from what it was shown. The poll sweep asks git
    to peel with its ref listing (`ls-refs` with `peel`; a v0 server lists a
    tag's commit as `refs/tags/NAME^{}`). The GitHub receiver reads a push's
    `head_commit`. A dispatch at a tag's ref with no `sha` resolves to the
    commit.
  - `dagron-ci events` and `GET /ci/v1/runs/{id}` show such an event under the
    id it came with. A lightweight tag has one id, and nothing changes for it.
  - An event queued before this change has no commit beside its id, and runs
    as it did.
- **A Docker task that runs as non-root can write its workspace.** A fresh
  volume takes its mount point from the image of the container that first
  mounts it, so the workspace was root's, mode 0755, and only a task running
  as root could write in it. Where the engine installs `dagron-ci-exec`, the
  init container now makes the volume from the `dagron-ci-exec` image's own
  `/workspace`, open to every user, and the task mounts it as it is, without
  its image's copy over it. The image carries that directory from this
  release; a checkout opens the workspace to every user too.
- **An evicted Kubernetes task says why.** A pod the kubelet evicts (a
  workspace past its `size:`, a node out of disk or memory) has no
  containers left to read a log from, so its task failed with the kubelet's
  "unable to retrieve container logs". It now fails with the pod's own
  reason: `the task's pod failed (Evicted): Usage of EmptyDir volume …
  exceeds the limit "16Mi"`.
- **A task that fails on Docker keeps what it said.** bollard reports a
  container's non-zero exit as an error, and the Docker executor took it for
  one: it removed the container before reading its logs, and the task's
  output was "Docker container wait error" and nothing else, which is what
  `dagron-ci run` showed for a failed step. A non-zero exit is now the task's
  result, with its logs, like any other.
- **The fleet sweep's settings are no longer typos.** `DAGRON_INSTALLATION`,
  `DAGRON_ORPHAN_SWEEP_SECS` and `DAGRON_ORPHAN_MIN_AGE_SECS` were read by the
  engine and documented, and missing from its list of settings. So an engine
  that armed the sweep warned at startup of a probable typo, and
  `dagron config` left out the one setting that lets a sweep remove
  containers.
- **A cancelled or timed-out run ends fully.** Cancelling a run, through the
  engine's API or dagron-api's, and failing it by `run_timeout_secs` used to
  change its status and nothing else. It wrote no `run.completed` outbox
  event, made no run-sink call, sent no webhook or Slack message and posted no
  commit status, so a commit marked "pending" stayed pending forever. Each
  path now writes the run's `run.completed` event (`cancelled`, or `failed`)
  in the transaction that ends it, and stamps the run
  (`workflow_runs.completion_pending_at`). The next tick on any engine replica
  claims the stamp, exactly once, and does what the reaper does for a run
  that finished: the run sink, OpenLineage, the `notify.git` status and the
  webhooks. A cancelled run is reported as cancelled: GitLab's `canceled`
  state, GitHub's `error` (its Statuses API has no cancelled state, and
  `failure` would claim a verdict the run never reached), and OpenLineage
  `ABORT` instead of `COMPLETE`. A run rerun before any tick claimed its end
  is not reported for that end; its next end is reported instead.
  - dagron-api's cancel now goes through the engine's own cancel
    (`dagron_core::db::cancel_run`) instead of its own SQL. It also calls off
    tasks waiting on an approval, which it used to skip, leaving the run
    running. The response is unchanged: `{"cancelled": <tasks>}`.
  - Migrations: SQLite 052–054, Postgres 067–071. The two Postgres indexes are
    partial and built `CONCURRENTLY` in migrations of their own.
  - Tested on both databases and through the shipped binary against a stub
    forge: a cancelled run posts `pending`, then one `error`; a run past its
    deadline posts `pending`, then one `failure`; each writes one
    `run.completed` event.

## [0.10.0] - 2026-10-03

### Added
- **OpenLineage events name the run's datasets.** The terminal `RunEvent` carried
  only the run and the job, so a lineage backend saw runs and no data. It now
  lists `inputs` (the workflow's `on_datasets:` and the run's `wait: { dataset: … }`
  sensors) and `outputs` (the `produces:` entries the ledger recorded for the run),
  each split the way OpenLineage names a dataset: `scheme://authority` as its
  namespace, the path as its name. Edges attach to the workflow, which is what
  `job.name` is. Reading a run's outputs is a by-run read of `dataset_events`,
  which was indexed only by dataset, so migrations 050 (SQLite) and 065
  (Postgres, `CONCURRENTLY`) add `idx_dataset_events_run_id`; without it every
  finalization with lineage on would scan the ledger. See
  [`docs/DATASETS.md`](docs/DATASETS.md).
- **`scheduler_signpost_hits_total{gate}` counts what this build refused.** The
  gates that answer with a pointer to what this build does include had no count,
  so there was no telling whether anyone met them. Three refuse a request while
  the engine keeps running, and those are counted: `external_cost_attribution`,
  `defer_connection` and `external_dataset_events`, each emitted at zero from
  the first scrape. A gate met at startup stops the engine, leaving nothing to
  scrape, and dagron-api serves no Prometheus endpoint, so neither is in it. See
  [`docs/METRICS.md`](docs/METRICS.md).
- **`dagron_wait_run` reports progress while it blocks.** The tool long-polls for
  up to 600 s and answered only at the end, and many MCP clients drop a request
  after 60 s without progress on it, so the tool failed for exactly the long runs
  it exists for. When a call carries `params._meta.progressToken`, the stdio
  server now sends `notifications/progress` with how many of the run's tasks
  have finished (`progress`) out of how many it has (`total`), read every 5 s and
  sent only when the count rose. A call without a token is answered exactly as
  before. Stdout now has one writer, fed by a queue that both replies and
  notifications go through, so a notification cannot split a reply or arrive
  after it. `handle` and `call_tool` keep their signatures;
  `handle_with_progress` and `call_tool_with_progress` take a `ProgressSink`.
  See [`docs/MCP.md`](docs/MCP.md).
- **Metrics guide, seven new Grafana dashboards, and the metrics they needed.**
  [`docs/METRICS.md`](docs/METRICS.md) documents every series the engine exports: what
  it measures, how to aggregate it across engines, query recipes and alerts.
  `examples/monitoring/` now ships eight dashboards, tested on Grafana 13.2.3: the
  overview, workflows by namespace, workflow statistics, run time, workflow jobs, dead
  letter queue, instance metrics, and health and latency. To make the per-workflow views
  possible the engine's `/metrics` gained: `scheduler_run_duration_seconds` (histogram,
  1 s to 4 h), `scheduler_workflow_run_duration_seconds` (sum and count by workflow and
  outcome), `scheduler_workflow_last_run_duration_seconds`,
  `scheduler_workflow_last_run_finished_timestamp_seconds`,
  `scheduler_workflow_last_run_success`, `scheduler_workflow_recent_runs` and
  `scheduler_environment_recent_runs` (runs created in the last 24 hours),
  `scheduler_workflow_active_tasks`, `scheduler_dead_letters_by_source`,
  `scheduler_dead_letters_oldest_age_seconds`, and on Linux the standard `process_*`
  series (CPU, memory, threads, file descriptors, start time). Workflow, environment and
  source labels are capped (50, 20, 20) with the rest summed as `other`. Each scrape
  runs three more datastore queries, all bounded by a time window or by unfinished work.
- **Template libraries: `use:` imports templates from another saved workflow.** A
  `templates:` block used to live in the spec that called it, so every Terraform
  workflow carried its own copy of the same 100-line `stage` template. A saved
  workflow that declares `templates:` is now a library, and `use: [iac-library]` (all
  its templates) or `use: [iac-library/tf_stage]` (one, plus the templates it calls)
  brings them in; `template:` calls and `arguments:` then work as before. Resolved by
  `dagron-api` when the workflow is saved or run, by name and as saved at that time. A
  name defined twice (in the spec and a library, or in two libraries) is refused rather
  than overridden, a library may not import another, and a missing library or template
  is a `400` on save. Paths that never reach the gateway refuse an unresolved `use:`
  with the cause instead of dropping it.
- **`examples/iac/`: Terraform, OpenTofu and Pulumi from one library.** `iac-library`
  holds `tf_checks`, `tf_stage` and `pulumi_stage`; `promote_terraform.yaml` (74 lines;
  `tool=tofu` runs OpenTofu, which needs 1.6 or newer) and `promote_pulumi.yaml` (49
  lines) are the same dev, staging, prod promotion as `examples/terraform/` with the
  repeated part gone. Pulumi has no reviewed plan file to apply, so its approval pins the
  reviewed preview and `apply` re-previews and refuses if the change summary moved: weaker
  than Terraform's saved plan, and not yet exercised against a live Pulumi stack. Each
  stage ends in a `done` task that succeeds only when the plan was applied or changed
  nothing, so a failed plan or a rejected review does not let the next stage run.
- **`examples/terraform/lifecycle.yaml`: plan, approve, apply, then approve, destroy, in
  one run.** One `stage` template called twice: `deploy` plans and applies behind a
  human gate, `teardown` plans with `-destroy` against what the deploy left and destroys
  behind a second gate. Each gate pins its plan with `binds`, and `destroy_after:
  "false"` leaves the environment up. Each stage ends in a `done` task that runs only when
  the stage applied or had nothing to apply, and the teardown waits for the deploy's: so
  rejecting the apply, or a failed check or plan, plans no teardown.
- **An approval decision now shows in the run log.** Approving or rejecting a
  `type: approval` gate writes `approved by <who> at <time>` (or `rejected`), then
  `comment: <text>` when the approver left one, to the gate's log, so it reads in the
  merged run log beside the tasks around it. Before, who decided and why lived only in
  `decided_by` / `decision_comment` on the row, and the gate's log was empty. A timeout
  decision logs the same way (`by timeout`). `output` is unchanged (`approved` /
  `rejected`), so `when:` conditions and trigger rules behave as before; gates decided
  before the upgrade keep an empty log.
- **The engine API no longer decides gates it cannot check.** Its approve/reject
  endpoints carry no caller identity, so they used to resolve a gate whatever its
  `approvers`, `not_triggerer` or `binds` said. They now answer 403 and name the rules
  and the gateway: `approvers` blocks both decisions; `not_triggerer` and `binds` block
  approving. A spec the engine cannot read is refused too. Plain gates are unchanged.
- **The gateway's live approval tests run in CI.** They (and the redrive and repo-sync
  live tests) skip without `TEST_DATABASE_URL`, and no CI job set it, so none of them
  had ever run there. `scripts/ci-test.sh` now starts a disposable Postgres for the
  `dagron-api` leg when run in CI. A new live test covers the end those checks exist
  for: a refused (outsider, or the run's own triggerer) or rejected plan is never
  applied, with the scheduler advancing between decisions.
- **`when:` gains `and`, `or`, `in [..]` and `not in [..]`.** `and` binds tighter than
  `or`, there are no parentheses, and the words are operators only when they stand
  alone, so existing conditions keep their meaning. The same grammar applies to task
  `when:`, `repeat.until` and schedule `when` gates (the gateway's copy now calls core's).
- **Templates can branch on their own tasks' output.** A runtime `{{ tasks.X.output }}`
  inside a template now follows `X` to its expanded name (`<call>.X`); before, the
  reference pointed at a task that did not exist and the spec was refused. In an `and`
  chain, the parts that use only parameters are decided at expansion: a false one
  removes the task, so one template can have an optional gate.
- **`promote_environments.yaml` is one `stage` template called three times.** Plan,
  destroy guard, review and apply are written once; `promote_through` becomes
  `when: "{{ promote_through }} in [staging, prod]"` on the staging call and
  `when: "{{ promote_through }} == prod"` on the prod call (dev always runs), so a
  stage beyond it is never created (the `level` helper task is gone), and dev's review is removed at
  expansion by `gated: "false"`. Tasks are now named `<env>.<step>`
  (`staging.review`, `prod.apply`).
- **Run a stored workflow with its parameters from the console.** The Run buttons
  (workflow list, editor, history) open a dialog built from the spec: `enum` becomes a
  dropdown, `required` is marked, `description` and `pattern` are shown, and values are
  checked against the same rules the engine applies before the request goes out (the
  engine stays authoritative). Only changed values are sent, so the spec's defaults
  still apply; a workflow with no parameters starts immediately, as before.
  `GET /api/workflows/{id}` (and the create/update responses) now return `parameters`
  and `param_schema`, and the MCP `dagron_get_workflow` / `dagron_run_workflow`
  descriptions point agents at them. `npm run check:params` covers the form rules.
- **A task's log keeps stderr, whether it succeeds or fails.** Migrations 049 (SQLite) /
  064 (Postgres) add `task_runs.log`: stdout and stderr lines in arrival order, streamed
  live by the local executor (and by the Docker executor, which already interleaved
  both but only streamed stdout). The log views (`/api/runs/{id}/logs`,
  `/api/runs/{id}/tasks/{tid}/logs`, and the engine's equivalents) read it, falling back
  to `output` for older rows and non-streaming executors. `output` is unchanged: still
  stdout (plus the stderr tail on failure), so `when:`, `repeat.until`, fan-out,
  `result_from` and the cache never see a stderr warning. The log resets when an
  attempt is claimed, and when the task has secrets it is rewritten once at exit,
  redacted as a whole, so a secret split across chunks cannot persist.
- **Terraform reference workflows and a conditions guide.** `examples/terraform/`
  gains `pr_check.yaml` (checks and a lock-free speculative plan for pull requests),
  `promote_environments.yaml` (dev, staging, prod with a reviewed gate per stage after
  dev, a `promote_through` cut-off, a destroy guard and a failure notification),
  `drift_check.yaml` (scheduled `plan -refresh-only` that fails the run on drift) and a
  README mapping each standard condition to the Dagron feature that expresses it.
  All parse in tests; the promotion, PR and drift flows were also run end to end.
- **Approval gates record who decided and why, and show the approver what they
  are approving.** A `type: approval` gate used to be a bare yes/no: the run
  history could not say who approved, and the approver saw only a task name.
  Two new task fields fix the second half: `approval_message` (free text,
  workflow parameters substituted, max 2000 chars) and `approval_show: ["<task>/<name>"]`
  (up to 16 artifact keys, same layout as `$DAGRON_ARTIFACTS/<task>/<name>`), both
  valid only on `type: approval` and included in the plan diff. `GET /api/approvals`
  returns them as `message` and `show: [{path, url}]`. The first half: migrations
  047 (SQLite) / 061 (Postgres) add `task_runs.decided_by` and `decision_comment`.
  Approve/reject accept an optional `{"comment": "..."}` body; the dagron-api
  gateway records the authenticated user as `decided_by`, the unauthenticated
  engine API records the comment only, and the timeout sweep records
  `decided_by = 'timeout'`. Both appear on the task rows of the run detail, and
  `dagron_approve_task` / `dagron_reject_task` take an optional `comment`. A
  second decision on an already-resolved gate is still a 409 and cannot
  overwrite the first record.
- **Approval gates can restrict who may decide.** Two new fields, valid only on
  `type: approval`: `approvers: [...]` (up to 64 entries; each is an email, an
  OIDC subject, or `group:<name>`, matched case-insensitively; empty means any
  signed-in user, as before) and `not_triggerer: true` (the person who started
  the run may not approve it, though they may still reject). The dagron-api
  gateway enforces them on approve/reject with a 403 that says why; the
  unauthenticated engine API cannot know who is calling and does not.
  `GET /api/approvals` gains `approvers`, `not_triggerer`, `can_approve` and
  `can_reject` for the caller, and the Approvals page disables the buttons
  accordingly. To know who started a run, migration 062 (Postgres) adds
  `workflow_runs.triggered_by`, set by the gateway on trigger, resubmit and
  run-now; runs started before it have no triggerer, so `not_triggerer` does not
  block them. **Behaviour change:** `dagron_approve_task` in `dagron-mcp` is now
  hidden and refused unless `DAGRON_MCP_ALLOW_APPROVE=1`, so an agent cannot
  approve a gate by default; `dagron_reject_task` stays available and
  `DAGRON_MCP_READONLY` still overrides both.
- **An approval can be pinned to the exact plan the approver read (`binds:`).**
  A `type: approval` gate may list `binds: ["<task>/<name>", ...]` (up to 16; shown
  like `approval_show`, and diffed in the plan). The worklist gives each bound
  artifact's current sha256; approving through the gateway must send those back as
  `digests` in the body (`400` without them, `409` when the artifact is missing or
  has changed since, `503` with no artifact store) -- so a plan re-planned after
  review cannot be approved unseen. On success the reviewed digests are written to
  `<gate>/approved.sha256` in `sha256sum -c` format, before the approval commits
  (a failed write aborts it), so a downstream `apply` can verify it is running what was
  approved (`cd "$DAGRON_ARTIFACTS" && sha256sum -c review/approved.sha256`). The
  Approvals page does this itself; `dagron_approve_task` takes an optional
  `digests` object. Enforced by the gateway only: the engine API neither checks nor
  writes them, so an apply that verifies `approved.sha256` fails closed if the gate
  was decided there. `examples/terraform/plan_review_apply.yaml` now uses it.
- **Workflow parameters can be constrained (`param_schema:`), and `max_active_runs`
  can be scoped per key (`concurrency_key:`).** `parameters: { name: default }` is
  unchanged. A new top-level `param_schema:` block adds rules keyed by parameter
  name: `required: true` (non-empty), `enum: [...]`, `pattern: "<regex>"` (must
  match the *whole* value) and a free-text `description`. They are enforced when a
  run is triggered, on the merged caller/schedule/environment values and before
  `{{ }}` is substituted into any task, and a refusal is a `400` that names the
  parameter (`parameter 'environment' must be one of: dev, staging, prod`). Saving
  a workflow does not demand `required` values, but a malformed rule, a key with
  no matching `parameters:` entry, or a default that breaks its own rule is an
  error; an empty value skips `enum`/`pattern`. Workflows without `param_schema`
  behave exactly as before. `concurrency_key: "{{ stack }}"` (templated, needs
  `max_active_runs`) makes the cap count only running runs of the workflow that
  share the resolved key, so `max_active_runs: 1` means one run per stack; the
  `429` names the key. Migrations 048 (SQLite) / 063 (Postgres) add
  `workflow_runs.concurrency_key`; on Postgres the admission lock narrows to
  (workflow, key). `dagron-core` gains a `regex` dependency.
  `examples/terraform/plan_review_apply.yaml` uses both.
- **Terraform examples keep the whole plan/apply output in the run log, and gain a destroy flow.**
  On the local executor the stderr of a task that succeeds is not stored, and terraform
  writes much of its output there, so every step in `examples/terraform/` now starts
  `exec 2>&1` and passes `-no-color`; `plan` no longer prints a marker word (a
  `plan_result` task reads a marker file instead), so `plan`'s log is the plan.
  New `plan_destroy_review.yaml`: `plan -destroy`, a bound approval, then apply of
  exactly that saved plan. Exercised end to end against a Postgres-backed engine,
  dagron-api and the built console: approve from the Approvals page, a stale digest
  refused with 409, `decided_by`/`decision_comment` recorded.
- **Task pods can tolerate taints, and take a priority class.**
  - `DAGRON_TASK_TOLERATIONS` (`key[=value][:effect]`, comma-separated) puts
    tolerations on every task pod the Kubernetes executor creates. A
    `DAGRON_TASK_NODE_SELECTOR` that points at a tainted pool no longer leaves
    them Pending.
  - An empty key is refused, since it would tolerate every taint, and so is a
    key or value the apiserver would refuse in a toleration: a key is a label
    key and a value a label value. A malformed value stops the engine at
    startup, rather than every task pod being refused.
  - `DAGRON_TASK_PRIORITY_CLASS` sets the pods' `priorityClassName`, and
    labels them `dagron.dev/priority-class` with it where the name fits a
    label (at most 63 characters), so the pods one class admitted can be
    selected by label.
  - Both are unset by default, and task pods are shaped as before.
  - The chart takes the priority classes: `engine.priorityClassName` and
    `dagronApi.priorityClassName` for their pods, and
    `engine.taskPriorityClassName` for every task pod the engine starts
    (`DAGRON_TASK_PRIORITY_CLASS`). Each class must exist in the cluster. All
    three are empty by default, and render nothing.
- **Every `dagron-mcp` tool now declares MCP tool annotations.** Before this, a
  tool definition carried only `name`, `description` and `inputSchema`, so a client
  that runs a tool unasked only when it says `readOnlyHint: true` asked a person
  before *every* dagron call, `dagron_list_runs` included. The annotations are
  derived, not written per tool. `readOnlyHint` is the read/write split
  `DAGRON_MCP_READONLY` already enforces, so the 24 tools a client may now run
  unasked are exactly the 24 a read-only server keeps. A write cannot be declared
  without its other three hints, and it states all three, because the spec reads
  an absent `destructiveHint` or `openWorldHint` as `true`. Each write's hints
  were decided against what its route actually does, not its name. Retry, rerun
  and clear are destructive because they wipe the task's captured output. Update
  is destructive because it replaces the definition the next scheduled run
  executes. Reject is open-world because a failed gate releases its `one_failed`
  handlers. Tests pin every write's hints by name. `initialize` still answers
  protocol `2024-11-05`: the annotations ride along, and why a bump is its own
  change is in [docs/MCP.md](docs/MCP.md#tool-annotations). **Behaviour change
  for gateways that pin tool definitions:** every dagron tool's definition
  changes once, so a pin that hashes annotations (MCPdef's does) needs one
  re-approval.
- **A NetworkPolicy for the engine and the bundled Postgres (`networkPolicy.enabled`, on by
  default).** The engine's ops API (`POST /runs`, `/cancel`, `/rerun`, approve) has no
  authentication, and the chart had no network boundary, so any pod in the cluster could
  submit arbitrary runs; a throwaway pod got a `run_id` back. Ingress to the engine is now
  allowed only from `networkPolicy.engine.allowFrom` and the `monitoring` namespace (so the
  ServiceMonitor keeps working); Postgres only from the release's engine, dagron-api, gitops
  and operator pods, so a task pod can reach neither. **Behaviour change:** anything else
  that calls the engine over HTTP must be added to `allowFrom`. It needs a CNI that enforces
  NetworkPolicy (proven on kind with a control test; not tested on EKS, where the VPC CNI's
  network-policy option must be on). No egress policy, and the API stays unauthenticated
  for the allowed peers; `docs/HARDENING.md` says exactly what is and is not covered.
  `loadtest/deploy/security-e2e/netpol.sh` (14 cases) is the proof.
- **Git-managed workflows.** A workflow synced from a connected repo now remembers
  its repo (`workflows.managed_by`, migration 060) and what git delivered
  (`managed_md5`). Consequences, all for the plain (unsigned) sync that most
  repos use: a sync that changes a spec writes a `workflow_versions` row
  (`created_by = git:<repo>@<rev>`; before, only signed bundles did, so
  `GET /versions` was empty and there was nothing to roll back to); a re-sync of
  the same commit writes nothing (it used to rewrite `updated_at` every poll);
  `PUT`/`DELETE /api/workflows/{id}` on a managed workflow answer `409` naming the
  repo and `sync-to-git` (an `admin` may pass `?force=true`), where a console edit
  used to be accepted and silently reverted at the next poll; the repo row's
  `drift` is now the number of managed workflows edited since the last sync (it
  was written `0` and never computed); and `prune: true` on a repo retires the
  workflows whose file was deleted (row and history kept), only from a sync with
  no file errors — a file that does not parse is one of those errors, so a typo in
  a spec suppresses the prune instead of retiring the workflow it broke. Git still wins over a console edit, but the edit stays in
  `/versions`. Authorization is unchanged: any authenticated user can still
  connect a repo.
- **`defer:` and `dagron-step-spark` were validated against a real cluster for
  the first time.** Until now the step had unit tests only. `loadtest/deploy/spark-kind/`
  and `loadtest/deploy/spark-eks/` hold the rig (kind + Spark operator 2.5.2, and an
  EKS variant behind `terraform/` with `enable_eks`), and `spark-eks/RESULTS.md` the
  numbers: 45 real Spark jobs against a 20-slot pool across three schedulers held the
  cap with no duplicate and no lost job through a scheduler kill; 2000 synthetic
  deferred runs peaked at 760 parked rows on ~290 m CPU across three schedulers,
  resolving p50 4.5 s / p99 6.7 s after the remote job finished; all three schedulers
  killed at once with ~500 parked came back in 6 s and every job still resolved. It
  found the three defects fixed below. `terraform/` gained `eks_node_ami_type`
  (x86 nodes for amd64 images), two ECR repos for the step and a Spark test image,
  and the EBS CSI driver with an IRSA role, without which a PVC never binds on a
  current EKS version.
- **Spark 4.2.0 runs.** `SPARK_IMAGE=spark:4.2.0` completed the workflow on kind
  and EKS. **Known issue:** the step writes `sparkVersion: "3.5.3"` into the
  `SparkApplication` whatever the image is; the operator tolerated it, but it is
  wrong metadata and there is no knob for it yet.
- **`defer.http.cancel` — a cancelled run, or a `max_wait_secs` that elapsed,
  now stops the remote job.** The built-in transport only ever GETs, so before
  this a cancelled Spark run went `cancelled` in dagron while the
  `SparkApplication` kept running (found on kind: still `RUNNING` two minutes
  after the cancel, `scheduler_external_orphans_total` still 0).

  ```yaml
  http:
    url: "https://…/sparkapplications/{{ handle }}"
    succeed_when: "status.applicationState.state == COMPLETED"
    cancel:
      url: "https://…/sparkapplications/{{ handle }}"   # method: DELETE by default
  ```

  `method` is `DELETE` (default), `POST`, `PUT` or `PATCH`; `body` is optional
  JSON. It reuses the block's headers, guarded client and redaction — a `content-type`
  the block already sets is kept rather than sent twice, and `{{ handle }}` is
  escaped where it lands inside the body's JSON. 2xx, 404 and
  410 count as torn down; anything else spends one of the three attempts. With
  `cancel:` set, `max_wait_secs` fails the task but keeps its handle so the same
  sweep tears the job down. Without it nothing changes. **The credential must now
  be allowed to `delete`**, not only `get`. A registered `ExternalPoller` still
  gets first refusal.
- **`SPARK_DRIVER_SERVICE_ACCOUNT` on `dagron-step-spark`** sets
  `spec.driver.serviceAccount`. The driver otherwise runs as the namespace's
  `default`, which cannot create executor pods, so any job that needs executors
  failed `Forbidden`. `SPARK_CONF_*` cannot carry this: it lowercases every key
  and Spark's are case-sensitive, so `…driver.serviceAccountName` went out as
  `…serviceaccountname` and was ignored.
- **`with_output_of:` chains off a producer that is itself fanned out.** The
  limitation `docs/LOOPS.md` shipped with: the producer was looked up by its
  authored name, so a producer that expansion had already turned into
  `regions.0`, `regions.1`, … matched no row. It failed at submit with
  "unknown task", because the validator runs on the expanded graph where the
  authored name no longer exists.

  ```yaml
    - name: regions
      command: ["list-partitions", "{{ item }}"]
      with_items: ["us", "eu"]
    - name: process
      command: ["handle", "{{ item }}"]
      depends_on: [regions]
      with_output_of: regions
  ```

  `regions.0` prints `["a","b"]`, `regions.1` prints `["c"]`, and `process`
  fans out into three carrying `a`, `b`, `c`. Downstream still depends on
  `process` alone and never learns there were three.

  **Which rows are read comes from the consumer's own `depends_on`**, which
  expansion already rewired onto exactly the producer's instances. That is what
  makes this precise rather than a prefix match against the run: a task the
  consumer does not depend on is unreachable in principle, there is no pattern
  to escape, and a workflow may legally have both `a` and `a.b` as authored
  tasks (an exact match wins over the prefix for that reason).

  Dependency order is instance order, so `regions.0`'s items come before
  `regions.1`'s. A string sort would put `regions.10` before `regions.2` and
  pair item lists with the wrong region without ever looking wrong.

  The same rule reaches a producer inside a `templates:` sub-DAG, whose rows
  are named `<call>.<task>`: `depends_on: [discover]` means the call's exits,
  so those are what is read. Both nesting shapes fall out of one rule rather
  than two special cases.

  An instance that printed nothing contributes nothing; every producer empty
  leaves the barrier succeeding with zero instances, as a single empty producer
  already did. An instance printing something that is not a list fails the
  fan-out and the message names **that row** — `regions.1`, not `regions`,
  which would send a reader to check all of them. `examples/templates/15_chained_fanout.yaml`
  is the scatter-gather-scatter shape end to end.
- **`with_output_of:` — fan out over what an earlier step printed.** The loop
  `docs/LOOPS.md` used to end by saying dagron could not express: *run a task
  that lists the partitions, then run the next step once per partition.*
  `with_items:` and `with_param:` are resolved by the expander before the run
  exists, which is exactly what lets `budget:` refuse a fan-out blow-up at
  submit — so a fan-out over a *result* could not use that path at all.

  ```yaml
    - name: process
      command: ["handle", "{{ item }}"]
      depends_on: [list-partitions]
      with_output_of: list-partitions
  ```

  The authored task becomes **one row that never executes**. When its
  dependencies are satisfied it parks — `status: running` holding no worker,
  the shape a wait sensor and a sub-workflow trigger already use — and a new
  reconcile sweep reads the producer's stdout, parses a JSON array, and inserts
  one `ready` row per element with `{{ item }}` substituted.

  That row then **stays, as the join point**. The alternative was re-parenting
  every dependent onto the new instances, which means rewriting dependency
  edges underneath a scheduler that is concurrently reading them; keeping the
  barrier means dependents were already wired to the thing they should wait
  for, `trigger_rule:` and `allow_failure:` keep working unchanged, and the
  only edges the sweep writes are new ones.

  In the console: Loop → For each item → Items from → **an earlier step's
  output**, which lists exactly the steps this one depends on — because the
  producer must be a dependency. That rule is not a nicety: the instance count
  is read from a task's output, so a producer the step does not wait for would
  make the count depend on scheduling order. It is refused at submit, and by
  the panel at the keystroke.

  **This is the one thing in dagron that makes a run bigger after admission**,
  so the run's task ceiling (`DAGRON_MAX_TASKS_PER_RUN`) is re-checked at
  insert time: a fan-out that would exceed it fails the task with that message
  *before* inserting anything. Expansion is one transaction gated on a CAS of
  the barrier's version, so two schedulers sweeping the same tick cannot both
  expand it; a crash mid-expansion commits nothing and it is simply expanded
  again. An empty array **succeeds** with zero instances, unlike
  `with_items: []` — at submit an empty list is an authoring mistake, but at
  run time "there were no partitions" is a result.

  (That shipped with one limitation — a producer that was itself fanned out or
  lived inside a template could not be addressed by its authored name. Lifted
  above.)

  Two consequences of the barrier being a fifth **park** shape, both caught in
  review: it is excluded from `POOL_RUNNING_COUNT` like the other four — its
  instances inherit its `pool`, so a parked barrier holding a cap-1 pool's only
  slot would deadlock against its own children — and each instance is stamped
  with the barrier's `version` at creation (`fanout_epoch`, the same idiom as
  `external_epoch`). A barrier's instances are its *dependencies*, so no reset
  path's downstream cone reaches them; without the stamp, clearing a producer
  and re-running would join against the previous attempt's rows and report
  success having processed the old data.
- **A loop's log shows every iteration, not just the last one.** `task_runs.output`
  is a single column, and three writers take turns clobbering it — the live-log
  chunker on an attempt's first chunk, `retry_task` when an iteration ends, and
  the success path at the end. `repeat:` routes through `retry_task` (the engine
  re-uses the retry machinery for loop iterations), so a 30-pass poll stored one
  pass and discarded twenty-nine. The same overwrite has always cost **retries**
  their per-attempt output: a task that failed twice and passed on the third try
  showed only the try that worked.

  Superseded attempts are now retained in a new `task_attempts` table and read
  at `GET /runs/{id}/tasks/{task_id}/attempts` (engine) and
  `GET /api/runs/{id}/tasks/{tid}/attempts` (console), with the same filter
  grammar as the log views. The console's task panel gains **"N earlier
  attempts"** beside the log pane — collapsed, and fetched only when opened,
  because the pane beside it polls once a second for as long as a task runs and
  a history attached to every poll would multiply the hot path by its own size.

  Each row says whether it was a loop pass (`iteration`) or a failure
  (`failed`), because a healthy 30-iteration poll rendered as 30 errors is a
  worse answer than none.

  **Retention is bounded, and that is the design, not a limitation.** Keeping
  attempts whole would multiply three quantities none of which is bounded:
  executor output has no cap on the write path, `RepeatSpec.max_iterations` is a
  `u32`, and `GC_RETENTION_SECS` is unset — GC off — by default. So what is kept
  is a **tail** of each attempt (`DAGRON_ATTEMPT_LOG_BYTES`, default 4096, hard
  ceiling 64 KiB) and a **window** of attempts per task
  (`DAGRON_ATTEMPT_LOG_KEEP`, default 50), which makes the added storage a
  product of two constants known before the run starts. Measured: **228 KiB**
  for a 1000-iteration loop, 12 KiB for a task that retried three times, and
  **zero** for a task that neither loops nor retries — the cost is paid only by
  the tasks whose logs were wrong. `DAGRON_ATTEMPT_LOG_BYTES=0` is a complete
  opt-out: no rows, no extra transaction work, the previous consumption byte for
  byte. `evicted` on the read API is how a history that lost its beginning says
  so, rather than passing itself off as starting at iteration 1.

  The read is bounded independently of retention: one request returns at most
  the newest 200 attempts, because `DAGRON_ATTEMPT_LOG_KEEP=0` disables
  eviction and an unlimited retention window must not imply an unlimited
  response.

  The retention write shares the state transition's transaction, so a retry that
  loses its CAS — a reclaimed worker's stale completion — writes no history for
  a task it no longer owns. `docs/ITERATION-LOGS.md` carries the measurements
  and the reasoning, including what this does *not* fix (`task_runs.output`
  itself is still uncapped, as it always was).
- **History reads as a diff, not as a stack of YAML documents.** Definition
  history listed every version behind a *View* button that printed the whole
  spec, so answering "what changed in v3" meant opening two of them and
  comparing by eye. The question people open history with is what changed, and
  the view answered a different one.

  Both history surfaces now answer it directly:

  * **Definition history** (workflow editor) gains a `Changed` column — `+4 −1`
    per save, computed from the specs already in hand, so most visits need no
    click at all. Opening a version diffs it against a base that **defaults to
    the origin** and stays put as you walk forward, because comparing v1→v2,
    v1→v3, v1→v4 is how drift is read. Full YAML is still one toggle away; it is
    how a version gets copied back out.
  * **Run history** gains *Definition changes*: the page's runs grouped into the
    definitions they actually ran, oldest first, each diffed from the base. The
    runs table gains a matching `Spec` column, so a block of failures and the
    edit that preceded them line up on one screen instead of being correlated by
    hand.

  Grouping is on the **spec text**, not `definition_id`: every run snapshots its
  own definition row, so two runs of an untouched workflow have different ids
  and identical specs. Only chronologically adjacent runs merge — an edit that
  is later reverted is a third era, not a re-entry into the first, because the
  workflow really did run something else in between.

  The differ is its own module and has no dependency: common prefix/suffix trim,
  then an LCS over what is left, with hunk collapsing so a three-line save
  doesn't render three hundred unchanged lines. Past a size ceiling it reports
  the changed region as one replacement **and says so on the header**, rather
  than quietly stopping being a diff. `npm run check:diff` holds it to
  reconstruction (dropping the additions rebuilds the base; dropping the
  removals rebuilds the new side) over both hand-written cases and a randomized
  pass, which is the property a wrong diff violates — and a wrong diff does not
  look broken, it looks like a change nobody made.

  Costs nothing at rest: *Definition changes* is collapsed by default and
  fetches only when opened.

- **`GET /api/runs/specs?ids=a,b,c` — many runs' specs in one call, grouped by
  content.** The view above needs a page of runs' definitions, and there was no
  way to ask for more than one at a time, so it made a request per run.

  Grouping is the point, not just batching. Every run snapshots its own
  `workflow_definitions` row, so twenty-five runs of a workflow nobody edited
  are twenty-five identical specs under twenty-five ids: answering per-run would
  send the same document twenty-five times to establish that nothing changed.
  Identical specs collapse into one entry listing the runs that used it — which
  is also the shape the caller wants, since one group *is* "nothing changed".

  Groups and their `run_ids` follow the **requested** order, because Postgres
  `= ANY` has none and the caller lines the result up against its own list.
  Unknown ids are **absent rather than a 404**: a page containing one archived
  run must still answer for the other twenty-four, and which ids came back is
  already the answer to which didn't. `400` is reserved for a malformed request
  — no ids, or more than 200.

- **Loops are a control in the console, not a YAML edit.** Repeating work was
  authorable but not *editable*: `repeat:` had a palette block and no panel
  field, so changing "poll 30 times" to "poll 5 times" meant opening the YAML
  tab — and `with_items:` was worse than that. Fan-out **locked Visual mode
  outright**, on the honest ground that a `with_items:` task is one node on the
  canvas and N rows at run time, and a node that quietly claims to be one task
  is a lie.

  The lock is lifted by fixing the lie rather than tolerating it. A looping node
  now carries a badge saying what it expands to — `⟳ ×4 parallel`,
  `⟳ 3× in place`, `⟳ until …` — the same bargain the `sub-DAG · 3 tasks` node
  has always made. The badge is load-bearing: it is what pays for the unlock.

  The task panel gains a **Loop** section over one vocabulary for the engine's
  two mechanisms, which are not interchangeable and are no longer presented as
  though they were:

  * **For each item** → `with_items:` / `with_param:`. Parallel, resolved by the
    expander at run creation, so the count is known up front and the node can
    say `×N`. Items come from a count, a literal list, or a workflow parameter,
    with `instance_key` offered as "name each copy by".
  * **Repeat N times** → `repeat:`. `RepeatSpec` has no count field, so this
    writes `until: "{{ attempt }} == N"` with a matching budget, and reads that
    exact shape back rather than degrading into an opaque condition.
  * **Repeat until** → `repeat:` with the author's own condition.

- **`⟳ Repeat whole workflow` — running the entire DAG N times.** The engine has
  no run-level loop, so the control moves the graph into a `templates:` sub-DAG
  and adds one task that calls it: `with_items:` for N independent copies at
  once, or a body that recurses with `{{ pass + 1 }}` under a `when:` base case
  for N passes end to end. Both unroll at run creation into one flat DAG.

  The rewrite stays invisible. The canvas keeps drawing the user's own steps —
  it edits the loop's *body*, with the recursion task hidden and its
  `depends_on` rebuilt on every edit so the next pass always waits for the real
  end of the graph. "Remove loop" puts everything back. Collapsing someone's
  pipeline into a single `loop-pass` node the moment they tick "repeat" would
  have been a strange trade for asking to repeat it.

  Sequential passes are capped at 50: a pass *is* a level of template recursion
  and the expander's depth cap is 64.

- **The external poll batch runs concurrently, and the comment claiming it
  already did is now true.** The sweep awaited each parked `defer:` row before
  starting the next, so a batch pointed at a black-holed vendor held the
  reconcile tick for `EXTERNAL_POLL_BATCH` × the request timeout — 32 × 15 s
  of nothing happening, with claim, dispatch, log drain and run reaping stalled
  behind it. The per-request timeout bounds one poll; it never bounded the tick.
  The code sat under a comment that read *"Concurrent, for the reason the
  url-sensor batch above is"*, describing the `wait.url` probes, which really
  are.

  Now three passes, and which work goes in which is the whole design:

  1. **Serial** — the `defer.max_wait_secs` ceiling, decided before anything is
     polled so a vendor we cannot reach cannot keep a task parked past its
     budget; and header resolution, the one step that touches the pool and the
     per-sweep credential cache. Doing it here keeps that cache a plain
     `&mut HashMap` instead of a mutex every in-flight poll contends on.
  2. **Concurrent** — the network, and nothing else. A `JoinSet` bounded by the
     same `EXTERNAL_POLL_BATCH` that already bounds the SQL, exactly as the
     `wait.url` probes are.
  3. **Serial** — every datastore transition and every meter bump, back on the
     reconcile thread one at a time, so the accounting stays single-threaded and
     `newly_terminal` means what it says. The concurrency bought wall-clock on
     the network and paid for none of it in the state machine.

  **A timeout is still not a verdict, and is now load-bearing twice.** A hung
  registered `ExternalPoller` used to stall the loop; concurrency alone would
  not have fixed that, it would only have moved the stall — one blocked
  implementation holds a `JoinSet` slot forever and the sweep awaiting the set
  never finishes its pass. The 30 s deadline expires into the `Err` the sweep
  already re-parks on, so a slow vendor still cannot fail a six-hour job.

  **What an operator will notice is on the other side of the wire.** A vendor
  can now see up to 32 simultaneous status requests from one scheduler where it
  saw one at a time. If that trips a concurrency limit the 429 re-parks rather
  than failing, and `EXTERNAL_POLL_SECS` still protects the rate
  ([`EXTERNAL_JOBS.md`](docs/EXTERNAL_JOBS.md)).

  **One regression caught on re-read, before it shipped.** Hoisting header
  resolution out of the poll made a row whose credential would not resolve skip
  the poll entirely — including the registered poller, which needs no header of
  ours and may have been resolving that kind perfectly well. It also reported
  the row through the once-per-kind *"nothing in this build resolves this
  defer.kind"* warning, which would send an operator to register a poller when
  their secret was simply missing. Those are now distinct: an unresolvable
  credential is a transient the next sweep retries, and only a genuinely absent
  transport is named.

- **A fleet sweep for workloads whose scheduler died.** 0.10.0 labelled every
  pod and container with the task that owns it, and made each dispatch delete
  that task's earlier attempts first. That reap has a blind spot by
  construction: it runs *when a task is dispatched*, and looks only at that
  task. A workload whose task never runs again — the run was cancelled while a
  scheduler was dying, the task failed terminally, retention collected the row
  — is invisible to it forever, and those are precisely the ones that keep
  costing.

  Every `DAGRON_ORPHAN_SWEEP_SECS` (default 300) the engine now lists the
  workloads carrying this installation's labels, asks the datastore which of
  their task ids are still non-terminal, and deletes the rest, counting
  `scheduler_orphan_workloads_reaped_total`. Anything but near-zero in steady
  state is worth alerting on: each increment is a scheduler that died between
  creating a workload and finishing it.

  **It is opt-in, and that is the whole design rather than caution.**
  `dagron.dev/managed-by=dagron` says *some* dagron made a workload; it does not
  say *which*. Two installations sharing one Kubernetes namespace both stamp it,
  so a sweep scoped on that alone reads the other install's pods, fails to find
  their task ids in its **own** database, concludes they are orphans, and
  deletes running work belonging to someone else — absence and foreignness are
  the same observation. A fifth label, `dagron.dev/installation`, distinguishes
  them, and dagron cannot infer its value: a namespace can hold two installs,
  one install can span namespaces, and a pod carries no pointer back to the
  database that created it. So `DAGRON_INSTALLATION` is the operator's to set,
  and without it the sweep does not run and says so at boot rather than leaving
  a metric at zero for a reason nobody can see. The per-dispatch reap is
  unaffected either way — it selects on a task id, which is a UUID and therefore
  needs no scope.

  An installation that will not express as a label contributes **no** label
  rather than a truncated one. Truncation is the dangerous outcome:
  `prod-eu-1-…` and `prod-eu-2-…` can share the 63-character prefix a
  sanitiser would produce, and then one installation's sweep selects the other's
  pods —
  the exact deletion the label exists to prevent. Contributing nothing leaves a
  workload unsweepable, which costs a leftover rather than live work.

  **A selector says nothing about the labels it does not name.** It constrains
  the ones it does — `managed-by=dagron` really is an equality check — but the
  sweep selects on `managed-by` and `installation` and then reads `task-id` off
  whatever comes back, and that third label is bound by nothing. A workload can
  match the first two exactly while carrying a `task-id` that is empty, or holds
  a slash, or runs to 200 characters.
  Nothing dagron creates looks like that, because the labelling refuses to
  label partially; a legacy install, another tool or a hand-edited manifest
  can. That workload is the sweep's worst case rather than an odd one: an id no
  row ever had matches no live row, the sweep acts on absence, and it gets
  deleted. Reading a candidate out of its labels is now the single operation
  allowed to refuse one — `ManagedWorkload::from_labels`, shared by both
  backends so neither can drift — and a task id that could never have named a
  row does not become a deletion candidate. It is the rule the stale-attempt
  check already applied to `attempt`, applied to the identity the whole
  judgement rests on rather than only to the tie-breaker. `run_id` and
  `attempt` stay unvalidated on purpose: they exist for the operator reading
  the log line, and a real orphan must not lose its cleanup over a field no
  decision reads.

  **A name addresses a slot; a uid addresses the occupant.** The sweep is
  spread across time by construction — it lists, asks the datastore, then
  deletes — and a name is not a promise that all three saw the same object.
  Taking the name from the listing rather than re-deriving a selector closes
  the easy half of that; the other half is that between the list and the
  delete, a name can come to mean something else. Every dagron pod is named
  `sched-<uuid4>`, so in practice nothing reuses one, but "our names happen to
  be unguessable" is a convention, not a guarantee, and the sweep's whole
  premise is deleting things nobody is watching. The Kubernetes delete now
  carries the listed pod's uid as a **precondition**, which makes it a
  compare-and-swap on identity: if the name resolves to a different object the
  apiserver answers `409` and does nothing, and the sweep says so rather than
  counting a reap. Docker needs no equivalent — a container id already is the
  identity rather than a name for one — so its deletion stays id-based.

  **Two races, closed two different ways.** The datastore and the apiserver
  share no transaction, so the sweep lists workloads *before* asking which tasks
  are live — anything it saw therefore predates the answer, and a task row is
  always written before its workload is created, so "listed but not live" cannot
  mean "its row had not been written yet". `DAGRON_ORPHAN_MIN_AGE_SECS` (default
  600) covers what ordering does not: clock skew, replication lag, and a
  workload whose creation timestamp is missing entirely, which counts as young
  because the alternative is judging a workload whose age is unknown.

  **The cap is on deletes, not candidates**, and that distinction was a bug
  before it was a design note. Capping the candidate list starves: an apiserver
  lists in a stable order, so a namespace whose first 200 workloads are
  long-running and live would be re-examined identically every sweep and never
  reach the leftovers behind them. Every candidate is asked about — the liveness
  query is chunked rather than truncated — and only the acting is rationed.

  **A datastore error skips the pass rather than failing the tick.** The sweep's
  listing and its deletes were best-effort from the start; its liveness query
  was not, and `?` there would have made an opt-in cleanup feature a new way for
  a transient database blip to terminate the scheduler daemon — `main` returns
  `run()`'s error straight out. One failed chunk now abandons the pass's deletes
  entirely rather than acting on a **partial** live set, which is worse than
  none: a task whose chunk never ran looks dead, and the whole sweep acts on
  absence.

  The seam is split in two — `Executor::list_orphan_candidates` and
  `delete_workload`, both provided methods defaulting to no-ops — because only
  the engine can answer the question in between. "Is this task still live?" is a
  database query, and handing an `Executor` a connection pool would put schema
  knowledge behind a trait anyone may implement. The local process pool inherits
  the no-op correctly: its children die with it.

- **`budget.external_cost` — a ceiling on external work that is arithmetic,
  not a guess.** `budget: { external_cost: N }` refuses a run whose deferred
  tasks add up to more than `N`, at creation, before one remote job is
  submitted. Each deferred task declares what one submission is worth with
  `defer: { cost: K }`, default `1` — so a spec that declares no unit costs
  simply caps the number of submissions, and a spec that does gets a weighted
  sum where the 200-node Spark job outweighs the one-row query.

  `pool:` was the only bound on remote spend this build had, and it bounds the
  wrong axis. It caps what is burning *simultaneously*; it says nothing about how
  many jobs a run submits over its life, and those are different failures — one
  exhausts the cluster now, the other arrives on the invoice.

  The count is task **rows**, the same gang-aware count admission and
  `budget.tasks` already use. A `gang: { size: 8 }` deferred task is one line in
  the YAML and eight submissions, so it costs `8 × cost`; budgeting the page
  would budget nothing. Refusal is at creation for the reason `budget.tasks` is:
  after expansion the sum is exact, so the run never starts rather than dying
  partway with cluster-hours already spent. Over the API it is a **400**, beside
  the `budget.tasks` refusal, because the same submission is refused identically
  forever.

  **`RunBudget`'s doc comment used to say a spend budget would be "a promise with
  no measurement behind it, which is worse than no field", and that still
  stands** — this is not that field, and the distinction is the only reason it
  could ship. `external_cost` sums numbers the *author* declared on their own
  tasks. The engine never learns what a cost unit means; it is not dollars, not
  GPU-hours, not anything dagron can verify. Exact arithmetic on a declaration,
  never a measurement of spend.

  Reconciling a vendor's actual invoice back to the run, workflow and team that
  caused it is the separate capability, and `budget: { external_cost_attribution:
  true }` is refused here with a signpost naming it. The flag is declared in the
  open struct on purpose: `DagSpec` carries no `deny_unknown_fields`, so serde
  would have silently dropped the key — a workflow written against a build that
  has attribution would have validated clean, run, and accounted nothing. A
  refusal is louder than a drop. The **ceiling** is not the gated part and will
  not become one; a guardrail you set for yourself over compute you already pay
  for prices nothing.

- **The funnel has a destination.** Twelve gates across the product end with a
  link to the README's "What this build does not do", and that section ended:
  *"The seams exist so an implementation you write — or one someone else ships —
  drops in without forking a file here. Nothing on this page depends on that
  happening."* Every signpost in the product converted to self-building. There
  was nothing to ask about and nobody to ask.

  It now names the six capabilities that are actually gated — managed ingestion
  connectors, the fleet plane, external dataset events, named connections for
  external compute, KMS-wrapped envelope encryption, and managed artifact
  transfer — each beside what this build does instead, and ends
  with one thing to do. The call to action is an issue on the public repo rather
  than an inbox: `PRODUCT.md` records `enterprise@dagron.dev` as the contact
  *and* records that it still needs verifying before it is relied on, and
  publishing an unverified address as the only route is worse than publishing
  none.

  **One destination, not four.** `docs/AI_WORKLOADS.md` carried a second section
  under the identical heading, and `docs/DATASETS.md` and `docs/EXTERNAL_JOBS.md`
  carried their own variants. Each keeps its page-local detail — the gang claimer,
  the `produces:` fallback for external events, named connections — because that detail is
  real and page-specific; what they no longer do is each be a terminus. All three
  now route to the canonical section. `docs/HPC_AUTOPSY.md`'s "Limits — what this
  does not do" is deliberately left alone: it is honest tool scope ("it does not
  schedule anything", "it does not collect telemetry"), not a list of things that
  ship elsewhere, and collapsing it into a commercial destination would misread
  it.

- **One test now holds every signpost to the same bar.** The eight anchor-linked
  gates asserted mutually different subsets of it: `source.rs` checked the
  fallback and the seam but not the anchor; `fleet.rs` and `link.rs` checked the
  anchor and the fallback but not the seam; `dagron-crypto` checked only that the
  gap was named; the engine's `403` was asserted by nothing at all. A funnel
  whose steps each enforce a different rule is a funnel that leaks.

  The test scans the source tree rather than exposing a helper the gates call,
  because no such helper can reach them all — `dagron-crypto` has zero dagron
  dependencies, `dagron-core` and `dagron-source` are *upstream* of
  `dagron-engine`, and `dagron-api` never builds a `Seams`. One reader covers
  every crate regardless of which way the arrows point.

  Getting it to actually bite took three corrections, each found by injecting a
  deliberately dead-end signpost rather than by reading the code:

  1. A fixed character window around the anchor reaches into whatever code sits
     nearby, and in a large file that almost always contains a marker — so the
     check passed for a message that said nothing. The window is now the message
     block: the run of consecutive non-blank lines the anchor sits in.
  2. `"this build"` is the strongest signal of an alternative ("This build
     streams with `SOURCE=stream`") and is *also* inside the gap phrase "not in
     this build" — so matching it against the whole message made the check
     vacuous. The gap phrase is removed before the alternative is looked for.
  3. The match had to be case-insensitive, because the alternative is a new
     sentence and starts with a capital.

  The test is green on the tree and fails on an injected dead end; both
  directions are verified rather than assumed.

- **A deferred task's `produces:` now reaches the ledger, and `{{ ds }}` exists.**
  Two small things that together are the difference between a partitioned
  warehouse story that runs and one that does not.

  `produces:` is a postcondition — "after this task succeeds, the dataset is
  current" — and a deferred task succeeds in the reconcile sweep, not on the
  worker-result path where the recorder lived. So `defer:` + `produces:` would
  have validated, run, and recorded nothing, leaving every downstream sensor and
  `on_datasets:` consumer parked forever. The sweep now records through the same
  `record_produces` the worker result and the memoization cache hit already
  share — a third path into one function rather than a second copy of it —
  guarded on the resolve having actually landed, so a re-sweep that lost the race
  cannot fabricate a second lineage row. The validation refusal that stood in for
  this since the primitive landed is removed, and the `produces:` kind guard's
  comment, which claimed the worker result was the only recording path, is
  corrected rather than left quietly false.

  The recorder needs the task's name; it now rides on the park row the sweep
  already fetched, rather than costing a second query per resolved job.

  **`{{ ds }}` and `{{ ds_nodash }}`** are the fire's logical date — `2026-09-14`
  and `20260914` — bound wherever `scheduled_time` already was. They did not
  exist, and their absence failed in a way that pointed nowhere near the cause:
  `expand::substitute` leaves an unknown placeholder *verbatim*, so a URI written
  `clickhouse://db/t/{{ ds }}` survived expansion with its braces intact and then
  tripped `validate_dataset_uri`'s whitespace check — the workflow refused at
  registration with an error about its URI rather than about a variable that is
  not bound. Every partitioned-warehouse example in the ecosystem is written with
  `ds`.

  Bound through one helper rather than three inserts at each of the four fire
  paths (cron, schedule, backfill, backfill jobs), because the derivation has to
  agree across all of them: a backfill and a cron fire that partition by the same
  expression must produce the same string, or they write to different partitions
  of one table. `ds` is the UTC day of `scheduled_time`, matching the timestamp
  beside it rather than any local calendar — a fire at 23:30 UTC-05:00 is the
  15th, not the 14th. A timestamp that will not parse binds `scheduled_time` as
  before and simply omits `ds`: a fire is never failed over a date format, and a
  missing variable surfaces as the refusal it always was rather than as a quietly
  wrong partition.

  `examples/warehouse/` is the whole shape in runnable form — wait for a
  partition, roll it up on a Spark cluster dagron does not own, announce the
  partition that came out, gate the publish on a scalar check. A test validates
  it the way `examples/templates/` is already validated: an example that stops
  parsing is worse than no example, because it is a working shape a reader copies
  and then debugs.

  **Not included: the OpenLineage emitter.** The roadmap pairs this work with
  emitting dataset facets on a terminal run event, but no such emitter exists in
  the tree — that is building one, not wiring one, and it needs a new
  `SELECT DISTINCT uri … WHERE run_id = ?` plus an index on `dataset_events`
  (the only read path today is indexed on `uri`) or it is a full ledger scan on
  the run-finalization hot path. Left for its own change rather than smuggled in
  here.

- **`dagron-step-spark` and `dagron-step-sql` — the two things that actually
  submit.** `defer:` gave the engine somewhere to park a remote job; these are
  what start one.

  **`dagron-step-spark`** submits and prints `dagron::handle=<id>`. Three
  backends: a `SparkApplication` CR (default — vendor-free, no account, and the
  reference adapter), a generic `rest` POST whose body and handle path you
  supply (one adapter for Databricks, EMR Serverless, Dataproc, Livy, Kyuubi),
  and `spark-submit` as the honest escape hatch — which needs a version-matched
  Spark distribution in the *task's* image, that being precisely the dependency
  problem dagron positions against.

  The job is named `dagron-<DAGRON_TASK_ID>-<DAGRON_EXTERNAL_EPOCH>`, and the
  name is the idempotency: a retry after a crash reuses it, the cluster answers
  AlreadyExists, and the step **adopts** the running job rather than starting a
  second one. Adoption is conditional on the found job being non-terminal —
  adopting a finished job would park the task on a corpse it can never leave. On
  the k8s backend that guarantee is the API's own semantics rather than anything
  dagron invented; on `rest` the same name goes wherever the vendor takes an
  idempotency token.

  Naming a vendor as the backend (`SPARK_BACKEND=livy`) is refused with a pointer
  at `rest`, where it already works, rather than silently accepted. A named
  backend per vendor is a permanent maintenance surface for a request shape the
  author can already write.

  **`dagron-step-sql`** runs one statement against one store. Named after what
  people search for — clickhouse, starrocks, doris, trino, postgres, redshift,
  mysql — and implemented against what actually varies: three wire protocols.
  Maintenance is bounded by protocol count, not vendor count, which is the
  conclusion Airflow reached the expensive way when it deprecated
  `SnowflakeOperator`, `BigQueryExecuteQueryOperator`, `PostgresOperator` and
  `TrinoOperator` in favour of one generic operator plus a connection.

  **Its output contract is the reason to use it over a `clickhouse-client`
  one-liner**, which the docs already tell people works. The engine appends every
  stdout line to the task's `output` column with `output = COALESCE(output,'') ||
  ?` and **no cap anywhere on that path**, so `SELECT *` to stdout is an
  unbounded write into the datastore. Therefore rows never go to stdout: `exec`
  keeps nothing, `scalar` writes one value bounded to 1 KiB (so a downstream
  `when:` can gate on it), and `rows` writes NDJSON into `$DAGRON_ARTIFACTS`.
  `SQL_MAX_ROWS` / `SQL_MAX_BYTES` are enforced **while fetching**, before the
  first byte lands — a check applied after the write is a report, not a budget,
  and failing the task afterwards does not unwrite anything. `rows` mode
  **refuses** when `$DAGRON_ARTIFACTS` is unset rather than falling back, because
  both fallbacks are wrong: stdout is the unbounded write, and the container's
  filesystem disappears with the task.

  The password goes in `SQL_PASSWORD`, never the DSN, and a DSN carrying an
  inline credential is refused with the fix named. `Redactor::DEFAULT_PATTERNS`
  matches task env vars by *name*, so `SQL_DSN=clickhouse://user:pw@host` is
  masked in no log line and no error message; a separately-named variable the
  patterns already catch is the difference between redacted everywhere and
  redacted nowhere. (The redactor's four-character minimum still applies: a
  shorter password is masked by nothing, here or anywhere.)

  Neither crate depends on `dagron-core`, whose `default = ["sqlite"]` would link
  a bundled SQLite into every task image running either step — in step-sql's case
  a *second*, conflicting database. step-sql takes `sqlx` directly instead, the
  same reasoning that keeps `dagron-gitops` out of the workspace build. The cost
  is a dozen duplicated lines of dotted-path lookup in step-spark, recorded where
  it happens.

  Both default to a lean backend set and both **images** carry every backend: a
  source build vendoring the binary should not be made to carry a Kubernetes
  client or a wire driver it will never speak, while one image that submits
  anywhere beats three a user has to choose between. CI compiles and tests the
  non-default features explicitly, the reason the `mqtt` leg exists — and that
  leg immediately earned itself by catching a dead import that `-D warnings`
  would have failed on.

- **Cancelling a run now reaches the cluster.** It was pure SQL: flip the rows
  terminal, clear the leases, return 200. Nothing touched the system actually
  running the work, so for a deferred task "we cancelled your run" meant "we
  stopped watching your cluster bill" — and `cancel_run`, `cancel_overdue_runs`
  and the API's own inlined SQL all had the same hole.

  A terminated task that still holds an `external_handle` now **owes a
  teardown**, and a sweep settles it through `ExternalPoller::cancel`.

  The debt is row state rather than something the cancel stamps, which is the
  design decision the rest follows from. The cancel path most callers actually
  use is inlined SQL in `dagron-api` — a binary that by design cannot depend on
  the engine crate and holds no `Seams` — so a teardown the cancel *performed*
  would be one the SDK and the MCP server never triggered. A cancel that merely
  leaves evidence is one every caller performs for free.

  The predicate is per task, not per run: holds a handle, is not parked. Per-run
  is wrong twice over — the happy-path finalizer would issue a redundant vendor
  cancel for every completed job, and a cancelled gang leaves its run `running`
  so those rows would never sweep at all. A job that finished by itself owes
  nothing, because resolving or failing it already cleared the handle.

  **It is best-effort, and now says so out loud.** A job we cannot reach is a job
  we cannot stop, so teardown gives up after three failed attempts or an hour
  from the task going terminal, whichever comes first — and when it does, the
  handle is cleared, `scheduler_external_orphans_total` increments, and a warning
  names the kind and the handle so it can be stopped by hand. That counter is the
  feature: the alternative to a visible leak is a silent one.

  `gc_old_runs` will no longer collect a run that still owes a teardown, for the
  same reason — deleting it drops the only record of a job running on someone
  else's cluster. The debt settles itself inside the give-up window, so
  collection is deferred by that and no longer.

  An engine with no poller for a kind hands the row back **without** consuming an
  attempt: a `RUNNER_CLASSES` pool runs the same binary with different seams, so
  a replica that cannot do the work must not exhaust the budget of the one that
  can. The wall-clock bound is what makes teardown terminate regardless of fleet
  shape — the attempt budget alone would not.

  The sweep is a lease claim every replica runs, not a leader-gated loop:
  leadership gates only the ops loops, and vendor-side idempotency would bound
  correctness but not call volume. Teardowns are issued concurrently with the
  batch bounded, so a vendor that is timing out cannot hold the reconcile tick.

  Note the built-in `defer.http` transport is a GET: it can tell you a job
  finished, it cannot stop one, so a `defer.http` job whose run is cancelled is
  orphaned rather than torn down. Registering an `ExternalPoller` whose `cancel`
  issues the vendor's own call is what turns that into a real teardown.

  **No `cancelling` run status**, deliberately. It is a wire-compatibility break
  for every shipped SDK — both pin `TERMINAL_RUN_STATUSES` to
  `{succeeded, failed, cancelled}`, so a pinned client would poll forever — plus
  a SQLite `CHECK` rebuild of `workflow_runs` that, because `task_runs.run_id`
  references it, would force dropping and restoring `task_runs` and
  `task_dependencies` too. The teardown ships inside the existing terminal
  transition instead.

  One thing checked rather than assumed: a worker whose task is cancelled
  mid-flight cannot resurrect the row and lose the handle. `cancel_run` does not
  bump `version`, but it does NULL `claimed_by`, and that is the guard
  `mark_task_succeeded` actually fences on — so the late result is rejected. A
  test pins it, because the whole teardown design rests on a cancelled row
  staying cancelled.

- **`defer.http` — one adapter, and no vendor code in dagron.** A deferred task
  can now name a status endpoint and two predicates, and the engine polls it to
  a verdict. That single transport reaches Databricks (`runs/get`), EMR
  Serverless (`GetJobRun`), Dataproc (`batches.get`), Livy, Kyuubi, YARN and a
  SparkApplication CR, because every one of them answers "is it done?" with a
  JSON document containing a state field. The consequence is the point: a vendor
  API change is a YAML edit by the person it affects, on their schedule, rather
  than a dagron release on ours.

  ```yaml
  defer:
    kind: databricks
    http:
      url: "https://dbc.example.com/api/2.1/jobs/runs/get?run_id={{ handle }}"
      headers: [{ name: Authorization, value_from: { secret: DATABRICKS_TOKEN } }]
      succeed_when: "state.result_state == SUCCESS"
      fail_when:    "state.result_state in [FAILED, TIMEDOUT, CANCELED]"
      error_from:   "state.state_message"
  ```

  The grammar is `path == V`, `path != V`, `path in [A, B]` over dotted paths
  that walk objects and arrays alike. Deliberately not expressible: wildcards,
  filters, recursive descent, arithmetic, boolean connectives — each is a step
  toward a query language nobody asked this project to maintain, and a response
  that needs one needs a step binary, which is a seam that already exists. It is
  also hand-written rather than a JSONPath dependency: `jsonpath-rust` is in
  `Cargo.lock` already, but only through `kube` behind the non-default
  `kubernetes` feature, so depending on it would pull it and `pest` into every
  build including the armv7 release leg — to evaluate a dotted path and a
  comparison.

  **Everything is parsed at submit.** A typo in `succeed_when` is otherwise a
  workflow that validates cleanly, starts a six-hour job, and only then discovers
  it cannot read the answer — the most expensive possible moment to find a typo.

  Three decisions worth stating, because each is the safe side of a coin flip
  that costs a job when called wrong:

  - **A missing path is `false` for every form, `!=` included.** A vendor that
    has not written `state.result_state` yet is not a vendor reporting failure.
    Absent means undecided; the next poll costs seconds and a wrong verdict costs
    the job.
  - **`fail_when` is evaluated before `succeed_when`.** If an author's predicates
    overlap, one is wrong — and the mistakes are not equal. A false failure costs
    a retry; a false success advances every dependent on a job that produced
    nothing. The overlap is logged rather than silently resolved.
  - **A non-2xx, an unparseable body, a reset connection or a timeout is not a
    verdict.** All re-park. Failing a healthy six-hour job because its vendor
    answered 503 once is the outcome the three-state `Verdict` exists to prevent.

- **The external-poll sweep claims its rows.** It listed them before, which is
  the shape the `wait.url` sweep has — and that shape gets away with it only
  because nobody runs hundreds of HTTP sensors. This is the feature whose selling
  point is hundreds of parked jobs, and every scheduler sweeps, so an unclaimed
  read meant N schedulers calling the vendor N times for one job. The claim is a
  CAS on `next_poll_at` (the shape `claim_due_dataset_triggers` already uses on
  its cursor), and it doubles as the crash bound: a scheduler that dies
  mid-request leaves the row due again at the claim's expiry, so there is nothing
  separate to expire and nothing to reap. The guard is null-safe (`IS` on SQLite,
  `IS NOT DISTINCT FROM` on Postgres) because a row parked with no interval
  carries `next_poll_at IS NULL`, and an `=` guard would make exactly those rows
  unclaimable forever, silently, by every scheduler.

- **The poller's network policy is the inverse of `wait.url`'s, deliberately.**
  `WAIT_URL_DENY_PRIVATE` is off by default, which is right for an
  unauthenticated readiness probe whose main use is an in-cluster address. It is
  wrong for a poll that carries a **bearer token**: the same permissiveness lets
  a workflow author aim an operator's credential at any address the scheduler can
  reach, `169.254.169.254` included. So `defer.http` gets its own client with
  `DEFER_HTTP_DENY_PRIVATE` **on** unless disabled, and `DEFER_HTTP_ALLOW_HOSTS`
  as the one-line, deliberate way to poll an in-cluster endpoint. The filter
  lives inside the resolver (so the addresses checked are the addresses dialled
  and a DNS rebind has no window) and redirects stay refused.

  Credentials resolve per run per sweep — two DB queries and an AES-GCM decrypt,
  shared across a batch of jobs against one workspace, and re-resolved next tick
  so a rotation lands without a restart. The redactor is built from the
  *resolved* headers, so the token is masked out of anything the poller writes
  back to the task, including a vendor error envelope that echoes `Authorization`
  straight back. Whatever `error_from` extracts is truncated before it reaches
  `output`, because nothing on that write path caps it.

  `wait: { url: … }` is untouched: still off-by-default, still no credentials on
  that field, still byte-identical.

- **`dagron-state` exists, and this is the first release note that says so.** The
  crate has been a workspace member, mirrored to the public repo, mounted at
  `/api/state`, and shipping a `dagron-state` binary and a companion doc
  (`docs/STATE_PLAN_USECASES.md`) — while appearing **zero** times in this
  changelog across every release. A component nobody was told about is a
  component nobody uses, and its whole premise is that someone who already runs
  dagron can adopt it in an afternoon.

  What it does: turns a backfill planner's **state plan** — the minimal,
  topologically ordered set of SQL models a change requires rebuilding — into a
  dagron run graph, and submits it. The planner answers "given this SQL change,
  what actually needs rebuilding?" and stays a library with no binary and no
  service so any orchestrator can embed it; this crate is the other half, for
  the orchestrator that happens to be dagron.

  Five mount-relative routes under `/api/state`: `contract` (the wire revision
  this build reads), `plans` (compile a plan to workflow YAML, submitting
  nothing), `plans/explain` (why each model rebuilds — summary, rows, markdown,
  Mermaid), and `plans/submit`. Only the last one creates anything, which is the
  point of having the first four: `dagron-state explain plan.json` touches no
  network, no database and no running engine.

  Two boundaries carry the design and neither is a linker edge — the planner
  reaches it as JSON on a frozen contract, and it reaches its host through one
  trait. That is what lets the component move without moving dagron, and why
  `MOUNT_PREFIX` is exported rather than written down twice.

- **`defer:` — a task can hand its work to a system dagron does not own, hold no
  worker while that work runs, and re-attach to it after the engine dies.** A
  task that runs a six-hour Spark job by blocking on `spark-submit` holds a
  worker slot for six hours. That is affordable once and ruinous at a few
  hundred — and it puts the job's fate in one process: kill the engine and the
  job is orphaned, while the row is reclaimed by lease recovery and **submitted
  again**.

  `defer:` splits the two things that were conflated. The task's `command` is the
  *submit*, and nothing else; the job is a row. The command runs exactly as any
  command task does — lease, `max_attempts`, `timeout_secs`, fault
  classification — and on success prints `dagron::handle=<id>`. The engine then
  parks the row: claim dropped, lease NULLed, `status` still `running`, the
  handle on the row. A reconcile sweep polls it to a verdict.

  **The NULL lease is the whole crash-recovery argument, and it is a property
  rather than an intention.** `recover_expired_leases` reclaims rows `WHERE
  lease_expires_at IS NOT NULL`, so a parked row is provably outside the set
  that sweep can touch. Kill every scheduler mid-job: the row is untouched, and
  any replica's next sweep resumes the poll, because the handle is durable state
  rather than an in-memory registration. Nothing resubmits, because nothing
  re-ran. A test asserts exactly that sequence.

  Three timeouts now mean three different things, which is the point of the
  split: `timeout_secs` bounds the submit, `defer.max_wait_secs` bounds the
  remote job, `run_timeout_secs` bounds the run. Conflating the first two is the
  mistake this prevents — the executor's 25-second default is right for a submit
  and absurd for a Spark run.

  **Adoption after the one genuinely dangerous window.** If the engine dies
  *after* the remote system accepted the job but *before* the park committed, a
  naive retry starts a second cluster. So the engine injects `DAGRON_TASK_ID`
  and `DAGRON_EXTERNAL_EPOCH`, and a step names its remote job
  `dagron-<task_id>-<epoch>`: the task id is stable across lease recovery, so
  the retried submit reuses the name, the remote system answers AlreadyExists,
  and the step adopts the running job. `attempt` cannot serve this — it
  increments on every claim *including* recovery, so a name built from it
  changes at exactly the moment adoption is needed. The epoch is bumped only
  where a row that already submitted is deliberately re-armed for a fresh job (a
  resolved remote failure, `rerun_from_failed`, clearing a task with its
  downstream), so a retry gets a new job while a recovery adopts the old one.

  **`pool:` means something different for a deferred task, deliberately.** The
  four existing park shapes wait on something free — a timer, an endpoint, a
  dataset cursor, a child run — and release their pool slot. A deferred row
  waits on a job burning someone's cluster the entire time, and keeps its slot.
  Releasing it would make `POOLS=spark:4` cap *submissions*, which take seconds
  and bound nothing; keeping it makes the same setting mean "at most four
  concurrent Spark jobs", which is the only bound on remote spend the open build
  has.

  A submit that exits 0 but prints no handle is a **failure**. Succeeding it
  would advance dependents on work that has not happened.

  Backends register through a new `ExternalPoller` seam on
  `dagron_engine::Seams`, returning `Result<Option<Verdict>>` — the same shape as
  `SourceFactory::build`. The `Result` arm is load-bearing rather than
  decorative: three verdicts (`Running` / `Succeeded` / `Failed`) describe the
  job, while a transport failure describes nothing about it and re-parks. Forced
  into a verdict, a 429 either kills a healthy job or gets swallowed. A
  `defer.kind` no poller owns leaves its rows parked with one warning per kind,
  because an engine rebuilt without the backend that owns a running job should
  not tear that job's task down on the next tick.

  Validation refuses what would park a row nothing can resolve: `repeat` (two
  loop operators that disagree about what ends the loop), `gang` (N members
  parking on N jobs leaves no all-or-nothing), the command-less task kinds, and
  `defer:` on a `template:` call, which was a real silent drop:
  expansion reads `defer` off the template's leaf, so a deferral written on the
  call vanished and left a task that submitted and succeeded immediately, which
  reads as working.

  `defer.connection:` — a named, access-controlled endpoint registry — is
  declared in the open struct and refused at validation with a signpost naming
  the open path. Declared rather than left unknown on purpose: `DagSpec` carries
  no `deny_unknown_fields`, so serde silently drops an unrecognised key, and a
  workflow written against a build that has connections would otherwise validate
  clean here, run, and submit with whatever the task's own env happened to
  carry, with no diagnostic anywhere.

  See [`docs/EXTERNAL_JOBS.md`](docs/EXTERNAL_JOBS.md). **Upgrade note:** a
  scheduler predating this release silently ignores `defer:` and succeeds the
  task while the job runs — roll schedulers before publishing `defer:` specs.

### Security
- **A task may run only as a ServiceAccount an operator allow-listed
  (`DAGRON_TASK_ALLOWED_SERVICE_ACCOUNTS`).** A task pod is created in the
  KubeExecutor's namespace, beside the engine, whose own ServiceAccount can
  create pods and read Secrets (the chart's engine Role). Nothing stopped a
  workflow-authored task from setting `service_account:` to that account — or any
  other in the namespace — and running its command with those rights: an
  escalation from "run a container" to "create pods and read Secrets here". The
  executor now checks a requested ServiceAccount against an allow-list, where the
  pod is built, so a disallowed one fails the task instead of running as it. The
  check is a pure function with unit tests: unset denies every request, an
  allow-list permits only its exact names (listing an IRSA task identity does not
  admit the engine's), and a task naming none is unaffected.
  - **Behaviour change:** the allow-list is **empty by default**, so a task that
    declares `service_account:` is now refused until an operator lists that name.
    An install that uses `service_account:` for IRSA sets
    `DAGRON_TASK_ALLOWED_SERVICE_ACCOUNTS` to the task identities it created for
    it — never the engine's own ServiceAccount. Tasks that declare no
    `service_account:` are unchanged. `docs/CONFIG.md` has the setting.
- **The read-only `viewer` role is enforced in every build, not just enterprise.** A viewer is
  refused every mutation — `403 {"error": "viewer role is read-only"}` — by middleware, before
  the request reaches a handler. `GET` is untouched, and so are `POST /api/login` / `/logout`:
  a viewer who cannot sign in cannot read either.

  It was gated on `--features enterprise`, which meant a role whose entire definition is
  "read-only" silently granted writes in the open build. The console only offers the role in
  the enterprise build, but that was never the whole story: `POST /api/users` accepts
  `groups: ["viewer"]` in **both**, and an instance downgraded from enterprise keeps the
  viewers it already had — who would quietly regain the ability to create, edit, run and
  retire workflows. That is a security bug, not a feature boundary.

  The predicate, the set of paths it applies to, and the refusal now live in `auth.rs` and are
  shared: the open build enforces the role, the enterprise build enforces it *and* audits, off
  one definition, so the two cannot drift on what counts as a mutation.

  **Behaviour change for enterprise:** that refusal was a plain-text body and is now the
  documented JSON envelope, matching every other error the API returns.

  Operators — users with *no* groups — are deliberately untouched: authoring workflows is that
  role's whole purpose. Per-team ownership, so one operator cannot edit another team's
  workflows, remains its own change.
- **Deleting a workflow now requires the `admin` group** (`403` otherwise) —
  `DELETE /api/workflows/{id}`. It was open to any authenticated session, including the
  console's "operator" role, which is a user with *no* groups. Delete is the one irreversible
  control action here and it cascades to the workflow's schedules, so an operator who only
  wanted to stop a workflow and reached for it lost the schedules too.

  `POST /api/workflows/{id}/state` with `retired` is **not** gated and is the path that was
  always recommended over delete (`docs/API.md`): it stops the workflow running and keeps its
  schedules. Creating and editing a workflow are not gated either — those are an operator's
  job, and a bad edit is recoverable from `/versions`. The `403` body names `state: retired`
  so the refusal points at the action the caller probably wanted.

  **Breaking** for a deployment where non-admins delete workflows; the same recovery as the
  repository gate below (an admin grants the group, or `DAGRON_ADMIN_EMAIL` +
  `DAGRON_ADMIN_PASSWORD` seed one). A `viewer` is refused this and every other mutation by the
  read-only gate (see the entry above), in both builds.
- **Connecting a repository, disconnecting one, and setting or clearing its credential now
  require the `admin` group** (`403` otherwise) — `POST`/`DELETE /api/git-repos` and
  `PUT`/`DELETE /api/git-repos/{id}/auth`. Until now any authenticated session could do all
  three. A connected repository is an authority over workflows, not a preference: the worker
  clones whatever `url` holds and writes the workflows it finds, and with the `prune` flag
  added in this same release it also *retires* the ones whose file is gone — so connecting a
  repository was a way for any account to create and remove workflows. The credential is an
  instance-held secret the caller can never read back.

  **Breaking** for a deployment whose operators are not in `admin`: the console hides those
  controls and the API refuses them. The group is granted by an admin through
  `POST /api/users` / the users page, or seeded at startup from `DAGRON_ADMIN_EMAIL` +
  `DAGRON_ADMIN_PASSWORD` (idempotent — an existing email is left untouched). An instance
  with no admin at all was already unable to manage users, which is the same recovery path.

  Deliberately **not** gated: listing repositories, which is what the console renders, and
  `POST /api/git-repos/{id}/sync`, which only asks for the poll `auto_sync` already performs
  on a timer. Deleting a *workflow* was a separate surface, left to its own change — the entry
  above, which gates it too, so nothing in this release leaves it open.

### Fixed
- **The engine no longer exits when dagron-api creates one of its tables first.**
  dagron-api's schema bootstrap creates `users`, `git_repos` and `environments` too
  (`migrations_pg` 008, 009 and 026). Started together on a fresh database, as compose and a
  Helm install start them, the engine's migration failed whenever dagron-api's
  `CREATE TABLE` committed first: it hit a catalog constraint instead of taking its
  `IF NOT EXISTS` no-op ("while executing migration 8: duplicate key value violates unique
  constraint "pg_type_typname_nsp_index""), and the engine exited before it served. With a
  dagron-api started right after the engine, that happened in 4 of 40 starts. `init_pool` now
  replays its migrators past such a race:
  - A migration that ran in a transaction rolled back whole and was not recorded, so it is
    replayed, up to four attempts. A unique violation counts only on a `pg_*` catalog index;
    a duplicate object or a deadlock always counts.
  - A `-- no-transaction` migration (the concurrent index builds) is never replayed, since a
    failure can leave it half done.
  - sqlx returns from a failed migration without releasing its advisory lock, so the failed
    attempt's connection is closed rather than returned to the pool. Returned, it would hold
    the lock, and the next migrator would wait on it forever.

  A live test (`TEST_DATABASE_URL`) keeps dagron-api's `CREATE TABLE users` uncommitted while
  the engine migrates, then commits it. It fails without the fix. It also starts a second
  migrator, which fails the test if the first left its lock held.
- **The Databricks example in `docs/EXTERNAL_JOBS.md` missed most ways a run can fail.**
  Its `fail_when` listed three result states, and the Jobs API has some 25
  terminal codes, so a run ending in, say, `MAX_CONCURRENT_RUNS_EXCEEDED` or
  `SKIPPED` matched neither predicate and polled until `max_wait_secs`. It now
  reads the Jobs API 2.2 `status` object (the older `state` object is
  deprecated): success waits for `status.state == TERMINATED`, because
  `termination_details` already appears while the run is `TERMINATING`, and
  failure is `status.termination_details.code != SUCCESS`. Checked against
  Databricks' API reference, and a test pins it. The engine's "both predicates
  matched" warning now fires only when both read the same field, since two fields
  matching together is how such a spec says "finished, and not failed". The same
  page named Dataproc's `batches.delete` as a way to stop a remote job; it refuses
  a batch that has not finished, so the page now names `operations.cancel` on the
  batch's operation.
- **The public mirror linked to guides it did not carry.** `docs/METRICS.md`,
  `docs/LOOPS.md` and `docs/ITERATION-LOGS.md` are linked from the README,
  `API.md`, `CONFIG.md`, `OPERATIONS.md` and `examples/monitoring`, and were
  never in the mirror's allowlist. They are now.
- **`dagron-api` no longer exits when its schema bootstrap deadlocks with the engine's
  migrations.** Started together on a fresh database, as compose and a Helm install start
  them, `dagron-api`'s `ALTER TABLE workflow_runs ADD COLUMN IF NOT EXISTS environment`
  could queue behind the engine's `CREATE INDEX CONCURRENTLY` on the same table
  (`migrations_pg` 047 and 048). The index build in turn waits out every older snapshot,
  that queued ALTER's included. Postgres broke the cycle by failing `dagron-api` with
  `deadlock detected`, and the bootstrap retried only duplicate-object errors, so it exited
  before it listened ("ensuring environments schema ... deadlock detected"). With the 0.9.3
  images that happened in 4 of 40 starts. The bootstrap now replays on `deadlock_detected`
  (40P01) too: every step is idempotent, and only two index builds touch that table, so the
  existing four attempts cover it. A live test (`TEST_DATABASE_URL`) builds the same cycle
  from an open write, an index build and the bootstrap, and fails without the fix.
- **The Terraform examples' compliance step runs, and their destroy guard fails closed.**
  `checkov` has no `--no-color` flag (it exits 2 on an unknown argument), so the compliance
  task in `pr_check.yaml`, `plan_review_apply.yaml`, `promote_environments.yaml` and
  `iac/iac-library.yaml` failed before scanning anything; the flag is gone, and piped output
  carries no colour codes anyway. The destroy guard in `promote_environments.yaml` and
  `tf_stage` counted every `"actions"` entry in `terraform show -json`, so a plan that only
  dropped an output was refused, and `show | grep ... || true` read a failed or empty `show`
  as "0 deletions" and let the plan through to approval. It now counts only
  `resource_changes`, captures `show` on its own so a failure stops the step, and reads the
  count with `jq -e`; `jq` is a new prerequisite, as it already was for `lifecycle.yaml`.
- **A fresh `compose.yaml` stack can publish approval `binds` digests.** `/artifacts` was a
  named volume over a path neither image has, so Docker created it root-owned while the
  engine and `dagron-api` run as uid 65532. The engine logged "could not prepare artifact
  dir", never set `DAGRON_ARTIFACTS`, and a gate with `binds` listed `sha256: null`, so it
  could not be approved from the Approvals page. Both images now ship `/artifacts` owned by
  65532, which a new volume inherits. A volume created before this stays root-owned: remove
  it (`docker compose down -v`) or `chown 65532:65532` it once.
- **The test of `notify.git`'s `{{ run.images }}` never ran.** Its `#[test]`
  sat on the README example's test instead, which ran twice. So nothing
  checked that each image is listed once, in the order it first appears, that
  `task_defaults` stands in for a task that names none, or that a workflow
  with no images renders empty. It runs again, and fails against a
  `run_images` that lists an image twice or ignores `task_defaults`.
- **A workspace engine started with nine false warnings.**
  - The typo scan took Kubernetes' service links for dagron knobs. The
    kubelet gives every pod its namespace's Services as variables, and the
    chart's Services are named `dagron-*`, so each engine warned of eight
    `DAGRON_ENGINE_*`, and seven `DAGRON_POSTGRES_*` beside the chart's
    Postgres. A name in the kubelet's exact shapes, beside its Service's
    `_SERVICE_HOST`, which nothing else sets, is no longer a typo. A real
    typo beside them still warns.
  - A resident server given no DAG file warned that it could not read the
    default `examples/simple_dag.yaml`. That file is in a checkout, not in
    an image, and the chart runs its engine with no DAG. Such a server (the
    management API, cron, DB schedules or GC on) now starts with no initial
    run, as `dagron dev` does without its file. A DAG file that was asked
    for and is missing still warns, and so does a one-shot run with nothing
    to run.
  - Unit tests cover both, each red against a broken variant. The built
    engine, started as the chart starts it, with the kubelet's variables for
    two Services and one real typo, warns of the typo alone.
- **The chart's engine could not seed `/workflows`.** The chart ran the
  engine as uid 10001, but the image runs as 65532, distroless's `nonroot`,
  which owns `/workflows` and the working directory, `/home/nonroot`.
  Kubernetes ignores the image's `VOLUME`, so the first-start copy of the
  bundled examples into `/workflows` failed, a `seed copy failed … Permission
  denied` warning for each, and a path the engine resolved from its working
  directory failed the same way. The chart now runs the engine as the image's
  user, 65532, as it already ran `dagron-api`. Engines roll once on upgrade.
- **Two knobs were reported as typos at every boot:**
  `DAGRON_TASK_PRIORITY_CLASS` and `DAGRON_TASK_TOLERATIONS`, both read by
  the Kubernetes executor, were missing from the engine's registry, so an
  engine given either logged a "looks like a dagron knob but is not one"
  warning. Both are registered now, and the registry test holds them, and
  `DAGRON_TASK_ISOLATION_FLOOR`.
- **Two more real knobs were reported as typos at every boot:**
  `DAGRON_TASK_ALLOWED_SERVICE_ACCOUNTS` and `DAGRON_TASK_SECRET_ENV`, both
  read by the Kubernetes executor and documented in `docs/CONFIG.md`, were
  missing from the engine's registry, so an engine given either would have
  warned. The test that pins
  the registry against the knobs other crates read now names both.
- **A failed dead-letter redrive no longer loses the dead letter.** Both redrive routes
  deleted the row as their claim and committed that before creating the run.
  `dagron-api`'s `POST /api/dead-letters/{id}/redrive` then lost the payload, with its
  `error`, `source`, `failures` and timestamps, on any refusal after the claim: an unknown
  environment (`400`), the workflow's `max_active_runs` cap (`429`), a datastore error
  (`500`). A retry answered `404`. The engine's `POST /dead-letters/{id}/redrive` re-parked
  its two capacity refusals (`503`/`507`) under a new id, with the refusal as the error and
  fresh timestamps, and lost the payload to a log line on any other failure. Both now
  commit the run and the delete in one transaction (`db::create_run_from_dead_letter`, both
  backends): a failed redrive leaves the row exactly as it was, and the same id can be
  redriven once the cause is fixed. Concurrent redrives of one id still make at most one
  run. The engine's `503`/`507` keep their codes and `dead_letter_id`, which is now the id
  that was sent. `docs/API.md` had described the engine's re-park for the `dagron-api`
  route too, which never did it.
- **No `ssh://` repository could sync under the chart.** The chart ran the gitops worker as
  uid 65532 while the image defines uid 10001, and OpenSSH refuses to run for a uid with no
  passwd entry (`No user exists for uid 65532`), so only https repositories could work. The
  chart now uses 10001, as the engine and operator charts already did, which also fixes
  anyone on the published image. Found by running the worker in its container for the first
  time: `loadtest/deploy/gitops-e2e/` (dagron + the worker + a real sshd on kind) is the
  suite that now covers it, and it also records, as known gaps, that any authenticated user
  can connect a repo and delete a workflow.
- **`sync-to-git` opened its PR where the pull loop was not looking.** The file went
  to `dags/` while a connected repo watches `dagron/` by default, so a merged PR never
  fed back. With `GIT_PATH_PREFIX` unset the prefix is now the connected repo's `path`
  (env still overrides; `dags/` when no connected repo matches `GIT_REPO`).
- **Polling the in-cluster Kubernetes API needed a setting no doc named.** The
  poller rejected the apiserver's cluster-CA certificate until the engine had
  `SSL_CERT_FILE=/var/run/secrets/kubernetes.io/serviceaccount/ca.crt` alongside
  `DEFER_HTTP_ALLOW_HOSTS`; only the second was documented. Also documented:
  `POOLS` is per-scheduler, so keep it identical through a rolling update.
- **`repeat:` on a `template:` call is refused instead of silently dropped.**
  A call is replaced by the template's tasks during expansion and the call's own
  fields go with it, so the loop never reached a row anything evaluates: the
  workflow submitted cleanly, ran its body exactly once, and said nothing — the
  failure mode that reads as a working workflow. Nothing downstream could catch
  it either, since post-expansion validation only sees leaves, by which point
  the field is gone.

  Now an error at expansion, beside the identical guard `defer:` already had,
  and it names the way out (`with_items:` on the call, or the loop on the
  template's own task). `dag.rs` already refused `repeat` on an approval gate
  and a wait sensor for exactly this reason; the call was the gap.
- **`dagron-step-sql`'s two transports disagreed about what a scalar costs.**
  The HTTP path admits its scalar line against the budget, because it arrives
  through the same `for_each_line` reader as any other row; the wire path never
  charged one. The same statement therefore spent a row over ClickHouse and
  nothing over Postgres or MySQL, which makes `SQL_MAX_ROWS` mean something
  slightly different per transport. Charged on both now.

  **The two transports also bound memory at different granularities**, which the
  docs now state rather than imply. The HTTP path sees raw bytes, so
  `Budget::check_pending` can refuse part-way through a row. The wire path gets
  a row already decoded by sqlx, so one row is the smallest thing it can refuse
  and a single enormous cell is resident before the budget is consulted. One row
  rather than the whole result is a real guarantee — it is just not the same
  one, and "resident memory is one row plus one chunk" read as though it were.

  **And `docs/dockerhub/dagron-step-sql.md` still carried the old caveat** —
  "what it does *not* bound is the read" — which the streaming change left
  behind. That page is the Docker Hub description, so it was the copy most
  likely to be read by someone deciding whether to use the image, and it
  described a limitation that no longer exists.

- **A `defer.http` poll could send one vendor's credential to another vendor.**
  The sweep caches resolved headers so a batch of parked jobs does not re-run
  two queries and an AES-GCM decrypt per row. It keyed that cache on `run_id`
  alone — but `defer.http.headers` is a property of the **task**, not the run.
  A run with two deferred tasks aimed at two systems resolved the first task's
  headers and then handed them to the second: vendor A's bearer token sent to
  vendor B's status endpoint, and the wrong principal authenticated even where
  nothing leaked.

  The key is now the run **and** the unresolved header block — the spec as
  authored, `value_from` references still unresolved, so the key itself never
  holds a plaintext credential. Two tasks naming the same secret still share one
  resolution, which is the whole point of the cache; two naming different ones
  never do. A spec that will not serialise is not cached rather than sharing a
  degenerate key: resolving twice costs two queries, sharing costs a credential.

- **A stale attempt could delete the live pod or container that replaced it.**
  The predecessor reap skipped only the attempt *equal* to its own, so anything
  carrying a different attempt was reaped — including a **higher** one. A
  scheduler that stalls between claiming a row and creating the workload can
  resume after its lease expired and another replica claimed the row, so attempt
  1 reaches the reaper while attempt 2 is already running. It then deleted
  attempt 2's live workload and started duplicate work: precisely the fence
  violation these labels were added to prevent.

  Both executors now share one predicate, `is_stale_attempt`, and only a
  **strictly lower** attempt is reapable. A missing or unparseable label is left
  alone on the same principle — a workload that cannot prove it is stale is not
  deleted, because an orphan that survives is a job for a fleet-wide sweep while
  a live pod deleted on a guess is work already lost. The comparison is numeric,
  so `"10"` is not stale against `9`.

- **Predecessor cleanup had no aggregate deadline, so dispatch delay grew with
  the mess.** Each removal was bounded at 10 s, and per-call timeouts do not
  compose: N leftovers against a wedged dockerd or an unresponsive apiserver
  delayed workload creation by N × 10 s. `ctx.timeout_secs` does not cover this
  — it starts later, at the wait and log collection. The whole reap, list and
  removals together, now runs inside one 20 s budget, after which dispatch
  proceeds: cleanup is best-effort by design, and refusing to start because a
  leftover would not die turns a cleanup failure into an outage.

- **The `defer.http` poller buffered an unbounded status body.** `resp.text()`
  read whatever the endpoint sent before anything could look at it; the existing
  512-byte truncation bounded the *log line*, not the memory. A deadline is not
  a size bound either — an endpoint that streams steadily sends gigabytes inside
  15 seconds. And this poller runs in the **scheduler**, so an oversized body
  exhausts the control plane and stops every parked job in the fleet rather than
  failing the one task that asked. The body is now counted as it arrives and
  refused past 256 KiB, with `Content-Length` given no authority: it is the
  sender's claim about a body it has not finished writing.

- **A registered `ExternalPoller` could stall the reconcile loop forever.** The
  trait carries no timeout contract and the sweep awaits each row before the
  next, so an implementation that blocks did not delay one job — it stopped
  every lease, deadline and schedule the loop drives. A seam whose worst case
  takes down the scheduler is not a seam anyone can safely implement. Each call
  is now bounded at 30 s, and expiry is deliberately **not** a verdict: it
  becomes the `Err` the sweep already treats as "no answer" and re-parks, so a
  slow vendor still cannot fail a six-hour job.

- **`SQL_MODE=scalar` printed the first of several rows as if it were the
  answer.** Both transports stopped at row one — which is what bounds memory,
  and was also what let a two-row result drive a downstream `when:` on whichever
  row the store happened to send first, under an ordering the statement never
  declared. Both now read **two** rows: the answer, and one more to prove there
  was no second. The read stays bounded at two rows rather than the result set,
  and the refusal names `LIMIT 1` and `SQL_MODE=rows`.

- **`SPARK_WAIT=inline` did not wait on a standalone master.** `inline` promises
  the task holds its worker until the job finishes, and `spark-submit
  --deploy-mode cluster` honours that on YARN and Kubernetes, whose
  `waitAppCompletion` properties default to true. Standalone's defaults to
  **false**, so the CLI returned as soon as the master accepted the driver and
  the task succeeded against a job that had not run — releasing every dependent
  against work still in flight, which is the exact failure the `inline` refusal
  on `k8s` and `rest` exists to prevent, arriving through the one backend where
  `inline` is legal. The step now sets
  `spark.standalone.submit.waitAppCompletion=true` for inline submits, and
  **refuses** a `SPARK_CONF_*` override that would cancel it. Anything that is
  not literally `true` counts, because Spark reads it as a boolean and a typo is
  a `false`.

- **Three documentation claims that were true of one backend and written as
  though true of all.** [`EXTERNAL_JOBS.md`](docs/EXTERNAL_JOBS.md) said the
  generated job name "is the idempotency" and that a crash-retry adopts the
  running job, with no backend named — but `AlreadyExists` is the apiserver's
  guarantee and applies to `k8s` only. On `rest` the retry submits a second job
  unless `{{ name }}` lands in the vendor's own idempotency field, and on
  `spark-submit` `--name` is a label and a retry starts a second application.
  The per-backend table the crate README already carried is now in the doc too.
  The same page called `k8s` the default without saying that a default-features
  source build links no Kubernetes client and refuses it. `dagron-step-spark`'s
  README claimed the binary "never waits", which `SPARK_WAIT=inline` has always
  contradicted.

- **`dagron-step-sql`'s result budget now bounds the read, not only the write.**
  It was called before each row was written, so an over-budget statement wrote
  nothing to the task's `output` column or its artifact — that guarantee held.
  What it did not do was bound memory, because both transports materialised the
  whole result set before the budget saw a row: `resp.text()` on the HTTP path,
  `fetch_all` on the wire path. So `SELECT * FROM huge_table` OOM-killed the
  process before the refusal naming `SQL_MAX_ROWS` could print, and the operator
  got an exit code with no explanation — the failure mode the budget exists to
  replace, arriving by another route.

  Both transports stream now. The HTTP path reads `bytes_stream()` and parses
  NDJSON incrementally, admitting each line as it completes; the wire path uses
  `fetch` rather than `fetch_all` and admits each row as sqlx yields it. The
  first refusal drops the stream, so an over-budget result stops arriving rather
  than being read in full and rejected afterwards. Resident memory is one row
  plus one chunk, whatever `SQL_MAX_BYTES` is set to. `exec` reads no body at
  all beyond a bounded error snippet, and `scalar` stops after the first line.

  **"Admit each row as it arrives" is not on its own a bound**, and assuming it
  was would have shipped the same bug with better prose: a store answering with
  one enormous row completes no row, so nothing is ever admitted and the line
  buffer grows without a check. `Budget::check_pending` refuses a row that is
  still arriving once the bytes already read exceed what the budget could admit.

  **Streaming the read does not weaken the write.** Writing rows out as they are
  admitted would leave a truncated artifact behind on a refusal — "cleaned up
  afterwards", which is exactly what this contract rejects. So `rows` mode
  writes to a hidden sibling temp file and renames it into place only once the
  whole result has been admitted; every error path, a budget refusal included,
  drops the temp file instead. An over-budget statement still leaves nothing at
  the artifact path.

  The cost is `reqwest`'s `stream` feature and a `futures-util` dependency. The
  lockfile is refreshed in the same commit, and the churn is one line: the
  `futures-util` edge on this crate. `stream` pulls `tokio-util`, which the lock
  already carried from another member's reqwest features, and `futures-util`
  takes the default feature set `dagron-executor` already resolved. Both are
  well under the declared 1.88 MSRV, so the `--locked` legs — build, test,
  clippy in both feature worlds, and the MSRV check — all still hold.

  **Measured against live stores**, which is what the finding was deferred for.
  A 50k-row / 206 MB result written to an artifact: peak RSS **415 MB → 11.9 MB**
  on the Postgres wire, **405 MB → 13.6 MB** over HTTP. Refusing a 50-row budget
  on that same table cost 219 MB and read the whole result first; it now costs
  11.5 MB and 0.10 s. Against an *unbounded* `SELECT *` over HTTP the old client
  was still reading — RSS climbing, refusal unprintable — 25 s in; the streaming
  client exits in 0.33 s with `SQL_MAX_ROWS (50)` named, having pulled one chunk
  before hanging up on the store. In every refusal the artifact directory is left
  empty: no result, and no partial file.

  Raised on review of this release and deferred there, because a dependency
  change could not be integration-tested against a live store in that session.

- **The lease never bounded concurrent execution under `kubernetes` or `docker`.**
  `ARCHITECTURE.md` claimed it did. What a lease actually bounds is who may mutate
  `task_runs` — and a pod or container outlives the scheduler that created it.
  Both backends named their workload `sched-<random uuid>` and kept that name
  **only on the stack of the `execute()` call that made it**, so:

  - an expired lease let a second scheduler create a *second* pod running the
    same command beside the first, with nothing connecting either to the task; and
  - a scheduler crash left its pod unreachable forever — no label, no derivation,
    nothing to select on. It ran to completion, or indefinitely if the command
    never exits, and no sweep could have found it.

  `ExecContext` now carries a `TaskIdentity` (`task_id`, `run_id`, `attempt`),
  both backends label every workload with it (`dagron.dev/task-id`,
  `dagron.dev/run-id`, `dagron.dev/attempt`, `dagron.dev/managed-by`), and **a
  dispatch deletes any workload for the same task from an earlier attempt before
  creating its own**. A reclaim no longer doubles the work.

  The identity validates rather than sanitises: a task id that will not express
  as a Kubernetes label contributes *no* labels instead of a partial set. Two
  different ids must never collapse onto one label, because a collision there
  means one task's reaper deleting another task's live pod — and a workload
  carrying `managed-by` with no `task-id` is exactly the shape a sweep would
  match and misread. dagron's own ids are hyphenated v4 UUIDs and pass unchanged.

  Reaping is **best-effort by design**: a transient apiserver or daemon error
  logs and proceeds to dispatch, because refusing to run would turn a cleanup
  failure into an outage, and the state it leaves is no worse than the behaviour
  that preceded this.

  **Still open, and stated rather than implied:** deletion is requested, not
  instantaneous, so a pod inside its termination grace period briefly overlaps
  its successor; and a task never re-dispatched — its run cancelled while a
  scheduler was dying — still leaves a labelled workload that nothing sweeps.
  The labels are what make such a workload findable at all. A fleet-wide sweep
  is follow-on work and needs an installation-scoping label first, or it would
  delete another dagron install's pods in a shared namespace.

- **The `kubernetes` feature world was covered by no CI leg at all** — not
  clippy, not test, not MSRV — while being one of the three ways a task actually
  runs. Found while adding the labels above, which is to say: a change to that
  backend could not have been caught by anything. All three legs added, the same
  treatment the step crates' feature worlds got and for the same reason.

- **`SQL_MODE=exec` was documented as printing a row count. It prints nothing.**
  Both transports return after a `tracing::info!` — and they do not even log the
  same thing, since the HTTP path has no count to report. A log field is not
  stdout and not a cross-transport contract, so a workflow gating on it would
  have been gating on nothing. `scalar` is the mode that produces a value.

  While correcting that: `scalar` is bounded by `MAX_SCALAR_BYTES` (1 KiB), not
  by `SQL_MAX_BYTES` (8 MiB) — the tighter limit applies precisely because it is
  the one mode that writes to the uncapped output column on purpose. The docs
  said "byte-bounded" without saying which bound, which invites the wrong one.

- **The teardown claim was not exclusive on Postgres.** `claim_due_external_cancels`
  read its candidate rows, stamped `next_poll_at` unguarded, and returned *every*
  row it read. Under READ COMMITTED two schedulers see the same due rows, the
  second `UPDATE` waits for the first and then overwrites it, and both replicas
  return the row — so both send a cancel for the same remote job and both spend
  an `external_cancel_attempts` on it, burning the give-up budget at N× the rate.
  The SQLite twin survives the identical code only because SQLite serialises
  writers through one connection.

  It now CAS-guards each stamp on the `next_poll_at` the sweep observed and keeps
  only the winners — the same shape `claim_due_external_polls` twenty lines above
  already used. The poll path got this right and the cancel path, written in the
  same release, did not.

- **`defer.http` could be steered at a private address two ways.** These requests
  carry the task's own `Authorization` header, so both matter:
  - The client installed `GuardedResolver` but never called `.no_proxy()`. With
    a proxy configured, reqwest resolves the *proxy* host — which passes the
    filter — and asks it to connect to the private address. `wait_url`'s client
    has called `.no_proxy()` since it was written.
  - An IP-literal URL never reaches a resolver at all, so `GuardedResolver` never
    saw `http://169.254.169.254/…`. It now rejects blocked literals before the
    request, reusing `wait_url`'s already-tested `literal_host_blocked` rather
    than a second copy of the range list.

- **`SQL_ENGINE=trino` was a name without its protocol.** It selected the HTTP
  transport, which is a ClickHouse client: it appended `FORMAT JSONEachRow` (which
  Trino rejects), sent `X-ClickHouse-User`, never followed `nextUri` — Trino's
  protocol *requires* looping until that field is absent — and in `exec` mode
  returned success on any 2xx without reading `QueryResults.error`, so a queued
  query reported success having returned nothing. Trino is removed from `Engine`
  until it has a transport of its own; the unknown-engine error names it
  explicitly so the removal reads as deliberate.

- **`SPARK_WAIT=inline` reported completion it had not waited for.** It only
  warned, then took the same submission path — so on `k8s` and `rest`, which
  submit and return, the task succeeded the moment the job was *accepted* and
  released every dependent against work that had not run. It is now refused on
  those two backends (a warning is not a guard) and stays legal on
  `spark-submit`, where `Command::output()` genuinely blocks.

- **A deferred `spark-submit` blocked for the whole job.** YARN and Kubernetes
  cluster mode make `spark-submit` wait for the remote application, so the
  submit process sat there for hours and `timeout_secs` — which bounds the
  submit, not the job — killed the task before the handle was ever printed. A
  deferred submit now sets `spark.yarn.submit.waitAppCompletion=false` and
  `spark.kubernetes.submission.waitAppCompletion=false`, before `SPARK_CONF_*`
  so an operator can still override it.

- **A `%` in `SQL_PASSWORD` authenticated as the wrong password.** The encoder
  escaped `@ : / ? #` but not `%` — the escape character itself — so `pa%73s`
  spliced in verbatim, the DSN parser decoded `%73` back to `s`, and sqlx
  connected as `pass`. An auth failure with nothing in the message to explain it.

- **The MSRV job compiled neither step crate's published feature world.**
  `--workspace` includes both, but resolver 2 does not activate a feature across
  packages and both declare `default = []` — so Rust 1.88 checked neither the
  Kubernetes client nor the sqlx wire drivers, which is exactly what the images
  are built from. A dependency could have raised the effective MSRV for a
  published image with every required job green.

- **Two step images shipped with blank Docker Hub pages, and a third's overview
  never published.** `docker.yml` builds and pushes `dagron-step-spark` and
  `dagron-step-sql`, but neither had an overview in `docs/dockerhub/` nor a row
  in its index — so both repos would go public describing nothing. Worse,
  `dagron-step-mcp.md` *existed* and still never reached the Hub: the publisher
  script's `MAPPING` listed only the six service images, so a file nobody
  publishes is a file nobody reads. All three are now mapped, with short blurbs
  that say "task binary, COPY it into your image" — which is the decision
  someone is actually making when they look at the listing.

  Both step crates also gained the README every other crate in the workspace
  has, and `defer:` — this release's headline task-level field — is now in
  [`docs/CONFIG.md`](docs/CONFIG.md)'s field reference rather than only in
  `EXTERNAL_JOBS.md`. That reference is where someone looks up "what can a task
  declare", and it listed `wait:`, `cache:` and `produces:` while the new block
  was absent.

- **`dagron-step-sql` claimed two capabilities it does not have.** Its module
  doc opened with "four things [a plain `command:`] cannot do" and listed a
  cancel hook (`KILL QUERY <query_id>`) and a defer handle among them. Neither
  is built: nothing in the crate captures a query id, and the step never prints
  `dagron::handle=`, so `defer:` on a `dagron-step-sql` task would park on
  nothing and a 40-minute query holds its worker for 40 minutes. The doc now
  claims the two that are real — the fetch-side result budget and one image
  instead of N vendor CLIs — and names the other two as unbuilt, in the same
  terms the rest of the product uses for a gap.

- **The quota seam only ever saw half the engine.** `Meter::on_task_completed`
  is the hook an alternate build accounts usage and enforces limits on. Three
  call sites reached it, all on the worker-result path — and a task that *parks*
  holds no worker and resolves in a reconcile sweep, so it never traverses that
  path at all. Every `wait` sensor, `wait.url` probe, `wait.dataset` sensor,
  sub-workflow trigger, approval-gate timeout and deferred `defer:` job, plus
  every memoization cache hit, reached a terminal state while being metered
  **zero** times. A limit like `max_tasks_per_day` silently stopped bounding all
  of them, which is worse than having no limit, because it reads as enforced.

  `scheduler_tasks_succeeded_total` and `scheduler_tasks_failed_total`
  under-reported by exactly the same set, for exactly the same reason — a
  workflow built from sensors and sub-workflows reported zero completed tasks on
  `/metrics` while running perfectly.

  Both now fire on all **22** paths that land a task in `succeeded` or `failed`,
  each honouring its mutation's guard so a stale fence — which changed no row —
  is not counted.

  The fix is one internal helper, `task_finished`, rather than twenty-two added
  calls, because "remember to meter" is what failed the first time. The counters
  and the hook are now unreachable except through it, and a crate test walks
  `dagron-engine/src` asserting so: a new park shape cannot bump a counter
  without also metering, because a bare bump no longer passes the test suite.
  (Verified by injecting one, the way the next park shape would.) The helper's
  own behaviour is asserted separately — a conformance scan over an empty
  function would pass while metering nothing.

  **Cancellation is deliberately still not reported.** `cancel_run` and
  gang-sibling cancellation terminalize rows as `cancelled`, which is neither
  arm of a `bool`; calling one a failure would spend quota a tenant never used
  and inflate the failure rate. Counting cancellations needs its own signal.

### Changed
- **Environment secrets reach Kubernetes task pods by reference, not as a literal in the pod
  spec.** They were encrypted at rest and never returned by the API, but `build_pod` put the
  decrypted value in `env[].value`, readable by anyone able to `get`/`describe` pods (and in
  API-server audit logs). A task with secret-sourced variables now gets a per-task Secret
  (`<pod>-env`) referenced with `secretKeyRef`, owned by the pod so Kubernetes deletes it
  with the pod. **Behaviour change:** the engine's Role needs `secrets` `create`/`patch`/
  `delete` (the chart, the load-test RBAC and the workshop lab are updated; a hand-written
  Role must be too). If the Secret cannot be created the task fails and names the missing
  permission; it never falls back to plaintext. `DAGRON_TASK_SECRET_ENV=inline` restores the
  old behaviour. Anyone who can `get secrets` in the namespace can still read the value, and
  the Docker and local executors still expose it (`docker inspect`, the process
  environment). `loadtest/deploy/security-e2e/secret-refs.sh` (12 cases) is the proof.
- **Multi-dataset fan-in is open.** `on_datasets: [a, b, …]` with
  `datasets_mode: any|all` used to be refused at validation, and the engine's
  subscription sweep skipped such specs rather than subscribing to a subset.
  Both are gone. Nothing was implemented to do it: `sync_dataset_triggers` and
  `claim_due_dataset_triggers` have handled `mode="all"` on SQLite and Postgres
  all along — the gate was a refusal with a working implementation behind it.

  It had to go because **the fallback it recommended is wrong**, not merely
  lesser. The error told you to "keep exactly one `on_datasets` entry … or split
  consumers into one workflow per upstream dataset", which in practice means
  trigger on one upstream and put a `wait: { dataset: … }` sensor on the other.
  A sensor stamps its cursor when the task **parks** (`park_wait_dataset`) and
  resolves only on `id > cursor`; a subscription stamps its cursor once at
  registration (`sync_dataset_triggers`) and advances it only when a fire
  consumes it. So in the canonical nightly mart with two upstreams arriving in
  no fixed order: the second one to land fires the run, the run reaches the
  sensor, and the sensor's cursor is now *past* the first upstream's update.
  It waits for tomorrow's — hanging to `run_timeout_secs`, and tripping
  `run.deadline_exceeded` first if the workflow declares a `deadline:`.
  `datasets_mode: all` has no such race: it
  fires exactly once both upstreams are fresh, consuming both cursors in the
  one claim, so neither update is replayed and neither is lost.

  A gate whose documented alternative loses data is a gate on correctness, and
  correctness is not where the line goes. What stays gated on the dataset page
  is the org-scale surface that *does* have a working fallback: external dataset
  events (`POST /datasets/events` → `403`; record the dataset from a small
  `produces:` task instead), freshness SLAs, partitions, and the lineage graph
  UI.

  Covered by a test that walks the whole shape — all-of stays silent on one
  upstream, fires once on the second, advances both cursors, and is empty to the
  next sweeper — plus the validation test, which is deliberately **not**
  `cfg`-split any more: an open build and an enterprise build must now agree.
  `dagron-api`'s console mirror is held to the same assertion.


## [0.9.2] - 2026-09-14

### Added
- **A local pool can stop trusting its specs.** An `EXECUTOR=local` pool runs
  its tasks as subprocesses of the engine — that is why it exists, and it is
  also the problem: `Command` inherits the parent environment, and `command:`
  comes from the workflow. So on such a pool a spec author could run any program
  the pool's user can run and read every variable the pool process holds. That
  was measured against the executor, not reasoned about: `["sh","-c","echo
  $DATABASE_URL"]` returns the datastore DSN, a task's `env:` entry replaces an
  engine-set variable, and `["id"]` returns whatever the pool runs as.

  Two settings close it, and **both are opt-in — unset, nothing changes at
  all**, which matters because every self-host and every `pulse` pool shares
  this code.

  `DAGRON_LOCAL_COMMAND_ALLOWLIST` names the programs a task may run. A pool is
  an engine process with `RUNNER_CLASSES` set, so a process-wide setting is
  per-pool. Matching is on the **resolved file**, not the word the task wrote,
  because a comparison on `command[0]` is defeated three ways — a planted
  program on a task-supplied `PATH`, an absolute path to a different file, and a
  `./` spelling all reached a decoy in testing. The permitted program is then
  spawned by its canonical path, so the exec does no lookup of its own and there
  is no window between the check and the exec.

  `DAGRON_LOCAL_ENV_PASSTHROUGH` names which of the pool's variables reach a
  task; everything else is cleared. A small baseline (`PATH`, `HOME`, TLS trust,
  proxy settings) always comes through, because `env_clear()` without it
  produces a task that fails with "No such file or directory" and an operator
  who turns the feature off. A task whose `env:` names one of the operator's
  variables is **refused** rather than quietly overruling the deployment.

  A refusal is a task failure classified as a configuration fault, so it costs
  one attempt rather than the task's whole retry budget — a refusal is
  deterministic, and three more attempts with backoff would produce three more
  identical errors and a slower answer.

  What an allowlist does not buy, and should not be sold as: it bounds *which
  program* runs, not what that program does.

- **`notify.git.description`: say what the run produced, not just that it
  passed.** A commit status has always read `dagron run succeeded`. That is the
  right default and it stays the default — every workflow with a `notify.git`
  block keeps the check it had. But when a run *produces* something the reviewer
  is looking for, the check is the place to say so, and there was no way to.

  A `notify.git` block can now carry a `description:`, `{{ param }}`-templated
  like every other field in it. Four `run.*` names also resolve there, because
  the things an author most wants in a check are the ones only the engine knows:
  `{{ run.id }}` (so `target_url` can link to a run without the caller passing an
  id it cannot know), `{{ run.workflow }}`, `{{ run.status }}`, and
  `{{ run.images }}` — the distinct task images the spec declares, in
  first-appearance order. The dot keeps them clear of ordinary parameter names,
  and where one collides the engine's value wins, so a check cannot be made to
  report whatever a caller put in a parameter.

  `{{ run.images }}` is what makes this worth having for a workflow whose image
  is built rather than pulled: `description: "built {{ run.images }}"` turns the
  check on the pull request that changed the image's definition into the answer
  to *which image came out of it*. Nothing has to be reported back from the run —
  a content-addressed reference is in the spec before the image exists.

  GitHub rejects a status whose description exceeds 140 characters, and a list of
  image references reaches that easily, so the text is cut with an ellipsis
  rather than the whole status being lost to a validation error. A template that
  resolved to nothing falls back to the state's wording: a blank check reads as a
  broken integration rather than a passing run.

### Fixed
- **Two real knobs were being reported as typos at every boot.** The
  configuration scan warns about a variable that looks like a dagron knob but
  is not one — and `DAGRON_MAX_TASK_TIMEOUT_SECS`, documented in
  `docs/CONFIG.md` and read by every executor, was missing from the registry
  the scan checks against. Anything setting it (the cloud provisioner stamps it
  onto every workspace engine) logged a warning for a knob that is real. The
  `DAGRON_BUILD_*` family drew the same warning for a dozen variables a build
  pool's own chart sets; those belong to a different binary and can never be
  registered here, so the prefix is now known-foreign. Over-warning is what
  trains an operator to ignore the scan, which is the failure the scan exists to
  prevent. A test now pins the registry against the constants other crates
  export, and the registry's sortedness against its own stated invariant.

### Added
- **The Python SDK covers the 0.9 API and the whole `TaskSpec` (`dagron-sdk`
  0.4.0 → 0.9.0).** It had been pinned at the 0.4 gateway while the API grew
  twenty-odd routes, so anything added since — access tokens, environments and
  their secrets, the archive, triage, datasets, workflow versions and lifecycle,
  signed bundles, artifacts, the settings pages, `search`, `readyz` — was
  reachable from the console and from `curl`, but not from Python. The version
  number is the API version it covers, the convention set when the SDKs were
  first lined up with the gateway; every change here is additive.

  The `Dag` builder had drifted the same way, and worse: it emitted 12 of the
  engine's ~35 task fields, so a DAG authored in Python could not express a
  fan-out, a sensor, an approval gate, a sub-workflow trigger, a pool, a
  priority, a cache, a `produces:`, a gang, a hook, a trigger rule or a retry
  budget — and nothing said so. `task()` now takes all of them, `Dag(...)` takes
  the spec-level block (`parameters`, `tags`, `environment`, `task_defaults`,
  `result_from`, `budget`, `deadline`, `notify`, `on_datasets`, …), and
  `Dag.template()` declares reusable sub-DAGs. `approval()`, `sensor()` and
  `trigger()` are shorthands for the three command-less kinds.

  Client-side validation grew to match, mirroring `validate_graph` field for
  field — one kind per task, `type: workflow` names a target, `arguments` have a
  callee, trigger rules and runner classes are well-formed, a `when:` depends on
  the task whose output it reads, `result_from` names a real task — and stops
  where the server stops, so a dependency that only resolves after expansion is
  still left to the engine rather than 400'd early by the client.

  Two calls are worth naming because they replace a loop: `wait_run` long-polls
  `GET /api/runs/{id}/wait` (the engine's own event feed, no poll interval to
  tune, and the failure summary comes back with it), and `stream_events` reads
  the account-wide SSE feed instead of polling every list. `Client.from_env()`
  plus `create_token` mean a CI job stores a revocable token rather than the
  password that mints one. `ROADMAP.md` carries the endpoint-by-endpoint matrix.
- **The TypeScript SDK catches up with it (`@dagron/sdk` 0.4.0 → 0.9.0).** The two
  SDKs have always been released together and are documented as covering the same
  ground, so bringing Python forward alone left that claim false. `Client` now
  mirrors the Python client **method for method** — 88 of them, camelCased — and
  a test asserts nothing more: the two surfaces are compared name by name.

  Its `Dag` had drifted furthest of anything here. It emitted **four** task
  fields — `image`, `command`, `dependsOn`, `runnerClass` — so a DAG authored
  in TypeScript could not set a retry, a timeout, an env var, an `input` or a
  `workflow_ref`, never mind a sensor or a gate, and `toSpec()` checked only
  that dependencies resolved. It now takes the whole `TaskSpec`, declares
  templates, and runs the same `validate_graph` mirror the Python builder does.

  One deliberate behaviour change: an unknown key in an options bag now throws
  instead of being dropped. `{ retryDelay: 5 }` for `retryDelaySecs` used to
  produce a task with no retry delay and no complaint — the same silent-drop the
  SDK already refuses for log filters, and the reason an options bag is riskier
  than Python's keyword arguments.

  `index.d.ts` was rewritten alongside it (typed `TaskOptions`, `DagOptions`,
  `WaitSpec`, `RunResult`, `CreatedToken`, …) and now type-checks under
  `--strict`, as does a consumer program written against it. The package's
  `files` list has always named a `README.md` that did not exist; it does now.
  No `[Symbol.dispose]` to pair with Python's context manager: this package
  supports Node 18, where that symbol is `undefined` and a computed
  `[Symbol.dispose]()` defines a method named "undefined" instead of a disposer.
- **Review round: six SDK findings, and one the reviewer had backwards.** The
  worst was a race in both `waitRun`/`wait_run`: they long-poll the gateway but
  used the client's shared transport timeout, whose default (30 s) is exactly
  the server's default wait budget — so `waitRun(id)` was a coin flip and
  `waitRun(id, {timeoutSecs: 600})` aborted 570 seconds early, every time. Both
  now size a per-call transport budget off the server's *clamped* `[1, 600]`
  range plus a margin, widening only that one request; the shared `timeout` is
  never mutated, because concurrent calls share it.

  Reproducing that turned up a second defect the review missed: in Python a read
  that times out after the connection is established comes out of
  `http.client` as a bare socket timeout, which `URLError` never sees — so it
  escaped as `TimeoutError` rather than `DagronError`, breaking the client's
  one-exception-type promise on exactly the calls that wait longest.

  The rest: `iter_runs`/`iterRuns` now refuse the `limit`/`offset` they drive
  themselves (Python raised an opaque `TypeError` from inside the generator;
  TypeScript silently overwrote the caller's value); `Template.to_spec`/`toSpec`
  deep-copy, so reading a template directly no longer hands out the builder's
  own task list; a `repeat:` block is checked against the engine's key spelling
  at build time, since TypeScript's validation accepted a camelCased
  `maxIterations` that would have left the required wire field unset; and both
  package READMEs stopped implying `create_token` populates the environment
  `from_env` reads.

  The one turned around: the review asked for `listGitRepos(): Promise<Dict[]>`.
  `GET /api/git-repos` returns an *object* — `{repos, worker_online,
  credentials_configured}`, the flags riding along so the console cannot promise
  "Auto-sync ON" where nothing polls — so the TypeScript declaration was right
  and **Python's `-> List[Dict[str, Any]]` was the wrong one**. Fixed in Python,
  and the TypeScript declaration is now a named `GitRepoList` so the shape is
  answered by the type rather than guessed at.
- **A `Makefile`: every command the docs tell you to paste, as a target.**
  Getting a first run out of dagron meant transcribing a compose invocation from
  the README, a login-then-submit pair from `docs/HOWTO.md`, and a `curl` with a
  run id in the middle of it from the monitoring section — and the two submit
  paths are *different* (the engine's management API takes raw YAML on `:8787`
  with no auth; `dagron-api` wants a session and a JSON envelope on `:8080`),
  which is exactly the kind of distinction a copy-paste loses. `make` prints the
  grouped list; `make up`, `make dev`, `make submit DAG=…`, `make run-logs
  RUN=…`, `make backup`, `make pi-load PROFILE=ramp` and `make ci` run the
  documented command **unchanged**, so the shortcut never becomes a second way
  to operate dagron that the docs do not describe.

  `make ci` is the one worth knowing if you are changing code: it runs the two
  feature worlds and the `mqtt` one in CI's order, which a bare `cargo test`
  does not — `--workspace` cannot compile this tree at all, and the split is
  easy to get wrong by hand.

  Per-invocation arguments (`NAME`, `PROFILE`, `DIR`, `DB`, `LEVEL`, …) are
  assigned empty rather than with `?=`, because make imports the caller's
  environment and those names are common in it: with `?=`, a shell `NAME` leaks
  into `make runs` as a query filter, and a shell `DB` picks the database
  `make restore` overwrites. A command-line `make runs NAME=etl` still wins.

- **Two new SDK examples per language, for the surface the 0.9.0 bump just
  landed.** The full `TaskSpec` a `Dag` can now build had no example using any
  of it: `06_advanced_dag.py` / `02_advanced_dag.mjs` chain a fan-out
  (`with_items`), a sensor, an approval gate resolved programmatically, and a
  sub-workflow trigger in one run, plus a reference snippet for
  `pool`/`priority`/`gang`/`cache`/`produces` that isn't submitted (those
  route to engine resources — replicas, gang groups — that have to already
  exist on the deployment running it). `07_notify_and_automation.py` /
  `03_notify_and_automation.mjs` cover the other two: `create_token` /
  `from_env` for the token a CI job should hold instead of a password, and
  `notify.git.description` templated with `{{ run.* }}`. Both run to
  completion against the local compose stack with no extra setup — the
  commit-status update is best-effort and a documented no-op without
  `GITHUB_TOKEN`/`GITLAB_TOKEN` configured.

### Fixed
- **Both of `docs/DEAD_LETTERS.md`'s SDK snippets imported a class that does not
  exist.** They opened `from dagron import Dagron` / `import { Dagron } from
  "@dagron/sdk"` and constructed `Dagron(...)`; the client has always been
  `Client` in both SDKs, so the first line of the example a reader copies raised
  `ImportError` (or `undefined is not a constructor`). Both use `Client` now,
  and point at `from_env()` / `fromEnv()`, since the surrounding section is
  about a token.
- **`examples/sdk/typescript/01_quickstart.mjs` hand-rolled `fetch` calls the
  SDK has covered since 0.3.0**, under a comment stating "the TS SDK is
  build+submit only; poll by hand". It drives `Client` now, and blocks on
  `waitRun` instead of a 60-iteration poll loop.
- **`docs/CONFIG.md`'s fleet-link section no longer cites a document this
  repository does not carry.** It pointed at an install runbook that is not
  published here, which left a reader of that section with nowhere to go. What
  the section actually needed is written out in it now: the enrol call returns
  the credential once and stores nothing, the two variables have to be set where
  *both* the engine and the API read their environment, and unlinking is a
  revoke-and-restart rather than a call.
- **`EnrolResult` (`frontend/src/types/dagron.ts`) drops a field no build here
  can receive.** `POST /api/link/enrol` answers `403` without the `enterprise`
  feature, so the field only ever appeared in a response this build does not
  produce. `enrolUnit` is generic in its result now
  (`enrolUnit<R extends EnrolResult = EnrolResult>`), which lets a caller that
  knows a wider response shape ask for it without a cast; the default is
  unchanged.

## [0.9.1] - 2026-09-04

A release-pipeline fix. 0.9.0 mirrored and tagged cleanly but **published no
images and no chart**: the `dagron-frontend` build failed, and because the
manifest job needs the whole matrix, five good images stayed as untagged
digests and the Helm chart never went out.

### Fixed
- **`mancube/dagron-frontend` is discontinued; 0.8.1 was its last tag.** It was
  announced for removal in a later breaking release, and that promise cannot be kept: the change
  that moved the console into `dagron-api` also made this image unbuildable.

  `frontend/next.config.js` sets `output: "export"` so `dagron-api` can embed
  the console, and an export build emits `out/` — never the `.next/standalone`
  the Dockerfile copied. The build died on `"/app/.next/standalone": not found`.

  Repointing it at `out/` would not have saved it. `BASE = "/api"` in
  `src/lib/dagron-api.ts` is a *relative* path and the rewrite that used to
  answer it is gone (it buffered `text/event-stream` and broke live updates, as
  0.9.0 records). A console served on `:3000` by anything that is not
  `dagron-api` 404s every API call it makes. The image could have been made to
  build; it could not have been made to work.

  **A tag that builds into a dead console is worse than no tag**, so the matrix
  entry, `frontend/Dockerfile` and the `frontend` compose service are gone one
  minor early. `frontend/` itself stays — it is what `dagron-api` builds. The
  chart's `frontend.image` is **pinned to 0.8.1** and no longer moves with the
  chart version, so `frontend.enabled=true` still pulls something that exists
  instead of an ImagePullBackOff on a tag that was never pushed.

- **The console inside `dagron-api` was stamped `dev`.** `APP_VERSION` is what
  puts a version in the sidebar brand, and the release workflow passed it only
  to `dagron-frontend` — correct while that image built the console, wrong from
  the moment `dagron-api` took over. Every published 0.9.0 API image would have
  shown `dev`, which is the exact failure the version plumbing exists to
  prevent: it says `dev` when it does not know, and it did not know. The build
  arg now follows the console to `dagron-api`, in the release workflow and in
  `compose.yaml`.

## [0.9.0] - 2026-09-04

The release the last three months of `main` had already become. Everything the
README marked **0.9** ships here: the console moves inside `dagron-api` (one
port, one origin, no proxy and no second image), the engine binary carries a
console of its own for the single-binary case, and `SOURCE=dir` turns a watched
directory into a live inbox.

Three themes beyond that. The **agent surface** stopped being a demo — the MCP
server went from 9 tools to 42 and gained a read-only mode, so an agent can
author, run, recover and inspect a workflow without a human dropping to `curl`.
The **constrained host** became a first-class target: an `edge` profile, a
pressure gate and a free-disk floor, MQTT ingestion, signed workflow bundles,
a tiered artifact store with an uplink budget, clock confidence on every run,
and armv7 binaries. And **history** got an exit: archive-before-purge is now
reachable per run, not only on the retention sweep's schedule.

Upgrading from 0.8.x is a restart. The schema migrates forward automatically
and there are no down migrations, so back up first
([`docs/OPERATIONS.md`](docs/OPERATIONS.md#backup--restore--what-is-the-state)).
The one thing to read before you upgrade is **Deprecated** below: if you deploy
`mancube/dagron-frontend`, stop — the console it served now comes from
`dagron-api` on the same port.

### Added
- **`dagron-mcp` closes the agent-API gap: 9 tools → 42.** The MCP surface could
  submit ad-hoc YAML and watch it fail, and not much else — so an agent hit
  `curl` for everything the bar actually asks of it. It can now *author* (register,
  update, pause, retire and run a **named** workflow, which is what makes a
  `type: workflow` parent/child DAG reachable at all), *wait*
  (`dagron_wait_run` long-polls to terminal instead of costing a round trip and a
  slice of context window per poll tick), *recover* (rerun, resubmit, retry a
  task, clear a task and its downstream cone, redrive or discard a dead letter),
  *resolve gates* (`dagron_list_approvals` + approve/reject — before this, a DAG
  with a human-in-the-middle gate was one an MCP agent could start and never
  finish), *read files* (`dagron_get_artifact`, the missing half of any
  file-producing DAG), and *write down what it concluded* (`dagron_triage_run`,
  the one write here a reviewer reads rather than reruns). Three shipped tools
  also caught up with routes that had outgrown them: `dagron_submit_run` now
  sends `parameters` and an `Idempotency-Key` (the client with the least ability
  to reason about its own retries was the one denied the safety net), and
  `dagron_list_runs` takes `status`/`name`/`trigger`/`limit`/`offset` instead of
  listing everything unfiltered. Plus visibility: `dagron_search` (every id-taking
  tool assumed the agent already held a uuid), `dagron_get_run_graph`,
  `dagron_get_run_spec`, `dagron_get_health`, `dagron_get_metrics_timeseries`,
  the dataset registry and lineage ledger, the archive, and workflow run/version
  history. That is 42 of `dagron-api`'s 84 method+path pairs; the other 42 —
  credentials, instance settings, GitOps wiring, schedules and backfills — stay
  declined or deferred with a row each in
  [docs/MCP.md](docs/MCP.md#full-coverage-matrix), so "no tool" is a recorded
  decision rather than an oversight.
- **`DAGRON_MCP_READONLY=1` — the write half, off.** Those additions turn this
  server from mostly-read into read-write against a live scheduler, so the
  safe-by-default posture it had when there was little to write is now a
  deliberate switch. It hides
  all eighteen mutating tools from `tools/list` *and* refuses them in the
  dispatcher: hidden because a tool an agent can see is a tool it will plan
  around, refused because a composing server (a downstream build may compose its
  catalogue on top of this one) must not be able to smuggle a write past it. The
  24 read tools stay, which is still enough to diagnose a failing cluster end to
  end.
- **Archive one run on demand, from the console.** Archiving used to be
  something only time did to you: the retention GC swept terminal runs past
  `GC_RETENTION_SECS` into the sink and purged them, and there was no way to
  say "this one, now". `POST /api/runs/{id}/archive` is that sweep aimed at a
  single run, and the console puts an **Archive** button on a terminal run's
  detail page for admins. It is deliberately the *same* operation, not a
  gentler one: archive-then-purge, in the sweep's fail-closed order — write the
  `dagron.run-archive.v1` document, verify it landed, index it, and only then
  delete the run from the hot store. The run moves out of Runs and into the
  Archive tab, and a failure at any step leaves it exactly where it was.
  Refusals are specific because the fixes differ: `409` for a non-terminal run
  (archiving one the scheduler is still driving would purge live state), and
  **`501` when no sink is configured on dagron-api** — without somewhere to
  archive *to*, the purge would be a delete wearing a kinder word, so it does
  not happen. Admin-only: retention is an instance-wide policy, and pulling
  runs out of the hot store ahead of it is the same authority exercised one run
  at a time. The fsync chain the local sink depends on moved to
  `dagron_core::archive` — two processes now treat a successful write as purge
  permission, and that contract needs one implementation, not two.
- **The transactional outbox can now be treated as a bounded spool.** The outbox
  was written for a delivery worker with a reachable endpoint: claim, post,
  mark. A scheduler on a metered or intermittent link fills it far faster than it
  drains it, and the disk it fills is usually a flash card — so `dagron-core`
  gained the measurements and the release valve that shape needs.
  `outbox_pending_bytes` and `outbox_oldest_pending_age_secs` are the two gauges
  a budget is set against; `shed_outbox(pool, max_bytes, max_age_secs, policy)`
  parks what no longer fits as `status = 'shed'` — oldest first, or largest
  first when one oversized payload is the problem — keeping the payload and
  recording which budget dropped it in `last_error`, so
  `SELECT COUNT(*), last_error FROM event_outbox WHERE status = 'shed' GROUP BY
  last_error` answers "what did this disk cost me". Shedding is deliberate data
  loss: the alternative is a spool that grows until the datastore stops
  accepting writes, which loses the run state too. Nothing sheds by default —
  both budgets are parameters, and `0` disables either. Alongside them,
  `claim_outbox_batch_for_uplink` returns each claimed row with its `created_at`
  (a receiver a week later needs to know when the *machine* produced the event,
  not when the link came back) and `latest_run_for_spec` resolves the newest run
  created from given spec bytes since a moment — the bridge for a submitter that
  has to report which run its submission became. Both backends, identical
  signatures; `event_outbox.status` has no `CHECK` constraint, so no migration.
- **`SOURCE=mqtt` — the open MQTT ingestion source** (`--features mqtt`,
  [docs/STREAMING.md](docs/STREAMING.md)). A plant floor, a gateway or a robot
  publishes a workflow spec to a topic and the unit runs it. Subscribes with
  manual acks; set a stable `MQTT_CLIENT_ID` to resume a **persistent session**,
  so QoS-1 messages published while the unit was offline are redelivered rather
  than lost — which is the case that matters on a duty-cycled link. Without one
  the client id is generated and the session is clean, because a broker cannot
  queue for a subscriber whose name changes at every restart. Delivery is
  at-least-once by default; `MQTT_POSITION_FIELD` names a per-topic monotonic id
  in the payload (Sparkplug's `seq`) and turns it into exactly-once by
  committing that cursor with the run it creates and acking, never re-running,
  anything at or below it. A broker that is down is a retry, never an exit; a
  dead-letter topic inside your own subscription is a startup error rather than
  an infinite loop. Managed, credentialed protocol sources are not in this
  build.
- **Constrained-host admission: a pressure gate and a free-disk floor.**
  `DAGRON_PRESSURE_FILE` lets a thermal, battery or maintenance daemon stop the
  engine claiming new tasks by touching a file — in-flight work finishes and
  every other loop keeps running. `DAGRON_MIN_FREE_BYTES` refuses to create new
  runs when the datastore's filesystem is nearly full (`507` + `Retry-After` on
  the ops API, a throttle rather than a dead letter at the source), because a
  half-written SQLite file on a full flash device is worse than a rejected run.
  Both are off by default; the new **`edge` profile** turns the floor on with
  the rest of the constrained-host tuning
  ([docs/EDGE_PROFILE.md](docs/EDGE_PROFILE.md)).
- **Clock confidence on every run** — `synced` / `drifted` / `unknown`, with the
  measured offset and what produced the verdict, on `GET /runs/{id}` and both
  `dagron-api` run reads. A unit that boots with no network and no clock still
  keeps its schedule, but its timestamps are not evidence, and until now nothing
  said so. The engine checks the wall clock against the monotonic clock, notices
  a step, and marks runs it is already executing; a boot whose clock predates the
  newest row in its own datastore is `drifted` on the spot. Recovery is never
  gated on the verdict — a drifted unit still reclaims its leases.
- **Tiered artifact store: capture locally, uplink on a budget**
  (`DAGRON_ARTIFACT_TIER`). Every write lands on local disk first and moves to
  the cloud tier later, under `DAGRON_ARTIFACT_UPLINK_BYTES_PER_DAY`, with a
  durable ledger so a restart resumes rather than re-uploads and identical bytes
  transfer once. Reads fall back to the remote tier when the local copy is gone,
  so a unit can free space without losing access to what it captured.
  `POST /api/artifacts/sync` drains on demand for a unit that has just docked.
- **Signed workflow bundles** ([docs/BUNDLES.md](docs/BUNDLES.md)). A bundle is a
  manifest of content-addressed specs with a detached ed25519 signature over the
  exact manifest bytes. Verification is all-or-nothing and **fails closed**: an
  unknown format, a missing or extra file, a hash mismatch or a signature that
  matches none of `DAGRON_BUNDLE_PUBKEYS` is an error, and there is no unsigned
  path. The GitOps worker applies a signed repository in one transaction
  (`DAGRON_BUNDLE_REQUIRE=1` refuses unsigned ones outright) and
  `POST /api/workflows/bundle` does the same over HTTP, stamping each
  `workflow_versions` row with the bundle's provenance. Verification ships in
  every build — the alternative would be an open build whose default is to
  execute unsigned remote definitions. Staged rollout across a fleet is not in
  this build.
- **`armv7-unknown-linux-musleabihf` release binaries** for the long tail of
  32-bit gateways already in the field, cross-compiled on the release runner.
- **Fault attribution — what a failure *was*, and whether another attempt is
  worth anything** ([docs/HPC_AUTOPSY.md](docs/HPC_AUTOPSY.md)). `status =
  'failed'` says the engine gave up; it has never been able to say what broke,
  so every consumer of a failure re-derived the cause from logs that are already
  rotated. On a GPU fleet that gap is the cost model: an XID 79 and a NaN loss
  are both `failed`, and retrying the second like the first burns another N
  GPU-hours to reach the same answer.
  - **The taxonomy** (`dagron_core::fault`): 23 classes, each carrying a
    *disposition* (infrastructure / application / platform / unknown), a
    *precedence* (root-cause / ambiguous / symptom), whether the node should be
    drained, and a default attempt budget — plus the NVIDIA XID table that
    splits kernel-side faults (13/31/43/45 — the job's pointer bug) from device
    faults (48/63/64/79/94/95 — the board). Getting that backwards either
    drains healthy nodes or retries a dead GPU forever. An uncorroborated NCCL
    collective timeout is classed `nccl-timeout` with an **unknown**
    disposition and **symptom** precedence, and is never promoted to a cause by
    text alone: it is printed by every rank that was *waiting* — the healthy
    ones — and the rank that died often prints nothing at all.
  - **`retry_budgets:`** on a task and in `task_defaults:` — per-class attempt
    budgets, resolved most-specific-first: the task's own entry for the class
    that occurred, then the class's disposition default (infra 5, platform 3,
    application 1), then `max_attempts`. **This moves attempt counts on
    workflows nobody edited, and is meant to.** Failures classify from their own
    output with no opt-in, so a task that never mentions `retry_budgets` still
    takes the disposition default whenever its failure matches a class:
    `max_attempts: 3` becomes 5 for a `gpu-ecc` and 1 for a `nan-loss`.
    `max_attempts` is the fallback, not a ceiling over the classified cases,
    which is the point of the feature (a dead GPU deserves another node; a NaN
    loss does not deserve two more hours). It is what you get in **two** cases:
    a failure that classified as nothing, and one whose class carries the
    *unknown* disposition — `nccl-timeout` and `unknown`, whose default budget
    is deliberately `0` so they decline to have an opinion. That matters most
    for `nccl-timeout`, the class a distributed failure reaches for first: an
    uncorroborated collective timeout is a symptom, so it sets no policy of its
    own and the task's `max_attempts` still decides. Write a class into
    `retry_budgets` to pin it to a number you choose; the task's own entry beats
    the default.
    The `retry_on_timeout: false` carve-out (#24) still applies first and still
    wins. A misspelled class is a parse error rather than a silently-inert
    policy — this is the code path that decides whether to spend another
    thousand GPU-hours. `task_defaults` merges per class, so a task overriding
    `nan-loss` still inherits the workflow's `gpu-ecc`.
  - **The durable record**: migrations 040 (SQLite) / 050-051 (Postgres) add
    `fault_class`, `fault_detail` and `fault_confidence` to `task_runs`,
    written fence-guarded on the retry path (`db::record_task_fault`) so a
    reclaimed attempt cannot relabel the newer one's failure. `NULL` (nothing
    looked) stays distinguishable from `'unknown'` (looked, could not tell).
    Read back via `db::get_task_fault` / `db::fault_class_counts`.
  - **Metrics**: `scheduler_task_faults_total{class,disposition}` and the
    `scheduler_task_faults_by_disposition_total` roll-up — fixed-cardinality by
    construction (the taxonomy is closed), with every class emitted at zero so
    `increase()` over a window predating the first fault still works.
  - **`dagron-autopsy`** (`crates/dagron-autopsy`, binary `dagron-autopsy`) - a
    job autopsy that **schedules nothing**. It sits beside an existing Slurm or
    Kubernetes cluster, joins `sacct` + DCGM + NCCL logs + InfiniBand counters
    against a job's node set and time window, and emits a fault-attributed job
    record ("GPU 3 on node-47 threw XID 79 at T-90s; drain the node, retry
    elsewhere") as operator text or JSON. Every input is a file, so it runs
    against last week's job with no database, daemon or agent. The join filters
    by the job's node set (else one broken node is blamed for every job on the
    cluster), extends the window past the job's end (the driver's XID lands in
    syslog after the process is gone), ranks by precedence *before* time (the
    earliest signal is almost always the symptom), and - with no device
    evidence - reads the rank topology: everyone stuck and nobody missing is a
    `deadlock`, everyone-but-a-few stuck names those few as `straggler-rank`,
    and a partition is neither. Fabric counters need a before/after pair
    because every healthy port has large lifetime error counters; only a rise
    inside the window is evidence. A verdict below `medium` confidence reports
    rather than acts, missing sources are confessed in the record, and no
    evidence at all is reported as *unattributed*, never as fine.

- **An operator console inside the engine binary** — `dagron` now serves a UI at
  `/` on its ops API (`API_ADDR`), embedded with `include_str!` exactly as `/docs`
  already embeds Swagger. **19 KB, no Node, no second process, no network fetch**,
  so it works air-gapped and against SQLite: the deployment the full console cannot
  reach, because that one needs `dagron-api` and Postgres.

  Scope is what the ops API can back, and no more: the runs list with a status
  filter and auto-refresh, a run's tasks with their attempt and timings, whole-run
  and per-task logs, cancel and rerun, per-task clear/approve/reject, dead letters
  with redrive, plus the effective configuration and the metrics exposition. Every
  API path is unchanged — the console only claims `/` and `/console`, so existing
  clients see no difference.

  It is deliberately **not** the `dagron-api` console trimmed down. That app speaks
  a different dialect (`/api/runs` returns a bare `RunSummary[]` carrying
  `definition_id`, `trigger_kind` and triage fields; this API returns
  `{"runs": [...]}`) and assumes an auth surface the engine has none of. Its Monaco
  editor alone is 14 MB against this file's 19 KB, and authoring is not what an
  operator on a constrained box needs. Unifying the two is worth doing later; it is
  not a prerequisite for seeing a run on a gateway.

  `DAGRON_CONSOLE=off` leaves it unmounted. Opt *out*, not in, because the API it
  drives is served on the same socket either way — hiding the UI does not reduce what
  that socket can do, so defaulting it off would buy the appearance of safety and no
  safety. **The management API has never had authentication**: `cancel`, `rerun`,
  `approve` and dead-letter redrive have always been open to anyone who can reach
  `API_ADDR`. What the console changes is discoverability, which is worth saying out
  loud — so a non-loopback bind now logs a warning naming exactly what is exposed, and
  the startup line reports whether the console is mounted.
- **`SOURCE=dir` — a directory of YAML as live input.** Drop a file into
  `WORKFLOW_DIR` and it runs; edit one and the next scan re-submits it. The default
  `file` source emits a single spec at startup and drains, which is why pointing the
  engine at a directory produced "Is a directory" and why a YAML dropped in later was
  never seen. "Put a file in a folder" is the model operators already have from every
  other tool on the box, and the on-ramp for people who will never wire up GitOps.

  **It polls, deliberately — no inotify.** File watching does not fire on the mounts
  this is for: Docker/Podman bind mounts from a Windows or macOS host, NFS and SMB
  shares on a NAS. A watcher that silently never triggers is worse than a scan that
  always works. One `readdir` plus a `stat` per file every `DIR_POLL_MS` (default
  2000, floor 100).

  Re-emission is keyed on (modified time, length), so an untouched file is never
  submitted twice and an edited one always is. Two edits inside one filesystem
  timestamp granularity *with an identical length* is the known blind spot; a content
  hash would close it by reading every file on every scan, the wrong trade for a
  directory someone edits by hand. **That key survives a restart**: it is committed
  to `source_offsets` in the same transaction as the run it becomes (one row per
  file, keyed `dir/<file>` — the exactly-once machinery `SOURCE=stream` already
  uses), so a crash-looping container does not re-run every workflow in the
  directory on each boot. A file the ingest actor could not turn into a run *yet*
  (a workflow at its `max_active_runs` cap) stays in flight and is redelivered on
  the next pull, rather than being dropped until someone edits it.
  `*.yaml`/`*.yml`, this directory only — no
  recursion, so a README or a vendored chart below it is not swept into a dead
  letter. An unreadable file is retried rather than recorded, which is what makes a
  large spec dropped in with `cp` land once it finishes copying; and a directory that
  does not exist yet idles instead of failing, because the mount may arrive after the
  engine does.

- **The release gate now runs a workflow against Postgres**
  ([.github/workflows/sync-dagron-oss.yml](../../../.github/workflows/sync-dagron-oss.yml)):
  a Postgres service, a three-task chain submitted to a real engine, and assertions
  that it reaches `run complete` / `"status":"succeeded"` with no decode error.

  This is the hole 0.8.0 fell through. The existing gate builds and tests two
  feature worlds and **neither runs the engine on Postgres** — the engine half is the
  SQLite build, and the Postgres half is `dagron-api`/`-gitops`, which never start a
  scheduler. So `dagron-core`'s Postgres backend compiled and was never executed, and
  a bare `SELECT 1` that cannot decode into `i64` reached a public release. Unit
  tests could not have caught it: SQLite is dynamically typed, and these are runtime
  `query_scalar` calls rather than compile-checked `sqlx::query!` macros.

  The chain is three tasks, not one, so at least one task becomes runnable *because a
  dependency finished* — the exact transition the 0.8.0 probe died on. **Verified by
  reintroducing the bug:** both assertions fire and the release fails.

  It runs with `LOG_FORMAT=json`. The default `full` format writes ANSI colour even
  to a file, which splits `status=succeeded` across escape sequences so a grep for it
  silently never matches — a gate that passes everything. That was measured, not
  assumed, and the step says so.

  **The image path is gated too.** The source tree was only half the exposure,
  because what operators pull is an image, and the release pipeline pushed one
  without ever running it. It now builds the engine image once with `load`, runs a
  three-task chain *in that image* against a Postgres service, and only then pushes
  — the second build is a layer-cache hit, so the gate costs one container start.
  Testing the artifact rather than a `cargo build` is the point: the runtime is
  distroless, so the smoke tasks run `dagron validate` (the image's only executable —
  a smoke test calling `echo` would fail for a reason that has nothing to do with
  the datastore, and that trap is exactly what running the real artifact finds).

- **The numbers, committed.** `results/` is no longer gitignored, and there is a
  harness that fills it. [`docs/LOW_LATENCY.md` §7](docs/LOW_LATENCY.md) has
  specified this since it was written, in words that were their own indictment:
  *"`results/` is currently gitignored and empty — every published number lives
  only in prose"*. Four pieces, all of them small:

  - [`loadtest/harness/micro_dags.py`](loadtest/harness/micro_dags.py) — the §1.1
    chain / fan / dense generators. Those three DAGs produced the table the whole
    low-latency plan rests on and **had never existed in the repo**; they lived on
    somebody's laptop. `["true"]` tasks, so what the wall time measures is the
    scheduler, not the work.
  - [`latency_bench.py`](loadtest/harness/latency_bench.py) — the runner. The §1.1
    reproduction (about a minute), the **depth sweep** 10²–10⁴ that
    `SCALABILITY-PLAN.md` calls "designed but never run" (its deepest chain to date
    was 6), and the **width sweep** to 10⁵ edges, where `create_run` is timed as an
    operator meets it — a `POST /runs` against a warm engine — with statement counts
    when `pg_stat_statements` is there. Per-hop p50/p99 come from the engine's own
    A-5 histogram, interpolated the way `histogram_quantile` does, so a dashboard
    and a committed file report the same number instead of each having their own.
  - [`hop_gate.py`](loadtest/harness/hop_gate.py) — §2's table encoded, and the
    >20 % hop-p99 regression that exits non-zero.
  - `.github/workflows/dagron-latency-gate.yml` — on a release tag, weekly, or on
    demand. Not on every push: a sweep sharing a runner with a compile job measures
    the compile job.

  **The first measurements are committed with it** (32-vCPU container, release
  build, stock knobs — [`docs/LOW_LATENCY.md` §7.2](docs/LOW_LATENCY.md) has the
  tables). §1.1's chain8 went 7.78 s → **0.096 s** on a stock build, 6× better than
  the hand-patched `poll=25ms` experiment that motivated Phases A and B. Two
  findings that only existed because someone finally ran the sweep:

  - **Per-hop cost is flat from 10² to 10³ chains (~5 ms) and 4× worse at 10⁴**
    (22.6 ms), moving the p50 rather than the tail — per-tick work that scales with
    the run's rows, not an occasional stall.
  - **A run submitted through the API waits a full `POLL_INTERVAL_MS` for its first
    dispatch on SQLite** (515 ms measured, against 14 ms for the same DAG from the
    file source) — the SQLite waker has no early-wake source, while Postgres
    `create_run` NOTIFYs in its own transaction. The Postgres sweep dispatches
    14–58 ms after create, which is the same claim measured from the other side.
    A-1 woke the loop for finished tasks; nothing wakes it for a new run.

  Also measured: `create_run` costs **281 statements for 110,000 rows** at 10⁵
  edges — B-3's batching is real — but 9 s of wall time, so the remaining cost is
  round trips and commits rather than a statement per row.

  Two deliberate refusals. The gate **skips rather than compares** when the
  baseline's backend, profile, machine, CPU count or `POLL_INTERVAL_MS` differ —
  absolute latency is a property of the machine, and a gate that pretends otherwise
  fails on hardware and passes on regressions. And §2's SLOs are **reported, not
  enforced** (`--enforce-slo` opts in), because §2 itself lists today's numbers as
  far outside them: a gate that is red before anyone touches it is a gate everyone
  learns to route around.

### Deprecated
- **`mancube/dagron-frontend` is deprecated and will be removed in a future breaking release.** The
  console it carried now ships inside `dagron-api` (see *Changed* below), so the
  image is a 281 MB Node runtime with nothing left to do. The intent was to keep
  building and publishing it until that removal so anyone pinned to it had a release
  boundary to migrate across rather than a tag that vanished underneath them. That
  did not survive contact with the build — see 0.9.1 above; **0.8.1 is the last
  tag.**

  **To migrate:** delete the `frontend` service, drop the `:3000` publish, and open
  the API's port instead. The console is at `/` and the API under `/api` on the same
  origin; `compose.quickstart.yaml` here is the worked example. If you front dagron
  with an ingress routing `/` to the frontend Service and `/api` to the API Service,
  both now point at the API.

  0.9.1 does that removal early: drops the image from
  `.github/workflows/docker.yml` and removes `frontend/Dockerfile`. The Docker Hub
  overview at `docs/dockerhub/dagron-frontend.md` stays as a tombstone pointing at
  `dagron-api`. The `frontend/` sources stay — they are what `dagron-api` builds
  and embeds.

- **The Helm chart stops deploying the frontend.** `frontend.enabled` defaults to
  `false`, and the ingress routes `/` to `dagron-api` — which now serves the console
  there — instead of to a frontend Service. A default install goes from three pods to
  two and stops running a 281 MB pod to serve a page the API already has.

  Waiting for the removal release would have shipped the console **twice** on every chart install
  for a whole minor line: once from the deprecated frontend pod that the ingress still
  pointed at, once from the API. That overlap is worth avoiding now; removing the
  block entirely still waits for the breaking release.

  `frontend.enabled=true` restores the old two-pod topology and points the ingress
  back at it, for anyone who needs the previous layout during 0.9. Two things went
  with the flip: the template precondition that *refused* an ingress unless the
  frontend was enabled (it now backs both paths itself), and the post-install notes,
  which had no branch for "no ingress, no frontend" and would have printed nothing at
  all about reaching the UI. The default path is now one port-forward to `:8080`
  rather than two plus a same-origin caveat.

- **`compose.yaml` stops starting the frontend too.** The service moves behind
  `profiles: ["frontend"]`, the way `gitops` already is, so the default
  `podman compose up --build` brings up Postgres, the engine and `dagron-api`
  — and the console at `http://127.0.0.1:8080/`, from the API that was already
  running. It is the same double-serving the chart change avoids: this file
  published `:3000` *and* `:8080`, so a dev stack on 0.9 stood up two copies of
  the same page and left it ambiguous which one a bug report meant.

  `--profile frontend` restored it; 0.9.1 removes the service outright, because
  the image behind it can no longer be built. One env var moved
  with the flip: `DAGRON_TRUST_PROXY_HEADERS` is now unset by default, because
  the console is served by the container the browser talks to — a login arrives
  from the real client address, and trusting `X-Forwarded-For` would mean
  trusting a header a directly-reachable caller can forge, for nothing. The
  frontend profile wants it back on, and says so where it is defined.

### Changed
- **The console ships inside `dagron-api` instead of its own image, and live
  updates work again as a consequence.** `mancube/dagron-frontend` was a 281 MB
  Node runtime whose only jobs were serving files and proxying `/api`. It never
  needed a server: 47 of the frontend's 65 sources are `"use client"`, it uses no
  server-only Next API at all (no `next/headers`, no `cookies()`, no server
  actions, route handlers or middleware), and every request goes through
  `BASE = "/api"` — a *relative* path. So the app is now built with
  `output: "export"` and handed to the Rust binary, which serves it at `/` as a
  fallback service beneath the API routes.

  **No components were rewritten.** The DAG view, log viewer, run tables and
  editor are untouched. The whole change is a Next config switch, four pages, and
  about twelve links.

  Deleting the proxy is the interesting half. Next's `rewrites()` buffers
  responses and never flushes headers for an infinite `text/event-stream`, so
  `/api/events/stream` hung — measured: no headers in 15 s through the proxy,
  immediate `200 text/event-stream` direct. That is why the console reported
  **Offline** on a healthy stack and why runs only appeared on a manual refresh.
  Same-origin, there is no proxy left to buffer, and the stream opens at once.

- **Detail pages moved from dynamic segments to query parameters** — `/runs/[id]`
  → `/runs/detail/?id=`, and likewise for `/runs/archive`, `/workflows/detail` and
  `/workflows/history`. A statically exported site resolves dynamic segments at
  build time via `generateStaticParams`, and a run id is not knowable then. These
  pages already did all their work in the browser after mount, so the change is
  `use(params)` → `useRouteId()` (`src/lib/route-id.ts`) inside a `<Suspense>`
  boundary, which `useSearchParams()` requires during prerender.

  **Old bookmarks are carried across.** A blocking inline script in the root layout
  maps a pre-0.9 path to its new home before hydration — `/runs/<id>` →
  `/runs/detail/?id=<id>`, and the same for `runs/archive`, `workflows/<id>` and
  `workflows/<id>/history` — preserving any query string, so a link from an alert
  carrying `?task=` still opens on that task. It runs inline rather than as an effect
  so there is no flash of the wrong page and it does not depend on how the client
  router treats a path that was never exported. Reserved page names (`detail`,
  `archive`, `history`, `new`) are excluded by an allowlist, so a run whose id happens
  to be `detail` cannot loop.

- **The quickstart is one port and one fewer container.** `compose.quickstart.yaml`
  drops the `frontend` service entirely, along with the `network_mode:
  "service:dagron-api"` trick that existed only to make the baked-in
  `localhost:8080` proxy target resolve. Open <http://localhost:8080>; the console
  is at `/` and the API under `/api`. `DAGRON_CONSOLE_DIR` (default
  `/usr/share/dagron/console`) relocates the assets, and when the directory is
  absent `dagron-api` serves the API alone — so `cargo run` on a dev box still
  needs no Node build.

### Fixed
- **A failing task's stderr is no longer discarded.** `LocalExecutor` logged the
  subprocess's stderr and then stored only stdout, so `RuntimeError`, `CUDA
  error`, every NCCL warning and every Python traceback - all written to stderr
  - never reached the stored `output`, the run detail, or anything reading it. A
  failing command's output now carries its stderr tail (16 KiB, cut on a line
  boundary where the tail has one — a single oversized line keeps its end
  instead; the tail rather than the head because the fatal error is the last
  thing a process prints). **Only on failure**: a successful task's output is
  byte-identical to before, because `repeat.until` terminates loops from it, the
  memo store caches it and `produces:` lineage reads it. This also makes the
  local backend agree with the other two - `DockerExecutor` already interleaves
  both streams and `KubeExecutor` stores the pod's combined log; local was the
  odd one out.


## [0.8.1] - 2026-08-28

Fixes a crash that made 0.8.0 unusable with Postgres. Anyone running the Postgres
backend should skip 0.8.0 entirely; SQLite deployments are unaffected.

### Changed
- **Every Node-based action in the three published workflows is on a Node 24
  release, and all are pinned by commit SHA.** The v0.8.0 runs — the first this
  repository ever executed — all carried the same annotation: *"Node.js 20 is
  deprecated … being forced to run on Node.js 24."* Eight actions were behind
  ([ci.yml](.github/workflows/ci.yml), [docker.yml](.github/workflows/docker.yml),
  [binaries.yml](.github/workflows/binaries.yml)): `actions/checkout` v4.2.2 → v7.0.1,
  `upload-artifact` v4 → v7.0.1, `download-artifact` v4 → v8.0.1,
  `docker/build-push-action` v6 → v7.3.0, `docker/login-action` v3 → v4.6.0,
  `docker/setup-buildx-action` v3 → v4.3.0, `docker/metadata-action` v5 → v6.2.0,
  `azure/setup-helm` v4 → v5.0.1. The runner is already forcing Node 24, so this
  aligns the declarations with what actually runs rather than changing it.

  Every one of these except `ci.yml`'s checkout was on a **floating major tag**
  (`@v4`, `@v3`), which for the two workflows that build and publish the release
  artifacts means the toolchain could move underneath a tagged build without a commit
  saying so. They are now SHA-pinned like everything else, with the version in a
  trailing comment. `Swatinem/rust-cache@v2` in `binaries.yml` is pinned to the same
  SHA `ci.yml` already used.

  The major bumps were checked against what these workflows actually pass, not taken
  on faith. `setup-buildx-action` v4 and `build-push-action` v7 remove deprecated
  inputs and two `DOCKER_BUILD_*` environment variables — none of which appear here.
  `download-artifact` v5's breaking change extracts a single matched artifact —
  whether by ID or a pattern that matches exactly one — directly into `path`
  instead of a named subdirectory; both workflows here match multiple artifacts
  with `merge-multiple`, so their layout is unaffected. v8 makes a digest
  mismatch an error instead of a warning, which is the behaviour you want on a
  release pipeline.

### Fixed
- **The engine crashed on Postgres the first tick it found work to do (0.8.0
  regression).** `advance_ready_tasks`' round gate probed with
  `SELECT 1 FROM task_runs WHERE status = 'pending' AND remaining_deps = 0` and
  decoded it into `Option<i64>`. A bare integer literal is **INT4** in Postgres, and
  sqlx refuses to decode INT4 into `i64`, so the reconcile loop died with
  `mismatched types; Rust type i64 (as SQL type INT8) is not compatible with SQL type
  INT4`. Probes are now `SELECT 1::bigint`, the cast the retention purge in the same
  file already used.

  The failure is the worst shape available: `fetch_optional` only decodes when a row
  comes back, so an **idle engine looks perfectly healthy** and dies the moment the
  first workflow arrives. `compose.quickstart.yaml` — the first thing anyone runs —
  reproduces it on boot.

  Four more instances of the same bare-literal probe are fixed alongside it, all on
  Postgres-backed paths: `task_exists` in the core ops path, and in `dagron-api`
  `control.rs` (approval resolution 404-vs-409 disambiguation), `logs.rs` (a run that exists
  but has no task rows yet) and `tokens.rs` (revoking an already-revoked token).
  Each would have returned 500 exactly in the case it was written to distinguish.
  The `sqlite.rs` equivalents are unaffected — SQLite is dynamically typed, which is
  precisely why nothing caught this.

  **Why the gate missed it.** The release gate builds `--workspace` minus
  `dagron-api`/`dagron-gitops` — the *sqlite* feature world — then those two on their
  own. So `dagron-core`'s Postgres backend is compiled but its runtime path is never
  executed, and these are runtime `query_scalar` calls rather than the compile-checked
  `sqlx::query!` macros. Nothing in CI starts the engine against a Postgres. A
  quickstart smoke test in the gate would have caught all five.

- **The one clippy finding that was blocking the lint from ever being a gate.**
  `crates/dagron-crypto/src/lib.rs` asserted a `bool` with
  `assert_eq!(..., true)`; it now uses `assert!(..)` with a message, which is what
  clippy's own suggestion said and what the line beneath it already did.

  Worth recording because 0.8.0 shipped a wrong number about it. The 0.8.0 changelog
  entry above and the comment in [ci.yml](.github/workflows/ci.yml) both said
  `clippy -D warnings` failed on "3 findings in `dagron-core`". The first run of that
  workflow — v0.8.0 was the first time it had ever executed — found exactly one, in a
  different crate. The claim had never been measured; it was asserted, and the
  workflow that could have checked it did not exist yet. The shipped 0.8.0 section is
  left as published rather than quietly rewritten.

  What clippy thinks of the rest of the tree is still unmeasured, and the comment in
  `ci.yml` now says so instead of guessing. The step runs under `set -euo pipefail`,
  so that single finding aborted the first `cargo clippy` before the second command
  (`dagron-api` + `dagron-gitops`) ran at all, and sibling crates in the first
  invocation were still checking when it bailed. The next run is the first honest
  reading. If it is clean, promoting clippy to a required check costs one line.

  rustfmt's number held up: 955 diffs against 955-ish claimed, and it stays advisory
  until there is a `rustfmt.toml` describing the style actually in use.

## [0.8.0] - 2026-08-28

### Added
- **The first outside contribution, from [@vladkens](https://github.com/vladkens)**
  ([mirror PR #2](https://github.com/lucheeseng827/dagron/pull/2)): the run-status
  badge at `GET /api/workflows/{name}/badge.svg` is now rendered by
  [`badgelib`](https://crates.io/crates/badgelib) instead of a hand-rolled
  shields-style SVG template. That retires about fifty lines this project was
  maintaining for no reason it could defend — a `seg_width` character-width
  heuristic guessing at Verdana metrics, and a four-replacement `xml_escape` — in
  favour of a crate whose job this is. The badge keeps its shape: grey label
  segment, coloured status segment, flat style. The two tests that would catch a
  renderer swap are untouched and still pass — `aria-label="dagron: succeeded"` is
  still emitted, and a status value of `a<b&c` still escapes to `a&lt;b&amp;c`.

  It is listed first because of how it arrived, not because of its size. It is the
  first change to reach this project from outside it, and the first to travel the
  inbound path described below: reviewed on the public mirror, carried upstream by
  `scripts/ossync-backport.py` with `Co-authored-by` and `Signed-off-by` intact,
  and republished here. Everything else in this section exists so that path works.

- **Pull-request CI in the public mirror**
  ([.github/workflows/ci.yml](.github/workflows/ci.yml)): build and test on every PR
  and every push to `main`, over the same two feature worlds the release gate uses (`--workspace` minus `dagron-api`/`dagron-gitops`, then those
  two on their own — `dagron-core` compiles exactly one sqlx backend, so a single
  unified build cannot work). Until now the mirror carried only the two
  tag-triggered publish workflows, so a contributor's PR sat with no checks at all
  even though [CONTRIBUTING.md](CONTRIBUTING.md) said otherwise; the build was
  gated privately, after the fact, where the contributor could not see it. Builds
  with `--locked`, so a PR that edits a manifest without refreshing `Cargo.lock`
  fails with a clear message instead of drifting the published lockfile. `fmt` and
  `clippy` run alongside but are **advisory**, measured on an untouched tree: stock
  rustfmt reports ~960 diffs (there is no `rustfmt.toml`, so it compares against a
  style this code was never written in) and `clippy -D warnings` fails on 3 findings
  in `dagron-core`. None of it is attributable to a contributor, so gating on either
  would paint every incoming PR red on arrival. Promoting them is cheap: clear the 3
  findings, add a `rustfmt.toml`, reformat in one commit, drop the two
  `continue-on-error` lines.
- **CONTRIBUTING.md now states the cadence**: an accepted PR is labelled
  `ready-to-backport` and travels upstream in a daily batch with the other accepted
  PRs, so the label can sit for up to a day before anything else happens. Without
  saying so, that gap reads as a stalled review.
- **CONTRIBUTING.md now explains how a PR actually lands** — reviewed and CI'd on
  the mirror, backported upstream with `Co-authored-by` and `Signed-off-by`
  preserved, republished in the next release sync, and the PR then *closed* with a
  link to the release carrying it. `main` here is a published snapshot, so a merge
  on this side is overwritten by the next sync; without saying so, a closed PR
  reads as a rejection it is not.
- **Low-latency profile, §4 (API & readiness)**
  ([docs/LOW_LATENCY.md](docs/LOW_LATENCY.md)): `GET /readyz` on both HTTP
  surfaces — on `dagron-api` it answers 200 only when a pooled connection is
  acquired and queried within its own budget (`DAGRON_READY_TIMEOUT_MS`, 500 ms),
  so an orchestrator stops routing to a replica whose pool or database is
  down; the shared SSE listener's state is advisory (reported in the `/readyz`
  body and as `event_listener` in `/api/health` — its failure is
  fleet-correlated via the one shared `DATABASE_LISTEN_URL`, so gating on it
  would empty the Service rather than reroute; `DAGRON_READY_REQUIRE_LISTENER=1`
  opts into strict gating). On the engine's ops API `/readyz` is a datastore
  round trip under the same budget knob. `/healthz` stays pure liveness; the
  Helm chart's readiness probe moves to `/readyz` (with explicit
  `timeoutSeconds` and an eviction threshold) and gains a startupProbe, and
  `dagron-api` drains on SIGTERM instead of severing requests mid-response —
  bounded by `DAGRON_SHUTDOWN_DRAIN_MS` (default 15 s), since open SSE
  streams and long-polls never finish on their own and an unbounded drain
  would hang every rollout until the SIGKILL. The API's pool is now tunable
  and warmable
  (`DAGRON_DB_MAX_CONNECTIONS`/`_MIN_CONNECTIONS`/`_ACQUIRE_TIMEOUT_MS`/
  `_TEST_BEFORE_ACQUIRE`, defaults preserving the old hard-coded behaviour;
  the warm floor is acquired eagerly before Ready). The submit path parses the
  document once for the environment-key read and the `workflow_ref` check
  (previously two extra full parses) and fetches referenced workflows one
  nesting level per query (`WHERE name = ANY(...)`) instead of one query per
  reference. `GET /api/runs/{id}/wait` blocks on the LISTEN-fed broadcast
  instead of a 200 ms busy-poll (5 queries/second per waiter, up to 200 ms of
  avoidable tail latency per sync invocation), with a slow re-check as the
  listener-reconnect safety net. Migrations 047–049 add the read-path indexes
  the runs list actually needs (`workflow_runs(created_at)`,
  `(status, created_at)`, `workflow_definitions(name)`; one single-statement
  CONCURRENTLY migration each, per the 022/023 convention). On
  enterprise builds the audit middleware no longer authenticates non-mutating
  requests it will never audit — for token callers that was two wasted DB
  round trips on every GET.
- **Low-latency profile, §5 (server-settings management)**: every knob the
  engine binary reads (92, mechanically derived) and every `dagron-api` knob
  (55) now live in per-process registries. `DAGRON_CONFIG=/etc/dagron/dagron.yaml`
  layers a reviewed YAML file UNDER the environment (env → file →
  `profile:` preset → default), with `profile: low-latency` applying the
  documented engine preset (`POLL_INTERVAL_MS=25`, `SWEEP_INTERVAL_MS=500`,
  `LEASE_SECS=5`); unknown file keys and unknown profiles are startup errors,
  while an env var that merely looks like a knob (`POLL_INTERVAL_M`) warns.
  `dagron config [--json]` and the ops API's `GET /config` print every knob's
  effective value (secrets redacted) with per-knob provenance, and both
  processes log and serve a stable configuration fingerprint (`dagron-api` in
  `/api/health` as `config_fingerprint`) so a fleet can alert on a replica
  whose settings drift from the reviewed set. The API's per-request env reads
  (proxy trust, session TTL, insecure-git opt-in) are memoized — settings are
  boot-immutable by policy. `docs/CONFIG.md` documents all of it.
- **Low-latency profile, Phase B** ([docs/LOW_LATENCY.md](docs/LOW_LATENCY.md)):
  set-based SQL on the dispatch critical path, in both backends.
  `advance_ready_tasks` now flips every default-shaped candidate
  (`trigger_rule: all_success`, not an approval, no runtime `when`) with one
  set-based UPDATE pair instead of two statements per task — the old shape made
  a 200-wide fan-out becoming ready cost ~400 sequential round trips; custom
  rules, approvals, and `when` conditions keep the original per-task code as
  the slow path, and a LIMIT-1 probe keeps the idle tick at the old cost.
  `mark_task_succeeded` promotes the default-shaped dependents its decrement
  just unblocked straight to `ready` in the same transaction, so the
  happy-path hop is mark → claim with no advance step between (measured:
  a dependency's terminal mark to its dependent's dispatch in 9.4 ms).
  `create_run` inserts task rows and dependency edges in multi-row chunks
  (1,000/5,000 rows per statement) instead of one statement per row — a
  2,000-task/19,500-edge DAG's submit→first-dispatch drops 2.26 s → 0.58 s,
  and `dagron-api`'s submit path inherits it via `dagron_core::db::create_run`.
  The parsed `TaskSpec` rides the dispatch payload and returns on the result,
  removing a per-completion input re-read + re-parse. Worker dispatch is
  idle-first (per-slot busy flags with a drop-guard clear and the old
  round-robin as fallback), eliminating head-of-line blocking behind
  long-running tasks. Cumulative at stock defaults since the phase-0 baseline:
  8-task chain 7.78 s → 0.14 s, 200-wide fan-out 6.35 s → 0.42 s, dense
  2,000-task DAG 48.4 s → 4.2 s. Parallel per-task dispatch prep and batched
  result marking are deferred with their motivating measurement recorded in
  the doc's Phase B gate.
- **Low-latency profile, Phase A** ([docs/LOW_LATENCY.md](docs/LOW_LATENCY.md)):
  the reconcile loop now wakes the moment a local task completes, instead of
  sleeping out the remainder of the poll interval — before this, every
  dependency hop of a short-task chain paid up to 500 ms (measured: an 8-task
  serial chain of no-op commands took 7.78 s; it now takes ~0.13 s at stock
  defaults, a 2,000-task/19,500-edge DAG went 48.4 s → ~5.9 s). A tick that
  marked work terminal re-enters immediately, so SQLite gets the same
  event-driven hops as Postgres. Three new knobs (defaults preserve today's
  behaviour exactly): `POLL_INTERVAL_MS` (timer bound, default 500, floor 10),
  `SWEEP_INTERVAL_MS` (maintenance sweeps — lease recovery, deadlines,
  approvals, sensors — now run on their own cadence instead of on every tick;
  default = poll interval), and `LEASE_SECS` (the claim lease window, formerly
  hard-coded 30 s in the claim SQL — the knob `docs/HA.md` had planned; the
  worker heartbeat scales with it at ⌊lease/3⌋ s (floor 1 s), preserving the
  two-missed-renewals headroom at any setting).
- **Dispatch-path latency metrics**: `scheduler_dispatch_latency_seconds`
  (became-claimable → handed to a worker), `scheduler_result_wait_seconds`
  (executor finished → result drained), and `scheduler_claim_batch_size`.
  Histogram buckets now resolve down to 250 µs (previously the smallest bound
  was 5 ms, so sub-tick scheduling latency was unmeasurable). To make ready-age
  true, the `pending → ready` transition now stamps `scheduled_at` with the
  readiness time (a run's tasks previously all carried the run's single
  creation timestamp): claim order within equal priority becomes
  readiness-FIFO, and the stale-ready alarm and spillover reclassing now
  measure time-since-claimable rather than time-since-run-creation.

- **`arguments:` on a `type: workflow` task — a sub-workflow trigger can now
  parameterise its child run.**

  ```yaml
  - name: turn
    type: workflow
    workflow: agent-turn
    arguments:
      state_dir: "/tmp/agent/{{ conversation }}"
  ```

  A trigger used to run the child's stored spec as-is, so it could only ever
  hand the child constants and every run of a child workflow was identical.
  That is what made the durable agent loop single-conversation: two runs of the
  same parent would read and write the same state, and nothing would notice.

  The field is the one `template:` calls already use, because it is one idea —
  arguments for whatever this task calls. The difference is where they are
  consumed: a template's are consumed by the expander, inline, so nothing
  survives on the leaf; a trigger's **survive expansion**, because the child run
  does not exist until dispatch. They are substituted at expansion, so a parent
  passes its own scope down (`{{ conversation }}` above), and the engine builds
  the child with the same call `POST /api/runs` makes for a caller-supplied
  `parameters` map.

  The child's stored definition is unchanged — parameters are applied to the
  graph, not written into the definition — so a workflow keeps one definition
  however many ways it is called.

  `arguments` with no `template` and no `type: workflow` is now rejected by the
  engine as well as the API. It was silently ignored there, which is the failure
  mode of a parameter that looks configured and goes nowhere.

### Added
- **`repeat:` now works on a `type: workflow` task — the durable agent loop.**
  A trigger with a `repeat:` runs its child workflow once per iteration, so a
  multi-turn conversation is a stack of runs: turn 7 has its own tasks, logs,
  timings and failures rather than being a line in a log file.

  ```yaml
  - name: turn
    type: workflow
    workflow: agent-turn
    repeat: { until: "{{ output }} == done", max_iterations: 10 }
  ```

  Nothing new was needed to make a conversation durable. The turn number is the
  task's `attempt` column, the state is whatever the turn writes, and the rest is
  rows — kill the engine mid-conversation and the next reconcile tick resumes it.

  **The decision is made before the trigger is resolved, never after.** Resolving
  a task decrements its dependents, so a loop that resolved and then re-armed
  would release the rest of the DAG on the first turn and again on every turn
  after it. A continuing iteration never touches a dependent.

  A **failed** turn fails the loop rather than being repeated: `repeat` decides
  when a conversation is over, not whether a failure counts — retrying a broken
  turn is `max_attempts`. Exhausting `max_iterations` also fails, and says what
  the last turn returned.

  `max_iterations` is the only hard bound on how many runs one submission
  creates — `SUBWORKFLOW_MAX_DEPTH` bounds nesting, and every iteration is a
  sibling at the same depth. See
  [Loops over sub-workflows](docs/CONFIG.md#loops-over-sub-workflows), and
  `examples/ai/agent_loop.yaml` for a runnable one.

  `repeat` on `type: wait` or `type: approval` stays rejected. Re-asking a person
  until they give the answer a condition wants is not a loop, and "wait again" is
  what a longer `for:` already says.

### Fixed
- **A sub-workflow's result now reaches its parent task.** `result_from` gives a
  run a single return value, and a `type: workflow` task threw it away — it
  recorded the constant `"sub-workflow succeeded"` instead. So
  `{{ tasks.<trigger>.output }}` in a parent DAG was a fixed string and nothing
  downstream could branch on what the child decided. A sub-workflow was a
  procedure; it is now a function.

  The failure side improves too: a child that died on its `run_timeout_secs`
  deadline records *why* on its run row, and the parent's failure summary now
  shows that rather than the word "failed". A child with no result of its own
  still reads `"sub-workflow succeeded"`, so nothing that worked changes.

- **`repeat:` on a task kind that never evaluates it is now rejected.** The loop
  operator is evaluated where an executor's result comes back. Every other kind
  reaches success through a *sweep* — an approval resolved by an operator, a
  sub-workflow resolved when its child run goes terminal, a wait sensor resolved
  at its deadline — and those paths never consult it.

  So `repeat:` on one of them was a silent no-op: the task succeeded once, the
  loop never ran, and nothing said so. A loop operator that quietly does not
  loop is worse than one that is unavailable, because the workflow reads as
  correct. It now fails validation, naming the kind. `repeat` on an ordinary
  command task — the only place it ever worked — is unchanged.

### Added
- **`dagron-step-mcp` — a workflow task that calls an MCP tool.** The inverse of
  the `dagron-mcp` server: that one lets an agent drive dagron, this lets a
  dagron DAG drive an agent's tools.

  The reason to want it is what a task already is. Once a tool call is a task it
  gets retries with backoff, a timeout, captured output, artifacts to hand to the
  next step, an approval gate in front of it, and a row in the run's history. A
  tool call inside an agent's own loop has none of that — when it fails halfway
  there is nothing to resume, and nothing that records it happened.

  Speaks **stdio JSON-RPC**, the one transport every MCP server supports: the
  server is spawned as a child and read exactly as `dagron-mcp` reads its own
  input. A task's `command` already spawns a process, so a child is native here
  in a way an HTTP connection is not.

  Configured by environment like every other dagron step. Server arguments are a
  JSON array rather than a command line, because a whitespace split breaks
  silently on any argument containing a space and a shell would make a workflow
  parameter injectable. All-text results come back as plain text so the next
  task can use them directly; mixed content keeps its JSON rather than losing
  the parts that are not text. A tool reporting `isError` fails the task —
  without writing the output file, so the retry it triggers is not satisfied by
  a stale error.

  See [`docs/MCP.md`](docs/MCP.md) and
  [`examples/ai/mcp_tool_step.yaml`](examples/ai/mcp_tool_step.yaml).
- **`GET /api/runs/{id}/wait` now carries `failure` too.** The same object the
  run detail returns, present only when the wait ended on a failure.

  The summary landed on the run detail first, which is the route a *human* opens
  and not the one a synchronous caller uses. An agent blocking on a run holds the
  wait response, so it still got a bare `status: "failed"` and had to start a
  second investigation with whatever tools it had. The reason now travels in the
  response the caller is already holding.

  The task rows behind the summary are read only when there is a failure to
  explain, so a succeeding wait — the common case, and the one that decides this
  endpoint's cost — pays nothing.

### Changed
- **`POST /api/workflows/{id}/run` now answers `201 Created`, not `200 OK`.**
  It creates a run, and every other route that creates one already said so.
  This was not merely inconsistent: a layer in front that counts created runs by
  status — a proxy, a quota, a meter — saw a creation it could not tell from a
  read, so runs submitted by name went uncounted. Clients check for 2xx, so
  nothing that works today stops working; anything asserting `== 200` on this
  route needs updating.

### Added
- **`budget:` on a workflow spec — a ceiling on how big one run may get.**
  `budget: { tasks: N }` refuses a run that would create more than `N` tasks,
  at creation, before anything executes. A scheduler called by an agent is an
  unbounded amplifier: one call can fan out to hundreds of tasks, and the first
  place anyone notices is the invoice.

  The count is the task **rows** a run would create, not the spec entries an
  author typed — the same gang-aware count admission uses. A `with_items`
  fan-out or a `gang:` co-scheduled task is one line on the page and many rows in
  the database, and budgeting the page would budget nothing. Refusing at creation
  rather than mid-run is deliberate: dagron already knows the exact final task
  count at submit, so a run that would break its budget never starts instead of
  being killed with work already spent.

  Distinct from `max_active_runs` (how many runs of a workflow may be in
  flight) and from `DAGRON_MAX_TASKS_PER_RUN` (the operator's process-wide
  ceiling against an OOM). This one lets a workflow's author say what *this*
  workflow is supposed to cost.

  **Only `tasks` is enforceable, and that is a statement about the data.** A
  task count is exact at creation; spend and model-token counts are not metered
  per run anywhere in the engine, so a `spend:` field would be a promise with no
  measurement behind it.
- **`GET /api/runs/{id}` now says why a run failed.** A new `failure` object —
  `{task_id, task_name, attempt, failed_tasks, message, truncated}`, or `null`
  when nothing failed. Previously a caller holding a failed run had a status and
  a list of task rows, and had to work out which one broke and then fetch its
  logs; the reason is now in the response it already has.

  It names the **earliest-finished** failed task rather than the first row: with
  fail-fast off several tasks fail independently, and the earliest is the
  likeliest cause rather than a downstream casualty. `failed_tasks` says how
  many there were, so summarising one out of several stays honest. `message` is
  the tail of that task's output, clipped to 20 lines / 4 KB with
  `truncated: true` — `GET /api/runs/{id}/logs` is still the full view.

  Two cases worth knowing. A run failed with **no** failed task — a
  `run_timeout_secs` overrun fails the run and *cancels* its tasks — reports the
  run's own reason with `task_id: null`, which is exactly the case that was
  previously unexplainable from the task rows. And the summary appears as soon
  as a task fails, so a still-`running` run can carry one; a caller polling it
  can stop waiting on work that is already doomed.

  Additive and nullable, so no existing client changes.

- **`POST /api/runs` accepts an optional `Idempotency-Key` header.** Repeating a
  submit with the same key returns the **same** `run_id` with `200` instead of
  creating a second run; a first submit still answers `201`. Without the header
  the endpoint is unchanged.

  This is the difference between an API a client calls and one it can call
  safely in a retry loop. A dropped response used to leave the caller unable to
  tell a duplicated run from a lost one, and both guesses are wrong: re-submit
  and a pipeline may run twice, don't and it may run zero times.

  A key reused for a *different* body is `409`, not a replay — handing back a
  run id for work that never ran is the worst available answer, so the request
  is fingerprinted (SHA-256 over the spec and parameters) and checked. An
  identical submit still in flight is also `409`; retry and get the run id.
  Keys are printable ASCII up to 255 characters, and an *empty* key is a `400`
  rather than being treated as absent — a client that sends one believes it has
  retry safety.

  Keys need no separate expiry: a key lives exactly as long as the run it names,
  and the retention GC that purges the run frees the key with it.

- **Both SDKs can use the above.** `submit_run` / `submitRun` take
  `parameters` (added to the API in the previous change but never reachable from
  the SDKs) and an idempotency key — `idempotency_key=` in Python,
  `{ idempotencyKey }` in TypeScript. Omitted parameters are not sent at all, so
  the request body older servers expect is unchanged.

## [0.7.0] - 2026-08-12

Minor, not patch: task pods created by the Kubernetes executor no longer receive
a ServiceAccount token unless they asked for one. That is a changed default, and
while dagron is 0.x this project treats a breaking change as a minor bump (see the header
above). Everything else here is additive or a fix.

### Fixed
- **`dagron-api` and `dagron-gitops` could not connect to any Postgres that
  requires TLS.** `sqlx` is declared `default-features = false`, and neither
  crate enabled a TLS backend — so an `sslmode=require` DSN failed at connect
  with *"TLS upgrade required by connect options but SQLx was built without TLS
  support enabled"*. That is not an edge case: it rules out Amazon RDS (which
  forces SSL by default), Cloud SQL, Azure Database, and any hardened
  self-hosted instance. The engine's `postgres` feature had the same gap. All
  three now enable `tls-rustls-ring`, the stack the rest of the workspace
  already resolves, so no second TLS implementation enters the build.

  If you run Postgres without TLS, nothing changes.

- **`POST /runs` bypassed admission control.** `MAX_INFLIGHT_RUNS` was enforced
  on the ingestion path but not on the engine's own HTTP API, so the route most
  likely to be scripted against was the one route with no valve on it.

### Added
- **A second admission dimension, counted in tasks.** Runs are the wrong unit on
  their own: a run of 100,000 tasks and a run of four both count as one against
  `MAX_INFLIGHT_RUNS`, so a fleet sitting comfortably under the run cap can be
  far past what the scheduler and the datastore can carry. `MAX_INFLIGHT_TASKS`
  (default `0`, off) caps the tasks in `pending`/`ready`/`running`. A task parked
  on an approval is not pressure and is excluded. The cap counts the rows a
  submission will actually insert — a `gang:` task is one spec but `size` rows,
  so a run of ten 64-member gangs is admitted as 640, not as 10. Only queried
  when set, so the default path pays no extra round trip.

- **`DAGRON_MAX_TASKS_PER_RUN`** — a per-run ceiling on the expanded graph.
  `expand()` materialises the whole graph in memory before anything reaches the
  database, so one runaway fan-out costs far more than the engine's idle
  footprint. May only *lower* the compiled ceiling of 100,000; a larger value is
  ignored, because the knob exists to tighten a bound that prevents an OOM.

- **Security context for Kubernetes task pods.** Task pods were built with no
  `securityContext` at all — no user, no capability drop, no seccomp profile, no
  deadline, no placement — which meant a task image whose final layer is
  `USER root` ran as root, and a hung task held a node slot until the run-level
  timeout noticed. Each control is opt-in and off by default, because turning
  them on wholesale would break images that legitimately need what they remove:
  `DAGRON_TASK_RUN_AS_USER`, `DAGRON_TASK_READ_ONLY_ROOT_FS`,
  `DAGRON_TASK_DROP_ALL_CAPABILITIES`, `DAGRON_TASK_SECCOMP_RUNTIME_DEFAULT`,
  `DAGRON_TASK_ACTIVE_DEADLINE_SECS`, `DAGRON_TASK_RUNTIME_CLASS` (e.g. `gvisor`,
  for images you do not control) and `DAGRON_TASK_NODE_SELECTOR`.

- **Chart: node placement for the engine and the API.** `engine.nodeSelector` /
  `tolerations` / `affinity` and the same three under `dagronApi`. With the
  `k8s` executor the engine and the task pods it creates are separate workloads
  with opposite shapes, and an engine that lands among its own task pods
  competes with them for the CPU it needs to keep dispatching.

- **Chart: `externalDatabaseSecret.listenKey`** — an optional second Secret key
  holding the *direct* connection string, for deployments whose main DSN points
  at a transaction pooler. Transaction pooling cannot serve a session-scoped
  `LISTEN`, which the reconcile loop's wake depends on, so that one connection
  needs a non-pooled endpoint of its own.

### Changed
- **Task pods no longer get a ServiceAccount token by default.** A task that did
  not declare `service_account:` never asked for an identity, and on a cluster
  using IAM-for-ServiceAccounts the token it was being handed is a cloud
  credential handed to arbitrary task code. Tasks that *do* declare
  `service_account:` are unaffected and keep their token. Set
  `DAGRON_TASK_AUTOMOUNT_SA_TOKEN=1` to restore the previous behaviour.

## [0.6.0] - 2026-08-09

Minor, not patch: `DagronClient::from_env` now returns `Result<Self>`, and
while dagron is 0.x this project treats a breaking change as a minor bump (see the header
above).

### Added
- **GitOps repositories can authenticate — with a token *or* an SSH key, set from
  the console.** Until now the only credential the sync had was the worker-wide
  `DAGRON_GIT_TOKEN`: one token for every repository, changeable only by
  redeploying the container, sent only to three hard-coded forge hosts, and
  HTTPS-only. Two repos on two accounts could not both be connected, a
  self-managed forge could not be reached at all (`DAGRON_GIT_TRUSTED_HOSTS` was
  documented but read by nothing), and a forge that offers SSH but not HTTPS
  clone was simply out of reach. The console offered no way to enter a credential
  of any kind.

  Each repository now carries its own optional credential — an **HTTPS token** or
  an **SSH private key** — set on the GitOps page or through
  `PUT /api/git-repos/{id}/auth`, and removed with `DELETE`. It is stored
  AES-256-GCM encrypted by the same `DAGRON_ENV_SECRET_KEY` / KEK machinery as
  environment secrets, decrypted only by the `dagron-gitops` worker, and is never
  readable back: the API returns a kind, a username and a hint (`••••cdef`, or
  `ssh-ed25519 SHA256:…` — the same fingerprint the forge prints beside the
  deploy key), never the value.

  Consequences worth knowing:

  - **SSH URLs are accepted now.** `ssh://git@host/owner/repo` and the scp-style
    `git@host:owner/repo` both connect; the `git@` that made them fail validation
    was being read as an embedded credential, which on SSH it is not. An `https://`
    URL still may not carry userinfo — there it *is* the credential, and storing
    it would write a token into `git_repos.url` in cleartext.
  - **Neither secret reaches the worker's command line.** The token goes to a
    0600 `credential.helper` file and the key to a 0600 file named by
    `GIT_SSH_COMMAND`, both in a scratch directory removed when the sync ends.
    The previous `https://x-access-token:TOKEN@host/…` clone URL put the token in
    argv, which `/proc/<pid>/cmdline` hands to anything in the container.
  - **The mismatch is refused at write time.** A token against an SSH repo, or a
    key against HTTPS, is a `400` — not a credential the console reports as
    installed while every sync fails "permission denied". Public keys pasted into
    the private-key box and passphrase-protected keys are rejected for the same
    reason: the worker has no terminal to be prompted at.
  - **`DAGRON_GIT_SSH_STRICT=1`** refuses an SSH sync for a repo with no pinned
    `known_hosts` rather than trusting whatever host answers. Off by default; the
    console says "host key unverified" on repos in that state either way.
  - **`DAGRON_GIT_TRUSTED_HOSTS` now works.** It has been in `docs/CONFIG.md`
    since the feature shipped and was read by nothing, so a token for a
    self-managed forge was silently dropped and the clone failed as "repository
    not found". It still applies only to the worker-wide token: a per-repo
    credential is not host-filtered, because attaching it to that repository is
    the operator saying where it may go.

  The `dagron-gitops` image gains `openssh-client` (git execs `ssh`; without it
  every SSH repo fails with "ssh: not found") and needs `DAGRON_ENV_SECRET_KEY`
  set to dagron-api's value — compose and the Helm chart wire it through. A repo
  with no credential behaves exactly as before.

- **`DAGRON_MCP_TOKEN` will no longer cross the network in the clear.** A token
  sent to a remote `DAGRON_API_URL` over plaintext `http://` is readable by
  anything on the path, so the server now **refuses to start** instead. The error
  names the three ways out — use `https://`, point at loopback, or set
  `DAGRON_MCP_ALLOW_PLAINTEXT_TOKEN=1` — and names the host, and *only* the host,
  since a base URL may carry `user:password@` and reporting a leaked credential
  by printing another one would be its own disclosure.

  This started as a warning, which was the wrong call: a process cannot verify
  that a mesh is protecting the connection, and a log line nobody reads doesn't
  stop a misconfigured deployment from shipping the token in cleartext. The
  opt-out keeps the legitimate case — plaintext to an in-cluster Service,
  transport secured below us — fully supported while making that exception
  explicit and auditable rather than silent.

  Loopback is decided by parsing the host as an `IpAddr` and asking
  `is_loopback()`, so the whole `127/8` block and `::1` need no opt-out, while an
  ordinary remote name like `127.0.0.1.example.com` gets no free pass.

  **Breaking:** `DagronClient::from_env` now returns `Result<Self>`.

### Changed
- **The enterprise audit trail is no longer published.** `routes/audit.rs` shipped
  the whole feature — the `audit_log` DDL, the recording middleware, the `viewer`
  read-only denial and the `/api/audit` read — into the public mirror behind
  `#[cfg(feature = "enterprise")]`, compiled off but fully readable. The 230 gated
  lines move to `routes/audit_ee.rs`, which the sync manifest excludes the way it
  already excludes `migrations_ee`; the open tree keeps the passthrough middleware
  and every call site in `main.rs` is spelled the same in both builds.

  It is `include!`d rather than declared as a `mod`, and that is the whole trick:
  rustfmt resolves every `mod` declaration regardless of `cfg`, so the module form
  makes `cargo fmt` fail outright in a checkout where the file is absent — exactly
  the mirror. An inactive `include!` it leaves alone, and `cargo build` / `cargo
  test` are happy either way.

- **The mirror stops naming the repository it was cut from.** A sweep of all 457
  staged files found the private monorepo's layout and vocabulary spread through
  it: the CI job name in `dagron-plan/Cargo.toml`, the closed crate path in
  `dagron-mcp/README.md`, an internal load-test note cited from five code
  comments, the sync manifest named in `docker.yml`, and 25 references to
  documents that are not published — every one a dead link or a private filename
  in a public repo. All reworded.

  Two of those documents were the walkthrough a *published* example pointed at, so
  the example shipped without the page explaining it: `BACKFILL_USECASES.md` and
  `WORKFLOW_UI_GUIDE.md` are now mirrored (the latter after dropping the monorepo
  path from its header and its quickstart). The Argo CD walkthrough is not, and
  cannot be as written — every example in it points `repoURL` at the private
  clone URL, the same reason the AWS backup runbook stays unpublished.

  Two guards were added for the class of mistake that nearly shipped it: the marker
  set had nothing matching the private repository's own name, or a path inside it.
  Both now fail the pre-publish scan. (The patterns are spelled out in the sync
  manifest, not here — a changelog that quotes them would trip them.)

- **Marketing framing out of the published docs.** `DATASETS.md` carried a section
  of go-to-market reasoning written for us, not the reader — "top-of-funnel",
  "the user learns the edition line at the moment they feel the need", and a
  closing note that a GitHub issue quoting an error "*is* a qualified lead". It is
  replaced by "Limits of the open build", which keeps every gate and the workaround
  for it and drops the funnel. `audit.rs` no longer calls the feature "paid-tier",
  and `README.md` says "managed fleet" where it said "commercial fleet". The
  capability tables all stay: the engine's own signpost errors point at them.

- **The private-key tripwire stopped firing on source that merely names one.** The
  pre-publish scan matched a bare `BEGIN … PRIVATE KEY`, which the new GitOps
  credential form trips legitimately — the textarea placeholder and the key
  validation tests both contain the literal header an operator has to recognise.
  It is now two rules that match key *material*: the header alone on a line (any
  real `.pem`, including an indented YAML block scalar) and the base64 of the
  `openssh-key-v1` container header, which is context-free and so still catches a
  key pasted into a source literal — the one shape the first rule gives up.

### Fixed
- **`MAX_INFLIGHT_RUNS=0` disables the admission cap, as the chart has always
  said it does.** The chart, its `values.yaml` and `docs/CONFIG.md` all documented
  `0` as "no cap", and `POST /runs` implemented exactly that (`max_inflight_runs
  > 0` before it checks). The env parse in between clamped with `.max(1)`, so `0`
  never reached either consumer: an operator who disabled the cap got a cap of
  **one active run** instead — a near-total stall, and the opposite of what they
  asked for.

  The clamp is now `.max(0)`, and the ingest actor's throttle grew the same
  `> 0` guard the API path already had — without it a `0` that finally reaches it
  compares `active >= 0`, which is always true, and the actor would throttle
  forever while admitting nothing. Negative values normalize to `0` rather than
  meaning some third thing. Both sites are pinned by unit tests
  (`max_inflight_runs_zero_disables_the_cap`, `zero_cap_never_closes_the_valve`),
  because the previous contract was documented in three places and enforced in
  none.

- **The MCP `dagron_submit_run` tool now actually submits.** It posted the spec
  as a raw body with `content-type: application/yaml`. `POST /api/runs` binds
  `Json<SubmitBody>` — an envelope, `{"yaml": "…"}` — and axum's `Json` extractor
  refuses any request that isn't `application/json` with **415 before the handler
  runs**. So the one tool an agent needs most, hand dagron a workflow, could not
  have worked against `dagron-api` in any deployment: every submit came back as an
  unsupported-media-type error the model then reported as a tool failure.

  It sends the JSON envelope now. Worth noting *why* it survived: the crate's
  tests covered the tool catalogue and the argument validation thoroughly, and
  nothing asserted what went on the wire — a shape only visible at the HTTP
  boundary. `submit_posts_the_spec_as_json` closes that by serving one request
  from an in-crate listener and asserting the method, the content type and the
  body, so the next drift of this kind fails a test rather than a demo.

  `DagronClient::get`/`post` are also public now, so a composing server can add
  tools without re-implementing base-URL and auth handling.

## [0.5.2] - 2026-07-28

### Fixed
- **`dagron-gitops` no longer carries perl.** The image scanned with five open
  advisories — CVE-2026-57433, CVE-2026-8376, CVE-2026-13221 and CVE-2026-42496
  against perl, plus CVE-2023-45853 against `zlib1g` — reported as 17 rows
  because each perl advisory appears once per installed package (`perl`,
  `perl-base`, `libperl5.36`, `perl-modules-5.36`).

  Read them more carefully than the scanner's colour, and two things matter.
  Debian triages several as `no-dsa` — "Minor issue; can be fixed in point
  release" — which is *why* the "fixed in" column is empty: a deliberate
  deprioritisation rather than an impossibility. And CVE-2026-8376 affects only
  32-bit builds, so it never applied to the amd64 and arm64 images published
  here.

  So the case is not "the image was critically vulnerable". It is that perl was
  never wanted in the first place — Debian's `git` hard-`Depends` on it, so
  `--no-install-recommends` could not exclude it, and `debian:bookworm-slim`
  ships `perl-base` before anything is installed at all — and an unneeded
  dependency carrying open unfixed advisories is worth removing when the cost is
  a base-image change. The runtime moves to Alpine, whose `git` depends only on
  musl, libcurl, libexpat, libpcre2 and libz. Verified in the built image: zero
  perl packages, no perl binary, zlib 1.3.2, and git 2.47.3 still cloning and
  committing as the non-root user. The image is 26.7 MB.

  Cheap to do here only because this crate pulls no OpenSSL: sqlx is
  `default-features = false` with no TLS feature. **`dagron-engine-localdev` is
  still `debian:bookworm-slim` and still carries the `perl-base` and `zlib1g`
  CVEs** — left alone because Alpine would swap its GNU userland for busybox,
  and that image exists so user-authored task commands resolve.

- **Release binaries actually publish.** 0.5.1 announced them and shipped none.
  The `x86_64-apple-darwin` leg targeted `macos-13`, which GitHub has retired,
  and a `runs-on` label matching no runner does not fail — it sits in `queued`
  until the 24-hour ceiling. Because `publish` waits on the whole matrix, four
  successful builds were stranded behind it and the release got zero assets
  rather than four.

### Removed
- **The `x86_64-apple-darwin` binary.** Dropped rather than repointed at another
  Intel image: Apple stopped selling Intel Macs in 2023 and those runners are
  being retired, so repointing buys a target that breaks again on someone
  else's schedule. Remaining: `x86_64`/`aarch64-unknown-linux-musl`,
  `aarch64-apple-darwin`, `x86_64-pc-windows-msvc`. Intel Mac users can build
  from source.

## [0.5.1] - 2026-07-28

### Added

- **Personal access tokens.** Automation had no credential of its own:
  `/api/login` mints a short-lived session token meant for a browser, so a CI
  job had to store a *password* and log in to get one — a secret that cannot be
  revoked without locking the human out and cannot be told apart from that
  human's own traffic.

  `POST /api/tokens` mints a named, optionally-expiring token, `GET` lists them,
  `DELETE` revokes. It goes anywhere the session bearer goes, recognised by its
  `dgp_` prefix, and the console grows an **API tokens** screen. Only
  `sha256(token)` is stored — the plaintext exists once, in the creation
  response, and no endpoint can show it again because nothing is kept that
  could. Two rules worth knowing: a token **cannot manage tokens** (that needs a
  password session, so one leaked credential cannot mint replacements faster
  than you revoke them), and a token carries its owner's permissions read
  *live*, so demoting a user takes effect on their existing tokens at once.
  There is no per-token scoping yet — give automation its own user.
  See [`docs/HOWTO.md`](docs/HOWTO.md).

- **Per-line log timestamps.** Every log line now carries a time. Where a line
  printed its own RFC3339 stamp — a task that logs structured output, or
  Docker/Kubernetes with their `timestamps` option — it is parsed into `ts` and
  removed from the text. Where it did not, which is most output, the console
  falls back to the owning task's start and shows the task's duration alongside,
  so the estimate is judged rather than trusted: a 200 ms task makes it
  millisecond-accurate, a four-hour task makes it nearly useless, and you can
  see which you are looking at.

  Deliberately *not* interpolated across the task's window by line position —
  that would spread lines evenly over a duration they were never evenly spread
  across, inventing precision that reads as recorded fact.

  Splitting the stamp off is also what keeps the filters honest: every predicate
  now runs on the message, so an anchored `regex=^ERROR` still matches once a
  runtime starts prefixing times. The trade is that a term appearing only in the
  timestamp no longer matches, so `exclude=2026-07-26` no longer removes stamped
  lines; time-range filtering belongs on the parsed `ts` and is not built yet.

- **Failure triage.** `status` says what the engine did; nothing said what a
  person then decided, so a failed run stayed failed forever and the overview's
  "needs attention" list could only drain by the failure ageing out — which is
  not triage, it is forgetting.

  `POST /api/runs/{id}/triage` records `acknowledged` (someone is on it),
  `resolved` (dealt with) or `ignored` (a real failure we accept, with a note
  explaining why); `DELETE` puts it back in the queue. Three states rather than
  one flag because they are different answers, and the run page grows a control
  shown only on a failed run. The overview now counts *untriaged* failures, so
  the queue drains because someone acted.

- **Workflow versioning and lifecycle.** Editing a workflow overwrote its spec
  in place — there was no version column and no history, so the previous
  definition was simply gone. `workflow_versions` is now append-only, written on
  create *and* every update, and the console shows the history with a read-only
  view of any stored spec. (Past *runs* were never at risk; `task_runs`
  snapshots its TaskSpec at dispatch.)

  Workflows also gain a state: `active` / `paused` / `retired`. Both non-active
  states refuse to run — enforced in the scheduler *and* on the run endpoint, so
  "paused" stops cron and the button alike — and **neither touches the
  workflow's schedules**. That is the whole point: the only stop button used to
  be `DELETE`, and `schedules.workflow_id` is `ON DELETE CASCADE`, so deleting a
  workflow silently took its cron schedules with it.

- **Dead-letter retry policy from the console.** `DEAD_LETTER_MAX_ATTEMPTS` was
  environment-only and read once at engine start, so changing it meant a
  restart. It is now a stored setting the engine reads on the failure path —
  no restart, and no window where the console shows a number the running
  process is not using. Unset keeps the environment value, so existing
  deployments are unchanged. `STREAM_DLQ_PATH` stays deployment config on
  purpose: it is a filesystem path on the engine's host.
  New guide: [`docs/DEAD_LETTERS.md`](docs/DEAD_LETTERS.md).

- **Release binaries.** A release shipped container images and an OCI chart and
  nothing you could run. `dagron` is now published for
  `x86_64`/`aarch64-unknown-linux-musl`, `x86_64`/`aarch64-apple-darwin` and
  `x86_64-pc-windows-msvc`, with `SHA256SUMS`. musl rather than glibc so a
  static binary runs on any distro rather than inheriting the builder's glibc
  floor. Each is smoke-tested on the runner that built it.

- **`compose.quickstart.yaml`** — pulls published images instead of compiling
  four Rust crates before you have seen anything. Paired with a new
  **`dagron-engine-localdev`** image: the production engine is distroless, and
  with `EXECUTOR=local` a task's command runs *inside* that container, so
  `command: ["echo", "hello"]` cannot resolve there. The localdev image is the
  same binary on debian-slim.

- **`scripts/examples_check.py`** — validates every example spec and runs the
  ones a laptop can run, classifying the rest with a reason so "skipped" never
  quietly means "broken". Currently 35 specs valid, 27 running.

- **`scripts/logfilter_e2e.py`** — end-to-end check of the log filter grammar,
  including parity between the engine's ops API and `dagron-api`. The two
  handlers implement one grammar and nothing compared them to each other.

- **Workflow log views with an explicit, adjustable filter.** Logs were readable
  one task at a time, unfiltered: to find out why a run failed you clicked
  through every task panel looking for the one that printed the stack trace, and
  a task that logged 200k lines gave you all 200k. Both halves are now fixed.

  A new **`GET /api/runs/{id}/logs`** (and `GET /runs/{id}/logs` on the engine
  ops API) returns *every* task's output in a run as one attributed stream —
  which task, which attempt, which line number — so a failure is one call away
  instead of N guesses. Both log endpoints, old and new, accept the same
  server-side filter: `q` / `exclude` / `regex` / `level` / `case` / `context` /
  `limit` / `tail`. Filtering happens on the server because a run's output can
  be hundreds of megabytes, and "download it all, then grep in the browser" is
  not a filter.

  The console gains a **Logs** view on the run page (next to Graph and Timeline)
  with the filter controls, level toggles, and a per-task scope picker; the task
  drawer gets the same controls, and a filter survives navigation and is carried
  in the URL, so "here's the link with the error filter" works. The filter also
  composes with live tailing: it applies *within* each `?offset=` slice while
  `next_offset` keeps counting the raw text, so a filtered tail neither loses nor
  repeats output.

  Three properties the implementation is deliberate about, because a log view
  that quietly hides things is worse than none: `total`/`matched` are counted
  **before** the line cap, so a truncated view always says how much it hid; line
  numbers are positions in the *unfiltered* output, so a filtered line can still
  be found in the raw log; and an invalid filter (uncompilable regex, unknown
  level) is a `400` naming the reason rather than a silently unfiltered response
  the caller would read as "nothing matched was hidden".

  Levels are **inferred** from the head of each line — task output carries no
  structured level — and only the head, so `echo "no errors found"` isn't an
  error line. The grammar lives in one place
  (`crates/dagron-logging/src/logfilter.rs`, feature `logfilter`) so a filter
  typed in the console, sent by an SDK, or written into a runbook all mean the
  same thing. It's off by default: the engine binary only writes logs and
  shouldn't link a regex engine.

  Also reachable from the MCP server (`dagron_get_run_logs`, plus filter
  arguments on `dagron_get_task_logs` — an agent triaging a failure now makes one
  call, not one per task) and both SDKs (`get_run_logs`/`getRunLogs`, filter
  kwargs on the task-log calls). Reference:
  [docs/API.md §Log filter](docs/API.md#log-filter).
- **`scripts/ossync-dryrun.py`** — run the OSS mirror sync locally before tagging.
  Stages exactly what would publish and runs four checks: the two fail-closed
  scans CI already enforces, plus **provenance** (private repo URL, monorepo
  paths, internal doc names) and **link closure**, which the CI scan cannot see
  because it matches tokens rather than meaning. All three of the problems it
  now catches were found by hand during 0.5.0: Argo CD examples pointing at the
  private monorepo, READMEs telling readers to `cd` into a directory the mirror
  does not have, and mirrored docs linking docs that were never included.

  It stages from `git ls-files`, not the working tree, because the real sync
  rsyncs from a CI checkout with no untracked or gitignored files — staging
  differently invents leaks that could never ship. Wired into the pre-publish
  checklist the release process runs.

### Changed

- **The console shell.** One accent instead of two competing ones (primary
  actions were blue while the brand and navigation were orange), a steel chrome
  layer around a dark content ground, and a density pass: page width 1180 →
  1560 so run and task tables stop being squeezed, sticky table headers, and a
  sticky sidebar. The overview now leads with **what needs attention** — failed
  runs, approvals waiting, dead letters, repos out of sync — and when that list
  is empty it names what it checked, so a quiet panel reads as verified rather
  than broken.

- **The dev `compose.yaml` configures the artifact store** (`DAGRON_ARTIFACT_DIR`)
  with a volume shared between the engine and `dagron-api`. Without it the
  checkpoint/resume and batch-inference examples failed immediately — correct
  behaviour, and a poor first meeting with the feature those examples exist to
  demonstrate.

### Fixed

- **Fonts are self-hosted.** The console `@import`ed a font family from
  `fonts.googleapis.com` at runtime — a render-blocking request it cannot make
  at all in an air-gapped install.
- **Keyboard focus is visible.** The console had no `:focus-visible` style
  anywhere, so every button, link and form control across all 21 routes was
  invisible to a keyboard user.
- **`--dim` failed AA.** The secondary text colour measured 4.0:1 on the page
  ground; it is now 5.1:1.
- **The sidebar version is the real one.** It read `v0.1.0` on every build,
  including released images: the version fell back to `frontend/package.json`,
  which nobody bumps, and the release workflow never passed `APP_VERSION`.
  Unstamped builds now say `dev` rather than inventing a number.
- **Sub-DAG nodes no longer overflow their box.** Graph nodes declared a fixed
  height that was right for a plain task and wrong for one carrying a middle
  row, so the bottom handle sat partway up the node. Layout also now reads
  React Flow's measured sizes rather than the pre-measurement declaration.
- **The log filter no longer queries mid-word.** Text filters commit on Enter or
  on leaving the box, with the edited field outlined until applied; a regex that
  does not parse yet is held back instead of spending a round trip that can only
  return 400.

- **The OSS release gate can build the mirror cut again.** It ran
  `cargo build --workspace`, which this workspace cannot satisfy *by design*:
  `dagron-core` compiles exactly one sqlx backend, the engine resolves `sqlite`,
  and `dagron-api`/`dagron-gitops` resolve `postgres` — `--workspace` unifies the
  features, enables both, and hits the `compile_error!`. It only started failing
  now because `dagron-api` gained its `dagron-core` dependency in the run-submit
  fix, after the last release. The gate (and the two docs that told you to run
  the same command) now builds the two feature worlds separately, which is how
  the images are built anyway. Every member is covered exactly once.
- **Mirror provenance scrub.** A dry run of the OSS sync — staging the include
  set exactly as the workflow does, from tracked files only — found leaks the
  fail-closed marker scan cannot see, because it matches tokens rather than
  meaning. The `examples/gitops/` Argo CD manifests pointed their `repoURL` and
  source paths at a repository that is not this one, so anyone applying them
  publicly got an auth failure against something they cannot see; the Python
  SDK's `Roadmap` URL did the same on PyPI. Both now point here, with
  repo-relative paths. `ARCHITECTURE.md`, `DLQ.md` and the backfill/monitoring/
  sdk examples told readers to `cd` into a directory this repository does not
  have. And ~16 files cited internal design documents a public reader cannot
  open — the explanations stay, the dangling citations are gone, along with a
  workflow comment that enumerated closed add-on images by name.

## [0.5.0] - 2026-07-26

### Added
- **The Helm chart can deploy the GitOps worker** (`gitops.enabled`, off by
  default — most installs never connect a repository). Until now the chart had no
  template for it, so a Helm install could register repos in the console and have
  nothing polling them. One replica by design: each would poll every repo against
  the same rows. Runs read-only-root like its siblings, with an emptyDir mounted
  at **`/tmp`** — the worker clones into `std::env::temp_dir()`, so mounting the
  scratch anywhere else leaves every sync failing on a read-only filesystem.
  Listed in the Artifact Hub image annotation, and `helm install` now says so when
  the worker is absent instead of leaving you to wonder why nothing syncs.

### Fixed
- **`docs/DATASETS.md` can be mirrored.** It linked a **private**
  a private internal roadmap doc and twice used the edition abbreviation the mirror's own
  fail-closed tripwire rejects, so it could never be published, while three mirrored
  files (CHANGELOG, `API.md`, `CONFIG.md`) linked to it. Scrubbed and included;
  the closed connector name it advertised is gone too.

### Fixed
- **The OSS mirror stops publishing dead documentation links.** `docs/OPERATIONS.md`
  is mirrored and had grown links to `BACKUP_RECOVERY.md`, `BACKUP_AWS.md` and the
  two `scripts/*.sh` backup helpers — none of which were in the `.ossync` include
  set, so every one of them 404'd on the public repo. `BACKUP_RECOVERY.md` and the
  scripts are now mirrored; `BACKUP_AWS.md` stays out (it walks through this repo's
  terraform, which is not published) and is referenced without a link. A link-closure
  check over the whole mirrored set — every relative link in every included file,
  resolved against the include list — is what found them, and the remaining
  offenders are all pre-existing.
- **A feature-on console no longer refuses specs the feature-on engine accepts.**
  `dagron-api`'s `enterprise` feature did not forward to `dagron-core` (it pinned
  `features = ["postgres", "ops"]`), so every dagron-core gate stayed shut in a
  feature-on build of the API. The visible symptom: a workflow using multi-dataset
  composition (`on_datasets: [a, b]` / `datasets_mode:`) submitted through the
  console was rejected with the gap signpost — pointing an operator at a capability
  they were already running — while the identical spec sent to the engine's
  ops API was accepted. Same cluster, two answers. A test now pins the console's
  answer to the engine's in both build shapes; it fails without the forwarding.
- **The GitOps worker is actually buildable and published.** #721 split GitOps
  into its own `dagron-gitops` crate + image, but the crate was never added to
  the workspace `members` and nothing path-depends on it — so
  `cargo check -p dagron-gitops` answered *"package ID specification
  `dagron-gitops` did not match any packages"*, and any cargo invocation quietly
  dropped it from `Cargo.lock`. The release workflow's image matrix listed four
  images and not this one, so a tag would have shipped the feature with no image
  to run it. Both fixed, plus a Docker Hub overview for the new public repo.

### Changed
- **Envelope encryption (BYOK/KMS) and encrypted artifacts at rest are
  feature-gated.** Drawn before 0.5.0 ships, deliberately: the KEK layer has never
  appeared in a tagged OSS release (`dagron-crypto/src/lib.rs` was 121 lines at
  `dagron-oss-v0.4.3` with no `KeyProvider`; it is 1468 now), so this is a line
  drawn rather than a takeback.

  **Open in every build, unchanged:** environment secrets encrypted with
  AES-256-GCM under `DAGRON_ENV_SECRET_KEY` — the `v1:` format the compose stack
  and [docs/HOWTO.md](docs/HOWTO.md) §5 use — and the plain artifact store.
  **Behind the feature:** KEK providers (AWS KMS / GCP KMS / Azure Key Vault / the
  command seam), envelope-wrapped data keys, artifact encryption at rest, and the
  store-wide rotation sweep (`POST /api/artifacts/rotate`, a route now absent from
  open builds).

  Enforced in one place — `dagron_crypto::build_provider`, the funnel every KEK
  consumer reaches through (env-secret `v2:` mode, artifact-at-rest, rotation) —
  so the line cannot drift apart across three subsystems. An open build handed a
  `DAGRON_ENV_KEK_PROVIDER` **errors with a signpost naming the edition and the
  open alternative**, rather than returning "no provider" and quietly encrypting
  with a single env-var key: silently downgrading a deployment that asked for KMS
  is not something anyone should discover from a ciphertext dump.

### Fixed
- **dagron-api: Argon2 password hashing no longer runs on the async runtime, and
  is bounded.** Measured on the compose stack: RSS went 67 MB → 187 MB after 20
  logins → 227 MB after 60, then plateaued; 30 concurrent SSE clients moved it
  3 MB, so streaming was never the cost. `Argon2::default()` is Argon2id with
  `m_cost = 19 MiB`, and glibc raises its mmap threshold after repeated
  allocations that size and keeps one arena per thread that ever hashed — with
  hashing done inline on tokio workers, that was ~19 MiB × worker-count resident
  *and* a worker pinned for the ~50–100 ms each hash takes, stalling every other
  request on that thread. All hashing now goes through one `pwhash` module: a
  semaphore (`DAGRON_PW_HASH_CONCURRENCY`, default 2 → ~38 MiB ceiling) taken
  *before* the work is handed to `spawn_blocking`. Parameters are unchanged —
  19 MiB is the OWASP floor and lowering it would weaken the hashes.

  The residency itself is glibc's dynamic mmap threshold: it grows as it sees
  repeated frees that size, after which the scratch comes off the heap and is
  never returned. `compose.yaml` pins `MALLOC_MMAP_THRESHOLD_=131072` (plus
  `MALLOC_ARENA_MAX=2` for the idle baseline). Measured over 30 logins:

  | | baseline | after |
  |---|---|---|
  | before | 67 MB | 227 MB |
  | arena cap only | 5.6 MB | 166 MB |
  | + threshold pinned | 5.6 MB | **5.9 MB** |
- **dagron-api: `POST /api/login` is rate limited** (`DAGRON_LOGIN_RATE_LIMIT`,
  default 30 per `DAGRON_LOGIN_RATE_WINDOW_SECS`=60, per client; `0` disables).
  It was the one unauthenticated route that spends ~19 MiB and ~100 ms of CPU per
  call — and spends it even for an email that doesn't exist, because the
  no-such-user path deliberately runs the same verify to avoid leaking which
  accounts are registered. That made it free memory/CPU amplification for anyone
  who could reach the port. Keyed on the socket peer (unforgeable) by default;
  `DAGRON_TRUST_PROXY_HEADERS=true` switches to the first `X-Forwarded-For` hop
  for deployments actually behind a proxy, which is what the console's `/api`
  rewrite is (set in `compose.yaml`). In-memory and per replica: a cost ceiling
  and a guessing brake, not a distributed lockout policy.

### Added
- **Point-in-time recovery on AWS** (`docs/BACKUP_AWS.md`, not mirrored — it references this repo's terraform) —
  the follow-on to the runbook's "nightly dumps are a floor, not a plan". Three
  concrete shapes with working config: RDS/Aurora managed PITR (terraform, and
  the `backup_retention_period = 0` footgun), CloudNativePG on EKS shipping WAL +
  base backups to S3 over IRSA, and pgBackRest on EC2. Includes the archiver
  heartbeat to alert on (`pg_stat_archiver.failed_count` — a permissions mistake
  in the verification drill produced `archived=6 failed=3` while the database
  accepted writes normally), and the dagron-specific steps after a restore:
  repoint `DATABASE_URL` and start one engine first, keep old
  `DAGRON_ENV_SECRET_KEY` versions for at least the PITR window (restored
  ciphertext predates a rotation and the current key will not decrypt it), expect
  the artifact store and GC archive to be inconsistent with the restored
  database, and cancel unsafe runs before the engine reclaims stale leases. The
  full PITR loop was executed end to end (base backup → data → target time →
  TRUNCATE → recover): recovery stopped before the offending transaction and the
  100 rows came back.
- **Backup / migration / disaster-recovery runbook**
  ([docs/BACKUP_RECOVERY.md](docs/BACKUP_RECOVERY.md)) plus
  `scripts/backup-postgres.sh` and `scripts/restore-postgres.sh`. Covers what is
  state (the database **and** `DAGRON_ENV_SECRET_KEY`, which lives outside it —
  lose the key and stored secrets are unrecoverable ciphertext), how migrations
  actually behave (embedded, applied at engine startup, no `dagron migrate`
  command, forward-only, two sets sharing one `_sqlx_migrations` ledger via
  `ignore_missing`), the production upgrade order, a restore rehearsal, and
  eight recovery playbooks. Every claim was executed against a live stack:
  dump/restore round-tripped identical counts (25 runs / 54 tasks / 42
  migrations), a tampered checksum produced
  `Error: migration 40 was previously applied but has been modified` with exit 1
  and an untouched database, and an injected version-900 row was absorbed by
  `ignore_missing` rather than blocking startup. Also documents the trap that
  dagron-api recreates its own tables empty, so a partial restore loses every
  user and secret without erroring.
- **Docs: where a long script lives** ([docs/HOWTO.md](docs/HOWTO.md) §6 +
  [examples/scripts/](examples/scripts/README.md)). A task's `command:` is argv
  and there is no `script:` field or file-include, which left "my script is 400
  lines" unanswered. Four worked patterns — bake into the image, absolute path on
  the engine host, fetch at run start, or the body in an env var — with the two
  facts that decide between them: only `command`/`env`/`docker_image` reach the
  task process (`input:` does not), and no executor mounts host paths, so
  `DAGRON_ARTIFACTS` passes files between host tasks but cannot carry a script
  into a container. All four validate through `dagron validate`; the env-var one
  runs with no setup.
- **Console: template calls (`templates:` / `template:`) are first-class.** The
  engine has expanded template calls since day one, but dagron-api's own spec
  mirror knew only `command` and `workflow_ref`, so a task calling a template was
  a 400 (`needs a command or a workflow_ref`) on **every** save/submit path —
  `examples/templates/01_dag_of_dags.yaml` could not be stored through the API at
  all, let alone opened in the editor. `TaskSpecInput` now carries
  `template`/`arguments` and `DagSpecInput` carries `templates`, with save-time
  validation the engine can't do lazily: template names unique and non-empty, no
  empty template, every call resolves to a declared template (spec-local, so a
  typo is caught on Save, not at run time), each template's own task list checked
  as a sub-graph, and a task is exactly one of leaf / call / chain. Two
  silent-drop paths are now explicit errors instead: a `workflow_ref` **inside** a
  template (the chain expander only walks top-level tasks) and a `workflow_ref`
  **to** a workflow that declares its own `templates:` (inlining copies tasks,
  not templates).
- **Console: the visual editor edits template calls.** A `template:` task renders
  as a dashed sub-DAG node (`⧉ etl · 3 tasks`); its panel has a **Calls template**
  picker over the declared templates and an **Arguments** row per parameter the
  chosen template declares (defaults as placeholders, unknown-but-present
  arguments kept and flagged). Leaf-only knobs (command, image, retries, timeout,
  trigger rule) are hidden on a call, which runs no command of its own. New
  **“DAG of DAGs (template call)”** starter and **Blocks → Control flow → “Sub-DAG
  (template call)”**, which scaffolds the `templates:` entry and its call together
  — a call is only valid alongside the template it names.

### Changed
- **The visual editor refuses to silently degrade.** The canvas draws one node
  per task, which is a lie for a spec using `with_items:`/`with_param:` (one node,
  N tasks), `hook:` (edges to every task, undrawn), `gang:`, `type: wait`/
  `workflow` (command-less kinds whose panel offered a command box), `cache:` or
  `resources:`. Loading such a spec now **disables the Visual tab** — "this
  workflow uses features the visual editor can't edit — use the YAML tab" — and
  lists the responsible fields. The rule is an allowlist, so a field the editor
  has never heard of (a new engine feature, a typo) locks the tab rather than
  becoming quietly editable; everything the block palette emits stays editable.
  Nothing is dropped either way — the spec is untouched and fully editable as
  YAML. Renaming a task in the visual editor now also follows `result_from:`,
  which otherwise saved a dangling reference.
- **Datasets: data-aware scheduling** (Airflow Datasets / Dagster asset
  sensors — the #27 dataset-sensor follow-on plus the round-2 `dataset://`
  stretch slice, as one subsystem; [docs/DATASETS.md](docs/DATASETS.md)). Three
  pieces on a shared ledger:
  - **`produces: [<uri>, …]`** on a command task — on success the engine
    upserts each URI in the new `datasets` registry and appends a
    `dataset_events` lineage row (producing workflow/run/task/time): the
    cross-workflow update trail, queryable via `GET /datasets` and
    `GET /datasets/events?uri=…`. URIs template per fan-out instance. Recording
    is fence-guarded (a reclaimed task's late result can't fabricate lineage)
    and best-effort (a ledger failure never fails the run).
  - **`wait: { dataset: <uri> }`** — a dataset sensor on the #27 park
    machinery (running + NULL lease, no worker slot, **no new status**): new
    `task_runs.wait_dataset` + `wait_dataset_cursor` columns hold the ledger's
    high-water mark at park time, so only an update recorded *after* the park
    resolves it (fresh data, never history). Joins `for`/`until`/`url` in the
    exactly-one-of rule.
  - **`on_datasets: [<uri>]`** on a registered workflow — dataset-triggered
    scheduling: a sweep (~5 s, `DATASET_TRIGGERS=0` opts out) syncs
    subscriptions into the new `dataset_triggers` table and fires a run when a
    subscribed dataset updates, injecting `{{ trigger_dataset }}`. Firing is a
    **CAS cursor advance** — HA-safe with no leadership, exactly one scheduler
    wins each fire; registration never fires on history; rapid updates coalesce
    into one run; a fire refused at `max_active_runs` rolls its cursor back and
    retries. SQLite migrations 032–033, Postgres 039–040; metrics
    `scheduler_dataset_updates_total` / `scheduler_dataset_fires_total`.
  - **Scope of this build**: the single-team loop —
    produce, lineage, sensor, single-dataset trigger — is complete here.
    **Multi-dataset composition** (`on_datasets: [a, b, …]` +
    `datasets_mode: any|all` fan-in) and **external dataset events**
    (`POST /datasets/events`, for CDC/object-store producers outside dagron)
    are not in this build; it answers both with signpost
    errors naming what is missing and the workaround (the SOURCE-connector
    funnel pattern).
- **HTTP wait sensor: `wait.url`** (parity fast-win #27 follow-on — Airflow
  `HttpSensor`) — a task `{ type: wait, wait: { url: <http(s)-endpoint> } }` parks
  holding **no worker slot** and the engine GETs the endpoint every
  `WAIT_POLL_SECS` (default 15 s), succeeding the task on the first `2xx` so its
  dependents advance. Reuses the #27 park mechanics: the parked task stays
  `running` with `claimed_by`/`lease_expires_at` NULL and the endpoint in a new
  `task_runs.wait_url` column with a `next_poll_at` throttle (SQLite migration
  031, Postgres 038) — so the claim scan skips it and lease recovery leaves it
  alone, needing no new status. A non-2xx / errored poll re-parks it for the next
  window; the per-request timeout keeps a hung endpoint from stalling the tick.
  Bounded by the run's own `run_timeout_secs`. `for` / `until` / `url` are
  mutually exclusive (exactly one). The `url` may template (`workflow parameters`).
  (A fourth form, `wait: { dataset: … }`, ships in the datasets entry above, so
  the exactly-one-of rule is over four fields, not three.)
- **OpenTelemetry OTLP span exporter** (parity fast-win #28 follow-on) — with an
  `otel`-feature build **and** `OTEL_EXPORTER_OTLP_ENDPOINT` set, `dagron-logging`
  now installs an OTLP **HTTP/protobuf** span exporter + a `tracing-opentelemetry`
  bridge layer, so the engine's own `tracing` spans are delivered to a collector.
  Each dispatched task is wrapped in a `task.dispatch` span (dependency-free,
  always on under `otel`) so there's a span per task to export. Transport is
  HTTP/protobuf over a blocking `reqwest`/rustls client — the gRPC/tonic
  transport is deliberately not pulled in; endpoint/headers/timeouts come from the
  standard `OTEL_EXPORTER_OTLP_*` env vars. Off by default in every sense: no
  opentelemetry dependency without the feature, and no exporter without the
  endpoint (spans still propagate, just aren't exported). Dashboards/sampling/
  retention remain observability's per `SCOPE.md`.
- **OpenTelemetry trace-context propagation** (parity fast-win #28 — Dagster
  #12353 / Prefect #9271 / Airflow external-trace #69633) — built with the new
  `otel` Cargo feature, the engine injects a fresh W3C `traceparent` into every
  dispatched task's env (`TRACEPARENT`), so the task's own OpenTelemetry
  instrumentation joins an external trace (external-trace embedding). The trace
  id is logged on dispatch for log↔trace correlation. Dependency-free and off by
  default — a default build never sets `TRACEPARENT`, unchanged behavior. The
  OTLP span exporter (span **delivery**) ships as the follow-on above.
- **Deferrable wait sensor: `type: wait`** (parity fast-win #27 — Airflow
  deferrable time sensors / Argo suspend-with-duration) — a task
  `{ type: wait, wait: { for: <duration> } }` (or `{ until: <rfc3339> }`) parks
  holding **no worker slot** until its deadline, then succeeds and its dependents
  advance. `for` is a relative duration (`30s`/`5m`/`2h`) anchored when the task
  is reached; `until` is an absolute instant. Reuses the #23 park mechanics: the
  parked task stays `running` with `claimed_by`/`lease_expires_at` NULL and its
  resume time in a new `task_runs.wake_at` column (SQLite migration 030, Postgres
  037) — so the claim scan skips it and lease recovery leaves it alone, needing
  no new status. A reconcile sweep resolves due sensors each tick (idempotent,
  HA-safe); a deadline already in the past resolves at once. The HTTP (`url`)
  sensor ships as a follow-on above; dataset sensors remain a follow-on.
- **Sub-workflow trigger: `type: workflow`** (parity fast-win #23 — Airflow
  TriggerDagRunOperator / Argo #6922 workflow-of-workflows) — a task
  `{ type: workflow, workflow: <name> }` submits the named **registered**
  workflow as a child run when reached, then parks until the child is terminal:
  the parent succeeds if the child succeeded and fails if it failed/cancelled,
  after which its dependents advance. The parked task stays `running` with
  `claimed_by`/`lease_expires_at` NULL and the child id in a new
  `task_runs.sub_run_id` column (SQLite migration 029, Postgres 036) — so the
  claim scan never re-picks it and lease recovery never reclaims it, needing **no
  new status value and no status-CHECK rebuild**. A reconcile sweep resolves
  parked triggers each tick (idempotent, HA-safe). If the child is at its own
  `max_active_runs` cap the trigger is released back to `ready` and retries; an
  unknown or unparseable child fails the task. Nesting is bounded by
  `SUBWORKFLOW_MAX_DEPTH` (default 8) — a workflow that names itself, directly
  or around a cycle, fails the offending task instead of spawning child runs
  without end (see **Security** below); the global `max_inflight_runs`
  admission valve caps concurrent depth on top of that.
- **Workflow tags** (parity fast-win #26 — Airflow #16432 colored tags / #24464
  folder view, Dagster #14530) — a DAG may declare `tags: [..]` (each
  `[A-Za-z0-9_.-]`, ≤64 chars) to organize and filter workflows. The registry
  surfaces them on `GET /api/workflows` and `GET /api/workflows/:id`, and
  `GET /api/workflows?tag=<t>` returns only workflows carrying that tag. Tags are
  **parsed from the stored spec on read** — no denormalized column to keep in
  sync, so they always reflect the current definition — and validated when the
  engine loads the spec. The **Workflows UI** renders per-tag colored chips
  (deterministic color per tag) in the table and board views; clicking a chip
  filters the list to that tag, and tags join the text search. Purely
  organizational; the engine ignores them.
- **Result memoization: `cache`** (parity fast-win #22 — Argo memoization /
  Prefect task caching) — a task may declare `cache: { key, max_age_secs? }`. A
  successful run stores its output keyed by `(workflow, task, resolved key)`; a
  later task whose key matches reuses that output and **skips execution
  entirely** — no worker, no secrets, no artifacts — then its dependents advance
  as for a normal success. The `key` is a template resolved at expansion, so
  `{{ scheduled_time }}` / `workflow parameters` make repeated and backfilled runs hit
  the cache (reproducible backfills). `max_age_secs` expires stale entries so the
  task re-runs and refreshes. New `task_memo` table (SQLite 028, Postgres 035);
  the memo write reuses the per-success spec parse the `repeat` operator already
  does, so uncached tasks pay nothing; `scheduler_cache_hits_total` metric. No
  behavior change without a `cache:` block.
- **Named concurrency pools: `pool`** (parity fast-win #21, second slice —
  Airflow #13975 "multiple pools" / Argo Kueue) — a task may name a `pool:`, and
  a scheduler claims a pooled task only while fewer than the pool's capacity are
  already running in it (capacities from the `POOLS` env, e.g. `POOLS=etl:4,db:2`).
  An over-budget task simply waits in `ready` until a slot frees — no run is
  dropped and there is no new run state — because the ready-claim itself is the
  enforcement point (`task_runs.pool`; SQLite migration 027, Postgres 033 column
  / 034 `CREATE INDEX CONCURRENTLY`). **When `POOLS` is unset the claim path is
  byte-for-byte unchanged** (zero cost/risk for deployments not using pools). On
  Postgres, unpooled/uncapped tasks keep the lock-free `FOR UPDATE SKIP LOCKED`
  fast path; only capped-pool claims serialize, under a global advisory lock, so
  concurrent controllers can't over-commit a pool's slots (SQLite is single-writer,
  so its one write-tx claim is already race-free). DAG-wide default via
  `task_defaults.pool`; fan-out instances inherit the parent's pool; names are
  validated `[a-z0-9_-]{1,64}`. Together with `max_active_runs` (run-level), this
  completes #21's concurrency-control story: per-run and per-task-pool limits.
- **Per-workflow concurrency limit: `max_active_runs`** (parity fast-win #21,
  first slice — Argo #12757 workflow concurrency control / Prefect deployment
  concurrency limits) — a DAG may set `max_active_runs: <n>` to cap how many of
  its runs (matched by workflow name) are `running` at once. `create_run`
  enforces it at the single choke point every fire path flows through, so the
  cap holds for API submits, cron/DB schedules, backfills, catch-up, queue
  sources, and dead-letter redrive alike — with **no change to the hot task-claim
  path**. Over the cap, `create_run` returns a typed `MaxActiveRunsReached`: the
  API answers **429 + Retry-After** (not 500), a queue source **requeues** the
  message instead of dead-lettering it, and schedule/backfill fires are held back
  (backfill releases its slot and retries when capacity frees). The count filters
  the small set of `running` runs and PK-joins their definitions — no schema
  change, no migration. `None`/`0` = unlimited (unchanged behavior). Caps
  concurrent *runs*, not per-task concurrency (named task pools are a follow-on).
- **Cause-conditional retry: `retry_on_timeout`** (parity fast-win #24 —
  Airflow #9232) — a task may set `retry_on_timeout: false` so that a run killed
  by its `timeout_secs` deadline **fails immediately** instead of burning the
  rest of its `max_attempts` (a deadline kill usually recurs, so the retries just
  delay the failure). Timeout-only: a non-zero exit or a backend error still
  retries under the normal attempts rule. Every executor backend (local, Docker,
  Kubernetes) now aborts a deadline via a typed `TimeoutError` that the worker
  downcasts, so the reconcile loop distinguishes a timeout from any other
  failure; the terminal-fail log records `timed_out` / `not_retried_due_to_timeout`.
  DAG-wide default via `task_defaults.retry_on_timeout` (a task's own value wins).
  Default `true` = unchanged behavior. Pure logic — no migration.
- **Task dispatch priority** (parity fast-win #25 — Airflow `priority_weight` /
  Argo Kueue analog) — a task may declare `priority: <int>` (default `0`); among
  the tasks that are `ready` at the same moment a scheduler claims higher
  priority first (`claim_ready` now orders `priority DESC, scheduled_at`), so a
  latency-sensitive branch jumps a deep backlog of low-priority work. A pure
  tiebreak: it never lets a task run before its dependencies, and it persists on
  the row so a retry / lease recovery keeps its place. A DAG-wide default is
  settable via `task_defaults.priority` (a task's own non-zero value wins);
  fan-out instances inherit their parent's priority. `task_runs.priority`
  (SQLite migration 026 + partial ready index; Postgres 031 column / 032
  `CREATE INDEX CONCURRENTLY`), threaded through both DB backends. No behavior
  change for existing/unprioritized workflows.
- **Gang co-scheduling schema (`gang: {size: N}`)** — a task expands at run
  creation into N member rows (`<name>.<rank>`, shared gang id), dependents
  wait for every member, and members receive `DAGRON_GANG_ID` /
  `DAGRON_GANG_RANK` / `DAGRON_GANG_SIZE` for rendezvous — in **any** build, so
  a member always knows its rank however it was scheduled. Validation enforces
  leaf, single-attempt gang tasks (die-together retry is a unit-level rerun).
  The all-or-nothing gang claimer (`RUNNER_GANGS=1` — claim a gang only when
  every member is ready and capacity fits it whole, cancel a failed member's
  siblings) is not in this build; without it, members
  schedule as ordinary tasks. Placement sweeps leave gang members in place.
- **Per-partition range leases + sharded streams (multi-consumer)** — a
  `source_partitions` lease table with claim/renew/release primitives (both
  backends) lets N engines split one logical stream, one consumer per
  partition, heartbeat-renewed and rebalanced when a consumer dies. Committed
  positions namespace per shard (`<source>/<partition>` in `source_offsets`)
  via the new `PendingPosition.substream`, so exactly-once holds per
  partition. Reference consumer: point `STREAM_PATH` at a **directory** and
  each `*.ndjson` file becomes a leased shard (`STREAM_SUFFIX`,
  `STREAM_MAX_PARTITIONS`) — the broker-free consumer group.
- **Exactly-once streaming ingestion (transactional source offsets)** — a
  source's resumable coordinate (byte offset, broker offset, replication LSN)
  now commits **in the same datastore transaction** as the run — or dead
  letter — it accounts for (`source_offsets` table; `create_run_with_offset`
  / `record_dead_letter_with_offset`), and the ingest actor hands the
  committed cursor back to the source at startup. A crash-replay of the whole
  stream creates zero duplicate runs and zero duplicate dead letters. Sources
  opt in via two new defaulted `WorkflowSource` methods (`pending_position`,
  `set_committed_position`) — existing sources are untouched; `SOURCE=stream`
  implements them (its offset file remains as a trailing mirror).
- **Cloud artifact stores: S3 / GCS / Azure Blob** — `DAGRON_ARTIFACT_URL=
  s3://bucket/prefix` (or `gs://`, `az://`; S3-compatible MinIO/Ceph via
  `AWS_ENDPOINT_URL`) selects an `object_store`-backed `ArtifactStore`
  (features `s3`/`gcs`/`azure` on `dagron-artifact`, forwarded by the
  `archive-*` features on the API binary). Same sanitized
  `<run>/<task>/<name>` layout as the local store, bounded-memory multipart
  `put_stream` (aborted on failure), streaming `get_stream`, listing (so
  KEK rotation sweeps buckets too), and transparent composition with the
  BYOK/KMS `EncryptedStore` — ciphertext in the bucket. The engine injects
  per-task `DAGRON_ARTIFACTS_URL` / `DAGRON_CHECKPOINT_URL` so checkpoints
  written on one machine resume on any other that can reach the bucket
  (multi-cloud checkpoint resume); a URL in a build without the matching
  backend feature errors loudly instead of falling back to local.
- **Streaming ingestion built in: `SOURCE=stream`** — follow an append-only
  NDJSON event file or named pipe, one workflow submission per line, with
  at-least-once delivery off a durable byte-offset checkpoint (committed only
  after the run is created; delete the offset file to replay, or
  `STREAM_FOLLOW=false` to drain a backlog and exit). Poison lines are
  dead-lettered (durable row + a `.dlq` NDJSON mirror) instead of wedging the
  stream. Selecting a managed connector kind (`kafka`/`nats`/`sqs`/`redis`/
  `events`) now errors with an accurate signpost naming the missing
  connector suite and the built-in alternatives. Guide: `docs/STREAMING.md`;
  runnable case studies: `examples/streaming/`.
- **Long-running tasks: lease heartbeat** — workers renew a running task's
  lease every 10 s (claim triple–guarded), so a task may run for hours under
  the same short-lease crash recovery; a worker that dies stops heartbeating
  and its task is reclaimed exactly as before. Losing the claim aborts the
  local execution (the reclaimer owns the task). Opt out with
  `TASK_LEASE_HEARTBEAT=false`.
- **Checkpoint-aware resume for retries** — a running task reports its
  committed checkpoint (`POST /runs/{id}/tasks/{task_id}/checkpoint` with its
  injected `DAGRON_RUN_ID`/`DAGRON_TASK_ID`, or the
  `$DAGRON_CHECKPOINT_DIR/latest` file convention); the pointer survives
  retries and *Rerun failed*, is cleared by *Clear task*, and the next attempt
  is dispatched with `DAGRON_RESUME_FROM` / `DAGRON_RESUME_MARKER` — resume
  from epoch N, not epoch 0. Tasks also now receive their identity
  (`DAGRON_RUN_ID`, `DAGRON_TASK`, `DAGRON_TASK_ID`) in env. Guide:
  `docs/AI_WORKLOADS.md`; runnable case studies: `examples/ai/`.
- **GPU resource sugar** — `resources: { gpu: { count: N, resource: … } }`
  expands to the Kubernetes extended-resource limit
  (default `nvidia.com/gpu`); an explicit `limits` entry for the same key
  wins. Validation rejects `count: 0`.

### Security
- **Sub-workflow nesting is bounded (`SUBWORKFLOW_MAX_DEPTH`, default 8).** A
  `type: workflow` task that names its own workflow — directly or around a cycle
  — spawned child runs without end, each leaving a parked parent row behind.
  There is no recursion on a stack here, so nothing failed: it was an unbounded
  run factory that a single spec could start. A trigger at or past the cap now
  fails that task with a message naming the depth. Depth is read by walking
  `task_runs.sub_run_id` up from the triggering task's own run — the parentage
  the parking column already records — so no `parent_run_id` column and no
  backfill are needed; SQLite migration 034 / Postgres 041 add the partial index
  that makes each hop a lookup (and also covers the reconcile sweep's existing
  forward read). The walk stops at the cap, so the check is O(depth) and
  terminates even if the column ever describes a cycle.
- **`wait.url` sensors no longer follow redirects.** A `wait.url` poll is issued
  by the **scheduler**, not the task sandbox, so an external endpoint answering
  `302 http://169.254.169.254/…` could previously use the scheduler as a deputy
  to reach addresses a task pod may not. The scheme check at validation time
  cannot see a redirect target; the client now declines to follow one, and a 3xx
  simply reads as "not ready" and re-parks. A failed client build is now a
  startup error rather than a silent fall back to a default (redirect-following,
  untimed) client.
- **Optional `WAIT_URL_DENY_PRIVATE=1`** restricts `wait.url` polls to
  globally-routable addresses — private/loopback/link-local (incl. the cloud
  metadata address), CGNAT, multicast and reserved ranges are refused across
  IPv4, IPv6 and IPv4-mapped IPv6. **Off by default on purpose:** the common
  `wait.url` *is* an internal address (`http://svc.default.svc/ready`), so a
  deny-by-default would break the primary use case to harden the case where the
  scheduler's network position is genuinely wider than the executor's
  (`EXECUTOR=kubernetes` with a differentiated NetworkPolicy). Enforcement is
  split so neither half is bypassable: IP-literal hosts are checked before the
  request (a literal never reaches a resolver), and names are filtered **inside
  the client's DNS resolver**, so the addresses dialed are exactly the ones that
  passed the filter — no check-to-connect window for a DNS rebind. Proxies are
  bypassed (`no_proxy`) while the policy is on: `HTTP_PROXY`/`HTTPS_PROXY` would
  otherwise mean the resolver only ever judges the *proxy's* address while the
  proxy dials the blocked target. A refused URL logs a WARN and re-parks as "not
  ready" rather than failing the task, so relaxing the policy resolves parked
  sensors in place. Editing the workflow spec does not: the endpoint is
  materialized into `task_runs.wait_url` when the task row is created, so a spec
  fix applies to the next run, not to a task already parked on the old URL.

## [0.4.0] - 2026-07-23

### Added
- **Live-updating UI with a global pause toggle** — the Runs, Workflows, and
  Overview pages now refresh in realtime off a new account-wide SSE endpoint
  (`GET /api/events/stream`, fanned out from the same shared Postgres
  `LISTEN task_events` as the per-run stream), replacing Overview's fixed 5s
  poll with event-driven refetches (debounced 400ms, flushed at least every 2s
  during bursts; a slow 30s poll keeps GitOps sync state and next-fire times
  fresh). A **Live/Paused pill** on each page header (and the run detail
  header) toggles one persisted preference (`localStorage`, synced across tabs
  and pages): paused holds no streams open and does zero background reads —
  data loads once, with a ⟳ manual-refresh button — for sessions where live
  reads are too costly; resuming (or an SSE reconnect) refetches to catch up.
  Hidden tabs cost nothing either: streams close on tab hide and reopen on
  re-show (with a catch-up refetch), and Overview's slow poll skips ticks
  while hidden.
- **DAG layout directions** (frontend) — the DAG graph (Submit live preview and
  the run viewer) now offers three layouts via a ↓/→/↘ segmented control on the
  canvas: vertical (top→bottom, the old default), horizontal (left→right, with
  edges leaving node sides), and a diagonal cascade (ranks step down-and-right
  like a staircase). The choice persists in localStorage and follows the user
  across views.
- **Resizable editor/preview split** (frontend) — the border between the YAML
  editor and the live DAG preview on Submit (and the rerun dialog) is now a
  draggable divider: slide it to grow either pane (clamped 20–80%), double-click
  to reset to even, and the ratio persists across sessions.
- **Offline UI assets (no CDN)** — the frontend self-hosts the Monaco editor
  (staged into `public/monaco/vs` at build, no `cdn.jsdelivr.net`) and the engine
  serves vendored Swagger UI assets at `/docs`, so no browser asset needs a CDN.
- **Native GCS + Azure Blob archive backends** (multi-cloud) — the GC archive
  sink, `dagron archive-compact`, and the `dagron-api` archive-history reads now
  speak `gs://` (feature `archive-gcs`) and `az://`/`azure://` (`archive-azure`)
  natively, alongside `s3://` (`archive-s3`, which also covers S3-compatible
  MinIO/Ceph). `GC_ARCHIVE_URL`'s scheme selects the backend; credentials come
  from each backend's standard env (`AWS_*`/`GOOGLE_*`/`AZURE_*`). URL→store
  dispatch is centralized in a small `objstore` module in both crates; a scheme
  whose backend feature isn't compiled in is a hard startup error, never a
  silent plain purge.
- **Archive history reads** — the archive-before-purge GC now upserts an
  `archived_runs` index row (run_id, name, status, timestamps; SQLite
  migration 020 / Postgres 024) before purging, and `dagron-api` gains
  `GET /api/archive/runs` (index-only list, filter by `name`, paged) and
  `GET /api/archive/runs/{id}` (fetches the run's `dagron.run-archive.v1`
  document from `GC_ARCHIVE_DIR`/`GC_ARCHIVE_URL`; the api's `archive-s3`
  feature mirrors the engine's). Purge now fails closed on an index-write
  failure too — an archived-but-unlisted run would be invisible history.
- **Parquet compaction** (`dagron archive-compact`, cargo feature
  `archive-parquet`) — a bounded, CronJob-shaped sweep that folds archived
  `run-*.json` documents older than `GC_ARCHIVE_COMPACT_MIN_AGE_DAYS`
  (default 30) into a date-partitioned Parquet dataset
  (`compact/tasks/dt=<date>/part-<uuid>.parquet`, one row per task with run
  columns denormalized), stamps `archived_runs.compacted_at`/`parquet_path`,
  and deletes source documents **only after** the part file verifiably
  landed (at-least-once across crashes — dedup on `(run_id, task_id)` when
  querying). Works over both the dir sink and, with `archive-s3`, the S3
  sink. A compacted run's detail endpoint answers 410 Gone with the
  `parquet_path`.
- **Split LISTEN DSN** (`DATABASE_LISTEN_URL`) — the engine's reconcile-loop
  `Waker` and dagron-api's shared SSE listener connect their session-scoped
  `LISTEN` to this URL when set (the direct Postgres endpoint), while
  `DATABASE_URL` may point at transaction-mode PgBouncer — which cannot serve
  `LISTEN`. Unset = the listener shares the pool config, exactly as before.
  Unlocks pooled query traffic on shared state-store cells.
- **S3-native GC archive sink** (`GC_ARCHIVE_URL=s3://bucket[/prefix]`,
  cargo feature `archive-s3`) — archive-before-purge without the intermediary
  volume: each expired run's `dagron.run-archive.v1` document is one atomic S3
  `PUT` (credentials/region/endpoint from standard `AWS_*` env), and only
  verified PUTs are purged. Setting the URL without the feature is a startup
  error (same contract as `EXECUTOR=kubernetes` without `kubernetes`), and
  `GC_ARCHIVE_DIR` local archiving is unchanged.
- **Runner-class routing** (`runner_class`) — segment the scheduler fleet by
  workload shape. A task (or a whole DAG via a spec-level default) may name a
  **runner class** (`[a-z0-9_-]{1,64}`, default `default`); the class is
  persisted on `task_runs` (SQLite migration 019 / Postgres 022, with a
  class-scoped partial ready-index) and a scheduler started with
  `RUNNER_CLASSES=etl,pulse` claims **only** those classes
  (`db::claim_ready_classes`, both backends — same `SKIP LOCKED`/CAS + lease +
  fence contract). Unset `RUNNER_CLASSES` claims every class, so existing
  single-pool deployments are unchanged. Template expansion substitutes
  `{{ param }}`s in `runner_class` like `docker_image`; invalid names fail
  validation at submit (spec) or startup (env). SDKs:
  `Dag(name, runner_class=...)` / `task(..., runner_class=...)` (Python),
  `new Dag(name, {runnerClass})` / `task(..., {runnerClass})` (TypeScript).
- **`DB_MAX_CONNECTIONS`** — the Postgres pool size (previously hard-coded 8)
  is env-tunable (min 2), so many lean engines can share one pooled state
  cluster.
- **Archive-before-purge retention GC** (`GC_ARCHIVE_DIR`) — with an archive
  directory configured, the leadership-gated GC sweep first exports each
  expired terminal run as a self-contained JSON document
  (`dagron.run-archive.v1`: run + definition + task rows + outbox events),
  written atomically (tmp → fsync → rename), and purges **only** the runs
  whose export verifiably landed (`db::archivable_runs` +
  `db::purge_runs_by_id`, both backends). A failed write keeps the run in the
  hot store; re-archiving after a crash is idempotent. Unset, GC behaves
  exactly as before.
- **Per-class backlog gauges + stale-ready alert** — `/metrics` now exposes
  `scheduler_ready_tasks_by_class{runner_class=…}` and
  `scheduler_ready_oldest_age_seconds{runner_class=…}`, and a leadership-gated
  loop WARNs when a class's oldest ready task waits longer than
  `READY_AGE_ALERT_SECS` (default 300 s; `0` disables;
  `READY_AGE_CHECK_INTERVAL_SECS` tunes the cadence). Catches the
  segmentation footgun where every scheduler's `RUNNER_CLASSES` excludes a
  class and its tasks age silently.

### Fixed
- **Approval-gate timeouts in lean builds** — `db::resolve_approval` /
  `db::resolve_expired_approvals` were `ops`-gated while the reconcile loop
  calls the sweep unconditionally, so an engine built without the `ops`
  feature failed to compile (and, conceptually, a lean engine would never
  fail-safe an expired gate). Both are now available in every build.
- **SDKs cover the 0.3.0 API.** The Python SDK (`dagron` 0.3.0) gains `approve_task`
  / `reject_task` for `type: approval` gates, the durable backfill-job methods
  (`create_backfill` / `list_backfills` / `get_backfill` / `cancel_backfill` over
  `/api/backfills`), and an optional `path` on `connect_git_repo`. The TypeScript
  SDK (`@dagron/sdk` 0.3.0) gains a full `Client` class (it previously shipped only
  the `Dag` builder) mirroring the Python client's whole surface, including the same
  new 0.3.0 methods and an SSE `streamRun` / `waitForRun`. (The TS package version
  jumps `0.1.1 → 0.3.0`, skipping `0.2.0`, to line up with the dagron-api / Python
  SDK version — no `0.2.0` TS release was lost.)

## [0.4.1] - 2026-07-23

### Added
- **Artifact Hub-ready OSS Helm chart** — chart annotations/metadata so the
  published OCI chart is listed and browsable.

## [0.4.2] - 2026-07-23

### Added
- **Task-oriented [`HOWTO.md`](docs/HOWTO.md)**, and the reference docs
  (`API`/`CONFIG`/`OPERATIONS`/`MCP`) added to the OSS mirror.
- Artifact Hub badge on the README; the Grafana dashboard screenshot committed
  (the repo-wide `*.png` ignore had silently dropped it).

### Fixed
- **Frontend image CVEs** — dependency bumps and a distroless runtime.
- Open-core framing scrubbed from the mirrored docs.

## [0.4.3] - 2026-07-23

### Added
- **`mancube/dagron-mcp`** — the OSS MCP server image (eight stdio tools over the
  same JWT-gated API the console uses).
- Product landing site (S3 + CloudFront + Route53) and Artifact Hub repo metadata.

### Fixed
- **CRITICAL CVE in the frontend image** — runtime moved to a continuously
  patched base.

## [0.3.0] - 2026-07-08

### Added
- **Human approval tasks** (`type: approval`) — a task can now be a **human
  gate**: when its dependencies are
  satisfied it parks in a new `awaiting_approval` status (never claimed by a
  worker, so it needs no command) and the run waits until an operator approves or
  rejects it. `POST /runs/{id}/tasks/{task_id}/approve` (→ the task succeeds and
  the DAG proceeds) and `.../reject` (→ it fails, so `all_success` downstream
  skips), mirrored on the UI edge (`POST /api/runs/:id/tasks/:tid/approve|reject`).
  A gate may set `approval_timeout_secs` with `approval_on_timeout: approve|reject`
  (default **reject** — a gate fails safe): a reconcile-loop sweep auto-resolves
  an expired gate. Reuses the trigger-rule dependency model (approve decrements
  dependents like any success; reject like any failure). New `awaiting_approval`
  task status + `task_runs.is_approval` / `approval_timeout_secs` /
  `approval_on_timeout` columns (SQLite migration 018 — a table rebuild to widen
  the status CHECK — Postgres 021). Named approvers/groups, notifications, and
  audit build on this primitive behind the `enterprise` feature.
- **Backfill as a first-class API object** — a
  date-range backfill is now a durable, listable, monitorable, cancellable
  *job* the scheduler **paces**, instead of the synchronous capped
  `POST /schedules/:id/backfill` that materializes a whole window in one call.
  `POST /api/backfills` (`{schedule_id, from, to, max_runs?}`) snapshots the
  schedule's cron + timezone + workflow spec into a `backfills` row; the engine's
  leadership-gated pacer fires a bounded number of the range's fire-times per
  tick (default 20, `BACKFILL_PACE_PER_TICK`), advancing a cursor, so a large
  backfill drips into the cluster over many ticks rather than stampeding it —
  and the paced job can cover far more than the synchronous endpoint's 1000-run
  cap (job cap 100k). `GET /api/backfills` (list, `?schedule_id=`),
  `GET /api/backfills/:id` (monitor `fired`/`requested`/`status`), and
  `POST /api/backfills/:id/cancel` (stop pacing) round out the lifecycle. Runs
  are still deduped through the shared `schedule_backfills` ledger (a job never
  double-runs a slot a manual/auto backfill already materialized) and each
  backfilled run gets its logical date as `{{ scheduled_time }}`. New `backfills`
  table (SQLite migration 017, Postgres 020).
- **Live log tailing** — a running task's output is now visible *as it runs*,
  not only after it exits. `LocalExecutor` streams each stdout line to the
  datastore mid-run (fence-guarded, so a stale attempt can't corrupt a re-run;
  secrets are masked per-chunk like the final output, #8), and the task-logs
  endpoints gained an `?offset=` tail: `GET /runs/{id}/tasks/{task_id}/logs`
  (engine ops) and `GET /api/runs/:id/tasks/:tid/logs` (UI edge) return only the
  output past a character offset plus `next_offset` (resume point) and `eof`
  (task terminal), so a client polls with `?offset=next_offset` until `eof`.
  Offsets are Unicode-scalar counts (never split a multibyte character). No
  schema change — appends reuse the existing `task_runs.output` column. Docker
  surfaces its captured output through the same tail path (true mid-run
  `follow: true` is a follow-up); Kubernetes is unchanged (output at completion).
- **Synchronous invocation + run results** (`result_from`) — makes dagron
  callable as a durable
  function. A workflow can name the task whose output *is* the run's result with
  `result_from: <task>`; on success the engine copies that task's output into
  `workflow_runs.output`. Two ways to get it back synchronously: `POST /runs?
  wait=true` blocks until the run is terminal and returns `{run_id, status,
  finished, result}` (200, not 201) instead of just the id; `GET /runs/{id}/wait`
  long-polls an already-submitted run to completion. Both take `?timeout_secs=`
  (default 30, clamp 1–600); a timed-out wait returns `finished: false` with the
  live status so the caller re-polls (not an error). Mirrored on the `dagron-api`
  UI edge (`GET /api/runs/:id/wait` + `result_from` on submit). New nullable
  `workflow_runs.result_from` column (SQLite migration 016, Postgres 019);
  `result_from` must name a real, non-hook task (rejected at parse time).
- **Clear task + downstream** — a new
  recovery verb that re-runs a *single completed task together with everything
  that transitively depends on it*, without re-running the whole DAG or waiting
  for a failure. `POST /runs/{id}/tasks/{task_id}/clear` (engine ops) and
  `POST /api/runs/:id/tasks/:tid/clear` (UI edge) reset the target and its
  downstream cone from any terminal state (`succeeded`/`failed`/`skipped`/
  `cancelled`) back to `pending`, recompute each reset task's `remaining_deps`,
  bump `version` to fence stale workers, and re-arm a finished run so the
  reconcile loop resumes — while ancestors and unrelated branches stay intact.
  Use it to pick up a fixed input on a *green* node (which `rerun-from-failed`
  can't reach, since that only resets the failure frontier). 404 for an unknown
  run/task, 409 if the task is still running/pending. Reuses the existing
  fencing + `remaining_deps` model, so no schema change.
- **Deadline alerts** (`deadline: { in: 45m }`) — a soft
  SLA on a run: when a still-running run passes it, the engine emits a
  `run.deadline_exceeded` event to the transactional outbox (drained by the
  outbox delivery worker for webhook/Slack) and bumps `scheduler_deadline_alerts_total`,
  **without** cancelling — unlike `run_timeout_secs`, which fails the run.
  Fire-once and winner-take-all across schedulers. New `alert_deadline_at` /
  `alert_fired_at` columns (SQLite migration 015, Postgres 018); a shared
  duration parser accepts `45m` / `2h` / `90s` / `1d` / bare seconds.
- **Lifecycle hooks + `allow_failure`** — `hook: on_exit` makes a task a
  finalizer that runs once every
  non-hook task is terminal, `hook: on_failure` runs it only when the run is
  failing; both are auto-wired to depend on all other tasks with the matching
  trigger rule (`all_done` / `one_failed`), so no `depends_on` is needed.
  `allow_failure: true` lets an optional/best-effort task fail without failing
  the run (the task still records `failed`). New `task_runs.allow_failure`
  column (SQLite migration 014, Postgres 017); `reap_completed_runs` ignores
  allow-failure tasks when deciding the run status.
- **Secret env references** (`value_from`) — a task env var can pull its value
  from an external secret instead of storing it inline, so a credential never
  lands in the workflow spec or the datastore:
  `env: [{ name: DB_PASSWORD, value_from: { secret: prod-db-password } }]`.
  Resolved at dispatch from `DAGRON_SECRET_<NAME>` (process env) or a file
  `<DAGRON_SECRETS_DIR>/<NAME>` (the SOPS / External-Secrets / k8s-secret mount
  convention); a missing secret fails the task rather than running it empty.
  Resolved `value_from` values are always masked in output (regardless of the
  var's name), building on the #8 redactor. (Vault/cloud secret backends are
  a follow-up behind the same seam.)
- **Task trigger rules** (`trigger_rule:`) — a task can now run
  based on its dependencies' *outcomes*, not just their success: `all_success`
  (default), `all_done` (cleanup joins), `one_failed` / `all_failed` (failure
  handlers), `none_failed`. The scheduler's dependency model was generalized so
  every terminal transition (success/failure/skip) decrements dependents, and
  `advance_ready_tasks` evaluates each task's rule once its deps are all terminal
  → `ready` or `skipped` (with skips cascading). New `task_runs.trigger_rule`
  column (SQLite migration 013, Postgres 016). **Behavior change:** a task
  skipped because an upstream failed now shows as `skipped` (was `cancelled`);
  `cancelled` now means only an operator cancel or a run-deadline sweep. The run
  is still `failed` if any task failed, and `rerun-from-failed` re-runs the
  failed + skipped frontier.
- **Secret masking in task output.** Sensitive task-env values are now masked to
  `***` before a task's output is stored or logged, so a task that echoes a
  credential (or a library that prints one in a stack trace) no longer leaks it
  into the datastore or UI. On by default: any task env var whose **name** matches
  a sensitive pattern (`TOKEN`/`PASSWORD`/`SECRET`/`KEY`/… — overridable via
  `DAGRON_SENSITIVE_ENV_PATTERNS`, empty to disable), plus the values of any
  engine-process vars named in `DAGRON_REDACT_ENV` (e.g. `DATABASE_URL`). Masking
  is applied centrally in the worker (covers local/Docker/Kubernetes backends)
  and to the local executor's live stderr log; only values ≥ 4 chars are masked.
- **Forge feedback — commit statuses + run badges.** A workflow can declare a
  `notify.git` block (`provider: github|gitlab`, `repo`, `sha`, optional
  `context`/`target_url`, all `{{ param }}`-templated); when the run finishes the
  engine posts a success/failure commit status to the forge, so a dagron run
  shows up as a green/red check on the commit that triggered it. Best-effort and
  off by default — active only when `GITHUB_TOKEN`/`GITLAB_TOKEN` is set
  (`GITHUB_API_BASE`/`GITLAB_API_BASE` override for GHE/self-managed) — and a
  forge being down never affects run execution (mirrors the OpenLineage
  emitter). New `dagron-forge` crate holds the `ForgeClient` + GitHub/GitLab
  request builders. Plus a public, unauthenticated **run badge**:
  `GET /api/badges/:name` returns a shields-style SVG of a workflow's latest run
  status for embedding in a README.
- **GitOps pull sync (Git → datastore).** The GitOps page's **Sync** action now
  performs a real reconcile instead of just flipping UI state: it shallow-clones
  the registered branch, validates every `*.yaml`/`*.yml` under the repo's
  configured `path` (default `dagron/`) through the same parser the submit path
  uses, and upserts each valid workflow into the `workflows` table keyed by name
  — the Git → datastore *pull* half of GitOps, with no CRDs required. The
  fetched commit (`rev`), synced count, and per-file validation errors are
  recorded on the repo row; one bad file doesn't block the good ones. `POST
  /api/git-repos` gains an optional `path`; private repos clone with a token from
  `DAGRON_GIT_TOKEN`/`GITHUB_TOKEN` (injected only into `https://` URLs and
  redacted from errors); only `https/http/git/ssh/file` URL schemes are accepted.
  (An `auto_sync` background poller is the remaining follow-up; the flag is
  stored.)
- **`dagron-plan` — workflow diff for pull requests.** A new binary crate
  (`crates/dagron-plan`, depends on `dagron-core` only) that resolves two specs
  through the real parse → expand → validate pipeline and reports what would
  actually change: added/removed/changed leaf tasks with field-level diffs
  (command, deps, image, env, retries, timeouts), run-timeout changes, and a
  Mermaid graph of the resulting DAG with added/changed tasks flagged. Because
  it diffs the *resolved* DAG, two different YAML spellings of the same fan-out
  show as no change. `dagron-plan <base.yaml> <head.yaml>` or
  `dagron-plan --git <base>..<head> <path>` (shells `git show`); `git diff`-style
  exit codes with `--exit-code` (2 when the plan is non-empty) for a CI drift
  gate. Pairs with `dagron validate` to gate merges.
- **Cron `when` gate + `stopStrategy`** — two
  optional per-schedule expressions, both reusing the task-level `when:`
  evaluator:
  - **`when`**: a per-fire conditional gate for conditions cron can't express
    (e.g. `"{{ day }} == {{ days_in_month }}"` = last day of month,
    `"{{ weekday }} <= 5"` = weekdays only). Evaluated against the scheduled
    time's calendar fields in the schedule's timezone; a false result skips the
    fire (only `next_fire_at` advances). Supported on both file-cron config
    entries (`when:`) and UI schedules (`when_expr`). Skips counted in
    `scheduler_schedule_gated_total`.
  - **`stopStrategy`** (`stop_expr`, UI schedules): a comparison over the
    schedule's run outcome counts — `{{ succeeded }}` / `{{ failed }}` /
    `{{ total }}` — evaluated before each fire; when true the schedule
    auto-stops (disabled, with `stopped_at`/`stop_reason` surfaced via the API).
    Re-enabling clears the stop record. Counted in
    `scheduler_schedules_stopped_total`.
  New `schedules.when_expr/stop_expr/stopped_at/stop_reason` columns and a
  `workflow_runs.schedule_id` stamp for outcome counting (SQLite migration 012,
  Postgres 015).
- **Timezone-aware cron schedules** — a schedule now carries an IANA `timezone`
  (e.g. `America/New_York`); its cron expression is evaluated in that zone so a
  "02:00 daily" job keeps firing at 02:00 wall-clock across DST transitions.
  Threaded through the file-cron
  config (`timezone:` per entry), the DB-schedule loop, the manual + automatic
  backfill catch-up, the `dagron-api` schedule drawer (`timezone` field on
  create/update, validated → 400 on an unknown zone), and the operator's
  `CronWorkflow` CRD (`spec.timezone`). New `schedules.timezone` column
  (SQLite migration 011, Postgres 014), `DEFAULT 'UTC'` so existing rows are
  unchanged. The tz-aware next-fire computation is one shared helper
  (`dagron-engine::schedule_time`) mirrored by the API.
- **`dagron validate <file|dir>... [--json]`** — offline workflow lint through
  the exact parse → template-expansion → graph-validation pipeline every submit
  path uses. Directories are walked recursively; `--json` emits one object per
  file for CI; exits non-zero on any invalid spec. (A pre-merge GitOps check.)
- **Run-level timeout** — `run_timeout_secs` on the workflow spec. The engine's
  deadline sweep marks an overdue run `failed`, cancels its remaining tasks
  (fence-guarded against late executor writes), and counts it in the new
  `scheduler_runs_deadline_exceeded_total` metric. New nullable
  `workflow_runs.deadline_at` column (SQLite migration 010, Postgres 013).
- **Retry backoff cap** — `retry_max_delay_secs` on a task clamps the
  exponential backoff to a ceiling.
- **Named fan-out instances** — `instance_key: "{{ item.region }}"` on a
  `with_items`/`with_param` task names each expanded instance `<task>.<label>`
  instead of `<task>.<index>`. Labels are
  sanitized to `[A-Za-z0-9_-]` and must be unique within the fan-out.
- **`{{ scheduled_time }}` parameter** — every time-originated run (file cron,
  DB schedules, automatic backfill catch-up) receives its *nominal* fire time as
  an RFC-3339 workflow parameter, so tasks can reference their logical date
  (the data-interval idiom; a backfilled run processes *its* interval,
  not "now").
- The Python and TypeScript SDKs (`sdks/`) now ship in the distribution, so
  the `examples/sdk/` scripts resolve against the bundled SDK out of the box.
- Runnable SDK examples under `examples/sdk/` (Python + TypeScript) that drive a
  live `dagron-api`: quickstart, workflow+schedule, live SSE streaming, and
  cascade-rerun recovery, with a README covering setup and env config.
- Initial open-source cut of the dagron engine.

### Fixed
- **TypeScript SDK `Dag.submit()`** posted the raw spec to `POST /api/runs`; the
  gateway expects `{"yaml": "<spec>"}` and rejected it with `422 missing field
  yaml`. It now wraps the spec and returns the parsed `run_id` (`@dagron/sdk`
  0.1.0 → 0.1.1). Mirrors the Python SDK's v0.2 fix.
- `Executor` trait + `LocalExecutor` (subprocess) reference backend.
- `WorkflowSource` trait + `FileSource` and `ChannelSource` reference sources.
- In-memory `run_dag` scheduler: dependency-driven concurrency, retries with
  exponential backoff, and downstream skip-on-failure.
- `dagron run <file.yaml>` CLI and a bundled example DAG.
