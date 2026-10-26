# One node — the engine and dagron-api on one SQLite file

dagron's console and its API with sign-in (`dagron-api`) have been
Postgres's. Built with `sqlite` instead, dagron-api is
one process beside one engine, the two on the same SQLite file, and nothing
else: no Postgres, no cluster. It is for a laptop, an edge site, or a small
air-gapped site, where one host does the work and a person wants to sign in
and see it.

What it serves is the core of the console: sign-in, users, tokens, workflows,
runs, their logs, and live updates. The rest of `/api` answers `501` and says
it is the Postgres build's ([§4](#4-what-answers-501)). An install that needs
those, or more than one host, is a Postgres one ([HA.md](HA.md)).

## 1. Build

There is no published image of it yet. From module_54:

```bash
# The engine. A source build's defaults are sqlite and ops.
cargo build --release -p dagron
# dagron-api, for SQLite. Its default is postgres.
cargo build --release -p dagron-api --no-default-features --features sqlite
```

`target/release/dagron` and `target/release/dagron-api` are what is installed.
The two are built one at a time: dagron-core takes one backend per build.

## 2. Run

Both are given the same file. Either may start first: each applies the
engine's migrations as it opens the file, and one that meets the other
opening it, or applying the same migration, at the same moment tries again,
up to five times (`dagron_core::db::init_pool`).

```bash
# The engine, in the file's directory: given no arguments, its datastore is
# ./workflow.db and it starts with no run, the API bringing the work. Its ops
# API stays on loopback.
cd /var/lib/dagron && EXECUTOR=local API_ADDR=127.0.0.1:8787 dagron

# dagron-api, on the same file.
DATABASE_URL=sqlite:///var/lib/dagron/workflow.db \
PORT=8080 \
DAGRON_JWT_SECRET="$(cat /etc/dagron/jwt-secret)" \
DAGRON_ADMIN_EMAIL=admin@example.com DAGRON_ADMIN_PASSWORD="$(cat /etc/dagron/admin-password)" \
  dagron-api
```

- `DATABASE_URL` is `sqlite://<path>`, `sqlite:<path>`, or the path itself.
  The engine's is its second argument, after a workflow file it runs first
  (`dagron <workflow.yaml> <path>`), or `workflow.db` where it starts.
- The file's directory must be writable by both: SQLite keeps its `-wal` and
  `-shm` beside it.
- `DAGRON_COOKIE_SECURE=false` where the console is reached over plain
  `http://`, as on a laptop; behind TLS, leave it.
- The console is served at `/` as the Postgres build serves it
  (`DAGRON_CONSOLE_DIR`).
- The settings of the [edge profile](EDGE_PROFILE.md) (`profile: edge`, the
  pressure file, the free-disk floor, SQLite on flash) are the engine's, and
  hold here as they do without dagron-api.

As two systemd units, each names the file, and dagron-api is ordered after the
engine only so that its log reads in order; neither needs the other to start:

```ini
# /etc/systemd/system/dagron.service
[Service]
WorkingDirectory=/var/lib/dagron
Environment=EXECUTOR=local API_ADDR=127.0.0.1:8787
ExecStart=/usr/local/bin/dagron
Restart=on-failure

# /etc/systemd/system/dagron-api.service
[Unit]
After=dagron.service

[Service]
Environment=DATABASE_URL=sqlite:///var/lib/dagron/workflow.db PORT=8080
# DAGRON_JWT_SECRET, and DAGRON_ADMIN_* for the first start
EnvironmentFile=/etc/dagron/api.env
ExecStart=/usr/local/bin/dagron-api
Restart=on-failure
```

## 3. What it serves

| Area | Routes |
| --- | --- |
| Sign-in and people | `POST /api/login`, `/api/logout`, `/api/me`, `/api/users` |
| Tokens | `/api/tokens` (make, list, revoke); a token is a `Bearer` for the rest |
| Workflows | `/api/workflows` (store, list, read, update, delete), `…/run`, `…/versions`, `…/state`, `…/runs` |
| Runs | submit YAML (`POST /api/runs`, with `Idempotency-Key`), list with filters, a run, its spec, `/api/runs/specs`, `/refs`, `/sources`, `…/wait` |
| Control | cancel, rerun, resubmit; a task's retry, clear, approve, reject |
| Logs | a run's, a task's, a task's attempts |
| Live updates | `/api/events/stream`, `/api/runs/{id}/stream` |
| Health | `/healthz`, `/readyz`, `/api/health` |

**Live updates are polled.** SQLite has no `LISTEN/NOTIFY`. The file's triggers
write each run a change touched to `run_changes`: a run made, its status, a
task made, a task's status or attempt, a task not over taken away. dagron-api
reads, every `DAGRON_SQLITE_POLL_MS` (500 ms by default, 50 at the least),
the runs written there since its last read, past a cursor of its own, and
sends each, as a NOTIFY would have. It writes nothing
there, so two dagron-api processes on one file each see every change. The
file keeps the last 65,536 changes and takes away what is older itself; at
its start, and if it falls further behind than that, dagron-api reads each
run that has not finished or finished in the last half minute, and sends
each, as each read did before. An update reaches the console up to that long
after the engine writes it.

**A decision here ends its run at the engine's next tick.** The file's
triggers write each run a change may have ended to `run_wakes`, whichever
process made the change: a gate approved or rejected in the console, a run
submitted with no task left in it, as well as the engine's own writes. The
engine takes those each tick and looks at those runs alone, and a slice of
the running runs each `REAP_INTERVAL_MS` is the net under them.

**Run no `ANALYZE` on the file.** The engine keeps no statistics, and the
indexes of its sweeps, its reap and its live counts are made for the plans
SQLite takes without them, on the SQLite the build bundles. An operator's
`ANALYZE`, or `PRAGMA optimize`, can move those plans onto other indexes;
it is not needed.

**One writer at a time.** SQLite lets one transaction write at a time, across
both processes; a writer waits for the other (the busy timeout) rather than
failing. dagron-api holds one connection, as the engine does, so a
transaction of its own never waits on itself. This is the same discipline
the engine on SQLite has always had, and it is what bounds the load: a team's
work on one host, not a fleet's.

## 4. What answers 501

Each of these answers `501` with `{"error": "not served by a one-node (SQLite)
dagron-api: this route is its Postgres build's"}`, so a client reads why rather
than a `404`:

- search, instance settings, environments and their secrets;
- a run's graph, triage and archive; artifacts, a workflow's artifacts, the
  archive;
- metrics, approvals (the list), datasets, dead letters;
- bundles, sync to Git, badges, Git repositories;
- schedules and backfills;
- the fleet, the link, the artifact store's own routes;
- remote runners (`/api/runners`, `/runner/v1`);
- the audit log's list.

An approval that a gate binds to artifacts (`approval_binds`) cannot be
checked without the artifact index, so approving such a gate answers `501`
too; a gate that binds nothing is approved as on Postgres.

The console's pages for these show the error. Schedules are still the
engine's: `CRON_CONFIG` fires them on one node as anywhere
([CONFIG.md](CONFIG.md#cron-config-file-cron_config)).

## 5. How it is tested

- `cargo test -p dagron-api --no-default-features --features sqlite`: the
  routes of §3 walked on a file of their own (`src/one_node_tests.rs`), the
  pump, and the URL forms. The Postgres build's tests are unchanged and run
  against Postgres 14–17.
- `scripts/one-node-e2e.sh`: the two processes started together on a new
  file; a person signs in, makes a token, stores a workflow and runs it, and
  submits another as YAML with the token; the engine runs both; dagron-api
  reads them back as succeeded, with each task's output, and a live update
  names the run; `/api/schedules` answers `501`.
- Both in `.github/workflows/dagron-one-node-e2e.yml`, in every pull request
  that touches dagron-api, dagron-core, the engine or the script, and again
  as it merges.

Not done: a published image or release binary of the SQLite dagron-api, and a
one-node compose file or package.
