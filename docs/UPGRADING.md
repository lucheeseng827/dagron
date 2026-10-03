# Upgrading dagron

How to move an installation to a new release, and what each release asks of you
before you do. The general procedure is short; the per-release sections are the
part to read every time.

- [The procedure](#the-procedure) — back up, one engine first, then the rest
- [Upgrading to 0.10.0](#upgrading-to-0100) — from 0.9.x
- [Upgrading from 0.8.x](#upgrading-from-08x) — the frontend image is gone
- [Rolling back](#rolling-back)

The detail behind every step — what the state is, how migrations work, and what
to do when one fails — is in [`BACKUP_RECOVERY.md`](BACKUP_RECOVERY.md).

## The procedure

dagron's only state is its database. Migrations are embedded in the engine and
applied when it starts; they are **forward-only**, so the backup you take first
is the only way back.

1. **Back up.**

   ```sh
   # Postgres
   pg_dump -U dagron -d workflow -Fc -f "workflow-$(date -u +%Y%m%dT%H%M%SZ).dump"
   # SQLite — safe while the daemon runs (WAL mode)
   sqlite3 workflow.db ".backup 'workflow-backup.db'"
   ```

   Never copy a running SQLite `workflow.db` on its own: the `-wal` and `-shm`
   files beside it hold committed data.
2. **Read the section for the version you are moving to**, below. Some releases
   need a setting or a permission in place *before* the new engine starts.
3. **Stop ingestion** — pause schedules, stop whatever submits runs — and let
   running work drain. Work that is still running when an engine stops is not
   lost: its lease expires (30 s by default) and the next engine re-dispatches
   it, so tasks must be safe to run twice.
4. **Upgrade one engine first.** It applies the migrations. Check it exited
   cleanly and that `GET /healthz` on its ops API answers before you touch the
   rest. Two engines starting at once against an unmigrated database both try to
   apply the same migrations; one wins and the other may exit.
5. **Roll the remaining engines, then `dagron-api`, then the GitOps worker**, and
   resume ingestion.

### Compose

The quickstart pins every image to `DAGRON_VERSION`:

```sh
DAGRON_VERSION=0.10.0 docker compose -f compose.quickstart.yaml pull
DAGRON_VERSION=0.10.0 docker compose -f compose.quickstart.yaml up -d
```

(`podman compose` works the same.) A checkout of a newer `compose.quickstart.yaml`
already defaults to its own release.

### Helm

The chart pins matching image tags for every component, but one `helm upgrade`
rolls the engine, `dagron-api` and the GitOps worker Deployments together, and
nothing in the chart waits for the engine's migrations before the others start.
So stage it, the same way as the procedure above. Read the release's section
first: 0.10.0 changes the chart's defaults.

```sh
# 1. The new chart with ONE engine on the new image, everything else still on
#    the old one. Use your current versions for the two old tags.
helm upgrade dagron oci://registry-1.docker.io/mancube/dagron --version 0.10.0   --reuse-values   --set engine.replicas=1   --set dagronApi.image=mancube/dagron-api:0.9.2   --set gitops.image=mancube/dagron-gitops:0.9.2
# 2. Wait for it. The engine applies the migrations before it starts its API,
#    so /healthz answering means they are done. The chart sets no readiness
#    probe, so a finished rollout alone does not. (The Deployment is
#    dagron-engine unless you set fullnameOverride.)
kubectl rollout status deployment/dagron-engine
kubectl port-forward deployment/dagron-engine 8080:8080 &
curl -fsS http://127.0.0.1:8080/healthz && kill %1
# 3. Everything on the new release, at your usual engine count.
helm upgrade dagron oci://registry-1.docker.io/mancube/dagron --version 0.10.0   --reuse-values   --set engine.replicas=<your count>   --set dagronApi.image=mancube/dagron-api:0.10.0   --set gitops.image=mancube/dagron-gitops:0.10.0
```

The chart's default is one engine; if you run one, step 1's `engine.replicas` is
already right. Drop the `gitops.image` lines if the worker is not enabled.

### Binaries

Stop the engine, replace the binary, start it. Same order as above when you run
more than one.

## Upgrading to 0.10.0

From 0.9.0 – 0.9.3. The full list of changes is the
[changelog](../CHANGELOG.md); this is what an upgrade has to act on.

### Before you start

Work through this list before the first 0.10.0 engine starts. Each row says who
it affects; skip the rows that do not apply to you.

| If you… | What changed | Do this first |
|---|---|---|
| set `service_account:` on any task (IRSA, workload identity) | A task may run only as a ServiceAccount on an allow-list, and the list is **empty by default**, so those tasks are refused | Set `DAGRON_TASK_ALLOWED_SERVICE_ACCOUNTS` to the task identities you use. Never list the engine's own ServiceAccount. Tasks without `service_account:` are unaffected |
| run tasks on Kubernetes with environment secrets and wrote the engine's Role yourself | Secrets now reach task pods by reference, through a per-task Secret, not as a literal in the pod spec. The engine needs `secrets` `create`/`patch`/`delete`, and a task fails, naming the permission, if it lacks them | Add those verbs to your Role. The chart's Role already has them. `DAGRON_TASK_SECRET_ENV=inline` restores the old behaviour |
| install with the Helm chart and call the engine's API over HTTP from anything other than dagron's own pods | `networkPolicy.enabled` is now **on by default**: the engine accepts traffic only from `networkPolicy.engine.allowFrom` and the `monitoring` namespace, and Postgres only from the release's own pods | Add those callers to `networkPolicy.engine.allowFrom`, or set `networkPolicy.enabled: false`. The policy only takes effect on a CNI that enforces NetworkPolicy. See [`HARDENING.md`](HARDENING.md) |
| have users who delete workflows, or connect, disconnect or re-key Git repositories, without being in the `admin` group | All of these now require `admin`, and answer `403` otherwise | Put those users in `admin`, or have them retire a workflow (`POST /api/workflows/{id}/state` with `retired`) rather than delete it. An instance with no admin can seed one from `DAGRON_ADMIN_EMAIL` + `DAGRON_ADMIN_PASSWORD` |
| have users in the `viewer` group on the open build | `viewer` is now read-only in every build: every mutation answers `403`. It used to be enforced only in the enterprise build | Move anyone who needs to write out of `viewer` |
| edit Git-synced workflows in the console or through the API | A workflow synced from a repository now answers `409` to `PUT` and `DELETE`, naming the repository; the edit used to be accepted and then silently reverted at the next sync | Change those workflows in the repository. An `admin` can still pass `?force=true` |
| let an agent approve gates through `dagron-mcp` | `dagron_approve_task` is now hidden and refused unless `DAGRON_MCP_ALLOW_APPROVE=1`. Rejecting is unaffected | Set the variable on the MCP server if an agent should approve |
| pin MCP tool definitions in a gateway that hashes annotations | Every dagron tool now declares annotations, so each definition changes once | Re-approve the tool definitions once |
| use `repeat:` on a `template:` call | It is now refused at validation; it used to be dropped silently | Move `repeat:` onto the template's tasks |

### What the engine does at startup

It applies these migrations, all additive:

| Backend | From 0.9.0 – 0.9.2 | From 0.9.3 |
|---|---|---|
| SQLite | 042 – 050 | 047 – 050 |
| Postgres | 053 – 065 | 061 – 065 |

Three of the Postgres ones (054, 058 and 065) build an index with
`CREATE INDEX CONCURRENTLY`, each in a migration of its own, so writers are not
blocked on a large table. A build that is interrupted can leave an index marked
`INVALID`, which a re-run skips because it already exists: drop that index and
start the engine again to rebuild it.

### When engines of two versions run together

Mixed versions during a rolling upgrade are safe for everything that existed in
0.9, with one exception: **`defer:`**. A 0.9 engine does not know the field,
ignores it, and marks a deferred task succeeded while its remote job is still
running. Finish rolling every engine to 0.10.0 before you register any workflow
that uses `defer:`.

### After upgrading

Nothing below is required; it is what 0.10.0 makes available.

- **Remote jobs that hold no worker** — `defer:`, with the `dagron-step-spark`
  and `dagron-step-sql` binaries: [`EXTERNAL_JOBS.md`](EXTERNAL_JOBS.md).
- **Fan-in over several datasets** — `on_datasets: [a, b]` with
  `datasets_mode: all`, now open: [`DATASETS.md`](DATASETS.md).
- **OpenLineage events with inputs and outputs**, when `OPENLINEAGE_URL` is set:
  [`DATASETS.md`](DATASETS.md).
- **New metrics** — per-workflow run time and outcome, `process_*`, signpost
  hits — and eight Grafana dashboards: [`METRICS.md`](METRICS.md) and
  [`examples/monitoring/`](../examples/monitoring/). The `last_run` queries take
  the newest engine's value, so they hold with several engines.
- **`dagron_wait_run` progress** — MCP clients that send a `progressToken` get
  `notifications/progress` while a wait blocks: [`MCP.md`](MCP.md).

### Rolling back from 0.10.0

As for any release, rolling back means restoring the backup and starting 0.9.x
against it ([below](#rolling-back)). One thing is specific to 0.10.0: a task
parked on a remote job (`defer:`) is a row only a 0.10.0 engine knows how to
poll. Starting a 0.9 engine against a database that holds parked tasks, rather
than against the backup, leaves them parked forever, with their remote jobs still
running. Cancel those runs before you go back, or restore the backup as the
procedure says.

## Upgrading from 0.8.x

`mancube/dagron-frontend` is discontinued: **0.8.1 is its last tag**. The console
it served now comes from `dagron-api` itself, on the same port as the API. Drop
the frontend container, and the `frontend.enabled` Helm value. Then follow
[Upgrading to 0.10.0](#upgrading-to-0100), which applies to you too.

## Rolling back

Migrations are forward-only; there are no down migrations. Rolling back is:
restore the pre-upgrade backup, then start the previous release against it.
Everything written after the backup is discarded with it.

If the new engine fails at startup, only the migration that failed is rolled
back. sqlx runs each migration in its own transaction, so the ones before it in
the same startup stay applied, and a concurrent index build runs outside a
transaction altogether (see [above](#what-the-engine-does-at-startup)). The
previous release usually still starts on that schema, because migrations only
add, but that is a way to keep running while you investigate, not a rollback.
To actually go back, restore the pre-upgrade backup.

Step-by-step recovery for the failure cases — a migration that errors partway,
"previously applied but has been modified", a startup that names an unknown
migration — is in [`BACKUP_RECOVERY.md`](BACKUP_RECOVERY.md) §4 and §6.
