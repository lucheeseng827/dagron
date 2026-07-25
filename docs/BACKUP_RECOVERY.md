# Backup, migration & recovery runbook

Production procedures for backing dagron up, upgrading it across schema
changes, and recovering when the database or a migration goes wrong.

Deploy/monitoring/security posture is in [`OPERATIONS.md`](OPERATIONS.md); env
knobs in [`CONFIG.md`](CONFIG.md).

Every command and error message here was executed against a live stack
(Postgres 16, engine image built from this tree). Where a claim is a
*measurement*, it says so.

---

## 1. What is state

Back up everything in the first table. Everything in the second is re-creatable.

| State | Where | Lose it and… |
| --- | --- | --- |
| **The database** | Postgres `workflow` DB, or the SQLite file | you lose runs, task history, workflow definitions, schedules, dead letters, users, environments, git-repo config, audit rows |
| **`DAGRON_ENV_SECRET_KEY`** | env var on engine **and** dagron-api | every stored environment secret is **permanently undecryptable** — the ciphertext is in the DB, the key is not |
| **`DAGRON_JWT_SECRET`** | env var on dagron-api | every existing session is invalidated (users re-login). Not fatal, but rotate deliberately |
| **Artifact KEK** (`dagron-crypto` provider config) — *Enterprise* | env / KMS | encrypted artifacts at rest become unreadable. Open builds have no KEK: artifacts are stored as written, and only `DAGRON_ENV_SECRET_KEY` above applies |
| **Artifact store** (`DAGRON_ARTIFACT_DIR` or object store) | filesystem / bucket | run outputs and checkpoints that tasks wrote |
| **GC archive** (`GC_ARCHIVE_DIR` / `GC_ARCHIVE_URL`) | filesystem / bucket | runs the retention GC already moved out of the DB — `/api/archive/*` stops resolving them |

| Not state | Why |
| --- | --- |
| Engine / worker / dagron-api processes | stateless; they coordinate only through the DB |
| `WORKFLOW_DIR` | an input directory; the specs it ingests are stored in the DB |
| GitOps-synced repos | the repo is the source of truth and re-syncs |

> **The two independent things.** The DB and `DAGRON_ENV_SECRET_KEY` must both
> survive, and a backup containing both together is a backup where one stolen
> artifact yields plaintext secrets. Store the key in a secret manager, back up
> the key there, and keep DB dumps separate.

### Restore the whole database, not a table subset

The engine owns most tables, but **dagron-api creates its own** at startup with
`CREATE TABLE IF NOT EXISTS` — `users`, `environments`, `environment_secrets`,
`git_repos`, plus audit/settings. That is convenient and it is a trap: restore a
dump that omitted them and dagron-api will start happily and **recreate them
empty**. Every user, environment and stored secret is gone, no error is raised,
and the bootstrap admin re-seeds from env so login still works. Always dump the
whole database.

---

## 2. Backup

### Postgres (production)

```bash
# nightly, plus before every upgrade
pg_dump -U dagron -d workflow -Fc -f "workflow-$(date -u +%Y%m%dT%H%M%SZ).dump"
```

`-Fc` (custom format) is what `pg_restore` consumes, and it compresses. Keep the
dumps somewhere that is not the database host.

For anything with a real RPO, dumps are a floor, not the plan: enable
**continuous archiving / PITR** (`archive_mode`, WAL shipping) or use your
provider's snapshot + point-in-time feature. A nightly dump means up to 24 h of
lost runs. Use your provider's continuous-archiving feature: RDS/Aurora
automated backups, CloudNativePG shipping WAL to an object store, or pgBackRest
on a self-managed box. Whichever you pick, the dagron-specific steps in §6 still
apply once the database is back.

**A backup you have not restored is not a backup.** §5 is a rehearsal you can
run in a few minutes against a scratch database.

### SQLite (single node)

```bash
sqlite3 workflow.db ".backup 'workflow-backup.db'"     # safe while running (WAL)
```

Never copy `workflow.db` alone from a running daemon — the `-wal`/`-shm`
sidecars carry committed data that has not been checkpointed. Either use
`.backup` or stop the daemon and copy all three files.

### What to back up alongside

- the value of `DAGRON_ENV_SECRET_KEY` (in your secret manager, not next to the dumps)
- the **Artifact KEK**, if artifacts are encrypted at rest — the provider config
  (`DAGRON_ENV_KEK_PROVIDER`) and its key reference (`DAGRON_ENV_KMS_KEY_ID`,
  `DAGRON_ENV_KMS_VAULT_URL`/`_KEY_VERSION`, or the local `DAGRON_ENV_KEK` material). For a
  KMS-managed key this means **retaining the key + version** — do not delete or rotate it
  away — *not* exporting raw key material; for a local KEK, keep its value in your secret
  manager. Lose it and restored ciphertext artifacts are permanently unreadable even though
  the DB restored cleanly. After a restore, verify it by fetching one encrypted artifact
  through the API (`GET /api/runs/{run}/artifacts/{task}/{name}`).
- the artifact store directory/bucket, if tasks write outputs you cannot recompute
- the GC archive directory/bucket, if you rely on `/api/archive/*`

---

## 3. How migrations actually work

Four facts decide every procedure below.

1. **Migrations are embedded in the binary and run at startup.** `init_pool`
   applies them before the scheduler serves anything
   (`crates/dagron-core/src/db/postgres.rs`, `…/sqlite.rs`). There is **no
   `dagron migrate` subcommand** — the only way to migrate is to start an engine.
2. **They are forward-only.** sqlx has no down-migrations here, so "rolling back
   a schema change" is not a thing. Rollback = restore the pre-upgrade backup.
3. **Two migration sets share one ledger.** Base (`migrations_pg`, versions
   1–042) and Enterprise (`migrations_pg_ee`, 900+) both write sqlx's single
   `_sqlx_migrations` table, and both run with `set_ignore_missing(true)` so
   neither trips over the other's rows.
4. **A failed migration is fail-fast, not half-applied.** Postgres DDL is
   transactional and sqlx runs each migration in one; a failure aborts startup
   with a non-zero exit before the scheduler runs. *Measured:* a tampered
   migration checksum produced `Error: migration 40 was previously applied but
   has been modified`, container exit code **1**, and the database was
   byte-for-byte unchanged (25 runs / 42 migration rows before and after).

`dagron-api` does **not** run sqlx migrations. It only `CREATE TABLE IF NOT
EXISTS`-es its own tables, so it can start against a database the engine has
never touched — which is exactly the trap in §1.

---

## 4. Production upgrade procedure

```text
1. Back up.                     pg_dump -Fc  (and verify per §5 if it matters)
2. Read the CHANGELOG.          note any migration listed for the version
3. Stop ingestion.              pause schedules / stop the queue source
4. Let running work drain.      or accept reclaim: leases expire in ~30 s and a
                                surviving node re-dispatches (tasks must be idempotent)
5. Upgrade ONE engine first.    it applies the migrations at startup
6. Verify.                      exit code 0, `GET /healthz`, migrations applied (§5)
7. Roll the rest.               remaining engines, then dagron-api, then frontend
8. Resume ingestion.
```

**Why one engine first:** migrations are additive and applied under a
transaction, but two engines starting simultaneously against an unmigrated DB
both try to apply the same set. One wins; the other may error and exit. Roll one,
confirm, then the rest.

**Old binary against a newer database** is usually fine — migrations are
additive, and `ignore_missing` means an engine that doesn't ship version *N*
tolerates finding *N* already applied. *Measured:* injecting a version-900 row
this OSS binary does not ship, then starting it, reached
`reconcile loop running` normally. That is what makes step 7 survivable if you
have to stop halfway. It is not a licence to run mixed versions permanently —
older code simply ignores newer columns.

Mixed versions during the roll are expected and safe for the duration of the
roll: the schema is a superset and coordination is through DB rows, not RPC.

---

## 5. Rehearse the restore (do this before you need it)

The whole drill, against a scratch database, without touching production:

```bash
# 1. dump
pg_dump -U dagron -d workflow -Fc -f /tmp/workflow.dump

# 2. restore into a scratch DB  (FORCE: DROP fails while anything is connected)
psql -U dagron -d postgres -c "DROP DATABASE IF EXISTS workflow_dr_test WITH (FORCE);"
psql -U dagron -d postgres -c "CREATE DATABASE workflow_dr_test;"
pg_restore -U dagron -d workflow_dr_test /tmp/workflow.dump

# 3. compare
psql -U dagron -d workflow_dr_test -tAc \
  "SELECT (SELECT count(*) FROM workflow_runs)||' runs, '
        ||(SELECT count(*) FROM task_runs)||' tasks, '
        ||(SELECT count(*) FROM _sqlx_migrations)||' migrations, max='
        ||(SELECT max(version) FROM _sqlx_migrations);"

# 4. prove the engine starts against it — this is the part people skip
DATABASE_URL=postgres://dagron:dagron@postgres:5432/workflow_dr_test \
  dagron   # expect: "scheduler starting" → "worker pool ready", exit 0
```

*Measured:* source and restored databases both reported
`25 runs, 54 tasks, 42 migrations, max=42`, and the engine started clean against
the restored copy.

`WITH (FORCE)` matters. Without it, `DROP DATABASE` fails with *"database … is
being accessed by other users"*, and a `pg_restore` into the surviving database
layers a second copy on top — which is how a "successful" restore quietly
produces a corrupt one.

---

## 6. Recovery playbooks

### 6.1 Database lost or unrecoverable

1. Provision a new Postgres. **Do not pre-create any dagron tables.**
2. `createdb workflow` then `pg_restore -d workflow <dump>`.
3. Set `DATABASE_URL` on engine + dagron-api; restore `DAGRON_ENV_SECRET_KEY` to
   the same value it had (§1 — a different key cannot decrypt stored secrets).
4. Start **one** engine. It applies any migrations newer than the dump.
5. Verify per §5 step 4, then start the rest and dagron-api.

Runs that were `running` when the DB died come back as `running` with expired
leases; the reconcile loop reclaims and re-dispatches them. Tasks must be
idempotent — a re-dispatched task may have already had an effect.

### 6.2 No backup exists and the DB is gone

You lose run history, but not necessarily your workflows: if you use **GitOps
sync**, the repo is the source of truth and re-syncs definitions into an empty
database. Same for any specs you submit from files. History, dead letters and
schedules are gone. This is the argument for GitOps in production.

### 6.3 `migration N was previously applied but has been modified`

The binary's copy of migration *N* differs from the checksum recorded when it was
applied. Real causes, in order of likelihood:

| Cause | Fix |
| --- | --- |
| Someone edited a released migration file | Revert the edit and ship a **new** migration instead. Never edit an applied one. |
| Two builds from different commits (a fork, a patched vendor image) | Deploy the binary whose migrations match, or roll forward with a build that supersedes the change. |
| The row itself was tampered with | Restore from backup. |

The engine exits **1** and does not serve — the database is untouched, so there
is no urgency to "fix" the DB. Fix the binary.

Editing `_sqlx_migrations` to match the new checksum makes the error go away and
leaves the schema silently not matching what the code expects. Only do it when
you have confirmed the two migration bodies are semantically identical, and write
down that you did.

### 6.4 Migration fails partway through an upgrade

The engine exits non-zero and the transaction rolls back, so the DB stays at the
previous version (§3, fact 4). Then:

1. Read the error — it names the failing statement.
2. If it is data-dependent (a constraint an existing row violates), fix the data
   and restart. The migration re-runs from the top.
3. If the migration itself is wrong, run the **previous** version's binary — the
   schema is still where it was — and report it.
4. Restore the pre-upgrade backup only if 1–3 leave you stuck; there is no
   partial state to clean up.

### 6.5 Startup complains about an unknown/newer migration

Expected and tolerated (`ignore_missing`), including the OSS↔Enterprise case
where the 900+ rows belong to a migrator this binary doesn't have. If you see it
as an *error* rather than a start-up, you are on a build predating that fix —
upgrade.

### 6.6 `DAGRON_ENV_SECRET_KEY` lost

Stored environment secrets are unrecoverable ciphertext. Recovery is
re-entry, not decryption:

1. Set a new key on engine + dagron-api.
2. Delete and re-create each environment secret (`PUT /api/environments/{id}/secrets/{name}`).
3. Rotate the underlying credentials — you cannot prove the old ciphertext was
   never exfiltrated.

Plain environment **variables** are stored unencrypted and survive a key loss.

### 6.7 Restored, but users/environments are empty

You restored a partial dump; dagron-api recreated its tables empty (§1). Restore
the full dump into a clean database. If the partial restore is now your only
copy, environments and users must be re-created by hand.

### 6.8 Runs stuck `running` after a restore

Normal. Their leases are stale; the reconcile loop reclaims them (default 30 s)
and re-dispatches. If a run must not re-execute, cancel it before starting the
engine.

---

## See also

- [`OPERATIONS.md`](OPERATIONS.md) — deploy, monitoring, security, symptom-first troubleshooting
- **HA / multi-node** — leases and leader election decide how §6.8 behaves
- [`CONFIG.md`](CONFIG.md) — every env knob named above
- **GitOps** — keeping definitions in git is what makes §6.2 survivable
