# Backups on-premises

A runbook for keeping dagron's state safe where there is no cloud: a
datacentre, an edge site, an air-gapped network. The
general procedures (what is state, upgrades, every recovery playbook) are in
[`BACKUP_RECOVERY.md`](BACKUP_RECOVERY.md); this is what to set up so that
they have something to restore.

## What to keep, and where

| What | How | Where |
|---|---|---|
| The database | a nightly `pg_dump`, by the chart's backup CronJob | a backup target off the database's host |
| `DAGRON_ENV_SECRET_KEY`, `DAGRON_JWT_SECRET`, the artifact KEK, the provenance and OIDC keys | once, and on each rotation | your secret store, not beside the dumps |
| The artifact store, the GC archive | as their own files (`rsync`, the storage's own snapshots) | wherever the dumps go |

A dump without the keys restores runs whose secrets and encrypted artifacts
can never be read again. Keep the keys apart from the dumps, so one stolen copy
is not both.

## A target

Pick one; the CronJob treats them the same.

### A volume: NFS, or any filesystem

The simplest on-premises target is a directory on storage that is not the
database's. A PersistentVolumeClaim on an NFS export (or CephFS, or a SAN
volume) that the cluster mounts:

```yaml
# backup-pvc.yaml: an NFS export, as a claim
apiVersion: v1
kind: PersistentVolume
metadata: {name: dagron-backups}
spec:
  capacity: {storage: 200Gi}
  accessModes: [ReadWriteMany]
  persistentVolumeReclaimPolicy: Retain
  nfs: {server: nas.corp.internal, path: /exports/dagron}
---
apiVersion: v1
kind: PersistentVolumeClaim
metadata: {name: dagron-backups, namespace: dagron}
spec:
  accessModes: [ReadWriteMany]
  storageClassName: ""
  volumeName: dagron-backups
  resources: {requests: {storage: 200Gi}}
```

```sh
kubectl apply -f backup-pvc.yaml
helm upgrade dagron oci://registry-1.docker.io/mancube/dagron -n dagron --reuse-values \
  --set backup.enabled=true \
  --set backup.to=file:///backup \
  --set backup.claimName=dagron-backups \
  --set backup.keep=14
```

The CronJob's pod runs as uid 65532 (on OpenShift, the project's uid in group
0), so the export must let it write: `chown 65532` on the export's directory,
or `all_squash,anonuid=65532` in `/etc/exports`. Kubernetes does not apply an
fsGroup to NFS.

### An S3-compatible store: MinIO, Ceph RGW

Where object storage runs on-premises already, point the CronJob at it as at
S3, with its endpoint:

```sh
kubectl create secret generic dagron-backup-s3 -n dagron \
  --from-literal=AWS_ACCESS_KEY_ID=… --from-literal=AWS_SECRET_ACCESS_KEY=…
helm upgrade dagron … --reuse-values \
  --set backup.enabled=true \
  --set backup.to=s3://dagron-backups/prod \
  --set-json 'backup.extraEnv=[
    {"name":"AWS_ENDPOINT_URL","value":"https://minio.corp.internal:9000"},
    {"name":"AWS_REGION","value":"us-east-1"},
    {"name":"AWS_ACCESS_KEY_ID","valueFrom":{"secretKeyRef":{"name":"dagron-backup-s3","key":"AWS_ACCESS_KEY_ID"}}},
    {"name":"AWS_SECRET_ACCESS_KEY","valueFrom":{"secretKeyRef":{"name":"dagron-backup-s3","key":"AWS_SECRET_ACCESS_KEY"}}}]'
```

A store with a certificate from a private CA needs that CA in the engine
image's trust store, as every other TLS connection the engine makes does
(`AWS_ALLOW_HTTP=true` takes a plain-HTTP endpoint, inside a trusted network
only). Turn on the bucket's versioning and object lock, where the store has
them: a dump that cannot be deleted for 14 days survives whoever gets the
backup credential.

### Without Kubernetes: one node, the appliance

The engine binary does the same without a cluster. On a host running
Postgres:

```sh
#!/bin/sh
# /usr/local/bin/dagron-nightly-backup (executable). DATABASE_URL is in
# /etc/dagron/backup.env: cron gives a job no environment of the engine's.
set -eu
. /etc/dagron/backup.env
f=/var/backups/dagron/dagron-$(date -u +%Y%m%dT%H%M%SZ).dump
# The local dump goes whether the job passes or fails, so failed nights do
# not fill the disk.
trap 'rm -f "$f"' EXIT
pg_dump --format=custom --no-owner --file="$f" "$DATABASE_URL"
dagron backup put "$f" file:///mnt/nas/dagron --keep 14
```

```sh
# /etc/cron.d/dagron-backup: one line a job (cron does not join a line
# ending in a backslash to the next)
17 2 * * * dagron /usr/local/bin/dagron-nightly-backup
```

A single node on SQLite has no `pg_dump`: `sqlite3 workflow.db ".backup '/var/backups/dagron/workflow.db'"`
makes a consistent copy while it runs, and `dagron backup put` sends it the
same way. The restore drill below is Postgres's; for SQLite, start the engine
on the copy and see it reach "worker pool ready".

## Off-site

A dump in the same building as the database survives a disk, not a fire.
Copy the target off-site on a schedule of its own: `rsync` of the NFS
directory to another site, `rclone sync` or the store's replication between
two MinIO sites, or tape. Keep the copy's retention longer than the target's.

## Encryption

`dagron backup` sends a dump as it is. Encrypt where it rests: the NFS
server's or SAN's encryption at rest, or the object store's (MinIO's SSE-KMS
with your KMS). A dump holds every run's parameters and outputs, and the
stored environment secrets encrypted under `DAGRON_ENV_SECRET_KEY`: treat the
target as you treat the database.

## Know that it ran

The CronJob's last success is a metric (kube-state-metrics'
`kube_cronjob_status_last_successful_time`). Alert when it is older than a day
and a half:

```yaml
- alert: DagronBackupStale
  expr: time() - kube_cronjob_status_last_successful_time{cronjob=~".*-backup"} > 129600
  labels: {severity: page}
  annotations:
    summary: "dagron's last good backup is more than 36 hours old"
```

`dagron backup list <target>` says what the target holds, newest first.

## The restore drill, on a schedule

A backup that has never been restored is a hope. `scripts/restore-drill.sh`
fetches the newest dump from the target, restores it into a scratch database
on a server you name, holds it to the live database, starts an engine on it
and drops it again ([`BACKUP_RECOVERY.md`](BACKUP_RECOVERY.md) §5). Run it
weekly, from a host that has `psql`, `pg_restore` and the engine binary, and
can reach both the target and a Postgres it may create a database on (a
staging server, not production's):

```sh
#!/bin/sh
# /usr/local/bin/dagron-restore-drill (executable)
exec /opt/dagron/scripts/restore-drill.sh file:///mnt/nas/dagron \
  --into postgres://drill:…@pg-staging.corp.internal:5432/postgres \
  --live postgres://readonly:…@pg.corp.internal:5432/dagron \
  --engine /usr/local/bin/dagron
```

```sh
# /etc/cron.d/dagron-restore-drill: Sundays, after the night's backup
45 3 * * 0 dagron /usr/local/bin/dagron-restore-drill
```

It exits non-zero on anything short of a pass and says why: a dump
`pg_restore` cannot read, a table empty that live's is not, a schema behind
live's, an engine that does not start.

## Restore for real

[`BACKUP_RECOVERY.md`](BACKUP_RECOVERY.md) §6.1, with the dump from the target:

```sh
dagron backup get file:///mnt/nas/dagron /var/restore     # the newest
scripts/restore-postgres.sh -f /var/restore/dagron-….dump -n dagron --create
```

Then put the keys back (`DAGRON_ENV_SECRET_KEY` and the rest), start one
engine, and see it reach "worker pool ready" before starting the others.
