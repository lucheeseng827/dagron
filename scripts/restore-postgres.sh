#!/usr/bin/env bash
# Restore a dagron Postgres dump, and report what landed.
#
#   # rehearse into a scratch DB (safe; this is docs/BACKUP_RECOVERY.md §5)
#   ./scripts/restore-postgres.sh -f workflow-2026….dump -n workflow_dr_test --create
#
#   # real recovery into a fresh database
#   ./scripts/restore-postgres.sh -f workflow-2026….dump -n workflow --create
#
# --create DROPs the target database first, WITH (FORCE) so it succeeds while
# clients are connected. Without a drop, pg_restore layers the dump on top of
# whatever is already there and you get a "successful" restore of a corrupt DB.
#
# Restoring is only half of it: afterwards, start ONE engine against the database
# and confirm it reaches "worker pool ready". Migrations are applied at engine
# startup and there is no `dagron migrate` command, so that is the only proof the
# schema is usable.
set -euo pipefail

FILE=""; DB=""; HOST=""; PORT=""; USER=""; CREATE=0; YES=0

usage() { sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }

while [ $# -gt 0 ]; do
  case "$1" in
    -f|--file)    FILE="$2"; shift 2 ;;
    -n|--dbname)  DB="$2"; shift 2 ;;
    -H|--host)    HOST="$2"; shift 2 ;;
    -p|--port)    PORT="$2"; shift 2 ;;
    -U|--user)    USER="$2"; shift 2 ;;
    --create)     CREATE=1; shift ;;
    -y|--yes)     YES=1; shift ;;
    -h|--help)    usage 0 ;;
    *) echo "unknown argument: $1" >&2; usage 1 ;;
  esac
done

[ -n "$FILE" ] || { echo "-f <dump file> is required" >&2; exit 1; }
[ -f "$FILE" ] || { echo "no such dump: $FILE" >&2; exit 1; }
[ -n "$DB" ]   || { echo "-n <dbname> is required" >&2; exit 1; }
command -v pg_restore >/dev/null || { echo "pg_restore not found in PATH" >&2; exit 1; }

conn=()
if [ -n "$HOST" ]; then conn+=(-h "$HOST"); fi
if [ -n "$PORT" ]; then conn+=(-p "$PORT"); fi
if [ -n "$USER" ]; then conn+=(-U "$USER"); fi

# Require the drop-and-recreate path. Without --create, pg_restore layers the
# dump onto whatever is already in "$DB" — a "successful" restore of a corrupt
# database. Restoring into a truly-empty DB you provisioned yourself? Create it
# and pass --create; the DROP IF EXISTS below is then a no-op.
[ "$CREATE" -eq 1 ] || {
  echo "--create is required: restore drops and recreates \"$DB\" so it can never layer onto existing state" >&2
  exit 2
}

if [ "$YES" -eq 0 ]; then
  # Dropping a database is not undoable. Say the name out loud.
  printf 'About to DROP DATABASE %s WITH (FORCE) and restore over it.\nType the database name to continue: ' "$DB"
  read -r confirm
  [ "$confirm" = "$DB" ] || { echo "aborted"; exit 1; }
fi

if [ "$CREATE" -eq 1 ]; then
  psql "${conn[@]}" -d postgres -v ON_ERROR_STOP=1 -q \
    -c "DROP DATABASE IF EXISTS \"$DB\" WITH (FORCE);"
  psql "${conn[@]}" -d postgres -v ON_ERROR_STOP=1 -q \
    -c "CREATE DATABASE \"$DB\";"
fi

pg_restore "${conn[@]}" -d "$DB" "$FILE"

echo
echo "restored into $DB:"
psql "${conn[@]}" -d "$DB" -tAc \
  "SELECT (SELECT count(*) FROM workflow_runs)||' runs, '
        ||(SELECT count(*) FROM task_runs)||' tasks, '
        ||(SELECT count(*) FROM workflows)||' workflows, '
        ||(SELECT count(*) FROM users)||' users, '
        ||(SELECT count(*) FROM _sqlx_migrations)||' migrations (max='
        ||(SELECT max(version) FROM _sqlx_migrations)||')';"

cat <<EOF

Compare those counts against the source database. A restore that reports zero
users or zero workflows restored a partial dump — see docs/BACKUP_RECOVERY.md §6.7.

Then prove the schema is usable:

  DATABASE_URL=postgres://…/$DB dagron     # expect "worker pool ready", exit 0
EOF
