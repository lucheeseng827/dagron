#!/usr/bin/env bash
# Back up a dagron Postgres database.
#
#   ./scripts/backup-postgres.sh                       # uses $DATABASE_URL
#   ./scripts/backup-postgres.sh -d /var/backups/dagron
#   PGPASSWORD=… ./scripts/backup-postgres.sh -H db -U dagron -n workflow
#
# Writes <dir>/workflow-<UTC timestamp>.dump in pg_dump custom format (-Fc),
# which is what restore-postgres.sh and pg_restore consume.
#
# Dump the WHOLE database. dagron-api creates its own tables (users,
# environments, environment_secrets, git_repos, …) and will silently recreate
# them EMPTY if a restore is missing them — see docs/BACKUP_RECOVERY.md §1.
#
# This does not back up DAGRON_ENV_SECRET_KEY. Without that key every stored
# environment secret in this dump is permanently undecryptable; keep it in a
# secret manager, and not in the same place as these files.
set -euo pipefail
# Database dumps carry every secret's ciphertext and every user row — never let
# them land group/world-readable.
umask 077

OUT_DIR="."
HOST=""; PORT=""; USER=""; DB=""
RETAIN=0

usage() { sed -n '2,20p' "$0" | sed 's/^# \{0,1\}//'; exit "${1:-0}"; }

while [ $# -gt 0 ]; do
  case "$1" in
    -d|--dir)     OUT_DIR="$2"; shift 2 ;;
    -H|--host)    HOST="$2"; shift 2 ;;
    -p|--port)    PORT="$2"; shift 2 ;;
    -U|--user)    USER="$2"; shift 2 ;;
    -n|--dbname)  DB="$2"; shift 2 ;;
    -k|--retain)  RETAIN="$2"; shift 2 ;;   # delete dumps older than N days
    -h|--help)    usage 0 ;;
    *) echo "unknown argument: $1" >&2; usage 1 ;;
  esac
done

command -v pg_dump >/dev/null || { echo "pg_dump not found in PATH" >&2; exit 1; }

# Connection: explicit flags win; otherwise pg_dump reads $DATABASE_URL / PG* env.
args=()
if [ -n "$HOST" ]; then args+=(-h "$HOST"); fi
if [ -n "$PORT" ]; then args+=(-p "$PORT"); fi
if [ -n "$USER" ]; then args+=(-U "$USER"); fi
if [ -n "$DB" ]; then
  args+=(-d "$DB")
elif [ -n "${DATABASE_URL:-}" ]; then
  args+=(-d "$DATABASE_URL")
else
  echo "no database: pass -n <dbname> or set DATABASE_URL" >&2
  exit 1
fi

mkdir -p "$OUT_DIR"
stamp="$(date -u +%Y%m%dT%H%M%SZ)"
# Unique per run: two backups can start in the same second, and a shared
# `.partial` would let their pg_dump output interleave into one corrupt file.
# mktemp gives each run its own partial (and, derived from it, its own final).
partial="$(mktemp "$OUT_DIR/workflow-$stamp.XXXXXX.partial")"
out="${partial%.partial}.dump"

# Rename on success, so a crashed or truncated run never leaves something that
# looks like a usable backup.
pg_dump "${args[@]}" -Fc -f "$partial"
mv "$partial" "$out"
echo "wrote $out ($(du -h "$out" | cut -f1))"

if [ "$RETAIN" -gt 0 ]; then
  # -mtime +N is "older than N days". Only ever matches this script's own names.
  find "$OUT_DIR" -maxdepth 1 -name 'workflow-*.dump' -mtime "+$RETAIN" -print -delete
fi

cat <<'EOF'

Next: a backup you have not restored is not a backup.
Rehearse it — docs/BACKUP_RECOVERY.md §5, or:

  ./scripts/restore-postgres.sh -f <this file> -n workflow_dr_test --create
EOF
