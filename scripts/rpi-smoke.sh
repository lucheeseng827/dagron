#!/usr/bin/env bash
# Install dagron on a bare Raspberry Pi and prove the quickstart works.
#
# Runs *on the Pi*, not on your laptop — the quickstart binds :8080 to
# 127.0.0.1, so the checks have to be local. Drive it from the host with:
#
#   scp -P 5373 compose.quickstart.yaml scripts/smoke_dag.yaml \
#       scripts/rpi-smoke.sh root@ssh.rpi-svc.hostedpi.com:/root/
#   ssh -p 5373 root@ssh.rpi-svc.hostedpi.com 'bash /root/rpi-smoke.sh'
#
# Idempotent: re-running reuses the installed docker and the existing stack.
# `--fresh` tears the stack and its volumes down first, which is the run that
# actually exercises first-boot migrations and the schema-gate race.
#
set -euo pipefail

DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# Two layouts have to resolve. The flat one: everything copied into a single
# directory, which is how the quick scp recipe works. The tree one: this script in
# `scripts/` with `compose.quickstart.yaml` a level up at the module root, which is
# what the load-test rig needs, since `loadtest/pi/run.sh` reaches the driver at
# `../../loadtest/harness/`. Preferring the sibling keeps the flat copy working.
if [ -f "$DIR/compose.quickstart.yaml" ]; then
  COMPOSE="${COMPOSE:-$DIR/compose.quickstart.yaml}"
else
  COMPOSE="${COMPOSE:-$(cd "$DIR/.." && pwd)/compose.quickstart.yaml}"
fi
SMOKE_DAG="${SMOKE_DAG:-$DIR/smoke_dag.yaml}"
API="http://127.0.0.1:8080"
EMAIL="admin@local"
PASSWORD="dagron-admin"
FRESH=0
[ "${1:-}" = "--fresh" ] && FRESH=1

say() { printf '\n=== %s\n' "$*"; }
die() { printf '\nFAIL: %s\n' "$*" >&2; exit 1; }

# Dump the stack's logs on any failure. Every interesting way this breaks —
# an image with no arm64 manifest, the api/engine DDL race, a migration —
# is legible in them and invisible from the exit code.
on_err() {
  printf '\n--- compose ps ---\n' >&2
  docker compose -f "$COMPOSE" ps >&2 2>&1 || true
  printf '\n--- compose logs (tail) ---\n' >&2
  docker compose -f "$COMPOSE" logs --tail=80 >&2 2>&1 || true
}
trap on_err ERR

# --- 0. the thing that actually bit us -------------------------------------
# Mythic Beasts hands out an armhf (32-bit userland) Bookworm by default, on a
# 64-bit kernel — so `uname -m` says aarch64 and lies to you. dagron publishes
# linux/amd64 + linux/arm64 only (.github/workflows/docker.yml), so on armhf
# every pull fails with "no matching manifest" after you have already spent ten
# minutes installing docker. Check the userland, not the kernel, and check it
# first.
say "arch"
ARCH="$(dpkg --print-architecture)"
echo "dpkg architecture: $ARCH   (kernel: $(uname -m))"
[ "$ARCH" = "arm64" ] || [ "$ARCH" = "amd64" ] \
  || die "userland is '$ARCH'; dagron images are amd64/arm64 only. Reprovision the Pi with a 64-bit (arm64) OS image."

[ -f "$COMPOSE" ] || die "no compose file at $COMPOSE"
[ -f "$SMOKE_DAG" ] || die "no DAG at $SMOKE_DAG"

# --- 1. prerequisites ------------------------------------------------------
say "prerequisites"
need_apt=()
for p in curl jq; do command -v "$p" >/dev/null || need_apt+=("$p"); done
if [ ${#need_apt[@]} -gt 0 ]; then
  echo "apt-get install: ${need_apt[*]}"
  apt-get update -qq
  DEBIAN_FRONTEND=noninteractive apt-get install -y -qq "${need_apt[@]}"
fi

if ! command -v docker >/dev/null; then
  echo "installing docker (get.docker.com)…"
  curl -fsSL https://get.docker.com | sh
fi
docker compose version >/dev/null || die "docker compose plugin missing"
docker version --format '{{.Server.Version}}' | sed 's/^/docker: /'

# --- 1b. give docker a filesystem it can actually stack on ------------------
# Mythic Beasts Pis are NFS-rooted with no local block device, and overlayfs
# refuses an NFS upperdir — so every pull dies at unpack with
#   failed to mount /tmp/containerd-mountNNN: mount source: "overlay" … invalid argument
# after the download has already completed. The cheap fix is a sparse ext4
# image file on the NFS root: overlay2 gets a real local filesystem, and a
# sparse file costs nothing until layers land in it. (vfs would also work and
# needs no loop device, but it stores every layer as a full copy — on an NFS
# mount with 4 KB rsize/wsize that is slow enough to matter and big enough to
# fill a 9 GB root.)
#
# BOTH roots have to move, which the error message hides: docker 28 uses the
# containerd image store, so the snapshots the overlay mount actually stacks
# live under /var/lib/*containerd*, and moving only /var/lib/docker changes
# nothing. One image with two bind mounts rather than two images, so the space
# is a single pool instead of a guess split two ways.
if [ "$(stat -f -c %T /var/lib/containerd 2>/dev/null || echo unknown)" = "nfs" ]; then
  say "NFS root detected — backing docker + containerd with a loopback ext4 image"
  systemctl stop docker docker.socket containerd 2>/dev/null || true
  rm -rf /var/lib/docker /var/lib/containerd
  mkdir -p /mnt/dockerdata /var/lib/docker /var/lib/containerd
  truncate -s "${DOCKER_IMG_SIZE:-6G}" /docker.img
  mkfs.ext4 -q -F /docker.img
  mount -o loop /docker.img /mnt/dockerdata
  mkdir -p /mnt/dockerdata/docker /mnt/dockerdata/containerd
  mount --bind /mnt/dockerdata/docker /var/lib/docker
  mount --bind /mnt/dockerdata/containerd /var/lib/containerd
  # Survive a reboot; the rig is meant to be re-run, not re-fixed by hand.
  if ! grep -q '^/docker.img ' /etc/fstab; then
    { echo "/docker.img /mnt/dockerdata ext4 loop,defaults 0 0"
      echo "/mnt/dockerdata/docker /var/lib/docker none bind 0 0"
      echo "/mnt/dockerdata/containerd /var/lib/containerd none bind 0 0"; } >> /etc/fstab
  fi
  systemctl start containerd docker
  for i in $(seq 1 30); do docker info >/dev/null 2>&1 && break; sleep 1; done
fi
df -h /var/lib/containerd | tail -1

# --- 2. bring the stack up -------------------------------------------------
if [ "$FRESH" = 1 ]; then
  say "tearing down (--fresh)"
  docker compose -f "$COMPOSE" down -v --remove-orphans || true
fi

say "docker compose up"
t0=$(date +%s)
docker compose -f "$COMPOSE" up -d
echo "pull + start: $(( $(date +%s) - t0 ))s"

# --- 3. wait for the API ---------------------------------------------------
# Generous: a Pi pulling four images over a shared uplink and then running
# first-boot migrations is slow, and a smoke test that flakes on a slow disk
# tells you nothing.
say "waiting for $API/healthz"
t0=$(date +%s)
for i in $(seq 1 180); do
  curl -fsS --max-time 3 "$API/healthz" >/dev/null 2>&1 && break
  [ "$i" = 180 ] && die "dagron-api never answered /healthz (180s)"
  sleep 1
done
echo "api ready in $(( $(date +%s) - t0 ))s"

# --- 4. auth ---------------------------------------------------------------
say "login"
TOKEN=$(curl -fsS "$API/api/login" \
  -H 'content-type: application/json' \
  -d "{\"email\":\"$EMAIL\",\"password\":\"$PASSWORD\"}" | jq -er .token) \
  || die "login failed for $EMAIL"
AUTH=(-H "Authorization: Bearer $TOKEN")
echo "token acquired"

curl -fsS "${AUTH[@]}" "$API/api/health" | jq -e '{api, edition, db, event_listener, scheduler_leader}'

# --- 5. the seeded run -----------------------------------------------------
# The compose file hands the engine one bundled DAG so the console is not empty
# on first open. It is also the only end-to-end proof that the *engine* — not
# just the API — can ingest, schedule and execute on this box.
say "seeded run (simple-pipeline)"
for i in $(seq 1 120); do
  # Filtered by name, not "the newest run", so a second (non---fresh) invocation
  # checks the seeded run again instead of re-reading the smoke run below and
  # passing for free.
  ROW=$(curl -fsS "${AUTH[@]}" "$API/api/runs?name=simple-pipeline&limit=1" | jq -c '.[0] // empty')
  case "$(printf '%s' "$ROW" | jq -r '.status // ""')" in
    succeeded) echo "$ROW" | jq -c '{id, name, status}'; break ;;
    failed|cancelled) echo "$ROW" | jq -c .; die "seeded run did not succeed" ;;
  esac
  [ "$i" = 120 ] && die "no seeded run reached a terminal state in 120s"
  sleep 1
done

# --- 6. submit our own DAG -------------------------------------------------
# The seeded run proves boot. This proves the write path: submit YAML, get a
# run id, block on it. smoke_dag.yaml is a diamond (a → b,c → d), so it also
# says the scheduler fans out and joins rather than just running a line.
say "submit smoke_dag.yaml"
RUN=$(jq -Rs '{yaml: .}' < "$SMOKE_DAG" \
  | curl -fsS "${AUTH[@]}" -H 'content-type: application/json' \
      -d @- "$API/api/runs" | jq -er .run_id) \
  || die "POST /api/runs rejected the DAG"
echo "run_id: $RUN"

RESULT=$(curl -fsS "${AUTH[@]}" "$API/api/runs/$RUN/wait?timeout_secs=120")
echo "$RESULT" | jq -c '{status, finished, failure}'
[ "$(echo "$RESULT" | jq -r .status)" = "succeeded" ] || die "smoke DAG did not succeed"

# Task-level, because a run can be 'succeeded' with a task that never ran if
# the DAG was mis-parsed. Four tasks in, four succeeded out.
TASKS=$(curl -fsS "${AUTH[@]}" "$API/api/runs/$RUN" \
  | jq -c '[.tasks[] | {name, status}] | sort_by(.name)')
echo "$TASKS"
[ "$(echo "$TASKS" | jq '[.[] | select(.status == "succeeded")] | length')" = 4 ] \
  || die "expected 4 succeeded tasks"

trap - ERR
say "PASS"
echo "console: $API  ($EMAIL / $PASSWORD) — ssh -L 8080:127.0.0.1:8080 to reach it from your laptop"
