#!/usr/bin/env bash
# The CI load harness: one cell on one machine's container runtime, a
# synthetic forge beside it, and steps of load sent through dagron-ci's
# webhook door.
#
#   run.sh build            build the three images from this checkout
#   run.sh up               start the cell: Postgres, the engines, dagron-api,
#                           dagron-ci, and the forge with the driver
#   run.sh step NAME JOBS SECS HOLD [LPS] [KIB] [PARK]
#                           one step of load, measured, into $OUT/NAME; PARK
#                           is work that waits beside the jobs, as
#                           `drive.mjs park --spec` takes it
#   run.sh campaign FILE    each step FILE lists, one after another, and a
#                           table of them; a line that begins `durable` is a
#                           durability step, the rest of it as below
#   run.sh durable NAME TARGET AT DOWN [PIPELINES] [SHAPE]
#                           the durability demo: PIPELINES
#                           pipelines of three jobs pushed, TARGET (engine,
#                           engines, ci or api) killed with SIGKILL at AT
#                           (intake: as the pushes are taken; mid: once
#                           every pipeline's second job runs), started again
#                           DOWN seconds later, and each pipeline checked to
#                           have ended as it should; into $OUT/NAME
#   run.sh watch            a Prometheus and a Grafana beside the cell, with
#                           the dashboard loaded, on http://127.0.0.1:3300
#   run.sh down             remove everything it started, volumes included
#
# The cell is what dagron runs as: engines on one Postgres, with dagron-api
# and dagron-ci in front. Jobs run on the engines' local executor, as
# processes inside the engine containers. So this loads the control plane (the
# scheduling database, the reconcile loop, leases, logs) and nothing of what a
# job costs on Kubernetes or Docker: no pod, no image pull, no network.
#
# What a step prints is a measurement of this machine, with the cell's every
# part on it. It is not a capacity figure for a deployment, and nothing here
# turns it into one. README.md says what each figure is and is not.
#
#   CONTAINER   docker (default) or podman
#   NAME        prefix of everything it makes (default dgl)
#   REPLICAS    engines (default 3)
#   WORKERS     each engine's WORKER_COUNT, the jobs one engine holds at once
#               (default 400). A step of more than REPLICAS x WORKERS jobs
#               has the rest wait, which is then what it measures.
#   ENGINE_IMAGE, API_IMAGE, CI_IMAGE
#               images to run, where `build` is not wanted. The engine's must
#               have a shell: the local executor runs a job's steps with `sh`
#               (the `localdev` target of the module's Dockerfile has one).
#   ENGINE_ENV  extra `-e NAME=value` arguments for the engines, to try a knob
#   GRAFANA_PORT  where `watch` publishes Grafana on this host (default 3300)
#   OUT         where results go on this host (default ./out)
set -euo pipefail
export MSYS_NO_PATHCONV=1

RT="${CONTAINER:-docker}"
N="${NAME:-dgl}"
REPLICAS="${REPLICAS:-3}"
WORKERS="${WORKERS:-400}"
native() { if command -v cygpath >/dev/null 2>&1; then cygpath -m "$1"; else printf '%s' "$1"; fi; }
HERE="$(native "$(cd "$(dirname "$0")" && pwd)")"
MODULE="$(native "$(cd "$HERE/../.." && pwd)")"
OUT="${OUT:-$HERE/out}"
ENGINE_IMAGE="${ENGINE_IMAGE:-$N-engine:local}"
API_IMAGE="${API_IMAGE:-$N-api:local}"
CI_IMAGE="${CI_IMAGE:-$N-ci:local}"
PG_IMAGE="${PG_IMAGE:-docker.io/library/postgres:16-alpine}"
NODE_IMAGE="${NODE_IMAGE:-docker.io/library/node:22-slim}"
PROM_IMAGE="${PROM_IMAGE:-docker.io/prom/prometheus:latest}"
GRAFANA_IMAGE="${GRAFANA_IMAGE:-docker.io/grafana/grafana:latest}"
PGEXP_IMAGE="${PGEXP_IMAGE:-docker.io/prometheuscommunity/postgres-exporter:latest}"
PGURL="postgres://postgres:pw@$N-pg:5432"
REPO="load/synthetic"
# The durability demo's repository: its one pipeline is pipelines/load/durable.
DURABLE_REPO="load/durable"
# The steps read before dagron-ci had done with their runs (`step`): their
# counts are not final, and the command fails once it has reported them.
NOT_FINAL=()

say()  { printf '\n== %s\n' "$*"; }
fail() { printf 'FAIL %s\n' "$*" >&2; exit 1; }
hub()  { "$RT" exec -i "$N-hub" "$@"; }
sec()  { hub cat "/secrets/$1"; }
psql() { "$RT" exec -i "$N-pg" psql -h 127.0.0.1 -U postgres -AtX "$@"; }
logs() { "$RT" logs "$1" 2>&1 | sed 's/\x1b\[[0-9;]*m//g' | tail -"${2:-25}"; }
engines() { seq 1 "$REPLICAS" | sed "s|.*|http://$N-engine-&:8081|" | paste -sd, -; }
# epoch_ms SQL: a timestamp of SQL, as milliseconds since the epoch
epoch_ms() { printf "(extract(epoch FROM %s) * 1000)::bigint" "$1"; }
# sleep_until EPOCH
sleep_until() { local left=$(( $1 - $(date +%s) )); [ "$left" -le 0 ] || sleep "$left"; }

build() {
  local engine api v
  v="$(git -C "$MODULE" rev-parse --short HEAD)"
  say "building the engine (with a shell), dagron-api and dagron-ci from $v"
  engine="$(bash "$MODULE/scripts/published-features.sh" engine)"
  api="$(bash "$MODULE/scripts/published-features.sh" api)"
  "$RT" build -q -t "$ENGINE_IMAGE" -f "$MODULE/Dockerfile" --target localdev --build-arg "FEATURES=$engine" --build-arg "APP_VERSION=$v" "$MODULE" >/dev/null || fail "the engine image"
  "$RT" build -q -t "$API_IMAGE" -f "$MODULE/crates/dagron-api/Dockerfile" --build-arg "CARGO_FEATURES=$api" --build-arg "APP_VERSION=$v" "$MODULE" >/dev/null || fail "the dagron-api image"
  "$RT" build -q -t "$CI_IMAGE" -f "$MODULE/crates/dagron-ci/Dockerfile" --build-arg "APP_VERSION=$v" "$MODULE" >/dev/null || fail "the dagron-ci image"
  echo "built $ENGINE_IMAGE, $API_IMAGE, $CI_IMAGE"
}

down() {
  local c
  for c in $("$RT" ps -a --format '{{.Names}}' | grep -E "^$N-(pg|hub|api|ci|peek|prom|grafana|pgexp|engine-[0-9]+)$" || true); do "$RT" rm -f "$c" >/dev/null 2>&1 || true; done
  for c in kit secrets data pgdata marks; do "$RT" volume rm -f "$N-$c" >/dev/null 2>&1 || true; done
  "$RT" network rm "$N" >/dev/null 2>&1 || true
}

up() {
  local image i
  for image in "$ENGINE_IMAGE" "$API_IMAGE" "$CI_IMAGE"; do
    "$RT" image inspect "$image" >/dev/null 2>&1 || fail "no image $image: run \`$0 build\`, or name one with ENGINE_IMAGE, API_IMAGE, CI_IMAGE"
  done
  down
  "$RT" network create "$N" >/dev/null
  for i in kit secrets data pgdata marks; do "$RT" volume create "$N-$i" >/dev/null; done

  say "Postgres, which counts its statements"
  "$RT" run -d --name "$N-pg" --network "$N" -e POSTGRES_PASSWORD=pw -v "$N-pgdata:/var/lib/postgresql/data" "$PG_IMAGE" \
    -c shared_preload_libraries=pg_stat_statements -c pg_stat_statements.max=5000 -c max_connections=300 >/dev/null
  for i in $(seq 1 60); do "$RT" exec "$N-pg" createdb -h 127.0.0.1 -U postgres engine >/dev/null 2>&1 && break; sleep 1; done
  psql -d postgres -c "CREATE DATABASE ci" -c "CREATE EXTENSION IF NOT EXISTS pg_stat_statements" >/dev/null || fail "Postgres never took its databases"

  say "the forge and the driver"
  # The harness's files go in by a volume, not a mount of this directory: a
  # runtime in a VM does not see every host path.
  (cd "$HERE" && tar -cf - forge.mjs drive.mjs report.mjs durable.mjs peek.mjs pipelines grafana) \
    | "$RT" run --rm -i -v "$N-kit:/kit" "$NODE_IMAGE" tar -xf - -C /kit || fail "staging the harness"
  # /marks is where the durability demo's jobs say that they ran: every
  # engine has it, and it outlives one that is killed.
  "$RT" run -d --name "$N-hub" --network "$N" -v "$N-kit:/kit:ro" -v "$N-secrets:/secrets" -v "$N-data:/data" \
    -v "$N-marks:/marks" -e PORT=9080 -e PIPELINES_DIR=/kit/pipelines "$NODE_IMAGE" node /kit/forge.mjs >/dev/null
  hub sh -c 'touch /marks/marks && chmod 1777 /marks && chmod 666 /marks/marks'
  # The containers' CPU time and memory, read from the runtime's own cgroup
  # tree (peek.mjs): dagron-api's and dagron-ci's images have no shell to
  # read their own with.
  "$RT" run -d --name "$N-peek" --network none --cgroupns=host -v /sys/fs/cgroup:/host-cgroup:ro -v "$N-kit:/kit:ro" \
    "$NODE_IMAGE" sleep infinity >/dev/null
  hub node /kit/drive.mjs secrets
  hub node /kit/drive.mjs wait http://127.0.0.1:9080/_load/health 30 || { logs "$N-hub"; fail "the forge never answered"; }

  say "$REPLICAS engines on the local executor, $WORKERS jobs each"
  # One at a time: the first applies the migrations. The engine's image
  # carries example workflows, and none is wanted here. And no limit on
  # processes: every job is a few of them inside its engine's container, and
  # a runtime's default (2,048 under Podman) is about five hundred jobs, past
  # which a job fails with "Cannot fork" and the step has measured the
  # container.
  for i in $(seq 1 "$REPLICAS"); do
    # METRICS_CACHE_SECS=0: the driver reads the datastore gauges (running tasks,
    # queue depth) every few seconds to time the window and to see the cell drain,
    # so they must be what the database says then, not the engine's last read; it
    # also keeps these results comparable with runs from before the cache existed.
    # MAX_INFLIGHT_RUNS=0: the runs `drive.mjs park` posts to the engines' own
    # API would be refused by the valve there, which counts dagron-ci's runs as
    # well; no other run comes that way. ENGINE_ENV comes last, so it can set
    # either back.
    # shellcheck disable=SC2086
    "$RT" run -d --name "$N-engine-$i" --network "$N" --tmpfs /etc/dagron/examples --pids-limit -1 -v "$N-marks:/marks" \
      -e "DATABASE_URL=$PGURL/engine" -e API_ADDR=0.0.0.0:8081 -e EXECUTOR=local -e "WORKER_COUNT=$WORKERS" \
      -e METRICS_CACHE_SECS=0 -e MAX_INFLIGHT_RUNS=0 ${ENGINE_ENV:-} "$ENGINE_IMAGE" >/dev/null
    hub node /kit/drive.mjs wait "http://$N-engine-$i:8081/healthz" 120 || { logs "$N-engine-$i"; fail "engine $i never answered /healthz"; }
  done

  say "dagron-api and dagron-ci, beside the forge"
  # In the forge's network namespace: dagron-ci takes a cell and a forge API
  # over plain http only on its own machine, and this makes them that.
  "$RT" run -d --name "$N-api" --network "container:$N-hub" -e "DATABASE_URL=$PGURL/engine" -e PORT=8080 \
    -e "DAGRON_JWT_SECRET=$(sec jwt-secret)" -e DAGRON_COOKIE_SECURE=false \
    -e DAGRON_ADMIN_EMAIL=load-admin@load.test -e "DAGRON_ADMIN_PASSWORD=$(sec admin-password)" "$API_IMAGE" >/dev/null
  hub node /kit/drive.mjs wait http://127.0.0.1:8080/healthz 90 || { logs "$N-api"; fail "dagron-api never answered /healthz"; }
  hub node /kit/drive.mjs identities || fail "dagron-ci's user and token"
  local ci_env=(-e "CI_DATABASE_URL=$PGURL/ci" -e CI_LISTEN=0.0.0.0:8090 -e CI_CELLS=default=http://127.0.0.1:8080
    -e CI_API_TOKEN_FILE=/secrets/ci-api-token -e CI_GITHUB_WEBHOOK_SECRET_FILE=/secrets/webhook-secret
    -e CI_GITHUB_API_URL=http://127.0.0.1:9080 -e CI_GITHUB_TOKEN_FILE=/secrets/github-token -e CI_POLL_SECS=0)
  "$RT" run -d --name "$N-ci" --network "container:$N-hub" -v "$N-secrets:/secrets:ro" "${ci_env[@]}" "$CI_IMAGE" serve >/dev/null
  hub node /kit/drive.mjs wait http://127.0.0.1:8090/readyz 60 || { logs "$N-ci"; fail "dagron-ci never answered /readyz"; }
  for i in "$REPO" "$DURABLE_REPO"; do
    "$RT" run --rm --network "container:$N-hub" -v "$N-secrets:/secrets:ro" "${ci_env[@]}" "$CI_IMAGE" repo add "github.com/$i" >/dev/null \
      || fail "connecting the repository $i"
  done
  echo "up: $REPLICAS engines, dagron-api, dagron-ci; the repositories github.com/$REPO and github.com/$DURABLE_REPO are connected"
}

# peeked: the CPU time and memory each container of the cell has used so far
# (peek.mjs), as a fragment of JSON: Postgres, then the engines, then
# dagron-api and dagron-ci, in that order.
peeked() {
  local c args=()
  for c in "$N-pg" $(seq 1 "$REPLICAS" | sed "s|^|$N-engine-|") "$N-api" "$N-ci"; do
    args+=("$c=$("$RT" inspect -f '{{.Id}}' "$c" 2>/dev/null || echo none)")
  done
  "$RT" exec "$N-peek" node /kit/peek.mjs "${args[@]}" 2>/dev/null \
    || printf '"cpu_usec": {}, "mem_bytes": {}, "anon_bytes": {}'
}
# snapshot: what the database and each container had done by now, as JSON.
# The statements, with their time, rows and buffers, are one reading,
# stamped by the database, with each database's size. The engine's two
# busiest tables, their updates and how many of those were HOT, and each of
# their indexes' reads, are another, from the engine's own database, where
# those counts are kept.
snapshot() {
  local before after
  before=$(date +%s)
  printf '{'
  peeked
  after=$(date +%s)
  printf ', "cpu_at": %s, ' "$(( (before + after) / 2 ))"
  psql -d postgres -c "
    SELECT '\"db_bytes\": ' || (SELECT json_object_agg(datname, pg_database_size(datname)) FROM pg_database WHERE datname IN ('engine', 'ci'))
      || ', \"at\": ' || extract(epoch FROM clock_timestamp())
      || ', \"statements\": ' || (SELECT coalesce(json_agg(s), '[]') FROM (
           SELECT d.datname AS db, left(regexp_replace(p.query, '\s+', ' ', 'g'), 200) AS query,
                  sum(p.calls)::bigint AS calls, round(sum(p.total_exec_time)::numeric, 2)::float8 AS ms, sum(p.rows)::bigint AS rows,
                  sum(p.shared_blks_hit + p.shared_blks_read)::bigint AS blks
           FROM pg_stat_statements p JOIN pg_database d ON d.oid = p.dbid
           WHERE d.datname IN ('engine', 'ci') GROUP BY 1, 2) s)
      || ', \"database\": ' || (SELECT json_object_agg(datname, json_build_object(
           'xact_commit', xact_commit, 'xact_rollback', xact_rollback, 'tup_inserted', tup_inserted,
           'tup_updated', tup_updated, 'tup_deleted', tup_deleted, 'blks_read', blks_read))
           FROM pg_stat_database WHERE datname IN ('engine', 'ci'))"
  printf ', '
  psql -d engine -c "
    SELECT '\"tables\": ' || (SELECT coalesce(json_object_agg(relname, json_build_object(
             'n_tup_upd', n_tup_upd, 'n_tup_hot_upd', n_tup_hot_upd)), '{}')
           FROM pg_stat_user_tables WHERE relname IN ('task_runs', 'workflow_runs'))
      || ', \"indexes\": ' || (SELECT coalesce(json_object_agg(indexrelname, json_build_object(
             'table', relname, 'idx_scan', idx_scan, 'idx_tup_read', idx_tup_read)), '{}')
           FROM pg_stat_user_indexes WHERE relname IN ('task_runs', 'workflow_runs'))
      || '}'"
}

step() {
  local name=$1 jobs=$2 secs=$3 hold=$4 lps=${5:-0} kib=${6:-0} park=${7:-}
  local dir="/data/$name" t0 driver parker=""
  [ "$jobs" -le $((REPLICAS * WORKERS)) ] || echo "note: $jobs jobs is more than the $((REPLICAS * WORKERS)) the engines hold at once: the rest will wait"
  say "step $name: $jobs jobs of $secs s at once, held $hold s (log $lps lines/s each, artifact $kib KiB)${park:+, beside $park}"
  hub sh -c "rm -rf '$dir' && mkdir -p '$dir'"
  hub node -e 'fetch("http://127.0.0.1:9080/_load/reset", { method: "POST" }).then(r => r.ok || process.exit(1))' || fail "the forge"
  t0=$(date +%s)
  # What waits beside the jobs is posted as the step begins, and what comes
  # due goes on to the window's end.
  if [ -n "$park" ]; then
    hub node /kit/drive.mjs park --spec "$park" --for $((secs + hold)) --engines "$(engines)" \
      --wait-url "http://$N-hub:9080/_load/wait" --out "$dir" &
    parker=$!
  fi
  hub node /kit/drive.mjs load --jobs "$jobs" --secs "$secs" --hold "$hold" --lps "$lps" --kib "$kib" \
    --engines "$(engines)" --repo "$REPO" --out "$dir" &
  driver=$!
  # The measured window is the HOLD seconds after the first SECS. The counts
  # of statements and of CPU time are read at its two ends.
  sleep_until $((t0 + secs + 1));        snapshot | hub sh -c "cat > '$dir/at-window-start.json'"
  sleep_until $((t0 + secs + hold + 1)); snapshot | hub sh -c "cat > '$dir/at-window-end.json'"
  wait "$driver" || fail "the driver"
  [ -z "$parker" ] || wait "$parker" || fail "parking"
  # What the forge was asked for and told, over the whole step, once
  # dagron-ci follows none of its runs: a run's last statuses are posted a
  # second or two after its job's end, and a read before them misses them.
  local began open deadline waited=$(date +%s) final=true
  began=$(hub sed -n 's/.*"began": "\([^"]*\)".*/\1/p' "$dir/driver.json")
  deadline=$(( waited + 120 ))
  until open=$(psql -d ci -c "SELECT count(*) FROM dagron_ci.ci_runs r JOIN dagron_ci.ci_events e ON e.id = r.event_id
                 WHERE e.received_at >= '$began'::timestamptz
                   AND (r.status NOT IN ('succeeded', 'failed', 'cancelled') OR r.report_due_at IS NOT NULL)") && [ "$open" -eq 0 ]; do
    [ "$(date +%s)" -lt "$deadline" ] || { final=false; echo "NOT FINAL: dagron-ci still follows ${open:-?} of the step's runs after two minutes; the forge is read as it is, and the step's report says so"; break; }
    sleep 1
  done
  # Whether the counts that follow are final, for report.mjs to say, and for
  # the step to fail on once it has written them.
  printf '{"final": %s, "runs_still_followed": %s, "waited_seconds": %s}\n' "$final" "$([[ "$open" =~ ^[0-9]+$ ]] && echo "$open" || echo null)" \
    "$(( $(date +%s) - waited ))" | hub sh -c "cat > '$dir/followed.json'"
  [ "$final" = true ] || NOT_FINAL+=("$name")
  hub node -e 'fetch("http://127.0.0.1:9080/_load/stats").then(r => r.text()).then(t => process.stdout.write(t))' | hub sh -c "cat > '$dir/forge.json'"
  # What was parked goes before the next step, whose sweeps would read it.
  [ -z "$park" ] || hub node /kit/drive.mjs unpark --engines "$(engines)" --out "$dir" || fail "letting the parked runs go"
  collect "$name"
}

# collect NAME: what the two databases say of the step, then the report.
collect() {
  local name=$1 dir="/data/$1" from to began
  from=$(hub sed -n 's/.*"from_epoch": \([0-9.]*\).*/\1/p' "$dir/driver.json")
  to=$(hub sed -n 's/.*"to_epoch": \([0-9.]*\).*/\1/p' "$dir/driver.json")
  began=$(hub sed -n 's/.*"began": "\([^"]*\)".*/\1/p' "$dir/driver.json")
  local quantiles="json_build_object('count', count(*), 'p50', percentile_cont(0.5) WITHIN GROUP (ORDER BY ms), 'p95', percentile_cont(0.95) WITHIN GROUP (ORDER BY ms), 'p99', percentile_cont(0.99) WITHIN GROUP (ORDER BY ms), 'max', max(ms))"
  # Of the events this step delivered: how long each waited for its run (for
  # those delivered in the window), and how they ended.
  psql -d ci -c "
    WITH e AS (SELECT * FROM dagron_ci.ci_events WHERE received_at >= '$began'::timestamptz),
         r AS (SELECT r.*, e.received_at FROM dagron_ci.ci_runs r JOIN e ON e.id = r.event_id),
         w AS (SELECT extract(epoch FROM (created_at - received_at)) * 1000 AS ms FROM r WHERE extract(epoch FROM received_at) BETWEEN $from AND $to)
    SELECT json_build_object(
      'events', (SELECT count(*) FROM e),
      'events_by_state', (SELECT json_object_agg(state, n) FROM (SELECT state, count(*) n FROM e GROUP BY 1) s),
      'runs', (SELECT count(*) FROM r),
      'runs_by_status', (SELECT json_object_agg(status, n) FROM (SELECT status, count(*) n FROM r GROUP BY 1) s),
      'event_to_run_ms_in_window', (SELECT $quantiles FROM w))" | hub sh -c "cat > '$dir/ci.json'"
  # Of the runs this step made: how long each waited for its job to start,
  # by the job's own first line (for runs made in the window), and how the
  # jobs ended. Not of the runs `drive.mjs park` made, which are no jobs.
  psql -d engine -c "
    WITH r AS (SELECT wr.id, wr.created_at::timestamptz AS created FROM workflow_runs wr JOIN workflow_definitions d ON d.id = wr.definition_id
               WHERE wr.created_at::timestamptz >= '$began'::timestamptz AND d.name NOT LIKE 'park-%'),
         t AS (SELECT t.status, t.output, r.created, (substring(coalesce(t.log, t.output) FROM 'load-job-started ([0-9]+)'))::numeric / 1e6 AS started_ms FROM task_runs t JOIN r ON r.id = t.run_id),
         w AS (SELECT started_ms - extract(epoch FROM created) * 1000 AS ms FROM t WHERE started_ms IS NOT NULL AND extract(epoch FROM created) BETWEEN $from AND $to)
    SELECT json_build_object(
      'runs', (SELECT count(*) FROM r),
      'jobs_by_status', (SELECT json_object_agg(status, n) FROM (SELECT status, count(*) n FROM t GROUP BY 1) s),
      'jobs_that_said_when_they_started', (SELECT count(*) FROM t WHERE started_ms IS NOT NULL),
      'failed_jobs_say', (SELECT coalesce(json_agg(f), '[]'::json) FROM (SELECT count(*) AS jobs, left(regexp_replace(coalesce(output, ''), '\s+', ' ', 'g'), 160) AS output FROM t WHERE status = 'failed' GROUP BY 2 ORDER BY 1 DESC LIMIT 5) f),
      'run_to_job_started_ms_in_window', (SELECT $quantiles FROM w))" | hub sh -c "cat > '$dir/engine.json'"
  # Webhook to job, run by run, from what the system wrote down,
  # in milliseconds since the epoch: dagron-ci's (the event received, its run
  # recorded, the run's first pending status posted) and the engine's (the
  # run made, its job claimed, as the outbox's `task.started` has it, and the
  # job's end), with the job's own first line. One Postgres keeps both
  # databases, so all but the job's line are of its clock. report.mjs puts
  # them together with the driver's deliveries.
  psql -d ci -c "
    WITH e AS (SELECT * FROM dagron_ci.ci_events WHERE received_at >= '$began'::timestamptz)
    SELECT coalesce(json_agg(json_build_object(
      'event', e.id, 'sha', e.sha, 'received', $(epoch_ms e.received_at), 'run', r.run_id, 'recorded', $(epoch_ms r.created_at),
      'pending', (SELECT $(epoch_ms 'min(l.posted_at)') FROM dagron_ci.ci_report_log l
                  WHERE l.event_id = e.id AND l.run_id = r.run_id AND l.state = 'pending' AND l.outcome = 'posted'))), '[]')
    FROM e LEFT JOIN dagron_ci.ci_runs r ON r.event_id = e.id" | hub sh -c "cat > '$dir/latency-ci.json'"
  psql -d engine -c "
    WITH r AS (SELECT wr.id, wr.created_at::timestamptz AS created FROM workflow_runs wr JOIN workflow_definitions d ON d.id = wr.definition_id
               WHERE wr.created_at::timestamptz >= '$began'::timestamptz AND d.name NOT LIKE 'park-%'),
         s AS (SELECT o.run_id, min(o.created_at::timestamptz) AS claimed FROM event_outbox o JOIN r ON r.id = o.run_id
               WHERE o.event_type = 'task.started' GROUP BY 1)
    SELECT coalesce(json_agg(json_build_object(
      'run', r.id, 'made', $(epoch_ms r.created), 'claimed', $(epoch_ms s.claimed),
      'first_line', ((substring(coalesce(t.log, t.output) FROM 'load-job-started ([0-9]+)'))::numeric / 1e6)::bigint,
      'finished', $(epoch_ms t.finished_at::timestamptz), 'status', t.status, 'attempt', t.attempt)), '[]')
    FROM r JOIN task_runs t ON t.run_id = r.id LEFT JOIN s ON s.run_id = r.id" | hub sh -c "cat > '$dir/latency-engine.json'"
  hub node /kit/report.mjs "$dir"
  mkdir -p "$OUT/$name"
  hub tar -C "$dir" -cf - . | tar -xf - -C "$OUT/$name"
  echo "the step's files are in $OUT/$name"
}

# now_ms: the cell's clock, which the jobs' marks are of, in milliseconds
now_ms() { hub date +%s%3N; }

# durable NAME TARGET AT DOWN [PIPELINES] [SHAPE]: the durability demo.
#
# PIPELINES pushes to $DURABLE_REPO, each a pipeline of three jobs in a line
# (pipelines/load/durable/proof.yaml) lasting SHAPE's seconds, build/test/ship
# (default 10/30/5). At AT, TARGET is killed with SIGKILL: `engine` the first
# engine, `engines` every engine at once, `ci` dagron-ci, `api` dagron-api.
# `intake` is as soon as every push was taken (202), with events still being
# made into runs; `mid` is once every pipeline's second job has started. It
# is started again DOWN seconds later. Then it waits for every pipeline to
# end in dagron-ci, and durable.mjs checks what the system and the jobs say
# happened: durable.json, and a failed check fails the step.
durable() {
  local name=$1 target=$2 at=$3 down=$4 count=${5:-30} shape=${6:-10/30/5}
  local dir="/data/$name" victims=() c began from n deadline hosts="" killed_at killed_by started_at ready_at ended_at
  case "$target" in
    engine) victims=("$N-engine-1") ;;
    engines) mapfile -t victims < <(seq 1 "$REPLICAS" | sed "s|^|$N-engine-|") ;;
    ci) victims=("$N-ci") ;;
    api) victims=("$N-api") ;;
    *) fail "durable: TARGET is engine, engines, ci or api, not $target" ;;
  esac
  case "$at" in intake|mid) ;; *) fail "durable: AT is intake or mid, not $at" ;; esac
  say "durable $name: $count pipelines of three jobs ($shape s), ${victims[*]} killed at $at and started $down s later"
  hub sh -c "rm -rf '$dir' && mkdir -p '$dir'"
  began=$(hub date -u +%Y-%m-%dT%H:%M:%S.%3NZ)
  from=$(hub sh -c 'wc -l < /marks/marks')
  # A push that is not taken is sent again, as a forge redelivers, until it
  # is; and five that were taken are sent again once all were, which is to
  # make nothing new. At `intake` they go two a second, and the kill comes
  # once a third are taken: the rest find dagron-ci (or, behind it,
  # dagron-api) down.
  for c in "${victims[@]}"; do hosts="$hosts\"$("$RT" inspect -f '{{.Config.Hostname}}' "$c")\", "; done
  local pusher
  hub node /kit/drive.mjs push --repo "$DURABLE_REPO" --ref "proof/$shape" --count "$count" --out "$dir" \
    --redeliver $((down + 300)) --again 5 --progress "$dir/progress" $([ "$at" = intake ] && echo --rate 2) &
  pusher=$!
  if [ "$at" = intake ]; then
    deadline=$(( $(date +%s) + 120 ))
    until n=$(hub sh -c "wc -l 2>/dev/null < '$dir/progress'" || true); [ "${n:-0}" -ge $(( (count + 2) / 3 )) ]; do
      [ "$(date +%s)" -lt "$deadline" ] || fail "only ${n:-0} of the pushes were taken"
      sleep 0.2
    done
  else
    wait "$pusher" || fail "the pushes were not all taken"
  fi
  if [ "$at" = mid ]; then
    deadline=$(( $(date +%s) + 300 ))
    until n=$(hub sh -c "tail -n +$((from + 1)) /marks/marks | grep -c '^start .* test\$'" || true); [ "${n:-0}" -ge "$count" ]; do
      [ "$(date +%s)" -lt "$deadline" ] || fail "only ${n:-0} of $count pipelines' second jobs started"
      sleep 1
    done
  fi
  killed_at=$(now_ms)
  for c in "${victims[@]}"; do "$RT" kill -s KILL "$c" >/dev/null || fail "killing $c"; done
  killed_by=$(now_ms)
  echo "killed ${victims[*]} with SIGKILL at $(hub date -u +%T)"
  sleep "$down"
  started_at=$(now_ms)
  for c in "${victims[@]}"; do "$RT" start "$c" >/dev/null || fail "starting $c again"; done
  for c in "${victims[@]}"; do
    case "$c" in
      "$N-ci") hub node /kit/drive.mjs wait http://127.0.0.1:8090/readyz 120 ;;
      "$N-api") hub node /kit/drive.mjs wait http://127.0.0.1:8080/healthz 120 ;;
      *) hub node /kit/drive.mjs wait "http://$c:8081/healthz" 120 ;;
    esac || { logs "$c"; fail "$c never answered again"; }
  done
  ready_at=$(now_ms)
  echo "started again, and answering, at $(hub date -u +%T)"
  [ "$at" != intake ] || wait "$pusher" || fail "the pushes were not all taken"
  # The end: every event made, its runs made and none still open in
  # dagron-ci (a run's statuses are final once it is due no more). Ten
  # minutes at most: what did not end is durable.mjs's to say.
  deadline=$(( $(date +%s) + 600 ))
  local q="WITH e AS (SELECT e.id FROM dagron_ci.ci_events e JOIN dagron_ci.ci_repos p ON p.id = e.repo_id
                      WHERE p.full_name = '$DURABLE_REPO' AND e.received_at >= '$began'::timestamptz)
    SELECT (SELECT count(*) FROM e) || ' ' || (SELECT count(*) FROM dagron_ci.ci_runs r JOIN e ON e.id = r.event_id)
      || ' ' || (SELECT count(*) FROM dagron_ci.ci_runs r JOIN e ON e.id = r.event_id
                 WHERE r.status NOT IN ('succeeded', 'failed', 'cancelled') OR r.report_due_at IS NOT NULL)"
  until n=$(psql -d ci -c "$q") && set -- $n && [ "$1" -eq "$count" ] && [ "$2" -ge "$count" ] && [ "$3" -eq 0 ]; do
    [ "$(date +%s)" -lt "$deadline" ] || { echo "not over after ten minutes: events, runs, runs open: $n"; break; }
    sleep 2
  done
  ended_at=$(now_ms)
  # What happened, as the system and the jobs wrote it down.
  hub sh -c "cat > '$dir/kill.json'" <<EOF
{"target": "$target", "at": "$at", "down": $down, "pipelines": $count, "shape": "$shape", "repo": "$DURABLE_REPO",
 "victims": [$(printf '"%s", ' "${victims[@]}" | sed 's/, $//')], "hostnames": [${hosts%, }],
 "began": "$began", "killed_at": $killed_at, "killed_by": $killed_by, "started_at": $started_at, "ready_at": $ready_at, "ended_at": $ended_at}
EOF
  hub sh -c "tail -n +$((from + 1)) /marks/marks > '$dir/marks.txt' && cp /data/statuses.jsonl '$dir/statuses.jsonl'"
  psql -d ci -c "
    WITH e AS (SELECT e.* FROM dagron_ci.ci_events e JOIN dagron_ci.ci_repos p ON p.id = e.repo_id
               WHERE p.full_name = '$DURABLE_REPO' AND e.received_at >= '$began'::timestamptz)
    SELECT json_build_object(
      'events', (SELECT coalesce(json_agg(json_build_object('id', id, 'sha', sha, 'state', state, 'attempts', attempts, 'reason', reason,
                   'received', $(epoch_ms received_at))), '[]') FROM e),
      'runs', (SELECT coalesce(json_agg(json_build_object('run', r.run_id, 'event', r.event_id, 'pipeline', r.pipeline, 'status', r.status,
                   'reopens', r.reopens, 'report_error', r.report_error, 'recorded', $(epoch_ms r.created_at), 'ended', $(epoch_ms r.ended_at))), '[]')
               FROM dagron_ci.ci_runs r JOIN e ON e.id = r.event_id),
      'posted', (SELECT coalesce(json_agg(json_build_object('sha', l.sha, 'run', l.run_id, 'context', l.context, 'state', l.state,
                   'outcome', l.outcome, 'at', $(epoch_ms l.posted_at)) ORDER BY l.id), '[]')
               FROM dagron_ci.ci_report_log l JOIN e ON e.id = l.event_id))" | hub sh -c "cat > '$dir/ci.json'"
  psql -d engine -c "
    WITH r AS (SELECT wr.id, wr.status, wr.created_at, wr.finished_at, rt.sha FROM workflow_runs wr JOIN run_triggers rt ON rt.run_id = wr.id
               WHERE lower(rt.repo) = '$DURABLE_REPO' AND wr.created_at::timestamptz >= '$began'::timestamptz)
    SELECT json_build_object(
      'runs', (SELECT coalesce(json_agg(json_build_object('id', id, 'sha', sha, 'status', status,
                   'made', $(epoch_ms created_at::timestamptz), 'finished', $(epoch_ms finished_at::timestamptz))), '[]') FROM r),
      'tasks', (SELECT coalesce(json_agg(json_build_object('id', t.id, 'run', t.run_id, 'name', t.name, 'status', t.status, 'attempt', t.attempt,
                   'finished', $(epoch_ms t.finished_at::timestamptz))), '[]') FROM task_runs t JOIN r ON r.id = t.run_id))" \
    | hub sh -c "cat > '$dir/engine.json'"
  mkdir -p "$OUT/$name"
  local verdict=0
  hub node /kit/durable.mjs "$dir" || verdict=1
  hub tar -C "$dir" -cf - . | tar -xf - -C "$OUT/$name"
  echo "the step's files are in $OUT/$name"
  return $verdict
}

# campaign FILE: a step a line, `NAME JOBS SECS HOLD [LPS] [KIB] [PARK]`, or
# a durability step, `durable NAME TARGET AT DOWN [PIPELINES] [SHAPE]`. A
# durability step that fails its checks fails the campaign, at its end, and
# so does a step whose counts were read before dagron-ci had done with its
# runs: its row says so, and is not to be taken as final.
campaign() {
  local file=$1 names=() durables=() failed=() name rest
  [ -f "$file" ] || fail "no such file: $file"
  # On descriptor 3: a step's own commands read the standard input.
  while read -r name rest <&3; do
    case "$name" in ''|'#'*) continue ;; esac
    if [ "$name" = durable ]; then
      # shellcheck disable=SC2086
      set -- $rest
      durable "$@" || failed+=("$1")
      durables+=("/data/$1")
      continue
    fi
    # shellcheck disable=SC2086
    step "$name" $rest
    names+=("/data/$name")
  done 3< "$file"
  if [ ${#names[@]} -gt 0 ]; then
    say "the campaign's steps, each as it was measured on this machine"
    hub node /kit/report.mjs --table "${names[@]}" | tee "$OUT/$(basename "$file").md"
    hub node /kit/report.mjs --proof "${names[@]}" | tee "$OUT/$(basename "$file").proof.md"
  fi
  if [ ${#durables[@]} -gt 0 ]; then
    say "the durability steps"
    hub node /kit/durable.mjs --table "${durables[@]}" | tee "$OUT/$(basename "$file").durable.md"
  fi
  [ ${#NOT_FINAL[@]} -eq 0 ] || printf 'FAIL steps read before dagron-ci had done with their runs, so not final: %s\n' "${NOT_FINAL[*]}" >&2
  [ ${#failed[@]} -eq 0 ] || printf 'FAIL durability steps whose checks failed: %s\n' "${failed[*]}" >&2
  [ ${#NOT_FINAL[@]} -eq 0 ] && [ ${#failed[@]} -eq 0 ] || exit 1
}

# watch: a Prometheus that scrapes the cell, a Grafana with the dashboard
# loaded, and postgres-exporter for the dashboard's one panel of the
# database's. Nothing a step measures comes from these: they are for looking.
watch() {
  local port="${GRAFANA_PORT:-3300}" i targets="" c
  for i in $(seq 1 "$REPLICAS"); do targets="$targets'$N-engine-$i:8081', "; done
  for c in prom grafana pgexp; do "$RT" rm -f "$N-$c" >/dev/null 2>&1 || true; done
  hub sh -c 'rm -rf /data/watch && mkdir -p /data/watch/provisioning/datasources /data/watch/provisioning/dashboards /data/watch/dashboards && cp /kit/grafana/*.json /data/watch/dashboards/'
  hub sh -c 'cat > /data/watch/prometheus.yml' <<EOF
global: { scrape_interval: 5s }
scrape_configs:
  - job_name: engine
    static_configs: [{ targets: [${targets%, }] }]
  - job_name: dagron-ci
    static_configs: [{ targets: ['$N-hub:8090'] }]
  - job_name: postgres
    static_configs: [{ targets: ['$N-pgexp:9187'] }]
EOF
  hub sh -c 'cat > /data/watch/provisioning/datasources/prometheus.yaml' <<EOF
apiVersion: 1
datasources:
  - { name: Prometheus, type: prometheus, access: proxy, url: "http://$N-prom:9090", isDefault: true }
EOF
  hub sh -c 'cat > /data/watch/provisioning/dashboards/dagron.yaml' <<EOF
apiVersion: 1
providers:
  - { name: dagron, type: file, options: { path: /data/watch/dashboards } }
EOF
  "$RT" run -d --name "$N-pgexp" --network "$N" -e "DATA_SOURCE_NAME=postgresql://postgres:pw@$N-pg:5432/postgres?sslmode=disable" "$PGEXP_IMAGE" >/dev/null
  "$RT" run -d --name "$N-prom" --network "$N" -v "$N-data:/data:ro" "$PROM_IMAGE" --config.file=/data/watch/prometheus.yml >/dev/null
  "$RT" run -d --name "$N-grafana" --network "$N" -p "127.0.0.1:$port:3000" -v "$N-data:/data:ro" \
    -e GF_PATHS_PROVISIONING=/data/watch/provisioning -e GF_AUTH_ANONYMOUS_ENABLED=true -e GF_AUTH_ANONYMOUS_ORG_ROLE=Admin \
    -e GF_AUTH_DISABLE_LOGIN_FORM=true "$GRAFANA_IMAGE" >/dev/null
  hub node /kit/drive.mjs wait "http://$N-prom:9090/-/ready" 60 || { logs "$N-prom"; fail "Prometheus never came ready"; }
  hub node /kit/drive.mjs wait "http://$N-grafana:3000/api/health" 90 || { logs "$N-grafana"; fail "Grafana never came ready"; }
  echo "watching: Grafana on http://127.0.0.1:$port/d/dagron-ci-load (Prometheus is $N-prom:9090 on the cell's network)"
}

case "${1:-}" in
  build) build ;;
  up) up ;;
  down) down ;;
  step) shift; [ $# -ge 4 ] || fail "step NAME JOBS SECS HOLD [LPS] [KIB] [PARK]"; step "$@"
    [ ${#NOT_FINAL[@]} -eq 0 ] || fail "step $1 was read before dagron-ci had done with its runs: its counts are not final" ;;
  durable) shift; [ $# -ge 4 ] || fail "durable NAME TARGET AT DOWN [PIPELINES] [SHAPE]"; durable "$@" ;;
  campaign) [ $# -eq 2 ] || fail "campaign FILE"; campaign "$2" ;;
  watch) watch ;;
  *) sed -n '2,26p' "$0"; exit 2 ;;
esac
