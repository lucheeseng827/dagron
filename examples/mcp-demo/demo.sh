#!/usr/bin/env bash
# The five-minute durable tool: dagron over MCP. See README.md.
#
#   ./demo.sh up               start the quickstart stack and register the tool
#   ./demo.sh client claude    print the `claude mcp add` line for Claude Code
#   ./demo.sh client desktop   print the mcpServers entry for Claude Desktop
#   ./demo.sh restart-engine   the beat: restart the engine mid-call
#   ./demo.sh approve          approve the pending gate, as the person would
#   ./demo.sh outage on|off    take the (simulated) status page down, or back
#   ./demo.sh down             remove the stack and its volumes
#
# Needs docker (or DOCKER=podman), curl and jq. Everything runs from published
# images; nothing is built.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
DOCKER="${DOCKER:-docker}"
DAGRON_VERSION="${DAGRON_VERSION:-0.10.0}"
export DAGRON_VERSION
PROJECT="${DEMO_PROJECT:-dagron-mcp-demo}"
COMPOSE_FILE="${DEMO_COMPOSE_FILE:-$HERE/../../compose.quickstart.yaml}"
DAGRON_URL="${DAGRON_URL:-http://localhost:8080}"
NETWORK="${PROJECT}_default"
MCP_IMAGE="docker.io/mancube/dagron-mcp:${DAGRON_VERSION}"
SPEC="$HERE/incident_summary.yaml"
WORKFLOW_NAME="summarise-incident"
TAG="agent-callable"

die() { echo "demo: $*" >&2; exit 1; }

compose() { "$DOCKER" compose -p "$PROJECT" -f "$COMPOSE_FILE" "$@"; }

need() {
  for tool in "$DOCKER" curl jq; do
    command -v "$tool" >/dev/null || die "$tool is required"
  done
}

# A session token for the quickstart's seeded admin. It stands in for the person
# who wires the agent up; the agent's MCP server sends it on every call.
token() {
  curl -fsS -X POST "$DAGRON_URL/api/login" -H 'content-type: application/json' \
    -d '{"email":"admin@local","password":"dagron-admin"}' | jq -er .token
}

api() { # api METHOD PATH [JSON]
  local method="$1" path="$2" body="${3:-}"
  if [ -n "$body" ]; then
    curl -fsS -X "$method" "$DAGRON_URL$path" -H "authorization: Bearer $TOKEN" \
      -H 'content-type: application/json' --data "$body"
  else
    curl -fsS -X "$method" "$DAGRON_URL$path" -H "authorization: Bearer $TOKEN"
  fi
}

wait_for_api() {
  for _ in $(seq 1 120); do
    curl -fsS "$DAGRON_URL/healthz" >/dev/null 2>&1 && return 0
    sleep 1
  done
  die "dagron-api did not answer $DAGRON_URL/healthz within 120 s ($DOCKER compose -p $PROJECT logs)"
}

workflow_id() {
  api GET "/api/workflows?tag=$TAG" \
    | jq -r --arg n "$WORKFLOW_NAME" '[.[] | select(.name == $n)][0].id // empty'
}

# Register the script as a named workflow. Running it twice updates it in place,
# which records the old definition as a version rather than making a second tool.
register() {
  TOKEN="$(token)"
  local id
  id="$(workflow_id)"
  if [ -z "$id" ]; then
    api POST /api/workflows "$(jq -Rn --rawfile s "$SPEC" '{spec: $s}')" >/dev/null
    id="$(workflow_id)"
    [ -n "$id" ] || die "registered $WORKFLOW_NAME, but it is not listed under tag $TAG"
    echo "registered $WORKFLOW_NAME ($id)"
  else
    api PUT "/api/workflows/$id" "$(jq -Rn --rawfile s "$SPEC" '{spec: $s}')" >/dev/null
    echo "updated $WORKFLOW_NAME ($id)"
  fi
}

up() {
  need
  compose up -d
  wait_for_api
  register
  echo
  echo "console: $DAGRON_URL  (admin@local / dagron-admin)"
  echo "next:    ./demo.sh client claude   (or: client desktop)"
}

# The MCP server joins the stack's network and reaches dagron-api by its service
# name, which works the same on Linux, macOS and Windows. That address is not
# loopback, so the server refuses to send a token over plain http unless told
# to; inside a private compose network that is the right call for a demo, and
# the wrong one anywhere else (docs/MCP.md, security best practices).
mcp_args() {
  printf '%s\n' run -i --rm --network "$NETWORK" \
    -e DAGRON_API_URL=http://dagron-api:8080 \
    -e "DAGRON_MCP_TOKEN=$1" \
    -e DAGRON_MCP_ALLOW_PLAINTEXT_TOKEN=1 \
    "$MCP_IMAGE"
}

client() {
  need
  TOKEN="$(token)"
  local -a args
  mapfile -t args < <(mcp_args "$TOKEN")
  case "${1:-claude}" in
    claude)
      printf 'claude mcp add dagron -- %s' "$DOCKER"
      printf ' %q' "${args[@]}"
      printf '\n'
      ;;
    desktop)
      # Through stdin, not --args: jq reads a later "-i" or "-e" as its own option.
      printf '%s\n' "${args[@]}" \
        | jq -R . | jq -s --arg cmd "$DOCKER" '{mcpServers: {dagron: {command: $cmd, args: .}}}'
      ;;
    *) die "client: claude or desktop" ;;
  esac
  echo "# the token is a session token for the quickstart admin; it expires, so rerun this if calls start failing with 401" >&2
}

restart_engine() {
  echo "restarting the engine; dagron-api, and so the agent's MCP session, stays up"
  compose restart engine
}

approve() {
  TOKEN="$(token)"
  local gates
  gates="$(api GET /api/approvals | jq -c --arg n "$WORKFLOW_NAME" '[.[] | select(.workflow_name == $n)]')"
  [ "$(jq length <<<"$gates")" -gt 0 ] || die "no $WORKFLOW_NAME gate is waiting"
  jq -c '.[]' <<<"$gates" | while read -r g; do
    local run task
    run="$(jq -r .run_id <<<"$g")"
    task="$(jq -r .task_id <<<"$g")"
    api POST "/api/runs/$run/tasks/$task/approve" '{"comment":"approved in the demo"}' >/dev/null
    echo "approved run $run"
  done
}

# The local executor runs tasks inside the engine container, so the flag lives
# there. It survives `restart-engine` (a restart keeps the container) and goes
# with `down`.
outage() {
  case "${1:-}" in
    on)  compose exec -T engine sh -c 'mkdir -p /tmp/dagron-mcp-demo && touch /tmp/dagron-mcp-demo/statuspage-down'
         echo "status page is down" ;;
    off) compose exec -T engine rm -f /tmp/dagron-mcp-demo/statuspage-down
         echo "status page is back" ;;
    *) die "outage: on or off" ;;
  esac
}

down() { compose down -v; }

main() {
  local cmd="${1:-}"
  shift || true
  case "$cmd" in
    up) up ;;
    register) need; register ;;
    client) client "$@" ;;
    restart-engine) restart_engine ;;
    approve) need; approve ;;
    outage) outage "$@" ;;
    down) down ;;
    *) sed -n '2,13p' "${BASH_SOURCE[0]}" | sed 's/^# \{0,1\}//'; exit 2 ;;
  esac
}

if [ "${BASH_SOURCE[0]}" = "$0" ]; then
  main "$@"
fi
