#!/usr/bin/env bash
# The demo's verify: plays the agent's side of the demo over MCP, with no model, and
# prints PASS or FAIL (docs/WORKSHOP_PLAN.md §8: nobody is in the room).
#
#   ./verify.sh            start the stack, run every beat, leave the stack up
#   KEEP=0 ./verify.sh     ... and take it down afterwards
#
# It speaks to the published dagron-mcp image over stdio exactly as Claude Code
# or Claude Desktop would, making the tool calls the demo's agent makes, while
# doing the person's part (restart, approve, outage) through demo.sh. So a PASS
# means the demo works with the images a viewer pulls, whatever the model says.
set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=demo.sh
. "$HERE/demo.sh"

KEEP="${KEEP:-1}"
STDERR_LOG="$(mktemp)"
MCP_PID=""

fail() {
  echo "FAIL: $*" >&2
  if [ -s "$STDERR_LOG" ]; then
    echo "--- dagron-mcp stderr (last 20 lines)" >&2
    tail -n 20 "$STDERR_LOG" >&2
  fi
  exit 1
}
step() { echo "== $*"; }

cleanup() {
  if [ -n "$MCP_PID" ]; then
    eval "exec ${MCP[1]}>&-" 2>/dev/null || true
    kill "$MCP_PID" 2>/dev/null || true
  fi
  rm -f "$STDERR_LOG"
  [ "$KEEP" = 1 ] || down >/dev/null 2>&1 || true
}
trap cleanup EXIT

# --- the MCP session: one server process for the whole demo, as a client holds
# one. Lines are newline-delimited JSON-RPC; notifications arrive in between.
NEXT_ID=0
RESP=""
PROGRESS=0

send() { printf '%s\n' "$1" >&"${MCP[1]}"; }

# Read until the response to request $1 arrives, counting progress notifications.
recv() {
  local want="$1" line
  while IFS= read -r -t 700 line <&"${MCP[0]}"; do
    if [ "$(jq -r '.method // empty' <<<"$line")" = notifications/progress ]; then
      PROGRESS=$((PROGRESS + 1))
      continue
    fi
    if [ "$(jq -r '.id // empty' <<<"$line")" = "$want" ]; then
      RESP="$line"
      return 0
    fi
  done
  fail "no response to request $want from dagron-mcp"
}

request() { # request METHOD PARAMS_JSON
  NEXT_ID=$((NEXT_ID + 1))
  send "$(jq -cn --argjson id "$NEXT_ID" --arg m "$1" --argjson p "$2" \
    '{jsonrpc: "2.0", id: $id, method: $m, params: $p}')"
  recv "$NEXT_ID"
  if jq -e '.error' <<<"$RESP" >/dev/null; then
    fail "$1 returned an error: $(jq -c .error <<<"$RESP")"
  fi
}

# call TOOL ARGS_JSON [META_JSON]: sets TOOL_TEXT to the tool's text result.
TOOL_TEXT=""
call() {
  local params
  params="$(jq -cn --arg n "$1" --argjson a "$2" --argjson meta "${3:-null}" \
    '{name: $n, arguments: $a} + (if $meta then {_meta: $meta} else {} end)')"
  request tools/call "$params"
  if [ "$(jq -r '.result.isError // false' <<<"$RESP")" = true ]; then
    fail "$1 failed: $(jq -r '.result.content[0].text' <<<"$RESP")"
  fi
  TOOL_TEXT="$(jq -r '.result.content[0].text' <<<"$RESP")"
}

# Wait the way an agent does: dagron_wait_run in a loop, stopping at a terminal
# state or when the run is parked on a gate. Sets WAIT to the last wait result.
WAIT=""
wait_run() { # wait_run RUN_ID
  local i
  for i in $(seq 1 10); do
    call dagron_wait_run "$(jq -cn --arg r "$1" '{run_id: $r, timeout_secs: 60}')" \
      "$(jq -cn --arg t "verify-$1-$i" '{progressToken: $t}')"
    WAIT="$TOOL_TEXT"
    [ "$(jq -r .finished <<<"$WAIT")" = true ] && return 0
    task_status "$1" review | grep -qx awaiting_approval && return 0
  done
  fail "run $1 neither finished nor reached its gate in 10 minutes"
}

task_field() { # task_field RUN_ID TASK FIELD (read as the person, over REST)
  api GET "/api/runs/$1" | jq -r --arg t "$2" --arg f "$3" \
    '[.tasks[] | select(.name == $t)][0][$f] // empty'
}
task_status() { task_field "$1" "$2" status; }

# ---------------------------------------------------------------------------

step "stack up, tool registered (dagron $DAGRON_VERSION)"
need
up >"$STDERR_LOG" 2>&1 || fail "the stack did not come up"
: >"$STDERR_LOG"
TOKEN="$(token)"
outage off >/dev/null

step "the agent connects"
mapfile -t ARGS < <(mcp_args "$TOKEN")
coproc MCP { "$DOCKER" "${ARGS[@]}" 2>"$STDERR_LOG"; }

request initialize '{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"d0-verify","version":"1"}}'
send '{"jsonrpc":"2.0","method":"notifications/initialized"}'
request tools/list '{}'
TOOLS="$(jq -r '.result.tools[].name' <<<"$RESP")"
for t in dagron_list_workflows dagron_run_workflow dagron_wait_run dagron_list_approvals dagron_get_run dagron_rerun_run; do
  grep -qx "$t" <<<"$TOOLS" || fail "tools/list has no $t"
done
# The approval is the person's: without DAGRON_MCP_ALLOW_APPROVE the agent cannot
# approve its own gate, which is what the demo tells the audience.
if grep -qx dagron_approve_task <<<"$TOOLS"; then
  fail "dagron_approve_task is offered; the demo says the agent cannot approve"
fi
echo "   $(wc -l <<<"$TOOLS" | tr -d ' ') tools, none of them approves a gate"

step "beat 1: the agent finds the tool by its tag"
call dagron_list_workflows "$(jq -cn --arg t "$TAG" '{tag: $t}')"
WF_ID="$(jq -r --arg n "$WORKFLOW_NAME" '[.[] | select(.name == $n)][0].id // empty' <<<"$TOOL_TEXT")"
[ -n "$WF_ID" ] || fail "dagron_list_workflows tag=$TAG does not list $WORKFLOW_NAME"

step "beat 2: the agent calls it with arguments"
call dagron_run_workflow "$(jq -cn --arg w "$WF_ID" \
  '{workflow_id: $w, parameters: {incident_id: "INC-4471", audience: "exec"}}')"
RUN="$(jq -r '.run_id // empty' <<<"$TOOL_TEXT")"
[ -n "$RUN" ] || fail "dagron_run_workflow returned no run_id: $TOOL_TEXT"
echo "   run $RUN"

step "beat 3: the engine restarts while the agent waits"
# The person's part, timed for the slow step: once `gather` is running, restart
# the engine. The agent's wait below is already blocked on dagron-api.
(
  for _ in $(seq 1 60); do
    [ "$(task_status "$RUN" gather)" = running ] && break
    sleep 1
  done
  sleep 3
  restart_engine >/dev/null 2>&1
  echo restarted >"$STDERR_LOG.restarted"
) &
RESTARTER=$!
PROGRESS=0
wait_run "$RUN"
wait "$RESTARTER" || true
[ -f "$STDERR_LOG.restarted" ] || fail "the engine was never restarted (gather never ran?)"
rm -f "$STDERR_LOG.restarted"
[ "$(task_status "$RUN" gather)" = succeeded ] || fail "gather did not succeed after the restart: $(task_status "$RUN" gather)"
GATHER_ATTEMPTS="$(task_field "$RUN" gather attempt)"
# The restart must have cost the step its attempt; a gather that finished before
# the restart would pass every other check here and prove nothing.
[ "${GATHER_ATTEMPTS:-0}" -ge 2 ] || fail "gather ran once (attempt $GATHER_ATTEMPTS): the restart did not land while it was running"
echo "   gather succeeded after the restart (attempt $GATHER_ATTEMPTS); $PROGRESS progress notifications"

step "beat 4: the run waits for a person, and the agent can say so"
[ "$(task_status "$RUN" review)" = awaiting_approval ] || fail "review is $(task_status "$RUN" review), expected awaiting_approval"
call dagron_list_approvals '{}'
jq -e --arg r "$RUN" 'map(select(.run_id == $r)) | length == 1' <<<"$TOOL_TEXT" >/dev/null \
  || fail "dagron_list_approvals does not show run $RUN's gate"
approve >/dev/null
wait_run "$RUN"
[ "$(jq -r .status <<<"$WAIT")" = succeeded ] || fail "run $RUN ended $(jq -r .status <<<"$WAIT")"
jq -r '.result // ""' <<<"$WAIT" | grep -q 'INC-4471' || fail "the result does not carry the summary: $(jq -c .result <<<"$WAIT")"
echo "   the agent got: $(jq -r .result <<<"$WAIT" | head -c 90)..."

step "beat 5: a failure the agent can read, and a rerun of only what failed"
outage on >/dev/null
call dagron_run_workflow "$(jq -cn --arg w "$WF_ID" \
  '{workflow_id: $w, parameters: {incident_id: "INC-4472", audience: "engineer"}}')"
RUN2="$(jq -r '.run_id // empty' <<<"$TOOL_TEXT")"
[ -n "$RUN2" ] || fail "second dagron_run_workflow returned no run_id"
wait_run "$RUN2"
approve >/dev/null
wait_run "$RUN2"
[ "$(jq -r .status <<<"$WAIT")" = failed ] || fail "run $RUN2 ended $(jq -r .status <<<"$WAIT"), expected failed with the status page down"
call dagron_get_run "$(jq -cn --arg r "$RUN2" '{run_id: $r}')"
[ "$(jq -r '.failure.task_name // empty' <<<"$TOOL_TEXT")" = publish ] \
  || fail "the failure summary does not name publish: $(jq -c .failure <<<"$TOOL_TEXT")"
jq -r '.failure.message // ""' <<<"$TOOL_TEXT" | grep -q 'status page unreachable' \
  || fail "the failure summary does not carry the reason: $(jq -c .failure <<<"$TOOL_TEXT")"
echo "   the agent reads: $(jq -r '.failure.message' <<<"$TOOL_TEXT" | tail -n 1)"
GATHER_BEFORE="$(task_field "$RUN2" gather attempt)"
outage off >/dev/null
call dagron_rerun_run "$(jq -cn --arg r "$RUN2" '{run_id: $r}')"
wait_run "$RUN2"
[ "$(jq -r .status <<<"$WAIT")" = succeeded ] || fail "rerun of $RUN2 ended $(jq -r .status <<<"$WAIT")"
[ "$(task_field "$RUN2" gather attempt)" = "$GATHER_BEFORE" ] || fail "the rerun pulled the timeline again"
[ "$(task_status "$RUN2" review)" = succeeded ] || fail "the rerun reopened the gate: review is $(task_status "$RUN2" review)"
echo "   rerun succeeded; gather was not repeated and the approval was not asked for again"

echo "PASS: tool found, called, survived an engine restart, waited for a person, failed legibly, reran only publish"
