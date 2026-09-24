# dagron MCP server

`dagron-mcp` exposes the dagron management API as [Model Context Protocol](https://modelcontextprotocol.io)
tools over stdio, so an AI agent (Claude Desktop, an IDE, or any MCP client) can
author, run, recover and inspect workflows — register a named workflow, run it
with arguments, wait on it, read its logs and artifacts, resolve an approval
gate, rerun what failed and record what it concluded — and observe the cluster's
live state while it does.

The MCP engine is the **agent-facing seam** to the Dagron cluster: it speaks the
same JWT-gated `dagron-api` the browser uses, so all access controls, validation,
and observability already enforced on the UI edge apply identically to agents.
See the [agent event-call sequence](ARCHITECTURE.md#58-mcp-agent-event-call--submit--bounded-sse-event-poll)
and the [system-context diagram](ARCHITECTURE.md#1-system-context) in
[`docs/ARCHITECTURE.md`](ARCHITECTURE.md).

## Tools

Forty-two tools spanning the whole loop — author, run, recover, inspect — so an
agent can take a workflow from "there is no such workflow" to "here is why it
failed and what I did about it" without a human dropping to `curl`.

**W** marks a tool that changes cluster state. `DAGRON_MCP_READONLY=1` hides all
eighteen of them from `tools/list` *and* refuses them on call — see
[read-only mode](#read-only-mode).

**Hints** is what each tool declares in its MCP `annotations`, which a client
reads to decide whether a call needs a person's approval. `read-only` is
`readOnlyHint: true`. A write states all three of `destructiveHint`,
`idempotentHint` and `openWorldHint`, and the column lists the ones that are
`true` as `destructive`, `idempotent` and `open-world`. On a dagron tool,
`open-world` means the call can set task code running. See
[tool annotations](#tool-annotations) for what each one means here and why each
write got its hints.

### Drive runs

| Tool | Arguments | dagron-api | Hints |
|---|---|---|---|
| `dagron_list_runs` | `status?`, `name?`, `trigger?`, `limit?`, `offset?` | `GET /api/runs` | read-only |
| `dagron_get_run` | `run_id` | `GET /api/runs/{id}` | read-only |
| **W** `dagron_submit_run` | `yaml`, `parameters?`, `idempotency_key?` | `POST /api/runs` | open-world |
| **W** `dagron_cancel_run` | `run_id` | `POST /api/runs/{id}/cancel` | destructive · idempotent |
| `dagron_wait_run` | `run_id`, `timeout_secs?` (1–600, default 30) | `GET /api/runs/{id}/wait` | read-only |

`dagron_wait_run` earns its own line. Without it an agent polls
`dagron_get_run` in a loop, spending a JSON-RPC round trip and a slice of
context window per tick to usually learn nothing. The route long-polls to
terminal and returns `result` and `failure` in the same body, so the tool is a
thin wrapper over work the server has already done.

`idempotency_key` is a **header** (`Idempotency-Key:`), not a body field: a
repeat of a submit under the same key returns the *same* `run_id` rather than
creating a second run. An agent retrying a submit it is not sure landed is
exactly the case that header exists for.

### Author and run registered workflows

| Tool | Arguments | dagron-api | Hints |
|---|---|---|---|
| `dagron_list_workflows` | `tag?` | `GET /api/workflows` | read-only |
| `dagron_get_workflow` | `workflow_id` | `GET /api/workflows/{id}` | read-only |
| **W** `dagron_create_workflow` | `spec`, `name?`, `description?` | `POST /api/workflows` → `201`; `409` duplicate name | idempotent · open-world |
| **W** `dagron_update_workflow` | `workflow_id`, `spec`, `name?`, `description?` | `PUT /api/workflows/{id}` (records the prior definition as a version) | destructive · open-world |
| **W** `dagron_delete_workflow` | `workflow_id` | `DELETE /api/workflows/{id}` | destructive · idempotent |
| **W** `dagron_set_workflow_state` | `workflow_id`, `state` (`active`/`paused`/`retired`) | `POST /api/workflows/{id}/state` | idempotent · open-world |
| **W** `dagron_run_workflow` | `workflow_id`, `parameters?` | `POST /api/workflows/{id}/run` → `201 {run_id}`; `409` paused/retired | open-world |
| `dagron_list_workflow_runs` | `workflow_id`, `limit?`, `offset?` | `GET /api/workflows/{id}/runs` | read-only |
| `dagron_list_workflow_versions` | `workflow_id` | `GET /api/workflows/{id}/versions` | read-only |

Registering a **named** workflow is what unlocks the parent/child DAG: a
`type: workflow` task resolves its child *by registered name*, so without
`dagron_create_workflow` that whole shape was unreachable from MCP.
`dagron_run_workflow` is the other half — it makes a stored workflow callable
as a function, arguments and all, instead of forcing the agent to fetch a spec,
splice values into it, and resubmit the result as new YAML.

`GET /api/workflows` has no server-side paging, so `dagron_list_workflows`
offers `tag` and returns the registry whole.

### Recover from a failure

| Tool | Arguments | dagron-api | Hints |
|---|---|---|---|
| **W** `dagron_rerun_run` | `run_id`, `from?` | `POST /api/runs/{id}/rerun` | destructive · open-world |
| **W** `dagron_resubmit_run` | `run_id` | `POST /api/runs/{id}/resubmit` → `201` fresh run from the same spec | open-world |
| **W** `dagron_retry_task` | `run_id`, `task_id` | `POST /api/runs/{id}/tasks/{tid}/retry` | destructive · open-world |
| **W** `dagron_clear_task` | `run_id`, `task_id` | `POST /api/runs/{id}/tasks/{tid}/clear` (task + downstream cone) | destructive · open-world |
| **W** `dagron_redrive_dead_letter` | `id` | `POST /api/dead-letters/{id}/redrive` | destructive · idempotent · open-world |
| **W** `dagron_delete_dead_letter` | `id` | `DELETE /api/dead-letters/{id}` | destructive · idempotent |
| **W** `dagron_triage_run` | `run_id`, `state` (`acknowledged`/`resolved`/`ignored`), `note?` | `POST /api/runs/{id}/triage` | destructive · idempotent |
| **W** `dagron_clear_triage` | `run_id` | `DELETE /api/runs/{id}/triage` | destructive · idempotent |

The triage pair is the one write in this group a reviewer *reads* rather than
reruns: it is where an agent writes down what it concluded about a failed run
(`triage_state`, `triage_note`, `triaged_by` live on the run row).

### Resolve approval gates

| Tool | Arguments | dagron-api | Hints |
|---|---|---|---|
| `dagron_list_approvals` | — | `GET /api/approvals` (the human-in-the-loop worklist) | read-only |
| **W** `dagron_approve_task` | `run_id`, `task_id` | `POST /api/runs/{id}/tasks/{tid}/approve` | idempotent · open-world |
| **W** `dagron_reject_task` | `run_id`, `task_id` | `POST /api/runs/{id}/tasks/{tid}/reject` | destructive · idempotent · open-world |

A `type: approval` task parks a run until someone resolves the gate. Before
these three, a DAG with a human-in-the-middle gate was a DAG an MCP agent could
start and then never finish.

### Read logs, artifacts and structure

| Tool | Arguments | dagron-api | Hints |
|---|---|---|---|
| `dagron_get_task_logs` | `run_id`, `task_id`, + [log filter](API.md#log-filter) | `GET /api/runs/{id}/tasks/{tid}/logs` | read-only |
| `dagron_get_run_logs` | `run_id`, `task`, + [log filter](API.md#log-filter) | `GET /api/runs/{id}/logs` | read-only |
| `dagron_get_run_graph` | `run_id` | `GET /api/runs/{id}/graph` — `{nodes[], edges[]}` | read-only |
| `dagron_get_run_spec` | `run_id` | `GET /api/runs/{id}/spec` — the YAML a run was actually made from | read-only |
| `dagron_get_artifact` | `run_id`, `task`, `name` | `GET /api/runs/{run_id}/artifacts/{task}/{name}` | read-only |
| `dagron_artifact_exists` | `run_id`, `task`, `name` | `GET /api/runs/{run_id}/artifacts/{task}/{name}/exists` | read-only |
| **W** `dagron_put_artifact` | `run_id`, `task`, `name`, `content` | `PUT /api/runs/{run_id}/artifacts/{task}/{name}` — seed an input file | destructive · idempotent |

`dagron_get_run_logs` is the one to reach for when a run failed: it returns
**every** task's output as one attributed, server-filtered stream, so the agent
makes one call instead of N guesses at which task printed the error. Both log
tools take the same filter arguments — `q`, `exclude`, `regex`, `level`, `case`,
`context`, `limit`, `tail` — all passed as strings and validated by the engine,
which owns the grammar. A typical triage call is
`{"run_id": "...", "level": "error", "context": "2"}`.

`dagron_get_artifact` is the missing half of any file-producing DAG — without it
the agent that *asked* for a file could not see the file. It returns text
inline; anything binary, or over the inline cap, comes back as size +
content-type + locator instead (see
[bounded by construction](#bounded-by-construction)). `dagron_get_run_spec`
closes an agent-specific loop: an agent that submitted ad-hoc YAML has no other
way to recover what it ran.

### Observe cluster internals

So the agent can reason about what the engine is doing, not just send commands:

| Tool | Arguments | dagron-api | Hints |
|---|---|---|---|
| `dagron_get_metrics` | — | `GET /api/metrics` (runs/tasks by status + dead-letter total) | read-only |
| `dagron_get_metrics_timeseries` | `days?` (1–90, default 14), `name?` | `GET /api/metrics/timeseries` | read-only |
| `dagron_get_health` | — | `GET /api/health` — `scheduler_leader`, `event_listener`, `active_runs`, `awaiting_approvals`, `dead_letters` | read-only |
| `dagron_search` | `q`, `limit?` (1–20, default 8) | `GET /api/search` — workflows/runs/schedules by name or id prefix | read-only |
| `dagron_list_dead_letters` | `limit?` (1–500, default 100) | `GET /api/dead-letters?limit=` | read-only |
| `dagron_get_run_events` | `run_id`, `wait_ms?` (100–10000, default 2000) | bounded read of `GET /api/runs/{id}/stream` (SSE) | read-only |

`dagron_search` is the quiet unlock. Every id-taking tool above assumes the
agent already holds a uuid; without it the only way to turn "the nightly ETL"
into one is to list everything and filter client-side, which costs a context
window to do badly.

`dagron_get_run_events` opens the same per-run SSE channel the browser uses,
collects events emitted within `wait_ms`, parses the SSE frames into JSON, and
returns them as a single tool response. The window is hard-capped at 10 s
wall-clock and 256 KiB so a JSON-RPC call always returns promptly — agents
poll the tool in a loop instead of holding a long-lived stream.

### Lineage and archive

| Tool | Arguments | dagron-api | Hints |
|---|---|---|---|
| `dagron_list_datasets` | `limit?` | `GET /api/datasets` — registry with `consumers[]` | read-only |
| `dagron_get_dataset_events` | `uri?`, `limit?` | `GET /api/datasets/events` — the lineage ledger | read-only |
| `dagron_list_archived_runs` | `name?`, `limit?`, `offset?` | `GET /api/archive/runs` | read-only |
| `dagron_get_archived_run` | `run_id` | `GET /api/archive/runs/{id}` — `410` once compacted to Parquet | read-only |

### Tool annotations

Every tool in `tools/list` carries MCP `annotations`, the hints a client reads
to decide whether a call needs a person's approval. Some clients run a tool
without asking only when it declares `readOnlyHint: true`, so before these
existed every dagron call waited for a person, `dagron_list_runs` included.

They are derived rather than written per tool. `readOnlyHint` is the same
read/write split `DAGRON_MCP_READONLY` enforces, so the 24 tools a client may
run unasked are exactly the 24 a read-only server keeps. A write cannot be
declared without its other three hints, and it states all three, `false` ones
included, because the spec reads an absent `destructiveHint` or `openWorldHint`
as `true`. For the same reason every read declares `openWorldHint: false`
(`destructiveHint` and `idempotentHint` mean nothing on a read-only tool, so a
read leaves them out). Tests hold every tool to this, and pin each write's hints
by name.

What each hint means on a dagron tool:

- **`destructiveHint`**: the call can end, discard, delete or overwrite
  something that already exists: a run or task in flight, a task's captured
  output, a record, a stored value or definition. `false` means it only adds
  (a run, a record, a step forward) or flips a lifecycle state that the same
  call flips back.
- **`idempotentHint`**: a repeat with the same arguments leaves things as the
  first call did. It is refused (`404`/`409`), finds nothing left to do, or
  writes the same values again, timestamps aside. A call that adds a record
  every time, whether a run or a version, is not idempotent.
- **`openWorldHint`**: the call can set task code running with no further call.
  It starts or re-runs tasks, releases the tasks behind a gate, or arms or
  changes what a schedule or dataset trigger runs. A task runs whatever commands
  its spec names, so what such a call reaches is not bounded by dagron, and
  whether *that* code destroys anything is the workflow's business, which
  `destructiveHint` cannot know. Read `openWorldHint: true` as **runs code**: a
  client that relaxes approval for `destructiveHint: false` should not relax it
  for this.

The writes whose hints a reader might not expect:

| Tool | Why |
|---|---|
| `dagron_submit_run`, `dagron_run_workflow`, `dagron_resubmit_run` | Not destructive, because each adds a run and touches nothing that exists. Not idempotent, because each call is one more run. `idempotency_key` makes a *keyed* repeat of a submit return the same run, but the hint describes the tool, and an unkeyed repeat is a second run. |
| `dagron_rerun_run`, `dagron_retry_task`, `dagron_clear_task` | Destructive: each resets tasks in place and clears their captured output. For rerun and retry that is the failed attempt's log; for clear it is the results of the task and everything downstream of it. |
| `dagron_approve_task` | Not destructive (a step forward), but open-world: the tasks behind the gate run next. |
| `dagron_reject_task` | Open-world too: failing the gate releases any `one_failed`/`all_done` task behind it. |
| `dagron_triage_run`, `dagron_put_artifact` | Destructive: each overwrites what was there (a triage note, an artifact's bytes), and nothing keeps the old one. |
| `dagron_create_workflow` | Idempotent, because a repeat is a `409` duplicate. Open-world, because a spec with `on_datasets:` starts runs on its own from then on. |
| `dagron_update_workflow` | Destructive: it replaces the live definition that the next scheduled or triggered run executes. Earlier versions are kept, but recording one is best-effort. Not idempotent, because a repeat records one more version. |
| `dagron_set_workflow_state` | Not destructive, because every state can be set back. Open-world, because `active` lets the workflow's schedules fire again, an overdue slot on the next tick. |
| `dagron_redrive_dead_letter` | Destructive: it deletes the dead letter, error and failure history included, in the transaction that creates the run. A redrive that fails deletes nothing. Idempotent: a repeat after a success is a `404`, never a second run. |

**Hints, not a boundary.** The spec tells a client not to trust annotations
from a server it does not trust, and nothing here depends on one doing so: the
hints decide who is asked, while `DAGRON_MCP_READONLY` and the token's scope
decide what can run. A gateway that pins tool definitions, and hashes
`annotations` into the pin as MCPdef does, sees every dagron tool it had
already pinned as changed on the upgrade that adds them; re-approve those pins
once. A tool it has never seen is pinned on first sight, as usual.

**Protocol revision.** `initialize` still answers `2024-11-05`, although
annotations arrived in `2025-03-26`. A `2024-11-05` tool definition does not
forbid extra fields, and a client that acts on annotations reads them from the
tool whatever revision was negotiated. One that honours them only under a newer
revision falls back to asking before every call, which is how this server
behaved before it sent any, and the safe direction. Advertising a newer
revision is a separate change. The server answers one fixed revision whatever
the client asked for, so raising it would hand a client that speaks only older
ones a revision it cannot use. Doing it properly means negotiating, and each
revision claimed brings rules this server does not meet yet: `2025-03-26`
requires accepting a JSON-RPC batch, which the stdio loop leaves unanswered
today, and `2025-06-18` requires an HTTP transport to refuse an unsupported
`MCP-Protocol-Version` header with `400`.

## Coverage — the agent-API gap, closed

The 1.0 bar for this crate was never "wrap everything": *an agent should be able
to author, run, recover and inspect a workflow without a human dropping to
`curl`*, and should be told plainly where the answer is deliberately no. That
bar is met. The forty-two tools above cover **42 of the 84 `dagron-api`
method+path pairs** (67 distinct routes) on 0.9.1; the remaining 42 are
[declined or deferred](#p3--deliberately-not-tools) by decision, not by
oversight, and every one of the 84 carries its disposition in the
[full coverage matrix](#full-coverage-matrix).

What that closed, in the order it mattered:

- **Authoring.** `POST /api/workflows` had no tool, so a *named* workflow could
  not be registered — and a `type: workflow` task resolves its child by
  registered name, which made parent/child DAGs unreachable from MCP alone.
- **Waiting.** Every synchronous invocation was a poll loop over
  `dagron_get_run`, paid for in round trips and context window.
- **Artifacts.** A task could write a file and nothing on this surface could
  read it back.
- **Recovery.** An agent could start work and watch it fail, but could not
  rerun it, retry a task, redrive a dead letter, or record what it concluded.
- **Gates.** `type: approval` parks a run until someone resolves it, and no
  agent could. A DAG with a human-in-the-middle gate was one an MCP agent could
  start and never finish.
- **Arguments the routes already took.** `dagron_submit_run` could send neither
  `parameters` nor an `Idempotency-Key`, and `dagron_list_runs` took no
  arguments at all, so every listing was unfiltered. The idempotency gap was the
  sharp one: the client with the least ability to reason about its own retries
  was the one denied the safety net.

### Read-only mode

P0/P1 turned this server from mostly-read into read-write against a live
scheduler, so the safe-by-default posture it had before became a switch rather
than a property. `DAGRON_MCP_READONLY=1` hides all eighteen mutating tools from
`tools/list` **and** refuses them in the dispatcher — hidden as well as refused,
because a tool an agent can see is a tool it will plan around, and refused as
well as hidden because a composing server (a composing server may build its own
catalogue on top of this one) must not be able to smuggle a write past the
switch.

The read half — 24 tools — stays fully available: an agent that can only look is
still worth pointing at a failing cluster.

### Bounded by construction

Five invariants apply to every tool and are each covered by a test:

- **Path segments are percent-encoded, never pattern-matched.** uuid-shaped ids
  (`run_id`, `task_id`, `workflow_id`, a dead-letter `id`) keep the strict
  `[A-Za-z0-9_-]+` check they always had. The segments that are legitimately
  free-form — an artifact's `task` and `name` — are percent-encoded instead, so
  an artifact called `../../etc/passwd` reaches dagron-api as
  `..%2F..%2Fetc%2Fpasswd`: one opaque segment, not a reshaped request path.
  The strict check was **not** relaxed to accommodate them.
- **Every listing is bounded locally.** `limit`, `offset`, `days`, `timeout_secs`
  and `wait_ms` are range-checked before the request, so a malformed argument
  costs one fast tool error naming the argument instead of a round trip and an
  opaque HTTP 400.
- **A state answer is not a tool error.** `409` — paused workflow, gate not
  awaiting approval, task not completed; `404` — no such run; `410` — run
  compacted to the Parquet dataset. These are states the agent should reason
  about, and each carries its own retry logic, so the code stays distinct in the
  result. All three come back as a structured
  `{status, outcome, detail}` result with `isError: false`; tool-level errors
  stay reserved for transport, auth and validation failures.
- **Binary bodies do not fit JSON-RPC.** `dagron_get_artifact` returns text
  inline only when the payload is valid UTF-8 *and* under
  `DAGRON_MCP_MAX_ARTIFACT_BYTES` (256 KiB by default). Otherwise it returns
  size, content type and a locator. Base64-inflating a 40 MB artifact into a
  context window is a denial of service the agent performs on itself.
- **`parameters` are strings.** dagron's `parameters:` are string
  substitutions, so `{"retries": 3}` is rejected by name rather than coerced —
  silently stringifying it would leave the agent believing it passed an integer.

### P3 — deliberately not tools

Not every route should become one. These stay `curl`/console territory unless a
concrete demand appears, because their blast radius is administrative rather
than operational and the [security posture](#security-best-practices) below
assumes the agent's token is *narrow*. They are listed row-by-row in the matrix
so the decision is visible and revisitable:

- **Secrets and identity** — `/api/environments/{id}/secrets/{name}`,
  `/api/tokens*`, `/api/users`, `/api/login`, `/api/logout`, `/api/me`. A tool
  that mints a token or writes a secret hands prompt-injection a credential.
- **Instance settings** — `/api/settings/notifications*`,
  `/api/settings/dead-letters`, and `POST /api/artifacts/rotate` (admin-only
  re-key).
- **Infrastructure wiring** — `/api/git-repos*` and
  `POST /api/workflows/{id}/sync-to-git`.
- **Schedules and backfills** (`P3†`) — `/api/schedules*`, `/api/backfills*`.
  Genuinely arguable, and the tier most likely to be promoted: a paced backfill
  is a reasonable agent request, but it is also the cheapest way for a confused
  agent to materialize 100k runs. Gate these behind per-caller budgets rather
  than shipping them on tool-count grounds.
- **`GET /api/events/stream`** — account-wide SSE. `dagron_get_run_events`'s
  bounded-window trick does not obviously survive an unfiltered firehose.
- **`GET /api/badges/{name}`** — an SVG status badge that answers
  **unauthenticated**. Nothing for an agent to do with it, but worth knowing it
  is a public read surface on an otherwise JWT-gated API.

A test enforces the line: no tool name in the catalogue may mention a token,
secret, login, environment, git repo, schedule or backfill.
### 1.x — the console as an agent surface

Post-1.0, and listed here because it is the question this crate keeps
attracting: *can the console's prompt box talk to the MCP server?* It cannot,
and the reason is worth writing down once so it stops being re-litigated.

**The MCP server is the callee, not the agent.** It exposes dagron's tools *to*
an agent; it holds no model and runs no loop. Its interface takes structured
tool calls, so a natural-language prompt means nothing to it — and a console
that reached it would be calling `dagron-api` through an extra hop with weaker
auth than the session it already holds. The transport is the smaller obstacle — this
server is stdio, which a browser cannot open, and an HTTP transport for the
same handler is not in this build — and solving it would not
make the idea work. The intelligence lives in the client — Claude Desktop, an
IDE, a pod — and that stays true at 1.x.

**A browser terminal is a non-goal**, not an unbuilt feature. dagron is not a
remote development environment: there is no IDE, no port forwarding and no
shell, and a shell in the console of an orchestrator whose case is
auditability trades that case for a convenience.

**What does work is the loop we already have.** A conversation is a run:
[`examples/ai/agent_loop.yaml`](../examples/ai/agent_loop.yaml) parks on a
`type: workflow` trigger, `repeat: until` reads the child's `result_from`
verdict to decide whether to go again, each turn is its own child run with its
own tasks, logs and timings, and the state is artifacts rather than process
memory — so the conversation survives the engine being killed. Human-in-the-loop
is the approval gate the console already renders.

That makes the console side **additive and route-free**. Everything it needs
ships today: `POST /api/runs {yaml, parameters}` to start a conversation,
`GET /api/runs/{id}/stream` to follow its turns, `/logs` to read them, and
`POST /api/runs/{id}/tasks/{tid}/approve|reject` for the gate. The work is:

| Item | Where | Note |
|---|---|---|
| `agent-turn` takes a `prompt` parameter | `examples/ai/agent_turn.yaml` | the loop threads it to the child today; only the turn's own signature is missing |
| A real model step | `examples/ai/agent_turn.yaml` | `think` is a shell stand-in on purpose. The open pattern is case study 04 — the artifact is the idempotency check, so a retry never re-spends tokens; a hardened LLM task binary is not in this build |
| Dock submits a conversation and follows it | `frontend/` | Send → `POST /api/runs`, then the run's SSE stream; turns render as they land |
| Approval gates surfaced in the dock | `frontend/` | the gate is where an agent conversation becomes supervisable, and it is already an API call |
| Conversation history keyed on runs | `frontend/` | replaces the browser-local prompt log — inspectable and attributable, unlike localStorage |

Two things this does **not** need, and should not grow: a new agent runtime
inside dagron (the loop is the engine), and a session store (the runs *are* the
sessions).

The nearer, narrower cousin is **`POST /api/ai/generate`** — one-shot
natural-language → DAG, which is P0's "an agent cannot author a workflow" seen
from the console side rather than the tool side. Natural-language workflow
generation, validated against the engine's own parser, is not in this build;
and what no build has yet is a route that reaches one from a browser.
It is a different job — *write me a workflow*, not *work with me* — so it is
worth having and is not a substitute.

**Until the cycle above is complete, the console's agent dock ships dormant.**
It is built (`frontend/src/components/AgentPanel.tsx`) and gated off by
`AGENT_DOCK_ENABLED` in `frontend/src/lib/agent.ts`, with the list of what must
land first beside it. A prompt box that cannot be answered is worse than no
prompt box: it teaches an operator that the feature is broken rather than
absent.

That constant is **not** edited to turn the dock on today — it reads
`NEXT_PUBLIC_AGENT_DOCK === "on"`, so a build opts in with
`NEXT_PUBLIC_AGENT_DOCK=on` at build time, for developing the cycle above
rather than shipping it. It becomes unconditional when the turn takes a prompt,
the model step is real, and Send submits a run.

### Full coverage matrix

Every `dagron-api` route on 0.9.1 with its disposition, so nothing is untracked.
`P3†` is "deferred pending budgets", not "declined".

| Tier | Pairs |
|---|---|
| ✅ shipped | 42 |
| P3 declined | 33 |
| P3† deferred | 9 |
| **Total** | **84** |

**Auth, session & instance**

| Route | Status | Tool / disposition |
|---|---|---|
| `POST /api/login` | P3 | session/identity — never a tool |
| `POST /api/logout` | P3 | session/identity — never a tool |
| `GET /api/me` | P3 | session/identity — never a tool |
| `POST /api/users` | P3 | admin user management |
| `GET /api/users` | P3 | admin user management |
| `POST /api/tokens` | P3 | credential material |
| `GET /api/tokens` | P3 | credential material |
| `DELETE /api/tokens/{id}` | P3 | credential material |
| `GET /api/health` | ✅ | `dagron_get_health` |
| `GET /api/search` | ✅ | `dagron_search` |
| `GET /api/environments` | P3 | secrets — write-only by design |
| `POST /api/environments` | P3 | secrets — write-only by design |
| `PUT /api/environments/{id}` | P3 | secrets — write-only by design |
| `DELETE /api/environments/{id}` | P3 | secrets — write-only by design |
| `PUT /api/environments/{id}/secrets/{name}` | P3 | secrets — write-only by design |
| `DELETE /api/environments/{id}/secrets/{name}` | P3 | secrets — write-only by design |
| `GET /api/settings/notifications` | P3 | admin instance settings |
| `PUT /api/settings/notifications` | P3 | admin instance settings |
| `POST /api/settings/notifications/test` | P3 | admin instance settings |

**Runs**

| Route | Status | Tool / disposition |
|---|---|---|
| `GET /api/runs` | ✅ | `dagron_list_runs` — `status`/`name`/`trigger`/`limit`/`offset` |
| `POST /api/runs` | ✅ | `dagron_submit_run` — `parameters` + `Idempotency-Key` |
| `GET /api/runs/{id}` | ✅ | `dagron_get_run` |
| `GET /api/runs/{id}/wait` | ✅ | `dagron_wait_run` |
| `GET /api/runs/{id}/graph` | ✅ | `dagron_get_run_graph` |
| `GET /api/runs/{id}/spec` | ✅ | `dagron_get_run_spec` |
| `GET /api/runs/{id}/logs` | ✅ | `dagron_get_run_logs` |
| `GET /api/runs/{id}/tasks/{tid}/logs` | ✅ | `dagron_get_task_logs` |
| `GET /api/runs/{id}/stream` | ✅ | `dagron_get_run_events` |
| `GET /api/events/stream` | P3 | account-wide firehose — no bounded-window story |
| `POST /api/runs/{id}/cancel` | ✅ | `dagron_cancel_run` |
| `POST /api/runs/{id}/rerun` | ✅ | `dagron_rerun_run` |
| `POST /api/runs/{id}/resubmit` | ✅ | `dagron_resubmit_run` |
| `POST /api/runs/{id}/tasks/{tid}/retry` | ✅ | `dagron_retry_task` |
| `POST /api/runs/{id}/tasks/{tid}/clear` | ✅ | `dagron_clear_task` |
| `POST /api/runs/{id}/tasks/{tid}/approve` | ✅ | `dagron_approve_task` |
| `POST /api/runs/{id}/tasks/{tid}/reject` | ✅ | `dagron_reject_task` |
| `POST /api/runs/{id}/triage` | ✅ | `dagron_triage_run` — set state/note |
| `DELETE /api/runs/{id}/triage` | ✅ | `dagron_clear_triage` |

**Archived runs**

| Route | Status | Tool / disposition |
|---|---|---|
| `GET /api/archive/runs` | ✅ | `dagron_list_archived_runs` |
| `GET /api/archive/runs/{id}` | ✅ | `dagron_get_archived_run` |
| `POST /api/runs/{id}/archive` | P3 | **no tool.** Admin-only and destructive — it purges the run from the hot store once the document is durably in the sink. Retention is an instance-wide policy; an agent that can pull individual runs out of the live store ahead of it is an agent that can make history disappear one call at a time. Console and `curl` territory |

**Observability & dead letters**

| Route | Status | Tool / disposition |
|---|---|---|
| `GET /api/metrics` | ✅ | `dagron_get_metrics` |
| `GET /api/metrics/timeseries` | ✅ | `dagron_get_metrics_timeseries` |
| `GET /api/approvals` | ✅ | `dagron_list_approvals` |
| `GET /api/dead-letters` | ✅ | `dagron_list_dead_letters` |
| `POST /api/dead-letters/{id}/redrive` | ✅ | `dagron_redrive_dead_letter` |
| `DELETE /api/dead-letters/{id}` | ✅ | `dagron_delete_dead_letter` |
| `GET /api/settings/dead-letters` | P3 | admin instance settings |
| `PUT /api/settings/dead-letters` | P3 | admin instance settings |
| `GET /api/badges/{name}` | P3 | public SVG badge — **unauthenticated** |
| `GET /api/fleet` | P3 | fleet plane — a signpost in this build |
| `GET /api/audit` | P3 | the audit trail itself (enterprise builds) — admin surface |

**Datasets (lineage & registry)**

| Route | Status | Tool / disposition |
|---|---|---|
| `GET /api/datasets` | ✅ | `dagron_list_datasets` |
| `GET /api/datasets/events` | ✅ | `dagron_get_dataset_events` |

**Workflows, schedules, GitOps**

| Route | Status | Tool / disposition |
|---|---|---|
| `GET /api/workflows` | ✅ | `dagron_list_workflows` |
| `POST /api/workflows` | ✅ | `dagron_create_workflow` |
| `GET /api/workflows/{id}` | ✅ | `dagron_get_workflow` |
| `PUT /api/workflows/{id}` | ✅ | `dagron_update_workflow` |
| `DELETE /api/workflows/{id}` | ✅ | `dagron_delete_workflow` |
| `POST /api/workflows/{id}/run` | ✅ | `dagron_run_workflow` |
| `POST /api/workflows/{id}/state` | ✅ | `dagron_set_workflow_state` |
| `GET /api/workflows/{id}/versions` | ✅ | `dagron_list_workflow_versions` |
| `GET /api/workflows/{id}/runs` | ✅ | `dagron_list_workflow_runs` |
| `POST /api/workflows/bundle` | P3 | signed bundle apply — supply-chain surface, needs signing keys |
| `POST /api/workflows/{id}/sync-to-git` | P3 | infrastructure wiring |
| `GET /api/git-repos` | P3 | infrastructure wiring / Git credentials |
| `POST /api/git-repos` | P3 | infrastructure wiring / Git credentials |
| `DELETE /api/git-repos/{id}` | P3 | infrastructure wiring / Git credentials |
| `POST /api/git-repos/{id}/sync` | P3 | infrastructure wiring / Git credentials |
| `PUT /api/git-repos/{id}/auth` | P3 | infrastructure wiring / Git credentials |
| `DELETE /api/git-repos/{id}/auth` | P3 | infrastructure wiring / Git credentials |
| `GET /api/schedules` | P3† | gate on per-caller budgets first |
| `POST /api/schedules` | P3† | gate on per-caller budgets first |
| `PUT /api/schedules/{id}` | P3† | gate on per-caller budgets first |
| `DELETE /api/schedules/{id}` | P3† | gate on per-caller budgets first |
| `POST /api/schedules/{id}/backfill` | P3† | gate on per-caller budgets first |
| `POST /api/backfills` | P3† | gate on per-caller budgets first |
| `GET /api/backfills` | P3† | gate on per-caller budgets first |
| `GET /api/backfills/{id}` | P3† | gate on per-caller budgets first |
| `POST /api/backfills/{id}/cancel` | P3† | gate on per-caller budgets first |

**Artifacts**

| Route | Status | Tool / disposition |
|---|---|---|
| `PUT /api/runs/{run_id}/artifacts/{task}/{name}` | ✅ | `dagron_put_artifact` — seed an input file |
| `GET /api/runs/{run_id}/artifacts/{task}/{name}` | ✅ | `dagron_get_artifact` |
| `GET /api/runs/{run_id}/artifacts/{task}/{name}/exists` | ✅ | `dagron_artifact_exists` |
| `POST /api/artifacts/rotate` | P3 | admin KEK re-key |
| `POST /api/artifacts/sync` | P3 | admin tiered-store drain |

### Still open

- **List pagination on `GET /api/workflows`.** The route returns the whole
  registry, so `dagron_list_workflows` has nothing to page with. Paging belongs
  on the route, not faked in the client — a client-side truncation would report
  a total it cannot see.
- **`P3†` schedules and backfills**, pending per-caller budgets.

### Provenance

Derived 2026-09-04 against `dagron-api` 0.9.1 by diffing this doc's tool table
against [`API.md`](API.md) and against the route table extracted from the
`dagron-api` binary. Read routes were probed on a live local stack and answered
`200`; documented write-route methods and status codes are quoted from
[`API.md`](API.md) rather than exercised. Each tool's request shape was then
checked against the handler it calls — `SubmitBody`, `UpsertBody`,
`RunWorkflowBody`, `StateRequest`, `TriageRequest`, `RerunBody`, `ListParams`,
`WaitParams` — so the wire shape matches the extractor rather than the prose.

Six of the 84 pairs were in the binary and answered on a live stack but appeared
in no doc. Their methods were confirmed from `Allow` response headers rather than
guessed, and all six are now documented in [`API.md`](API.md) — including the
unauthenticated badge route in the auth statement, which previously said every
route but `/healthz`, `/readyz`, `POST /api/login` and `POST /api/logout`
requires a JWT:

| Route | Evidence |
|---|---|
| `POST`/`DELETE /api/runs/{id}/triage` | `GET` → `405` with `allow: POST,DELETE`; the run row already carries `triage_state`/`triage_note`/`triaged_at`/`triaged_by` |
| `GET /api/runs/{id}/spec` | `200 {"yaml": …}`; `allow: GET,HEAD` |
| `GET`/`PUT /api/settings/dead-letters` | `200 null`; `allow: GET,HEAD,PUT` |
| `GET /api/badges/{name}` | `200` SVG **with no token presented**; `allow: GET,HEAD` |

Three of those six now have tools (`dagron_triage_run`, `dagron_clear_triage`,
`dagron_get_run_spec`), which is why documenting them could not wait: a reader of
the reference should not have to read the MCP catalogue to learn a route exists.
The other three stay P3 — instance settings and a public badge are not agent
surface — but they are reference material either way.

## The other direction: a workflow that calls MCP tools

`dagron-mcp` lets an agent drive dagron. **`dagron-step-mcp` lets a dagron DAG
drive an agent's tools** — a task whose job is to call one tool on one MCP
server.

The reason to want it is what a task already is. Once a tool call is a task it
inherits retries with backoff, a timeout, captured output, artifacts to hand to
the next step, an approval gate in front of it if you want one, and a row in the
run's history. A tool call inside an agent's own loop has none of that: when it
fails halfway there is nothing to resume, and nothing that records it happened.

```yaml
- name: fetch
  command: ["dagron-step-mcp"]
  env:
    - { name: DAGRON_MCP_STEP_SERVER, value: "npx" }
    - name: DAGRON_MCP_STEP_SERVER_ARGS
      value: '["-y", "@modelcontextprotocol/server-filesystem", "/data"]'
    - { name: DAGRON_MCP_STEP_TOOL, value: "read_file" }
    - { name: DAGRON_MCP_STEP_ARGS, value: '{"path": "{{ doc }}"}' }
    - { name: DAGRON_MCP_STEP_OUTPUT, value: "/artifacts/doc.txt" }
  max_attempts: 3
```

Full example: [`examples/ai/mcp_tool_step.yaml`](../examples/ai/mcp_tool_step.yaml).

| Env | Purpose |
|---|---|
| `DAGRON_MCP_STEP_SERVER` | MCP server program to spawn (**required**, unless a gateway is set) |
| `DAGRON_MCP_STEP_SERVER_ARGS` | JSON **array** of its arguments |
| `DAGRON_MCP_STEP_GATEWAY` | call a governed **gateway** instead of spawning a server — see below |
| `DAGRON_MCP_STEP_GATEWAY_TOKEN` | bearer token, if the gateway requires auth |
| `DAGRON_MCP_STEP_TOOL` | tool name to call (**required**) |
| `DAGRON_MCP_STEP_ARGS` | JSON **object** of tool arguments (default `{}`) |
| `DAGRON_MCP_STEP_ARGS_FILE` | read them from a file instead (`-` = stdin) |
| `DAGRON_MCP_STEP_OUTPUT` | write the result here instead of stdout |
| `DAGRON_MCP_STEP_TIMEOUT_SECS` | whole-exchange deadline (default 300) |

### Calling through a gateway

Set `DAGRON_MCP_STEP_GATEWAY` and the step stops spawning the server itself. It
POSTs the same JSON-RPC to a gateway over Streamable HTTP, and the gateway routes
to the real server:

```yaml
env:
  - { name: DAGRON_MCP_STEP_GATEWAY, value: "127.0.0.1:7878" }
  - { name: DAGRON_MCP_STEP_TOOL, value: "read_file" }
  - { name: DAGRON_MCP_STEP_ARGS, value: '{"path": "{{ doc }}"}' }
```

Nothing else about the task changes. What the task *gains* is whatever the
gateway enforces — an allow-list, per-argument policy, a tool-definition pin that
catches a server changing a tool after approval, a rate limit, and an audit
record of the call. dagron keeps what it already owned: the retry, the timeout,
the artifact, the approval gate in front of it.

Details worth knowing:

- **A bare `host:port` is fine** and means `http://host:port/mcp`. A full URL
  keeps its path.
- **Plain HTTP only.** `https://` is refused rather than silently downgraded.
  The step is meant to reach a loopback or cluster-local gateway; if TLS is in
  the way, terminate it in front of the step.
- **`DAGRON_MCP_STEP_SERVER` is ignored** when a gateway is set, and the step
  logs that once. A workflow that already names its server can gain governance by
  setting one variable on the task, without being edited.
- **A denial is a task failure with the reason.** A gateway that refuses the call
  answers with an MCP tool error, which this step treats like any other failing
  tool: non-zero exit, the reason in the run's logs, and the engine's retry
  policy applies.
- **Stateful gateways work.** An `Mcp-Session-Id` returned from `initialize` is
  carried on every later request of the step, and so is the protocol version the
  handshake settled on. A gateway that mints no session — MCPdef does not — sends
  no header and nothing is carried.
- **Streamed answers work.** A `text/event-stream` reply is read for the response
  to *this* request; progress notifications ahead of it are stepped over rather
  than mistaken for the result. Chunked bodies are decoded, which is how such a
  reply normally arrives.
- **A bearer token on a non-loopback gateway is logged as a warning.** Plain HTTP
  means anything on the path can read it. It is a warning and not a refusal,
  because a pod-local gateway is a legitimate deployment.
- **Tested end to end** against stand-in gateways — `crates/dagron-step-mcp/tests/gateway.rs`
  covers the handshake, the bearer token, the denial path, a stateful gateway
  that 404s without its session id, a chunked SSE answer behind two progress
  notifications, and an IPv6 endpoint.

`module_58` (MCPdef) is the in-house gateway this was built against; any
Streamable-HTTP MCP gateway that speaks the same wire works — with the one
exception above, that it must be reachable without TLS.

Details worth knowing before you write one:

- **Server arguments are a JSON array, not a command line.** A whitespace split
  breaks on any argument containing a space — which is most paths worth passing
  — and it does so silently. Handing the string to a shell instead would make a
  workflow parameter injectable into a command line.
- **All-text results come back as plain text**, so `{{ tasks.fetch.output }}`
  works in the next task without a JSON step in between. Mixed content (an
  image, a resource) comes back as the JSON `content` array, because flattening
  it would drop the parts that are not text.
- **A tool that reports `isError` fails the task**, so the engine's retry policy
  and the run's failure summary both see it. The result is deliberately *not*
  written to `DAGRON_MCP_STEP_OUTPUT` in that case — an error file would satisfy
  the idempotency check below and the retry would skip the call.
- **Set `DAGRON_MCP_STEP_OUTPUT` for anything that matters.** Besides handing the
  result to the next task, a non-empty output file makes a retry skip the call:
  if a prior attempt succeeded and then died before exiting 0, the tool is not
  invoked twice. That matters more here than for a text completion, because an
  MCP tool need not be read-only.
- **The binary must be on the task's PATH**, and so must the MCP server it
  spawns — both run inside whatever image the task runs in.
- The server's **stderr is inherited**, so its diagnostics land in the task's
  captured output instead of being discarded with the child.

## Configuration

| Env | Purpose |
|---|---|
| `DAGRON_API_URL` | dagron-api base URL (default `http://localhost:8080`) |
| `DAGRON_MCP_TOKEN` | optional session JWT, sent as `Authorization: Bearer …` |
| `DAGRON_MCP_READONLY` | `1`/`true`/`yes` hides **and** refuses every mutating tool — see [read-only mode](#read-only-mode) |
| `DAGRON_MCP_MAX_ARTIFACT_BYTES` | largest artifact returned inline to the agent (default `262144`) |
| `DAGRON_MCP_ALLOW_PLAINTEXT_TOKEN` | send `DAGRON_MCP_TOKEN` over plaintext `http://` to a non-loopback host (refused otherwise) |

## Run

Build from source, or use the published OSS image (`mancube/dagron-mcp`,
multi-arch, distroless — the same open server, containerized):

```sh
cargo run -p dagron-mcp        # from source; speaks JSON-RPC over stdio

# or the published image (stdio over `docker run -i`)
docker run -i --rm \
  -e DAGRON_API_URL=http://host.docker.internal:8080 \
  -e DAGRON_MCP_TOKEN=<session-jwt> \
  mancube/dagron-mcp
```

Register it with an MCP client — either the binary or the image (example
`mcpServers` entries):

```json
{
  "mcpServers": {
    "dagron": {
      "command": "dagron-mcp",
      "env": {
        "DAGRON_API_URL": "http://localhost:8080",
        "DAGRON_MCP_TOKEN": "<session-jwt>"
      }
    },
    "dagron-docker": {
      "command": "docker",
      "args": ["run", "-i", "--rm",
               "-e", "DAGRON_API_URL", "-e", "DAGRON_MCP_TOKEN",
               "mancube/dagron-mcp"],
      "env": {
        "DAGRON_API_URL": "http://host.docker.internal:8080",
        "DAGRON_MCP_TOKEN": "<session-jwt>"
      }
    }
  }
}
```

The transport is newline-delimited JSON-RPC 2.0 on stdio (protocol `2024-11-05`,
with tool annotations sent anyway; see the
[protocol revision note](#tool-annotations)); logs go to **stderr** so stdout
carries only protocol messages.

### Lifecycle — one process per client session, not per tool call

The most common wrong mental model is that a tool call spawns a server. It does
not. `dagron-mcp` is a **stdio subprocess of the MCP client**, and its lifetime
is the client's session:

```text
client starts
  └─ spawns the command from mcpServers, with the `env` block in ITS environment
       └─ writes  {"method":"initialize"}        ──▶ stdin
          reads   {"result":{"serverInfo":…}}    ◀── stdout
          writes  {"method":"tools/list"}        ──▶ stdin
          …every later tool call is another line on the SAME pipe…
client exits
  └─ closes stdin → the server reads EOF → exits
```

So a session pays the process-start cost once, holds one long-lived pipe, and
tears it down on exit. Nothing is spawned per call, and there is no port, no
socket and no service to health-check: if the pipe is open, the server is up.

**Containerised, the container is that process.** `docker run -i` (or `podman
run -i`) keeps stdin attached for the client to write to, and `--rm` reaps the
container when the binary exits — so the container appears in `docker ps` when
the session starts and is gone when it ends.

Two details worth copying from a working setup:

- **Pass the token by reference, not by value.** In the `mcpServers` entry above
  the args carry `-e DAGRON_MCP_TOKEN` with *no* `=value`, and the value lives in
  the `env` block. The runtime forwards it from its own environment into the
  container, so the JWT never appears in `argv` — where any local user could read
  it out of a process listing. (It is still readable via `docker inspect` and in
  the client's config file on disk; §Security below is about the rest.)
- **Reach the API by sharing a network namespace, when you can.** Against a
  compose or pod deployment on the same host, joining it beats guessing at a
  host gateway:

  ```json
  {
    "mcpServers": {
      "dagron": {
        "command": "podman",
        "args": ["run", "-i", "--rm", "--pod", "dagron-dev",
                 "-e", "DAGRON_API_URL", "-e", "DAGRON_MCP_TOKEN",
                 "mancube/dagron-mcp"],
        "env": {
          "DAGRON_API_URL": "http://localhost:8080",
          "DAGRON_MCP_TOKEN": "<session-jwt>"
        }
      }
    }
  }
  ```

  Inside that namespace `localhost:8080` *is* `dagron-api` — no
  `host.docker.internal`, no published port, no DNS. The docker equivalent is
  `--network container:<dagron-api>` or a shared user-defined network.

**Debugging it.** The image is distroless: there is no shell to `exec` into, and
`docker exec … sh` fails with `executable file 'sh' not found`. Talk to it
instead — the protocol is line-oriented, so a smoke test is three lines on
stdin:

```sh
printf '%s\n%s\n%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke","version":"0"}}}' \
  '{"jsonrpc":"2.0","method":"notifications/initialized"}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' \
  | docker run -i --rm -e DAGRON_API_URL -e DAGRON_MCP_TOKEN mancube/dagron-mcp
```

**That is a transport probe for this server, not a client example.** It buffers
the whole sequence, which is what keeps it to one command against an image with
no shell — and it works only because `handle` dispatches per message and holds
no session state, so `tools/list` never waits on the handshake. The
`notifications/initialized` line is there because a conformant client owes it,
but it goes out *before* the `initialize` response arrives, which the
`2024-11-05` lifecycle does not allow. Do not copy this shape into a client.

**The lifecycle-correct version** reads each response before writing the next,
which needs a coprocess rather than a pipe. Use this one when talking to a
server that enforces the handshake, or as the reference for writing a client.
**Bash only** — `coproc` and the `{fd}` redirections below are not POSIX, and
`/bin/sh` fails to parse this before the probe ever starts:

```bash
coproc MCP { docker run -i --rm -e DAGRON_API_URL -e DAGRON_MCP_TOKEN mancube/dagron-mcp; }

printf '%s\n' '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"smoke","version":"0"}}}' >&"${MCP[1]}"
IFS= read -r init <&"${MCP[0]}"; printf 'initialize -> %s\n' "$init"

# Only now is the session established, so the notification and the first real
# request are both in-order.
printf '%s\n' '{"jsonrpc":"2.0","method":"notifications/initialized"}' >&"${MCP[1]}"
printf '%s\n' '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' >&"${MCP[1]}"
IFS= read -r tools <&"${MCP[0]}"; printf 'tools/list -> %s\n' "$tools"

w=${MCP[1]}; exec {w}>&-   # EOF on stdin: the server exits and `--rm` cleans up
wait "$MCP_PID"
```

A response to id 1 proves the process and the protocol; a response to id 2 lists
the tools. Neither touches `dagron-api` — the first call that does is a
`tools/call`, which is also where an expired `DAGRON_MCP_TOKEN` surfaces, as a
401 in the tool result rather than a startup failure. Server-side diagnostics
are on stderr, which the client captures and `docker run` passes through.

## Security best practices

`dagron-mcp` runs as a per-agent stdio subprocess that holds a bearer token for
`dagron-api`. Treat the MCP server like any other client of your management API
— the agent's prompt is *untrusted input* that influences which tools get
called. The defaults are safe; the practices below cover the parts that depend
on how you deploy it.

### Decide first whether the agent may write at all

Eighteen of the forty-two tools change cluster state: they create and delete
workflows, submit and cancel runs, resolve approval gates, and discard
dead-letter payloads. `DAGRON_MCP_READONLY=1` removes all of them — from
`tools/list` and from the dispatcher both — leaving the 24 read tools, which is
still enough for an agent to diagnose a failing cluster end to end.

Reach for it whenever the agent's prompt carries text you don't control, and
treat enabling writes as a decision with the same weight as the token's scope.
The two compose: a narrow token bounds *what* the agent can reach, read-only
mode bounds *what it can do* with it.

Annotations do not set a client's approval policy; the client does. A client
whose policy is to ask before every tool not marked `readOnlyHint: true`, and
only before those, asks a person before each of the eighteen and runs the 24
reads unasked. Another client may ask before every call, or skip approval on
other hints. Either way the [tool annotations](#tool-annotations) are only what
the policy reads. The switch is the part that holds whatever the client does.

### Use a dedicated, least-privilege session token

- Don't reuse a human's session JWT in `DAGRON_MCP_TOKEN`. Mint a separate
  account for the agent, scope it to only the workflows/projects it needs to
  reach, and rotate it on a schedule.
- Treat the token like any secret: pass it via the MCP client's `env` block (or
  a secret manager), never embed it in `command`/`args`, prompts, or repo files.
- If `DAGRON_MCP_TOKEN` is unset, the server makes unauthenticated calls — only
  acceptable when `DAGRON_API_URL` points at a private, network-isolated edge.

### Pin and isolate the network edge

- Set `DAGRON_API_URL` to the **public UI gateway** (`dagron-api`), never the
  engine's internal `api.rs` ops API. The ops API is unauthenticated and meant
  to be cluster-private — exposing it via MCP would bypass auth entirely.
- Use HTTPS in production. The crate builds reqwest with the `rustls-tls`
  feature, so TLS works out of the box; just point `DAGRON_API_URL` at `https://…`.
- If your agent runs on an end-user device, keep `dagron-api` behind a VPN or
  mTLS edge — the bearer token alone is one breach away from full API access.

### Sandbox what a submitted spec can launch

- `dagron_create_workflow` only *stores* a definition; `dagron_submit_run` and
  `dagron_run_workflow` are the two that ask for execution, and each ends with
  the engine launching a
  task as a subprocess (or Docker/Kube container) per its `Executor`. Run
  agent-driven workloads under an executor that **isolates** them: a dedicated
  Kubernetes namespace with a restricted PodSecurityPolicy/Pod Security
  Standard, a Docker daemon with locked-down capabilities, or a separate
  dagron cluster entirely. Never point an agent at a scheduler whose tasks
  execute on a shared host filesystem.
- Set per-task `timeout_secs` defaults and submission quotas on the
  agent's account so a runaway prompt can't fan out unbounded work.
- `dagron_delete_workflow` cascades a workflow's schedules away. If an agent
  needs to stop a workflow rather than remove it, `dagron_set_workflow_state`
  with `paused` or `retired` is the reversible answer, and the tool description
  says so — but a token that can delete can delete.

### Defend against prompt-injection escalation

- Any text the agent reads — workflow YAML, task logs, dead-letter payloads —
  can carry instructions aimed at the LLM. Tools like `dagron_get_task_logs`,
  `dagron_get_run_logs` and `dagron_list_dead_letters` return that text verbatim. If the agent will
  act on it, treat results as data, not directives.
- The MCP server validates uuid-shaped path-segment ids (`run_id`, `task_id`,
  `workflow_id`, a dead-letter `id`) against `[A-Za-z0-9_-]+` before any HTTP
  call so a crafted id can't reshape the request path or smuggle query/header
  content. Don't disable that check, and don't widen it: the free-form segments
  the artifact tools take (`task`, `name`) are **percent-encoded** instead, which
  is what keeps an artifact called `../../etc/passwd` one opaque segment rather
  than a rewritten path.
- Consider `DAGRON_MCP_READONLY=1` when the agent reads text you don't control.
  Injected instructions can only reach the tools the agent actually has.
- The Bearer token never appears in MCP responses — only `dagron-api`'s body
  is forwarded. Keep it that way if you fork the client.

### Bound and observe the agent's reach

- `dagron_get_run_events`'s window is hard-capped (10 s / 256 KiB) so an agent
  can't pin a connection or exhaust memory by polling a chatty stream.
- Every listing and every timeout is range-checked locally before the request —
  `limit`, `offset`, `days`, `timeout_secs`, `wait_ms` — so a malformed argument
  fails fast as a tool error naming the argument rather than reaching the server.
- `dagron_get_artifact` never inlines a non-UTF-8 body, and never inlines more
  than `DAGRON_MCP_MAX_ARTIFACT_BYTES` (256 KiB by default); past that it returns
  size, content type and a locator. A task that writes a 2 GB file cannot be used
  to exhaust the agent's context window.
- Enable `dagron-api`'s access log on the edge; every MCP call appears as an
  HTTP request from the agent's JWT subject, giving you a single audit trail
  for human and agent traffic.
- Log to stderr only (the default). `tracing-subscriber` is wired to stderr
  precisely so stdout stays a clean JSON-RPC channel — a sensitive log line
  written to stdout would corrupt protocol framing *and* leak through MCP.

> This makes dagron's durable engine drivable by agents — the foundation for the
> agentic durable-execution step types on the roadmap (LLM / tool / approval steps).
