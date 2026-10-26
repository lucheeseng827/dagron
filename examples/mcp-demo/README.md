# The five-minute durable tool — dagron over MCP

**What it shows:** a script you already have, registered once, becomes a tool
your agent calls by name. The call survives the engine restarting halfway
through, waits for a person to approve before it does anything visible, and when
it fails, the agent reads why and reruns only the step that broke.

It runs on the published images in `compose.quickstart.yaml`; nothing is built.
`verify.sh` runs every beat below without a model and prints PASS or FAIL. Run it
before you show this to anyone.

| File | What it is |
|---|---|
| [`incident_summary.yaml`](incident_summary.yaml) | The tool: validate → gather (slow) → summarise → **approval** → publish |
| [`demo.sh`](demo.sh) | Stack up, register, client config, and the presenter's moves (restart, approve, outage) |
| [`verify.sh`](verify.sh) | Plays the agent over MCP against the real `mancube/dagron-mcp` image and checks each beat |

## Before you start

You need `docker` (or `DOCKER=podman`), `curl` and `jq`, plus an MCP client:
Claude Code or Claude Desktop.

```bash
cd examples/mcp-demo
./demo.sh up                 # quickstart stack + registers `summarise-incident`
./verify.sh                  # PASS, or a reason it would not demo
./demo.sh client claude      # prints a `claude mcp add dagron -- docker run …` line; run it
./demo.sh client desktop     # or: an mcpServers entry for claude_desktop_config.json
```

Open the console at <http://localhost:8080> (`admin@local` / `dagron-admin`)
beside the agent. The audience watches both.

## Run of show

Each beat has what you do, what you say to the agent, and what the audience
should notice. The prompts are suggestions: say them in your own words. The
agent works out the calls from the tool descriptions.

### 1. The agent finds the tool

> *"What can you run on dagron for incidents?"*

The agent calls `dagron_list_workflows` with the tag `agent-callable` and finds
`summarise-incident`, its description and its two arguments.
**Notice:** nobody wrote any glue. The workflow's `description:` and
`parameters:` are what the agent read.

### 2. The agent calls it

> *"Summarise INC-4471 for the exec team and post it."*

`dagron_run_workflow` with `incident_id: INC-4471, audience: exec`, then
`dagron_wait_run`. The console shows the run start.
**Notice:** the agent passes arguments, not YAML. Try `incident_id: "../etc"`
another time: `validate` refuses it before anything runs.

### 3. Restart the engine mid-call

While `gather` is running (about 20 seconds):

```bash
./demo.sh restart-engine
```

**Say:** "That was the process running the work. The agent is still waiting."
The killed attempt is reclaimed and `gather` runs again; the console shows
attempt 2. The agent's wait never broke, because it waits on the API, not on the
engine. This is the beat to do live rather than describe.

### 4. A person approves

The run parks at `review`. Ask the agent:

> *"Is it done?"*

It calls `dagron_list_approvals` and says the run is waiting for a person.
**Notice:** the agent cannot approve its own gate. `dagron_approve_task` is not
offered unless the server is started with `DAGRON_MCP_ALLOW_APPROVE=1`. Approve
in the console (Approvals), or:

```bash
./demo.sh approve
```

The agent's wait returns, and the summary comes back as the tool's result.

### 5. A failure the agent can act on

```bash
./demo.sh outage on          # the (simulated) status page goes down
```

> *"Now do INC-4472 for engineers."*

Approve again when it parks. `publish` then fails. Ask:

> *"What went wrong? Fix it if you can."*

The agent reads the failure summary from `dagron_get_run`: the failing step and
the tail of its output, `status page unreachable`. Bring the page back, and let
it rerun:

```bash
./demo.sh outage off
```

It calls `dagron_rerun_run`. **Notice:** only `publish` runs again. The timeline
is not pulled twice and nobody is asked to approve again.

## What this demo does not show

Say these out loud. They are what is not built yet, not things to work around.

- **The tool is called through `dagron_run_workflow`**, not by its own name.
  The agent finds it by tag and passes its ID. A workflow is not yet a tool of
  its own in `tools/list`.
- **The approval happens in the console or a script, not in the chat.**
- **The client connects over stdio**, so this runs on the presenter's machine.
- **The token is the quickstart admin's session token.** It expires: if calls
  start failing with 401, run `./demo.sh client …` again.
- **Every step is a stand-in.** `gather`, `summarise` and `publish` echo canned
  text. Swap in your own scripts; nothing else changes.

## Reset and clean up

```bash
./demo.sh outage off         # in case a rehearsal left it on
./demo.sh down               # removes the stack and its volumes
```

The stack is a compose project named `dagron-mcp-demo` (`DEMO_PROJECT` to
change it) on port 8080, so it does not touch another quickstart's volumes.
Pin another release with `DAGRON_VERSION=…`.

## How `verify.sh` checks it

It starts one `mancube/dagron-mcp` process, as a client would, and makes the
agent's calls over stdio JSON-RPC: `initialize`, `tools/list`,
`dagron_list_workflows`, `dagron_run_workflow`, `dagron_wait_run` (with a
progress token), `dagron_list_approvals`, `dagron_get_run`, `dagron_rerun_run`.
The presenter's moves go through `demo.sh`. It fails if any of these is not
true:

- the server does not offer `dagron_approve_task`;
- the restart lands while `gather` is running, and `gather` succeeds on a
  second attempt;
- the run parks at `review` until approved, and its result carries the summary;
- with the status page down, the failure summary names `publish` and its reason;
- the rerun succeeds without running `gather` again or reopening `review`.

`KEEP=0 ./verify.sh` removes the stack afterwards.
