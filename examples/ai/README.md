# AI-workload case studies — five live pipelines, and one in reverse

Five end-to-end, laptop-runnable AI pipelines. Each stands in for a real
production shape — spot-preempted training, GPU pool routing, sharded batch
inference, LLM content generation with human sign-off, a train→eval quality
gate — using the primitives documented in
[`docs/AI_WORKLOADS.md`](../../docs/AI_WORKLOADS.md): the lease heartbeat
(long tasks), checkpoint-aware resume, `resources.gpu`, `runner_class` pools,
approval gates, and fan-out.

The "GPU work" is simulated with short shell loops so every study runs on any
machine in seconds — the orchestration behaviour (what survives a kill, what
resumes, what routes where) is the real thing.

| # | Case study | Primitive under test |
|---|---|---|
| [`01`](#01--training-that-survives-preemption) | Spot-preempted training | checkpoint report + `DAGRON_RESUME_FROM` resume |
| [`02`](#02--gpu-pool-routing) | Train/eval placement | `runner_class` pools + `resources.gpu` |
| [`03`](#03--sharded-batch-inference) | Batch inference | `with_param` fan-out + artifact gather |
| [`04`](#04--llm-content-pipeline-with-sign-off) | LLM generation | durable LLM step + `type: approval` + `result_from` |
| [`05`](#05--traineval-quality-gate) | Model promotion | `when:` on task output + `repeat:` convergence poll |
| [`06`](#06--retry-budgets-that-follow-the-fault) | Fault-class retry budgets | `retry_budgets:` + `task_runs.fault_class` |
| [`mcp`](#mcp--a-workflow-that-calls-an-agents-tools) | dagron driving MCP tools | `dagron-step-mcp` + retries + artifacts |
| [`loop`](#loop--a-conversation-whose-every-turn-is-a-run) | Durable agent loop | `repeat:` on a `type: workflow` trigger |

## One-time setup

```bash
cargo build --release
cd examples/ai
export DAGRON=../../target/release/dagron
export DAGRON_ARTIFACT_DIR=$PWD/artifacts     # checkpoint/artifact substrate
```

---

## 01 — Training that survives preemption

**Scenario.** A 10-epoch training job on spot capacity. Mid-run the instance
is preempted (the task simulates the kill after epoch 4 of the first
attempt). The retry does **not** start at epoch 0: dispatch hands it
`DAGRON_RESUME_FROM` — the last checkpoint the dying attempt committed — and
training continues at epoch 5.

```bash
$DAGRON 01_train_checkpoint_resume.yaml
cat artifacts/*/\.checkpoints/train/latest      # the surviving pointer
```

Watch the log: attempt 1 checkpoints each epoch then dies ("simulated spot
preemption"); attempt 2 opens with `resuming from …/epoch-4.ckpt` and finishes
epochs 5–10. The task uses the zero-API **file convention**
(`$DAGRON_CHECKPOINT_DIR/latest`); long-running daemons can `curl` the
checkpoint route instead — both are in
[`docs/AI_WORKLOADS.md`](../../docs/AI_WORKLOADS.md). A real spot notice slots
in as `trap 'checkpoint_and_exit' TERM`.

The heartbeat half: while an attempt runs, its lease is renewed every 10 s, so
a *multi-hour* epoch loop is never reclaimed mid-run — and when a worker
machine dies outright, renewal stops and the task re-dispatches (resuming!)
within the 30 s lease window.

## 02 — GPU pool routing

**Scenario.** `train` must land on spot GPU capacity, `evaluate` on cheap CPU
nodes. Tasks pin `runner_class`; each scheduler process claims only its
classes — segmentation with no extra infrastructure.

```bash
mkdir -p data && touch data/empty.ndjson   # join-only feed for scheduler #2

# T1 — the "spot GPU pool" scheduler (serves default too); submits the run:
RUNNER_CLASSES=spot-gpu,default API_ADDR=127.0.0.1:8787 \
  $DAGRON 02_gpu_pool_routing.yaml pools.db

# T2 — the "CPU pool" scheduler joining the same datastore (submits nothing —
# it follows an empty stream and just claims its classes):
SOURCE=stream STREAM_PATH=data/empty.ndjson RUNNER_CLASSES=cpu \
  $DAGRON 02_gpu_pool_routing.yaml pools.db
```

The run's `train` task waits until a `spot-gpu` scheduler is live, then its
`evaluate` task waits for the `cpu` one — watch each terminal claim only its
stage. `resources.gpu: {count: 4}` rides along: on the Kubernetes executor it
becomes `limits["nvidia.com/gpu"]=4` on the task pod. (Cost/preemption-aware
placement of these pools across clouds is a fleet layer this build does not carry.)

## 03 — Sharded batch inference

**Scenario.** Score a dataset in parallel shards, then gather. `with_param`
fans one template task out over a JSON shard list; every shard writes scores
into the run's shared `DAGRON_ARTIFACTS` dir; the gather task (which runs only
after every shard succeeds) merges them into one report. Shards are
retry-safe: a re-attempted shard overwrites its own output file, nothing else.

```bash
$DAGRON 03_batch_inference.yaml
```

## 04 — LLM content pipeline with sign-off

**Scenario.** Generate release-note copy with an LLM, hold it at a **human
approval gate**, publish only on approval — the shape every "LLM writes,
human approves" pipeline shares. The generate step is a plain `command:` that
uses your `ANTHROPIC_API_KEY` if present and falls back to a deterministic
offline stub otherwise, so the study runs anywhere. Durability is the point:
the draft persists as task output/artifacts, the gate survives restarts, and
a retry never re-spends tokens (the output file is the idempotency check).

```bash
API_ADDR=127.0.0.1:8787 $DAGRON 04_llm_content_pipeline.yaml &
sleep 3
RUN=$(curl -s 'localhost:8787/runs?limit=1' | sed -n 's/.*"id":"\([^"]*\)".*/\1/p' | head -1)
GATE=$(curl -s localhost:8787/runs/$RUN | grep -o '"id":"[^"]*"\|"name":"[^"]*"' \
      | awk -F'"' '/^"name/{if($4=="review"){print prev; exit}} /^"id/{prev=$4}')
curl -s -X POST localhost:8787/runs/$RUN/tasks/$GATE/approve   # ship it
curl -s "localhost:8787/runs/$RUN/wait?timeout_secs=30"        # → the published copy
```

(A hardened LLM task binary — idempotent retries, output capture, credential
egress guard — plus natural-language *workflow generation* ship with
[not in this build](../../README.md#what-this-build-does-not-do).)

## 05 — Train→eval quality gate

**Scenario.** Never promote a model on vibes: `train` → `evaluate` emits a
score → the `deploy` task exists **only if** the score clears the bar
(`when:` on the eval task's output, decided by the engine at runtime), while
`hold` fires on the reject path. A `repeat:` task models the
poll-until-converged loop (bounded, so a stuck training run fails loudly
instead of wedging).

```bash
$DAGRON 05_model_eval_gate.yaml           # eval scores "ship" → deploy runs
THRESHOLD=99 $DAGRON 05_model_eval_gate.yaml   # unreachable bar → hold runs
```

## 06 — Retry budgets that follow the fault

**Scenario.** `max_attempts` spends the same budget on every failure, so an ECC
error and a NaN loss draw from the same three attempts: infrastructure faults
give up too early, and application faults burn GPU-hours reproducing a
determinism nobody doubted. At 128 GPUs, three blind retries of a diverged run
is roughly 3,000 wasted GPU-hours to learn nothing.

`retry_budgets:` sizes the budget by **what actually broke**. dagron classifies
the failure from the task's output, records the verdict on the row
(`fault_class`, `fault_detail`, `fault_confidence`), and resolves the budget
most-specific-first: the task's own entry for that class → the class's
disposition default (infra 5, platform 3, application 1) → `max_attempts`. An
unclassified failure lands on `max_attempts`, so nothing about an existing
workflow changes.

```bash
$DAGRON 06_fault_class_retry_budgets.yaml
# what was attributed, and how many attempts each fault was worth:
sqlite3 dagron.db \
  "SELECT name, attempt, fault_class, fault_confidence FROM task_runs
    WHERE fault_class IS NOT NULL"
```

The vocabulary is `dagron-autopsy --explain`; the same taxonomy drives the
job-autopsy binary that produces these verdicts for jobs running under Slurm
rather than dagron ([docs/HPC_AUTOPSY.md](../../docs/HPC_AUTOPSY.md)).

---

## mcp — a workflow that calls an agent's tools

[`mcp_tool_step.yaml`](mcp_tool_step.yaml) runs the relationship the other way
round from the rest of this directory. `dagron-mcp` lets an agent drive dagron;
`dagron-step-mcp` makes one MCP tool call into one dagron task.

What that buys is everything a task already has. The tool call gets retries with
backoff, a timeout, its output captured, an artifact to hand the next step, and a
row in the run's history. The same call inside an agent's own loop has none of
it: when it fails halfway there is nothing to resume, and nothing that records it
was ever attempted.

```bash
# 1. Build the step binary and put it on the task's PATH.
cargo build -p dagron-step-mcp
export PATH="$(cargo metadata --format-version 1 --no-deps | jq -r .target_directory)/debug:$PATH"

# 2. Create the directories the tasks use and the file the example reads.
mkdir -p /data /artifacts
echo "the quick brown fox jumps over the lazy dog" > /data/report.md

# 3. Run it. The MCP servers are fetched on demand by npx (needs Node 18+ and
#    network); nothing else to install.
$DAGRON mcp_tool_step.yaml
```

The example reads `/data/report.md` through the filesystem MCP server, counts its
words with an ordinary shell task, and writes the count back through the same
server's `write_file` tool — so the interesting part is visible: an MCP result is
just task output, and a task's output is just an argument to the next thing. Both
MCP steps use `@modelcontextprotocol/server-filesystem`, so the flow runs with no
server you have to supply; point either step at a different server and tool for
the real thing.

See [`docs/MCP.md`](../../docs/MCP.md) for the environment contract and the
details worth knowing (server arguments are a JSON array, all-text results come
back as plain text, and a tool reporting `isError` fails the task rather than
writing an error file a retry would then skip).

---

## loop — a conversation whose every turn is a run

[`agent_loop.yaml`](agent_loop.yaml) + [`agent_turn.yaml`](agent_turn.yaml) are
the durable agent loop: a multi-turn conversation where **each turn is its own
run**.

```bash
# The turn's `act` task spawns dagron-step-mcp, so put it on PATH.
cargo build -p dagron-step-mcp
export PATH="$(cargo metadata --format-version 1 --no-deps | jq -r .target_directory)/debug:$PATH"

# One engine, one database, the API on the dev port (127.0.0.1:8787 — the
# default is off). It runs the reconcile loop that drives the turns and serves
# the API the registration needs; it also submits the conversation on start.
# `agent_loop.yaml` names `workflow: agent-turn`, so register the turn against
# this same engine and database, before the conversation's first turn dispatches.
API_ADDR=127.0.0.1:8787 $DAGRON agent_loop.yaml agent.db &
sleep 2
curl -sX POST 127.0.0.1:8787/api/workflows -H 'content-type: application/json' \
  --data "$(jq -Rn --rawfile s agent_turn.yaml '{name:"agent-turn",spec:$s}')"
```

The conversation is one task:

```yaml
- name: turn
  type: workflow
  workflow: agent-turn
  repeat: { until: "{{ output }} == done", max_iterations: 10, delay_secs: 1 }
```

`type: workflow` starts the turn as a child run and parks — holding no worker
slot while it works. When the child finishes, `repeat` compares its result
against `until`: not satisfied and the trigger is re-armed, so the next turn
starts as a new child run; satisfied and the loop is over and downstream
proceeds. The child's result is its `result_from` task's output, which is what
makes a sub-workflow a *function* the parent can branch on.

**There is no loop state in any process.** The turn number is the task's
`attempt` column, the conversation is files, and the rest is rows. That is the
claim, and it is the one worth testing:

```bash
docker compose restart engine    # mid-conversation
```

The next reconcile tick picks the loop up where it was.

**Two things to know before pointing this at real work**, both about scale
rather than correctness:

- **`max_iterations` is the only hard bound on how many runs one submission
  creates.** `SUBWORKFLOW_MAX_DEPTH` does not help — every iteration is a
  sibling at the same depth, not a deeper nesting — and a `budget:` on the
  parent counts the parent's tasks, not the children's. See
  [Loops over sub-workflows](../../docs/CONFIG.md#loops-over-sub-workflows).
- **A turn cannot be told which conversation it belongs to.** A sub-workflow
  trigger runs the child's stored spec as-is and passes no parameters down, so
  the state path is fixed and one state path means one conversation at a time.

---

**Scaling these up** is configuration, not rewrites: point `EXECUTOR=kubernetes`
at a GPU cluster, keep the same workflows, and let `runner_class` pools map to
real capacity. Managed multi-cloud placement, maintained ML runner images, the
hardened LLM step, and workflow generation are the
[fleet](../../README.md#what-this-build-does-not-do) layer on the same
engine.
