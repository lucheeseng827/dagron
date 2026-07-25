# AI-workload case studies — five live pipelines

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
placement of these pools across clouds is the dagron Enterprise layer.)

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
[dagron Enterprise](../../README.md#dagron-enterprise).)

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

---

**Scaling these up** is configuration, not rewrites: point `EXECUTOR=kubernetes`
at a GPU cluster, keep the same workflows, and let `runner_class` pools map to
real capacity. Managed multi-cloud placement, maintained ML runner images, the
hardened LLM step, and workflow generation are the
[dagron Enterprise](../../README.md#dagron-enterprise) layer on the same
engine.
