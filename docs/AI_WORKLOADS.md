# AI workloads — long, preemptible, checkpointed

Training jobs and other AI-adjacent work are **long** (hours–days),
**preemptible** (spot capacity vanishes mid-epoch), and **expensive to restart
from zero**. dagron's lease/claim scheduler was built for exactly this failure
shape — a dead worker's task is *re-dispatched, not lost* — and three
primitives turn that into first-class AI-workload support:

1. **Long-lived tasks (lease heartbeat).** Workers renew a running task's
   lease every 10 s, so a task may run for hours under the same short-lease
   crash recovery: a worker that dies stops heartbeating and its task is
   reclaimed and re-dispatched within seconds-to-lease-expiry. On by default
   (`TASK_LEASE_HEARTBEAT`, [CONFIG.md](CONFIG.md)); pair with a realistic
   `timeout_secs` per task.
2. **Checkpoint-aware resume.** A running task reports each committed
   checkpoint; the pointer is stored durably and survives retries, and the
   next attempt starts with `DAGRON_RESUME_FROM` set — resume from epoch N,
   not epoch 0. dagron owns *pointer durability*; what's inside the checkpoint
   belongs to the framework (PyTorch/JAX/DeepSpeed/…).
3. **Accelerator-aware routing.** `resources.gpu` sugar for Kubernetes
   extended resources, and `runner_class` pools (e.g. `spot-gpu` vs
   `ondemand-gpu` vs `cpu`) so each stage lands on the right capacity.

## The checkpoint/resume contract

Injected into every task's environment at dispatch:

| Variable | When | Meaning |
|---|---|---|
| `DAGRON_RUN_ID` / `DAGRON_TASK` / `DAGRON_TASK_ID` | always | The task's identity — what the checkpoint report route needs. |
| `DAGRON_ARTIFACTS` | artifact store on (`DAGRON_ARTIFACT_DIR`) | The run's shared artifact dir (pass files between tasks). |
| `DAGRON_CHECKPOINT_DIR` | artifact store on | Per-task checkpoint dir under the run's artifacts. |
| `DAGRON_ARTIFACTS_URL` | cloud store on (`DAGRON_ARTIFACT_URL`) | The run's cloud location (`s3://…/<run>`) — reach it with your own tooling. |
| `DAGRON_CHECKPOINT_URL` | cloud store on | Per-task cloud checkpoint prefix (`…/<run>/.checkpoints/<task>`). |
| `DAGRON_RESUME_FROM` | retry attempts, if a checkpoint was reported | URI/path of the last committed checkpoint — load it and continue. |
| `DAGRON_RESUME_MARKER` | with `DAGRON_RESUME_FROM` | The optional progress marker reported with it (e.g. `epoch=7`). |

A task reports a checkpoint in either (or both) of two ways:

```bash
# 1. HTTP (any executor; durable in the datastore, carries a marker):
curl -s -X POST "$DAGRON_API/runs/$DAGRON_RUN_ID/tasks/$DAGRON_TASK_ID/checkpoint" \
     -H 'content-type: application/json' \
     -d "{\"uri\":\"$DAGRON_CHECKPOINT_DIR/epoch-$e.ckpt\",\"marker\":\"epoch=$e\"}"

# 2. File convention (no API needed — one-shot/local runs):
#    write the checkpoint, then point `latest` at it:
cp model.ckpt "$DAGRON_CHECKPOINT_DIR/epoch-$e.ckpt"
echo "$DAGRON_CHECKPOINT_DIR/epoch-$e.ckpt" > "$DAGRON_CHECKPOINT_DIR/latest"
```

On the next attempt (retry after failure/preemption, `Rerun failed` in the UI):

```bash
if [ -n "$DAGRON_RESUME_FROM" ]; then
  echo "resuming from $DAGRON_RESUME_FROM (${DAGRON_RESUME_MARKER:-})"
  load_checkpoint "$DAGRON_RESUME_FROM"      # torch.load, flax restore, …
fi
```

### Cloud checkpoints (multi-cloud resume)

Point `DAGRON_ARTIFACT_URL` at a bucket (`s3://bucket/prefix`, `gs://…`,
`az://…`; credentials from the standard `AWS_*`/`GOOGLE_*`/`AZURE_*` env) and
the checkpoint substrate becomes cloud-durable: tasks get
`DAGRON_CHECKPOINT_URL` (a per-task prefix under the run) to upload
checkpoints with their own tooling, report the object URI via the checkpoint
route, and the *next* attempt — **on any machine, in any cloud that can reach
the bucket** — resumes from it. That is what makes spot-preempted training
portable: the worker dies with the instance, the checkpoint doesn't.

```bash
aws s3 cp model.ckpt "$DAGRON_CHECKPOINT_URL/epoch-$e.ckpt"
curl -s -X POST "$DAGRON_API/runs/$DAGRON_RUN_ID/tasks/$DAGRON_TASK_ID/checkpoint" \
     -H 'content-type: application/json' \
     -d "{\"uri\":\"$DAGRON_CHECKPOINT_URL/epoch-$e.ckpt\",\"marker\":\"epoch=$e\"}"
```

The artifact API reads and writes the same bucket through the
`dagron-artifact` cloud backend (build features `s3` / `gcs` / `azure`;
S3-compatible MinIO/Ceph via `AWS_ENDPOINT_URL`), and the BYOK/KMS
[`EncryptedStore`](../crates/dagron-artifact/src/lib.rs) wraps it unchanged —
ciphertext in the bucket, keys never stored beside the data.

Semantics worth knowing: the pointer survives `retry` and **Rerun failed**
(resume is the point), and is cleared by **Clear task** (an explicit
fresh-start). Only a *running* attempt may report, so a reclaimed straggler
can't overwrite a newer attempt's progress. Spot preemption notices (the
30–120 s warning) slot in as a shell `trap`: flush a checkpoint, report it,
exit non-zero — the retry resumes where the notice interrupted. Retries,
backoff and `max_attempts` are the ordinary task knobs
([README](../README.md#workflow-format)).

## GPUs and capacity pools

```yaml
name: train_then_eval
tasks:
  - name: train
    command: ["python", "train.py"]
    runner_class: spot-gpu            # claimed only by schedulers serving this pool
    timeout_secs: 14400               # 4 h wall clock; heartbeat keeps the lease live
    max_attempts: 5                   # preemptions are retries, resumed via checkpoint
    resources:
      gpu: { count: 4 }               # → limits["nvidia.com/gpu"] = "4" on the task pod
      requests: { cpu: "8", memory: "64Gi" }
  - name: evaluate
    command: ["python", "eval.py"]
    depends_on: [train]
    runner_class: cpu
```

`resources.gpu.resource` overrides the extended-resource key
(`amd.com/gpu`, `google.com/tpu`, a MIG profile like `nvidia.com/mig-1g.5gb`);
an explicit `limits` entry for the same key wins. Start one scheduler per pool
with `RUNNER_CLASSES=spot-gpu`, `RUNNER_CLASSES=cpu`, … and each claims only
its classes — capacity segmentation with no extra moving parts.

## Gang co-scheduling (distributed training)

Multi-node training needs **N ranks together or none**. A `gang:` task expands
into member instances (`train.0` … `train.N-1`) sharing one gang id, and
dependents wait for every member:

```yaml
tasks:
  - name: train
    command: ["torchrun", "--nnodes", "4", "train.py"]
    runner_class: spot-gpu
    gang: { size: 4 }        # 4 ranks, claimed all-or-nothing
```

Members receive `DAGRON_GANG_ID` / `DAGRON_GANG_RANK` / `DAGRON_GANG_SIZE` for
rendezvous (on Kubernetes, a headless Service per gang id is the master-addr
fast path). Semantics are **die-together**: one member failing cancels its
siblings (their heartbeats lose the fenced claim and abort), and the gang
retries as a unit via run-level rerun — so gang tasks are single-attempt
(`max_attempts: 1`, enforced at validation, like `repeat`/approval combos).

The gang spec, expansion, and rendezvous env are open; the **all-or-nothing
claimer** (claim a gang only when every member is ready and capacity fits the
whole gang, never a partial gang) runs in the dagron Enterprise scheduler
(`RUNNER_GANGS=1`). Without it, members schedule as ordinary independent
tasks.

## What the DAG model already gives AI pipelines

Fan-out over shards/hyperparameters (`with_items` / `with_param`), quality
gates (`when:` on an upstream task's output), human sign-off before deploy
(`type: approval`), poll-until-converged (`repeat:`), durable-function calls
(`result_from` + `POST /runs?wait=true`), SLA alerts (`deadline:`), and
per-environment secrets — see the [README](../README.md) and
[HOWTO](HOWTO.md).

### Runnable case studies

Five end-to-end examples live in [`examples/ai/`](../examples/ai/): training
that survives a mid-run kill via checkpoint resume, spot/on-demand pool
routing, sharded batch inference with a gather step, an LLM content pipeline
with a human approval gate, and a train→eval→deploy quality gate.

## The complete suite (dagron Enterprise)

The primitives above are open source deliberately — they are the programming
model. **dagron Enterprise** layers the managed fleet on top:

- **Workflow generation** — describe a pipeline in natural language, get a
  schema-validated dagron spec (generation is validated against the same
  parser the engine runs, with automatic repair rounds).
- **A hardened LLM task step** — a drop-in task binary for LLM calls with
  durable retries, idempotent re-runs (no double-spend on retry), output
  capture to artifacts, and an egress guard for credentials.
- **Fleet placement** — cost/preemption-aware routing of `runner_class` pools
  across clouds and regions, quota enforcement, and workspace isolation on a
  managed control surface.
- **Checkpoint placement policy** — write-local/replicate-async mirroring
  (checkpoints land in the nearest bucket first, then replicate for
  cross-cloud resume), on top of the open cloud backends above.
- **Runner images** — maintained ML training/serving runner images for the
  pools the scheduler routes to.

The open engine is the same code the managed fleet runs — a workflow proven
here needs no rewrite there. See
[README → dagron Enterprise](../README.md#dagron-enterprise).
