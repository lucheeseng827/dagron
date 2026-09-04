# Job autopsy — what actually broke

`dagron-autopsy` is a small binary that **schedules nothing**. It sits beside an
existing Slurm cluster, reads what is already on disk, and answers the question
the scheduler cannot: *why did this job fail?*

`sacct` is the required input — the job's node set and time window are what
everything else is joined against — so the shipped tool is Slurm-based. The
device, fabric and log collectors are scheduler-agnostic, but there is **no
Kubernetes collector**: reading pod status and events as a fifth source is a
documented gap, not a feature. See [Limits](#limits--what-this-does-not-do).

```bash
$ dagron-autopsy 88123 --sacct sacct.txt --dcgm dcgm.ndjson --nccl slurm-88123.out

job 88123  train_llama_70b
  slurm state   FAILED   nodes 16   gpus 128
  gpu-hours     997.6 spent before the failure

  VERDICT       gpu-fallen-off-bus  (infrastructure, high confidence)
  why           dcgm reported gpu-fallen-off-bus on node-47 (gpu3), and the job's
                own logs show the failure downstream of it
  first fault   2026-08-26T09:57:11+00:00  node-47 gpu3  via dcgm
  blast radius  2 of 16 job nodes: node-40, node-47

  ACTION        drain node-47 and retry elsewhere — the job did nothing wrong and
                the next attempt must not land back on this hardware
  retry         yes
  drain         node-47
  budget        retry_budgets: { gpu-fallen-off-bus: 5 }

  EVIDENCE      (most believable first)
    09:57:11  dcgm    gpu-fallen-off-bus gpu3 NVRM: Xid (PCI:0000:1b:00): 79, GPU has fallen off the bus
   ~09:58:41  nccl    nccl-comm-abort gpu3 node-47:41999:42088 [3] NCCL WARN Call to ncclUnhandledCudaError failed
   ~09:58:41  nccl    nccl-timeout [rank0]: [E ProcessGroupNCCL.cpp:563] Watchdog caught collective operation timeout…
```

No database. No daemon. No agent. Nothing about how jobs are submitted changes.

---

## Why this exists

Everything needed to diagnose a failed GPU job is already on the cluster, and
none of it is joined:

| source | knows | does not know |
|---|---|---|
| Slurm (`sacct`) | job state, node set, time window | what a GPU is |
| DCGM | XIDs, ECC, row-remap, **per device** | what a job is |
| NCCL / framework logs | which ranks hung | anything below the process |
| InfiniBand counters | link flaps, symbol errors | which job was on the link |

The scheduler is not an observability system and the observability tools do not
own job state, so nobody performs the join. `dagron-autopsy` does exactly that
join and nothing else.

### The rule that matters most

**A collective timeout is a symptom, never a cause.**

It is printed by every rank that was *waiting* — which is to say, by the healthy
ones. The rank that actually died frequently prints nothing at all. Attributing
the failure to the loudest line in the log is how "NCCL timeout" becomes a
network ticket for what was a stuck dataloader, and it is the most common wrong
diagnosis in this domain.

So `nccl-timeout` is classified with an **unknown** disposition and **symptom**
precedence. It can corroborate a cause found elsewhere, and the pattern of *who
stayed silent* can name the culprit, but no amount of it adds up to a device
fault on its own.

---

## Install and run

The binary is part of the dagron workspace:

```bash
cargo build --release -p dagron-autopsy
# → target/release/dagron-autopsy   (one binary; copy it to a login node)
```

That is a normal dynamically-linked build against the host's libc, which is
fine when the login node matches the build host. For a binary that runs
anywhere, build it against musl — the module has no target or linker
configuration for this, so pass it explicitly:

```bash
rustup target add x86_64-unknown-linux-musl
cargo build --release -p dagron-autopsy --target x86_64-unknown-linux-musl
```

### The inputs

Every input is a file (or `-` for stdin), so you can run it against last week's
job on your laptop.

```bash
# 1. sacct — the frame. The one required input: without a node set and a time
#    window there is nothing to join against.
sacct -j 88123 -P -n \
  -o JobID,JobName,State,ExitCode,Start,End,Elapsed,NodeList,AllocTRES,Reason \
  > sacct.txt

# 2. DCGM — dcgmi text, CSV with a header, or NDJSON from dcgm-exporter.
#    Whatever shape your site already exports; all three are accepted.

# 3. The job's stdout/stderr, as-is.

# 4. InfiniBand counters, sampled before and after the job. Both are needed:
#    every healthy port has large lifetime error counters, so only a *rise*
#    inside the job's window is evidence.

dagron-autopsy 88123 \
  --sacct sacct.txt \
  --dcgm  /var/log/dcgm/events.json \
  --nccl  slurm-88123.out \
  --ib-before ib-pre.txt --ib-after ib-post.txt
```

`--collect` runs `sacct` for you instead of `--sacct`. It is opt-in rather than a
fallback: a diagnostic tool that shells out without being asked is one an admin
cannot reason about, and on a busy login node an unexpected `sacct` is a real
cost.

Missing a source? The tool still runs, and **says what it could not consult**:

```text
  CAVEATS
   - no --dcgm input: XID, ECC and row-remap events were not consulted, so a
     device fault cannot be found even if there was one
```

A verdict reached on partial evidence says so rather than reading like a complete
one.

### Output

`--format text` (default) leads with the answer; `--format json` emits the stable
machine contract:

```json
{
  "job_id": "88123",
  "job_name": "train_llama_70b",
  "state": "FAILED",
  "nodes": ["node-40", "node-47"],
  "gpus": 128,
  "gpu_hours_lost": 997.6,
  "class": "gpu-fallen-off-bus",
  "disposition": "infrastructure",
  "confidence": "high",
  "rationale": "dcgm reported gpu-fallen-off-bus on node-47 (gpu3), and the job's own logs show the failure downstream of it",
  "first_fault": {
    "at": "2026-08-26T09:57:11+00:00",
    "dated": true,
    "node": "node-47",
    "device": "gpu3",
    "source": "dcgm",
    "detail": "NVRM: Xid (PCI:0000:1b:00): 79, GPU has fallen off the bus"
  },
  "affected_nodes": ["node-40", "node-47"],
  "recommendation": {
    "retry": true,
    "drain_node": "node-47",
    "retry_budget_hint": "retry_budgets: { gpu-fallen-off-bus: 5 }",
    "summary": "drain node-47 and retry elsewhere — the job did nothing wrong and the next attempt must not land back on this hardware"
  },
  "evidence": [
    {
      "at": "2026-08-26T09:57:11+00:00",
      "dated": true,
      "node": "node-47",
      "device": "gpu3",
      "source": "dcgm",
      "class": "gpu-fallen-off-bus",
      "confidence": "high",
      "detail": "NVRM: Xid (PCI:0000:1b:00): 79, GPU has fallen off the bus"
    }
  ],
  "warnings": []
}
```

Absent fields are omitted rather than null — `gpus` and `gpu_hours_lost` do not
appear at all when the allocation did not name GPUs, so a consumer never reads
a fabricated zero as a real measurement.

Feed it to a fleet database, a ticket, or dagron's own `task_runs.fault_class`.

---

## The taxonomy

`dagron-autopsy --explain` prints the whole table. The columns that drive
behaviour:

- **disposition** — is another attempt worth anything?
  `infrastructure` (the hardware broke; retry elsewhere, liberally) ·
  `application` (the job's own code or data; another attempt reproduces it) ·
  `platform` (preemption, wall clock, cancellation — a policy question) ·
  `unknown` (not enough evidence; **never guessed into one of the others**).
- **budget** — the default attempts the class gets; `task's` means the class
  declines to have an opinion and your `max_attempts` applies.
- **drain** — whether the node should leave the pool before anything retries
  onto it.
- **believed as** — `a cause`, `either`, or `a symptom only`.

| class | disposition | drain | believed as |
|---|---|---|---|
| `gpu-xid` | infrastructure | yes | a cause |
| `gpu-ecc` | infrastructure | yes | a cause |
| `gpu-fallen-off-bus` | infrastructure | yes | a cause |
| `nvlink` | infrastructure | yes | a cause |
| `gpu-unresponsive` | infrastructure | yes | a cause |
| `fabric-ib` | infrastructure | yes | a cause |
| `node-fail` | infrastructure | yes | a cause |
| `storage` | infrastructure | – | a cause |
| `nccl-comm-abort` | infrastructure | – | either |
| `nccl-timeout` | **unknown** | – | **a symptom only** |
| `deadlock` | application | – | either |
| `straggler-rank` | application | – | either |
| `dataloader-stall` | application | – | either |
| `host-oom` / `gpu-oom` | application | – | a cause |
| `nan-loss` | application | – | a cause |
| `checkpoint-corrupt` | application | – | a cause |
| `user-code` | application | – | either |
| `config` | application | – | a cause |
| `preemption` | platform | – | a cause |
| `walltime-exceeded` / `cancelled` | platform | – | a cause |
| `unknown` | unknown | – | a symptom only |

### XIDs: whose fault is it

The split that matters is not "how bad" but **whose fault**:

- **13, 31, 43, 45** — the driver reporting that a *kernel* misbehaved (illegal
  address, graphics exception). That is the job's code. Moving it to another GPU
  does not help, and draining the node for it takes a healthy machine out of the
  pool.
- **48, 63, 64, 92, 94, 95** (ECC), **74/80/81** (NVLink), **79** (fell off the
  bus), **62/119–122** (firmware wedged) — the device. Drain it.

An XID with no mapping is reported as `gpu-xid` at **low** confidence rather than
being dropped (which hides a real device event) or promoted (which invents a
disposition for it).

---

## How the join works

1. **Filter by the job's node set.** A GPU that died on a node this job never
   held is not this job's failure, however dramatic the line. Without this, one
   broken node is blamed for every job on the cluster.
2. **Filter by the job's window**, extended 120 s past its end (`--grace`) and
   30 s at both ends for clock skew (`--skew`). The window extends *past* the
   exit because the driver's XID lands in syslog after the process is already
   gone — a window that closes at the exit misses exactly the evidence it was
   opened for.
3. **Rank by precedence, then by time.** In that order, deliberately: the
   earliest signal is almost always the symptom, because every healthy rank
   notices a hang before the broken device finishes reporting itself.
4. **Never promote a symptom.** If nothing believable as a cause is in the
   window, the rank topology gets a turn: *everyone stuck and nobody missing* is
   a `deadlock` (the collective is inconsistent — no rank was late); *everyone
   but a few stuck* names those few as `straggler-rank`, because the ranks that
   printed nothing are the ones the others were waiting on. A partition — half
   the job silent — is neither, and the tool says so rather than guessing.
5. **Low confidence never acts.** A verdict below `medium` recommends gathering
   the missing source, not draining a node.
6. **No evidence is reported as unattributed, never as fine.** Silence about a
   failure otherwise reads as a healthy cluster.

### Clocks

The four sources are stamped by four different clocks, and a systematic offset
does not degrade this join gracefully — it silently empties the intersection and
the tool reports a clean cluster. A timestamp with an explicit offset is believed
as written; **a naive timestamp is assumed UTC**. If your sources disagree, fix
the clocks or widen `--skew`; do not let the tool guess.

---

## Fault-class-aware retry budgets

The autopsy tool is the offline half. Inside dagron, the same taxonomy drives
**how many attempts a task gets given what broke** — because `max_attempts`
spends the same budget on every failure, so infra faults give up too early and
application faults burn GPU-hours proving a determinism nobody doubted.

```yaml
name: train
task_defaults:
  retry_budgets:
    gpu-ecc: 8              # the node broke; try elsewhere, liberally
    gpu-fallen-off-bus: 8
    fabric-ib: 8
    preemption: 20          # on spot, retry is the whole strategy
    nan-loss: 0             # never again — the next attempt diverges too
    checkpoint-corrupt: 0
tasks:
  - name: pretrain
    command: ["torchrun", "train.py"]
    max_attempts: 3         # still applies to anything unclassified
    runner_class: spot-gpu
```

Resolution order, most specific first:

1. the task's own `retry_budgets:` entry for the class that occurred;
2. the class's disposition default — infrastructure **5**, platform **3**,
   application **1**;
3. `max_attempts`, when the failure is unclassified or the class declines to have
   an opinion (`unknown`, `nccl-timeout`).

**Every existing workflow keeps its current behaviour**: with no `retry_budgets:`
and an unclassified failure, the rule is exactly `attempt < max_attempts`. The
`retry_on_timeout: false` carve-out still applies first and still wins — a fault
class never resurrects a deadline kill the author opted out of retrying.

`retry_budgets: { <class>: 0 }` means the attempt that just ran was the last one.
An unrecognised class name is a **parse error**, not a silently-inert policy —
this is the code path that decides whether to spend another thousand GPU-hours.

`task_defaults.retry_budgets` merges **per class**: a task that overrides
`nan-loss` still inherits the workflow's `gpu-ecc`.

### What gets recorded

Every classified failure writes `fault_class`, `fault_detail` (the quoted line)
and `fault_confidence` onto the `task_runs` row, fence-guarded like the state
transition beside it. `NULL` means nothing looked; `'unknown'` means something
looked and could not tell — the two need different follow-up.

Two Prometheus series come with it, both fixed-cardinality by construction:

```text
scheduler_task_faults_total{class="gpu-ecc",disposition="infrastructure"} 12
scheduler_task_faults_by_disposition_total{disposition="application"} 3
```

Both are **counts of failed attempts**, grouped by their labels — not
GPU-hours, and not a reclaimed-versus-burned measure. They answer "what is
breaking, and is it ours or the hardware's". Costing it needs the job's GPU
count and elapsed time, which the metrics do not carry; `gpu_hours_lost` on an
autopsy record does, and `db::fault_class_counts` gives the same split over the
task table.

---

## Limits — what this does not do

- **It does not schedule anything.** It places no work, drains no nodes, and
  submits nothing. It prints a recommendation; acting on it is yours.
- **It does not collect telemetry.** It reads what your site already exports. If
  DCGM is not deployed, no device fault can be found — and the record says so.
- **It is not a node health system.** It reads DCGM; it does not run diagnostics.
- **It cannot see inside the framework.** A rank stuck in Python is visible only
  as silence, which is enough to *name* the rank and never enough to explain it.
- **One job at a time.** Fleet aggregation — GPU-hours by class by week, node
  leaderboards — is not in this binary.
- **Slurm is the frame.** `sacct` supplies the node set and time window every
  other source is joined against, so a job that never ran under Slurm has
  nothing to join. A Kubernetes collector (pod status and events as a fifth
  source) is a gap, not a shipped feature.

## See also

- [`AI_WORKLOADS.md`](AI_WORKLOADS.md) — long/checkpointed tasks, GPU routing,
  the resume contract these budgets sit on top of.
- [`CONFIG.md`](CONFIG.md) — engine configuration reference.
