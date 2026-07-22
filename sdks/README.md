# dagron SDKs

Define dagron DAGs in code (instead of writing YAML) and drive `dagron-api` from
your language of choice. The SDKs emit JSON — valid dagron input, since dagron
parses YAML and JSON is a YAML subset — and submit it through the gateway.

- [`typescript/`](typescript) — `@dagron/sdk` (ESM, zero deps + `.d.ts` types). DAG
  builder **plus a full `Client`** mirroring the Python client's control-plane
  surface (runs, approvals, workflows, schedules, backfill jobs, dead-letters,
  GitOps, SSE streaming).
- [`python/`](python) — `dagron` (standard-library only). DAG builder **plus a full
  `Client`** covering the whole `dagron-api` control plane (runs, workflows,
  schedules, dead-letters, GitOps). See [`python/README.md`](python/README.md) and
  the coverage [`python/ROADMAP.md`](python/ROADMAP.md).

## TypeScript / JavaScript

```ts
import { Dag } from "@dagron/sdk";

const dag = new Dag("etl");
const extract = dag.task("extract", { image: "alpine", command: ["echo", "hi"] });
dag.task("load", { image: "alpine", command: ["true"], dependsOn: [extract] });

await dag.submit("http://localhost:8080", { token: process.env.DAGRON_TOKEN });
// or: console.log(dag.toJSON())
```

Or drive the control plane with `Client` (same surface as the Python client):

```ts
import { Client } from "@dagron/sdk";

const api = new Client("http://localhost:8080", { token: process.env.DAGRON_TOKEN });
const runId = await api.submitRun(dag);
for await (const ev of api.streamRun(runId)) console.log(ev.event, ev.data);
await api.approveTask(runId, "review-gate"); // type: approval gates
```

Test: `cd typescript && node --test`.

## Python

The Python SDK goes beyond authoring: a `Client` covers the full `dagron-api`
control plane (see [`python/README.md`](python/README.md)).

```python
import os
from dagron import Dag, Client

dag = Dag("etl")
extract = dag.task("extract", image="alpine", command=["echo", "hi"])
dag.task("load", image="alpine", command=["true"], depends_on=[extract])

api = Client("http://localhost:8080", token=os.environ.get("DAGRON_TOKEN"))
run_id = api.submit_run(dag)          # trigger a run
api.create_workflow(dag)              # …or save it as a reusable workflow
# one-liner shorthand: dag.submit("http://localhost:8080", token=...)
```

Test: `cd python && python -m unittest`.

Both SDKs validate the DAG at build time (unknown `depends_on`, and the Python
builder also checks leaf-xor-chain and cycles) and omit empty fields so the
emitted spec stays clean. The gateway accepts a submitted DAG as `{"yaml": "..."}`;
the Python `Client` wraps that for you.
