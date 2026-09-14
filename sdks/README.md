# dagron SDKs

Define dagron DAGs in code (instead of writing YAML) and drive `dagron-api` from
your language of choice. The SDKs emit JSON — valid dagron input, since dagron
parses YAML and JSON is a YAML subset — and submit it through the gateway.

Both cover the same ground, method for method: a builder over the engine's whole
`TaskSpec` — every task kind, fan-out, sensors, approval gates, templates — and a
`Client` over the whole `dagron-api` surface (runs, workflows and their
lifecycle, schedules and backfills, environments and secrets, datasets,
artifacts, archive, triage, access tokens, dead-letters, GitOps, settings,
metrics). Their versions track the API version they speak to, so `0.9.x` in both
means "covers the 0.9 gateway".

- [`python/`](python) — `dagron-sdk` (standard-library only). See
  [`python/README.md`](python/README.md) and the endpoint-by-endpoint
  [`python/ROADMAP.md`](python/ROADMAP.md), which is the coverage matrix for both.
- [`typescript/`](typescript) — `@dagron/sdk` (ESM, zero deps + hand-written
  `.d.ts`). See [`typescript/README.md`](typescript/README.md).

## TypeScript / JavaScript

```ts
import { Dag } from "@dagron/sdk";

const dag = new Dag("etl");
const extract = dag.task("extract", { image: "alpine", command: ["echo", "hi"] });
dag.task("load", { image: "alpine", command: ["true"], dependsOn: [extract] });

await dag.submit("http://localhost:8080", { token: process.env.DAGRON_TOKEN });
// or: console.log(dag.toJSON())
```

Or drive the control plane with `Client`:

```ts
import { Client } from "@dagron/sdk";

const api = Client.fromEnv();                  // DAGRON_API_URL + DAGRON_TOKEN
const runId = await api.submitRun(dag);
const result = await api.waitRun(runId);       // blocks server-side
await api.approveTask(runId, "review-gate");   // type: approval gates
```

Test: `cd typescript && node --test`.

## Building the image a task runs in

Both SDKs take a `Recipe` wherever a task takes an image. The image is then built
by the workflow that needs it, instead of by a pipeline somewhere else:

```python
from dagron import Dag, Recipe, RecipeFile

recipe = Recipe("etl", "python:3.12-slim", pip=["duckdb==1.1.3"])
dag = Dag("nightly")
dag.task("report", image=recipe, command=["python", "/app/report.py"])
```

```js
const recipe = new Recipe("etl", "python:3.12-slim", { pip: ["duckdb==1.1.3"] });
const dag = new Dag("nightly");
dag.task("report", { image: recipe, command: ["python", "/app/report.py"] });
```

Passing a recipe adds a build task, makes the task depend on it, and fills in
`docker_image` with the reference that build will produce. The reference is
known at author time because it is derived from the recipe — which is also why
re-submitting an unchanged recipe finds the image already built rather than
building it again.

`recipe-vectors.json` in this directory is why both SDKs can be trusted to name
the same image the builder produces. The tag is a promise about an image that
does not exist yet, so a one-byte disagreement between an SDK and the builder
pins a task to something nothing will ever push, and nothing fails until the run
does. The vectors are generated from the builder and asserted against by every
implementation of that hash — both SDKs, the console, and the builder itself.

Running the build needs a runner pool that has a builder on it. That is an
enterprise capability: the SDKs can describe a build, but they cannot perform
one, and without such a pool the build task stays pending.

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
print(api.wait_run(run_id)["status"]) # …and block on it server-side
api.create_workflow(dag)              # …or save it as a reusable workflow
# one-liner shorthand: dag.submit("http://localhost:8080", token=...)
```

Test: `cd python && python -m unittest`.

Both builders mirror the server's `validate_graph` in full — one kind per task,
valid trigger rules and runner classes, resolvable template calls and
dependencies, no cycles — so a spec the gateway would reject fails locally
instead, and both omit empty fields so the emitted spec stays clean. Both stop
where the server stops: a dependency that only resolves after a chain is inlined
is left to the engine rather than rejected early. The gateway accepts a submitted
DAG as `{"yaml": "..."}`; both `Client`s wrap that for you.
