# Build a workflow end-to-end (UI guide)

> Audience: users who want to **author, run, chain, and version** workflows from
> the dagron console — no YAML-by-hand required.

This is the click-by-click path from an empty editor to a workflow that runs,
chains another workflow, and lands in Git for review.

---

## 0. What you can do in the UI

| Action | Where | Notes |
|---|---|---|
| **Author** a DAG visually or as YAML | Workflows → New | The two views stay in sync. |
| **Chain** another saved workflow as a step | a task's `workflow_ref` | The headline of this guide — §5. |
| **Call** a reusable sub-DAG declared in the same spec | `templates:` + a task's `template:` | DAG-of-DAGs without a second saved workflow — §5.5. |
| **Run** a workflow and watch it live | ▶ Run → run view | DAG graph colored by status; per-task logs over SSE. |
| **Control** a run | run view | Cancel, retry a task, rerun from failure. |
| **Schedule** it | editor → Schedules drawer | Cron-fired by the engine. |
| **Version** it in Git | editor → Sync to Git | Opens a pull request with the spec (§7). |

---

## 1. Bring up the stack (local)

The whole stack — Postgres, the engine, the authenticated API, and the web UI —
comes up with one command ([`compose.yaml`](../compose.yaml), validated with
podman):

```bash
podman compose up --build      # or: docker compose up --build
```

This starts:

- **postgres** — the datastore (source of truth).
- **engine** — the scheduler/reconcile loop + executors (`EXECUTOR=local`, so
  `command:` tasks run as local processes). It also runs the DB migrations.
- **dagron-api** (`:8080`) — the authenticated gateway, and the console: it
  serves the UI at `/` and the API under `/api` on the same origin.

Open **http://localhost:8080**.

> Before 0.9 the console was a separate `frontend` container on `:3000`.
> `compose.yaml` still carries it behind `--profile frontend` until 1.0.0
> removes it; nothing needs it.

> **Sign in** with the seeded admin from `compose.yaml`:
> `admin@local` / `dagron-admin`. (dagron-api owns login and mints its own
> session cookie — no external IdP needed for local dev.)

---

## 2. Author your first workflow

1. Go to **Workflows → New**.
2. Pick **“Start from an example…” → Hello world** (or just edit the default).
   You now have:

   ```yaml
   name: hello-world
   tasks:
     - name: greet
       command: ["sh", "-c", "echo hello from dagron"]
   ```

3. Toggle between **Visual** and **YAML** at the top. They are the same spec:
   - **Visual**: the **Blocks** rail on the left holds premade steps (S3
     staging, compression, CSV→Parquet, model training, webhooks, …) — click
     one to append it to the pipeline, or **drag it onto the canvas** to place
     it exactly where you want it (§2b). `+ Task` adds a blank node; drag a
     node’s bottom handle onto another node to add a dependency
     (`depends_on`); select a node to edit its command, retries, and timeout
     in the right panel; select an edge or node and press Delete to remove it.
     Cycles are blocked as you draw them.
   - **YAML**: full control, with the same live graph beside it.

   > **When Visual is locked.** The canvas draws one node per task. Some spec
   > features mean that picture isn't the graph that runs — `with_items:` fans
   > one task out into many, `hook:` wires a task to every other task, `type:
   > wait` is a sensor with no command, `cache:` can skip execution entirely.
   > Rather than let you edit a drawing that lies, the editor **disables the
   > Visual tab** for such a spec ("this workflow uses features the visual
   > editor can't edit — use the YAML tab") and lists the fields responsible.
   > Nothing is dropped: the spec is untouched and fully editable as YAML.
4. Click **Save**. You’re redirected to the saved workflow; **▶ Run**, **Sync to
   Git**, **Delete**, and the **Schedules** drawer appear.

### The task fields

| Field | YAML key | Meaning |
|---|---|---|
| Command | `command` | argv to run, e.g. `["sh","-c","echo hi"]`. |
| Docker image | `docker_image` | container image to run the command in; empty = host executor. |
| Depends on | `depends_on` | task names that must finish first (the DAG edges). |
| Max attempts | `max_attempts` | retries on failure (default 1 = no retry). |
| Retry delay s | `retry_delay_secs` | base backoff; actual = base · 2^(attempt-1). |
| Retry max delay s | `retry_max_delay_secs` | clamp on the exponential backoff. |
| Timeout s | `timeout_secs` | per-task timeout. |
| Run when | `trigger_rule` | when the task fires vs its deps' outcomes: `all_success` (default), `all_done`, `one_failed`, `all_failed`, `none_failed`. |
| Runs workflow | `workflow_ref` | **chain another saved workflow** — see §5. |
| Calls template | `template` | **call a `templates:` sub-DAG declared in this spec** — see §5.5. |
| Arguments | `arguments` | values passed to the called template, filling its `parameters`. |
| (task kind) | `type: approval` | human approval gate: no command/ref; optional `approval_timeout_secs` + `approval_on_timeout: reject` (default) \| `approve`. |

Run-level fields (top of the spec, beside `name`):

| YAML key | Meaning |
|---|---|
| `run_timeout_secs` | auto-terminate: cancel the whole run past this wall-clock budget. |
| `result_from` | the named task's output becomes the run's result (`GET /api/runs/:id/wait`). |

### 2b. The blocks palette (visual, Submit & re-run editors)

The workflow editor's **Visual** view, the Submit page, and the "Re-run with
changes" dialog all carry a **Blocks** rail: a searchable library of premade,
ready-to-run steps so data engineers, ML engineers, and data scientists can
compose a pipeline by clicking instead of writing spec by hand (the YAML view
is always there for fine-tuning). Two ways to use it:

- **Click** a block → it is appended and auto-chained onto the current end of
  the pipeline (the live preview / canvas redraws immediately).
- **Drag** a block onto the visual canvas → it is placed exactly where you
  drop it, unchained — then drag node handles to wire its dependencies.
- **Drag** a block onto a dependency **edge** (it highlights blue) → the block
  is spliced into it: `a → b` becomes `a → block → b`. Other tasks that
  depended on `a` keep their direct edge.

What's in the library (search with e.g. `s3`, `parquet`, `ml`):

| Category | Blocks |
|---|---|
| Tasks | shell task, Docker task, retrying task, task with timeout. |
| Data & storage | S3 create temp storage, S3 download input, S3 upload results, S3 delete temp storage (`all_done`), compress files (tar.gz), extract archive. |
| Data engineering | CSV → Parquet (pandas), Postgres → CSV export (psql), dbt run, data quality gate (fails the run on bad data). |
| ML & Python | Python script, train model (scikit-learn), evaluate model (accuracy gate), batch inference. |
| Integration | HTTP fetch → file (with retries), notify webhook (Slack/Teams). |
| Control flow | sub-workflow call, cleanup (`all_done`), failure handler (`one_failed`), approval gate. |
| Run settings | auto-terminate run, result-from task. |

Every block emits only fields the engine executes, with obvious inline
placeholders (`my-data-bucket`, `db-host`, `hooks.slack.com/…`) to edit in the
task panel or YAML. Notes for real use:

- Blocks that pass files use `${DAGRON_ARTIFACTS:-/tmp}` — with the artifact
  store on (`DAGRON_ARTIFACT_DIR`, [`CONFIG.md`](CONFIG.md)) host-executor
  tasks in a run share that directory. Containerized tasks don't share a
  filesystem; pass intermediates through external storage (the S3 blocks).
- Cloud/database credentials come from the executor's environment (host
  executor inherits the worker's env). The demo images install their CLI on
  the fly (`apk add aws-cli`, `pip install …`); bake your own image to make
  those steps fast.

---

## 3. Run it and watch

1. Click **▶ Run**. You land on the **run view** (`/runs/<id>`).
2. The DAG renders with nodes colored by status (pending → ready → running →
   succeeded/failed). Updates stream live over SSE — no refresh needed.
3. Click a task to open its panel: status, attempt count, and **logs/output**.
4. If something fails: **Retry** the task, or **Rerun from failure** to reset the
   whole failed frontier and resume. **Cancel** stops a running DAG.

Try the **Diamond** starter next (`a → {b,c} → d`) to see real parallelism and a
retry policy on `d`.

---

## 4. A two-step pipeline (the building block)

Use the **Two-step sequence** starter:

```yaml
name: my-workflow
tasks:
  - name: prepare
    command: ["sh", "-c", "echo prepare"]
  - name: process
    command: ["sh", "-c", "echo process"]
    depends_on: [prepare]
```

In the visual view this is two nodes with one edge (`prepare → process`). Save and
run it. This is the unit you’ll **chain** next.

---

## 5. Compose: chain a workflow (`workflow_ref`) or call a sub-DAG (`template:`)

This is the “workflow of workflows” flow: a step in one workflow **runs another
saved workflow**. You compose pipelines you’ve already built instead of copying
their tasks.

### 5.1 Save the child

1. **Workflows → New → “Reusable sub-workflow (etl)”**. You get a
   build → process → publish pipeline named `etl`.
2. **Save** it. (The child must exist and be saved before a parent can reference
   it by name.)

### 5.2 Build the parent that calls it

1. **Workflows → New → “Chained workflows (calls etl)”**:

   ```yaml
   name: nightly
   tasks:
     - name: prepare
       command: ["sh", "-c", "echo prepare"]
     - name: run-etl
       workflow_ref: etl          # ← chain the saved `etl` workflow
       depends_on: [prepare]
     - name: notify
       command: ["sh", "-c", "echo done"]
       depends_on: [run-etl]
   ```

2. Notice the **`run-etl`** node renders as a **dashed “sub-workflow” node**
   showing `⧉ etl` — it has no command; it points at another workflow.
3. **Save**, then **▶ Run**.

### 5.3 What happens at run time

When the run starts, dagron-api **inlines** `etl`’s tasks in place of `run-etl`,
namespaced under it, and rewires the edges:

```text
prepare → run-etl.build → run-etl.process → run-etl.publish → notify
```

So the run view shows the *expanded* DAG — `prepare`, the three `run-etl.*`
steps, and `notify` — and runs it as one graph. The stored workflow keeps the
compact `workflow_ref` form; only the run is expanded.

### 5.4 Rules & guard rails

- A task is **either** a `command` (leaf) **or** a `workflow_ref` (call), never
  both.
- The referenced workflow is resolved **by name** from your saved workflows; an
  unknown name fails the run with `references unknown workflow '<name>'`.
- Chains can **nest** (a child may chain its own children). Reference **cycles**
  (A → B → A, or A → A) are rejected with the offending chain in the message.
- Depth and total-task caps stop a runaway fan-out from exhausting memory.

> **Authoring tip:** create new `workflow_ref` steps in the **YAML** view (set
> `workflow_ref: <name>` and drop `command`). The visual editor renders chained
> nodes and lets you wire their dependencies, but the reference target is edited
> in YAML.

### 5.5 A sub-DAG in the same spec (`template:`) — DAG of DAGs

`workflow_ref` composes two **saved** workflows. A `template:` composes within
one spec: declare a reusable sub-DAG under `templates:`, then call it from a
task. Nothing else has to be saved first, so the workflow is self-contained —
this is the **“Start from an example → DAG of DAGs (template call)”** starter:

```yaml
name: dag-of-dags

templates:
  - name: etl
    tasks:
      - { name: build,   command: ["sh", "-c", "echo build"] }
      - { name: process, command: ["sh", "-c", "echo process"], depends_on: [build] }
      - { name: publish, command: ["sh", "-c", "echo publish"], depends_on: [process] }

tasks:
  - { name: prepare, command: ["sh", "-c", "echo prepare"] }
  - { name: run-etl, template: etl, depends_on: [prepare] }
  - { name: notify,  command: ["sh", "-c", "echo done"], depends_on: [run-etl] }
```

At run creation the engine expands the call in place, exactly like a chain:

```text
prepare → run-etl.build → run-etl.process → run-etl.publish → notify
```

**In the visual editor** `run-etl` renders as a dashed **sub-DAG** node showing
`⧉ etl · 3 tasks`. Select it and the panel gives you the call itself: a
**Calls template** picker over every declared template (with its task count),
and an **Arguments** row per parameter that template declares — defaults shown
as placeholders, so calling a template tells you what it takes. The template's
*internal* tasks are edited in the YAML view; the call is edited visually.

The **Blocks → Control flow → “Sub-DAG (template call)”** block scaffolds both
halves at once (a two-task template plus a call wired onto the current end of
the pipeline), since a call is only valid alongside the template it names.

**Rules.** Except for `type: approval` tasks (a human gate that runs no command),
a task is exactly one of `command` (leaf), `template` (call), or
`workflow_ref` (chain). A `template:` must name a template declared in the same
spec — an unknown name is rejected on **Save**, not at run time. Templates may
call other templates. A `workflow_ref` may not appear *inside* a template (the
chain expander only walks top-level tasks, so it would be silently dropped), and
a chained workflow may not declare its own `templates:` (inlining copies its
tasks, not its templates) — both are refused with a message saying so.

Parameters, fan-out (`with_items`), conditionals and recursion build on the same
mechanism; see [`../examples/templates/`](../examples/templates/README.md).
Those specs run fine, but they lock the Visual tab (§2) — the canvas cannot
honestly draw a task that becomes N tasks.

---

## 6. Schedule it

In a saved workflow, open the **Schedules** drawer and add a cron expression
(engine’s 6/7-field form, `sec min hour dom mon dow [year]` — e.g. `0 0 * * * *`
= top of every hour). The engine fires it on the HA leader. Enable/disable or
delete schedules from the same drawer.

---

## 7. Version it in Git (reverse sync)

Click **Sync to Git** to save the current spec and **open a pull request** that
commits it to your configured repo (`dags/<name>.yaml`). The PR is review +
history; merging it does not redeploy by itself. Configure `GITHUB_TOKEN` /
`GIT_REPO` on dagron-api first — until then the button returns a clear “not
configured” error.

---

## 8. Spec reference (quick)

```yaml
name: <workflow-name>           # required
run_timeout_secs: 3600          # optional: auto-terminate the whole run (wall clock)
result_from: <task-name>        # optional: that task's output becomes the run result
templates:                      # optional: reusable sub-DAGs (§5.5)
  - name: <template-name>
    parameters: { key: default }  # optional defaults, overridable per call
    tasks: [ … ]                  # the sub-DAG, same task shape as below
tasks:
  - name: <task-name>           # required, unique
    # exactly one of:
    command: ["sh", "-c", "…"]  #   leaf: argv to execute
    template: <template-name>   #   call: expand a `templates:` sub-DAG here
    arguments: { key: value }   #     (with `template`) fills its parameters
    workflow_ref: <other-name>  #   chain: run another saved workflow here
    depends_on: [<task>, …]     # edges; must be acyclic
    docker_image: alpine:3.20   # container to run in (leaf only; empty = host)
    trigger_rule: all_success   # or all_done | one_failed | all_failed | none_failed
    max_attempts: 1             # retries (leaf only)
    retry_delay_secs: 0         # backoff base (leaf only)
    retry_max_delay_secs: 300   # clamp on the exponential backoff (leaf only)
    timeout_secs: 25            # per-task timeout (leaf only)
  - name: <gate-name>           # human approval gate: no command/ref
    type: approval
    approval_timeout_secs: 3600 # optional deadline…
    approval_on_timeout: reject # …and what it does: reject (default) | approve
```

Ready-to-load specs live in [`../examples/ui/`](../examples/ui/) and are the same
ones behind the “Start from an example” menu. The richer template patterns —
parameters, fan-out, conditionals, recursion — live in
[`../examples/templates/`](../examples/templates/README.md); they save and run
through this API too, but only the plain call (§5.5) is *visually* editable.

---

## 9. Troubleshooting

| Symptom | Cause / fix |
|---|---|
| `references unknown workflow 'X'` on Run | Save workflow `X` first (chained children must exist). |
| `workflow reference cycle: …` | A chain loops back on itself; break the cycle. |
| `task '…' needs a command (leaf), a template (sub-DAG call) or a workflow_ref (chain)` | A task needs exactly one of the three. |
| `task '…' calls unknown template 'X'` | Declare `X` under `templates:` (names are spec-local). |
| Visual tab is disabled | The spec uses features the visual editor can't represent; the banner lists them. Edit it in YAML (§2). |
| Visual tab says “Can’t render graph” | The YAML has a structural error; the message points at it. Fix it in YAML. |
| Sync to Git returns “not configured” | Set `GITHUB_TOKEN` and `GIT_REPO` on dagron-api (§7). |
| Run stays `pending` | The engine container isn’t running, or `EXECUTOR` can’t run the command; check `podman compose logs engine`. |
