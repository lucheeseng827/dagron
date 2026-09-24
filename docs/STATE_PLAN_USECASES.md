# State plan use cases — Analytics Engineering, Data Platform, DS/ML, SRE

> How dagron's `state` surface (`dagron-state` + the console's **State plans** tab)
> solves real "what does this SQL change actually rebuild" problems.
> 27 cases across five roles, each mapped to the exact mechanism.

A **state plan** is "given a SQL change, rebuild the minimal set of models — and
nothing else." It is the `state` noun, not `backfill`: no cron, no time window, no
`backfills` table. Input is a *state diff* (current SQL fingerprints vs. the last
committed snapshot); output is a topologically ordered set of models that becomes
one ordinary dagron **run**.

Companion docs: [`BACKFILL_USECASES.md`](BACKFILL_USECASES.md) (the time-window
sibling), [`crates/dagron-state/README.md`](../crates/dagron-state/README.md) (the
component) — which also covers why the coupling is the wire and not the linker.

---

## 0. Where the plan JSON actually comes from

This is the question every integration hits first, so it comes before the cases.

**dagron never computes a plan.** It has no SQL, no project graph, and no
fingerprints — by design: the coupling is the wire, not the linker, so
the planner stays orchestrator-agnostic. The plan is always *inbound*, produced by
`freshet` against a checkout:

```sh
freshet plan --project ./models --state .planner/state.json --json > plan.json
```

Three inputs decide the answer:

| Input | What it is | Missing → |
|---|---|---|
| `--project ./models` | every `*.sql` is a model (name = file stem), plus optional `sources.json` and `materializations.json` | no models, empty plan |
| `--state <file>` | the prior snapshot: `{"fingerprints": {model: u64}, "watermarks": {…}}` | **cold plan — everything rebuilds** |
| the working tree itself | current SQL, fingerprinted AST-canonically | — |

### The state file is the whole ballgame

`.planner/state.json` is the **only** persistent state in the planner, and it is
written **only** by `plan --commit`. In CI that makes state placement an
architectural decision, not a detail:

| Where you keep it | How | Trade-off |
|---|---|---|
| **Object store (S3/GCS)** — recommended | pull before plan, push after a successful run | Durable and shared across runners. Needs a lock (or a single-writer deploy job) if merges can overlap. |
| **CI cache** (`actions/cache`, etc.) | key on the deploy target, not the branch | Zero infra. Eviction is safe (one cold plan) but silently costs you a full rebuild. |
| **Committed to git** | bot commit on the deploy job | Auditable and reviewable in-band. Commit noise, and concurrent merges race. |
| **Nowhere** | ephemeral runner | Every plan is cold. State plans buy you **nothing** — you have re-invented `dbt run`. |

The asymmetry that drives all of this:

- **Losing state is safe.** One full cold plan. Over-rebuild wastes money, not correctness.
- **Committing early is not safe.** State claims work that never happened, the next
  plan under-rebuilds, and you ship stale marts. This is the exact bug class the
  tool exists to prevent.

So the state file must track **what is actually materialized in the warehouse**,
never what is merely present in a branch. Practically: one state file per deploy
target (prod, staging), advanced only by the post-merge job that really ran.

### The loop, and the part dagron does not close

```
freshet plan --json ──► dagron-state ──► POST /plans/submit ──► run_id
                                                                  │
                                          GET /api/runs/{id}/wait │  (long-poll, ≤600s)
                                                                  ▼
                                                            succeeded?
                                                                  │ yes
                                             freshet plan --commit ◄┘
```

`PlanResponse` carries a `next_state` snapshot, and `dagron-state` passes it
through **opaquely — it never acts on it** (`crates/dagron-state/src/wire.rs:29`).
dagron has no business owning your fingerprints, and it does not. **Closing the
loop is the CI author's job:** wait for the run to reach terminal success, and only
then advance state. Nothing in dagron will do this for you, and no error tells you
that you forgot.

### Reference CI shape

```yaml
# PR job — read-only. Never commits state.
- run: aws s3 cp s3://$BUCKET/state/prod.json .planner/state.json || true
- run: freshet plan --project ./models --state .planner/state.json --json > plan.json
- run: dagron-state explain plan.json > plan.md        # S4 — PR comment body
- run: dagron-state explain plan.json --exit-code      # S5 — exit 2 = "this PR rebuilds something"

# Deploy job — the only writer of state.
- run: |
    RUN_ID=$(jq '{plan: ., options: {command_template: ["sh","-c","dbt run --select {{ model }}"]}}' plan.json \
      | curl -sS -X POST "$DAGRON/api/state/plans/submit" \
          -H 'content-type: application/json' -H "authorization: Bearer $DAGRON_TOKEN" \
          -d @- | jq -r .run_id)
    STATUS=$(curl -sS "$DAGRON/api/runs/$RUN_ID/wait?timeout_secs=600" \
          -H "authorization: Bearer $DAGRON_TOKEN" | jq -r .status)
    [ "$STATUS" = "succeeded" ] || exit 1
- run: freshet plan --project ./models --state .planner/state.json --commit   # S12 — only now
- run: aws s3 cp .planner/state.json s3://$BUCKET/state/prod.json
```

An empty plan returns **422**, not 400 — "nothing to rebuild" is the planner's
*good* outcome and a per-commit caller hits it constantly. Handle it as success.

---

## Mechanism cheat-sheet

The cases below reference these by name.

| # | Mechanism | What it is | Surface |
|---|---|---|---|
| **S1** | **Cold plan** | No committed state → every model is `directly_changed`. Safe, expensive. | `freshet plan` |
| **S2** | **Incremental plan** | Fingerprint diff vs. the committed snapshot → only what moved. | `freshet plan` |
| **S3** | **Column-level pruning** | A downstream model rebuilds only if it consumes a column that *actually changed* — not merely because it sits downstream. This is the product. | `freshet-core` |
| **S4** | **Explain (read-only)** | Summary, per-model rows (why / unit / waits-on), markdown + Mermaid. Touches no DB, no network, no dagron. | `dagron-state explain`, `POST /plans/explain`, console |
| **S5** | **CI gate** | `--exit-code` returns **2** when the plan is non-empty (`git diff` shape: 0 ok, 1 error, 2 non-empty). | `dagron-state explain --exit-code` |
| **S6** | **Compile-only** | Plan → dagron workflow YAML. Submits nothing. | `dagron-state plan`, `POST /plans` |
| **S7** | **Submit** | Compile, then create a run through the host's *same* parse → expand → validate path as `POST /api/runs`. A plan cannot enter by a laxer door than hand-written YAML. | `POST /plans/submit` |
| **S8** | **Ordering choice** | `derived` (parallel; exact **only** when you supply `graph`) vs `sequential` (chain in plan order; always correct). | `options.ordering`, console checkbox |
| **S9** | **Command template** | Per-model argv as `sh -c`, with `{{ model }}`, `{{ unit }}`, `{{ partitions }}` (comma-joined). Required — the component refuses to guess one. | `options.command_template` |
| **S10** | **Partition-scoped unit** | `unit: partitions[…]` instead of `full_model`, for models hinted `incremental` in `materializations.json`. Un-hinted models widen to full — deliberately, because over-rebuild is safe. | `materializations.json` |
| **S11** | **Restatement** | Replan specific partitions of one model, ignoring fingerprints entirely. Never reads or writes state. | `freshet restate --model --partitions` |
| **S12** | **Commit discipline** | `--commit` advances the snapshot. Only after the run actually succeeded. | `freshet plan --commit` |
| **S13** | **Empty plan → 422** | Well-formed request, nothing to rebuild, no run to make of it. Not a client error. | all submit paths |
| **S14** | **Console review gate** | Paste → Explain → Submit. Submit is disabled until Explain succeeds, and *any* edit clears the explanation — you cannot run a plan nobody read. | `/state` tab |
| **S15** | **Traceability** | The sanitized dagron task name is not a reliable key back to the model, so each task's `input` preserves the original name, the reason, and the partitions. | compiled task `input` |

---

## Analytics Engineer (6)

*Writes the SQL. The primary user.*

### AE-1 — "I changed one CASE statement. What breaks?"
**Pain:** you edited `stg_orders.sql`; the project has 400 models and you have no
idea what depends on the column you touched.
**dagron:** **S2** + **S3** + **S4**. Plan, then explain — the table names each
model, whether it is `directly changed` or `downstream`, **which upstream it was
blamed on, and via which columns**. `mart_revenue` appears because it consumes
`amount`; `mart_sessions`, which only reads `order_id`, does not.
```sh
freshet plan --project ./models --state .planner/state.json --json | dagron-state explain -
```

### AE-2 — Cosmetic edit that should rebuild nothing
**Pain:** you reformatted SQL / renamed an alias and don't want a 40-minute rebuild.
**dagron:** **S2**. Fingerprints are **AST-canonical**, so whitespace, comments and
formatting do not move the hash. The plan comes back empty → **S13** 422 → the
console shows *"Nothing to rebuild — every model matches its committed state."*
**Caveat:** if that model's SQL fails to parse, the fingerprint falls back to the
v1 text scheme and cosmetic edits **will** trigger rebuilds. `freshet plan` says so
on stderr (`N model(s) fell back to the text scheme`). Fix the SQL if it should parse.

### AE-3 — Review the blast radius before merging
**Pain:** your reviewer asks "what does this touch?" and the honest answer is a shrug.
**dagron:** **S4**. `dagron-state explain` emits markdown **and** Mermaid; GitHub
renders both natively in a PR comment. This is the `dbt state explain` surface, and
structurally the same thing `dagron-plan` already does for workflow changes.

### AE-4 — Run just the plan, from the browser, without curl
**Pain:** you have `plan.json` from a colleague and no CI wiring.
**dagron:** **S14**. Console → **State plans** → paste the planner's `--json` output
(the page wraps it in the `{plan, options}` envelope for you) → Explain → read the
table → Submit as run. The paste path accepts the CLI output unwrapped, so there is
nothing to hand-edit.

### AE-5 — Partition-scoped rebuild instead of a full table
**Pain:** `fct_events` is 4 TB; a one-day change should not rebuild all of it.
**dagron:** **S10** + **S9**. Hint it `incremental` in `materializations.json`, and
the plan comes back `unit: partitions["2026-09-07"]`. Template the partitions into
the command:
```json
{"command_template": ["sh","-c","dbt run --select {{ model }} --vars '{days: \"{{ partitions }}\"}'"]}
```

### AE-6 — Force a rebuild the fingerprints won't ask for
**Pain:** an upstream *source table* was corrected. No SQL changed, so no fingerprint
moved, so **S2** correctly plans nothing — but the data is wrong.
**dagron:** **S11**. `freshet restate` ignores fingerprint state entirely and never
touches it, so it is safe to run from anywhere:
```sh
freshet restate --project ./models --model fct_events --partitions 2026-09-01,2026-09-02 --json \
  | dagron-state explain -
```

---

## Data Platform / CI-CD Engineer (7)

*Owns the pipeline, the state file, and the loop. The role with the real decisions.*

### DP-1 — Decide where state lives
**Pain:** the first integration always puts `state.json` on an ephemeral runner and
quietly gets a cold rebuild on every commit.
**dagron:** §0. OSS ships only `FileStateBackend` (a JSON file) and
`MemoryStateBackend` — persistence is deliberately yours. Pick from the placement
table; key the file to the **deploy target**, never the branch.

### DP-2 — Close the plan → run → commit loop
**Pain:** you submitted the plan and committed state in the same job; a failed run
left state claiming work that never happened, and the next deploy under-rebuilt.
**dagron:** **S7** + **S12**. Submit, then `GET /api/runs/{id}/wait?timeout_secs=600`
to terminal, and gate `--commit` on `succeeded`. See the reference shape in §0. This
is the single most important thing to get right, and nothing warns you.

### DP-3 — Gate a PR on "this change rebuilds something"
**Pain:** you want CI to flag data-affecting PRs without blocking cosmetic ones.
**dagron:** **S5**. `dagron-state explain plan.json --exit-code` returns 2 when the
plan is non-empty. Branch on it: post the **S4** markdown as a comment, request a
data-owner review, whatever your policy is. Exit 0 means nothing rebuilds — merge freely.

### DP-4 — Plan on a PR branch without corrupting prod state
**Pain:** PR jobs running `--commit` would advance prod's snapshot for work that was
never deployed.
**dagron:** **S12** discipline as a job-shape rule: PR jobs **read** the prod state
file and never pass `--commit`; exactly one post-merge job writes it. Planning is
side-effect free by default — you have to opt into the write.

### DP-5 — Concurrent merges racing on one state file
**Pain:** two deploys finish out of order; the loser's `--commit` overwrites the
winner's snapshot and a model silently never rebuilds.
**dagron:** serialize the writer. Use a deployment concurrency group (one in-flight
deploy job per target) or a lock around the S3 read-modify-write. **S1** is the
failure mode you *want* if you get this wrong — but only if you resolve conflicts by
**deleting** the state file (cold, safe), never by picking one arbitrarily.

### DP-6 — Guarantee plans can't bypass workflow validation
**Pain:** security asks whether a pasted plan is a way to inject arbitrary workflows.
**dagron:** **S7**. `ApiSubmitter` routes through `submit_yaml_with_params` — the
identical parse → expand → validate → budget path `POST /api/runs` uses
(`crates/dagron-api/src/state_plane.rs:51`). The component ships **no auth of its
own** by design; dagron layers `require_auth` over the mount, accepting exactly the
credentials every other mutating route accepts (session cookie or PAT) and no others.
A standalone deployment **must** supply its own — do not expose it open.

### DP-7 — Detect a planner contract drift before it reaches prod
**Pain:** you upgraded `freshet` and the plan JSON no longer deserializes.
**dagron:** `GET /api/state/contract` reports the wire revision this dagron build
reads (`planner-embed/1`). Assert it in CI against your planner version. The DTOs are
deliberately duplicated on the dagron side, guarded by `WIRE_CONTRACT_VERSION` plus a
fixture captured verbatim from the real CLI — if a contract test fails, the contract
moved; fix `wire.rs` and bump, **do not** edit the fixture to match the code.

---

## Data Scientist / ML Engineer (5)

### DS-1 — Feature table changed; retrain only what depends on it
**Pain:** you changed one feature's definition and don't know which downstream
training tables are now stale.
**dagron:** **S3** + **S4**. Column-level attribution tells you exactly which
feature tables consume the column you edited — not the whole downstream cone.

### DS-2 — Rebuild a feature backfill with a different command
**Pain:** your feature build is a Python job, not `dbt run`.
**dagron:** **S9**. The command template is any argv; the planner decides *which*
models and *in what order*, not *how* to build them.
```json
{"command_template": ["sh","-c","python -m features.build --model {{ model }} --unit {{ unit }}"]}
```

### DS-3 — Correctness matters more than wall-clock
**Pain:** your feature DAG has diamond dependencies and a partial parallel rebuild
would produce a subtly inconsistent training set.
**dagron:** **S8** `sequential`. A plan carries a **single-cause** `because_of`
attribution, not the project's edge set — a model downstream of both `a` and `b`
names only one of them, so derived edges can under-constrain and let it start too
early. Sequential chains the plan in topological order: no parallelism, always correct.

### DS-4 — Restate a specific date range after a labeling bug
**Pain:** 2026-05-01..05-07 have flipped labels; no SQL changed.
**dagron:** **S11**. `freshet restate --model train_labels --partitions …` — ignores
fingerprints, never advances state, so your normal incremental flow is unaffected.

### DS-5 — Reproduce which plan produced a given run
**Pain:** a training table looks wrong and you need to know what rebuilt it and why.
**dagron:** **S15**. Model names are sanitized into dagron task names
(`[A-Za-z0-9_-]`, collisions suffixed), so the task name is not a reliable key back.
Each task's `input` preserves the original model name, the reason it was in the plan,
and its partitions.

---

## SRE / Platform Operator (5)

### SRE-1 — A SQL change is about to rebuild 300 models at 09:00
**Pain:** you find out when the warehouse bill spikes.
**dagron:** **S4** before **S7**. Explain is read-only and free; the summary reports
the model count and, **when you pass `graph`**, the pruning percentage. The
percentage appears *only* with `graph` — the plan alone cannot see the models it did
not select, and claiming a saving it cannot measure would undercut the one number
worth reporting.

### SRE-2 — Cap the blast radius of a submitted plan
**Pain:** a compiled plan could expand into more tasks than the cluster should take.
**dagron:** **S7**. Because submission goes through the normal path, the engine's
`TaskBudgetExceeded` and `max_active_runs` apply unchanged — a plan that would create
too many tasks is rejected with a 400 that names the budget, and a workflow at its
concurrency cap gets a 429, not a 500.

### SRE-3 — Retry and timeout policy for model builds
**Pain:** a flaky warehouse connection fails one model and sinks the whole rebuild.
**dagron:** **S9** envelope options — `max_attempts`, `timeout_secs`, `env` are set
per task at compile time. Beyond that a state-plan run is an ordinary run, so
`POST /api/runs/{id}/rerun` resets only the failed cone and keeps succeeded models
(see `BACKFILL_USECASES.md` **M4**).

### SRE-4 — "Is this a backfill?" — routing the request correctly
**Pain:** someone asks for a "backfill" and means a schema change.
**dagron:** the noun table. Different input (time window vs. state diff), different
unit (cron fire-times vs. models), different object (a paced `backfills` job with a
cursor vs. a run), different endpoint. They are not merged, and the cost of two nouns
is paid down by naming.

| | `backfill` | `state` |
|---|---|---|
| Input | a time window `[from, to]` | a state diff (SQL vs. committed fingerprints) |
| Unit | cron fire-times | models, with column-level attribution |
| Object | a paced `backfills` job with a cursor | a run |
| Endpoint | `POST /api/backfills` | `POST /api/state/plans/submit` |

### SRE-5 — Run the explain surface with no dagron at all
**Pain:** you want the blast-radius check in a pre-merge hook on a laptop or an
air-gapped runner.
**dagron:** **S4** offline. `dagron-state` is a standalone binary that never talks to
a dagron — `freshet plan --json | dagron-state explain -` needs no network, no
database, and no running server.

---

## Reviewer / Tech Lead (4)

### RL-1 — Judge a PR's data impact without reading 40 SQL files
**dagron:** **S4** markdown in the PR body: model count, per-model reason, and the
Mermaid graph GitHub renders inline.

### RL-2 — Enforce "no unreviewed rebuild"
**dagron:** **S14**. In the console, Submit is gated on a successful Explain, and any
edit to the plan, the command, or the ordering checkbox **clears** the explanation
(`frontend/src/app/state/page.tsx:83`). Without that, someone could explain plan A,
edit to plan B, and submit while the table still shows A. The gate is the feature.

### RL-3 — Tell an over-rebuild from a correctness bug
**dagron:** **S1** vs **S2**. If every model reads `directly changed`, you have a cold
plan — no committed state, or a fingerprint-scheme change — not 400 broken models.
Commit once after a successful run and subsequent plans are minimal.

### RL-4 — Know what the tool cannot tell you
**dagron:** honest limits, all three worth stating in review:
- `derived` ordering is **best-effort** without an explicit `graph`, and the
  toolchain has **no first-class producer** for that edge map — `freshet graph`
  prints a flat topological list, not `{model: [upstreams]}`. Build it from your
  own manifest, or use **S8** `sequential`.
- Models not hinted `incremental` widen to `full_model`. Deliberate: over-rebuild is safe.
- The planner sees SQL, not the warehouse. A source-data correction moves no
  fingerprint — that is **S11**'s job, not **S2**'s.

---

## Picking the right mechanism

```text
Did the SQL change?
   …and you want to know what it costs?      → S4 explain (free, read-only)
   …and CI should flag it?                   → S5 --exit-code 2
   …and you want to run it?                  → S7 submit  (S14 if by hand)
Did the DATA change but not the SQL?         → S11 restate (fingerprints can't see it)
Did a scheduled run never fire?              → not this surface: BACKFILL_USECASES.md M1/M2
Is every model coming back "directly changed"? → S1 cold plan: no state, or state lost
Is nothing coming back at all?               → S13 422 — the good outcome
Correctness-critical, no explicit graph?     → S8 sequential
Model too big to rebuild whole?              → S10 materializations.json → incremental
Run finished — now what?                     → S12 commit, and ONLY now
```

**Golden rule:** *planning is free and side-effect free; committing is neither.*
Explain as often as you like — it touches nothing. Advance state **only** after the
run it describes actually succeeded, because losing state costs one over-rebuild
while committing early costs you a silently stale warehouse.
