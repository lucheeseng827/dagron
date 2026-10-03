# Loops — repeating a step, and repeating the whole DAG

> Everything here is authored from the **console's visual editor** — the loop
> controls write the YAML below, so nothing on this page needs you to open the
> YAML tab. The page exists to say what those controls emit and where each
> mechanism stops — including
> [iterating over a task's output](#iterating-over-a-tasks-output), which
> resolves while the run is going rather than before it starts.

dagron has no run-level "repeat" field, because it has no run-level loop. It
has **three** loop mechanisms, at the task level, and they are not
interchangeable. Two of them differ in *shape* — many rows at once, or one row
many times:

```text
  fan-out (with_items / with_param)        repeat (repeat: { until, … })
  resolved by the EXPANDER                 evaluated by the ENGINE
  at run-creation time                     after each success
┌─────────────────────────────┐          ┌─────────────────────────────┐
│  process                    │          │  poll                       │
│    ↓ expands to             │          │    ↓ re-runs in place       │
│  process.0  process.1  …    │          │  poll (attempt 1, 2, 3 …)   │
│  N rows, ALL AT ONCE        │          │  ONE row, ONE AFTER ANOTHER │
│  N known before anything ran│          │  N not known until it stops │
└─────────────────────────────┘          └─────────────────────────────┘
```

The third, [`with_output_of:`](#iterating-over-a-tasks-output), has the
fan-out's shape but is resolved **while the run is going**, by a sweep that
reads an upstream task's output — so its row count is not known at submit
either.

Pick by the question you are answering:

| You want | Use | Console control |
|---|---|---|
| The same step over 5 shards, in parallel | `with_items:` / `with_param:` | Loop → **For each item** |
| The same step 5 times, one after another | `repeat:` counting `{{ attempt }}` | Loop → **Repeat N times** |
| Keep polling until something is ready | `repeat:` with your own `until` | Loop → **Repeat until** |
| The same step once per thing an earlier step *printed* | `with_output_of:` | Loop → **For each item** → an earlier step's output |
| …and once per thing *each copy* of an earlier fan-out printed | `with_output_of:` over a fanned-out producer | the same control — the union is automatic |
| The **whole graph** N times | a `templates:` sub-DAG + a call that loops it | **⟳ Repeat whole workflow** |

## Looping one step

The Loop section in the task panel writes these. The node then carries a loop
badge (`⟳ ×4 parallel`, `⟳ 3× in place`) — a looping step is one node standing
for several rows, and the badge is what keeps the canvas honest about it.

### For each item — N in parallel

```yaml
  - name: process
    command: ["echo", "processing {{ item }}"]
    with_items: [1, 2, 3, 4]        # "a count" — {{ item }} is the pass number
```

Four rows named `process.0` … `process.3`, all runnable at once. The items can
also be a literal list (`["a", "b"]`, or objects, where `{{ item.key }}`
addresses a field), or a **workflow parameter** holding a JSON array:

```yaml
  - name: sync
    command: ["sync", "--region", "{{ item.region }}"]
    with_items:
      - { region: us-east-1 }
      - { region: eu-west-2 }
    instance_key: "{{ item.region }}"   # names them sync.us-east-1, sync.eu-west-2
```

`instance_key` ("Name each copy by") is what turns `sync.0` into
`sync.us-east-1` in the run view and the logs. Labels are sanitized to
`[A-Za-z0-9_-]` and must be unique within the fan-out.

Fan-out is resolved when the **run is created**, so the row count is fixed
before anything executes — which is what lets the budget refuse a runaway
fan-out at submit instead of halfway through. It also means the item list
cannot depend on anything the run itself produces; see
[below](#what-is-not-here-iterating-over-a-tasks-output).

### Repeat N times — N in a row

`RepeatSpec` has no count field, so a fixed number of passes is expressed by
counting the engine's own iteration counter:

```yaml
  - name: pass
    command: ["sh", "-c", "echo pass"]
    repeat: { until: "{{ attempt }} == 3", max_iterations: 3 }
```

One row, run three times, each pass starting after the last one succeeded.
Each pass's output is kept — the console's task panel offers **"N earlier
attempts"** beside the log pane, because the log itself shows only the attempt
currently on the row ([`ITERATION-LOGS.md`](ITERATION-LOGS.md)).
`{{ attempt }}` is the 1-based pass number, and it belongs in `until` and
nowhere else: commands are substituted once, at expansion, where `attempt` is
not bound — it is runtime state the engine binds only when evaluating `until`
after each success. Put it in a command and every pass prints the literal
`{{ attempt }}`. A step that needs its own number wants a fan-out, where
`{{ item }}` is substituted per instance.

The console reads this exact shape back as "Repeat N times"; anything else you
write in `until` reads back as "Repeat until".

### Repeat until — poll

```yaml
  - name: poll
    command: ["sh", "-c", "check-status"]
    repeat: { until: "{{ output }} == done", max_iterations: 30, delay_secs: 10 }
```

`until` is checked after each success against `{{ output }}` (the task's
trimmed stdout) and `{{ attempt }}`. **Running out of iterations fails the
task** — a condition that never came true is an error, not a quiet success. The
task holds no worker while it waits out `delay_secs`.

`repeat:` applies to command tasks and `type: workflow` triggers. On an approval
gate or a wait sensor it is rejected, and on a `template:` call it is rejected
too — a call is replaced by the template's tasks during expansion, so the loop
would have no row to attach to. (That rejection replaced a silent drop: the
workflow submitted cleanly and ran its body exactly once.)

> **The one loop on this page the console cannot write.** A `type: workflow`
> trigger locks Visual mode outright — it is a child-run step, not a command,
> so the panel that would edit it has no honest fields to show
> ([`spec-support.ts`](../frontend/src/lib/spec-support.ts)). Looping one is a
> YAML edit, and the Loop control offers **Repeat N times** and **Repeat
> until** on leaf steps only. Everything else here is authored from the
> editor.

## Looping the whole workflow

**⟳ Repeat whole workflow** above the canvas. There is no run-level loop field,
so ticking it moves your graph into a `templates:` sub-DAG and adds one task
that calls it N times. The canvas keeps drawing **your** steps, not the wrapper
— the loop lives in the bar, and "Remove loop" puts everything back.

The bar offers two shapes, and the difference is the whole point of the choice:

### all at once — N independent copies

```yaml
templates:
  - name: loop-body
    parameters: { pass: "1" }
    tasks:
      - { name: prepare, command: ["echo", "prepare"] }
      - { name: process, command: ["echo", "process"], depends_on: [prepare] }
tasks:
  - name: loop-pass
    template: loop-body
    arguments: { pass: "{{ item }}" }
    with_items: [1, 2, 3]
```

Three copies of the whole graph in **one run** — `loop-pass.0.prepare`,
`loop-pass.1.prepare`, … — with nothing ordering them. Each copy knows its
number as `{{ pass }}`. This is the load-test / N-shards shape.

### one after another — N sequential passes

```yaml
templates:
  - name: loop-body
    parameters: { pass: "1", passes: "1" }
    tasks:
      - { name: prepare, command: ["echo", "prepare"] }
      - { name: process, command: ["echo", "process"], depends_on: [prepare] }
      - name: loop-next-pass
        template: loop-body
        arguments: { pass: "{{ pass + 1 }}", passes: "{{ passes }}" }
        depends_on: [process]
        when: "{{ passes }} > {{ pass }}"
tasks:
  - name: loop-pass
    template: loop-body
    arguments: { pass: "1", passes: "3" }
```

The body ends by calling itself with the pass counter bumped, guarded by a
`when:` base case — the expander's recursion, unrolled at run creation into one
flat DAG where pass *k+1* depends on pass *k*'s last task. `loop-next-pass` is
plumbing: the console hides it on the canvas and rebuilds its `depends_on`
whenever you add a step, so the next pass always waits for the real end of the
graph.

Both shapes are ordinary specs. A workflow hand-written in either shape is
picked up by the console as a loop, provided the top level is exactly the one
`loop-pass` call — the editor claims a spec by its whole shape, so a workflow
that merely contains a template named `loop-body` is left alone.

The wrapper *owns* that name, though: it writes its own template there. A
workflow that already declares a `loop-body` of its own cannot be wrapped, and
the control says so rather than overwriting it — rename the template first.

### Limits

| | Ceiling | Why |
|---|---|---|
| Whole-workflow passes, **either mode** | **50** | Sequential passes are levels of template recursion and the expander's depth cap is 64. Parallel ones are bounded instead by the task budget — far higher — but this control multiplies the *whole graph*, so 500 passes over a 20-task workflow would be 10,000 rows from one click. The smaller number governs both, so switching mode never silently re-clamps the count. |
| Step-level `for each` copies | **500** | The multiplier here is one task, not the graph. A guard rail under the run-wide task budget (`DAGRON_MAX_TASKS_PER_RUN`, 100k), so one control can't spend it all. |
| `repeat` iterations | **1000** | The loop holds a task row `running` for its whole life. |
| `with_output_of` instances | the run's remaining task budget | The only fan-out with no ceiling of its own: the count comes from data, so there is no field for the console to clamp. Bounded instead by `DAGRON_MAX_TASKS_PER_RUN`, re-checked at insert time — see [above](#what-it-costs). |

A whole-workflow loop multiplies the task count: 3 passes over a 20-task graph
is 60 rows, counted against the budget **before** the run is created
([`budget:`](../README.md)), so an over-large loop is refused at submit rather
than killed halfway.

## Iterating over a task's output

The third fan-out, and the one that needed an engine change rather than a
control:

> *"Run a task that lists the partitions, then run the next step once per
> partition."*

```yaml
  - name: list-partitions
    command: ["sh", "-c", "echo '[\"2026-01\", \"2026-02\", \"2026-03\"]'"]
  - name: process
    command: ["handle", "{{ item }}"]
    depends_on: [list-partitions]        # required — see below
    with_output_of: list-partitions
    instance_key: "{{ item }}"           # names them process.2026-01, …
```

Console: Loop → **For each item** → Items from → **An earlier step's output**.
The picker is enabled once the step depends on something, and offers exactly
the steps it depends on.

The producer prints a **JSON array** on stdout. Each element becomes one task,
with the element bound to `{{ item }}` (and `{{ item.key }}` for objects) —
the same bindings `with_items:` gives, because it is the same `{{ item }}`.

### How it differs from the other two

`with_items:` and `with_param:` are resolved by the expander before the run
exists. This one cannot be, so the mechanism is different in a way that shows:

```text
  with_items / with_param                  with_output_of
  resolved by the EXPANDER                 resolved by a RECONCILE SWEEP
  at run-creation time                     while the run is going
┌─────────────────────────────┐          ┌─────────────────────────────┐
│  process                    │          │  process        (one row)    │
│    ↓ expands to             │          │    ↓ parks, waiting          │
│  process.0  process.1  …    │          │  producer prints ["a","b"]   │
│  N rows before anything ran │          │    ↓ sweep inserts           │
│  N counted against budget:  │          │  process.a  process.b        │
│  at submit                  │          │  + process stays as the join │
└─────────────────────────────┘          └─────────────────────────────┘
```

The authored task becomes **one row that never executes**. When its
dependencies are satisfied it parks — `status: running` holding no worker, the
same shape a wait sensor or a sub-workflow trigger uses — and the sweep reads
the producer's output and inserts the instances.

That row then stays, as the **join point**. Anything that depended on `process`
still depends on `process`, and `process` now waits for its own instances. It
is why `trigger_rule:` and `allow_failure:` keep meaning what they meant, and
why nothing downstream is re-parented while the scheduler is reading the graph.
Its own output is a summary — `{"instances":3,"succeeded":3,"failed":0,"skipped":0}`
— rather than the instances' concatenated stdout, which is on each instance's
own row where it belongs.

### The rules, and why each one is there

| Rule | Why |
|---|---|
| The producer must be in `depends_on` | The instance count is read from its output. A producer this step does not wait for would make the count depend on scheduling order. Same rule a runtime `when:` output reference carries. |
| One fan-out source per task | `with_items`/`with_param` resolve at submit, this resolves mid-run; a task setting both would be half-expanded in each place. |
| Not on a `template:` call | A call is replaced by the template's tasks during expansion and its own fields go with it — there would be no row to park. |
| Not on a `gang:` | A gang is claimed all-or-nothing, which requires knowing its size before it is claimed. |
| Command tasks only | There has to be a command to substitute `{{ item }}` into. |
| Output must be a JSON array | Anything else fails the task with a message naming what it got. Silently fanning out over nothing looks like success. |
| An empty array **succeeds** | Unlike `with_items: []`, which is an authoring mistake refused at submit. At run time "there were no partitions" is a result, so the step succeeds with zero instances and its dependents run. |

### What it costs

This is the one thing in dagron that makes a run **bigger after admission**.
Everywhere else a run's task count is fixed before the run exists, which is
exactly what lets `budget:` refuse a fan-out blow-up at submit. So the ceiling
is re-checked at insert time: a runtime fan-out that would push the run past
`DAGRON_MAX_TASKS_PER_RUN` fails the task with that message, before inserting
anything, rather than inserting the rows and finding out.

Two consequences worth planning for:

* `budget:` on the workflow is checked against the **expanded** graph at submit,
  where a runtime fan-out still counts as one task. An author who knows the
  producer returns ~200 rows should budget for them. A chained fan-out
  multiplies: N producer instances each returning M items is N×M rows from two
  authored tasks.
* A run's task list grows while you are looking at it. The run view builds its
  graph from the datastore on each poll, so the instances appear on their own.

### Chaining: a producer that is itself fanned out

`with_output_of:` names an **authored** task, and the producer may be one that
expansion has already turned into many rows. Each instance prints its own list
and the consumer fans out over the **union**:

```yaml
  - name: regions
    command: ["list-partitions", "{{ item }}"]
    with_items: ["us", "eu"]          # → regions.0, regions.1
  - name: process
    command: ["handle", "{{ item }}"]
    depends_on: [regions]             # → depends on regions.0 AND regions.1
    with_output_of: regions
```

`regions.0` prints `["a","b"]`, `regions.1` prints `["c"]`, and `process` fans
out into three: `process.0` … `process.2`, carrying `a`, `b`, `c` in that
order. Anything downstream still depends on `process` alone and never has to
know there were three.

Which rows get read is decided by the consumer's **own dependencies**, not by
matching names against the run. `depends_on: [regions]` is rewired by expansion
onto exactly `regions.0` and `regions.1`, so the dependency list already names
the producer's rows — a task this one does not depend on is unreachable even in
principle, and there is no prefix to collide with.

That also settles the order: dependency order is instance order, so `regions.0`'s
items come before `regions.1`'s. A string sort would put `regions.10` before
`regions.2` and pair item lists with the wrong region without ever looking wrong.

The same rule reaches a producer inside a `templates:` sub-DAG, whose rows are
named `<call>.<task>`:

```yaml
  - { name: discover, template: finder }
  - name: process
    command: ["handle", "{{ item }}"]
    depends_on: [discover]
    with_output_of: discover          # reads the call's EXIT tasks
```

`depends_on: [discover]` means the call's exits, so that is what is read. With
one exit this is simply "the sub-DAG's result". With several, their lists are
concatenated — which is worth knowing before pointing `with_output_of` at a
template whose exits are unrelated tasks.

An instance that prints nothing contributes nothing, so one region with no
partitions costs its share and no more; every producer empty leaves the barrier
to succeed with zero instances, exactly as a single empty producer does. An
instance that prints something that is **not** a list fails the fan-out, and
the message names *that row* — `regions.1`, not `regions`, which would send you
to check all of them.

The two things that read a task's output at run time *besides* this one:

* `when: "{{ tasks.check.output }} == go"` — a **runtime gate**. It chooses
  whether a task runs, not how many of it there are.
* `repeat: { until: "{{ output }} == done" }` — a loop over **one task's own**
  output.

## See also

* [`ITERATION-LOGS.md`](ITERATION-LOGS.md) — why a loop's log used to show one
  iteration, what retaining all of them costs, and the bound that makes it
  affordable.
* [`HOWTO.md`](HOWTO.md) — copy-paste recipes.
* [`examples/templates/`](../examples/templates/) — the expander's own fan-out,
  recursion and DAG-of-DAGs examples.
* `frontend/scripts/check-loops.mjs` (`npm run check:loops`) — proves the
  console writes these shapes; `loops_the_console_writes_expand` in
  `crates/dagron-core/src/expand.rs` holds the same YAML against the real
  expander. They are a pair: change one and change the other.
