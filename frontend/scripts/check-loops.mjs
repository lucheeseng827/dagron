#!/usr/bin/env node
// Hold the loop editor to the spec it claims to write.
//
//     npm run check:loops
//
// Every loop the console can express is built through the real modules, dumped
// to YAML, and read back — because the whole feature is a claim about YAML the
// *engine* will accept, and the editor is the only thing that writes it. Two
// properties matter and both are checked here:
//
//   1. **Round-trip.** What `writeLoop` / `wrapWorkflowLoop` emit must read back
//      as the same loop. A loop that doesn't survive a reload is a control that
//      silently resets, and the YAML tab is the reload.
//   2. **Shape.** The emitted keys have to be the ones the engine reads
//      (`with_items`, `with_param`, `repeat.until`, …). A typo here is a spec
//      that saves, looks right on the canvas, and runs once.
//
// The matching engine-side test is `loops_the_console_writes_expand` in
// `crates/dagron-core/src/expand.rs`: it holds the *same* YAML against the real
// expander, so this file proves the console writes it and that one proves the
// engine runs it. Keep the two in step — if a shape changes here, that test's
// fixture has to change with it.
//
// No test framework, matching `check-recipe-vectors.mjs`: Node strips the types
// and the assertions are `assert`.

import assert from "node:assert/strict";
import { register } from "node:module";

register("./alias-hook.mjs", import.meta.url);

const { modelToYaml, parseModel } = await import("@/lib/spec-model");
const { readLoop, writeLoop, loopError, NO_LOOP } = await import("@/lib/loop-model");
const { readWorkflowLoop, wrapWorkflowLoop, unwrapWorkflowLoop, loopBodyModel, applyLoopBody } =
  await import("@/lib/workflow-loop");

let checks = 0;
const dump = process.argv.includes("--print");

/// A two-task starter graph, the shape the editor's own SAMPLE has.
const base = () => ({
  name: "my-workflow",
  templates: [],
  tasks: [
    { name: "prepare", command: ["echo", "prepare"], depends_on: [] },
    { name: "process", command: ["echo", "process"], depends_on: ["prepare"] },
  ],
});

/// Emit a model as YAML and parse it back, exactly as switching editor tabs
/// does. Returns the reparsed model, failing loudly if the YAML doesn't parse.
function roundTrip(model, label) {
  const yaml = modelToYaml(model);
  if (dump) console.log(`\n--- ${label} ---\n${yaml}`);
  const { model: back, error } = parseModel(yaml);
  assert.ok(back, `${label}: emitted YAML did not parse back: ${error}`);
  return { back, yaml };
}

// ── step-level loops ────────────────────────────────────────────────────────

/// Each case: the loop to write, and the task keys it must produce.
const STEP_CASES = [
  {
    label: "foreach/count",
    loop: { ...NO_LOOP, kind: "foreach", source: "count", count: 4 },
    expect: (t) => {
      assert.deepEqual(t.with_items, [1, 2, 3, 4], "count fans out over 1..N");
      assert.equal(t.repeat, undefined, "a fan-out is not a repeat");
    },
  },
  {
    label: "foreach/list + instance_key",
    loop: {
      ...NO_LOOP,
      kind: "foreach",
      source: "list",
      items: '[{"region": "us-east-1"}, {"region": "eu-west-2"}]',
      label: "{{ item.region }}",
    },
    expect: (t) => {
      assert.deepEqual(t.with_items, [{ region: "us-east-1" }, { region: "eu-west-2" }]);
      assert.equal(t.instance_key, "{{ item.region }}");
    },
  },
  {
    label: "foreach/param",
    loop: { ...NO_LOOP, kind: "foreach", source: "param", param: "{{ shards }}" },
    expect: (t) => {
      assert.equal(t.with_param, "{{ shards }}");
      assert.equal(t.with_items, undefined, "with_items and with_param are exclusive");
    },
  },
  {
    label: "repeat/count",
    loop: { ...NO_LOOP, kind: "repeat", count: 5, delaySecs: 2 },
    expect: (t) => {
      assert.equal(t.repeat.until, "{{ attempt }} == 5", "a count loop counts attempts");
      assert.equal(t.repeat.max_iterations, 5, "the budget matches the count");
      assert.equal(t.repeat.delay_secs, 2);
      assert.equal(t.with_items, undefined);
    },
  },
  {
    label: "until/poll",
    loop: { ...NO_LOOP, kind: "until", until: "{{ output }} == ready", count: 30, delaySecs: 10 },
    expect: (t) => {
      assert.equal(t.repeat.until, "{{ output }} == ready");
      assert.equal(t.repeat.max_iterations, 30);
      assert.equal(t.repeat.delay_secs, 10);
    },
  },
];

for (const c of STEP_CASES) {
  const model = base();
  model.tasks[1] = writeLoop(model.tasks[1], c.loop);
  const { back } = roundTrip(model, `step: ${c.label}`);
  const task = back.tasks.find((t) => t.name === "process");

  c.expect(task);

  // The loop the panel would show after a reload must be the loop that was set.
  const reread = readLoop(task);
  assert.equal(reread.kind, c.loop.kind, `${c.label}: kind survives the round-trip`);
  if (c.loop.kind === "foreach") {
    assert.equal(reread.source, c.loop.source, `${c.label}: item source survives`);
    assert.equal(reread.label, c.loop.label ?? "", `${c.label}: instance_key survives`);
  }
  if (c.loop.kind === "repeat" || c.loop.kind === "until") {
    assert.equal(reread.count, c.loop.count, `${c.label}: iteration budget survives`);
    assert.equal(reread.delaySecs, c.loop.delaySecs, `${c.label}: delay survives`);
  }
  if (c.loop.source === "list") {
    assert.deepEqual(JSON.parse(reread.items), JSON.parse(c.loop.items), "items survive");
  }
  checks++;
}

// Clearing a loop must leave no trace: a stale `with_items` beside a fresh
// `repeat` is a spec the engine accepts and the canvas can't draw.
{
  const model = base();
  const looped = writeLoop(model.tasks[1], { ...NO_LOOP, kind: "foreach", source: "count", count: 3 });
  const cleared = writeLoop(looped, NO_LOOP);
  for (const k of ["with_items", "with_param", "instance_key", "repeat"]) {
    assert.equal(cleared[k], undefined, `clearing the loop clears ${k}`);
  }
  model.tasks[1] = cleared;
  const { yaml } = roundTrip(model, "step: cleared");
  assert.ok(!/with_items|repeat|instance_key/.test(yaml), "cleared loop leaves no keys in the YAML");
  checks++;
}

// Switching kind must not leave the previous kind's keys behind.
{
  const fan = writeLoop(base().tasks[1], { ...NO_LOOP, kind: "foreach", source: "count", count: 3 });
  const rep = writeLoop(fan, { ...NO_LOOP, kind: "repeat", count: 2 });
  assert.equal(rep.with_items, undefined, "switching fan-out → repeat drops with_items");
  assert.ok(rep.repeat, "…and sets repeat");
  const back = writeLoop(rep, { ...NO_LOOP, kind: "foreach", source: "param", param: "{{ xs }}" });
  assert.equal(back.repeat, undefined, "switching repeat → fan-out drops repeat");
  assert.equal(back.with_param, "{{ xs }}");
  checks++;
}

// `instance_key` is only legal beside a fan-out — the engine rejects it alone.
{
  const t = { ...base().tasks[1], instance_key: "{{ item }}" };
  const { back } = roundTrip({ ...base(), tasks: [t] }, "step: orphan instance_key");
  assert.equal(back.tasks[0].instance_key, undefined, "a label with nothing to label is not emitted");
  checks++;
}

// ── whole-workflow loops ────────────────────────────────────────────────────

for (const mode of ["parallel", "sequential"]) {
  const wrapped = wrapWorkflowLoop(base(), { mode, passes: 3 });
  const { back } = roundTrip(wrapped, `workflow: ${mode}`);

  assert.equal(back.tasks.length, 1, `${mode}: one top-level task — the call`);
  assert.equal(back.tasks[0].name, "loop-pass");
  assert.equal(back.tasks[0].template, "loop-body");

  const body = back.templates.find((t) => t.name === "loop-body");
  assert.ok(body, `${mode}: the body template is emitted`);

  if (mode === "parallel") {
    assert.deepEqual(back.tasks[0].with_items, [1, 2, 3], "parallel fans the call out over the passes");
    assert.equal(body.tasks.length, 2, "parallel body is just the user's tasks");
  } else {
    assert.equal(back.tasks[0].arguments.passes, "3", "sequential carries the count in arguments");
    const next = body.tasks.find((t) => t.name === "loop-next-pass");
    assert.ok(next, "sequential body recurses");
    assert.equal(next.template, "loop-body", "…into itself");
    assert.equal(next.arguments.pass, "{{ pass + 1 }}", "…with the pass bumped");
    assert.equal(next._extra.when, "{{ passes }} > {{ pass }}", "…guarded by a base case");
    assert.deepEqual(next.depends_on, ["process"], "…after the body's exit task");
  }

  // Read back as the same loop.
  const found = readWorkflowLoop(back);
  assert.ok(found, `${mode}: reads back as a loop`);
  assert.equal(found.loop.mode, mode);
  assert.equal(found.loop.passes, 3);

  // The canvas draws the user's graph, not the wrapper.
  const shown = loopBodyModel(back, found);
  assert.deepEqual(
    shown.tasks.map((t) => t.name),
    ["prepare", "process"],
    `${mode}: the canvas shows the body, with the plumbing hidden`,
  );

  // Unwrapping restores the original workflow exactly.
  const plain = unwrapWorkflowLoop(back, found);
  assert.equal(readWorkflowLoop(plain), null, `${mode}: unwrapped is no longer a loop`);
  assert.deepEqual(
    plain.tasks.map((t) => t.name),
    ["prepare", "process"],
    `${mode}: unwrap restores the tasks`,
  );
  assert.deepEqual(plain.templates, [], `${mode}: unwrap removes the body template`);
  checks++;
}

// Editing the body keeps the loop, and the sequential guard follows the new
// exit task — otherwise the next pass would start before the graph finished.
{
  const wrapped = wrapWorkflowLoop(base(), { mode: "sequential", passes: 4 });
  const found = readWorkflowLoop(wrapped);
  const shown = loopBodyModel(wrapped, found);
  const edited = {
    ...shown,
    tasks: [...shown.tasks, { name: "publish", command: ["echo", "publish"], depends_on: ["process"] }],
  };
  const { back } = roundTrip(applyLoopBody(found, edited), "workflow: body edited");
  const again = readWorkflowLoop(back);
  assert.ok(again, "the loop survives a body edit");
  assert.equal(again.loop.passes, 4, "…with its pass count");
  const next = again.body.tasks.find((t) => t.name === "loop-next-pass");
  assert.deepEqual(next.depends_on, ["publish"], "the next pass waits for the new last task");
  checks++;
}

// Re-wrapping must change the loop, not nest a second one around it.
{
  const once = wrapWorkflowLoop(base(), { mode: "parallel", passes: 3 });
  const twice = wrapWorkflowLoop(once, { mode: "sequential", passes: 5 });
  const found = readWorkflowLoop(twice);
  assert.equal(found.loop.mode, "sequential", "re-wrap switches mode");
  assert.equal(found.loop.passes, 5);
  assert.equal(twice.templates.filter((t) => t.name === "loop-body").length, 1, "one body, not two");
  assert.deepEqual(
    found.body.tasks.filter((t) => t.name !== "loop-next-pass").map((t) => t.name),
    ["prepare", "process"],
    "the user's graph is untouched by the re-wrap",
  );
  checks++;
}

// ── the Visual tab stays usable ─────────────────────────────────────────────

// Every loop the console writes must leave Visual mode unlocked.
//
// `visualSupport` is what decides whether the canvas is allowed to edit a spec,
// and it locks on anything it can't honestly draw. If a loop control emitted a
// spec it rejects, ticking that control would throw the user into the YAML tab
// — which is the exact thing these controls exist to avoid. So the gate is
// checked against the editor's own output, not trusted to agree by inspection.
{
  const { visualSupport } = await import("@/lib/spec-support");
  const cases = [
    ...STEP_CASES.map((c) => {
      const m = base();
      m.tasks[1] = writeLoop(m.tasks[1], c.loop);
      return [`step: ${c.label}`, m];
    }),
    ["workflow: parallel", wrapWorkflowLoop(base(), { mode: "parallel", passes: 3 })],
    ["workflow: sequential", wrapWorkflowLoop(base(), { mode: "sequential", passes: 3 })],
  ];
  for (const [label, model] of cases) {
    const s = visualSupport(modelToYaml(model));
    assert.ok(s.ok, `${label}: Visual mode stays unlocked, got: ${s.reasons.join("; ")}`);
    checks++;
  }
}

// …and a loop key in a shape the model can't hold still locks, rather than
// being dropped on the round-trip.
{
  const { visualSupport } = await import("@/lib/spec-support");
  const bad = [
    ["with_items: not-a-list", "name: w\ntasks:\n  - { name: a, command: [x], with_items: nope }\n"],
    ["both sources", 'name: w\ntasks:\n  - { name: a, command: [x], with_items: [1], with_param: "{{ p }}" }\n'],
    ["repeat without max_iterations", 'name: w\ntasks:\n  - name: a\n    command: [x]\n    repeat: { until: "{{ output }} == ok" }\n'],
    // `typeof NaN === "number"` is true, so a support check written on typeof
    // alone passes this — and `parseRepeat` (Number.isFinite) then returns
    // undefined and the WHOLE repeat block is dropped on the round-trip. The
    // gate and the parser have to agree key for key.
    ["repeat.max_iterations: NaN", 'name: w\ntasks:\n  - name: a\n    command: [x]\n    repeat: { until: "{{ output }} == ok", max_iterations: .nan }\n'],
    ["repeat.max_iterations: Infinity", 'name: w\ntasks:\n  - name: a\n    command: [x]\n    repeat: { until: "{{ output }} == ok", max_iterations: .inf }\n'],
    ["repeat.delay_secs: not a number", 'name: w\ntasks:\n  - name: a\n    command: [x]\n    repeat: { until: "{{ output }} == ok", max_iterations: 3, delay_secs: soon }\n'],
    ["repeat with a key the model drops", 'name: w\ntasks:\n  - name: a\n    command: [x]\n    repeat: { until: "{{ output }} == ok", max_iterations: 3, backoff: 2 }\n'],
    ["repeat: not a mapping", 'name: w\ntasks:\n  - name: a\n    command: [x]\n    repeat: yes\n'],
  ];
  for (const [label, yaml] of bad) {
    assert.equal(visualSupport(yaml).ok, false, `${label}: locks the Visual tab`);
    checks++;
  }
}

// ── palette blocks ──────────────────────────────────────────────────────────

// Every loop block in the palette has to survive the YAML round-trip and read
// back as the loop it advertises.
//
// This exists because of a specific way to get it wrong: a block that puts a
// *modeled* key in `_extra` is dropped on the way out, because `taskToYaml`
// skips `_extra` entries the model owns. The block inserts, the canvas looks
// right, and the emitted YAML has no loop in it at all.
{
  const { SNIPPETS, applySnippet } = await import("@/lib/palette");
  const LOOP_BLOCKS = {
    "repeat-until": "until",
    "repeat-times": "repeat",
    "for-each-item": "foreach",
  };
  for (const [id, kind] of Object.entries(LOOP_BLOCKS)) {
    const snippet = SNIPPETS.find((s) => s.id === id);
    assert.ok(snippet, `palette still has the '${id}' block`);
    const r = applySnippet(modelToYaml(base()), snippet);
    assert.ok(r.spec != null, `${id}: inserts cleanly (${r.error})`);
    const { model: after } = parseModel(r.spec);
    assert.ok(after, `${id}: the inserted spec parses`);
    // The block appends, so the loop is on the task it just added.
    const added = after.tasks[after.tasks.length - 1];
    assert.equal(
      readLoop(added).kind,
      kind,
      `${id}: reaches the YAML as a '${kind}' loop, not as a dropped _extra key`,
    );
    checks++;
  }
}

// A user task that happens to be called `loop-next-pass` is not the loop's
// plumbing, and must not be hidden from the canvas or dropped on the fold-back.
// `loop-next-pass` is a legal task name; only the task that *also* calls the
// body is the recursion.
{
  const plain = base();
  plain.tasks.push({ name: "loop-next-pass", command: ["echo", "mine"], depends_on: ["process"] });
  const wrapped = wrapWorkflowLoop(plain, { mode: "parallel", passes: 2 });
  const found = readWorkflowLoop(wrapped);
  assert.ok(found, "wraps normally");
  const shown = loopBodyModel(wrapped, found);
  assert.ok(
    shown.tasks.some((t) => t.name === "loop-next-pass" && t.command[1] === "mine"),
    "the user's same-named task stays on the canvas",
  );
  const refolded = readWorkflowLoop(applyLoopBody(found, shown));
  assert.ok(
    refolded.body.tasks.some((t) => t.name === "loop-next-pass" && t.command[1] === "mine"),
    "…and survives being folded back into the wrapper",
  );
  checks++;
}

// A `repeat:` the gate accepts must survive the round-trip intact. This is the
// other half of the check above: the gate locking on a shape it cannot hold is
// only correct if everything it *passes* comes back unchanged.
{
  const { visualSupport } = await import("@/lib/spec-support");
  const yaml =
    'name: w\ntasks:\n  - name: a\n    command: [x]\n    repeat: { until: "{{ output }} == ok", max_iterations: 3, delay_secs: 7 }\n';
  assert.ok(visualSupport(yaml).ok, "a well-formed repeat keeps Visual mode open");
  const { model } = parseModel(yaml);
  const back = parseModel(modelToYaml(model)).model;
  assert.deepEqual(
    back.tasks[0].repeat,
    { until: "{{ output }} == ok", max_iterations: 3, delay_secs: 7 },
    "every field the gate accepted round-trips",
  );
  checks++;
}

// Wrapping must never overwrite a template the user already owns. The wrapper
// writes its own `loop-body`, so a workflow that already declares one has to be
// refused — silently replacing it would discard that template's tasks and
// silently re-point every task calling it at the generated body.
{
  const { wrapBlocker } = await import("@/lib/workflow-loop");
  assert.equal(wrapBlocker(base()), null, "an ordinary workflow wraps freely");

  const collides = {
    ...base(),
    templates: [{ name: "loop-body", tasks: [{ name: "mine", command: ["true"], depends_on: [] }] }],
  };
  const why = wrapBlocker(collides);
  assert.ok(why, "a workflow with its own loop-body template is refused");
  assert.match(why, /loop-body/, "…and the refusal names the template to rename");

  // Re-wrapping an already-looping workflow is not a collision — replacing the
  // loop's own body is exactly what changing the loop does.
  assert.equal(
    wrapBlocker(wrapWorkflowLoop(base(), { mode: "parallel", passes: 3 })),
    null,
    "an already-wrapped workflow can be re-wrapped",
  );
  checks++;
}

// A half-typed item list must not reach the model. `writeLoop` turns
// unparseable JSON into `with_items: []`, so persisting every keystroke would
// destroy the list being edited the moment the user typed the first bracket.
{
  const valid = { ...NO_LOOP, kind: "foreach", source: "list", items: '["a", "b", "c"]' };
  assert.equal(loopError(valid, true), null, "the finished list is valid");
  const halfTyped = { ...valid, items: '["a' };
  assert.ok(loopError(halfTyped, true), "a half-typed list is invalid, so the panel holds it back");
  // …and were it written anyway, this is what would land — the loss the guard
  // in EditableDag's `set` exists to prevent.
  assert.deepEqual(
    writeLoop(base().tasks[1], halfTyped).with_items,
    [],
    "writeLoop on an invalid draft empties the list, which is why it must not be persisted",
  );
  checks++;
}

// A workflow that merely *has* a template called loop-body is not a looping
// one — the detector claims a spec only by its whole top-level shape.
{
  const decoy = {
    ...base(),
    templates: [{ name: "loop-body", tasks: [{ name: "x", command: ["true"], depends_on: [] }] }],
  };
  assert.equal(readWorkflowLoop(decoy), null, "a lookalike template is not a loop wrapper");
  checks++;
}

// ── Runtime fan-out: `with_output_of` ────────────────────────────────────────
//
// The one loop the console could not write. It is the same `foreach` kind as
// the other three, but it resolves mid-run, so what matters here is that the
// editor emits the key the *sweep* reads and refuses the shapes the engine
// rejects — a spec that saves and never fans out is the failure mode.
{
  const m = base();
  const loop = { ...NO_LOOP, kind: "foreach", source: "output", producer: "prepare" };
  m.tasks[1] = writeLoop(m.tasks[1], loop);
  const { back } = roundTrip(m, "foreach/output");

  assert.equal(
    back.tasks[1].with_output_of,
    "prepare",
    "the editor emits with_output_of — the key reconcile_fanouts reads",
  );
  assert.equal(back.tasks[1].with_items, undefined, "and none of the expansion-time sources");
  assert.equal(back.tasks[1].with_param, undefined);

  const read = readLoop(back.tasks[1]);
  assert.equal(read.kind, "foreach");
  assert.equal(read.source, "output", "and it reads back as the same source, not as a list");
  assert.equal(read.producer, "prepare");
  checks += 2;
}

// The dependency rule, which is the engine's and not a nicety: the instance
// count is read from a task's output, so a producer this step does not wait for
// makes the count depend on scheduling order. `dag.rs` bails at submit; the
// panel has to catch it at the keystroke or the user learns it from a 400.
{
  const loop = { ...NO_LOOP, kind: "foreach", source: "output", producer: "prepare" };
  assert.equal(loopError(loop, true, ["prepare"]), null, "a dependency is a valid producer");
  assert.ok(
    loopError(loop, true, [])?.includes("Depends on"),
    "a producer that is not a dependency is refused, and says how to fix it",
  );
  assert.ok(
    loopError({ ...loop, producer: "  " }, true, ["prepare"]),
    "an unnamed producer is refused rather than emitting an empty with_output_of",
  );
  checks += 3;
}

// `instance_key` labels a runtime fan-out too — the sweep applies the same
// rules the expander does. It must survive the round trip, or a spec that names
// its instances reloads as one that numbers them.
{
  const m = base();
  m.tasks[1] = writeLoop(m.tasks[1], {
    ...NO_LOOP,
    kind: "foreach",
    source: "output",
    producer: "prepare",
    label: "{{ item.region }}",
  });
  const { back } = roundTrip(m, "foreach/output + instance_key");
  assert.equal(back.tasks[1].instance_key, "{{ item.region }}");
  assert.equal(readLoop(back.tasks[1]).label, "{{ item.region }}");
  checks += 2;
}

// Switching away from it must clear the key. Leaving it beside a fresh
// `with_items` is a spec the engine refuses outright ("a task fans out from one
// source") — the exact corruption `writeLoop`'s clear-everything shape exists
// to prevent.
{
  const m = base();
  m.tasks[1] = writeLoop(m.tasks[1], {
    ...NO_LOOP,
    kind: "foreach",
    source: "output",
    producer: "prepare",
  });
  const switched = writeLoop(m.tasks[1], { ...NO_LOOP, kind: "foreach", source: "count", count: 2 });
  assert.equal(switched.with_output_of, undefined, "switching source clears the old one");
  assert.deepEqual(switched.with_items, [1, 2]);
  const off = writeLoop(m.tasks[1], NO_LOOP);
  assert.equal(off.with_output_of, undefined, "and turning the loop off clears it too");
  checks += 3;
}

// Chained: the producer is itself a fan-out. The editor edits the AUTHORED
// graph, where `regions` is one task, so the control needs no idea that
// expansion will turn it into `regions.0`/`regions.1` — but both loops have to
// survive one round trip in the same document, or saving a chain drops half of
// it.
{
  const m = base();
  m.tasks[0] = writeLoop(m.tasks[0], {
    ...NO_LOOP,
    kind: "foreach",
    source: "list",
    items: '["us", "eu"]',
  });
  m.tasks[1] = writeLoop(m.tasks[1], {
    ...NO_LOOP,
    kind: "foreach",
    source: "output",
    producer: "prepare",
  });
  const { back } = roundTrip(m, "chained fan-out");

  assert.deepEqual(back.tasks[0].with_items, ["us", "eu"], "the producer keeps its fan-out");
  assert.equal(back.tasks[1].with_output_of, "prepare", "and the consumer keeps its producer");
  assert.deepEqual(
    back.tasks[1].depends_on,
    ["prepare"],
    "the dependency the engine resolves the producer's rows through is intact",
  );
  assert.equal(readLoop(back.tasks[0]).source, "list");
  assert.equal(readLoop(back.tasks[1]).source, "output");
  checks += 3;
}

console.log(`loop editor: ${checks} checks passed`);
