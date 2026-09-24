// Looping the *whole* workflow, not one step.
//
// "Run this DAG N times" has no field in the spec, because the engine has no
// run-level loop: `repeat:` loops one task, `with_items:` fans one task out. The
// only thing that can stand for a whole graph is a `templates:` sub-DAG, so a
// looping workflow is the graph moved into a template plus one task that calls
// it N times. This module owns that rewrite in both directions.
//
// Two shapes, because "N times" means two different things:
//
//   **parallel** — the call fans out (`with_items: [1…N]`), so N independent
//   copies of the graph run at once. One run, N× the tasks. This is the load
//   test / N-shards shape.
//
//   **sequential** — the body ends with a task that calls the body again with
//   `pass + 1`, guarded by `when: passes > pass`. The expander unrolls that into
//   N copies chained end to end, so pass k+1 starts only after pass k finishes.
//   This is the iterate / retry-the-pipeline shape. Pass number *is* recursion
//   depth, so `MAX_SEQUENTIAL_PASSES` stays under the expander's depth cap.
//
// Both are ordinary specs — nothing here needs an engine change, and a spec
// written this way by hand is picked up by `readWorkflowLoop` just the same.
//
// **The editor never shows the wrapper.** `loopBodyModel` hands the canvas the
// body's tasks as if they were the workflow's own, and `applyLoopBody` folds
// edits back in. Collapsing a user's graph into one `loop-pass` node the moment
// they tick "repeat" would be a strange trade: they asked for a loop, not for
// their pipeline to disappear.

import type { Task, Template, WorkflowModel } from "@/lib/spec-model";
import { MAX_SEQUENTIAL_PASSES, countItems } from "@/lib/loop-model";

/// Names the wrapper owns. Reserved: `readWorkflowLoop` only claims a spec whose
/// *whole* top level is one task with this name calling this template, so a
/// workflow that happens to contain a template called `loop-body` is not
/// mistaken for a looping one.
export const LOOP_BODY = "loop-body";
export const LOOP_CALL = "loop-pass";
/// The recursive call that makes `sequential` sequential. Lives inside the body
/// and is hidden from the canvas — it is the loop's plumbing, not a step the
/// user wrote.
export const LOOP_NEXT = "loop-next-pass";

export type WorkflowLoopMode = "parallel" | "sequential";

export interface WorkflowLoop {
  mode: WorkflowLoopMode;
  /// How many times the whole graph runs. ≥ 2 — a "loop" of one pass is just
  /// the workflow, and is stored by unwrapping instead.
  passes: number;
}

/// A recognized wrapper: the loop it encodes plus the body template it wraps.
export interface FoundLoop {
  loop: WorkflowLoop;
  body: Template;
}

/// Read the loop a model is wrapped in, or null for an ordinary workflow.
///
/// Structural, not name-only: the top level must be exactly the one call task,
/// so an edited or hand-written spec that no longer has that shape falls back to
/// being an ordinary workflow (and keeps its tasks) rather than being edited
/// through a wrapper that isn't there.
export function readWorkflowLoop(model: WorkflowModel): FoundLoop | null {
  if (model.tasks.length !== 1) return null;
  const call = model.tasks[0];
  if (call.name !== LOOP_CALL || call.template !== LOOP_BODY) return null;
  const body = model.templates.find((t) => t.name === LOOP_BODY);
  if (!body) return null;

  // Parallel: the call fans out, and the instance count is the pass count.
  if (call.with_items?.length) {
    const passes = call.with_items.length;
    return passes >= 2 ? { loop: { mode: "parallel", passes }, body } : null;
  }
  // Sequential: the pass count rides in the call's arguments, and the body
  // carries the recursive step that consumes it.
  const passes = Number(call.arguments?.passes);
  const recurses = body.tasks.some((t) => t.name === LOOP_NEXT && t.template === LOOP_BODY);
  if (recurses && Number.isInteger(passes) && passes >= 2) {
    return { loop: { mode: "sequential", passes }, body };
  }
  return null;
}

/// Why this workflow can't be wrapped in a loop, or null when it can.
///
/// The wrapper *owns* the `loop-body` name: `buildWrapper` writes its own
/// template at that name and drops any other. A workflow that already declares
/// a `loop-body` of its own would therefore lose that template's tasks on the
/// first click, and every task calling it would silently start calling the
/// generated body instead — a wrong graph that still parses and still runs.
///
/// Refusing is the honest move rather than generating a free name: the name is
/// also how `readWorkflowLoop` recognizes the shape, so a workflow wrapped
/// under some other name would stop reading back as a loop. Renaming one
/// template is a smaller ask than an unwrappable spec.
///
/// An already-wrapped model is fine — re-wrapping replaces the loop's own body,
/// which is the point.
export function wrapBlocker(model: WorkflowModel): string | null {
  if (readWorkflowLoop(model)) return null;
  if (model.templates.some((t) => t.name === LOOP_BODY)) {
    return `This workflow already has a template named “${LOOP_BODY}”, which the loop needs for itself. Rename that template first.`;
  }
  return null;
}

/// Clamp a pass count to what both the UI and the expander will accept.
///
/// One limit for both modes, deliberately. A sequential loop's ceiling is the
/// expander's recursion depth (a pass *is* a level of it). A parallel one is
/// bounded instead by the run's task budget, which is far larger — but a
/// whole-workflow loop multiplies the **entire graph**, so 500 parallel passes
/// over a 20-task workflow is 10,000 rows from one click. The step-level
/// fan-out keeps its own larger `MAX_FOREACH_COUNT`, where the multiplier is
/// one task; this control stays at the smaller number on purpose.
export function clampPasses(n: number): number {
  if (!Number.isFinite(n)) return 2;
  return Math.min(MAX_SEQUENTIAL_PASSES, Math.max(2, Math.floor(n)));
}

/// Is this the loop's own recursive step, rather than a task the user wrote?
///
/// Name *and* target, not name alone: `loop-next-pass` is a legal task name, and
/// a user who happens to use it would otherwise have that task hidden from the
/// canvas and dropped the next time the body was folded back in. The plumbing is
/// the only task that both carries the name and calls the body.
function isPlumbing(t: Task): boolean {
  return t.name === LOOP_NEXT && t.template === LOOP_BODY;
}

/// The tasks nothing else in the list depends on — where the next pass has to
/// wait. Without this the recursive call would start alongside the body instead
/// of after it, and "sequential" would quietly be "parallel".
function exitTasks(tasks: Task[]): string[] {
  const depended = new Set(tasks.flatMap((t) => t.depends_on));
  return tasks.filter((t) => !depended.has(t.name)).map((t) => t.name);
}

/// The recursive step for a sequential loop: run the body again with the pass
/// counter bumped, unless this was the last pass. `{{ pass + 1 }}` is the
/// expander's arithmetic and `when:` is its base case — the same pair the
/// engine's own recursion test uses.
function nextPassTask(bodyTasks: Task[]): Task {
  return {
    name: LOOP_NEXT,
    command: [],
    depends_on: exitTasks(bodyTasks),
    template: LOOP_BODY,
    arguments: { pass: "{{ pass + 1 }}", passes: "{{ passes }}" },
    _extra: { when: "{{ passes }} > {{ pass }}" },
  };
}

/// The call task that runs the body — the workflow's only top-level task.
function callTask(loop: WorkflowLoop): Task {
  if (loop.mode === "parallel") {
    return {
      name: LOOP_CALL,
      command: [],
      depends_on: [],
      template: LOOP_BODY,
      // `{{ item }}` is the 1-based pass number from `countItems`, handed to the
      // body so a task can tell the copies apart (`--shard {{ pass }}`).
      arguments: { pass: "{{ item }}" },
      with_items: countItems(loop.passes),
    };
  }
  return {
    name: LOOP_CALL,
    command: [],
    depends_on: [],
    template: LOOP_BODY,
    arguments: { pass: "1", passes: String(loop.passes) },
  };
}

/// The body template's declared parameters. Defaults only — every call passes
/// both — but a template parameter that isn't declared has no default to fall
/// back on, and `{{ pass }}` in a task would render as literal text.
function bodyParameters(mode: WorkflowLoopMode): Record<string, string> {
  return mode === "parallel" ? { pass: "1" } : { pass: "1", passes: "1" };
}

/// Build the wrapper around `bodyTasks` unconditionally. The one place the
/// shape is constructed, so `wrapWorkflowLoop` (wrap a plain workflow) and
/// `setWorkflowLoop` (re-wrap an existing one) cannot drift apart — and neither
/// calls the other, which is what keeps "wrap an already-wrapped model" from
/// being a recursion.
function buildWrapper(
  model: WorkflowModel,
  bodyTasks: Task[],
  loop: WorkflowLoop,
): WorkflowModel {
  const spec: WorkflowLoop = { mode: loop.mode, passes: clampPasses(loop.passes) };
  const body: Template = {
    name: LOOP_BODY,
    parameters: bodyParameters(spec.mode),
    tasks: spec.mode === "sequential" ? [...bodyTasks, nextPassTask(bodyTasks)] : bodyTasks,
  };
  return {
    ...model,
    // The body goes first: the engine's examples declare templates before the
    // tasks that call them, and `modelToYaml` emits them in that order anyway.
    templates: [body, ...model.templates.filter((t) => t.name !== LOOP_BODY)],
    tasks: [callTask(spec)],
  };
}

/// Wrap a workflow so the whole graph runs `loop.passes` times.
///
/// The tasks move into the body template unchanged — including their
/// `depends_on`, which still resolves because a template's dependencies are
/// scoped to its own tasks. Everything else about the model (name, other
/// templates, top-level `_extra` keys like `parameters:` and `tags:`) is left
/// alone. Re-wrapping an already-looping model changes the loop rather than
/// nesting a second one around it.
export function wrapWorkflowLoop(model: WorkflowModel, loop: WorkflowLoop): WorkflowModel {
  const found = readWorkflowLoop(model);
  const bodyTasks = found ? found.body.tasks.filter((t) => !isPlumbing(t)) : model.tasks;
  return buildWrapper(model, bodyTasks, loop);
}

/// Undo the wrap: the body's tasks become the workflow's tasks again, and the
/// loop's plumbing is dropped. Total — an unwrapped model comes back unchanged.
export function unwrapWorkflowLoop(model: WorkflowModel, found?: FoundLoop | null): WorkflowModel {
  const f = found ?? readWorkflowLoop(model);
  if (!f) return model;
  return {
    ...model,
    templates: model.templates.filter((t) => t.name !== LOOP_BODY),
    tasks: f.body.tasks.filter((t) => !isPlumbing(t)),
  };
}

/// Change the pass count or mode of an already-wrapped workflow, keeping the
/// body. Switching mode rebuilds the plumbing rather than patching it: the two
/// shapes differ in the call *and* in whether the body recurses, and a
/// half-converted wrapper is a spec that still parses and loops wrongly.
export function setWorkflowLoop(
  model: WorkflowModel,
  found: FoundLoop,
  loop: WorkflowLoop,
): WorkflowModel {
  return buildWrapper(model, found.body.tasks.filter((t) => !isPlumbing(t)), loop);
}

/// The model the canvas should draw and edit: the body's tasks presented as the
/// workflow's own, with the loop's plumbing hidden. Other templates stay
/// visible, so a body that calls a sub-DAG still edits normally.
export function loopBodyModel(model: WorkflowModel, found: FoundLoop): WorkflowModel {
  return {
    ...model,
    templates: model.templates.filter((t) => t.name !== LOOP_BODY),
    tasks: found.body.tasks.filter((t) => !isPlumbing(t)),
  };
}

/// Fold an edit made against `loopBodyModel` back into the wrapper.
///
/// The recursive step is rebuilt rather than preserved: its `depends_on` is the
/// body's exit set, so adding a task at the end of the graph has to move it, or
/// the new last task would run *after* the next pass started.
export function applyLoopBody(found: FoundLoop, edited: WorkflowModel): WorkflowModel {
  // `edited` already carries everything outside the body — the other templates,
  // the name, the top-level `_extra` keys — because `loopBodyModel` only
  // replaced `tasks` and hid the body template.
  return buildWrapper(edited, edited.tasks, found.loop);
}

/// One line describing what the loop does, for the banner over the canvas.
export function describeWorkflowLoop(loop: WorkflowLoop, tasks: number): string {
  return loop.mode === "parallel"
    ? `${loop.passes} copies of this graph run at once — ${loop.passes * tasks} tasks in one run. Each copy gets its number as {{ pass }}.`
    : `This graph runs ${loop.passes} times, one pass after the last. Each pass gets its number as {{ pass }}.`;
}
