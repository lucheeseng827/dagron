// One loop vocabulary for the editor, over the engine's two loop mechanisms.
//
// dagron loops a task in exactly two ways, and they are not interchangeable:
//
//   * **fan-out** (`with_items:` / `with_param:`) is resolved by the *expander*
//     at run-creation time. One authored task becomes N task rows that run in
//     **parallel**, named `<task>.0…N-1` (or `<task>.<instance_key>`). The count
//     is fixed before anything executes.
//   * **repeat** (`repeat: { until, max_iterations, delay_secs }`) is evaluated
//     by the *engine* after each success. One task row runs again **in place**,
//     **sequentially**, until `until` holds — so the count is not known until it
//     stops.
//
// Collapsing those into a single "loop" widget would be a lie in both
// directions: a fan-out has no `until` to offer, and a repeat has no N to draw.
// So `LoopKind` keeps them apart and names them by what the user gets —
// `foreach` (N in parallel), `repeat` (N in a row), `until` (poll until true) —
// while this module owns the only translation between that vocabulary and the
// spec fields. The panel edits a `LoopSpec`; the node draws a `LoopSpec`; only
// `readLoop`/`writeLoop` know which YAML keys are behind it.

import type { Task } from "@/lib/spec-model";

/// How a task loops. `none` is the ordinary one-run task.
export type LoopKind = "none" | "foreach" | "repeat" | "until";

/// Where a `foreach` loop's items come from. Each maps to a different spec
/// shape; `output` (`with_output_of:`) is the one resolved at run time rather
/// than at expansion — see `LOOP_SOURCE_SUPPORTED`.
export type LoopSource = "count" | "list" | "param" | "output";

/// Iteration ceiling offered for a `repeat`/`until` loop. Not an engine limit
/// (`max_iterations` is a u32) — a guard rail, because the loop holds a task row
/// `running` for the whole time and a fat-fingered 10000 is a wedged run.
export const MAX_REPEAT_ITERATIONS = 1000;

/// Instances a `foreach` count may produce. The engine's own ceiling is
/// `DAGRON_MAX_TASKS_PER_RUN` (100k by default) over the *whole expanded graph*;
/// this is a per-loop bound so one slider can't spend the run's entire budget.
export const MAX_FOREACH_COUNT = 500;

/// Sequential passes a whole-workflow loop may run. The hard stop is the
/// expander's `MAX_DEPTH` (64) — the loop is a self-calling template, so pass
/// number *is* recursion depth. Kept under it so the wrapper's own nesting
/// doesn't push a legal-looking N over the cap at submit.
export const MAX_SEQUENTIAL_PASSES = 50;

/// `foreach` sources the editor can write.
///
/// The table remains because the distinction it drew still matters, even now
/// that every entry is true. `count`/`list`/`param` are resolved by the
/// expander before the run exists; `output` is resolved by a sweep *during* the
/// run, which is why its instance count reads as unknown everywhere in this
/// module and why its error text talks about dependencies rather than syntax.
export const LOOP_SOURCE_SUPPORTED: Record<LoopSource, boolean> = {
  count: true,
  list: true,
  param: true,
  output: true,
};

export interface LoopSpec {
  kind: LoopKind;
  /// `foreach` — where the items come from.
  source: LoopSource;
  /// `foreach`+`count`, and `repeat` — how many instances / passes.
  count: number;
  /// `foreach`+`list` — the items, as authored JSON array text. Kept as text,
  /// not a parsed array: a half-typed `["a", ` must survive a re-render, and
  /// round-tripping through a parse would either drop it or reformat under the
  /// cursor.
  items: string;
  /// `foreach`+`param` — the parameter reference holding a JSON array,
  /// e.g. `{{ shards }}`.
  param: string;
  /// `foreach`+`output` — the upstream task whose stdout holds the JSON array.
  /// A task name, not a template: the engine looks the row up by name, and it
  /// must be one this task depends on.
  producer: string;
  /// `foreach` — `instance_key`: a template rendered per instance to name it
  /// (`sync.us-east-1` instead of `sync.0`). Empty = bare indexes.
  label: string;
  /// `until` — the engine's `repeat.until` condition verbatim.
  until: string;
  /// `repeat`/`until` — seconds between passes (`repeat.delay_secs`).
  delaySecs: number;
}

export const NO_LOOP: LoopSpec = {
  kind: "none",
  source: "count",
  count: 3,
  items: '["a", "b", "c"]',
  param: "",
  producer: "",
  label: "",
  until: "{{ output }} == done",
  delaySecs: 0,
};

/// The `repeat.until` a count loop is written as. `{{ attempt }}` is the engine's
/// 1-based iteration counter, so "stop when attempt reaches N" *is* "run N
/// times" — there is no separate count field on `RepeatSpec` to use instead.
export function countUntil(n: number): string {
  return `{{ attempt }} == ${n}`;
}

/// Recognize the expression `countUntil` writes, so a spec authored by this
/// editor reads back as a count loop rather than as an opaque condition the
/// user then has to re-enter by hand. Tolerant of whitespace only — anything
/// else the author wrote is theirs, and is read back as an `until` loop.
function parseCountUntil(until: string): number | null {
  const m = /^\{\{\s*attempt\s*\}\}\s*==\s*(\d+)$/.exec(until.trim());
  if (!m) return null;
  const n = Number(m[1]);
  return Number.isInteger(n) && n >= 1 ? n : null;
}

/// True when `items` is exactly `[1, 2, … n]` — what a count loop writes. Lets
/// a count round-trip instead of degrading into a literal list on reload.
function parseCountItems(items: unknown[]): number | null {
  if (!items.length) return null;
  for (let i = 0; i < items.length; i++) if (items[i] !== i + 1) return null;
  return items.length;
}

/// The `with_items` a count loop writes: 1-based, so `{{ item }}` reads as a
/// pass number in a command and matches `{{ attempt }}` in the repeat form.
export function countItems(n: number): unknown[] {
  return Array.from({ length: n }, (_, i) => i + 1);
}

/// Derive the loop a task is already carrying. Total: any task that isn't
/// looping reads back as `NO_LOOP`, so the panel has no undefined state.
export function readLoop(task: Task): LoopSpec {
  if (task.repeat) {
    const n = parseCountUntil(task.repeat.until ?? "");
    // A count loop is a `repeat` whose `until` counts *and* whose budget is the
    // same number. If they disagree the author meant something else (a poll
    // with a cap), and showing it as "repeat N times" would misreport the cap.
    const isCount = n != null && task.repeat.max_iterations === n;
    const max = task.repeat.max_iterations;
    return {
      ...NO_LOOP,
      kind: isCount ? "repeat" : "until",
      // Both forms show the same box — "how many times" for a count, "give up
      // after" for a poll — so it reads from `max_iterations` either way rather
      // than from the count parsed out of `until`. They agree when it *is* a
      // count; when they don't, the cap is the one the engine will enforce.
      count: Number.isInteger(max) && max >= 1 ? max : NO_LOOP.count,
      until: task.repeat.until ?? "",
      delaySecs: task.repeat.delay_secs ?? 0,
    };
  }
  if (task.with_output_of !== undefined) {
    return {
      ...NO_LOOP,
      kind: "foreach",
      source: "output",
      producer: task.with_output_of,
      label: task.instance_key ?? "",
    };
  }
  if (task.with_param !== undefined) {
    return { ...NO_LOOP, kind: "foreach", source: "param", param: task.with_param, label: task.instance_key ?? "" };
  }
  if (task.with_items !== undefined) {
    const n = parseCountItems(task.with_items);
    return {
      ...NO_LOOP,
      kind: "foreach",
      source: n != null ? "count" : "list",
      count: n ?? NO_LOOP.count,
      items: n != null ? NO_LOOP.items : JSON.stringify(task.with_items),
      label: task.instance_key ?? "",
    };
  }
  return NO_LOOP;
}

/// Apply a loop back onto a task, clearing whichever fields the *other* kinds
/// use. Every path assigns all four spec keys, so switching kind can never
/// leave a stale `with_items` beside a fresh `repeat` — a combination the engine
/// accepts (they compose: each fan-out instance repeats) and the node could not
/// honestly draw.
export function writeLoop(task: Task, loop: LoopSpec): Task {
  const cleared: Task = {
    ...task,
    with_items: undefined,
    with_param: undefined,
    with_output_of: undefined,
    instance_key: undefined,
    repeat: undefined,
  };
  switch (loop.kind) {
    case "none":
      return cleared;
    case "repeat":
      return {
        ...cleared,
        repeat: {
          until: countUntil(loop.count),
          max_iterations: loop.count,
          ...(loop.delaySecs ? { delay_secs: loop.delaySecs } : {}),
        },
      };
    case "until":
      return {
        ...cleared,
        repeat: {
          until: loop.until,
          // A poll needs a ceiling or it wedges the run; the engine requires
          // >= 1 and the panel can't submit 0, but a spec loaded from YAML can
          // carry one, so clamp rather than write an invalid budget back.
          max_iterations: Math.max(1, loop.count),
          ...(loop.delaySecs ? { delay_secs: loop.delaySecs } : {}),
        },
      };
    case "foreach": {
      const label = loop.label.trim() ? { instance_key: loop.label.trim() } : {};
      if (loop.source === "output") {
        return { ...cleared, with_output_of: loop.producer.trim(), ...label };
      }
      if (loop.source === "param") return { ...cleared, with_param: loop.param, ...label };
      if (loop.source === "count") return { ...cleared, with_items: countItems(loop.count), ...label };
      // `list`: keep the authored text when it doesn't parse. The spec is
      // briefly wrong, which the panel says out loud — but silently substituting
      // an empty list would *save* something the user never typed.
      const parsed = parseItems(loop.items);
      return { ...cleared, with_items: parsed.items ?? [], ...label };
    }
  }
}

/// Parse the `list` source's JSON text. Returns the items, or the reason it
/// isn't usable — the engine requires a non-empty JSON **array**, and rejects
/// an empty fan-out at submit rather than running zero tasks.
export function parseItems(text: string): { items?: unknown[]; error?: string } {
  const t = text.trim();
  if (!t) return { error: "no items yet — a fan-out over an empty list is refused at submit" };
  let v: unknown;
  try {
    v = JSON.parse(t);
  } catch {
    return { error: "not valid JSON — write a list like [\"a\", \"b\"]" };
  }
  if (!Array.isArray(v)) return { error: "must be a JSON array" };
  if (!v.length) return { error: "an empty list is refused at submit" };
  return { items: v };
}

/// Why this loop can't be saved as written, or null when it can. The engine is
/// the authoritative validator (submit 400s); this catches the same conditions
/// at the keystroke so the user isn't told at submit time.
export function loopError(loop: LoopSpec, isLeaf: boolean, deps: string[] = []): string | null {
  if (loop.kind === "none") return null;
  if (loop.kind === "repeat" || loop.kind === "until") {
    // The engine drops `repeat:` on a template call during expansion — the call
    // is replaced by the template's tasks and the field goes with it. Offering
    // it here would write a knob that does nothing.
    if (!isLeaf) return "A sub-DAG call can't repeat in place — loop it with “For each” instead, or wrap the workflow.";
    if (loop.kind === "until" && !loop.until.trim()) return "A poll needs a condition.";
    if (!Number.isInteger(loop.count) || loop.count < 1) return "Iterations must be a whole number ≥ 1.";
    if (loop.count > MAX_REPEAT_ITERATIONS) return `At most ${MAX_REPEAT_ITERATIONS} iterations.`;
    return null;
  }
  if (loop.source === "output") {
    const producer = loop.producer.trim();
    if (!producer) return "Name the step whose output holds the list.";
    // The engine refuses this at submit, and it has to: the instance count is
    // read from a task's output, so a producer this step does not wait for
    // would make the count depend on scheduling order. Caught here so it is a
    // hint beside the picker rather than a 400 after Save.
    if (!deps.includes(producer)) {
      return `This step must run after “${producer}” — add it under Depends on.`;
    }
    return null;
  }
  if (loop.source === "param") {
    if (!loop.param.trim()) return "Name the parameter holding the list, e.g. {{ shards }}.";
    return null;
  }
  if (loop.source === "count") {
    if (!Number.isInteger(loop.count) || loop.count < 1) return "Copies must be a whole number ≥ 1.";
    if (loop.count > MAX_FOREACH_COUNT) return `At most ${MAX_FOREACH_COUNT} copies per loop.`;
    return null;
  }
  return parseItems(loop.items).error ?? null;
}

/// How many task rows this loop produces, or null when the number isn't known
/// until the run happens (`with_param` resolves from a parameter at submit; a
/// `repeat` stops when its condition holds).
export function loopInstances(loop: LoopSpec): number | null {
  if (loop.kind !== "foreach") return null;
  if (loop.source === "count") return loop.count;
  if (loop.source === "list") return parseItems(loop.items).items?.length ?? null;
  // `param` resolves at submit and `output` resolves mid-run; neither has a
  // number to draw when the graph is being edited.
  return null;
}

/// The node's loop line: a short badge and the full sentence for its tooltip.
/// Null when the task doesn't loop — the node then draws no loop row, which is
/// what `statusNodeHeight` counts on.
export function describeLoop(loop: LoopSpec): { badge: string; title: string } | null {
  switch (loop.kind) {
    case "none":
      return null;
    case "repeat":
      return {
        badge: `⟳ ${loop.count}× in place`,
        title: `Runs ${loop.count} times in a row — one task, re-run after each success${
          loop.delaySecs ? `, ${loop.delaySecs}s apart` : ""
        }.`,
      };
    case "until":
      return {
        badge: `⟳ until …`,
        title: `Re-runs until ${loop.until} holds, up to ${loop.count} times${
          loop.delaySecs ? `, polling every ${loop.delaySecs}s` : ""
        }. Not reaching it fails the task.`,
      };
    case "foreach": {
      const n = loopInstances(loop);
      if (loop.source === "output") {
        return {
          badge: "⟳ each result",
          title: `Fans out in parallel over whatever “${
            loop.producer || "the upstream step"
          }” prints — a JSON array, read and turned into tasks while the run is going, so the count is not known until then.`,
        };
      }
      if (loop.source === "param") {
        return {
          badge: "⟳ each item",
          title: `Fans out in parallel over ${loop.param} — one task per item, counted when the run is created.`,
        };
      }
      return {
        badge: n == null ? "⟳ each item" : `⟳ ×${n} parallel`,
        title:
          n == null
            ? "Fans out in parallel — one task per item."
            : `Fans out into ${n} parallel tasks when the run is created.`,
      };
    }
  }
}
