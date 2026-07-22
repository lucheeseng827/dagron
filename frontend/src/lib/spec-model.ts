// Structured, editable workflow model + lossless YAML round-trip.
//
// The visual editor mutates a `WorkflowModel`; the YAML tab edits text. This
// module is the bridge: parseModel (YAML -> model) and modelToYaml (model ->
// YAML), plus pure update helpers and a cycle check so the graph editor can keep
// the DAG acyclic. The shape mirrors the engine's dag::TaskSpec.

import yaml from "js-yaml";

export interface Task {
  name: string;
  command: string[];
  depends_on: string[];
  max_attempts?: number;
  retry_delay_secs?: number;
  timeout_secs?: number;
  /// Container image to run the command in (engine `docker_image`); unset means
  /// the host executor.
  docker_image?: string;
  /// When this task fires relative to its deps' outcomes (engine `trigger_rule`).
  /// One of TRIGGER_RULES; unset means the default `all_success`.
  trigger_rule?: string;
  /// Chain another *saved* workflow as this step. A task is either a leaf
  /// (`command`) or a call (`workflow_ref`) — the server inlines the referenced
  /// workflow at run time. Mutually exclusive with `command`.
  workflow_ref?: string;
  /// Spec keys this editor doesn't model (e.g. `env`, `input`, `type: approval`
  /// knobs) preserved verbatim so the visual round-trip is lossless — switching
  /// to the visual tab never silently drops a field.
  _extra?: Record<string, unknown>;
}

/// Engine trigger-rule vocabulary (see dagron-core `trigger_rule_ready`), in the
/// order shown by pickers. The first entry is the engine default.
export const TRIGGER_RULES = [
  "all_success",
  "all_done",
  "one_failed",
  "all_failed",
  "none_failed",
] as const;

export interface WorkflowModel {
  name: string;
  tasks: Task[];
  /// Top-level spec keys this editor doesn't model (e.g. `parameters`,
  /// `templates`), preserved verbatim across the visual round-trip.
  _extra?: Record<string, unknown>;
}

/// Task keys the editor models directly; everything else rides in `Task._extra`.
const KNOWN_TASK_KEYS = new Set([
  "name",
  "command",
  "depends_on",
  "max_attempts",
  "retry_delay_secs",
  "timeout_secs",
  "docker_image",
  "trigger_rule",
  "workflow_ref",
]);

const num = (v: unknown): number | undefined =>
  typeof v === "number" && Number.isFinite(v) ? v : undefined;

/// Collect the keys of `src` not in `known` into a plain object (or undefined if
/// none) — the bag of fields preserved verbatim through the round-trip.
function extraKeys(src: Record<string, unknown>, known: (k: string) => boolean): Record<string, unknown> | undefined {
  const out: Record<string, unknown> = {};
  for (const k of Object.keys(src)) if (!known(k)) out[k] = src[k];
  return Object.keys(out).length ? out : undefined;
}

/// Parse a YAML spec into the editable model, or return an error string.
export function parseModel(specYaml: string): { model?: WorkflowModel; error?: string } {
  let doc: Record<string, unknown>;
  try {
    doc = (yaml.load(specYaml) ?? {}) as Record<string, unknown>;
  } catch (e) {
    return { error: e instanceof Error ? e.message : "invalid YAML" };
  }
  if (!doc || typeof doc !== "object" || !Array.isArray(doc.tasks)) {
    return { error: "spec must have a `tasks:` list" };
  }
  const tasks: Task[] = [];
  const seen = new Set<string>();
  for (const rawUnknown of doc.tasks as unknown[]) {
    const raw = rawUnknown as Record<string, unknown>;
    if (!raw || typeof raw.name !== "string") {
      return { error: "every task needs a string `name`" };
    }
    // Names are the graph node ids, so they must be unique.
    if (seen.has(raw.name)) {
      return { error: `duplicate task name: ${raw.name}` };
    }
    seen.add(raw.name);
    const command = Array.isArray(raw.command) ? raw.command.map((c) => String(c)) : [];
    const depends_on = Array.isArray(raw.depends_on)
      ? raw.depends_on.filter((d): d is string => typeof d === "string")
      : [];
    tasks.push({
      name: raw.name,
      command,
      depends_on,
      max_attempts: num(raw.max_attempts),
      retry_delay_secs: num(raw.retry_delay_secs),
      timeout_secs: num(raw.timeout_secs),
      docker_image: typeof raw.docker_image === "string" ? raw.docker_image : undefined,
      trigger_rule: typeof raw.trigger_rule === "string" ? raw.trigger_rule : undefined,
      workflow_ref: typeof raw.workflow_ref === "string" ? raw.workflow_ref : undefined,
      _extra: extraKeys(raw, (k) => KNOWN_TASK_KEYS.has(k)),
    });
  }
  return {
    model: {
      name: typeof doc.name === "string" ? doc.name : "",
      tasks,
      _extra: extraKeys(doc, (k) => k === "name" || k === "tasks"),
    },
  };
}

/// Serialize the model back to YAML, omitting empty/default optionals so the
/// output stays clean and diff-friendly. Unmodeled keys in `_extra` are merged
/// back so nothing the editor doesn't understand is lost.
export function modelToYaml(model: WorkflowModel): string {
  const obj: Record<string, unknown> = {};
  if (model.name) obj.name = model.name;
  for (const [k, v] of Object.entries(model._extra ?? {})) {
    if (k !== "name" && k !== "tasks") obj[k] = v;
  }
  obj.tasks = model.tasks.map((t) => {
    const o: Record<string, unknown> = { name: t.name };
    // Preserve authored values losslessly: emit workflow_ref whenever present
    // (even ""), and command unless an empty array is superseded by a ref or by
    // an approval gate (a command-less leaf by definition — `command: []` would
    // be a semantic no-op the engine tolerates but the docs say not to write).
    const isApproval = t._extra?.type === "approval";
    if (t.workflow_ref !== undefined) o.workflow_ref = t.workflow_ref;
    if (t.command.length > 0 || (t.workflow_ref === undefined && !isApproval)) o.command = t.command;
    if (t.depends_on.length) o.depends_on = t.depends_on;
    if (t.max_attempts != null) o.max_attempts = t.max_attempts;
    if (t.retry_delay_secs != null) o.retry_delay_secs = t.retry_delay_secs;
    if (t.timeout_secs != null) o.timeout_secs = t.timeout_secs;
    if (t.docker_image) o.docker_image = t.docker_image;
    if (t.trigger_rule) o.trigger_rule = t.trigger_rule;
    for (const [k, v] of Object.entries(t._extra ?? {})) {
      if (!KNOWN_TASK_KEYS.has(k)) o[k] = v;
    }
    return o;
  });
  return yaml.dump(obj, { lineWidth: 100, noRefs: true });
}

/// Tokenize a command line into argv, respecting single/double quotes and
/// backslash escapes so commands like `python -c "print(\"x y\")"` round-trip.
/// Single quotes are literal; double quotes and unquoted runs honor `\` escapes.
export function parseCommand(line: string): string[] {
  const out: string[] = [];
  let cur = "";
  let started = false; // distinguishes "" (empty arg) from no arg
  let quote: '"' | "'" | null = null;
  for (let i = 0; i < line.length; i++) {
    const ch = line[i];
    if (quote === "'") {
      if (ch === "'") quote = null;
      else cur += ch;
      continue;
    }
    if (quote === '"') {
      if (ch === "\\" && (line[i + 1] === '"' || line[i + 1] === "\\")) {
        cur += line[++i];
      } else if (ch === '"') {
        quote = null;
      } else {
        cur += ch;
      }
      continue;
    }
    // unquoted
    if (ch === "\\" && i + 1 < line.length) {
      cur += line[++i];
      started = true;
    } else if (ch === '"' || ch === "'") {
      quote = ch;
      started = true;
    } else if (/\s/.test(ch)) {
      if (started) {
        out.push(cur);
        cur = "";
        started = false;
      }
    } else {
      cur += ch;
      started = true;
    }
  }
  if (started || quote) out.push(cur);
  return out;
}

/// Render argv back to a single editable line. Args with whitespace, quotes, or
/// backslashes are double-quoted with `"` and `\` escaped, so parseCommand round-trips.
export function formatCommand(argv: string[]): string {
  return argv
    .map((a) => {
      if (a === "") return '""';
      if (!/[\s"'\\]/.test(a)) return a;
      return `"${a.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
    })
    .join(" ");
}

/// True if adding `from -> to` (to depends_on from) would create a cycle, or if
/// the edge is a self-loop. Used to reject illegal dependency edges.
export function wouldCycle(tasks: Task[], from: string, to: string): boolean {
  if (from === to) return true;
  // Edge means `to depends_on from`. A cycle exists if `from` already (transitively)
  // depends on `to`. Walk dependencies of `from`.
  const byName = new Map(tasks.map((t) => [t.name, t]));
  const seen = new Set<string>();
  const stack = [from];
  while (stack.length) {
    const cur = stack.pop()!;
    if (cur === to) return true;
    if (seen.has(cur)) continue;
    seen.add(cur);
    const t = byName.get(cur);
    if (t) stack.push(...t.depends_on);
  }
  return false;
}

/// Splice `task` (already uniquely named) into the dependency edge
/// `source -> target`: the new task depends on `source`, and `target`'s
/// dependency on `source` is rewired to the new task, so `source -> task ->
/// target`. Other tasks that depend on `source` keep their direct edge. The
/// result is acyclic whenever the input was: the new node's only edges replace
/// an existing path.
export function spliceTask(tasks: Task[], source: string, target: string, task: Task): Task[] {
  return [
    ...tasks.map((t) =>
      t.name === target
        ? { ...t, depends_on: t.depends_on.map((d) => (d === source ? task.name : d)) }
        : t,
    ),
    { ...task, depends_on: [source] },
  ];
}

/// Generate a fresh unique task name ("task-1", "task-2", …).
export function nextTaskName(tasks: Task[]): string {
  const names = new Set(tasks.map((t) => t.name));
  for (let i = 1; ; i++) {
    const n = `task-${i}`;
    if (!names.has(n)) return n;
  }
}
