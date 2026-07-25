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
  /// Call a `templates:` sub-DAG declared in the same spec (engine `template`) —
  /// the DAG-of-DAGs pattern. Like `workflow_ref` this is a *call*, not a leaf:
  /// mutually exclusive with `command`, and expanded into the template's tasks
  /// (named `<task>.<inner>`) when the run is created.
  template?: string;
  /// Arguments for the called template, filling its declared `parameters`.
  /// Only meaningful with `template`.
  arguments?: Record<string, string>;
  /// Spec keys this editor doesn't model (e.g. `env`, `input`, `type: approval`
  /// knobs) preserved verbatim so the visual round-trip is lossless — switching
  /// to the visual tab never silently drops a field. Fields the *visual model*
  /// cannot honestly represent (fan-out, sensors, …) don't rely on this: they
  /// lock the visual tab outright, see `spec-support.ts`.
  _extra?: Record<string, unknown>;
}

/// A `templates:` entry — a reusable sub-DAG a task calls with `template:`.
/// Its tasks live in their own namespace (`depends_on` inside a template refers
/// to the template's own tasks), so they are modeled but edited separately from
/// the main graph.
export interface Template {
  name: string;
  /// Default parameter values, overridable per call via the task's `arguments`.
  parameters?: Record<string, string>;
  tasks: Task[];
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
  /// Reusable sub-DAGs (`templates:`) callable from a task's `template:`.
  templates: Template[];
  /// Top-level spec keys this editor doesn't model (e.g. `parameters`, `tags`),
  /// preserved verbatim across the visual round-trip.
  _extra?: Record<string, unknown>;
}

/// Task keys the editor models directly; everything else rides in `Task._extra`.
export const KNOWN_TASK_KEYS = new Set([
  "name",
  "command",
  "depends_on",
  "max_attempts",
  "retry_delay_secs",
  "timeout_secs",
  "docker_image",
  "trigger_rule",
  "workflow_ref",
  "template",
  "arguments",
]);

/// Top-level keys the editor models directly.
export const KNOWN_SPEC_KEYS = new Set(["name", "tasks", "templates"]);

const num = (v: unknown): number | undefined =>
  typeof v === "number" && Number.isFinite(v) ? v : undefined;

/// Collect the keys of `src` not in `known` into a plain object (or undefined if
/// none) — the bag of fields preserved verbatim through the round-trip.
function extraKeys(src: Record<string, unknown>, known: (k: string) => boolean): Record<string, unknown> | undefined {
  const out: Record<string, unknown> = {};
  for (const k of Object.keys(src)) if (!known(k)) out[k] = src[k];
  return Object.keys(out).length ? out : undefined;
}

/// A string→string map (a `parameters:` / `arguments:` block), or undefined when
/// the value isn't one. Non-string scalars are stringified — YAML `count: 3`
/// parses as a number but the engine's parameter maps are string-valued.
function strMap(v: unknown): Record<string, string> | undefined {
  if (!v || typeof v !== "object" || Array.isArray(v)) return undefined;
  const out: Record<string, string> = {};
  for (const [k, val] of Object.entries(v as Record<string, unknown>)) {
    if (val != null && typeof val !== "object") out[k] = String(val);
  }
  return out;
}

/// Parse one task entry. Returns an error string instead of a task when the
/// entry is unusable as a graph node.
function parseTask(rawUnknown: unknown): { task?: Task; error?: string } {
  const raw = rawUnknown as Record<string, unknown>;
  if (!raw || typeof raw.name !== "string") {
    return { error: "every task needs a string `name`" };
  }
  const command = Array.isArray(raw.command) ? raw.command.map((c) => String(c)) : [];
  const depends_on = Array.isArray(raw.depends_on)
    ? raw.depends_on.filter((d): d is string => typeof d === "string")
    : [];
  return {
    task: {
      name: raw.name,
      command,
      depends_on,
      max_attempts: num(raw.max_attempts),
      retry_delay_secs: num(raw.retry_delay_secs),
      timeout_secs: num(raw.timeout_secs),
      docker_image: typeof raw.docker_image === "string" ? raw.docker_image : undefined,
      trigger_rule: typeof raw.trigger_rule === "string" ? raw.trigger_rule : undefined,
      workflow_ref: typeof raw.workflow_ref === "string" ? raw.workflow_ref : undefined,
      template: typeof raw.template === "string" ? raw.template : undefined,
      arguments: strMap(raw.arguments),
      _extra: extraKeys(raw, (k) => KNOWN_TASK_KEYS.has(k)),
    },
  };
}

/// Parse a task list, rejecting duplicate names (they are the graph node ids).
function parseTasks(rawTasks: unknown[]): { tasks?: Task[]; error?: string } {
  const tasks: Task[] = [];
  const seen = new Set<string>();
  for (const rawUnknown of rawTasks) {
    const { task, error } = parseTask(rawUnknown);
    if (!task) return { error };
    if (seen.has(task.name)) return { error: `duplicate task name: ${task.name}` };
    seen.add(task.name);
    tasks.push(task);
  }
  return { tasks };
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
  const { tasks, error } = parseTasks(doc.tasks as unknown[]);
  if (!tasks) return { error };

  const templates: Template[] = [];
  if (doc.templates !== undefined) {
    if (!Array.isArray(doc.templates)) {
      return { error: "`templates:` must be a list" };
    }
    const seen = new Set<string>();
    for (const rawUnknown of doc.templates as unknown[]) {
      const raw = rawUnknown as Record<string, unknown>;
      if (!raw || typeof raw.name !== "string") {
        return { error: "every template needs a string `name`" };
      }
      if (seen.has(raw.name)) return { error: `duplicate template name: ${raw.name}` };
      seen.add(raw.name);
      if (!Array.isArray(raw.tasks)) {
        return { error: `template '${raw.name}' needs a \`tasks:\` list` };
      }
      const inner = parseTasks(raw.tasks as unknown[]);
      if (!inner.tasks) return { error: `template '${raw.name}': ${inner.error}` };
      templates.push({
        name: raw.name,
        parameters: strMap(raw.parameters),
        tasks: inner.tasks,
        _extra: extraKeys(raw, (k) => k === "name" || k === "parameters" || k === "tasks"),
      });
    }
  }

  return {
    model: {
      name: typeof doc.name === "string" ? doc.name : "",
      tasks,
      templates,
      _extra: extraKeys(doc, (k) => KNOWN_SPEC_KEYS.has(k)),
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
    if (!KNOWN_SPEC_KEYS.has(k)) obj[k] = v;
  }
  // Templates precede the tasks that call them, as in the engine's examples.
  if (model.templates.length) {
    obj.templates = model.templates.map((tpl) => {
      const o: Record<string, unknown> = { name: tpl.name };
      if (tpl.parameters && Object.keys(tpl.parameters).length) o.parameters = tpl.parameters;
      for (const [k, v] of Object.entries(tpl._extra ?? {})) o[k] = v;
      o.tasks = tpl.tasks.map(taskToYaml);
      return o;
    });
  }
  obj.tasks = model.tasks.map(taskToYaml);
  return yaml.dump(obj, { lineWidth: 100, noRefs: true });
}

/// Serialize one task. Preserves authored values losslessly: a call field
/// (`workflow_ref` / `template`) is emitted whenever present (even ""), and
/// `command` unless an empty array is superseded by a call or by an approval
/// gate (a command-less leaf by definition — `command: []` would be a semantic
/// no-op the engine tolerates but the docs say not to write).
function taskToYaml(t: Task): Record<string, unknown> {
  const o: Record<string, unknown> = { name: t.name };
  const isApproval = t._extra?.type === "approval";
  const isCall = t.workflow_ref !== undefined || t.template !== undefined;
  if (t.workflow_ref !== undefined) o.workflow_ref = t.workflow_ref;
  if (t.template !== undefined) o.template = t.template;
  if (t.arguments && Object.keys(t.arguments).length) o.arguments = t.arguments;
  if (t.command.length > 0 || (!isCall && !isApproval)) o.command = t.command;
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
