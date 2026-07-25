// Can the visual editor honestly edit this spec?
//
// The visual editor draws one node per task and edits a fixed set of fields.
// Anything it doesn't model is carried verbatim through the round-trip
// (`Task._extra`), which keeps *saving* lossless — but lossless is not the same
// as honest. A `with_items:` task is one node on the canvas and N tasks at run
// time; a `type: wait` sensor has no command but the panel offers a command box;
// a `hook:` task is wired to every other task by edges the canvas never draws.
// Editing those is how a spec gets quietly corrupted.
//
// So this module answers one question — is the drawn graph the graph that runs?
// — and the editor refuses Visual mode when the answer is no, pointing at the
// YAML tab instead. The rule is an **allowlist**: a key is fine only if the
// editor models it (a panel field), the block palette emits it, or it is inert
// metadata no panel field contradicts. A field nobody here has heard of — a new
// engine feature, a typo — locks the tab rather than being silently editable.

import yaml from "js-yaml";
import { KNOWN_SPEC_KEYS, KNOWN_TASK_KEYS } from "@/lib/spec-model";

/// Task keys the editor doesn't put in the panel but can carry without the
/// canvas telling a lie: one task in, one task out, and no panel field means
/// something different because of them. Most are emitted by the block palette.
const CARRIED_TASK_KEYS = new Set([
  "type", // value-checked below: approval/task only
  "approval_timeout_secs",
  "approval_on_timeout",
  "retry_max_delay_secs",
  "retry_on_timeout",
  "when", // conditional skip — still exactly one node
  "repeat", // re-runs in place — still exactly one node
  "allow_failure",
  "priority",
  "pool",
  "runner_class",
  "service_account",
  "produces",
  "env",
  "input",
]);

/// Task keys that make the drawn graph wrong, with the reason shown to the user.
/// (Keys absent from every set are reported as unknown — same lock, different
/// message.) Kept in the engine's vocabulary so the message matches the YAML.
const UNSUPPORTED_TASK_KEYS: Record<string, string> = {
  with_items: "fans out into one task per item — the canvas would draw a single node",
  with_param: "fans out into one task per item — the canvas would draw a single node",
  instance_key: "labels fan-out instances, which the canvas doesn't draw",
  gang: "expands into co-scheduled member tasks the canvas doesn't draw",
  gang_member: "is engine-internal gang bookkeeping, not an authored field",
  hook: "is auto-wired to every other task by edges the canvas doesn't draw",
  wait: "is a deferred sensor, not a command step",
  workflow: "triggers a child run, not a command step",
  cache: "can skip execution entirely, which the canvas doesn't show",
  resources: "sets pod CPU/memory the editor can't show or change",
};

/// Top-level keys the editor doesn't model but carries verbatim.
const CARRIED_SPEC_KEYS = new Set([
  "parameters",
  "tags",
  "run_timeout_secs",
  "max_active_runs",
  "deadline",
  "notify",
  "result_from",
  "runner_class",
  "environment",
  "task_defaults",
  "on_datasets",
  "datasets_mode",
]);

/// `type:` values the editor understands. `approval` is a palette block; `task`
/// is the ordinary default. `wait` / `workflow` are command-less kinds whose
/// panel would offer a command box — they lock instead.
const SUPPORTED_TASK_TYPES = new Set(["approval", "task"]);

export interface VisualSupport {
  /// True when every field in the spec is one the visual editor can edit or
  /// safely carry — i.e. the graph it draws is the graph that runs.
  ok: boolean;
  /// One human-readable line per blocking field, e.g.
  /// `task 'shard': with_items fans out into one task per item …`.
  /// Empty when `ok`.
  reasons: string[];
}

const SUPPORTED: VisualSupport = { ok: true, reasons: [] };

/// Inspect a raw YAML spec. A spec that doesn't parse is *not* reported as
/// unsupported — the editor already has a "can't render graph" path for that,
/// and a half-typed spec must not latch the tab shut.
export function visualSupport(specYaml: string): VisualSupport {
  let doc: Record<string, unknown>;
  try {
    doc = (yaml.load(specYaml) ?? {}) as Record<string, unknown>;
  } catch {
    return SUPPORTED;
  }
  if (!doc || typeof doc !== "object" || !Array.isArray(doc.tasks)) return SUPPORTED;

  const reasons: string[] = [];
  for (const key of Object.keys(doc)) {
    if (KNOWN_SPEC_KEYS.has(key) || CARRIED_SPEC_KEYS.has(key)) continue;
    reasons.push(`\`${key}:\` is a top-level field the visual editor doesn't know`);
  }
  for (const t of doc.tasks as unknown[]) {
    reasons.push(...taskReasons(t));
  }
  // A template's tasks are a sub-DAG the canvas doesn't draw, but the editor
  // never edits them either — it edits the *call*. They still have to be
  // representable, or "expands to 3 tasks" on the call node is a lie.
  if (Array.isArray(doc.templates)) {
    for (const tplRaw of doc.templates as unknown[]) {
      const tpl = tplRaw as Record<string, unknown>;
      const label = typeof tpl?.name === "string" ? tpl.name : "?";
      if (!Array.isArray(tpl?.tasks)) continue;
      for (const t of tpl.tasks as unknown[]) {
        reasons.push(...taskReasons(t, true).map((r) => `template '${label}': ${r}`));
      }
    }
  }
  return reasons.length ? { ok: false, reasons: dedupe(reasons) } : SUPPORTED;
}

function taskReasons(raw: unknown, insideTemplate = false): string[] {
  const t = raw as Record<string, unknown>;
  if (!t || typeof t !== "object") return [];
  const name = typeof t.name === "string" ? t.name : "?";
  const out: string[] = [];
  for (const key of Object.keys(t)) {
    // `workflow_ref` is a valid top-level chain, but the server rejects it
    // *inside* a template (the chain expander only walks top-level tasks — see
    // control.rs `validate_templates`). Accepting it here would leave the Visual
    // tab unlocked on a spec that 400s on Save — the corruption this gate exists
    // to prevent.
    if (insideTemplate && key === "workflow_ref") {
      out.push(
        `task '${name}': \`workflow_ref\` chains a saved workflow, which is only supported on top-level tasks — not inside a template`,
      );
      continue;
    }
    const why = UNSUPPORTED_TASK_KEYS[key];
    if (why) {
      out.push(`task '${name}': \`${key}\` ${why}`);
    } else if (!KNOWN_TASK_KEYS.has(key) && !CARRIED_TASK_KEYS.has(key)) {
      out.push(`task '${name}': \`${key}\` is a field the visual editor doesn't know`);
    }
  }
  if (typeof t.type === "string" && !SUPPORTED_TASK_TYPES.has(t.type)) {
    out.push(`task '${name}': \`type: ${t.type}\` is a task kind the visual editor can't edit`);
  }
  return out;
}

/// The same line can be produced twice (a template called from two places, a
/// key repeated in a nested list); the user needs it once.
function dedupe(reasons: string[]): string[] {
  return [...new Set(reasons)];
}
