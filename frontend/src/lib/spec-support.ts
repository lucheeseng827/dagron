// Can the visual editor honestly edit this spec?
//
// The visual editor draws one node per task and edits a fixed set of fields.
// Anything it doesn't model is carried verbatim through the round-trip
// (`Task._extra`), which keeps *saving* lossless — but lossless is not the same
// as honest. A `type: wait` sensor has no command but the panel offers a command
// box; a `hook:` task is wired to every other task by edges the canvas never
// draws. Editing those is how a spec gets quietly corrupted.
//
// "One node, many rows" is *not* on that list, and deliberately so. A template
// call has always drawn as a single node reading `sub-DAG · 3 tasks`, because a
// node that says what it expands to is not pretending to be a leaf. A loop is
// the same bargain: `with_items:` draws one node badged `⟳ ×3 parallel`, and
// `repeat:` draws one node badged `⟳ 3× in place` — which is also the literal
// truth about `repeat`, since it really is one row run repeatedly. What made
// fan-out dishonest before was the *silence*: an unbadged node claiming to be a
// single task. The badge is what pays for the unlock, so it is load-bearing —
// see `describeLoop` in `loop-model.ts` and the loop row in `StatusNode`.
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
  out.push(...loopReasons(t, name));
  return out;
}

/// Value-shape checks for the loop keys. These are *modeled* fields, so unlike
/// an unknown key they are not carried in `_extra` — `parseModel` reads them
/// into typed slots and `modelToYaml` writes those slots back. A value in the
/// wrong shape reads as nothing and would therefore be **dropped** on the
/// round-trip, which is the one outcome this module exists to prevent. So a
/// misshapen loop locks the tab instead, the same way `type:` does above.
function loopReasons(t: Record<string, unknown>, name: string): string[] {
  const out: string[] = [];
  const has = (k: string) => t[k] !== undefined && t[k] !== null;
  if (has("with_items") && !Array.isArray(t.with_items)) {
    out.push(`task '${name}': \`with_items\` must be a list for the visual editor to show the fan-out`);
  }
  for (const k of ["with_param", "with_output_of", "instance_key"]) {
    if (has(k) && typeof t[k] !== "string") {
      out.push(`task '${name}': \`${k}\` must be a string for the visual editor to show the fan-out`);
    }
  }
  // The engine rejects this pair outright ("sets both with_items and
  // with_param"); the editor's loop model has one source, so it would silently
  // keep one and drop the other on the way back out.
  const sources = ["with_items", "with_param", "with_output_of"].filter(has);
  if (sources.length > 1) {
    out.push(
      `task '${name}': ${sources.map((k) => `\`${k}\``).join(" and ")} are set together — a task fans out from one source`,
    );
  }
  if (has("repeat")) out.push(...repeatReasons(t.repeat, name));
  return out;
}

/// The keys `Task.repeat` models. Anything else inside a `repeat:` block is
/// dropped by `parseRepeat`, so its presence has to lock the tab.
const REPEAT_KEYS = new Set(["until", "max_iterations", "delay_secs"]);

/// Why this `repeat:` block can't round-trip, or nothing when it can.
///
/// Held to exactly what `parseRepeat` accepts, key for key — this check and
/// that parser are two halves of one contract, and every gap between them is a
/// field that reads as nothing and is therefore **deleted** on the way back
/// out. `typeof x === "number"` is the subtle one: it is true of `NaN` and
/// `Infinity`, which `parseRepeat` rejects via `Number.isFinite`, so a spec
/// with `max_iterations: .nan` would pass this gate and lose its whole
/// `repeat:` block to a Visual-mode save.
function repeatReasons(raw: unknown, name: string): string[] {
  const where = `task '${name}': \`repeat\``;
  if (!raw || typeof raw !== "object" || Array.isArray(raw)) {
    return [`${where} must be a mapping for the visual editor to show the loop`];
  }
  const r = raw as Record<string, unknown>;
  const finite = (v: unknown) => typeof v === "number" && Number.isFinite(v);
  const out: string[] = [];
  if (typeof r.until !== "string") out.push(`${where}.until must be a string`);
  if (!finite(r.max_iterations)) {
    out.push(`${where}.max_iterations must be a finite number`);
  }
  if (r.delay_secs !== undefined && r.delay_secs !== null && !finite(r.delay_secs)) {
    out.push(`${where}.delay_secs must be a finite number`);
  }
  const extra = Object.keys(r).filter((k) => !REPEAT_KEYS.has(k));
  if (extra.length) {
    out.push(`${where} has ${extra.map((k) => `\`${k}\``).join(", ")}, which the visual editor doesn't model`);
  }
  return out;
}

/// The same line can be produced twice (a template called from two places, a
/// key repeated in a nested list); the user needs it once.
function dedupe(reasons: string[]): string[] {
  return [...new Set(reasons)];
}
