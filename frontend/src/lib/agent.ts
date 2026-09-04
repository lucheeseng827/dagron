"use client";

// The agent dock's data layer: is a generator reachable, and what has been
// asked of it.
//
// Two things this file is deliberately honest about, because the alternative is
// a panel that lies:
//
//  1. **There is no generator behind this API.** Natural-language → DAG ships
//     outside this build and is reached there through an MCP tool;
//     `dagron-api` exposes no route for it in any build
//     (docs/MCP.md, "Roadmap to 1.0"). So the dock *probes* for one and adapts
//     rather than assuming: with a generator, a prompt is sent and answered
//     here; without one, the prompt is composed and kept here and handed to
//     whatever agent the operator drives dagron with.
//
//  2. **A run does not yet say which agent submitted it.** `trigger_kind` is
//     `manual | schedule | backfill`; the principal that G-AG4 adds is not on
//     the run record. Until it is, "what the agent is working on" can only
//     honestly mean "what arrived through the API" — which is what the activity
//     list shows, and what its caption says.

import { probeAiGenerator } from "@/lib/dagron-api";

/// **The dock is dormant until an agent session is a complete cycle.**
///
/// A prompt box that cannot be answered is worse than no prompt box: it teaches
/// an operator the feature is broken rather than absent. So the panel is built,
/// reviewable and off — every surface that opens it (the tab, the sidebar
/// button, the ⌘K action) reads this constant, and none of them render while it
/// is false.
///
/// Flip it when all of these are true (docs/MCP.md, "1.x — the console as an
/// agent surface"):
///
///   1. `examples/ai/agent_turn.yaml` takes a `prompt` parameter.
///   2. Its `think` step is a real model call, not the shell stand-in — the
///      case-study-04 pattern, or a hardened LLM task binary, which this
///      build does not carry.
///   3. Send submits a conversation (`POST /api/runs`) and the dock follows the
///      run's SSE stream, rendering each turn as its child run lands.
///   4. Approval gates inside a conversation are resolvable from the dock.
///
/// A build may opt in early with `NEXT_PUBLIC_AGENT_DOCK=on` — for developing
/// the above, not for shipping it.
export const AGENT_DOCK_ENABLED = process.env.NEXT_PUBLIC_AGENT_DOCK === "on";

const HISTORY_KEY = "dagron.agentHistory";
const HISTORY_EVT = "dagron:agent-history";
const MAX_HISTORY = 50;

export type GeneratorState = "probing" | "available" | "absent";

export interface PromptEntry {
  id: string;
  /// Epoch millis — rendered relative, stored absolute.
  at: number;
  text: string;
  /// The generator's answer, when there was one to receive.
  reply?: string;
  /// Set when the send failed for a reason worth showing.
  error?: string;
}

/// Probe once for a natural-language generator behind dagron-api. The dock
/// asks on mount and shapes its composer around the answer.
export async function probeGenerator(): Promise<GeneratorState> {
  return (await probeAiGenerator()) ? "available" : "absent";
}

function readHistory(): PromptEntry[] {
  try {
    const raw = localStorage.getItem(HISTORY_KEY);
    if (!raw) return [];
    const parsed: unknown = JSON.parse(raw);
    if (!Array.isArray(parsed)) return [];
    // Stored by an older build, hand-edited, or corrupted: keep only entries
    // that still have every field a renderer reads. `at` is one of them —
    // `new Date(undefined).toISOString()` throws a RangeError, so an entry
    // missing it would take the whole dock down on open rather than render
    // badly.
    return parsed.filter(
      (e): e is PromptEntry =>
        !!e &&
        typeof (e as PromptEntry).id === "string" &&
        typeof (e as PromptEntry).text === "string" &&
        typeof (e as PromptEntry).at === "number" &&
        Number.isFinite((e as PromptEntry).at),
    );
  } catch {
    return [];
  }
}

function writeHistory(entries: PromptEntry[]) {
  const bounded = entries.slice(0, MAX_HISTORY);
  try {
    localStorage.setItem(HISTORY_KEY, JSON.stringify(bounded));
    // Persisted, so the store is the truth again and the fallback must go —
    // left set, it would shadow writes from every other tab.
    volatile = null;
  } catch {
    // Quota or private mode. The event alone was not enough: consumers answer
    // it by re-reading the store, which still holds the PREVIOUS value, so the
    // prompt just typed would vanish on the render that announced it. Hold the
    // session's history here instead and let `historySnapshot` prefer it.
    volatile = bounded;
  }
  window.dispatchEvent(new Event(HISTORY_EVT));
}

export function subscribeHistory(cb: () => void): () => void {
  window.addEventListener("storage", cb);
  window.addEventListener(HISTORY_EVT, cb);
  return () => {
    window.removeEventListener("storage", cb);
    window.removeEventListener(HISTORY_EVT, cb);
  };
}

/// Cached so `useSyncExternalStore` sees a stable reference between changes —
/// re-parsing on every render would hand it a new array each time and spin.
let cache: PromptEntry[] | null = null;
let cacheRaw: string | null = null;

/// The session's history when persistence is refused (quota, private mode).
/// Non-null only while the store is behind, and it is a stable reference for as
/// long as it is set, which is what `useSyncExternalStore` needs.
let volatile: PromptEntry[] | null = null;

export function historySnapshot(): PromptEntry[] {
  // Prefer the fallback: while it is set the store is known to be stale, and a
  // reader that trusted the store would drop what this session has typed.
  if (volatile) return volatile;
  let raw: string | null = null;
  try {
    raw = localStorage.getItem(HISTORY_KEY);
  } catch {
    raw = null;
  }
  if (raw !== cacheRaw || cache == null) {
    cacheRaw = raw;
    cache = readHistory();
  }
  return cache;
}

export function addPrompt(text: string): PromptEntry {
  const entry: PromptEntry = {
    id: `${Date.now()}-${Math.random().toString(36).slice(2, 8)}`,
    at: Date.now(),
    text,
  };
  writeHistory([entry, ...historySnapshot()]);
  return entry;
}

export function updatePrompt(id: string, patch: Partial<PromptEntry>) {
  writeHistory(historySnapshot().map((e) => (e.id === id ? { ...e, ...patch } : e)));
}

export function clearHistory() {
  writeHistory([]);
}
