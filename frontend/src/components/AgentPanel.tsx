"use client";

// The agent dock — a right-hand rail for driving dagron with an agent and
// watching what it did.
//
// Two halves, because an operator working with an agent has two questions and
// they are not the same question:
//
//   ACTIVITY  what has arrived through the API, live. This is the honest
//             answer to "what is the agent working on" until G-AG4 puts the
//             calling principal on the run record: a run's `trigger_kind` is
//             `manual | schedule | backfill`, and everything an MCP client
//             submits lands as `manual` alongside anything a human posted.
//             The caption says so rather than implying attribution we do not
//             have.
//
//   PROMPT    compose an instruction, keep the history. With a generator
//             mounted at /ai/generate the prompt is answered here. Without one
//             — every build today — Send is not shown at all; the button
//             copies the prompt for the agent that *does* hold the MCP session,
//             which is the flow that actually works right now.
//
// Nothing in here polls: the activity list reuses the shared SSE stream through
// `useLiveRefresh`, so an open dock costs one subscription that the run pages
// were already holding.

import Link from "next/link";
import { useCallback, useEffect, useRef, useState, useSyncExternalStore } from "react";
import {
  addPrompt,
  AGENT_DOCK_ENABLED,
  clearHistory,
  historySnapshot,
  probeGenerator,
  subscribeHistory,
  updatePrompt,
  type GeneratorState,
} from "@/lib/agent";
import { aiGenerate, listRuns } from "@/lib/dagron-api";
import { statusColor } from "@/lib/adapter";
import { useLiveRefresh, useLiveUpdates } from "@/lib/live";
import { useAgentDock } from "@/lib/shell-prefs";
import { timeAgo } from "@/lib/time";
import type { RunSummary } from "@/types/dagron";

const EMPTY: RunSummary[] = [];
/// Stable empty array for the SSR/first-render history snapshot — a fresh `[]`
/// each call would make `useSyncExternalStore` see a new value every render.
const EMPTY_HISTORY: ReturnType<typeof historySnapshot> = [];

export default function AgentPanel() {
  const [dockPref, setOpen] = useAgentDock();
  // A stored `on` from a build where the dock was enabled must not keep the
  // probe and the SSE subscription alive in one where it is not — the panel
  // renders nothing, so the work behind it would be invisible as well as
  // pointless. Every effect below reads this, not the raw preference.
  const open = AGENT_DOCK_ENABLED && dockPref;
  const [runs, setRuns] = useState<RunSummary[]>(EMPTY);
  const [gen, setGen] = useState<GeneratorState>("probing");
  const [draft, setDraft] = useState("");
  const [busy, setBusy] = useState(false);
  const [copied, setCopied] = useState<string | null>(null);
  const [live] = useLiveUpdates();
  const taRef = useRef<HTMLTextAreaElement | null>(null);

  const history = useSyncExternalStore(subscribeHistory, historySnapshot, () => EMPTY_HISTORY);

  // Ask once per mount whether a generator is reachable. Cheap, and the answer
  // decides which composer the operator gets.
  useEffect(() => {
    if (!open) return;
    let alive = true;
    void probeGenerator().then((s) => alive && setGen(s));
    return () => {
      alive = false;
    };
  }, [open]);

  const refetch = useCallback(() => {
    // `manual` is every API submission — an agent's and a human's alike. See
    // the note at the top of this file.
    listRuns({ trigger: "manual", limit: 12 })
      .then(setRuns)
      .catch((e) => console.warn("AgentPanel: listRuns failed", e));
  }, []);

  useEffect(() => {
    if (open) refetch();
  }, [open, refetch]);
  useLiveRefresh(open && live, refetch);

  async function send() {
    const text = draft.trim();
    if (!text || busy) return;
    const entry = addPrompt(text);
    setDraft("");
    if (gen !== "available") {
      // Copy-only mode: the entry is the record, and the button says
      // "Save & copy" — so it has to reach the clipboard too, or the label is
      // a promise the handler does not keep.
      await copy(entry.id, text);
      return;
    }
    setBusy(true);
    try {
      const yaml = await aiGenerate(text);
      updatePrompt(entry.id, { reply: yaml });
    } catch (e) {
      updatePrompt(entry.id, { error: e instanceof Error ? e.message : "generation failed" });
    } finally {
      setBusy(false);
    }
  }

  async function copy(id: string, text: string) {
    try {
      await navigator.clipboard.writeText(text);
      setCopied(id);
      setTimeout(() => setCopied((c) => (c === id ? null : c)), 1400);
    } catch {
      // Clipboard denied (insecure origin, permissions) — select it instead so
      // the operator can copy by hand rather than being told nothing happened.
      taRef.current?.focus();
    }
  }

  // Dormant: no tab, no probe, no subscription. See AGENT_DOCK_ENABLED for the
  // list of what has to land before this turns on.
  if (!AGENT_DOCK_ENABLED) return null;

  if (!open) {
    return (
      <button
        type="button"
        className="dy-dock-tab"
        onClick={() => setOpen(true)}
        title="Open the agent dock"
        aria-label="Open the agent dock"
      >
        <AgentGlyph />
      </button>
    );
  }

  return (
    <aside className="dy-dock" aria-label="Agent">
      <header className="dy-dock-head">
        <AgentGlyph />
        <div style={{ lineHeight: 1.2, flex: 1, minWidth: 0 }}>
          <div style={{ fontSize: 13, fontWeight: 600 }}>Agent</div>
          <div className="mono" style={{ fontSize: 10, color: "var(--dim)" }}>
            {gen === "probing"
              ? "checking for a generator…"
              : gen === "available"
                ? "generator connected"
                : "no generator — compose and copy"}
          </div>
        </div>
        <button
          type="button"
          className="dy-dock-x"
          onClick={() => setOpen(false)}
          title="Close"
          aria-label="Close the agent dock"
        >
          <svg viewBox="0 0 24 24" width="15" height="15" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round">
            <line x1="6" y1="6" x2="18" y2="18" />
            <line x1="18" y1="6" x2="6" y2="18" />
          </svg>
        </button>
      </header>

      <section className="dy-dock-sect">
        <div className="dy-dock-label">
          Activity
          <span className="dy-dock-hint" title="Runs created through the API. A run does not yet record which principal submitted it, so an agent's runs and a human's appear together.">
            API submissions
          </span>
        </div>
        {runs.length === 0 ? (
          <p className="dy-dock-empty">Nothing submitted through the API yet.</p>
        ) : (
          <ul className="dy-dock-runs">
            {runs.map((r) => (
              <li key={r.id}>
                <Link href={`/runs/detail/?id=${encodeURIComponent(r.id)}`} className="dy-dock-run">
                  <span className="dy-dock-dot" style={{ background: statusColor(r.status) }} />
                  <span className="dy-dock-run-name">{r.name ?? "—"}</span>
                  <span className="dy-dock-run-when mono">{timeAgo(r.created_at)}</span>
                </Link>
              </li>
            ))}
          </ul>
        )}
      </section>

      <section className="dy-dock-sect dy-dock-grow">
        <div className="dy-dock-label">
          Prompts
          {history.length > 0 && (
            <button type="button" className="dy-dock-clear" onClick={() => clearHistory()}>
              Clear
            </button>
          )}
        </div>
        {history.length === 0 ? (
          <p className="dy-dock-empty">
            {gen === "available"
              ? "Ask for a workflow — the generator answers with a spec you can review and submit."
              : "Write what you want the agent to do. It is kept here, on this browser, and copied for the agent driving dagron over MCP."}
          </p>
        ) : (
          <ol className="dy-dock-hist">
            {history.map((h) => (
              <li key={h.id} className="dy-dock-turn">
                <div className="dy-dock-turn-head">
                  <span className="mono" style={{ fontSize: 10, color: "var(--dim)" }}>
                    {timeAgo(new Date(h.at).toISOString())}
                  </span>
                  <button type="button" className="dy-dock-copy" onClick={() => void copy(h.id, h.text)}>
                    {copied === h.id ? "Copied" : "Copy"}
                  </button>
                </div>
                <p className="dy-dock-turn-text">{h.text}</p>
                {h.reply && <pre className="dy-dock-reply">{h.reply}</pre>}
                {h.error && <p className="dy-dock-err">{h.error}</p>}
              </li>
            ))}
          </ol>
        )}
      </section>

      <footer className="dy-dock-compose">
        <textarea
          ref={taRef}
          className="dy-dock-input"
          rows={3}
          placeholder={
            gen === "available"
              ? "Describe the workflow you want…"
              : "Describe what the agent should do…"
          }
          value={draft}
          onChange={(e) => setDraft(e.target.value)}
          onKeyDown={(e) => {
            // Enter sends, Shift-Enter is a newline — the convention every
            // composer in this shape uses, and the one fingers already know.
            if (e.key === "Enter" && !e.shiftKey) {
              e.preventDefault();
              void send();
            }
          }}
        />
        <div className="dy-dock-actions">
          <span className="mono" style={{ fontSize: 10, color: "var(--dim)" }}>
            {gen === "available" ? "Enter to send" : "Enter to save"}
          </span>
          <button
            type="button"
            className="dy-btn dy-btn-primary dy-dock-send"
            onClick={() => void send()}
            disabled={!draft.trim() || busy}
          >
            {busy ? "Generating…" : gen === "available" ? "Send" : "Save & copy"}
          </button>
        </div>
      </footer>
    </aside>
  );
}

function AgentGlyph() {
  return (
    <svg
      viewBox="0 0 24 24"
      width="17"
      height="17"
      fill="none"
      stroke="currentColor"
      strokeWidth="1.8"
      strokeLinecap="round"
      strokeLinejoin="round"
      aria-hidden="true"
    >
      <rect x="4" y="8" width="16" height="12" rx="3" />
      <path d="M12 8V4" />
      <circle cx="12" cy="3" r="1.2" />
      <circle cx="9.5" cy="14" r="1.1" fill="currentColor" stroke="none" />
      <circle cx="14.5" cy="14" r="1.1" fill="currentColor" stroke="none" />
    </svg>
  );
}
