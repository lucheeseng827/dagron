"use client";

// Global ⌘K command palette: one search box over workflows, runs, schedules
// (server-side, capped — see routes/search.rs) plus client-side page shortcuts.
// Opens via Ctrl/Cmd-K or the sidebar Search button (a `dagron:open-search`
// window event); arrows + Enter navigate, Esc closes.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { useRouter } from "next/navigation";
import { globalSearch } from "@/lib/dagron-api";
import { statusColor } from "@/lib/adapter";
import { timeAgo } from "@/lib/time";
import type { SearchResponse } from "@/types/dagron";

export const OPEN_SEARCH_EVENT = "dagron:open-search";

/// Static page shortcuts, filtered client-side by the same query.
const PAGES: { label: string; href: string; keywords: string }[] = [
  { label: "Overview", href: "/overview", keywords: "overview home dashboard" },
  { label: "Workflows", href: "/workflows", keywords: "workflows definitions dags" },
  { label: "Runs", href: "/runs", keywords: "runs executions history" },
  { label: "Approvals", href: "/approvals", keywords: "approvals gates pending human" },
  { label: "Submit", href: "/submit", keywords: "submit new run yaml" },
  { label: "GitOps", href: "/gitops", keywords: "gitops repos git sync" },
  { label: "Backfills", href: "/backfills", keywords: "backfills catchup range" },
  { label: "Dead letters", href: "/dead-letters", keywords: "dead letters dlq poison" },
  { label: "Metrics", href: "/metrics", keywords: "metrics charts stats observability" },
  { label: "Notifications", href: "/settings/notifications", keywords: "notifications slack webhook alerts settings" },
  { label: "Users", href: "/settings/users", keywords: "users accounts admin settings" },
  { label: "Audit log", href: "/settings/audit", keywords: "audit log history admin" },
];

interface Item {
  key: string;
  group: string;
  title: string;
  sub?: string;
  dot?: string;
  href: string;
}

const EMPTY_RESULTS: SearchResponse = { query: "", workflows: [], runs: [], schedules: [] };

export default function CommandPalette() {
  const router = useRouter();
  const [open, setOpen] = useState(false);
  const [q, setQ] = useState("");
  const [results, setResults] = useState<SearchResponse>(EMPTY_RESULTS);
  const [sel, setSel] = useState(0);
  const inputRef = useRef<HTMLInputElement | null>(null);
  // Serial guard: a slow response for an old query never overwrites a newer one.
  const seq = useRef(0);

  // Open on Ctrl/Cmd-K or the sidebar button's window event.
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => {
      if ((e.metaKey || e.ctrlKey) && e.key.toLowerCase() === "k") {
        e.preventDefault();
        setOpen((o) => !o);
      }
    };
    const onOpen = () => setOpen(true);
    window.addEventListener("keydown", onKey);
    window.addEventListener(OPEN_SEARCH_EVENT, onOpen);
    return () => {
      window.removeEventListener("keydown", onKey);
      window.removeEventListener(OPEN_SEARCH_EVENT, onOpen);
    };
  }, []);

  // Reset + focus on open; restore focus to the opener element on close.
  const prevFocus = useRef<HTMLElement | null>(null);
  useEffect(() => {
    if (open) {
      prevFocus.current = document.activeElement as HTMLElement | null;
      setQ("");
      setResults(EMPTY_RESULTS);
      setSel(0);
      // Focus after the modal paints.
      setTimeout(() => inputRef.current?.focus(), 0);
    } else {
      prevFocus.current?.focus?.();
      prevFocus.current = null;
    }
  }, [open]);

  // Debounced server search — 250ms after the last keystroke. The sequence
  // bumps on EVERY transition (close/clear included) so a slow response for an
  // old query can never repopulate an emptied palette.
  useEffect(() => {
    const mySeq = ++seq.current;
    if (!open) return;
    const needle = q.trim();
    if (!needle) {
      setResults(EMPTY_RESULTS);
      setSel(0);
      return;
    }
    const t = setTimeout(() => {
      globalSearch(needle)
        .then((r) => {
          if (seq.current === mySeq) {
            setResults(r);
            setSel(0);
          }
        })
        .catch(() => {});
    }, 250);
    return () => clearTimeout(t);
  }, [q, open]);

  // Flattened, grouped item list (pages filtered client-side).
  const items = useMemo<Item[]>(() => {
    const needle = q.trim().toLowerCase();
    const out: Item[] = [];
    if (needle) {
      for (const p of PAGES) {
        if (p.label.toLowerCase().includes(needle) || p.keywords.includes(needle)) {
          out.push({ key: `page:${p.href}`, group: "Pages", title: p.label, href: p.href });
        }
      }
    }
    for (const w of results.workflows) {
      out.push({
        key: `wf:${w.id}`,
        group: "Workflows",
        title: w.name,
        sub: w.description ?? undefined,
        href: `/workflows/${w.id}/history`,
      });
    }
    for (const r of results.runs) {
      out.push({
        key: `run:${r.id}`,
        group: "Runs",
        title: `${r.name ?? "—"} · ${r.id.slice(0, 8)}`,
        sub: `${r.status} · ${timeAgo(r.created_at)}`,
        dot: statusColor(r.status),
        href: `/runs/${r.id}`,
      });
    }
    for (const s of results.schedules) {
      out.push({
        key: `sch:${s.id}`,
        group: "Schedules",
        title: `${s.workflow_name} · ${s.cron_expr}`,
        sub: s.enabled ? "enabled" : "paused",
        href: `/workflows/${s.workflow_id}`,
      });
    }
    return out;
  }, [q, results]);

  const go = useCallback(
    (item: Item) => {
      setOpen(false);
      router.push(item.href);
    },
    [router],
  );

  const onInputKey = (e: React.KeyboardEvent) => {
    if (e.key === "Escape") {
      setOpen(false);
    } else if (e.key === "Tab") {
      // Contain focus: the palette is keyboard-driven via arrows, so Tab
      // must not escape into the page behind the overlay.
      e.preventDefault();
    } else if (e.key === "ArrowDown") {
      e.preventDefault();
      setSel((s) => Math.min(s + 1, items.length - 1));
    } else if (e.key === "ArrowUp") {
      e.preventDefault();
      setSel((s) => Math.max(s - 1, 0));
    } else if (e.key === "Enter" && items[sel]) {
      go(items[sel]);
    }
  };

  if (!open) return null;

  let lastGroup = "";

  return (
    <div className="dy-cmdk-overlay" onMouseDown={() => setOpen(false)}>
      <div
        className="dy-cmdk"
        role="dialog"
        aria-modal="true"
        aria-label="Global search"
        onMouseDown={(e) => e.stopPropagation()}
      >
        <input
          ref={inputRef}
          value={q}
          onChange={(e) => setQ(e.target.value)}
          onKeyDown={onInputKey}
          placeholder="Search workflows, runs, schedules, pages…"
          aria-label="Global search"
          className="dy-cmdk-input"
        />
        <div className="dy-cmdk-results">
          {items.map((item, i) => {
            const header = item.group !== lastGroup ? item.group : null;
            lastGroup = item.group;
            return (
              <div key={item.key}>
                {header && <div className="dy-cmdk-group">{header}</div>}
                <button
                  className={`dy-cmdk-item ${i === sel ? "dy-cmdk-item-active" : ""}`}
                  onMouseEnter={() => setSel(i)}
                  onClick={() => go(item)}
                >
                  {item.dot && <span className="dy-dot dy-dot-sm" style={{ background: item.dot }} />}
                  <span style={{ fontWeight: 600, whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}>
                    {item.title}
                  </span>
                  {item.sub && (
                    <span style={{ marginLeft: "auto", fontSize: 12, color: "var(--dim)", whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis", maxWidth: "45%" }}>
                      {item.sub}
                    </span>
                  )}
                </button>
              </div>
            );
          })}
          {q.trim() && items.length === 0 && <div className="dy-cmdk-empty">No matches for “{q.trim()}”.</div>}
          {!q.trim() && (
            <div className="dy-cmdk-empty">
              Type to search — workflow names, run ids, schedules, or a page. <kbd>↑↓</kbd> to move,{" "}
              <kbd>Enter</kbd> to open, <kbd>Esc</kbd> to close.
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
