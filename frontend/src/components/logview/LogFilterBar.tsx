"use client";

import { useEffect, useRef, useState } from "react";
import {
  EMPTY_FILTER,
  LOG_LEVELS,
  isEmptyFilter,
  levelColor,
  toggleLevel,
  type LogFilterState,
} from "@/lib/log-filter";

export interface LogFilterBarProps {
  value: LogFilterState;
  onChange: (next: LogFilterState) => void;
  /// Compact layout for the narrow task drawer: the advanced row stays collapsed
  /// and the controls wrap tighter.
  compact?: boolean;
  /// `{matched} of {total}` summary rendered inline. Omitted while loading.
  summary?: { matched: number; total: number; truncated: boolean; limit: number } | null;
  /// Server-side rejection (a bad regex comes back as a 400 with the reason).
  error?: string | null;
  /// Extra controls rendered at the end of the top row, for whatever the host
  /// screen wants next to the filter (a saved-filter picker, an export button)
  /// without this component having to know about it.
  children?: React.ReactNode;
}

// The free-text fields commit on Enter or on leaving the box — never while
// typing. Any debounce is a guess at when someone has finished a word, and it
// is wrong often enough to be felt: each keystroke is a server round-trip over
// possibly-large output, and a pane that reshuffles under a half-typed term is
// worse than one that waits to be asked.
//
// The cost of an explicit commit is that nothing happens until you ask, which
// reads as broken unless the box says so. `dirty` below drives that: an edited
// box is outlined and labelled until it is applied.

/// Is this pattern still half-typed?
///
/// Used only to *hold back* a request, never to declare a pattern good: the
/// server's Rust `regex` is the authority and rejects things JS accepts (a
/// lookahead, say). But a pattern JS cannot parse either — an unclosed group or
/// class, which is what every regex looks like partway through being typed — is
/// certainly not worth a round trip that comes back 400 and flashes an error at
/// someone who is simply mid-word.
function isIncompleteRegex(pattern: string): boolean {
  if (!pattern) return false;
  try {
    new RegExp(pattern);
    return false;
  } catch {
    return true;
  }
}

/// The log filter controls: text include/exclude, regex, case, level toggles,
/// context, cap. Shared by the task drawer and the workflow log view so the two
/// can't drift into offering different filters over the same data.
export default function LogFilterBar({
  value,
  onChange,
  compact = false,
  summary,
  error,
  children,
}: LogFilterBarProps) {
  // Text inputs are held locally and pushed up on a debounce; `value` remains
  // the source of truth, so an external change (URL load, saved view, Clear)
  // still lands in the boxes.
  const [q, setQ] = useState(value.q);
  const [exclude, setExclude] = useState(value.exclude);
  const [regex, setRegex] = useState(value.regex);
  const [advanced, setAdvanced] = useState(!compact && hasAdvanced(value));
  // The debounced commit below must read the filter as it is *when the timer
  // fires*, not as it was when the timer was scheduled. Without this, clicking a
  // level pill mid-typing is silently reverted 300ms later by a timer still
  // holding the pre-click value — a real lost update, since typing then clicking
  // is the normal way to use a filter bar.
  const latest = useRef({ value, onChange });
  latest.current = { value, onChange };

  useEffect(() => {
    setQ(value.q);
    setExclude(value.exclude);
    setRegex(value.regex);
  }, [value.q, value.exclude, value.regex]);

  const incompleteRegex = isIncompleteRegex(regex);
  /// Text in a box that hasn't been applied yet. All three commit together, so
  /// this is one flag, not three.
  const dirty = q !== value.q || exclude !== value.exclude || regex !== value.regex;

  /// Apply what's in the boxes. Refuses a regex that doesn't parse — on Enter
  /// as much as on blur, because a request that can only 400 helps nobody.
  const commit = () => {
    if (!dirty || incompleteRegex) return;
    latest.current.onChange({ ...latest.current.value, q, exclude, regex });
  };

  const onFieldKey = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") {
      e.preventDefault();
      commit();
    } else if (e.key === "Escape") {
      // Abandon the edit rather than apply it — the counterpart to blur
      // committing, so leaving a box is never the only way out.
      e.preventDefault();
      setQ(value.q);
      setExclude(value.exclude);
      setRegex(value.regex);
    }
  };
  const active = !isEmptyFilter(value);
  const inputStyle: React.CSSProperties = {
    background: "var(--bg)",
    color: "var(--fg)",
    border: `1px solid ${dirty ? "var(--accent)" : "var(--border)"}`,
    borderRadius: 6,
    padding: "5px 8px",
    fontSize: 12,
    minWidth: 0,
  };

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 6 }}>
      <div style={{ display: "flex", gap: 6, alignItems: "center", flexWrap: "wrap" }}>
        <input
          value={q}
          onChange={(e) => setQ(e.target.value)}
          onKeyDown={onFieldKey}
          onBlur={commit}
          placeholder="Filter logs…"
          aria-label="Filter log lines containing"
          style={{ ...inputStyle, flex: compact ? "1 1 120px" : "1 1 220px" }}
        />
        {LOG_LEVELS.map((lv) => {
          const on = value.levels.includes(lv);
          return (
            <button
              key={lv}
              onClick={() => onChange(toggleLevel(value, lv))}
              className={`dy-pill ${on ? "dy-pill-active" : ""}`}
              aria-pressed={on}
              title={
                lv === "plain"
                  ? "Lines with no recognizable level — most ordinary output"
                  : `Show only ${lv} lines (level is inferred from the line's head)`
              }
              style={{
                cursor: "pointer",
                fontSize: 11,
                padding: "3px 8px",
                color: on ? levelColor(lv) : "var(--muted)",
              }}
            >
              {lv}
            </button>
          );
        })}
        <button
          onClick={() => setAdvanced((a) => !a)}
          className={`dy-pill ${advanced || hasAdvanced(value) ? "dy-pill-active" : ""}`}
          aria-expanded={advanced}
          title="Exclude terms, regex, case sensitivity, context lines, line cap"
          style={{ cursor: "pointer", fontSize: 11, padding: "3px 8px" }}
        >
          {advanced ? "▾" : "▸"} more
        </button>
        {active && (
          <button
            onClick={() => onChange(EMPTY_FILTER)}
            className="dy-pill"
            title="Clear the filter and show every line"
            style={{ cursor: "pointer", fontSize: 11, padding: "3px 8px" }}
          >
            ✕ clear
          </button>
        )}
        {children}
      </div>

      {advanced && (
        <div style={{ display: "flex", gap: 6, alignItems: "center", flexWrap: "wrap" }}>
          <input
            value={exclude}
            onChange={(e) => setExclude(e.target.value)}
            onKeyDown={onFieldKey}
            onBlur={commit}
            placeholder="Exclude…"
            aria-label="Exclude log lines containing"
            style={{ ...inputStyle, flex: "1 1 140px" }}
          />
          <input
            value={regex}
            onChange={(e) => setRegex(e.target.value)}
            onKeyDown={onFieldKey}
            onBlur={commit}
            placeholder="Regex…"
            aria-label="Filter log lines matching regular expression"
            aria-invalid={incompleteRegex || undefined}
            spellCheck={false}
            className="mono"
            style={{
              ...inputStyle,
              flex: "1 1 160px",
              // Says "still typing", not "wrong" — the pane deliberately hasn't
              // moved, and silence without a reason reads as a hang.
              borderColor: incompleteRegex ? "var(--amber)" : undefined,
            }}
            title={incompleteRegex ? "Incomplete pattern — not searched yet" : undefined}
          />
          <button
            onClick={() => onChange({ ...value, caseSensitive: !value.caseSensitive })}
            className={`dy-pill ${value.caseSensitive ? "dy-pill-active" : ""}`}
            aria-pressed={value.caseSensitive}
            title="Match case exactly"
            style={{ cursor: "pointer", fontSize: 11, padding: "3px 8px" }}
          >
            Aa
          </button>
          <label
            style={{ display: "inline-flex", alignItems: "center", gap: 4, fontSize: 11, color: "var(--muted)" }}
            title="Also keep this many unmatched lines either side of each match"
          >
            context
            <input
              type="number"
              min={0}
              max={20}
              value={value.context}
              onChange={(e) => onChange({ ...value, context: clamp(e.target.value, 0, 20) })}
              aria-label="Context lines around each match"
              style={{ ...inputStyle, width: 52 }}
            />
          </label>
          <label
            style={{ display: "inline-flex", alignItems: "center", gap: 4, fontSize: 11, color: "var(--muted)" }}
            title="Max lines returned (0 = server default)"
          >
            max
            <input
              type="number"
              min={0}
              max={50000}
              step={500}
              value={value.limit}
              onChange={(e) => onChange({ ...value, limit: clamp(e.target.value, 0, 50000) })}
              aria-label="Maximum lines returned"
              style={{ ...inputStyle, width: 72 }}
            />
          </label>
          <button
            onClick={() => onChange({ ...value, tail: !value.tail })}
            className={`dy-pill ${value.tail ? "dy-pill-active" : ""}`}
            aria-pressed={value.tail}
            title="When capped, keep the last lines instead of the first"
            style={{ cursor: "pointer", fontSize: 11, padding: "3px 8px" }}
          >
            tail
          </button>
        </div>
      )}

      {/* Nothing is queried while typing, so an edited box has to say so —
          otherwise the pane looking unchanged reads as a hang, which is the
          whole risk of committing explicitly. */}
      {dirty && (
        <p style={{ color: "var(--accent)", fontSize: 11, margin: 0 }} role="status">
          {incompleteRegex
            ? "Incomplete pattern — finish it, then press Enter"
            : "Press Enter to apply · Esc to discard"}
        </p>
      )}
      {error && (
        <p style={{ color: "var(--red)", fontSize: 11, margin: 0 }} role="alert">
          {error}
        </p>
      )}
      {summary && (
        <p style={{ color: "var(--dim)", fontSize: 11, margin: 0 }}>
          {/* Always say what was hidden. A filtered pane that silently drops
              lines reads as "that's all there was", which is the one thing a
              log view must never imply. */}
          {active
            ? `${fmt(summary.matched)} of ${fmt(summary.total)} lines match`
            : `${fmt(summary.total)} lines`}
          {summary.truncated && (
            <span style={{ color: "var(--amber)" }}>
              {" "}
              · showing {fmt(summary.limit)}, raise “max” to see more
            </span>
          )}
        </p>
      )}
    </div>
  );
}

function hasAdvanced(f: LogFilterState): boolean {
  return Boolean(f.exclude || f.regex || f.caseSensitive || f.context || f.limit || f.tail);
}

function clamp(raw: string, lo: number, hi: number): number {
  const n = Number(raw);
  if (!Number.isFinite(n)) return lo;
  return Math.min(hi, Math.max(lo, Math.floor(n)));
}

function fmt(n: number): string {
  return n.toLocaleString();
}
