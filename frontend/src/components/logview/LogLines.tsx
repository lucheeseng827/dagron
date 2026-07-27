"use client";

import { forwardRef } from "react";
import { levelColor } from "@/lib/log-filter";
import type { LogLevel } from "@/types/dagron";

/// One rendered line. `task` is set only in the merged workflow view — in a
/// single task's drawer the attribution column would be the same string on
/// every row.
export interface RenderedLine {
  key: string;
  n: number;
  level: LogLevel;
  text: string;
  /// False for a line kept only as `context=N` padding around a match.
  matched: boolean;
  /// Wall-clock for this line, if one can be stated. `exact` when the line
  /// carried its own RFC3339 stamp; otherwise the owning task's start, which
  /// only bounds the line — `within` says how wide that bound is so the reader
  /// can judge it instead of trusting a number that looks precise.
  time?: { iso: string; exact: boolean; within?: string };
  task?: string;
  /// Click target for the task label (opens that task's drawer).
  onTaskClick?: () => void;
}

export interface LogLinesProps {
  lines: RenderedLine[];
  /// Rendered when `lines` is empty — "no output yet" and "nothing matched your
  /// filter" are different situations and must not share a message.
  empty: React.ReactNode;
  /// Show the source line number gutter.
  showLineNumbers?: boolean;
  /// Show the time gutter. Off in panes too narrow to carry it.
  showTimes?: boolean;
  style?: React.CSSProperties;
}

/// A filtered log pane: monospace lines, level-coloured, with the source line
/// number in a gutter so a filtered view can still be located in the raw log.
///
/// Context lines (`matched: false`) are dimmed rather than hidden — context you
/// can't tell apart from a hit makes the filter look like it matched more than
/// it did.
/// Clock time only — the date is the run's, and repeating it on every line
/// costs the width the message needs.
function hhmmss(iso: string): string {
  const d = new Date(iso);
  if (Number.isNaN(d.getTime())) return "--:--:--";
  const p = (n: number, w = 2) => String(n).padStart(w, "0");
  return `${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}.${p(d.getMilliseconds(), 3)}`;
}

const LogLines = forwardRef<HTMLDivElement, LogLinesProps>(function LogLines(
  { lines, empty, showLineNumbers = true, showTimes = true, style },
  ref,
) {
  return (
    <div
      ref={ref}
      className="mono"
      style={{
        background: "var(--bg)",
        borderRadius: 6,
        border: "1px solid var(--border)",
        fontSize: 12,
        overflow: "auto",
        padding: "6px 0",
        ...style,
      }}
    >
      {lines.length === 0 ? (
        <div style={{ color: "var(--dim)", padding: "8px 12px", fontSize: 12 }}>{empty}</div>
      ) : (
        lines.map((l) => (
          <div
            key={l.key}
            style={{
              display: "flex",
              gap: 8,
              padding: "0 12px",
              opacity: l.matched ? 1 : 0.55,
              borderLeft: `2px solid ${l.matched ? levelColor(l.level) : "transparent"}`,
            }}
          >
            {showLineNumbers && (
              <span
                style={{
                  color: "var(--dim)",
                  minWidth: 44,
                  textAlign: "right",
                  userSelect: "none",
                  flexShrink: 0,
                }}
                // The number is a position in the *raw* output, not in this
                // filtered pane — say so, or it reads as a render bug.
                title={`line ${l.n} of the unfiltered output`}
              >
                {l.n}
              </span>
            )}
            {showTimes && l.time != null && (
              <span
                style={{
                  color: "var(--dim)",
                  minWidth: 92,
                  textAlign: "right",
                  userSelect: "none",
                  flexShrink: 0,
                  // An estimate is drawn as an estimate. A tilde is cheaper to
                  // scan than a tooltip nobody opens, and a log view that shows
                  // a derived time as if it were recorded is worse than one
                  // that shows no time at all.
                  fontStyle: l.time.exact ? "normal" : "italic",
                }}
                title={
                  l.time.exact
                    ? `${l.time.iso} — printed by the task`
                    : `Estimated: this line has no timestamp of its own, so this is when its task started` +
                      (l.time.within ? ` (task ran ${l.time.within})` : "")
                }
              >
                {l.time.exact ? "" : "~"}
                {hhmmss(l.time.iso)}
              </span>
            )}
            {l.task != null && (
              <span
                onClick={l.onTaskClick}
                role={l.onTaskClick ? "button" : undefined}
                tabIndex={l.onTaskClick ? 0 : undefined}
                onKeyDown={(e) => {
                  if (l.onTaskClick && (e.key === "Enter" || e.key === " ")) {
                    e.preventDefault();
                    l.onTaskClick();
                  }
                }}
                title={l.onTaskClick ? `Open ${l.task}` : l.task}
                style={{
                  color: "var(--muted)",
                  minWidth: 120,
                  maxWidth: 120,
                  overflow: "hidden",
                  textOverflow: "ellipsis",
                  whiteSpace: "nowrap",
                  flexShrink: 0,
                  cursor: l.onTaskClick ? "pointer" : "default",
                }}
              >
                {l.task}
              </span>
            )}
            <span
              style={{
                color: l.level === "plain" ? "var(--fg)" : levelColor(l.level),
                whiteSpace: "pre-wrap",
                wordBreak: "break-word",
                flex: 1,
              }}
            >
              {l.text || " "}
            </span>
          </div>
        ))
      )}
    </div>
  );
});

export default LogLines;
