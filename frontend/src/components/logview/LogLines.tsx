"use client";

import { forwardRef, useMemo, useState } from "react";
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
  /// Fold a GitHub workflow's `::group::` lines. Off while a filter is
  /// set: a group whose end the filter left out would fold every match
  /// after it.
  fold?: boolean;
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

const GROUP = "::group::";
const ENDGROUP = "::endgroup::";

/// One line as it is drawn: a GitHub workflow's `::group::` line is the
/// header of the lines up to its `::endgroup::`, which fold under it.
interface Row {
  line: RenderedLine;
  /// Set on a group's header: its title and how many lines it holds.
  header?: { title: string; size: number };
  /// The key of the group the line is in, where it is in one.
  within?: string;
  /// An `::endgroup::` line, which is not drawn.
  end?: boolean;
}

/// A group's key: its task's and its line's number in that task's output,
/// which stay its own as the pane's window moves.
function groupKey(line: RenderedLine): string {
  return `${line.task ?? ""}:${line.n}`;
}

/// The groups of `lines`, each task's its own (tasks' lines may come in
/// turn). A group ends at its `::endgroup::`, or at its task's next
/// `::group::`: GitHub's do not nest.
function grouped(lines: RenderedLine[]): Row[] {
  const rows: Row[] = [];
  const open = new Map<string, { key: string; header: Row }>();
  for (const line of lines) {
    const text = line.text.trimStart();
    const task = line.task ?? "";
    const group = open.get(task);
    if (text.startsWith(GROUP)) {
      const header: Row = { line, header: { title: text.slice(GROUP.length), size: 0 } };
      rows.push(header);
      open.set(task, { key: groupKey(line), header });
    } else if (text.startsWith(ENDGROUP)) {
      rows.push({ line, end: true });
      open.delete(task);
    } else if (group) {
      group.header.header!.size += 1;
      rows.push({ line, within: group.key });
    } else {
      rows.push({ line });
    }
  }
  return rows;
}

const LogLines = forwardRef<HTMLDivElement, LogLinesProps>(function LogLines(
  { lines, empty, showLineNumbers = true, showTimes = true, fold = true, style },
  ref,
) {
  const rows = useMemo(
    () => (fold ? grouped(lines) : lines.map((line) => ({ line }) as Row)),
    [lines, fold],
  );
  // The groups opened: each is folded until it is, as GitHub folds them.
  const [opened, setOpened] = useState<Set<string>>(() => new Set());
  const toggle = (key: string) =>
    setOpened((was) => {
      const next = new Set(was);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
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
        rows
          .filter((r) => !r.end && (r.within == null || opened.has(r.within)))
          .map(({ line: l, header }) => (
          <div
            key={l.key}
            onClick={header ? () => toggle(groupKey(l)) : undefined}
            role={header ? "button" : undefined}
            tabIndex={header ? 0 : undefined}
            aria-expanded={header ? opened.has(groupKey(l)) : undefined}
            onKeyDown={
              header
                ? (e) => {
                    if (e.key === "Enter" || e.key === " ") {
                      e.preventDefault();
                      toggle(groupKey(l));
                    }
                  }
                : undefined
            }
            title={
              header ? (opened.has(groupKey(l)) ? "Fold this group" : "Unfold this group") : undefined
            }
            style={{
              display: "flex",
              gap: 8,
              padding: "0 12px",
              opacity: l.matched ? 1 : 0.55,
              borderLeft: `2px solid ${l.matched ? levelColor(l.level) : "transparent"}`,
              cursor: header ? "pointer" : undefined,
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
              {header ? (
                <>
                  <span style={{ userSelect: "none" }}>{opened.has(groupKey(l)) ? "▾ " : "▸ "}</span>
                  <strong>{header.title || "group"}</strong>
                  <span style={{ color: "var(--dim)" }}>
                    {" "}
                    ({header.size} {header.size === 1 ? "line" : "lines"})
                  </span>
                </>
              ) : (
                l.text || " "
              )}
            </span>
          </div>
        ))
      )}
    </div>
  );
});

export default LogLines;
