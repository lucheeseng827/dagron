"use client";

import { useMemo, useState } from "react";
import { diffLines, diffStat, toHunks, type DiffLine } from "@/lib/diff";

/// A unified diff of two workflow specs.
///
/// History used to be shown one whole YAML document per entry, which answers
/// "what did v3 say" and not "what changed" — and the second is the question
/// people actually open history with. So the diff is the default here and the
/// full text is a toggle, not the other way round. The toggle stays because
/// copying a version out is how you restore one today.
///
/// Unchanged runs collapse to a `⋯ N unchanged lines` rule: a save usually
/// touches three lines of three hundred, and rendering the other 297 buries
/// them.
export default function SpecDiff({
  base,
  head,
  baseLabel,
  headLabel,
  /// Wording for the no-change case — a version table and a run list mean
  /// different things by "identical".
  identical = "No change.",
  maxHeight = 420,
}: {
  base: string;
  head: string;
  baseLabel: string;
  headLabel: string;
  identical?: string;
  maxHeight?: number;
}) {
  const [full, setFull] = useState(false);
  const d = useMemo(() => diffLines(base, head), [base, head]);
  const hunks = useMemo(() => toHunks(d.lines, 3), [d.lines]);
  const stat = diffStat(d);

  return (
    <div style={{ border: "1px solid var(--border)", borderRadius: 6, overflow: "hidden" }}>
      <div
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          flexWrap: "wrap",
          padding: "6px 10px",
          borderBottom: "1px solid var(--border)",
          background: "var(--panel-2)",
          fontSize: 12,
        }}
      >
        <span className="mono" style={{ color: "var(--muted)" }}>
          {baseLabel} <span style={{ color: "var(--dim)" }}>→</span> {headLabel}
        </span>
        {stat ? (
          <span className="mono" style={{ fontSize: 11.5 }}>
            {d.added > 0 && <span style={{ color: "var(--green)" }}>+{d.added}</span>}
            {d.added > 0 && d.removed > 0 && " "}
            {d.removed > 0 && <span style={{ color: "var(--red)" }}>−{d.removed}</span>}
          </span>
        ) : (
          <span style={{ color: "var(--dim)", fontSize: 11.5 }}>{identical}</span>
        )}
        {d.truncated && (
          <span
            style={{ color: "var(--amber)", fontSize: 11 }}
            title="These specs are too large to diff line by line, so the changed region is reported as one replacement."
          >
            ⚠ shown as a whole-block replacement
          </span>
        )}
        <span style={{ flex: 1 }} />
        <button
          type="button"
          className="dy-btn"
          style={{ fontSize: 11.5, padding: "2px 8px" }}
          onClick={() => setFull((v) => !v)}
          title={full ? "Show only what changed" : "Show the whole spec for this entry"}
        >
          {full ? "Diff" : "Full YAML"}
        </button>
      </div>

      {full ? (
        <pre className="mono" style={{ ...preStyle, maxHeight }}>
          {head || "(empty)"}
        </pre>
      ) : hunks.length === 0 ? (
        <p style={{ margin: 0, padding: "14px 12px", color: "var(--dim)", fontSize: 12.5 }}>
          {identical}
        </p>
      ) : (
        <div className="mono" style={{ maxHeight, overflow: "auto", fontSize: 12, lineHeight: "18px" }}>
          {hunks.map((h, i) => (
            <div key={`${h.a}:${h.b}:${i}`}>
              {h.skipped > 0 && (
                <div
                  style={{
                    padding: "2px 10px",
                    background: "rgba(255,255,255,0.025)",
                    borderTop: i === 0 ? "none" : "1px solid var(--border)",
                    borderBottom: "1px solid var(--border)",
                    color: "var(--dim)",
                    fontSize: 11,
                  }}
                >
                  ⋯ {h.skipped} unchanged {h.skipped === 1 ? "line" : "lines"}
                </div>
              )}
              {h.lines.map((l, k) => (
                <Row key={k} line={l} />
              ))}
            </div>
          ))}
        </div>
      )}
    </div>
  );
}

/// One diff row: two line-number gutters, the marker, the text.
///
/// Both numbers are shown rather than one, because the question a reader brings
/// to a spec diff is "which line is this now" as often as "which line was it".
function Row({ line }: { line: DiffLine }) {
  const tone =
    line.kind === "add"
      ? { bg: "rgba(63,185,80,0.13)", fg: "var(--green)", mark: "+" }
      : line.kind === "del"
        ? { bg: "rgba(240,100,90,0.13)", fg: "var(--red)", mark: "−" }
        : { bg: "transparent", fg: "var(--muted)", mark: " " };
  return (
    <div style={{ display: "flex", background: tone.bg, whiteSpace: "pre" }}>
      <span style={gutter} aria-hidden>
        {line.a ?? ""}
      </span>
      <span style={gutter} aria-hidden>
        {line.b ?? ""}
      </span>
      <span style={{ width: 14, flexShrink: 0, textAlign: "center", color: tone.fg }}>
        {tone.mark}
      </span>
      {/* The text wraps rather than scrolling sideways: a long `command:` line
          is the thing you are trying to read, and a horizontal scrollbar per
          row would hide exactly the end of it that changed. */}
      <span style={{ flex: 1, minWidth: 0, color: line.kind === "same" ? "var(--muted)" : "var(--fg)", whiteSpace: "pre-wrap", wordBreak: "break-word", paddingRight: 8 }}>
        {line.text || " "}
      </span>
    </div>
  );
}

const gutter: React.CSSProperties = {
  width: 40,
  flexShrink: 0,
  textAlign: "right",
  paddingRight: 8,
  color: "var(--dim)",
  fontSize: 11,
  userSelect: "none",
};

const preStyle: React.CSSProperties = {
  margin: 0,
  padding: 12,
  background: "var(--bg)",
  fontSize: 12.5,
  overflow: "auto",
  whiteSpace: "pre-wrap",
};
