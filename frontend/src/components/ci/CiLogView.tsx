"use client";

import { useMemo, useState } from "react";
import {
  countsLine,
  hasCiReport,
  parseCiLog,
  stepReads,
  type CiAnnotation,
  type CiLog,
  type CiStep,
} from "@/lib/ci-log";

const PRE: React.CSSProperties = {
  margin: 0,
  background: "var(--bg)",
  padding: "0.6rem 0.75rem",
  fontSize: 12,
  whiteSpace: "pre-wrap",
  wordBreak: "break-word",
};

const TONE: Record<string, string> = {
  ok: "var(--green)",
  failed: "var(--red)",
  running: "var(--blue)",
  unknown: "var(--dim)",
};

const LEVEL: Record<CiAnnotation["level"], string> = {
  failure: "var(--red)",
  warning: "var(--amber)",
  notice: "var(--muted)",
};

/// A CI job's log by step, with what the job said of its tests above it.
///
/// A step that succeeded is closed, since what a reader came for is the one
/// that did not: a failed step, a step still running, and one whose end is
/// not in the log are open. Opening or closing one by hand is kept.
export default function CiLogView({
  output,
  running,
  style,
}: {
  output: string;
  /// Whether the job's task is still running: a step with no end is then
  /// running, and otherwise did not end.
  running: boolean;
  style?: React.CSSProperties;
}) {
  const log = useMemo(() => parseCiLog(output), [output]);
  // What the reader opened or closed, by the step's place in the log.
  const [chosen, setChosen] = useState<Record<number, boolean>>({});

  return (
    <div style={{ display: "flex", flexDirection: "column", gap: 8, minHeight: 80, ...style }}>
      {hasCiReport(log) && <CiReport log={log} />}
      <div style={{ border: "1px solid var(--border)", borderRadius: 6, overflow: "hidden" }}>
        {log.segments.length === 0 && <pre style={PRE}>(no output)</pre>}
        {log.segments.map((segment, i) => {
          if (segment.kind === "lines") {
            return (
              <pre key={i} style={{ ...PRE, color: "var(--muted)" }}>
                {segment.lines.join("\n")}
              </pre>
            );
          }
          const reads = stepReads(segment.step, running);
          const open = chosen[i] ?? reads.tone !== "ok";
          return (
            <StepRows
              key={i}
              step={segment.step}
              label={reads.label}
              color={TONE[reads.tone]}
              open={open}
              onToggle={() => setChosen((c) => ({ ...c, [i]: !open }))}
            />
          );
        })}
      </div>
    </div>
  );
}

function StepRows({
  step,
  label,
  color,
  open,
  onToggle,
}: {
  step: CiStep;
  label: string;
  color: string;
  open: boolean;
  onToggle: () => void;
}) {
  return (
    <div style={{ borderTop: "1px solid var(--border)" }}>
      <button
        onClick={onToggle}
        aria-expanded={open}
        style={{
          display: "flex",
          alignItems: "center",
          gap: 8,
          width: "100%",
          padding: "6px 10px",
          background: "var(--panel)",
          border: "none",
          color: "var(--fg)",
          font: "inherit",
          fontSize: 12.5,
          textAlign: "left",
          cursor: "pointer",
        }}
      >
        <span aria-hidden style={{ color: "var(--dim)", width: 10 }}>
          {open ? "▾" : "▸"}
        </span>
        <span className="dy-dot dy-dot-sm" style={{ background: color, flex: "none" }} />
        <span style={{ fontWeight: 600, overflow: "hidden", textOverflow: "ellipsis", whiteSpace: "nowrap" }}>
          {step.n}. {step.name}
        </span>
        <span style={{ marginLeft: "auto", color, whiteSpace: "nowrap", fontSize: 11.5 }}>{label}</span>
      </button>
      {open && <pre style={PRE}>{step.lines.length ? step.lines.join("\n") : "(no output)"}</pre>}
    </div>
  );
}

/// What a job said of its tests, and what its steps wrote to their
/// summaries. It is the job's own word, and is labelled so: how the job
/// ended is its status, above.
export function CiReport({ log }: { log: CiLog }) {
  const counts = log.counts;
  const line = counts ? countsLine(counts) : "";
  // A failure that names a place is shown at its place, above: the list by
  // name is of the rest.
  const placed = new Set(log.annotations.map((a) => a.title));
  const unplaced = log.failed.filter((f) => !placed.has(f.name));
  return (
    <div
      style={{
        display: "flex",
        flexDirection: "column",
        gap: 6,
        padding: "0.6rem 0.7rem",
        borderRadius: 6,
        border: "1px solid var(--border-strong)",
        background: "var(--panel)",
        fontSize: 12.5,
      }}
    >
      <div style={{ display: "flex", gap: 8, alignItems: "baseline" }}>
        <strong>Report</strong>
        <span style={{ color: "var(--dim)", fontSize: 11 }}>what the job said of itself</span>
      </div>
      {line && <div>{line}</div>}
      {log.annotations.map((a, i) => (
        <div key={`a${i}`} style={{ display: "flex", flexDirection: "column", gap: 1 }}>
          <span className="mono" style={{ color: LEVEL[a.level], wordBreak: "break-all" }}>
            {a.path}:{a.start_line}
            {a.end_line > a.start_line ? `-${a.end_line}` : ""}
          </span>
          <span>
            {a.title && <strong>{a.title}: </strong>}
            <span style={{ whiteSpace: "pre-wrap" }}>{a.message}</span>
          </span>
        </div>
      ))}
      {unplaced.length > 0 && (
        <ul style={{ margin: 0, paddingLeft: 18 }}>
          {unplaced.map((f, i) => (
            <li key={`f${i}`}>
              <span className="mono">{f.name}</span>
              {f.message && <span style={{ color: "var(--muted)" }}>: {f.message.split("\n")[0]}</span>}
            </li>
          ))}
        </ul>
      )}
      {counts && counts.more_failed > 0 && (
        <span style={{ color: "var(--dim)" }}>… and {counts.more_failed} more failed.</span>
      )}
      {counts && counts.more_annotations > 0 && (
        <span style={{ color: "var(--dim)" }}>
          {counts.more_annotations} more named a place and {counts.more_annotations === 1 ? "is" : "are"} not shown.
        </span>
      )}
      {counts?.problems.map((p, i) => (
        <span key={`p${i}`} style={{ color: "var(--amber)" }}>
          {p}
        </span>
      ))}
      {log.summary.trim() !== "" && (
        <>
          <span style={{ color: "var(--dim)", fontSize: 11, marginTop: 2 }}>What its steps wrote to their summaries</span>
          <pre style={{ ...PRE, borderRadius: 4 }}>{log.summary.trim()}</pre>
        </>
      )}
    </div>
  );
}
