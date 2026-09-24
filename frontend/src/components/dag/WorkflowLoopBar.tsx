"use client";

import {
  clampPasses,
  describeWorkflowLoop,
  readWorkflowLoop,
  unwrapWorkflowLoop,
  wrapBlocker,
  wrapWorkflowLoop,
  type WorkflowLoopMode,
} from "@/lib/workflow-loop";
import { MAX_SEQUENTIAL_PASSES } from "@/lib/loop-model";
import type { WorkflowModel } from "@/lib/spec-model";

/// "Run this whole DAG N times" — the control above the canvas.
///
/// The spec has no run-level loop, so turning this on rewrites the workflow into
/// a sub-DAG plus a call that runs it N times (`workflow-loop.ts`). That rewrite
/// stays invisible: the canvas below keeps drawing the user's own graph, and
/// this bar is the only place the loop appears. Off, it is one unobtrusive
/// button — a workflow that doesn't loop shouldn't pay screen space for the
/// feature.
export default function WorkflowLoopBar({
  model,
  onChange,
}: {
  model: WorkflowModel;
  onChange: (m: WorkflowModel) => void;
}) {
  const found = readWorkflowLoop(model);
  // Wrapping an empty graph produces a loop around nothing, which the engine
  // rejects at submit ("template has no tasks") — so the control waits until
  // there is something to repeat.
  const bodyTasks = found ? found.body.tasks.length : model.tasks.length;
  const empty = bodyTasks === 0;
  // The wrapper needs the `loop-body` template name for itself; wrapping a
  // workflow that already has one would overwrite it. Refuse instead.
  const blocked = wrapBlocker(model);

  if (!found) {
    const why = empty ? "Add a task first — there is nothing to repeat yet." : blocked;
    return (
      <div style={barStyle}>
        <button
          className="dy-btn"
          disabled={empty || blocked != null}
          title={
            why ??
            "Run this whole graph more than once. Rewrites the workflow into a repeatable sub-DAG; the canvas keeps showing your steps."
          }
          onClick={() => onChange(wrapWorkflowLoop(model, { mode: "parallel", passes: 3 }))}
          style={{ cursor: why ? "not-allowed" : "pointer" }}
        >
          ⟳ Repeat whole workflow
        </button>
        <span style={{ color: blocked ? "var(--amber)" : "var(--dim)", fontSize: 11.5 }}>
          {blocked ?? "Runs once. Loop a single step from its panel instead."}
        </span>
      </div>
    );
  }

  const { loop } = found;
  const set = (p: Partial<typeof loop>) => onChange(wrapWorkflowLoop(model, { ...loop, ...p }));

  return (
    <div style={{ ...barStyle, border: "1px solid rgba(88,166,255,0.35)", background: "rgba(88,166,255,0.08)" }}>
      <span style={{ color: "var(--blue)", fontWeight: 600, fontSize: 12.5, whiteSpace: "nowrap" }}>
        ⟳ Whole workflow repeats
      </span>
      <input
        type="number"
        min={2}
        max={MAX_SEQUENTIAL_PASSES}
        value={loop.passes}
        onChange={(e) => set({ passes: clampPasses(Number(e.target.value)) })}
        title={`How many times the whole graph runs (2–${MAX_SEQUENTIAL_PASSES}).`}
        style={{
          width: 62,
          padding: "3px 6px",
          background: "var(--bg)",
          color: "var(--fg)",
          border: "1px solid var(--border)",
          borderRadius: 5,
          fontSize: 12.5,
        }}
      />
      <span style={{ color: "var(--muted)", fontSize: 12.5 }}>times</span>
      <select
        value={loop.mode}
        onChange={(e) => set({ mode: e.target.value as WorkflowLoopMode })}
        title="Parallel runs every pass at once; sequential starts each pass after the one before it finishes."
        style={{
          padding: "3px 6px",
          background: "var(--bg)",
          color: "var(--fg)",
          border: "1px solid var(--border)",
          borderRadius: 5,
          fontSize: 12.5,
        }}
      >
        <option value="parallel">all at once</option>
        <option value="sequential">one after another</option>
      </select>
      <span style={{ color: "var(--muted)", fontSize: 11.5, flex: 1, minWidth: 180 }}>
        {describeWorkflowLoop(loop, bodyTasks)}
      </span>
      <button
        className="dy-btn"
        onClick={() => onChange(unwrapWorkflowLoop(model, found))}
        title="Remove the loop and put the steps back at the top level."
        style={{ cursor: "pointer" }}
      >
        Remove loop
      </button>
    </div>
  );
}

const barStyle: React.CSSProperties = {
  display: "flex",
  alignItems: "center",
  gap: 8,
  flexWrap: "wrap",
  marginBottom: 8,
  padding: "7px 10px",
  borderRadius: 8,
  border: "1px solid var(--border)",
  background: "var(--card)",
};
