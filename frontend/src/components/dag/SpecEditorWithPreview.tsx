"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import Editor from "@monaco-editor/react";
import "@/lib/monaco"; // self-host the Monaco runtime (air-gap; no CDN)
import DagGraph from "./DagGraph";
import SnippetPalette from "./SnippetPalette";
import { useDagDirection } from "./direction";
import { specToGraph } from "@/lib/spec-graph";
import { applySnippet, type Snippet } from "@/lib/palette";
import { parseModel } from "@/lib/spec-model";

export interface SpecEditorWithPreviewProps {
  value: string;
  onChange: (v: string) => void;
}

// The editor's share of the editor+preview pane. Clamped so neither side can
// be collapsed into uselessness; double-clicking the divider resets to even.
const SPLIT_KEY = "dagron.editor-split";
const SPLIT_DEFAULT = 0.5;
const SPLIT_MIN = 0.2;
const SPLIT_MAX = 0.8;

/// A YAML DAG editor with a live DAG preview beside it. The preview re-renders as
/// you type (debounced) so you can see the graph shape — Start/End markers, tasks,
/// and dependency edges — take form. The divider between code and graph drags to
/// resize either side. Parsing is client-side and best-effort; the server remains
/// the authoritative validator on submit.
export default function SpecEditorWithPreview({ value, onChange }: SpecEditorWithPreviewProps) {
  // Debounce the spec fed to the preview so the DAG only re-lays-out when typing
  // pauses, not on every keystroke.
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const t = setTimeout(() => setDebounced(value), 250);
    return () => clearTimeout(t);
  }, [value]);

  const parsed = useMemo(() => specToGraph(debounced), [debounced]);
  const graph = parsed.graph && parsed.graph.nodes.length > 0 ? parsed.graph : null;

  // Held here (not in DagGraph) so the choice survives the topology-keyed
  // remounts below without a one-frame flash of the default layout.
  const [dir, setDir] = useDagDirection();

  // Draggable editor/preview split, restored from localStorage after mount
  // (SSR paints the default). Committed back on drag end, not every move.
  const paneRef = useRef<HTMLDivElement>(null);
  const splitRef = useRef(SPLIT_DEFAULT);
  const [split, setSplitState] = useState(SPLIT_DEFAULT);
  const [dragging, setDragging] = useState(false);
  const setSplit = (v: number) => {
    splitRef.current = v;
    setSplitState(v);
  };
  useEffect(() => {
    try {
      const stored = Number(window.localStorage.getItem(SPLIT_KEY));
      if (Number.isFinite(stored) && stored >= SPLIT_MIN && stored <= SPLIT_MAX) setSplit(stored);
    } catch {
      // Storage unavailable — keep the default.
    }
  }, []);
  const persistSplit = (v: number) => {
    try {
      window.localStorage.setItem(SPLIT_KEY, String(v));
    } catch {
      // Persistence is best-effort.
    }
  };

  // The palette inserts via the model round-trip, so it's unusable while the
  // spec doesn't parse (a blank editor is fine — insertion scaffolds a fresh
  // workflow). Derived from the debounced text; the 250ms lag is imperceptible.
  const paletteDisabled = useMemo(
    () => debounced.trim() !== "" && !parseModel(debounced).model,
    [debounced],
  );
  // Insert failures are user-visible ("result_from needs a task", or the tiny
  // broken-parse race) — shown transiently in the palette rail, then cleared.
  const [insertError, setInsertError] = useState<string | null>(null);
  useEffect(() => {
    if (!insertError) return;
    const t = setTimeout(() => setInsertError(null), 4000);
    return () => clearTimeout(t);
  }, [insertError]);
  const onInsert = (s: Snippet) => {
    // Insert against the live value (not the debounced copy) so the user's last
    // keystrokes are never dropped.
    const r = applySnippet(value, s);
    if (r.spec != null) {
      setInsertError(null);
      onChange(r.spec);
    } else {
      setInsertError(r.error ?? "couldn't insert block");
    }
  };
  // Remount the graph (refit the view) only when the topology changes, so adding
  // a task or edge re-centers the preview without flicker on every keystroke.
  const topoKey = graph
    ? graph.nodes.map((n) => n.id).join(",") + "|" + graph.edges.map((e) => `${e.source}>${e.target}`).join(",")
    : "";

  return (
    <div style={{ display: "flex", height: "100%", width: "100%", minHeight: 0 }}>
      <SnippetPalette
        onInsert={onInsert}
        disabled={paletteDisabled}
        disabledReason="Fix the YAML to insert blocks."
        error={insertError}
      />
      <div
        ref={paneRef}
        style={{
          flex: 1,
          minWidth: 0,
          display: "flex",
          // Dragging the divider must not start selecting editor text.
          userSelect: dragging ? "none" : undefined,
        }}
      >
        <div style={{ width: `${split * 100}%`, minWidth: 0, flexShrink: 0 }}>
          <Editor
            height="100%"
            defaultLanguage="yaml"
            theme="vs-dark"
            value={value}
            onChange={(v) => onChange(v ?? "")}
            options={{
              minimap: { enabled: false },
              fontSize: 13,
              tabSize: 2,
              scrollBeyondLastLine: false,
              // The pane resizes as the divider drags; Monaco must follow.
              automaticLayout: true,
            }}
          />
        </div>
        <div
          role="separator"
          aria-orientation="vertical"
          title="Drag to resize — double-click to reset"
          onPointerDown={(e) => {
            e.currentTarget.setPointerCapture(e.pointerId);
            setDragging(true);
          }}
          onPointerMove={(e) => {
            if (!dragging || !paneRef.current) return;
            const r = paneRef.current.getBoundingClientRect();
            setSplit(Math.min(SPLIT_MAX, Math.max(SPLIT_MIN, (e.clientX - r.left) / r.width)));
          }}
          onPointerUp={() => {
            setDragging(false);
            persistSplit(splitRef.current);
          }}
          onDoubleClick={() => {
            setSplit(SPLIT_DEFAULT);
            persistSplit(SPLIT_DEFAULT);
          }}
          style={{
            width: 6,
            flexShrink: 0,
            cursor: "col-resize",
            background: dragging ? "var(--accent)" : "var(--border)",
            transition: dragging ? "none" : "background 0.12s",
            touchAction: "none",
          }}
        />
        <div
          style={{
            flex: 1,
            minWidth: 0,
            position: "relative",
            background: "var(--bg)",
          }}
        >
          <span
            style={{
              position: "absolute",
              top: 8,
              right: 12,
              zIndex: 5,
              fontSize: 11,
              letterSpacing: "0.06em",
              textTransform: "uppercase",
              color: "var(--muted)",
              pointerEvents: "none",
            }}
          >
            Live preview
          </span>
          {graph ? (
            <DagGraph key={topoKey} graph={graph} direction={dir} onDirectionChange={setDir} />
          ) : (
            <div
              style={{
                height: "100%",
                display: "flex",
                alignItems: "center",
                justifyContent: "center",
                padding: 24,
                textAlign: "center",
                color: parsed.error ? "var(--red)" : "var(--muted)",
                fontSize: 13,
              }}
            >
              {parsed.error
                ? `Can't render graph: ${parsed.error}`
                : "Add a task with a name to see the DAG."}
            </div>
          )}
        </div>
      </div>
    </div>
  );
}
