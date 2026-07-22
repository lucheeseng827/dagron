"use client";

import { useMemo, useState } from "react";
import {
  SNIPPETS,
  SNIPPET_CATEGORIES,
  SNIPPET_MIME,
  type Snippet,
} from "@/lib/palette";

/// The premade-block rail beside the spec/visual editors. Each block is a
/// ready-made, engine-supported piece of spec — a task (chained onto the current
/// pipeline end) or a run-level setting — so users compose instead of typing.
/// With `draggable`, task blocks can also be dragged onto the DAG canvas and
/// dropped exactly where they should sit — on empty canvas unchained (the user
/// wires the edges), or on a dependency edge to splice between the two tasks.
export default function SnippetPalette({
  onInsert,
  draggable,
  disabled,
  disabledReason,
  error,
}: {
  onInsert: (s: Snippet) => void;
  /// Enable HTML5 drag on task blocks (the visual editor's canvas accepts them).
  draggable?: boolean;
  /// True when the current spec doesn't parse — inserting would clobber it.
  disabled?: boolean;
  disabledReason?: string;
  /// Transient message when the last insert was refused (e.g. result_from with
  /// no tasks yet).
  error?: string | null;
}) {
  const [query, setQuery] = useState("");
  // Case-insensitive match on label, description, and category so "s3", "ml",
  // or "parquet" each surface the right blocks from the full library.
  const visible = useMemo(() => {
    const q = query.trim().toLowerCase();
    if (!q) return SNIPPETS;
    return SNIPPETS.filter((s) =>
      `${s.label} ${s.description} ${s.category}`.toLowerCase().includes(q),
    );
  }, [query]);

  return (
    <aside
      style={{
        width: 220,
        flexShrink: 0,
        borderRight: "1px solid var(--border)",
        background: "var(--side)",
        overflowY: "auto",
        padding: "10px 8px",
      }}
    >
      <div
        style={{
          fontSize: 11,
          letterSpacing: "0.06em",
          textTransform: "uppercase",
          color: "var(--muted)",
          padding: "0 6px 4px",
        }}
      >
        Blocks
      </div>
      <input
        value={query}
        onChange={(e) => setQuery(e.target.value)}
        placeholder="Search blocks… (s3, ml, dbt)"
        aria-label="Search blocks"
        style={{
          width: "100%",
          background: "var(--bg)",
          color: "var(--fg)",
          border: "1px solid var(--border)",
          borderRadius: 6,
          padding: "5px 8px",
          fontSize: 12,
          marginBottom: 4,
        }}
      />
      {disabled && (
        <p style={{ color: "var(--muted)", fontSize: 11, padding: "0 6px", marginTop: 2 }}>
          {disabledReason ?? "Fix the YAML to insert blocks."}
        </p>
      )}
      {error && (
        <p style={{ color: "var(--red)", fontSize: 11, padding: "0 6px", marginTop: 2 }}>{error}</p>
      )}
      {visible.length === 0 && (
        <p style={{ color: "var(--dim)", fontSize: 12, padding: "0 6px", marginTop: 8 }}>
          No blocks match “{query}”.
        </p>
      )}
      {SNIPPET_CATEGORIES.map((cat) => {
        const inCat = visible.filter((s) => s.category === cat);
        if (inCat.length === 0) return null;
        return (
          <div key={cat} style={{ marginTop: 10 }}>
            <div style={{ fontSize: 11, color: "var(--dim)", padding: "0 6px 4px" }}>{cat}</div>
            <div style={{ display: "flex", flexDirection: "column", gap: 2 }}>
              {inCat.map((s) => (
                <PaletteItem
                  key={s.id}
                  snippet={s}
                  draggable={draggable}
                  disabled={disabled}
                  onInsert={onInsert}
                />
              ))}
            </div>
          </div>
        );
      })}
    </aside>
  );
}

function PaletteItem({
  snippet,
  draggable,
  disabled,
  onInsert,
}: {
  snippet: Snippet;
  draggable?: boolean;
  disabled?: boolean;
  onInsert: (s: Snippet) => void;
}) {
  const [hover, setHover] = useState(false);
  // Only task blocks are draggable — a run-level setting has no node to place.
  const canDrag = !!draggable && !disabled && snippet.kind === "task";
  return (
    <button
      onClick={() => onInsert(snippet)}
      disabled={disabled}
      draggable={canDrag}
      onDragStart={(e) => {
        e.dataTransfer.setData(SNIPPET_MIME, snippet.id);
        e.dataTransfer.effectAllowed = "copy";
      }}
      onMouseEnter={() => setHover(true)}
      onMouseLeave={() => setHover(false)}
      title={
        snippet.kind === "task"
          ? canDrag
            ? "Click to add after the current end of the pipeline; drag onto the canvas to place it, or onto an edge to splice it between two steps"
            : "Adds a step after the current end of the pipeline"
          : "Sets a run-level option"
      }
      style={{
        textAlign: "left",
        background: hover && !disabled ? "var(--panel-2)" : "transparent",
        border: "1px solid transparent",
        borderRadius: 6,
        padding: "6px 6px",
        cursor: disabled ? "default" : canDrag ? "grab" : "pointer",
        opacity: disabled ? 0.45 : 1,
        color: "var(--fg)",
        display: "flex",
        flexDirection: "column",
        gap: 1,
      }}
    >
      <span style={{ fontSize: 12.5, fontWeight: 600 }}>＋ {snippet.label}</span>
      <span style={{ fontSize: 11, color: "var(--muted)" }}>{snippet.description}</span>
    </button>
  );
}
