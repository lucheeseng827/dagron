// Synthetic Start/End "terminal" nodes that frame a DAG so a run reads as a flow
// with an explicit beginning and end — not just a cloud of command steps. The
// Start node fans out to every root task (no dependencies); every leaf task
// (nothing depends on it) fans into the End node.
//
// These are pure view sugar: they are never part of the workflow model or the
// engine's graph, so they carry ids that can't collide with a real task name.

import type { Edge, Node } from "@xyflow/react";
import type { RunStatus } from "@/types/dagron";
import { NODE_H, NODE_W } from "./layout";

export const START_ID = "__dagron_start__";
export const END_ID = "__dagron_end__";

// Terminal markers are smaller than task cards and pill-shaped.
export const SENTINEL_W = 132;
export const SENTINEL_H = 44;

// Neutral accent for the Start marker — it's an entry point, not a status, so it
// must not read as "succeeded".
export const START_COLOR = "#8b949e";

// Structural framing edges are dashed + muted to set them apart from real
// task-to-task dependencies.
const SENTINEL_EDGE_STYLE = { stroke: "#484f58", strokeDasharray: "4 4" } as const;

export type SentinelKind = "start" | "end";

/// True for a Start/End boundary marker (not a real task node). Callers use this
/// to ignore clicks on the framing markers.
export function isSentinel(node: { type?: string }): boolean {
  return node.type === "sentinel";
}

export interface SentinelData {
  kind: SentinelKind;
  label: string;
  /// Drives the End marker's accent color (reuses the status palette). Ignored
  /// for the Start marker, whose color is driven by `active`.
  status: string;
  /// Start marker only: true once the run is underway, so it "lights up" green
  /// instead of staying neutral. Always false for an unrun definition.
  active?: boolean;
  [key: string]: unknown;
}

/// roots = nodes with no incoming edge; leaves = nodes with no outgoing edge.
export function frontier(ids: string[], edges: { source: string; target: string }[]) {
  const hasIncoming = new Set(edges.map((e) => e.target));
  const hasOutgoing = new Set(edges.map((e) => e.source));
  return {
    roots: ids.filter((id) => !hasIncoming.has(id)),
    leaves: ids.filter((id) => !hasOutgoing.has(id)),
  };
}

/// Terminal state of the End marker, derived from the run's status. While the run
/// is pending/running the end is "not yet reached" (grey, "End"); once terminal it
/// takes the run's outcome color + label.
/// The Start marker "lights up" once the run is underway (running or finished).
/// It stays neutral while the run is still pending (queued) or for an unrun
/// definition in the editor (no run status).
function startActive(runStatus?: RunStatus): boolean {
  return runStatus != null && runStatus !== "pending";
}

function endStateFor(runStatus?: RunStatus): { status: string; label: string } {
  switch (runStatus) {
    case "succeeded":
      return { status: "succeeded", label: "Succeeded" };
    case "failed":
      return { status: "failed", label: "Failed" };
    case "cancelled":
      return { status: "cancelled", label: "Cancelled" };
    default:
      return { status: "pending", label: "End" };
  }
}

export interface SentinelOpts {
  /// Run status (run viewer). Omitted in the editor, where the definition is unrun.
  runStatus?: RunStatus;
}

/// Build the two sentinel Nodes (at origin) plus the edges wiring Start → every
/// root and every leaf → End. Returns empty arrays for an empty graph (nothing to
/// frame). Callers lay the nodes out — either via dagre (run viewer) or by
/// positioning relative to already-placed task nodes (editor).
export function buildSentinels(
  taskNodes: Node[],
  taskEdges: Edge[],
  opts: SentinelOpts = {},
): { nodes: Node[]; edges: Edge[] } {
  if (taskNodes.length === 0) return { nodes: [], edges: [] };

  const { roots, leaves } = frontier(taskNodes.map((n) => n.id), taskEdges);
  const end = endStateFor(opts.runStatus);

  // Sentinels are framing only: never draggable/selectable/deletable/connectable.
  const common = {
    type: "sentinel",
    position: { x: 0, y: 0 },
    width: SENTINEL_W,
    height: SENTINEL_H,
    draggable: false,
    selectable: false,
    deletable: false,
    connectable: false,
  } as const;

  const nodes: Node[] = [
    { ...common, id: START_ID, data: { kind: "start", label: "Start", status: "pending", active: startActive(opts.runStatus) } satisfies SentinelData },
    { ...common, id: END_ID, data: { kind: "end", label: end.label, status: end.status } satisfies SentinelData },
  ];

  const edgeCommon = { deletable: false, selectable: false, style: SENTINEL_EDGE_STYLE } as const;
  const edges: Edge[] = [
    ...roots.map((r) => ({ id: `${START_ID}->${r}`, source: START_ID, target: r, ...edgeCommon })),
    ...leaves.map((l) => ({ id: `${l}->${END_ID}`, source: l, target: END_ID, ...edgeCommon })),
  ];

  return { nodes, edges };
}

const dim = (v: number | null | undefined, fallback: number) => (typeof v === "number" ? v : fallback);

/// Position sentinels relative to already-laid-out task nodes: Start centered
/// above the root row, End centered below the leaf row. Used by the editable DAG,
/// which persists task positions and must not re-run the full dagre layout.
export function positionedSentinels(
  taskNodes: Node[],
  taskEdges: Edge[],
  opts: SentinelOpts = {},
): { nodes: Node[]; edges: Edge[] } {
  const base = buildSentinels(taskNodes, taskEdges, opts);
  if (base.nodes.length === 0) return base;

  const { roots, leaves } = frontier(taskNodes.map((n) => n.id), taskEdges);
  const byId = new Map(taskNodes.map((n) => [n.id, n]));
  const GAP = 70; // vertical gap between a sentinel and the nearest task row

  // These measure the *task* nodes, so fall back to the task card size, not the
  // sentinel's own smaller footprint.
  const centerX = (ids: string[]): number => {
    const src = ids.length ? ids : taskNodes.map((n) => n.id);
    const xs = src.map((id) => {
      const n = byId.get(id);
      return (n?.position?.x ?? 0) + dim(n?.width, NODE_W) / 2;
    });
    return xs.reduce((a, b) => a + b, 0) / xs.length;
  };
  const topY = Math.min(...taskNodes.map((n) => n.position?.y ?? 0));
  const bottomY = Math.max(...taskNodes.map((n) => (n.position?.y ?? 0) + dim(n.height, NODE_H)));

  const nodes = base.nodes.map((n) =>
    n.id === START_ID
      ? { ...n, position: { x: centerX(roots) - SENTINEL_W / 2, y: topY - GAP } }
      : { ...n, position: { x: centerX(leaves) - SENTINEL_W / 2, y: bottomY + GAP } },
  );
  return { nodes, edges: base.edges };
}
