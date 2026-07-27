// Dagre auto-layout: React Flow does not position nodes itself.
import dagre from "@dagrejs/dagre";
import { Position, type Edge, type Node } from "@xyflow/react";
import type { LayoutDirection } from "./direction";

export const NODE_W = 190;
/// A plain task node: name row + status row.
export const NODE_H = 52;
/// A node carrying a middle row as well — a sub-DAG/sub-workflow call showing
/// what it calls, or a step showing its container image. These were declared at
/// NODE_H like everything else, but React Flow applies a declared height to the
/// node wrapper, so the third row overflowed the box: the bottom handle sat
/// mid-node and the status line spilled past the border.
export const NODE_H_TALL = 68;

/// Height a StatusNode will actually render at, from the same data the node
/// renders. Both the graph and the editor build nodes through this so dagre
/// reserves the space the node really occupies.
export function statusNodeHeight(d: {
  templateRef?: string | null;
  workflowRef?: string | null;
  dockerImage?: string | null;
}): number {
  const hasMiddleRow = Boolean(d.templateRef || d.workflowRef || d.dockerImage);
  return hasMiddleRow ? NODE_H_TALL : NODE_H;
}

// "DG" (diagonal cascade) shears the top-down layout: every pixel a node sits
// lower also pushes it this many pixels right, so each rank steps down and to
// the right like a staircase.
const DIAGONAL_SHEAR = 0.8;

// Prefer what the node actually measured to; React Flow v12 reports that on
// `measured`, and `width`/`height` are only the pre-measurement declaration.
// Fall back to the declared dims, then to the default card size.
const w = (n: Node): number =>
  n.measured?.width ?? (typeof n.width === "number" ? n.width : NODE_W);
const h = (n: Node): number =>
  n.measured?.height ?? (typeof n.height === "number" ? n.height : NODE_H);

/// Assign x/y to nodes via a dagre layout in the given direction. Honors
/// per-node width/height (so smaller Start/End markers sit tight against the
/// tasks) and points each node's handles along the flow axis, so horizontal
/// edges leave from the sides rather than the top/bottom.
export function layout(nodes: Node[], edges: Edge[], dir: LayoutDirection = "TB"): Node[] {
  const g = new dagre.graphlib.Graph().setDefaultEdgeLabel(() => ({}));
  // Dagre only knows TB/LR; the diagonal cascade is a sheared TB layout.
  // LR ranks are wide (node width, not height), so give them more separation.
  g.setGraph({ rankdir: dir === "LR" ? "LR" : "TB", nodesep: 40, ranksep: dir === "LR" ? 80 : 60 });
  nodes.forEach((n) => g.setNode(n.id, { width: w(n), height: h(n) }));
  edges.forEach((e) => g.setEdge(e.source, e.target));
  dagre.layout(g);

  const handles =
    dir === "LR"
      ? { sourcePosition: Position.Right, targetPosition: Position.Left }
      : { sourcePosition: Position.Bottom, targetPosition: Position.Top };

  return nodes.map((n) => {
    const pos = g.node(n.id);
    // Shear by the rank's center-y so every node in a rank shifts equally,
    // regardless of its own height.
    const shear = dir === "DG" ? pos.y * DIAGONAL_SHEAR : 0;
    return {
      ...n,
      ...handles,
      position: { x: pos.x - w(n) / 2 + shear, y: pos.y - h(n) / 2 },
    };
  });
}
