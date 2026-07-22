// Dagre auto-layout: React Flow does not position nodes itself.
import dagre from "@dagrejs/dagre";
import { Position, type Edge, type Node } from "@xyflow/react";
import type { LayoutDirection } from "./direction";

export const NODE_W = 190;
export const NODE_H = 52;

// "DG" (diagonal cascade) shears the top-down layout: every pixel a node sits
// lower also pushes it this many pixels right, so each rank steps down and to
// the right like a staircase.
const DIAGONAL_SHEAR = 0.8;

// Fall back to the default card size when a node doesn't declare its own dims.
const w = (n: Node): number => (typeof n.width === "number" ? n.width : NODE_W);
const h = (n: Node): number => (typeof n.height === "number" ? n.height : NODE_H);

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
