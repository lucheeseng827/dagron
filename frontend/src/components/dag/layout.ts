// Dagre auto-layout: React Flow does not position nodes itself.
import dagre from "@dagrejs/dagre";
import type { Edge, Node } from "@xyflow/react";

const NODE_W = 190;
const NODE_H = 52;

/// Assign x/y to nodes via a top-to-bottom (TB) dagre layout.
export function layout(nodes: Node[], edges: Edge[], dir: "TB" | "LR" = "TB"): Node[] {
  const g = new dagre.graphlib.Graph().setDefaultEdgeLabel(() => ({}));
  g.setGraph({ rankdir: dir, nodesep: 40, ranksep: 60 });
  nodes.forEach((n) => g.setNode(n.id, { width: NODE_W, height: NODE_H }));
  edges.forEach((e) => g.setEdge(e.source, e.target));
  dagre.layout(g);

  return nodes.map((n) => {
    const pos = g.node(n.id);
    return {
      ...n,
      position: { x: pos.x - NODE_W / 2, y: pos.y - NODE_H / 2 },
    };
  });
}
