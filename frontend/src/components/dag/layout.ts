// Dagre auto-layout: React Flow does not position nodes itself.
import dagre from "@dagrejs/dagre";
import { Position, type Edge, type Node } from "@xyflow/react";
import type { LayoutDirection } from "./direction";

export const NODE_W = 190;

/// Row metrics for a StatusNode, shared with the component so the height React
/// Flow *declares* and the height the content *renders* to are computed from one
/// source.
///
/// They have to agree. React Flow sizes the node wrapper from the declaration,
/// and StatusNode is a fixed-height flex column — so when the rows need more
/// room than was declared, the difference is taken out of the flex items rather
/// than overflowing. The name row is the one that shrinks visibly: it carries
/// `overflow: hidden`, so a squeezed line box crops the glyphs mid-letter
/// instead of spilling. That is what clipped the task names, and it is why
/// these are constants rather than numbers picked to look right: a hardcoded
/// height silently under-reserves as soon as a font or a row changes.
export const NODE_PAD_Y = 8;
export const NODE_BORDER = 1;
/// Name row — 13px/600. Explicit, because `line-height: normal` resolves from
/// font metrics and differs across the fallback stack.
export const NODE_TITLE_LH = 18;
/// Status row and middle row — 11px.
export const NODE_ROW_LH = 14;
/// `marginTop` on the status row, and on the middle row when present.
export const NODE_ROW_GAP = 4;
export const NODE_MID_GAP = 2;

const NODE_CHROME = 2 * NODE_PAD_Y + 2 * NODE_BORDER;

/// A plain task node: name row + status row.
export const NODE_H = NODE_CHROME + NODE_TITLE_LH + NODE_ROW_GAP + NODE_ROW_LH;
/// One middle row's cost: its own line box plus the gap above it. A node can
/// carry more than one — a task with both a `workflow_ref` and a `template:`
/// renders a subtitle for each, since StatusNode gates them independently — so
/// the height counts rows rather than adding a single fixed bump. These rows
/// were once declared at NODE_H like everything else, but React Flow applies a
/// declared height to the node wrapper, so any extra row overflowed the box: the
/// bottom handle sat mid-node and the status line spilled past the border.
export const NODE_MID_ROW = NODE_MID_GAP + NODE_ROW_LH;
/// Height of a node carrying exactly one middle row.
export const NODE_H_TALL = NODE_H + NODE_MID_ROW;

/// Height a StatusNode will actually render at, from the same data the node
/// renders. Both the graph and the editor build nodes through this so dagre
/// reserves the space the node really occupies. The middle-row count mirrors
/// StatusNode exactly: the sub-workflow row and the template row are independent
/// and can both show, while the image row shows only when neither reference does.
export function statusNodeHeight(d: {
  templateRef?: string | null;
  workflowRef?: string | null;
  dockerImage?: string | null;
}): number {
  const isWorkflowRef = Boolean(d.workflowRef);
  const isTemplate = Boolean(d.templateRef);
  const midRows =
    Number(isWorkflowRef) +
    Number(isTemplate) +
    Number(!isWorkflowRef && !isTemplate && Boolean(d.dockerImage));
  return NODE_H + midRows * NODE_MID_ROW;
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
