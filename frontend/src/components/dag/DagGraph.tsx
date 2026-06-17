"use client";

import { useMemo } from "react";
import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  type Edge,
  type Node,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";

import StatusNode from "./StatusNode";
import { layout } from "./layout";
import { statusColor } from "@/lib/adapter";
import type { GraphResponse, TaskStatus } from "@/types/dagron";

const nodeTypes = { status: StatusNode };

export interface DagGraphProps {
  graph: GraphResponse;
  onNodeClick?: (taskId: string) => void;
}

/// The hero view: dagre-laid-out, status-colored DAG. Parent MUST give this a
/// sized container (explicit height) or React Flow renders 0px.
export default function DagGraph({ graph, onNodeClick }: DagGraphProps) {
  const { nodes, edges } = useMemo(() => {
    const rawNodes: Node[] = graph.nodes.map((n) => ({
      id: n.id,
      type: "status",
      position: { x: 0, y: 0 },
      // Explicit dims (match the dagre layout) so the MiniMap draws node rects —
      // custom nodes don't always report measured size to the minimap.
      width: 190,
      height: 52,
      data: { name: n.name, status: n.status, attempt: n.attempt },
    }));
    const rawEdges: Edge[] = graph.edges.map((e) => ({
      id: `${e.source}->${e.target}`,
      source: e.source,
      target: e.target,
      animated: false,
    }));
    return { nodes: layout(rawNodes, rawEdges), edges: rawEdges };
  }, [graph]);

  return (
    <div style={{ width: "100%", height: "100%" }}>
      <ReactFlow
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        fitView
        onNodeClick={(_, node) => onNodeClick?.(node.id)}
        proOptions={{ hideAttribution: true }}
      >
        <Background />
        <Controls />
        <MiniMap
          pannable
          zoomable
          maskColor="rgba(0,0,0,0.6)"
          nodeColor={(n) => statusColor(((n.data?.status as TaskStatus) ?? "pending") as TaskStatus)}
          nodeStrokeColor="#e6edf3"
          nodeStrokeWidth={3}
          nodeBorderRadius={3}
        />
      </ReactFlow>
    </div>
  );
}
