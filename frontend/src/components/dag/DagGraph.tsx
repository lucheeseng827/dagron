"use client";

import { useMemo } from "react";
import {
  Background,
  Controls,
  MiniMap,
  Panel,
  ReactFlow,
  type Edge,
  type Node,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";

import StatusNode from "./StatusNode";
import SentinelNode from "./SentinelNode";
import { buildSentinels, isSentinel } from "./sentinels";
import { layout } from "./layout";
import { useDagDirection, type LayoutDirection } from "./direction";
import DirectionControl from "./DirectionControl";
import { statusColor } from "@/lib/adapter";
import type { GraphResponse, RunStatus, TaskStatus } from "@/types/dagron";

const nodeTypes = { status: StatusNode, sentinel: SentinelNode };

export interface DagGraphProps {
  graph: GraphResponse;
  /// The run's status, so the End marker can reflect the run's outcome. Omit for
  /// an unrun definition.
  runStatus?: RunStatus;
  onNodeClick?: (taskId: string) => void;
  /// Controlled layout direction. Pass both to hold the direction in the parent
  /// (the editor does, so it survives topology-keyed remounts); omit to let the
  /// graph manage its own persisted direction.
  direction?: LayoutDirection;
  onDirectionChange?: (d: LayoutDirection) => void;
}

/// The hero view: dagre-laid-out, status-colored DAG framed by Start/End markers,
/// with a segmented control to switch the layout between vertical, horizontal,
/// and diagonal-cascade flow. Parent MUST give this a sized container (explicit
/// height) or React Flow renders 0px.
export default function DagGraph({ graph, runStatus, onNodeClick, direction, onDirectionChange }: DagGraphProps) {
  const [ownDir, setOwnDir] = useDagDirection();
  const dir = direction ?? ownDir;
  const setDir = onDirectionChange ?? setOwnDir;

  const { nodes, edges } = useMemo(() => {
    const rawNodes: Node[] = graph.nodes.map((n) => ({
      id: n.id,
      type: "status",
      position: { x: 0, y: 0 },
      // Explicit dims (match the dagre layout) so the MiniMap draws node rects —
      // custom nodes don't always report measured size to the minimap.
      width: 190,
      height: 52,
      data: {
        name: n.name,
        status: n.status,
        attempt: n.attempt,
        scheduledAt: n.scheduled_at,
        finishedAt: n.finished_at,
      },
    }));
    const rawEdges: Edge[] = graph.edges.map((e) => ({
      id: `${e.source}->${e.target}`,
      source: e.source,
      target: e.target,
      animated: false,
    }));
    // Frame the DAG with Start/End boundary markers, then lay out everything
    // together so the markers sit tight against the root/leaf tasks.
    const sentinels = buildSentinels(rawNodes, rawEdges, { runStatus });
    const allNodes = [...rawNodes, ...sentinels.nodes];
    const allEdges = [...rawEdges, ...sentinels.edges];
    return { nodes: layout(allNodes, allEdges, dir), edges: allEdges };
  }, [graph, runStatus, dir]);

  return (
    <div style={{ width: "100%", height: "100%" }}>
      {/* Keyed on direction so switching remounts the flow and fitView re-frames
          the new shape (fitView only runs on init). */}
      <ReactFlow
        key={dir}
        nodes={nodes}
        edges={edges}
        nodeTypes={nodeTypes}
        fitView
        // Start/End markers aren't tasks — clicking one opens no task panel.
        onNodeClick={(_, node) => !isSentinel(node) && onNodeClick?.(node.id)}
        proOptions={{ hideAttribution: true }}
      >
        <Panel position="top-left">
          <DirectionControl dir={dir} onChange={setDir} />
        </Panel>
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

