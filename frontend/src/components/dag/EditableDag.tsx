"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  ReactFlowProvider,
  useEdgesState,
  useNodesState,
  useReactFlow,
  type Connection,
  type Edge,
  type Node,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";

import StatusNode from "./StatusNode";
import SentinelNode from "./SentinelNode";
import SnippetPalette from "./SnippetPalette";
import DirectionControl from "./DirectionControl";
import { useDagDirection, type LayoutDirection } from "./direction";
import { isSentinel, positionedSentinels } from "./sentinels";
import { layout, NODE_H, NODE_W } from "./layout";
import {
  buildPaletteTask,
  leafNames,
  snippetById,
  SNIPPET_MIME,
  type Snippet,
} from "@/lib/palette";
import {
  formatCommand,
  nextTaskName,
  parseCommand,
  spliceTask,
  wouldCycle,
  TRIGGER_RULES,
  type Task,
  type WorkflowModel,
} from "@/lib/spec-model";

const nodeTypes = { status: StatusNode, sentinel: SentinelNode };

export interface EditableDagProps {
  model: WorkflowModel;
  onChange: (model: WorkflowModel) => void;
}

/// Editable DAG: drag to lay out, drag handles to connect (adds a dependency),
/// select+Delete to remove, a premade-block palette (click to append; drag a
/// block onto the canvas to place it, or onto a dependency edge to splice it
/// between the two tasks), "+ Task" to add, and a side panel to edit the
/// selected task's fields. Structure is driven by `model`; positions are local.
export default function EditableDag(props: EditableDagProps) {
  // useReactFlow (drop-position mapping) needs a provider above the component.
  return (
    <ReactFlowProvider>
      <EditableDagInner {...props} />
    </ReactFlowProvider>
  );
}

function EditableDagInner({ model, onChange }: EditableDagProps) {
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [selected, setSelected] = useState<string | null>(null);
  // Layout direction (↓/→/↘), persisted + shared with the run viewer. Drives the
  // auto-layout of unpositioned nodes and the "re-arrange" control on the canvas.
  const [dir, setDir] = useDagDirection();
  // Remember positions across model rebuilds so edits don't reshuffle the graph.
  const positions = useRef<Record<string, { x: number; y: number }>>({});
  // screenToFlowPosition maps drop coords; fitView re-frames after a re-layout.
  const { screenToFlowPosition, fitView } = useReactFlow();

  // Rebuild RF nodes/edges whenever the model's structure changes.
  useEffect(() => {
    const rawNodes: Node[] = model.tasks.map((t) => ({
      id: t.name,
      type: "status",
      position: positions.current[t.name] ?? { x: 0, y: 0 },
      width: 190,
      height: 52,
      data: { name: t.name, status: "pending", attempt: 0, workflowRef: t.workflow_ref, dockerImage: t.docker_image },
      selected: t.name === selected,
    }));
    const rawEdges: Edge[] = model.tasks.flatMap((t) =>
      t.depends_on.map((dep) => ({ id: `${dep}->${t.name}`, source: dep, target: t.name })),
    );
    // Lay out only nodes without a remembered position.
    const needLayout = rawNodes.some((n) => !positions.current[n.id]);
    const laid = needLayout ? layout(rawNodes, rawEdges, dir) : rawNodes;
    for (const n of laid) positions.current[n.id] = n.position;
    // Frame the graph with read-only Start/End markers, placed relative to the
    // laid-out tasks so they never disturb the user's manual positions. Markers
    // go last (same order as the run viewer) to keep z-order consistent.
    const s = positionedSentinels(laid, rawEdges);
    setNodes([...laid, ...s.nodes]);
    setEdges([...rawEdges, ...s.edges]);
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [model, selected]);

  // Persist drag positions back into the ref.
  const handleNodesChange: typeof onNodesChange = useCallback(
    (changes) => {
      for (const c of changes) {
        if (c.type === "position" && c.position) positions.current[c.id] = c.position;
      }
      onNodesChange(changes);
    },
    [onNodesChange],
  );

  // Connecting two nodes = target depends_on source (if it stays acyclic).
  const onConnect = useCallback(
    (c: Connection) => {
      if (!c.source || !c.target) return;
      if (wouldCycle(model.tasks, c.source, c.target)) return;
      onChange({
        ...model,
        tasks: model.tasks.map((t) =>
          t.name === c.target && !t.depends_on.includes(c.source!)
            ? { ...t, depends_on: [...t.depends_on, c.source!] }
            : t,
        ),
      });
    },
    [model, onChange],
  );

  const onEdgesDelete = useCallback(
    (removed: Edge[]) => {
      const drop = new Set(removed.map((e) => `${e.source}->${e.target}`));
      onChange({
        ...model,
        tasks: model.tasks.map((t) => ({
          ...t,
          depends_on: t.depends_on.filter((d) => !drop.has(`${d}->${t.name}`)),
        })),
      });
    },
    [model, onChange],
  );

  const onNodesDelete = useCallback(
    (removed: Node[]) => {
      const drop = new Set(removed.map((n) => n.id));
      for (const id of drop) delete positions.current[id];
      onChange({
        ...model,
        tasks: model.tasks
          .filter((t) => !drop.has(t.name))
          .map((t) => ({ ...t, depends_on: t.depends_on.filter((d) => !drop.has(d)) })),
      });
      setSelected((s) => (s && drop.has(s) ? null : s));
    },
    [model, onChange],
  );

  const addTask = () => {
    const name = nextTaskName(model.tasks);
    onChange({ ...model, tasks: [...model.tasks, { name, command: ["echo", name], depends_on: [] }] });
    setSelected(name);
  };

  // Re-arrange the whole canvas in a new layout direction (↓/→/↘). Unlike an
  // edit, this discards manual positions and re-runs the auto-layout, so the
  // user gets a clean vertical / horizontal / diagonal arrangement on demand —
  // the same control the read-only run viewer has.
  const applyDirection = useCallback(
    (newDir: LayoutDirection) => {
      setDir(newDir);
      const rawNodes: Node[] = model.tasks.map((t) => ({
        id: t.name,
        type: "status",
        position: { x: 0, y: 0 },
        width: 190,
        height: 52,
        data: { name: t.name, status: "pending", attempt: 0, workflowRef: t.workflow_ref, dockerImage: t.docker_image },
        selected: t.name === selected,
      }));
      const rawEdges: Edge[] = model.tasks.flatMap((t) =>
        t.depends_on.map((dep) => ({ id: `${dep}->${t.name}`, source: dep, target: t.name })),
      );
      const laid = layout(rawNodes, rawEdges, newDir);
      positions.current = {};
      for (const n of laid) positions.current[n.id] = n.position;
      const s = positionedSentinels(laid, rawEdges);
      setNodes([...laid, ...s.nodes]);
      setEdges([...rawEdges, ...s.edges]);
      // Re-frame once React Flow has painted the new positions, so the whole
      // re-arranged chain stays in view (esp. horizontal, which runs wide).
      requestAnimationFrame(() => fitView({ duration: 300, padding: 0.2 }));
    },
    [model, selected, setDir, setNodes, setEdges, fitView],
  );

  // Palette insert failures ("result_from needs a task") are shown transiently
  // in the palette rail, then cleared — same UX as the spec editor's rail.
  const [paletteError, setPaletteError] = useState<string | null>(null);
  useEffect(() => {
    if (!paletteError) return;
    const t = setTimeout(() => setPaletteError(null), 4000);
    return () => clearTimeout(t);
  }, [paletteError]);

  // Clicking a block appends it: a task chains onto the current leaf tasks
  // (mirroring the spec editor's applySnippet), a run setting patches the model.
  const insertSnippet = useCallback(
    (s: Snippet) => {
      if (s.kind === "task") {
        const task = buildPaletteTask(s, model.tasks, leafNames(model.tasks));
        onChange({ ...model, tasks: [...model.tasks, task] });
        setSelected(task.name);
      } else {
        const next = { ...model };
        const err = s.patchRun(next);
        if (err) setPaletteError(err);
        else onChange(next);
      }
    },
    [model, onChange],
  );

  // Dropping a block places it exactly where it lands. Dropped on empty canvas
  // it starts unchained — the user draws the dependency edges. Dropped on a
  // dependency edge it is spliced into it: a -> b becomes a -> block -> b. The
  // position is remembered before the model rebuild so the new node skips
  // auto-layout and stays put.
  // The task->task edge currently under the drag cursor (highlighted as the
  // splice target). React Flow edges hit-test their own 20px interaction
  // stroke, so the drag event's target tells us which edge we're over; sentinel
  // Start/End edges are non-interactive and can't match, but validate both
  // endpoints against the model anyway.
  const [dropEdgeId, setDropEdgeId] = useState<string | null>(null);
  const edgeUnderDrag = useCallback(
    (e: React.DragEvent): { id: string; source: string; target: string } | null => {
      const id = (e.target as Element | null)
        ?.closest?.(".react-flow__edge")
        ?.getAttribute("data-id");
      if (!id) return null;
      const edge = edges.find((x) => x.id === id);
      const names = new Set(model.tasks.map((t) => t.name));
      if (!edge || !names.has(edge.source) || !names.has(edge.target)) return null;
      return { id, source: edge.source, target: edge.target };
    },
    [edges, model],
  );
  useEffect(() => {
    setEdges((es) =>
      es.map((ed) => {
        const hot = ed.id === dropEdgeId;
        if (hot === (ed.className === "dagron-edge-drop")) return ed;
        return { ...ed, className: hot ? "dagron-edge-drop" : undefined };
      }),
    );
  }, [dropEdgeId, setEdges]);
  const onDragOver = useCallback(
    (e: React.DragEvent) => {
      if (!e.dataTransfer.types.includes(SNIPPET_MIME)) return;
      e.preventDefault();
      e.dataTransfer.dropEffect = "copy";
      setDropEdgeId(edgeUnderDrag(e)?.id ?? null);
    },
    [edgeUnderDrag],
  );
  const onDragLeave = useCallback(() => setDropEdgeId(null), []);
  const onDrop = useCallback(
    (e: React.DragEvent) => {
      const s = snippetById(e.dataTransfer.getData(SNIPPET_MIME));
      if (!s || s.kind !== "task") return;
      e.preventDefault();
      setDropEdgeId(null);
      const p = screenToFlowPosition({ x: e.clientX, y: e.clientY });
      const task = buildPaletteTask(s, model.tasks, []);
      positions.current[task.name] = { x: p.x - NODE_W / 2, y: p.y - NODE_H / 2 };
      const hit = edgeUnderDrag(e);
      onChange({
        ...model,
        tasks: hit
          ? spliceTask(model.tasks, hit.source, hit.target, task)
          : [...model.tasks, task],
      });
      setSelected(task.name);
    },
    [model, onChange, screenToFlowPosition, edgeUnderDrag],
  );

  const sel = model.tasks.find((t) => t.name === selected) ?? null;

  return (
    <div style={{ display: "flex", height: "100%", width: "100%" }}>
      <SnippetPalette draggable onInsert={insertSnippet} error={paletteError} />
      <div style={{ flex: 1, minWidth: 0, position: "relative" }}>
        <div
          style={{ position: "absolute", top: 10, left: 10, zIndex: 5, display: "flex", gap: 8, alignItems: "center" }}
        >
          <button onClick={addTask} className="dy-btn dy-btn-primary">
            + Task
          </button>
          <DirectionControl dir={dir} onChange={applyDirection} />
        </div>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes}
          onNodesChange={handleNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          onEdgesDelete={onEdgesDelete}
          onNodesDelete={onNodesDelete}
          onNodeClick={(_, n) => !isSentinel(n) && setSelected(n.id)}
          onPaneClick={() => setSelected(null)}
          onDragOver={onDragOver}
          onDragLeave={onDragLeave}
          onDrop={onDrop}
          fitView
          proOptions={{ hideAttribution: true }}
        >
          <Background />
          <Controls />
          <MiniMap
            pannable
            zoomable
            maskColor="rgba(0,0,0,0.6)"
            nodeColor="#8b949e"
            nodeStrokeColor="#e6edf3"
            nodeStrokeWidth={3}
            nodeBorderRadius={3}
          />
        </ReactFlow>
      </div>
      <TaskPanel
        task={sel}
        allTasks={model.tasks}
        onChange={(updated, prevName) => {
          const next = applyTaskEdit(model, prevName, updated);
          onChange(next);
          // Follow an accepted rename so `selected` doesn't point at the old
          // (now-gone) name, which would null out `sel` and drop the edit panel.
          if (
            prevName !== updated.name &&
            !model.tasks.some((t) => t.name === updated.name) &&
            next.tasks.some((t) => t.name === updated.name)
          ) {
            setSelected(updated.name);
          }
        }}
        onDelete={(n) => onNodesDelete([{ id: n } as Node])}
        onSelectName={setSelected}
      />
    </div>
  );
}

/// Apply an edited task back into the model, renaming dependency references when
/// the task's name changed. Rejects a rename that collides with another task.
function applyTaskEdit(model: WorkflowModel, prevName: string, updated: Task): WorkflowModel {
  const renamed = updated.name !== prevName;
  if (renamed) {
    const collide = model.tasks.some((t) => t.name === updated.name);
    if (!updated.name.trim() || collide) {
      // Keep the old name; apply only the other field edits.
      updated = { ...updated, name: prevName };
    }
  }
  return {
    ...model,
    tasks: model.tasks.map((t) => {
      if (t.name === prevName) return updated;
      if (renamed && t.depends_on.includes(prevName)) {
        return { ...t, depends_on: t.depends_on.map((d) => (d === prevName ? updated.name : d)) };
      }
      return t;
    }),
  };
}

function TaskPanel({
  task,
  allTasks,
  onChange,
  onDelete,
  onSelectName,
}: {
  task: Task | null;
  allTasks: Task[];
  onChange: (updated: Task, prevName: string) => void;
  onDelete: (name: string) => void;
  onSelectName: (name: string) => void;
}) {
  if (!task) {
    return (
      <aside style={panelStyle}>
        <p style={{ color: "var(--muted)", fontSize: 13 }}>
          Click a block to append it to the pipeline, drag one onto the canvas to place it, or drop
          it on an edge to splice it between two steps. Select a task to edit it; drag from a
          node&apos;s handle to another to add a dependency.
        </p>
      </aside>
    );
  }
  const prev = task.name;
  const patch = (p: Partial<Task>) => onChange({ ...task, ...p }, prev);
  // Empty clears the field; otherwise require an integer >= the field's minimum
  // (retry delay allows 0, counts/timeouts require 1).
  const intField = (v: string, min: number): number | undefined => {
    if (v === "") return undefined;
    const n = Number(v);
    return Number.isInteger(n) && n >= min ? n : undefined;
  };

  return (
    <aside style={panelStyle}>
      <div className="dy-cardhead">
        <strong>Task</strong>
        <button className="dy-btn dy-btn-danger" onClick={() => onDelete(task.name)}>
          Delete
        </button>
      </div>

      <Label>Name</Label>
      <input style={inputStyle} value={task.name} onChange={(e) => patch({ name: e.target.value })} />

      {task.workflow_ref ? (
        <>
          <Label>Runs workflow</Label>
          <input style={{ ...inputStyle, marginBottom: 4 }} value={task.workflow_ref} readOnly />
          <p style={{ color: "var(--muted)", fontSize: 11, marginTop: 0, marginBottom: 12 }}>
            This step chains another saved workflow. Its tasks are inlined when the run starts.
            Change the reference in the YAML view.
          </p>
        </>
      ) : (
        <>
          <Label>Command</Label>
          <input
            style={inputStyle}
            value={formatCommand(task.command ?? [])}
            onChange={(e) => patch({ command: parseCommand(e.target.value) })}
            placeholder='echo "hello world"'
          />
          <Label>Docker image</Label>
          <input
            style={inputStyle}
            value={task.docker_image ?? ""}
            onChange={(e) => patch({ docker_image: e.target.value || undefined })}
            placeholder="(runs on host — e.g. alpine:3.20)"
            title="Container image to pull and run the command in; empty runs on the host executor"
          />
        </>
      )}

      {!task.workflow_ref && (
        <div style={{ display: "flex", gap: 8, alignItems: "stretch" }}>
          <div style={fieldCol}>
            <Label>Max attempts</Label>
            <input
              style={fieldInput}
              type="number"
              min={1}
              value={task.max_attempts ?? ""}
              onChange={(e) => patch({ max_attempts: intField(e.target.value, 1) })}
            />
          </div>
          <div style={fieldCol}>
            <Label>Retry delay s</Label>
            <input
              style={fieldInput}
              type="number"
              min={0}
              value={task.retry_delay_secs ?? ""}
              onChange={(e) => patch({ retry_delay_secs: intField(e.target.value, 0) })}
            />
          </div>
          <div style={fieldCol}>
            <Label>Timeout s</Label>
            <input
              style={fieldInput}
              type="number"
              min={1}
              value={task.timeout_secs ?? ""}
              onChange={(e) => patch({ timeout_secs: intField(e.target.value, 1) })}
            />
          </div>
        </div>
      )}

      {!task.workflow_ref && (
        <>
          <Label>Run when</Label>
          <select
            style={inputStyle}
            value={task.trigger_rule ?? ""}
            onChange={(e) => patch({ trigger_rule: e.target.value || undefined })}
            title="When this task fires relative to its dependencies' outcomes (engine trigger_rule)"
          >
            <option value="">all_success (default)</option>
            {TRIGGER_RULES.filter((r) => r !== "all_success").map((r) => (
              <option key={r} value={r}>
                {r}
              </option>
            ))}
          </select>
        </>
      )}

      <Label>Depends on</Label>
      <div style={{ display: "flex", flexDirection: "column", gap: 4 }}>
        {allTasks.filter((t) => t.name !== task.name).length === 0 && (
          <span style={{ color: "var(--dim)", fontSize: 12 }}>No other tasks yet.</span>
        )}
        {allTasks
          .filter((t) => t.name !== task.name)
          .map((t) => {
            const checked = task.depends_on.includes(t.name);
            const cyclic = !checked && wouldCycle(allTasks, t.name, task.name);
            return (
              <label
                key={t.name}
                style={{ display: "flex", alignItems: "center", gap: 8, fontSize: 13, color: cyclic ? "var(--dim)" : "var(--fg)" }}
                title={cyclic ? "Would create a cycle" : undefined}
              >
                <input
                  type="checkbox"
                  checked={checked}
                  disabled={cyclic}
                  onChange={(e) =>
                    patch({
                      depends_on: e.target.checked
                        ? [...task.depends_on, t.name]
                        : task.depends_on.filter((d) => d !== t.name),
                    })
                  }
                />
                <span onClick={() => onSelectName(t.name)} style={{ cursor: "pointer" }}>
                  {t.name}
                </span>
              </label>
            );
          })}
      </div>
    </aside>
  );
}

const panelStyle: React.CSSProperties = {
  width: 280,
  flexShrink: 0,
  borderLeft: "1px solid var(--border)",
  background: "var(--side)",
  padding: 16,
  overflow: "auto",
};
const inputStyle: React.CSSProperties = {
  width: "100%",
  background: "var(--bg)",
  color: "var(--fg)",
  border: "1px solid var(--border)",
  borderRadius: 7,
  padding: "7px 9px",
  marginBottom: 12,
  fontSize: 13,
};
// A field column in a multi-column row: stretch to the tallest column (so a label that wraps to
// two lines doesn't shift this input up), with the input pinned to the bottom so all inputs align.
const fieldCol: React.CSSProperties = {
  flex: 1,
  display: "flex",
  flexDirection: "column",
};
const fieldInput: React.CSSProperties = { ...inputStyle, marginTop: "auto" };
function Label({ children }: { children: React.ReactNode }) {
  return (
    <div style={{ fontSize: 11, color: "var(--muted)", textTransform: "uppercase", letterSpacing: "0.04em", marginBottom: 5 }}>
      {children}
    </div>
  );
}
