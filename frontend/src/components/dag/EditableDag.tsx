"use client";

import { useCallback, useEffect, useRef, useState } from "react";
import {
  Background,
  Controls,
  MiniMap,
  ReactFlow,
  useEdgesState,
  useNodesState,
  type Connection,
  type Edge,
  type Node,
} from "@xyflow/react";
import "@xyflow/react/dist/style.css";

import StatusNode from "./StatusNode";
import { layout } from "./layout";
import {
  formatCommand,
  nextTaskName,
  parseCommand,
  wouldCycle,
  type Task,
  type WorkflowModel,
} from "@/lib/spec-model";

const nodeTypes = { status: StatusNode };

export interface EditableDagProps {
  model: WorkflowModel;
  onChange: (model: WorkflowModel) => void;
}

/// Editable DAG: drag to lay out, drag handles to connect (adds a dependency),
/// select+Delete to remove, "+ Task" to add, and a side panel to edit the
/// selected task's fields. Structure is driven by `model`; positions are local.
export default function EditableDag({ model, onChange }: EditableDagProps) {
  const [nodes, setNodes, onNodesChange] = useNodesState<Node>([]);
  const [edges, setEdges, onEdgesChange] = useEdgesState<Edge>([]);
  const [selected, setSelected] = useState<string | null>(null);
  // Remember positions across model rebuilds so edits don't reshuffle the graph.
  const positions = useRef<Record<string, { x: number; y: number }>>({});

  // Rebuild RF nodes/edges whenever the model's structure changes.
  useEffect(() => {
    const rawNodes: Node[] = model.tasks.map((t) => ({
      id: t.name,
      type: "status",
      position: positions.current[t.name] ?? { x: 0, y: 0 },
      width: 190,
      height: 52,
      data: { name: t.name, status: "pending", attempt: 0, workflowRef: t.workflow_ref },
      selected: t.name === selected,
    }));
    const rawEdges: Edge[] = model.tasks.flatMap((t) =>
      t.depends_on.map((dep) => ({ id: `${dep}->${t.name}`, source: dep, target: t.name })),
    );
    // Lay out only nodes without a remembered position.
    const needLayout = rawNodes.some((n) => !positions.current[n.id]);
    const laid = needLayout ? layout(rawNodes, rawEdges) : rawNodes;
    for (const n of laid) positions.current[n.id] = n.position;
    setNodes(laid);
    setEdges(rawEdges);
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

  const sel = model.tasks.find((t) => t.name === selected) ?? null;

  return (
    <div style={{ display: "flex", height: "100%", width: "100%" }}>
      <div style={{ flex: 1, minWidth: 0, position: "relative" }}>
        <button
          onClick={addTask}
          className="dy-btn dy-btn-primary"
          style={{ position: "absolute", top: 10, left: 10, zIndex: 5 }}
        >
          + Task
        </button>
        <ReactFlow
          nodes={nodes}
          edges={edges}
          nodeTypes={nodeTypes}
          onNodesChange={handleNodesChange}
          onEdgesChange={onEdgesChange}
          onConnect={onConnect}
          onEdgesDelete={onEdgesDelete}
          onNodesDelete={onNodesDelete}
          onNodeClick={(_, n) => setSelected(n.id)}
          onPaneClick={() => setSelected(null)}
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
          Select a task to edit, or drag from a node&apos;s handle to another to add a dependency.
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
        </>
      )}

      {!task.workflow_ref && (
        <div style={{ display: "flex", gap: 8 }}>
          <div style={{ flex: 1 }}>
            <Label>Max attempts</Label>
            <input
              style={inputStyle}
              type="number"
              min={1}
              value={task.max_attempts ?? ""}
              onChange={(e) => patch({ max_attempts: intField(e.target.value, 1) })}
            />
          </div>
          <div style={{ flex: 1 }}>
            <Label>Retry delay s</Label>
            <input
              style={inputStyle}
              type="number"
              min={0}
              value={task.retry_delay_secs ?? ""}
              onChange={(e) => patch({ retry_delay_secs: intField(e.target.value, 0) })}
            />
          </div>
          <div style={{ flex: 1 }}>
            <Label>Timeout s</Label>
            <input
              style={inputStyle}
              type="number"
              min={1}
              value={task.timeout_secs ?? ""}
              onChange={(e) => patch({ timeout_secs: intField(e.target.value, 1) })}
            />
          </div>
        </div>
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
function Label({ children }: { children: React.ReactNode }) {
  return (
    <div style={{ fontSize: 11, color: "var(--muted)", textTransform: "uppercase", letterSpacing: "0.04em", marginBottom: 5 }}>
      {children}
    </div>
  );
}
