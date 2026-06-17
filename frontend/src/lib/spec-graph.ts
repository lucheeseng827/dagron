// Parse a DAG YAML spec (client-side) into a GraphResponse for the visual editor.
// Mirrors the engine's dag shape: { name, tasks: [{ name, depends_on? }] }.

import yaml from "js-yaml";
import type { GraphResponse } from "@/types/dagron";

interface RawTask {
  name?: unknown;
  depends_on?: unknown;
}
interface RawSpec {
  name?: unknown;
  tasks?: unknown;
}

/// Convert a YAML spec to graph nodes/edges. Returns the graph, or an error
/// string if the YAML is unparseable / not the expected shape. All nodes are
/// rendered as `pending` (this is an unrun definition, not a live run).
export function specToGraph(specYaml: string): { graph?: GraphResponse; error?: string } {
  let doc: RawSpec;
  try {
    doc = (yaml.load(specYaml) ?? {}) as RawSpec;
  } catch (e) {
    return { error: e instanceof Error ? e.message : "invalid YAML" };
  }
  if (!doc || typeof doc !== "object" || !Array.isArray(doc.tasks)) {
    return { error: "spec must have a `tasks:` list" };
  }

  const names = new Set<string>();
  for (const t of doc.tasks as RawTask[]) {
    if (t && typeof t.name === "string") names.add(t.name);
  }

  const nodes = [...names].map((name) => ({
    id: name,
    name,
    status: "pending" as const,
    attempt: 0,
    scheduled_at: null,
    finished_at: null,
  }));

  const edges: { source: string; target: string }[] = [];
  for (const t of doc.tasks as RawTask[]) {
    if (!t || typeof t.name !== "string") continue;
    const deps = Array.isArray(t.depends_on) ? t.depends_on : [];
    for (const dep of deps) {
      if (typeof dep === "string" && names.has(dep)) {
        edges.push({ source: dep, target: t.name });
      }
    }
  }

  return { graph: { nodes, edges } };
}
