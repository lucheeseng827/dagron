// SPDX-License-Identifier: Apache-2.0
//! DAG model, parsing, and validation.

use anyhow::{bail, Result};
use petgraph::{algo::is_cyclic_directed, graph::DiGraph};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// A single task in a workflow.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct TaskSpec {
    pub name: String,
    pub command: Vec<String>,
    #[serde(default)]
    pub depends_on: Vec<String>,
    #[serde(default)]
    pub input: Option<serde_json::Value>,
    /// How many times this task may be attempted before it is marked failed.
    /// 1 = no retries (default). Must be ≥ 1.
    #[serde(default = "default_max_attempts")]
    pub max_attempts: u32,
    /// Base delay in seconds between retries.
    /// Actual delay = `retry_delay_secs * 2^(attempt-1)`. 0 = immediate retry.
    #[serde(default)]
    pub retry_delay_secs: u64,
    /// Per-task subprocess timeout in seconds. Falls back to a 25 s default when absent.
    pub timeout_secs: Option<u64>,
}

fn default_max_attempts() -> u32 {
    1
}

/// A parsed workflow definition.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct DagSpec {
    pub name: String,
    #[serde(default)]
    pub tasks: Vec<TaskSpec>,
}

/// A validated DAG: an acyclic dependency graph over named tasks.
pub struct DagGraph {
    pub spec: DagSpec,
    graph: DiGraph<String, ()>,
    node_index: HashMap<String, petgraph::graph::NodeIndex>,
}

impl DagGraph {
    /// Parse and validate a DAG from a YAML (or JSON) string. Rejects duplicate
    /// task names, unknown dependencies, `max_attempts == 0`, and cycles.
    pub fn from_yaml(yaml: &str) -> Result<Self> {
        let spec: DagSpec = serde_yaml::from_str(yaml)?;

        let mut graph = DiGraph::new();
        let mut node_index = HashMap::new();

        for task in &spec.tasks {
            if node_index.contains_key(&task.name) {
                bail!("duplicate task name '{}' in DAG '{}'", task.name, spec.name);
            }
            if task.max_attempts == 0 {
                bail!(
                    "invalid max_attempts=0 for task '{}' in DAG '{}'; expected >= 1",
                    task.name,
                    spec.name
                );
            }
            let idx = graph.add_node(task.name.clone());
            node_index.insert(task.name.clone(), idx);
        }

        for task in &spec.tasks {
            for dep in &task.depends_on {
                let &from = node_index.get(dep).ok_or_else(|| {
                    anyhow::anyhow!("unknown dependency '{dep}' in task '{}'", task.name)
                })?;
                let &to = node_index.get(&task.name).unwrap();
                graph.add_edge(from, to, ());
            }
        }

        if is_cyclic_directed(&graph) {
            bail!("DAG '{}' contains a cycle", spec.name);
        }

        Ok(Self {
            spec,
            graph,
            node_index,
        })
    }

    /// Number of incoming edges (direct dependencies) for a task.
    ///
    /// # Panics
    /// Panics if `task_name` is not a task in this DAG. Callers iterate over
    /// `spec.tasks`, so every name is known to be valid.
    pub fn dep_count(&self, task_name: &str) -> usize {
        let idx = self.node_index[task_name];
        self.graph
            .edges_directed(idx, petgraph::Direction::Incoming)
            .count()
    }

    pub fn task_spec(&self, task_name: &str) -> Option<&TaskSpec> {
        self.spec.tasks.iter().find(|t| t.name == task_name)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_cycle() {
        let yaml = r#"
name: cyclic
tasks:
  - { name: a, command: ["true"], depends_on: ["b"] }
  - { name: b, command: ["true"], depends_on: ["a"] }
"#;
        assert!(DagGraph::from_yaml(yaml).is_err());
    }

    #[test]
    fn rejects_unknown_dependency() {
        let yaml = r#"
name: bad
tasks:
  - { name: a, command: ["true"], depends_on: ["ghost"] }
"#;
        assert!(DagGraph::from_yaml(yaml).is_err());
    }

    #[test]
    fn counts_dependencies() {
        let yaml = r#"
name: diamond
tasks:
  - { name: a, command: ["true"] }
  - { name: b, command: ["true"], depends_on: ["a"] }
  - { name: c, command: ["true"], depends_on: ["a"] }
  - { name: d, command: ["true"], depends_on: ["b", "c"] }
"#;
        let dag = DagGraph::from_yaml(yaml).unwrap();
        assert_eq!(dag.dep_count("a"), 0);
        assert_eq!(dag.dep_count("d"), 2);
    }
}
